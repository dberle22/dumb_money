"""DuckDB-backed Market Performance section builders and renderers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dumb_money import _matplotlib  # noqa: F401
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd
from matplotlib.figure import Figure

from dumb_money.analytics.company import (
    build_benchmark_comparison,
    build_indexed_price_series,
    build_trailing_return_comparison,
    prepare_price_history,
)
from dumb_money.analytics.scorecard import resolve_secondary_benchmark
from dumb_money.config import AppSettings, get_settings
from dumb_money.storage import query_canonical_data, warehouse_table_exists
from dumb_money.transforms.benchmark_sets import stage_benchmark_sets
from dumb_money.research.company import load_gold_ticker_metrics_row

SECTION_WINDOWS: tuple[str, ...] = ("1m", "3m", "6m", "1y")
SERIES_COLORS: dict[str, str] = {
    "company": "#111827",
    "primary_benchmark": "#2563eb",
    "secondary_benchmark": "#0f766e",
}


@dataclass(slots=True)
class MarketPerformanceSectionData:
    """Query-ready Market Performance inputs and derived outputs."""

    ticker: str
    primary_benchmark: str
    secondary_benchmark: str | None
    price_history: pd.DataFrame
    histories: dict[str, pd.DataFrame]
    trailing_return_comparison: pd.DataFrame
    benchmark_comparison: pd.DataFrame


def _format_percent(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}%}"


def _ensure_market_performance_duckdb_inputs(
    *,
    benchmark_set_id: str,
    settings: AppSettings,
) -> None:
    # Stage benchmark-set definitions into DuckDB if they are not materialized yet.
    if not warehouse_table_exists("benchmark_sets", settings=settings):
        stage_benchmark_sets(
            settings=settings,
            set_id=benchmark_set_id,
            write_warehouse=True,
            write_csv=True,
        )

def _load_benchmark_mapping(
    ticker: str,
    *,
    settings: AppSettings,
) -> dict[str, object]:
    # Query the canonical benchmark mapping row for this ticker directly from DuckDB.
    mapping_sql = """
        select
            ticker,
            sector,
            primary_benchmark,
            sector_benchmark,
            industry_benchmark,
            style_benchmark,
            custom_benchmark
        from benchmark_mappings
        where ticker = ?
        limit 1
    """
    mapping = query_canonical_data(mapping_sql, parameters=[ticker], settings=settings)
    return mapping.iloc[0].to_dict() if not mapping.empty else {}


def _load_benchmark_set_members(
    benchmark_set_id: str,
    *,
    settings: AppSettings,
) -> pd.DataFrame:
    if not warehouse_table_exists("benchmark_sets", settings=settings):
        return pd.DataFrame(columns=["ticker", "member_order"])

    # Query the named benchmark set from DuckDB so the section can fall back to a
    # known list of benchmark tickers if a mapped secondary benchmark is unavailable.
    benchmark_set_sql = """
        select ticker, member_order
        from benchmark_sets
        where set_id = ?
        order by member_order
    """
    return query_canonical_data(benchmark_set_sql, parameters=[benchmark_set_id], settings=settings)


def _load_price_history_from_duckdb(
    tickers: list[str],
    *,
    settings: AppSettings,
) -> pd.DataFrame:
    placeholders = ", ".join(["?"] * len(tickers))

    # Query all section price history from DuckDB in one read so the code makes the
    # source-of-truth table and filter conditions explicit.
    price_sql = f"""
        select
            ticker,
            date,
            interval,
            source,
            currency,
            open,
            high,
            low,
            close,
            adj_close,
            volume
        from normalized_prices
        where interval = '1d'
          and ticker in ({placeholders})
        order by ticker, date
    """
    return query_canonical_data(price_sql, parameters=tickers, settings=settings)


def build_market_performance_section_data(
    ticker: str,
    *,
    benchmark_set_id: str = "sample_universe",
    settings: AppSettings | None = None,
) -> MarketPerformanceSectionData:
    """Build the Market Performance section from canonical DuckDB tables."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    # Materialize any missing benchmark-set or benchmark-price inputs into DuckDB
    # before querying the section. After this point the section reads from DuckDB.
    _ensure_market_performance_duckdb_inputs(
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )

    mart_row = load_gold_ticker_metrics_row(normalized_ticker, settings=settings)
    mapping = _load_benchmark_mapping(normalized_ticker, settings=settings)
    benchmark_set_members = _load_benchmark_set_members(benchmark_set_id, settings=settings)
    benchmark_set_tickers = benchmark_set_members["ticker"].astype(str).str.upper().tolist()

    candidate_tickers = [normalized_ticker, *benchmark_set_tickers]
    mart_benchmarks = [
        str(mart_row.get("primary_benchmark") or "").upper(),
        str(mart_row.get("secondary_benchmark") or "").upper(),
    ]
    candidate_tickers.extend([value for value in mart_benchmarks if value])
    if mapping:
        candidate_tickers.extend(
            [
                str(mapping.get("primary_benchmark") or "").upper(),
                str(mapping.get("sector_benchmark") or "").upper(),
                str(mapping.get("industry_benchmark") or "").upper(),
                str(mapping.get("style_benchmark") or "").upper(),
                str(mapping.get("custom_benchmark") or "").upper(),
            ]
        )
    candidate_tickers = [value for value in candidate_tickers if value]
    ordered_unique_tickers = list(dict.fromkeys(candidate_tickers))

    prices = _load_price_history_from_duckdb(ordered_unique_tickers, settings=settings)
    if prices.empty:
        raise ValueError(f"no canonical DuckDB prices found for {normalized_ticker}")

    # Split the queried DuckDB price table into ticker-specific histories used by the
    # shared return and indexing transforms.
    histories = {
        value: prepare_price_history(prices, value)
        for value in ordered_unique_tickers
    }
    histories = {
        value: history
        for value, history in histories.items()
        if not history.empty
    }

    if normalized_ticker not in histories:
        raise ValueError(f"no canonical DuckDB price history found for {normalized_ticker}")

    available_benchmarks = {value for value in histories if value != normalized_ticker}
    primary_benchmark = str(mart_row.get("primary_benchmark") or "").upper()
    if not primary_benchmark:
        primary_benchmark = str(mapping.get("primary_benchmark") or "").upper() if mapping else ""
    if not primary_benchmark or primary_benchmark not in available_benchmarks:
        primary_benchmark = benchmark_set_tickers[0] if benchmark_set_tickers else ""
        if primary_benchmark not in available_benchmarks:
            primary_benchmark = next(iter(sorted(available_benchmarks)), "")

    mapped_secondary_candidates = [str(mart_row.get("secondary_benchmark") or "").upper()]
    mapped_secondary_candidates.extend(
        [
            str(mapping.get("sector_benchmark") or "").upper(),
            str(mapping.get("style_benchmark") or "").upper(),
            str(mapping.get("industry_benchmark") or "").upper(),
            str(mapping.get("custom_benchmark") or "").upper(),
        ]
    )
    mapped_secondary = next(
        (
            value
            for value in mapped_secondary_candidates
            if value and value != primary_benchmark and value in available_benchmarks
        ),
        None,
    )
    secondary_benchmark = mapped_secondary or resolve_secondary_benchmark(
        str(mapping.get("sector")) if mapping else None,
        available_benchmarks - ({primary_benchmark} if primary_benchmark else set()),
    )

    comparison_benchmarks = {
        benchmark: histories[benchmark]
        for benchmark in [primary_benchmark, secondary_benchmark]
        if benchmark and benchmark in histories
    }

    # Transform the queried DuckDB histories into the shared section outputs we want
    # to review: trailing returns and excess returns versus benchmarks.
    trailing_return_comparison = build_trailing_return_comparison(
        histories[normalized_ticker],
        comparison_benchmarks,
    )
    benchmark_comparison = build_benchmark_comparison(
        histories[normalized_ticker],
        comparison_benchmarks,
    )

    return MarketPerformanceSectionData(
        ticker=normalized_ticker,
        primary_benchmark=primary_benchmark,
        secondary_benchmark=secondary_benchmark if secondary_benchmark in comparison_benchmarks else None,
        price_history=prices,
        histories=histories,
        trailing_return_comparison=trailing_return_comparison,
        benchmark_comparison=benchmark_comparison,
    )


def build_market_performance_table(data: MarketPerformanceSectionData) -> pd.DataFrame:
    """Build a standardized Market Performance comparison table."""

    comparison = data.trailing_return_comparison.copy()
    if comparison.empty:
        return pd.DataFrame(columns=["Window", data.ticker])

    comparison = comparison.loc[comparison["window"].isin(SECTION_WINDOWS)].copy()
    comparison = comparison.rename(columns={"window": "Window", "company_return": data.ticker})

    preferred_columns = ["Window", data.ticker]
    for benchmark in [data.primary_benchmark, data.secondary_benchmark]:
        if benchmark and f"{benchmark}_return" in comparison.columns:
            comparison = comparison.rename(columns={f"{benchmark}_return": benchmark})
            preferred_columns.append(benchmark)

    for benchmark in [data.primary_benchmark, data.secondary_benchmark]:
        if benchmark and benchmark in comparison.columns:
            excess_label = f"{data.ticker} vs {benchmark}"
            comparison[excess_label] = pd.to_numeric(comparison[data.ticker], errors="coerce") - pd.to_numeric(
                comparison[benchmark],
                errors="coerce",
            )
            preferred_columns.append(excess_label)

    display = comparison[preferred_columns].copy()
    for column in display.columns:
        if column != "Window":
            display[column] = display[column].map(_format_percent)
    return display.reset_index(drop=True)


def render_market_performance_section(data: MarketPerformanceSectionData) -> Figure:
    """Render the full Market Performance section as one reviewable figure."""

    fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(11, 13))
    indexed_ax, returns_ax, excess_ax = axes

    indexed_series_frames: list[pd.DataFrame] = []
    display_order = [
        (data.ticker, "company"),
        (data.primary_benchmark, "primary_benchmark"),
        (data.secondary_benchmark, "secondary_benchmark"),
    ]

    # Plot indexed price history for the company and selected benchmarks. Each series
    # is rebased to 100 at the start of the display window so relative performance is visible.
    for ticker, role in display_order:
        if not ticker or ticker not in data.histories:
            continue
        series = build_indexed_price_series(data.histories[ticker], ticker=ticker)
        if series.empty:
            continue
        indexed_series_frames.append(series)
        indexed_ax.plot(
            series["date"],
            series["indexed_price"],
            label=ticker,
            linewidth=2.2,
            color=SERIES_COLORS.get(role, "#475569"),
        )

    indexed_ax.set_title(f"{data.ticker} Market Performance", loc="left", fontsize=14, fontweight="bold")
    indexed_ax.set_ylabel("Indexed to 100")
    indexed_ax.grid(axis="y", linestyle=":", alpha=0.35)
    indexed_ax.legend(loc="upper left")
    if indexed_series_frames:
        indexed_values = pd.concat(indexed_series_frames, ignore_index=True)["indexed_price"]
        y_min = float(indexed_values.min())
        y_max = float(indexed_values.max())
        padding = max((y_max - y_min) * 0.08, 3.0)
        indexed_ax.set_ylim(y_min - padding, y_max + padding)

    # Plot trailing returns only where the section has enough history to calculate them.
    trailing = data.trailing_return_comparison.copy()
    trailing = trailing.loc[trailing["window"].isin(SECTION_WINDOWS)].copy()
    x_positions = list(range(len(trailing)))
    bar_width = 0.22
    offsets = [-bar_width, 0.0, bar_width]

    series_map = [(data.ticker, "company_return", "company")]
    for benchmark, role in [
        (data.primary_benchmark, "primary_benchmark"),
        (data.secondary_benchmark, "secondary_benchmark"),
    ]:
        if benchmark and f"{benchmark}_return" in trailing.columns:
            series_map.append((benchmark, f"{benchmark}_return", role))

    if trailing.empty or pd.to_numeric(trailing["company_return"], errors="coerce").dropna().empty:
        returns_ax.text(0.5, 0.5, "Insufficient history for trailing return comparison", ha="center", va="center")
        returns_ax.axis("off")
    else:
        for offset, (label, column, role) in zip(offsets, series_map, strict=False):
            values = pd.to_numeric(trailing[column], errors="coerce")
            returns_ax.bar(
                [x + offset for x in x_positions],
                values,
                width=bar_width,
                label=label,
                color=SERIES_COLORS.get(role, "#475569"),
            )

        returns_ax.axhline(0, color="#475569", linewidth=1)
        returns_ax.set_xticks(x_positions, trailing["window"])
        returns_ax.set_ylabel("Return")
        returns_ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=0))
        returns_ax.grid(axis="y", linestyle=":", alpha=0.35)
        returns_ax.legend(loc="upper left")
    returns_ax.set_title("Trailing Return Comparison", loc="left")

    # Plot excess return as stock return minus benchmark return over each trailing window.
    benchmark_comparison = data.benchmark_comparison.copy()
    benchmark_comparison = benchmark_comparison.loc[benchmark_comparison["window"].isin(SECTION_WINDOWS)].copy()
    if benchmark_comparison.empty:
        excess_ax.text(0.5, 0.5, "No excess return data available", ha="center", va="center")
        excess_ax.axis("off")
    else:
        pivoted = benchmark_comparison.pivot(index="window", columns="benchmark_ticker", values="excess_return")
        pivoted = pivoted.reindex(list(SECTION_WINDOWS))
        selected_columns = [
            benchmark
            for benchmark in [data.primary_benchmark, data.secondary_benchmark]
            if benchmark and benchmark in pivoted.columns
        ]
        if selected_columns:
            pivoted = pivoted[selected_columns]
        pivoted = pivoted.apply(pd.to_numeric, errors="coerce")
        if pivoted.dropna(how="all").empty:
            excess_ax.text(0.5, 0.5, "Insufficient history for excess return comparison", ha="center", va="center")
            excess_ax.axis("off")
        else:
            pivoted.plot(
                kind="bar",
                ax=excess_ax,
                color=[SERIES_COLORS["primary_benchmark"], SERIES_COLORS["secondary_benchmark"]][: len(pivoted.columns)],
            )
            excess_ax.axhline(0, color="#475569", linewidth=1)
            excess_ax.set_xlabel("Window")
            excess_ax.set_ylabel("Excess Return")
            excess_ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=0))
            excess_ax.grid(axis="y", linestyle=":", alpha=0.35)
            excess_ax.legend(loc="upper left", title="Benchmark")
    excess_ax.set_title(f"{data.ticker} Excess Return", loc="left")

    fig.tight_layout()
    return fig


def save_market_performance_section(
    ticker: str,
    *,
    output_dir: str | Path,
    benchmark_set_id: str = "sample_universe",
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Save Market Performance review artifacts for one ticker."""

    section_data = build_market_performance_section_data(
        ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    artifact_stem = f"{section_data.ticker.lower()}_market_performance"
    figure_path = destination / f"{artifact_stem}.png"
    table_path = destination / f"{artifact_stem}_table.csv"

    figure = render_market_performance_section(section_data)
    figure.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(figure)

    build_market_performance_table(section_data).to_csv(table_path, index=False)

    return {
        "figure_path": figure_path,
        "table_path": table_path,
    }


__all__ = [
    "MarketPerformanceSectionData",
    "build_market_performance_section_data",
    "build_market_performance_table",
    "render_market_performance_section",
    "save_market_performance_section",
]
