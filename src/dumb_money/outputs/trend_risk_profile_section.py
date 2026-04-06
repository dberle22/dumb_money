"""DuckDB-aware Trend and Risk Profile section builders and renderers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from dumb_money import _matplotlib  # noqa: F401
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd
from matplotlib.figure import Figure

from dumb_money.analytics.company import (
    build_drawdown_series,
    build_moving_average_series,
    calculate_risk_metrics,
    calculate_trend_metrics,
)
from dumb_money.config import AppSettings, get_settings
from dumb_money.outputs.market_performance_section import (
    SERIES_COLORS,
    build_market_performance_section_data,
)
from dumb_money.research.company import CompanyResearchPacket

SUMMARY_COLUMNS = ["Metric", "Value", "Assessment", "Shared Input"]


@dataclass(slots=True)
class TrendRiskProfileSectionData:
    """Query-ready Trend and Risk Profile inputs and standardized outputs."""

    ticker: str
    primary_benchmark: str | None
    secondary_benchmark: str | None
    company_history: pd.DataFrame
    benchmark_histories: dict[str, pd.DataFrame]
    moving_average_series: pd.DataFrame
    drawdown_series: dict[str, pd.DataFrame]
    risk_metrics: dict[str, float | None]
    trend_metrics: dict[str, float | bool | None]
    summary_table: pd.DataFrame
    summary_strip: pd.DataFrame
    interpretation_text: str


def _format_percent(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}%}"


def _format_number(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}f}"


def _format_signal_strength(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value * 100:.0f} / 100"


def _normalize_higher_is_better(value: float | None, bands: list[tuple[float, float]]) -> float | None:
    if value is None or pd.isna(value):
        return None
    for threshold, normalized_score in bands:
        if float(value) >= threshold:
            return normalized_score
    return 0.0


def _normalize_lower_is_better(value: float | None, bands: list[tuple[float, float]]) -> float | None:
    if value is None or pd.isna(value):
        return None
    for threshold, normalized_score in bands:
        if float(value) <= threshold:
            return normalized_score
    return 0.0


def _assess_normalized_value(normalized_value: float | None) -> str:
    if normalized_value is None or pd.isna(normalized_value):
        return "Unavailable"
    if normalized_value >= 0.95:
        return "Clear strength"
    if normalized_value >= 0.70:
        return "Supportive"
    if normalized_value >= 0.45:
        return "Mixed"
    if normalized_value > 0:
        return "Constraint"
    return "Weak"


def _trend_structure_assessment(
    price_vs_sma_50: float | None,
    price_vs_sma_200: float | None,
    sma_50_above_sma_200: bool | None,
) -> tuple[str, float | None]:
    if sma_50_above_sma_200 is None:
        return "Unavailable", None
    if sma_50_above_sma_200 and (price_vs_sma_50 or 0) > 0 and (price_vs_sma_200 or 0) > 0:
        return "Trend intact", 1.0
    if sma_50_above_sma_200 or (price_vs_sma_200 is not None and price_vs_sma_200 > 0):
        return "Mostly constructive", 0.75
    if (price_vs_sma_50 is not None and price_vs_sma_50 > -0.05) or (
        price_vs_sma_200 is not None and price_vs_sma_200 > -0.08
    ):
        return "Mixed", 0.5
    return "Trend under pressure", 0.25


def _build_interpretation_text(data: TrendRiskProfileSectionData) -> str:
    price_vs_sma_200 = data.trend_metrics.get("price_vs_sma_200")
    volatility_1y = data.risk_metrics.get("annualized_volatility_1y")
    downside_volatility_1y = data.risk_metrics.get("downside_volatility_1y")
    beta_1y = data.risk_metrics.get("beta_1y")
    current_drawdown = data.risk_metrics.get("current_drawdown")
    max_drawdown_1y = data.risk_metrics.get("max_drawdown_1y")
    trend_structure = data.summary_table.loc[
        data.summary_table["Metric"] == "Trend Structure",
        "Assessment",
    ].iloc[0]
    benchmark_note = (
        f" Drawdown context includes {data.primary_benchmark}"
        + (f" and {data.secondary_benchmark}." if data.secondary_benchmark else ".")
        if data.primary_benchmark
        else ""
    )

    risk_sentence = (
        f"Trailing one-year volatility is {_format_percent(volatility_1y)}, downside volatility is {_format_percent(downside_volatility_1y)}, "
        f"and beta versus the primary benchmark is {_format_number(beta_1y)}."
        if beta_1y is not None
        else (
            f"Trailing one-year volatility is {_format_percent(volatility_1y)} and downside volatility is "
            f"{_format_percent(downside_volatility_1y)}. "
            "Beta is unavailable when the section does not have enough overlapping benchmark return history."
        )
    )

    return (
        f"{data.ticker} shows a {trend_structure.lower()} price profile. "
        f"Price is {_format_percent(price_vs_sma_200)} versus the 200-day average, "
        f"with current drawdown at {_format_percent(current_drawdown)} and trailing one-year max drawdown at "
        f"{_format_percent(max_drawdown_1y)}. "
        f"{risk_sentence}"
        f"{benchmark_note}"
    )


def build_trend_risk_profile_table(data: TrendRiskProfileSectionData) -> pd.DataFrame:
    """Build the standardized Trend and Risk Profile summary table."""

    return data.summary_table.copy()


def build_trend_risk_profile_strip_table(data: TrendRiskProfileSectionData) -> pd.DataFrame:
    """Build the compact Trend and Risk Profile summary strip."""

    return data.summary_strip.copy()


def build_trend_risk_profile_text_from_data(data: TrendRiskProfileSectionData) -> str:
    """Build the reusable short interpretation for this section."""

    return data.interpretation_text


def build_trend_risk_profile_section_data_from_packet(
    packet: CompanyResearchPacket,
) -> TrendRiskProfileSectionData:
    """Build standardized Trend and Risk outputs from the shared research packet."""

    primary_benchmark = packet.scorecard.summary.get("primary_benchmark")
    secondary_benchmark = packet.scorecard.summary.get("secondary_benchmark")
    benchmark_histories = {
        ticker: history
        for ticker, history in packet.benchmark_histories.items()
        if ticker in {primary_benchmark, secondary_benchmark} and not history.empty
    }

    # The section intentionally consumes the packet's shared risk and trend metrics
    # instead of recalculating notebook-only values.
    moving_average_series = build_moving_average_series(packet.company_history, moving_averages=(50, 200))
    drawdown_series = {
        packet.ticker: build_drawdown_series(packet.company_history, ticker=packet.ticker),
    }
    for ticker, history in benchmark_histories.items():
        drawdown_series[ticker] = build_drawdown_series(history, ticker=ticker)

    trend_structure, trend_signal = _trend_structure_assessment(
        packet.trend_metrics.get("price_vs_sma_50"),
        packet.trend_metrics.get("price_vs_sma_200"),
        packet.trend_metrics.get("sma_50_above_sma_200"),
    )
    volatility_signal = _normalize_lower_is_better(
        packet.risk_metrics.get("annualized_volatility_1y"),
        [(0.20, 1.0), (0.30, 0.75), (0.40, 0.5), (0.55, 0.25)],
    )
    max_drawdown_signal = _normalize_lower_is_better(
        abs(packet.risk_metrics["max_drawdown_1y"]) if packet.risk_metrics.get("max_drawdown_1y") is not None else None,
        [(0.10, 1.0), (0.20, 0.75), (0.30, 0.5), (0.40, 0.25)],
    )
    current_drawdown_signal = _normalize_lower_is_better(
        abs(packet.risk_metrics["current_drawdown"]) if packet.risk_metrics.get("current_drawdown") is not None else None,
        [(0.05, 1.0), (0.10, 0.75), (0.20, 0.5), (0.30, 0.25)],
    )
    price_vs_sma_50_signal = _normalize_higher_is_better(
        packet.trend_metrics.get("price_vs_sma_50"),
        [(0.08, 1.0), (0.02, 0.75), (-0.02, 0.5), (-0.08, 0.25)],
    )
    price_vs_sma_200_signal = _normalize_higher_is_better(
        packet.trend_metrics.get("price_vs_sma_200"),
        [(0.12, 1.0), (0.03, 0.75), (-0.03, 0.5), (-0.10, 0.25)],
    )
    downside_volatility_signal = _normalize_lower_is_better(
        packet.risk_metrics.get("downside_volatility_1y"),
        [(0.12, 1.0), (0.20, 0.75), (0.28, 0.5), (0.40, 0.25)],
    )
    beta_value = packet.risk_metrics.get("beta_1y")
    beta_signal = _normalize_lower_is_better(
        abs(beta_value - 1.0) if beta_value is not None else None,
        [(0.15, 1.0), (0.35, 0.75), (0.60, 0.5), (1.0, 0.25)],
    )

    summary_table = pd.DataFrame(
        [
            {
                "Metric": "Trend Structure",
                "Value": (
                    "50D above 200D"
                    if packet.trend_metrics.get("sma_50_above_sma_200")
                    else "50D below 200D"
                    if packet.trend_metrics.get("sma_50_above_sma_200") is not None
                    else "N/A"
                ),
                "Assessment": trend_structure,
                "Shared Input": "trend_metrics.sma_50_above_sma_200",
            },
            {
                "Metric": "Price vs 50D MA",
                "Value": _format_percent(packet.trend_metrics.get("price_vs_sma_50")),
                "Assessment": _assess_normalized_value(price_vs_sma_50_signal),
                "Shared Input": "trend_metrics.price_vs_sma_50",
            },
            {
                "Metric": "Price vs 200D MA",
                "Value": _format_percent(packet.trend_metrics.get("price_vs_sma_200")),
                "Assessment": _assess_normalized_value(price_vs_sma_200_signal),
                "Shared Input": "trend_metrics.price_vs_sma_200",
            },
            {
                "Metric": "1M Volatility",
                "Value": _format_percent(packet.risk_metrics.get("annualized_volatility_1m")),
                "Assessment": "Context only",
                "Shared Input": "risk_metrics.annualized_volatility_1m",
            },
            {
                "Metric": "3M Volatility",
                "Value": _format_percent(packet.risk_metrics.get("annualized_volatility_3m")),
                "Assessment": "Context only",
                "Shared Input": "risk_metrics.annualized_volatility_3m",
            },
            {
                "Metric": "1Y Volatility",
                "Value": _format_percent(packet.risk_metrics.get("annualized_volatility_1y")),
                "Assessment": _assess_normalized_value(volatility_signal),
                "Shared Input": "risk_metrics.annualized_volatility_1y",
            },
            {
                "Metric": "Current Drawdown",
                "Value": _format_percent(packet.risk_metrics.get("current_drawdown")),
                "Assessment": _assess_normalized_value(current_drawdown_signal),
                "Shared Input": "risk_metrics.current_drawdown",
            },
            {
                "Metric": "Max Drawdown (1Y)",
                "Value": _format_percent(packet.risk_metrics.get("max_drawdown_1y")),
                "Assessment": _assess_normalized_value(max_drawdown_signal),
                "Shared Input": "risk_metrics.max_drawdown_1y",
            },
            {
                "Metric": "Beta vs Primary Benchmark",
                "Value": _format_number(beta_value),
                "Assessment": _assess_normalized_value(beta_signal),
                "Shared Input": "risk_metrics.beta_1y",
            },
            {
                "Metric": "Downside Volatility",
                "Value": _format_percent(packet.risk_metrics.get("downside_volatility_1y")),
                "Assessment": _assess_normalized_value(downside_volatility_signal),
                "Shared Input": "risk_metrics.downside_volatility_1y",
            },
        ],
        columns=SUMMARY_COLUMNS,
    )

    summary_strip = pd.DataFrame(
        [
            {
                "signal": "Trend Structure",
                "band_score": trend_signal,
                "band_score_display": _format_signal_strength(trend_signal),
                "value_display": summary_table.loc[summary_table["Metric"] == "Trend Structure", "Value"].iloc[0],
                "assessment": trend_structure,
            },
            {
                "signal": "Price vs 50D MA",
                "band_score": price_vs_sma_50_signal,
                "band_score_display": _format_signal_strength(price_vs_sma_50_signal),
                "value_display": _format_percent(packet.trend_metrics.get("price_vs_sma_50")),
                "assessment": _assess_normalized_value(price_vs_sma_50_signal),
            },
            {
                "signal": "Price vs 200D MA",
                "band_score": price_vs_sma_200_signal,
                "band_score_display": _format_signal_strength(price_vs_sma_200_signal),
                "value_display": _format_percent(packet.trend_metrics.get("price_vs_sma_200")),
                "assessment": _assess_normalized_value(price_vs_sma_200_signal),
            },
            {
                "signal": "1Y Volatility",
                "band_score": volatility_signal,
                "band_score_display": _format_signal_strength(volatility_signal),
                "value_display": _format_percent(packet.risk_metrics.get("annualized_volatility_1y")),
                "assessment": _assess_normalized_value(volatility_signal),
            },
            {
                "signal": "Max Drawdown (1Y)",
                "band_score": max_drawdown_signal,
                "band_score_display": _format_signal_strength(max_drawdown_signal),
                "value_display": _format_percent(packet.risk_metrics.get("max_drawdown_1y")),
                "assessment": _assess_normalized_value(max_drawdown_signal),
            },
            {
                "signal": "Downside Volatility",
                "band_score": downside_volatility_signal,
                "band_score_display": _format_signal_strength(downside_volatility_signal),
                "value_display": _format_percent(packet.risk_metrics.get("downside_volatility_1y")),
                "assessment": _assess_normalized_value(downside_volatility_signal),
            },
            {
                "signal": "Beta vs Benchmark",
                "band_score": beta_signal,
                "band_score_display": _format_signal_strength(beta_signal),
                "value_display": _format_number(beta_value),
                "assessment": _assess_normalized_value(beta_signal),
            },
        ]
    )

    data = TrendRiskProfileSectionData(
        ticker=packet.ticker,
        primary_benchmark=primary_benchmark,
        secondary_benchmark=secondary_benchmark,
        company_history=packet.company_history,
        benchmark_histories=benchmark_histories,
        moving_average_series=moving_average_series,
        drawdown_series=drawdown_series,
        risk_metrics=packet.risk_metrics,
        trend_metrics=packet.trend_metrics,
        summary_table=summary_table,
        summary_strip=summary_strip,
        interpretation_text="",
    )
    data.interpretation_text = _build_interpretation_text(data)
    return data


def build_trend_risk_profile_section_data(
    ticker: str,
    *,
    benchmark_set_id: str = "sample_universe",
    settings: AppSettings | None = None,
) -> TrendRiskProfileSectionData:
    """Build the Trend and Risk Profile section from canonical shared inputs."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    # Reuse the Market Performance section's DuckDB-aware history contract so this
    # section stays aligned on benchmark selection and canonical price access.
    market_data = build_market_performance_section_data(
        normalized_ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )
    company_history = market_data.histories[normalized_ticker]
    benchmark_histories = {
        benchmark: market_data.histories[benchmark]
        for benchmark in [market_data.primary_benchmark, market_data.secondary_benchmark]
        if benchmark and benchmark in market_data.histories
    }
    primary_benchmark_history = benchmark_histories.get(market_data.primary_benchmark)
    risk_metrics = calculate_risk_metrics(company_history, benchmark_history=primary_benchmark_history)
    trend_metrics = calculate_trend_metrics(company_history)

    packet = CompanyResearchPacket(
        ticker=normalized_ticker,
        company_name=None,
        as_of_date=None,
        company_history=company_history,
        benchmark_histories=benchmark_histories,
        return_windows=pd.DataFrame(),
        trailing_return_comparison=market_data.trailing_return_comparison,
        risk_metrics=risk_metrics,
        trend_metrics=trend_metrics,
        benchmark_comparison=market_data.benchmark_comparison,
        fundamentals_summary={},
        peer_return_comparison=pd.DataFrame(),
        peer_return_summary_stats={},
        peer_valuation_comparison=pd.DataFrame(),
        peer_summary_stats={},
        sector_snapshot={},
        scorecard=SimpleNamespace(
            summary={
                "primary_benchmark": market_data.primary_benchmark,
                "secondary_benchmark": market_data.secondary_benchmark,
            }
        ),
    )
    return build_trend_risk_profile_section_data_from_packet(packet)


def render_trend_risk_profile_section(data: TrendRiskProfileSectionData) -> Figure:
    """Render the full Trend and Risk Profile section as one reviewable figure."""

    fig, axes = plt.subplots(
        nrows=4,
        ncols=1,
        figsize=(11.5, 15.0),
        gridspec_kw={"height_ratios": [1.0, 1.8, 1.8, 2.0]},
    )
    strip_ax, drawdown_ax, moving_average_ax, summary_ax = axes

    strip = data.summary_strip.copy()
    y_positions = list(range(len(strip)))
    band_score = strip["band_score"].fillna(0.0)
    strip_ax.barh(y_positions, [1.0] * len(strip), color="#e2e8f0", height=0.55)
    strip_ax.barh(y_positions, band_score, color="#0f766e", height=0.55)
    strip_ax.set_yticks(y_positions, strip["signal"])
    strip_ax.invert_yaxis()
    strip_ax.set_xlim(0, 1)
    strip_ax.set_xlabel("Assessment Band Score")
    strip_ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=0))
    strip_ax.set_title(f"{data.ticker} Trend and Risk Profile", loc="left", fontsize=14, fontweight="bold")
    strip_ax.grid(axis="x", linestyle=":", alpha=0.35)
    for y_position, score, value_display in zip(y_positions, band_score, strip["value_display"], strict=False):
        strip_ax.text(min(float(score) + 0.03, 0.98), y_position, value_display, va="center", ha="left", fontsize=9)
    strip_ax.text(
        0.0,
        -0.14,
        "Bars show threshold-based band scores, not raw values.",
        transform=strip_ax.transAxes,
        fontsize=8.5,
        color="#475569",
    )

    display_order = [
        (data.ticker, "company"),
        (data.primary_benchmark, "primary_benchmark"),
        (data.secondary_benchmark, "secondary_benchmark"),
    ]
    any_drawdown = False
    # Plot company and benchmark drawdowns on the same trailing window so the
    # section shows whether risk came from stock-specific weakness or market stress.
    for ticker, role in display_order:
        if not ticker or ticker not in data.drawdown_series:
            continue
        series = data.drawdown_series[ticker]
        if series.empty:
            continue
        any_drawdown = True
        drawdown_ax.plot(
            series["date"],
            series["drawdown"],
            label=ticker,
            linewidth=2.2 if ticker == data.ticker else 1.8,
            color=SERIES_COLORS.get(role, "#475569"),
        )
    if any_drawdown:
        drawdown_ax.axhline(0, color="#475569", linewidth=1)
        drawdown_ax.set_ylabel("Drawdown")
        drawdown_ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=0))
        drawdown_ax.grid(axis="y", linestyle=":", alpha=0.35)
        drawdown_ax.legend(loc="lower left")
    else:
        drawdown_ax.text(0.5, 0.5, "No drawdown history available", ha="center", va="center")
        drawdown_ax.axis("off")
    drawdown_ax.set_title("Drawdown / Underwater Chart", loc="left")
    drawdown_ax.text(
        0.0,
        -0.20,
        "0% means the stock is at a fresh high for the displayed window; -20% means it is 20% below its running peak.",
        transform=drawdown_ax.transAxes,
        fontsize=8.5,
        color="#475569",
    )

    moving_average_series = data.moving_average_series.copy()
    if moving_average_series.empty:
        moving_average_ax.text(0.5, 0.5, "No moving-average history available", ha="center", va="center")
        moving_average_ax.axis("off")
    else:
        # Plot the company price against 50-day and 200-day averages to make the
        # trend interpretation visible rather than relying on summary metrics alone.
        moving_average_ax.plot(
            moving_average_series["date"],
            moving_average_series["adj_close"],
            label=data.ticker,
            linewidth=2.2,
            color=SERIES_COLORS["company"],
        )
        moving_average_ax.plot(
            moving_average_series["date"],
            moving_average_series["sma_50"],
            label="50D MA",
            linewidth=1.8,
            color="#2563eb",
        )
        moving_average_ax.plot(
            moving_average_series["date"],
            moving_average_series["sma_200"],
            label="200D MA",
            linewidth=1.8,
            color="#b45309",
        )
        moving_average_ax.set_ylabel("Price")
        moving_average_ax.grid(axis="y", linestyle=":", alpha=0.35)
        moving_average_ax.legend(loc="upper left")
    moving_average_ax.set_title("Price With Moving Averages", loc="left")

    summary_ax.axis("off")
    summary_ax.set_title("Risk Summary Table", loc="left")
    rendered_table = summary_ax.table(
        cellText=data.summary_table.values,
        colLabels=list(data.summary_table.columns),
        cellLoc="left",
        colLoc="left",
        bbox=[0.0, 0.32, 1.0, 0.66],
    )
    rendered_table.auto_set_font_size(False)
    rendered_table.set_fontsize(8.5)
    for (row, col), cell in rendered_table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#e2e8f0")
        elif col == 0:
            cell.set_text_props(weight="bold")
    summary_ax.text(0.0, 0.24, "Short Interpretation", fontsize=10, fontweight="bold", va="top")
    summary_ax.text(0.0, 0.18, data.interpretation_text, fontsize=9.5, va="top", wrap=True)

    fig.tight_layout()
    return fig


def save_trend_risk_profile_section(
    ticker: str,
    *,
    output_dir: str | Path,
    benchmark_set_id: str = "sample_universe",
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Save Trend and Risk Profile review artifacts for one ticker."""

    section_data = build_trend_risk_profile_section_data(
        ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    artifact_stem = f"{section_data.ticker.lower()}_trend_risk_profile"
    figure_path = destination / f"{artifact_stem}.png"
    table_path = destination / f"{artifact_stem}_table.csv"
    strip_path = destination / f"{artifact_stem}_strip.csv"
    text_path = destination / f"{artifact_stem}_summary.md"

    figure = render_trend_risk_profile_section(section_data)
    figure.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(figure)

    section_data.summary_table.to_csv(table_path, index=False)
    section_data.summary_strip.to_csv(strip_path, index=False)
    text_path.write_text(section_data.interpretation_text + "\n")

    return {
        "figure_path": figure_path,
        "table_path": table_path,
        "strip_path": strip_path,
        "text_path": text_path,
    }


__all__ = [
    "TrendRiskProfileSectionData",
    "build_trend_risk_profile_section_data",
    "build_trend_risk_profile_section_data_from_packet",
    "build_trend_risk_profile_strip_table",
    "build_trend_risk_profile_table",
    "build_trend_risk_profile_text_from_data",
    "render_trend_risk_profile_section",
    "save_trend_risk_profile_section",
]
