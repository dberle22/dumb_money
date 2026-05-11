"""DuckDB-aware Peer Positioning section builders and renderers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import textwrap

from dumb_money import _matplotlib  # noqa: F401
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from dumb_money.analytics.company import (
    build_fundamentals_summary,
    build_peer_return_comparison,
    build_peer_return_summary_stats,
    build_peer_summary_stats,
    build_peer_valuation_comparison,
)
from dumb_money.analytics.scorecard import CompanyScorecard
from dumb_money.config import AppSettings, get_settings
from dumb_money.research.company import (
    CompanyResearchPacket,
    load_peer_sets,
    load_sector_snapshots,
    load_security_master,
    load_staged_fundamentals,
    load_staged_prices,
)

RETURN_TABLE_COLUMNS = [
    "Role",
    "Ticker",
    "Company",
    "Peer Source",
    "Relationship",
    "Selection Method",
    "Peer Order",
    "Window",
    "Total Return",
    "Excess vs Company",
    "Return Rank",
]
VALUATION_TABLE_COLUMNS = [
    "Role",
    "Ticker",
    "Company",
    "Peer Source",
    "Relationship",
    "Selection Method",
    "Peer Order",
    "Market Cap",
    "Forward P/E",
    "Forward P/E Rank",
    "EV/EBITDA",
    "EV/EBITDA Rank",
    "Price/Sales",
    "Price/Sales Rank",
    "FCF Yield",
    "FCF Yield Rank",
]
RANKING_PANEL_COLUMNS = [
    "signal",
    "value_display",
    "peer_median_display",
    "rank_display",
    "rank_pct",
    "assessment",
]


@dataclass(slots=True)
class PeerPositioningSectionData:
    """Query-ready Peer Positioning inputs and standardized outputs."""

    ticker: str
    company_name: str | None
    sector: str | None
    industry: str | None
    report_date: str | None
    focus_window: str
    peer_return_comparison: pd.DataFrame
    peer_return_summary_stats: dict[str, object]
    peer_valuation_comparison: pd.DataFrame
    peer_summary_stats: dict[str, object]
    sector_snapshot: dict[str, object]
    return_context_table: pd.DataFrame
    valuation_context_table: pd.DataFrame
    ranking_panel: pd.DataFrame
    interpretation_text: str


def _format_percent(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}%}"


def _format_ratio(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}f}x"


def _format_billions(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value / 1_000_000_000:.{digits}f}B"


def _rank_percentile(rank: int | None, total: int | None) -> float | None:
    if rank is None or total is None or total <= 1:
        return None
    return (total - rank) / (total - 1)


def _tier_from_percentile(percentile: float | None) -> str:
    if percentile is None or pd.isna(percentile):
        return "Unavailable"
    if float(percentile) >= 0.75:
        return "Leader"
    if float(percentile) >= 0.50:
        return "Upper tier"
    if float(percentile) >= 0.25:
        return "Middle tier"
    return "Laggard"


def _rank_display(rank: int | None, total: int | None) -> str:
    if rank is None or total is None:
        return "N/A"
    return f"{rank} / {total}"


def _safe_rank_map(series: pd.Series, *, ascending: bool) -> dict[str, int]:
    numeric = pd.to_numeric(series, errors="coerce")
    available = numeric.dropna()
    if available.empty:
        return {}
    ranked = available.rank(method="min", ascending=ascending)
    return {str(index): int(value) for index, value in ranked.items()}


def _company_name_map(packet: CompanyResearchPacket) -> dict[str, str]:
    mapping: dict[str, str] = {}
    valuation = packet.peer_valuation_comparison.copy()
    if not valuation.empty and "company_name" in valuation.columns:
        for row in valuation.itertuples():
            if pd.notna(row.company_name):
                mapping[str(row.ticker)] = str(row.company_name)
    if packet.company_name:
        mapping[packet.ticker] = packet.company_name
    return mapping


def _build_return_context_table(packet: CompanyResearchPacket) -> tuple[pd.DataFrame, str]:
    peer_returns = packet.peer_return_comparison.copy()
    focus_window = str(packet.peer_return_summary_stats.get("focus_window") or "1y")
    if peer_returns.empty:
        return pd.DataFrame(columns=RETURN_TABLE_COLUMNS), focus_window

    display = peer_returns.loc[peer_returns["window"] == focus_window].copy()
    if display.empty:
        return pd.DataFrame(columns=RETURN_TABLE_COLUMNS), focus_window

    display["ticker"] = display["ticker"].astype(str).str.upper()
    display["total_return_raw"] = display.apply(
        lambda row: row["company_return"] if bool(row["is_focal_company"]) else row["peer_return"],
        axis=1,
    )
    rank_map = _safe_rank_map(display.set_index("ticker")["total_return_raw"], ascending=False)
    total_ranked = len(rank_map) if rank_map else None
    name_map = _company_name_map(packet)

    # Keep the canonical peer lineage fields visible in the section output so review
    # artifacts still make it clear which peer rows are automatic versus curated.
    display["Role"] = display["is_focal_company"].map(lambda value: "Company" if value else "Peer")
    display["Company"] = display["ticker"].map(lambda ticker: name_map.get(str(ticker), str(ticker)))
    display["Peer Order"] = display["peer_order"].fillna(pd.NA)
    display["Total Return"] = display["total_return_raw"].map(_format_percent)
    display["Excess vs Company"] = display["excess_return"].map(_format_percent)
    display["Return Rank"] = display["ticker"].map(
        lambda ticker: _rank_display(rank_map.get(str(ticker)), total_ranked)
    )

    table = display[
        [
            "Role",
            "ticker",
            "Company",
            "peer_source",
            "relationship_type",
            "selection_method",
            "Peer Order",
            "window",
            "Total Return",
            "Excess vs Company",
            "Return Rank",
        ]
    ].rename(
        columns={
            "ticker": "Ticker",
            "peer_source": "Peer Source",
            "relationship_type": "Relationship",
            "selection_method": "Selection Method",
            "window": "Window",
        }
    )
    return table, focus_window


def _build_valuation_context_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    valuation = packet.peer_valuation_comparison.copy()
    if valuation.empty:
        return pd.DataFrame(columns=VALUATION_TABLE_COLUMNS)

    valuation["ticker"] = valuation["ticker"].astype(str).str.upper()
    forward_pe_ranks = _safe_rank_map(valuation.set_index("ticker")["forward_pe"], ascending=True)
    ev_to_ebitda_ranks = _safe_rank_map(valuation.set_index("ticker")["ev_to_ebitda"], ascending=True)
    price_to_sales_ranks = _safe_rank_map(valuation.set_index("ticker")["price_to_sales"], ascending=True)
    fcf_yield_ranks = _safe_rank_map(valuation.set_index("ticker")["free_cash_flow_yield"], ascending=False)

    display = valuation.copy()
    display["Role"] = display["is_focal_company"].map(lambda value: "Company" if value else "Peer")
    display["Company"] = display["company_name"].fillna(display["ticker"])
    display["Peer Order"] = display["peer_order"].fillna(pd.NA)
    display["Market Cap"] = display["market_cap"].map(_format_billions)
    display["Forward P/E"] = display["forward_pe"].map(_format_ratio)
    display["Forward P/E Rank"] = display["ticker"].map(
        lambda ticker: _rank_display(forward_pe_ranks.get(str(ticker)), len(forward_pe_ranks) or None)
    )
    display["EV/EBITDA"] = display["ev_to_ebitda"].map(_format_ratio)
    display["EV/EBITDA Rank"] = display["ticker"].map(
        lambda ticker: _rank_display(ev_to_ebitda_ranks.get(str(ticker)), len(ev_to_ebitda_ranks) or None)
    )
    display["Price/Sales"] = display["price_to_sales"].map(_format_ratio)
    display["Price/Sales Rank"] = display["ticker"].map(
        lambda ticker: _rank_display(price_to_sales_ranks.get(str(ticker)), len(price_to_sales_ranks) or None)
    )
    display["FCF Yield"] = display["free_cash_flow_yield"].map(_format_percent)
    display["FCF Yield Rank"] = display["ticker"].map(
        lambda ticker: _rank_display(fcf_yield_ranks.get(str(ticker)), len(fcf_yield_ranks) or None)
    )

    return display[
        [
            "Role",
            "ticker",
            "Company",
            "peer_source",
            "relationship_type",
            "selection_method",
            "Peer Order",
            "Market Cap",
            "Forward P/E",
            "Forward P/E Rank",
            "EV/EBITDA",
            "EV/EBITDA Rank",
            "Price/Sales",
            "Price/Sales Rank",
            "FCF Yield",
            "FCF Yield Rank",
        ]
    ].rename(
        columns={
            "ticker": "Ticker",
            "peer_source": "Peer Source",
            "relationship_type": "Relationship",
            "selection_method": "Selection Method",
        }
    )


def _build_ranking_panel(
    packet: CompanyResearchPacket,
    *,
    focus_window: str,
) -> pd.DataFrame:
    peer_returns = packet.peer_return_comparison.copy()
    peer_valuation = packet.peer_valuation_comparison.copy()
    if peer_returns.empty and peer_valuation.empty:
        return pd.DataFrame(columns=RANKING_PANEL_COLUMNS)

    rows: list[dict[str, object]] = []

    one_window = peer_returns.loc[peer_returns["window"] == focus_window].copy() if not peer_returns.empty else pd.DataFrame()
    if not one_window.empty:
        one_window["total_return_raw"] = one_window.apply(
            lambda row: row["company_return"] if bool(row["is_focal_company"]) else row["peer_return"],
            axis=1,
        )
        rank_map = _safe_rank_map(one_window.set_index("ticker")["total_return_raw"], ascending=False)
        focal_row = one_window.loc[one_window["is_focal_company"].astype("boolean").fillna(False)].iloc[0]
        focal_rank = rank_map.get(str(focal_row["ticker"]))
        total_ranked = len(rank_map) or None
        rows.append(
            {
                "signal": f"{focus_window.upper()} Return",
                "value_display": _format_percent(focal_row["total_return_raw"]),
                "peer_median_display": _format_percent(packet.peer_return_summary_stats.get("median_peer_return")),
                "rank_display": _rank_display(focal_rank, total_ranked),
                "rank_pct": _rank_percentile(focal_rank, total_ranked),
                "assessment": _tier_from_percentile(_rank_percentile(focal_rank, total_ranked)),
            }
        )

    metric_specs = [
        ("forward_pe", "Forward P/E", True, _format_ratio, packet.peer_summary_stats.get("median_forward_pe")),
        ("ev_to_ebitda", "EV/EBITDA", True, _format_ratio, packet.peer_summary_stats.get("median_ev_to_ebitda")),
        ("free_cash_flow_yield", "FCF Yield", False, _format_percent, packet.peer_summary_stats.get("median_free_cash_flow_yield")),
    ]
    for column, label, ascending, formatter, peer_median in metric_specs:
        if peer_valuation.empty or column not in peer_valuation.columns:
            continue
        rank_map = _safe_rank_map(peer_valuation.set_index("ticker")[column], ascending=ascending)
        focal_rows = peer_valuation.loc[peer_valuation["is_focal_company"].astype("boolean").fillna(False)].copy()
        if focal_rows.empty:
            continue
        focal_row = focal_rows.iloc[0]
        focal_rank = rank_map.get(str(focal_row["ticker"]))
        total_ranked = len(rank_map) or None
        rows.append(
            {
                "signal": label,
                "value_display": formatter(focal_row[column]),
                "peer_median_display": formatter(peer_median),
                "rank_display": _rank_display(focal_rank, total_ranked),
                "rank_pct": _rank_percentile(focal_rank, total_ranked),
                "assessment": _tier_from_percentile(_rank_percentile(focal_rank, total_ranked)),
            }
        )

    return pd.DataFrame(rows, columns=RANKING_PANEL_COLUMNS)


def _build_interpretation_text(data: PeerPositioningSectionData) -> str:
    company_label = data.company_name or data.ticker
    peer_count = int(data.peer_return_summary_stats.get("peer_count") or data.peer_summary_stats.get("peer_count") or 0)
    if peer_count <= 0:
        return (
            f"{company_label} does not yet have enough canonical peer coverage to place it cleanly in the current "
            "peer-positioning section."
        )

    ranking = data.ranking_panel.set_index("signal") if not data.ranking_panel.empty else pd.DataFrame()
    return_row = ranking.loc["1Y Return"] if "1Y Return" in ranking.index else (
        ranking.loc["12M Return"] if "12M Return" in ranking.index else None
    )
    if return_row is None and "1Y RETURN" in ranking.index:
        return_row = ranking.loc["1Y RETURN"]
    forward_pe_row = ranking.loc["Forward P/E"] if "Forward P/E" in ranking.index else None
    fcf_yield_row = ranking.loc["FCF Yield"] if "FCF Yield" in ranking.index else None

    closest_peer = None
    valuation_peers = data.valuation_context_table.copy()
    if not valuation_peers.empty:
        peer_rows = valuation_peers.loc[valuation_peers["Role"] == "Peer"].copy()
        if not peer_rows.empty:
            peer_rows["Peer Order"] = pd.to_numeric(peer_rows["Peer Order"], errors="coerce")
            closest_peer = peer_rows.sort_values(["Peer Order", "Ticker"]).iloc[0]

    clauses = [
        f"{company_label} sits in a {return_row['assessment'].lower() if return_row is not None else 'mixed'} position across {peer_count} canonical peers."
    ]
    if return_row is not None:
        clauses.append(
            f"Its trailing {data.focus_window.upper()} return ranks {str(return_row['rank_display']).lower()} "
            f"at {return_row['value_display']} versus a peer median of {return_row['peer_median_display']}."
        )
    if forward_pe_row is not None:
        clauses.append(
            f"On valuation, forward P/E ranks {str(forward_pe_row['rank_display']).lower()} at "
            f"{forward_pe_row['value_display']} versus a peer median of {forward_pe_row['peer_median_display']}."
        )
    if fcf_yield_row is not None and fcf_yield_row["value_display"] != "N/A":
        clauses.append(
            f"Free-cash-flow yield stands at {fcf_yield_row['value_display']} compared with a peer median of "
            f"{fcf_yield_row['peer_median_display']}."
        )
    if closest_peer is not None:
        clauses.append(
            f"The closest canonical comparison is {closest_peer['Company']} "
            f"({closest_peer['Peer Source']}, {closest_peer['Relationship']})."
        )
    sector_return = data.sector_snapshot.get("median_return_1y")
    if sector_return is not None and not pd.isna(sector_return):
        clauses.append(f"Sector median 1Y return is {_format_percent(float(sector_return))}.")
    return " ".join(clauses)


def build_peer_positioning_return_table(data: PeerPositioningSectionData) -> pd.DataFrame:
    """Build the standardized peer return context table for this section."""

    return data.return_context_table.copy()


def build_peer_positioning_valuation_table(data: PeerPositioningSectionData) -> pd.DataFrame:
    """Build the standardized peer valuation context table for this section."""

    return data.valuation_context_table.copy()


def build_peer_positioning_ranking_panel(data: PeerPositioningSectionData) -> pd.DataFrame:
    """Build the compact ranking panel for this section."""

    return data.ranking_panel.copy()


def build_peer_positioning_text_from_data(data: PeerPositioningSectionData) -> str:
    """Build the reusable short interpretation for this section."""

    return data.interpretation_text


def build_peer_positioning_section_data_from_packet(
    packet: CompanyResearchPacket,
) -> PeerPositioningSectionData:
    """Build standardized Peer Positioning outputs from the shared research packet."""

    return_context_table, focus_window = _build_return_context_table(packet)
    valuation_context_table = _build_valuation_context_table(packet)
    ranking_panel = _build_ranking_panel(packet, focus_window=focus_window)

    score_summary = packet.scorecard.summary
    data = PeerPositioningSectionData(
        ticker=packet.ticker,
        company_name=packet.company_name,
        sector=packet.fundamentals_summary.get("sector"),
        industry=packet.fundamentals_summary.get("industry"),
        report_date=score_summary.get("score_date") or packet.as_of_date,
        focus_window=focus_window,
        peer_return_comparison=packet.peer_return_comparison.copy(),
        peer_return_summary_stats=packet.peer_return_summary_stats.copy(),
        peer_valuation_comparison=packet.peer_valuation_comparison.copy(),
        peer_summary_stats=packet.peer_summary_stats.copy(),
        sector_snapshot=packet.sector_snapshot.copy() if packet.sector_snapshot else {},
        return_context_table=return_context_table,
        valuation_context_table=valuation_context_table,
        ranking_panel=ranking_panel,
        interpretation_text="",
    )
    data.interpretation_text = _build_interpretation_text(data)
    return data


def build_peer_positioning_section_data(
    ticker: str,
    *,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> PeerPositioningSectionData:
    """Build the Peer Positioning section from canonical shared inputs."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    prices = load_staged_prices(settings=settings)
    fundamentals = load_staged_fundamentals(settings=settings)
    security_master = load_security_master(settings=settings)
    peer_sets = load_peer_sets(settings=settings)
    sector_snapshots = load_sector_snapshots(settings=settings)

    fundamentals_summary = build_fundamentals_summary(fundamentals, normalized_ticker)
    security_rows = (
        security_master.loc[security_master["ticker"] == normalized_ticker].copy()
        if "ticker" in security_master.columns
        else pd.DataFrame()
    )
    security_row = security_rows.iloc[-1].to_dict() if not security_rows.empty else {}
    peer_rows = (
        peer_sets.loc[peer_sets["ticker"] == normalized_ticker].copy()
        if "ticker" in peer_sets.columns
        else pd.DataFrame()
    )

    peer_return_comparison = build_peer_return_comparison(
        normalized_ticker,
        peer_rows,
        prices,
    )
    peer_return_summary_stats = build_peer_return_summary_stats(peer_return_comparison)
    peer_valuation_comparison = build_peer_valuation_comparison(
        normalized_ticker,
        peer_rows,
        fundamentals,
    )
    peer_summary_stats = build_peer_summary_stats(peer_valuation_comparison)

    company_sector = fundamentals_summary.get("sector") or security_row.get("sector")
    sector_snapshot_rows = (
        sector_snapshots.loc[sector_snapshots["sector"] == company_sector].copy()
        if company_sector and "sector" in sector_snapshots.columns
        else pd.DataFrame()
    )
    sector_snapshot = sector_snapshot_rows.iloc[-1].to_dict() if not sector_snapshot_rows.empty else {}

    packet = CompanyResearchPacket(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        as_of_date=fundamentals_summary.get("as_of_date"),
        company_history=pd.DataFrame(),
        benchmark_histories={},
        return_windows=pd.DataFrame(),
        trailing_return_comparison=pd.DataFrame(),
        risk_metrics={},
        trend_metrics={},
        benchmark_comparison=pd.DataFrame(),
        fundamentals_summary=fundamentals_summary,
        peer_return_comparison=peer_return_comparison,
        peer_return_summary_stats=peer_return_summary_stats,
        peer_valuation_comparison=peer_valuation_comparison,
        peer_summary_stats=peer_summary_stats,
        sector_snapshot=sector_snapshot,
        scorecard=CompanyScorecard(
            summary={
                "ticker": normalized_ticker,
                "company_name": fundamentals_summary.get("long_name"),
                "sector": fundamentals_summary.get("sector") or security_row.get("sector"),
                "industry": fundamentals_summary.get("industry") or security_row.get("industry"),
                "score_date": fundamentals_summary.get("as_of_date"),
            },
            category_scores=pd.DataFrame(),
            metrics=pd.DataFrame(),
        ),
    )
    return build_peer_positioning_section_data_from_packet(packet)


def render_peer_positioning_section(data: PeerPositioningSectionData) -> Figure:
    """Render the full Peer Positioning section as one reviewable figure."""

    fig, axes = plt.subplots(
        nrows=5,
        ncols=1,
        figsize=(13.5, 18.0),
        gridspec_kw={"height_ratios": [0.9, 1.4, 2.3, 3.0, 0.9]},
    )
    header_ax, ranking_ax, returns_ax, valuation_ax, text_ax = axes

    header_ax.axis("off")
    header_ax.set_title(f"{data.ticker} Peer Positioning", loc="left", fontsize=14, fontweight="bold")
    header_rows = pd.DataFrame(
        [
            ("Company", data.company_name or data.ticker),
            ("Sector", data.sector or "N/A"),
            ("Industry", data.industry or "N/A"),
            ("Report Date", data.report_date or "N/A"),
            ("Peer Window", data.focus_window.upper()),
            ("Peer Count", str(int(data.peer_return_summary_stats.get("peer_count") or data.peer_summary_stats.get("peer_count") or 0))),
            ("Sector Benchmark", data.sector_snapshot.get("sector_benchmark") or "N/A"),
        ],
        columns=["Field", "Value"],
    )
    header_table = header_ax.table(
        cellText=header_rows.values,
        colLabels=header_rows.columns,
        cellLoc="left",
        colLoc="left",
        bbox=[0.0, 0.0, 1.0, 0.9],
    )
    header_table.auto_set_font_size(False)
    header_table.set_fontsize(9)
    for (row, col), cell in header_table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#e2e8f0")
        elif col == 0:
            cell.set_text_props(weight="bold")

    if data.ranking_panel.empty:
        ranking_ax.text(0.5, 0.5, "No peer ranking panel available", ha="center", va="center")
        ranking_ax.axis("off")
    else:
        panel = data.ranking_panel.copy()
        y_positions = list(range(len(panel)))
        ranking_ax.barh(y_positions, [1.0] * len(panel), color="#e2e8f0", height=0.55)
        ranking_ax.barh(y_positions, panel["rank_pct"].fillna(0.0), color="#0f766e", height=0.55)
        ranking_ax.set_yticks(y_positions, panel["signal"])
        ranking_ax.invert_yaxis()
        ranking_ax.set_xlim(0, 1.0)
        ranking_ax.set_xlabel("Relative Rank Percentile")
        ranking_ax.set_title("Peer Ranking Panel", loc="left")
        ranking_ax.grid(axis="x", linestyle=":", alpha=0.35)
        for y_position, rank_display, value_display, peer_median_display in zip(
            y_positions,
            panel["rank_display"],
            panel["value_display"],
            panel["peer_median_display"],
            strict=False,
        ):
            ranking_ax.text(
                min(float(panel.loc[y_position, "rank_pct"] or 0.0) + 0.03, 0.98),
                y_position,
                f"{rank_display} | {value_display} | peer {peer_median_display}",
                va="center",
                ha="left",
                fontsize=8.5,
            )

    returns_ax.axis("off")
    returns_ax.set_title("Peer Return Context", loc="left")
    if data.return_context_table.empty:
        returns_ax.text(0.5, 0.5, "No peer return context available", ha="center", va="center")
    else:
        display_table = data.return_context_table.copy()
        for column, width in {"Company": 18, "Selection Method": 18}.items():
            if column in display_table.columns:
                display_table[column] = display_table[column].map(
                    lambda value: textwrap.fill(str(value), width=width) if pd.notna(value) and str(value) else ""
                )
        rendered_table = returns_ax.table(
            cellText=display_table.values,
            colLabels=display_table.columns,
            cellLoc="left",
            colLoc="left",
            bbox=[0.0, 0.0, 1.0, 0.97],
        )
        rendered_table.auto_set_font_size(False)
        rendered_table.set_fontsize(7.5)
        for (row, col), cell in rendered_table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight="bold")
                cell.set_facecolor("#e2e8f0")
            elif col == 0:
                cell.set_text_props(weight="bold")
            cell.get_text().set_wrap(True)

    valuation_ax.axis("off")
    valuation_ax.set_title("Peer Valuation Context", loc="left")
    if data.valuation_context_table.empty:
        valuation_ax.text(0.5, 0.5, "No peer valuation context available", ha="center", va="center")
    else:
        display_table = data.valuation_context_table.copy()
        for column, width in {"Company": 18, "Selection Method": 18}.items():
            if column in display_table.columns:
                display_table[column] = display_table[column].map(
                    lambda value: textwrap.fill(str(value), width=width) if pd.notna(value) and str(value) else ""
                )
        rendered_table = valuation_ax.table(
            cellText=display_table.values,
            colLabels=display_table.columns,
            cellLoc="left",
            colLoc="left",
            bbox=[0.0, 0.0, 1.0, 0.98],
        )
        rendered_table.auto_set_font_size(False)
        rendered_table.set_fontsize(6.6)
        for (row, col), cell in rendered_table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight="bold")
                cell.set_facecolor("#e2e8f0")
            elif col == 0:
                cell.set_text_props(weight="bold")
            cell.get_text().set_wrap(True)

    text_ax.axis("off")
    text_ax.set_title("Short Interpretation", loc="left")
    text_ax.text(0.0, 0.95, data.interpretation_text, va="top", ha="left", wrap=True, fontsize=10)

    fig.tight_layout()
    return fig


def save_peer_positioning_section(
    ticker: str,
    *,
    output_dir: str | Path,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Save Peer Positioning review artifacts for one ticker."""

    section_data = build_peer_positioning_section_data(
        ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    artifact_stem = f"{section_data.ticker.lower()}_peer_positioning"
    figure_path = destination / f"{artifact_stem}.png"
    return_table_path = destination / f"{artifact_stem}_returns.csv"
    valuation_table_path = destination / f"{artifact_stem}_valuation.csv"
    ranking_path = destination / f"{artifact_stem}_ranking.csv"
    text_path = destination / f"{artifact_stem}_summary.md"

    figure = render_peer_positioning_section(section_data)
    figure.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(figure)

    section_data.return_context_table.to_csv(return_table_path, index=False)
    section_data.valuation_context_table.to_csv(valuation_table_path, index=False)
    section_data.ranking_panel.to_csv(ranking_path, index=False)
    text_path.write_text(section_data.interpretation_text + "\n")

    return {
        "figure_path": figure_path,
        "return_table_path": return_table_path,
        "valuation_table_path": valuation_table_path,
        "ranking_path": ranking_path,
        "text_path": text_path,
    }


__all__ = [
    "PeerPositioningSectionData",
    "build_peer_positioning_ranking_panel",
    "build_peer_positioning_return_table",
    "build_peer_positioning_section_data",
    "build_peer_positioning_section_data_from_packet",
    "build_peer_positioning_text_from_data",
    "build_peer_positioning_valuation_table",
    "render_peer_positioning_section",
    "save_peer_positioning_section",
]
