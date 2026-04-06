"""DuckDB-aware Final Research Summary section builders and renderers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import textwrap

from dumb_money import _matplotlib  # noqa: F401
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from dumb_money.analytics.company import (
    build_benchmark_comparison,
    build_fundamentals_summary,
    build_peer_return_comparison,
    build_peer_return_summary_stats,
    build_peer_summary_stats,
    build_peer_valuation_comparison,
    build_trailing_return_comparison,
    calculate_return_windows,
    calculate_risk_metrics,
    calculate_trend_metrics,
    prepare_price_history,
)
from dumb_money.analytics.scorecard import build_company_scorecard
from dumb_money.config import AppSettings, get_settings
from dumb_money.outputs.peer_positioning_section import (
    PeerPositioningSectionData,
    build_peer_positioning_section_data_from_packet,
)
from dumb_money.outputs.research_summary_section import (
    ResearchSummarySectionData,
    build_research_summary_section_data_from_packet,
)
from dumb_money.outputs.trend_risk_profile_section import (
    TrendRiskProfileSectionData,
    build_trend_risk_profile_section_data_from_packet,
)
from dumb_money.outputs.valuation_section import (
    ValuationSectionData,
    build_valuation_section_data_from_packet,
)
from dumb_money.research.company import (
    CompanyResearchPacket,
    load_benchmark_mappings,
    load_benchmark_prices,
    load_benchmark_set,
    load_peer_sets,
    load_sector_snapshots,
    load_security_master,
    load_staged_fundamentals,
    load_staged_prices,
)

SUMMARY_COLUMNS = ["Component", "Summary", "Shared Inputs"]


@dataclass(slots=True)
class FinalResearchSummarySectionData:
    """Query-ready Final Research Summary inputs and standardized outputs."""

    ticker: str
    company_name: str | None
    sector: str | None
    industry: str | None
    report_date: str | None
    total_score: float | None
    confidence_score: float | None
    interpretation_label: str
    strongest_pillar: str
    weakest_pillar: str
    research_summary: ResearchSummarySectionData
    trend_risk_profile: TrendRiskProfileSectionData
    valuation: ValuationSectionData
    peer_positioning: PeerPositioningSectionData
    what_is_working: list[str]
    what_is_not_working: list[str]
    what_to_watch: list[str]
    bottom_line: str
    closing_summary_table: pd.DataFrame
    final_memo_text: str


def _format_percent(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}%}"


def _format_score(score: float | None, target: float) -> str:
    if score is None or pd.isna(score):
        return f"N/A / {target:.0f}"
    return f"{float(score):.1f} / {target:.0f}"


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _collapse_points(items: list[str]) -> str:
    if not items:
        return "N/A"
    return " | ".join(items)


def _find_summary_value(table: pd.DataFrame, metric: str, column: str) -> str | None:
    if table.empty or metric not in table.get("Metric", pd.Series(dtype=str)).tolist():
        return None
    rows = table.loc[table["Metric"] == metric, column]
    if rows.empty:
        return None
    value = rows.iloc[0]
    if value is None or pd.isna(value):
        return None
    return str(value)


def _score_ratio(score: float | None, target: float) -> float | None:
    if score is None or pd.isna(score) or target <= 0:
        return None
    return float(score) / float(target)


def _strongest_valuation_signal(data: ValuationSectionData) -> pd.Series | None:
    if data.summary_table.empty:
        return None
    supportive = data.summary_table.loc[data.summary_table["Assessment"].isin(["Clear support", "Supportive"])].copy()
    if supportive.empty:
        return None
    return supportive.iloc[0]


def _weakest_valuation_signal(data: ValuationSectionData) -> pd.Series | None:
    if data.summary_table.empty:
        return None
    constrained = data.summary_table.loc[data.summary_table["Assessment"].isin(["Weak", "Constraint", "Mixed"])].copy()
    if constrained.empty:
        return None
    priority = pd.Categorical(
        constrained["Assessment"],
        categories=["Weak", "Constraint", "Mixed"],
        ordered=True,
    )
    return constrained.assign(priority=priority).sort_values("priority").iloc[0]


def _supportive_peer_signal(data: PeerPositioningSectionData) -> pd.Series | None:
    if data.ranking_panel.empty:
        return None
    supportive = data.ranking_panel.loc[data.ranking_panel["assessment"].isin(["Leader", "Upper tier"])].copy()
    if supportive.empty:
        return None
    return supportive.iloc[0]


def _constrained_peer_signal(data: PeerPositioningSectionData) -> pd.Series | None:
    if data.ranking_panel.empty:
        return None
    constrained = data.ranking_panel.loc[data.ranking_panel["assessment"].isin(["Middle tier", "Laggard"])].copy()
    if constrained.empty:
        return None
    priority = pd.Categorical(
        constrained["assessment"],
        categories=["Laggard", "Middle tier"],
        ordered=True,
    )
    return constrained.assign(priority=priority).sort_values("priority").iloc[0]


def _build_working_points(
    research_summary: ResearchSummarySectionData,
    trend_risk: TrendRiskProfileSectionData,
    valuation: ValuationSectionData,
    peer_positioning: PeerPositioningSectionData,
) -> list[str]:
    points = [
        f"{research_summary.strongest_pillar} is the clearest support at "
        f"{research_summary.category_scores.loc[research_summary.category_scores['category'] == research_summary.strongest_pillar, 'score_display'].iloc[0]}."
    ]
    if research_summary.strengths:
        points.append(f"Top supporting evidence: {research_summary.strengths[0]}.")

    trend_assessment = _find_summary_value(trend_risk.summary_table, "Trend Structure", "Assessment")
    price_vs_200d = _find_summary_value(trend_risk.summary_table, "Price vs 200D MA", "Value")
    current_drawdown = _find_summary_value(trend_risk.summary_table, "Current Drawdown", "Value")
    if trend_assessment in {"Trend intact", "Mostly constructive"}:
        points.append(
            f"Trend and risk remain workable: {trend_assessment.lower()}, with price at {price_vs_200d or 'N/A'} "
            f"versus the 200-day average and current drawdown at {current_drawdown or 'N/A'}."
        )

    valuation_signal = _strongest_valuation_signal(valuation)
    if valuation_signal is not None:
        peer_context = str(valuation_signal.get("Relative vs Peer") or "").strip()
        points.append(
            f"Valuation has at least one supportive input in {str(valuation_signal['Metric']).lower()} at "
            f"{valuation_signal['Value']}"
            f"{'' if not peer_context or peer_context == 'N/A' else f' ({peer_context})'}."
        )

    peer_signal = _supportive_peer_signal(peer_positioning)
    if peer_signal is not None:
        points.append(
            f"Peer context is constructive on {str(peer_signal['signal']).lower()}: "
            f"{peer_signal['rank_display']} with {str(peer_signal['assessment']).lower()} standing."
        )

    return _dedupe_keep_order(points)[:3]


def _build_not_working_points(
    research_summary: ResearchSummarySectionData,
    trend_risk: TrendRiskProfileSectionData,
    valuation: ValuationSectionData,
    peer_positioning: PeerPositioningSectionData,
) -> list[str]:
    points = [
        f"{research_summary.weakest_pillar} is the main constraint at "
        f"{research_summary.category_scores.loc[research_summary.category_scores['category'] == research_summary.weakest_pillar, 'score_display'].iloc[0]}."
    ]
    if research_summary.constraints:
        points.append(f"Most visible drag in current score coverage: {research_summary.constraints[0]}.")

    trend_assessment = _find_summary_value(trend_risk.summary_table, "Trend Structure", "Assessment")
    current_drawdown = _find_summary_value(trend_risk.summary_table, "Current Drawdown", "Value")
    max_drawdown = _find_summary_value(trend_risk.summary_table, "Max Drawdown (1Y)", "Value")
    if trend_assessment in {"Mixed", "Trend under pressure"}:
        points.append(
            f"Risk backdrop is not fully settled: trend structure is {trend_assessment.lower()}, "
            f"with current drawdown at {current_drawdown or 'N/A'} and one-year max drawdown at {max_drawdown or 'N/A'}."
        )

    valuation_signal = _weakest_valuation_signal(valuation)
    if valuation_signal is not None:
        peer_context = str(valuation_signal.get("Relative vs Peer") or "").strip()
        points.append(
            f"Valuation still needs work in {str(valuation_signal['Metric']).lower()} at {valuation_signal['Value']}"
            f"{'' if not peer_context or peer_context == 'N/A' else f' ({peer_context})'}."
        )

    peer_signal = _constrained_peer_signal(peer_positioning)
    if peer_signal is not None:
        points.append(
            f"Peer positioning is not a clear tailwind on {str(peer_signal['signal']).lower()}: "
            f"{peer_signal['rank_display']} with {str(peer_signal['assessment']).lower()} standing."
        )

    return _dedupe_keep_order(points)[:3]


def _build_watch_points(
    research_summary: ResearchSummarySectionData,
    trend_risk: TrendRiskProfileSectionData,
    valuation: ValuationSectionData,
    peer_positioning: PeerPositioningSectionData,
) -> list[str]:
    points = [
        f"Watch whether {research_summary.weakest_pillar.lower()} improves from the current "
        f"{research_summary.category_scores.loc[research_summary.category_scores['category'] == research_summary.weakest_pillar, 'score_display'].iloc[0]} footing."
    ]

    valuation_signal = _weakest_valuation_signal(valuation)
    if valuation_signal is not None:
        points.append(
            f"Watch {str(valuation_signal['Metric']).lower()} for relief from the current {valuation_signal['Value']} level."
        )

    if peer_positioning.ranking_panel.empty:
        points.append(
            "Watch for richer canonical peer or catalyst fields as the closing memo contract expands beyond score-driven evidence."
        )

    trend_assessment = _find_summary_value(trend_risk.summary_table, "Trend Structure", "Assessment")
    current_drawdown = _find_summary_value(trend_risk.summary_table, "Current Drawdown", "Value")
    if trend_assessment is not None:
        points.append(
            f"Watch trend stability and drawdown control; the profile is currently {trend_assessment.lower()} "
            f"with current drawdown at {current_drawdown or 'N/A'}."
        )

    if not peer_positioning.ranking_panel.empty:
        peer_signal = _constrained_peer_signal(peer_positioning)
        if peer_signal is not None:
            points.append(
                f"Watch whether peer standing improves on {str(peer_signal['signal']).lower()} from the current "
                f"{peer_signal['rank_display']} rank."
            )

    return _dedupe_keep_order(points)[:3]


def _build_bottom_line(
    *,
    company_label: str,
    total_score: float | None,
    interpretation_label: str,
    strongest_pillar: str,
    weakest_pillar: str,
    working_points: list[str],
    watch_points: list[str],
) -> str:
    score_text = "N/A" if total_score is None or pd.isna(total_score) else f"{float(total_score):.1f}/100"
    first_watch = watch_points[0] if watch_points else "Watch-item coverage is still limited."
    first_working = working_points[0] if working_points else f"{strongest_pillar} is the main support."
    return (
        f"{company_label} closes with a {score_text} score and a {interpretation_label.lower()}. "
        f"The setup is still anchored by {strongest_pillar.lower()}, while {weakest_pillar.lower()} is the main reason to stay selective. "
        f"{first_working} {first_watch}"
    ).strip()


def build_final_research_summary_text_from_data(data: FinalResearchSummarySectionData) -> str:
    """Build the reusable short final memo text for this section."""

    company_label = data.company_name or data.ticker
    return (
        f"{data.bottom_line} "
        f"What is working: {_collapse_points(data.what_is_working)} "
        f"What is not working: {_collapse_points(data.what_is_not_working)} "
        f"What to watch: {_collapse_points(data.what_to_watch)}"
    ).strip()


def build_final_research_summary_table(data: FinalResearchSummarySectionData) -> pd.DataFrame:
    """Build the standardized closing summary table for this section."""

    return data.closing_summary_table.copy()


def build_final_research_summary_section_data_from_packet(
    packet: CompanyResearchPacket,
) -> FinalResearchSummarySectionData:
    """Build standardized Final Research Summary outputs from the shared research packet."""

    research_summary = build_research_summary_section_data_from_packet(packet)
    trend_risk_profile = build_trend_risk_profile_section_data_from_packet(packet)
    valuation = build_valuation_section_data_from_packet(packet)
    peer_positioning = build_peer_positioning_section_data_from_packet(packet)

    # Reuse upstream section contracts first so the closing memo synthesizes stable
    # evidence instead of restating notebook-only prose.
    working_points = _build_working_points(
        research_summary,
        trend_risk_profile,
        valuation,
        peer_positioning,
    )
    not_working_points = _build_not_working_points(
        research_summary,
        trend_risk_profile,
        valuation,
        peer_positioning,
    )
    watch_points = _build_watch_points(
        research_summary,
        trend_risk_profile,
        valuation,
        peer_positioning,
    )

    company_label = packet.company_name or packet.ticker
    bottom_line = _build_bottom_line(
        company_label=company_label,
        total_score=research_summary.total_score,
        interpretation_label=research_summary.interpretation_label,
        strongest_pillar=research_summary.strongest_pillar,
        weakest_pillar=research_summary.weakest_pillar,
        working_points=working_points,
        watch_points=watch_points,
    )

    closing_summary_table = pd.DataFrame(
        [
            {
                "Component": "What is working",
                "Summary": _collapse_points(working_points),
                "Shared Inputs": "research_summary.category_scores + research_summary.strengths + trend_risk_profile.summary_table + valuation.summary_table + peer_positioning.ranking_panel",
            },
            {
                "Component": "What is not working",
                "Summary": _collapse_points(not_working_points),
                "Shared Inputs": "research_summary.category_scores + research_summary.constraints + trend_risk_profile.summary_table + valuation.summary_table + peer_positioning.ranking_panel",
            },
            {
                "Component": "Bottom line",
                "Summary": bottom_line,
                "Shared Inputs": "scorecard.summary.total_score + research_summary.interpretation_label + strongest/weakest pillar context",
            },
            {
                "Component": "What to watch",
                "Summary": _collapse_points(watch_points),
                "Shared Inputs": "valuation.summary_table + trend_risk_profile.summary_table + peer_positioning.ranking_panel",
            },
        ],
        columns=SUMMARY_COLUMNS,
    )

    data = FinalResearchSummarySectionData(
        ticker=packet.ticker,
        company_name=packet.company_name,
        sector=packet.fundamentals_summary.get("sector"),
        industry=packet.fundamentals_summary.get("industry"),
        report_date=research_summary.report_date or packet.as_of_date,
        total_score=research_summary.total_score,
        confidence_score=research_summary.confidence_score,
        interpretation_label=research_summary.interpretation_label,
        strongest_pillar=research_summary.strongest_pillar,
        weakest_pillar=research_summary.weakest_pillar,
        research_summary=research_summary,
        trend_risk_profile=trend_risk_profile,
        valuation=valuation,
        peer_positioning=peer_positioning,
        what_is_working=working_points,
        what_is_not_working=not_working_points,
        what_to_watch=watch_points,
        bottom_line=bottom_line,
        closing_summary_table=closing_summary_table,
        final_memo_text="",
    )
    data.final_memo_text = build_final_research_summary_text_from_data(data)
    return data


def build_final_research_summary_section_data(
    ticker: str,
    *,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> FinalResearchSummarySectionData:
    """Build the Final Research Summary section from canonical shared inputs."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    prices = load_staged_prices(settings=settings)
    fundamentals = load_staged_fundamentals(settings=settings)
    security_master = load_security_master(settings=settings)
    benchmark_mappings = load_benchmark_mappings(settings=settings)
    benchmark_set = load_benchmark_set(settings=settings, set_id=benchmark_set_id)
    benchmark_prices = load_benchmark_prices(settings=settings)
    peer_sets = load_peer_sets(settings=settings)
    sector_snapshots = load_sector_snapshots(settings=settings)

    security_rows = (
        security_master.loc[security_master["ticker"] == normalized_ticker].copy()
        if "ticker" in security_master.columns
        else pd.DataFrame()
    )
    benchmark_mapping_rows = (
        benchmark_mappings.loc[benchmark_mappings["ticker"] == normalized_ticker].copy()
        if "ticker" in benchmark_mappings.columns
        else pd.DataFrame()
    )
    peer_rows = (
        peer_sets.loc[peer_sets["ticker"] == normalized_ticker].copy()
        if "ticker" in peer_sets.columns
        else pd.DataFrame()
    )
    security_row = security_rows.iloc[-1].to_dict() if not security_rows.empty else {}
    benchmark_mapping_row = benchmark_mapping_rows.iloc[-1].to_dict() if not benchmark_mapping_rows.empty else {}

    benchmark_tickers = benchmark_set.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist()
    candidate_tickers = {
        normalized_ticker,
        *benchmark_tickers,
        *peer_rows.get("peer_ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
    }

    # This section only needs the focal ticker, its canonical peers, and its mapped
    # benchmarks, so prefilter the shared tables before building upstream evidence.
    filtered_prices = prices.loc[prices["ticker"].astype(str).str.upper().isin(candidate_tickers)].copy()
    filtered_benchmark_prices = benchmark_prices.loc[
        benchmark_prices["ticker"].astype(str).str.upper().isin(set(benchmark_tickers))
    ].copy()
    filtered_fundamentals = fundamentals.loc[
        fundamentals["ticker"].astype(str).str.upper().isin(candidate_tickers)
    ].copy()

    company_history = prepare_price_history(filtered_prices, normalized_ticker)
    if company_history.empty:
        raise ValueError(f"no staged price history found for ticker {normalized_ticker}")

    fundamentals_summary = build_fundamentals_summary(filtered_fundamentals, normalized_ticker)
    benchmark_histories = {
        benchmark_ticker: prepare_price_history(filtered_benchmark_prices, benchmark_ticker)
        for benchmark_ticker in benchmark_tickers
    }
    benchmark_histories = {
        benchmark_ticker: history
        for benchmark_ticker, history in benchmark_histories.items()
        if not history.empty
    }

    resolved_primary_benchmark = benchmark_mapping_row.get("primary_benchmark")
    if not resolved_primary_benchmark and benchmark_histories:
        resolved_primary_benchmark = next(iter(benchmark_histories))
    primary_benchmark_history = benchmark_histories.get(resolved_primary_benchmark) if resolved_primary_benchmark else None

    return_windows = calculate_return_windows(company_history)
    risk_metrics = calculate_risk_metrics(company_history, benchmark_history=primary_benchmark_history)
    trend_metrics = calculate_trend_metrics(company_history)
    benchmark_comparison = build_benchmark_comparison(company_history, benchmark_histories)
    trailing_return_comparison = build_trailing_return_comparison(company_history, benchmark_histories)
    end_dates = return_windows["end_date"].dropna() if "end_date" in return_windows.columns else pd.Series(dtype=object)
    score_date = (
        str(pd.to_datetime(end_dates.iloc[-1]).date())
        if not return_windows.empty and not end_dates.empty
        else fundamentals_summary.get("as_of_date")
    )

    peer_valuation_comparison = build_peer_valuation_comparison(
        normalized_ticker,
        peer_rows,
        filtered_fundamentals,
    )
    peer_return_comparison = build_peer_return_comparison(
        normalized_ticker,
        peer_rows,
        filtered_prices,
    )
    peer_return_summary_stats = build_peer_return_summary_stats(peer_return_comparison)
    peer_summary_stats = build_peer_summary_stats(peer_valuation_comparison)
    scorecard = build_company_scorecard(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        sector=fundamentals_summary.get("sector") or security_row.get("sector"),
        industry=fundamentals_summary.get("industry") or security_row.get("industry"),
        score_date=score_date,
        benchmark_comparison=benchmark_comparison,
        risk_metrics=risk_metrics,
        trend_metrics=trend_metrics,
        fundamentals_summary=fundamentals_summary,
        peer_valuation_comparison=peer_valuation_comparison,
        primary_benchmark=resolved_primary_benchmark,
        secondary_benchmark=(
            benchmark_mapping_row.get("sector_benchmark")
            or benchmark_mapping_row.get("style_benchmark")
            or benchmark_mapping_row.get("industry_benchmark")
            or benchmark_mapping_row.get("custom_benchmark")
        ),
    )
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
        company_history=company_history,
        benchmark_histories=benchmark_histories,
        return_windows=return_windows,
        trailing_return_comparison=trailing_return_comparison,
        risk_metrics=risk_metrics,
        trend_metrics=trend_metrics,
        benchmark_comparison=benchmark_comparison,
        fundamentals_summary=fundamentals_summary,
        peer_return_comparison=peer_return_comparison,
        peer_return_summary_stats=peer_return_summary_stats,
        peer_valuation_comparison=peer_valuation_comparison,
        peer_summary_stats=peer_summary_stats,
        sector_snapshot=sector_snapshot,
        scorecard=scorecard,
    )
    return build_final_research_summary_section_data_from_packet(packet)


def render_final_research_summary_section(data: FinalResearchSummarySectionData) -> Figure:
    """Render the full Final Research Summary section as one reviewable figure."""

    fig, axes = plt.subplots(
        nrows=3,
        ncols=1,
        figsize=(12.5, 10.5),
        gridspec_kw={"height_ratios": [1.0, 1.8, 1.0]},
    )
    header_ax, summary_ax, memo_ax = axes

    header_ax.axis("off")
    header_ax.set_title(f"{data.ticker} Final Research Summary", loc="left", fontsize=14, fontweight="bold")
    header_rows = pd.DataFrame(
        [
            ("Company", data.company_name or data.ticker),
            ("Sector", data.sector or "N/A"),
            ("Industry", data.industry or "N/A"),
            ("Report Date", data.report_date or "N/A"),
            ("Total Score", _format_score(data.total_score, 100.0)),
            ("Confidence", _format_percent(data.confidence_score)),
            ("Interpretation", data.interpretation_label),
            ("Strongest Pillar", data.strongest_pillar),
            ("Main Constraint", data.weakest_pillar),
        ],
        columns=["Field", "Value"],
    )
    header_table = header_ax.table(
        cellText=header_rows.values,
        colLabels=header_rows.columns,
        cellLoc="left",
        colLoc="left",
        bbox=[0.0, 0.0, 1.0, 0.94],
    )
    header_table.auto_set_font_size(False)
    header_table.set_fontsize(9)
    for (row, col), cell in header_table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#e2e8f0")
        elif col == 0:
            cell.set_text_props(weight="bold")

    summary_ax.axis("off")
    summary_ax.set_title("Closing Summary Card", loc="left")
    display_table = data.closing_summary_table.copy()
    for column, width in {"Summary": 72, "Shared Inputs": 52}.items():
        display_table[column] = display_table[column].map(
            lambda value: textwrap.fill(str(value), width=width) if pd.notna(value) and str(value) else ""
        )
    rendered_table = summary_ax.table(
        cellText=display_table.values,
        colLabels=display_table.columns,
        cellLoc="left",
        colLoc="left",
        bbox=[0.0, 0.0, 1.0, 0.98],
    )
    rendered_table.auto_set_font_size(False)
    rendered_table.set_fontsize(8.2)
    for (row, col), cell in rendered_table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#e2e8f0")
        elif col == 0:
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#f8fafc")
        cell.get_text().set_wrap(True)

    memo_ax.axis("off")
    memo_ax.set_title("Short Final Memo", loc="left")
    memo_ax.text(0.0, 0.95, data.final_memo_text, va="top", ha="left", wrap=True, fontsize=10)

    fig.tight_layout()
    return fig


def save_final_research_summary_section(
    ticker: str,
    *,
    output_dir: str | Path,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Save Final Research Summary review artifacts for one ticker."""

    section_data = build_final_research_summary_section_data(
        ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    artifact_stem = f"{section_data.ticker.lower()}_final_research_summary"
    figure_path = destination / f"{artifact_stem}.png"
    table_path = destination / f"{artifact_stem}_table.csv"
    text_path = destination / f"{artifact_stem}_summary.md"

    figure = render_final_research_summary_section(section_data)
    figure.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(figure)

    section_data.closing_summary_table.to_csv(table_path, index=False)
    text_path.write_text(section_data.final_memo_text + "\n")

    return {
        "figure_path": figure_path,
        "table_path": table_path,
        "text_path": text_path,
    }


__all__ = [
    "FinalResearchSummarySectionData",
    "build_final_research_summary_section_data",
    "build_final_research_summary_section_data_from_packet",
    "build_final_research_summary_table",
    "build_final_research_summary_text_from_data",
    "render_final_research_summary_section",
    "save_final_research_summary_section",
]
