"""Notebook-friendly company report tables and charts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dumb_money import _matplotlib  # noqa: F401
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.figure import Figure

from dumb_money.config import AppSettings, get_settings
from dumb_money.analytics.company import (
    build_drawdown_series,
    build_indexed_price_series,
    build_moving_average_series,
)
from dumb_money.analytics.scorecard import CATEGORY_TARGET_WEIGHTS
from dumb_money.outputs.balance_sheet_strength_section import (
    BalanceSheetStrengthSectionData,
    build_balance_sheet_strength_section_data,
    build_balance_sheet_strength_section_data_from_packet,
    build_balance_sheet_strength_table as build_section_balance_sheet_strength_table,
    render_balance_sheet_strength_section,
    save_balance_sheet_strength_section,
)
from dumb_money.outputs.final_research_summary_section import (
    FinalResearchSummarySectionData,
    build_final_research_summary_section_data,
    build_final_research_summary_section_data_from_packet,
    build_final_research_summary_text_from_data,
    render_final_research_summary_section,
    save_final_research_summary_section,
)
from dumb_money.outputs.growth_profitability_section import (
    GrowthProfitabilitySectionData,
    build_growth_profitability_section_data,
    render_growth_profitability_section,
    save_growth_profitability_section,
)
from dumb_money.outputs.market_performance_section import (
    MarketPerformanceSectionData,
    build_market_performance_section_data,
    render_market_performance_section,
    save_market_performance_section,
)
from dumb_money.outputs.peer_positioning_section import (
    PeerPositioningSectionData,
    build_peer_positioning_section_data,
    build_peer_positioning_return_table as build_section_peer_positioning_return_table,
    build_peer_positioning_section_data_from_packet,
    build_peer_positioning_valuation_table as build_section_peer_positioning_valuation_table,
    render_peer_positioning_section,
    save_peer_positioning_section,
)
from dumb_money.outputs.research_summary_section import (
    ResearchSummarySectionData,
    build_research_summary_section_data,
    build_research_summary_section_data_from_packet,
    build_research_summary_strip_table as build_section_research_summary_strip_table,
    build_research_summary_table as build_section_research_summary_table,
    build_research_summary_text_from_data,
    render_research_summary_section,
    save_research_summary_section,
)
from dumb_money.outputs.score_decomposition_section import (
    ScoreDecompositionSectionData,
    build_score_decomposition_section_data,
    render_score_decomposition_section,
    save_score_decomposition_section,
)
from dumb_money.outputs.trend_risk_profile_section import (
    TrendRiskProfileSectionData,
    build_trend_risk_profile_section_data,
    build_trend_risk_profile_section_data_from_packet,
    build_trend_risk_profile_table as build_section_trend_risk_profile_table,
    render_trend_risk_profile_section,
    save_trend_risk_profile_section,
)
from dumb_money.outputs.valuation_section import (
    ValuationSectionData,
    build_valuation_section_data,
    build_valuation_section_data_from_packet,
    build_valuation_summary_table as build_section_valuation_summary_table,
    render_valuation_section,
    save_valuation_section,
)
from dumb_money.research.company import CompanyResearchPacket

REPORT_COLUMNS = [
    "label",
    "value",
]

CATEGORY_COLORS: dict[str, str] = {
    "Market Performance": "#1d4ed8",
    "Growth and Profitability": "#047857",
    "Balance Sheet Strength": "#b45309",
    "Valuation": "#7c3aed",
}

FULL_REPORT_SECTION_ORDER: tuple[tuple[str, str], ...] = (
    ("market_performance", "Market Performance"),
    ("research_summary", "Research Summary"),
    ("trend_risk_profile", "Trend and Risk Profile"),
    ("balance_sheet_strength", "Balance Sheet Strength"),
    ("valuation", "Valuation"),
    ("peer_positioning", "Peer Positioning"),
    ("score_decomposition", "Score Decomposition"),
    ("growth_profitability", "Growth and Profitability"),
    ("final_research_summary", "Final Research Summary"),
)


@dataclass(slots=True)
class FullCompanyReportBundle:
    """Fully assembled Sprint 6 report built from section modules only."""

    ticker: str
    benchmark_set_id: str | None
    market_performance: MarketPerformanceSectionData
    research_summary: ResearchSummarySectionData
    trend_risk_profile: TrendRiskProfileSectionData
    balance_sheet_strength: BalanceSheetStrengthSectionData
    valuation: ValuationSectionData
    peer_positioning: PeerPositioningSectionData
    score_decomposition: ScoreDecompositionSectionData
    growth_profitability: GrowthProfitabilitySectionData
    final_research_summary: FinalResearchSummarySectionData


def _format_percent(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}%}"


def _format_ratio(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}f}x"


def _format_number(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}f}"


def _format_billions(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value / 1_000_000_000:.{digits}f}B"


def _format_currency(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value:,.{digits}f}"


def _format_metric_raw_value(metric_id: str, value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"

    percent_metric_ids = {
        "return_vs_spy_1y",
        "return_vs_secondary_1y",
        "max_drawdown_1y",
        "price_vs_sma_200",
        "operating_margin",
        "free_cash_flow_margin",
        "roe",
        "roa",
        "gross_margin",
        "yield_metric",
    }
    multiple_metric_ids = {
        "net_debt_to_ebitda",
        "current_ratio",
        "free_cash_flow_to_debt",
        "cash_to_debt",
        "forward_pe",
        "ev_to_ebitda",
        "price_to_sales",
    }
    plain_number_metric_ids = {"debt_to_equity"}

    if metric_id in percent_metric_ids:
        return _format_percent(value)
    if metric_id in multiple_metric_ids:
        return _format_ratio(value)
    if metric_id in plain_number_metric_ids:
        return _format_number(value)
    return _format_number(value, digits=4)


def _flag_from_normalized_value(normalized_value: float | None, *, inverse: bool = False) -> str:
    if normalized_value is None or pd.isna(normalized_value):
        return "Unavailable"

    effective_value = 1 - float(normalized_value) if inverse else float(normalized_value)
    if effective_value >= 0.95:
        return "Clear strength"
    if effective_value >= 0.70:
        return "Supportive"
    if effective_value >= 0.45:
        return "Mixed"
    if effective_value > 0:
        return "Constraint"
    return "Weak"


def _category_score_frame(packet: CompanyResearchPacket) -> pd.DataFrame:
    data = packet.scorecard.category_scores.copy()
    data["target"] = data["category"].map(CATEGORY_TARGET_WEIGHTS)
    data["score_pct"] = data["category_score"] / data["target"]
    data["score_display"] = data.apply(
        lambda row: f"{row['category_score']:.1f} / {row['target']:.0f}",
        axis=1,
    )
    data["assessment"] = data["score_pct"].map(
        lambda value: _flag_from_normalized_value(value)
    )
    return data


def _build_strength_constraint_lists(packet: CompanyResearchPacket) -> tuple[list[str], list[str]]:
    metrics = packet.scorecard.metrics.copy()
    if metrics.empty:
        return ([], [])

    available = metrics.loc[metrics["metric_available"]].copy()
    if available.empty:
        return ([], [])

    available["score_capture"] = available["metric_score"] / available["metric_weight"]
    strengths = available.sort_values(["score_capture", "metric_weight"], ascending=[False, False]).head(3)
    constraints = available.sort_values(["score_capture", "metric_weight"], ascending=[True, False]).head(3)

    strength_list = [
        f"{row.metric_name} ({_format_metric_raw_value(row.metric_id, row.raw_value)})"
        for row in strengths.itertuples()
    ]
    constraint_list = [
        f"{row.metric_name} ({_format_metric_raw_value(row.metric_id, row.raw_value)})"
        for row in constraints.itertuples()
    ]
    return (strength_list, constraint_list)


def _lookup_metric(packet: CompanyResearchPacket, metric_id: str) -> pd.Series:
    rows = packet.scorecard.metrics.loc[packet.scorecard.metrics["metric_id"] == metric_id]
    if rows.empty:
        raise KeyError(metric_id)
    return rows.iloc[0]


def build_company_overview_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a compact company overview table for notebooks and reports."""

    summary = packet.fundamentals_summary
    rows = [
        ("Ticker", packet.ticker),
        ("Company", packet.company_name or "N/A"),
        ("Sector", summary.get("sector") or "N/A"),
        ("Industry", summary.get("industry") or "N/A"),
        ("Report Date", packet.scorecard.summary.get("score_date") or packet.as_of_date or "N/A"),
        ("Fundamentals As Of", packet.as_of_date or "N/A"),
        ("Market Cap", _format_billions(summary.get("market_cap"))),
        ("Revenue TTM", _format_billions(summary.get("revenue_ttm"))),
        ("Free Cash Flow", _format_billions(summary.get("free_cash_flow"))),
        ("Net Cash", _format_billions(summary.get("net_cash"))),
    ]
    return pd.DataFrame(rows, columns=REPORT_COLUMNS)


def build_scorecard_summary_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a compact scorecard summary table with category scores."""

    summary = packet.scorecard.summary
    category_frame = _category_score_frame(packet)
    strongest = category_frame.sort_values("score_pct", ascending=False).iloc[0]["category"]
    weakest = category_frame.sort_values("score_pct", ascending=True).iloc[0]["category"]
    rows = [
        ("Total Score", f"{summary['total_score']:.1f} / 100"),
        ("Interpretation", build_research_summary_text(packet, short=True)),
        ("Confidence", _format_percent(summary["confidence_score"])),
        ("Primary Benchmark", summary["primary_benchmark"]),
        ("Secondary Benchmark", summary["secondary_benchmark"] or "N/A"),
        ("Strongest Pillar", strongest),
        ("Main Constraint", weakest),
    ]

    for category in CATEGORY_TARGET_WEIGHTS:
        category_score = category_frame.loc[category_frame["category"] == category, "category_score"].iloc[0]
        rows.append((category, f"{category_score:.1f} / {CATEGORY_TARGET_WEIGHTS[category]:.0f}"))

    return pd.DataFrame(rows, columns=REPORT_COLUMNS)


def build_research_summary_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a memo-style research summary table."""

    section_data = build_research_summary_section_data_from_packet(packet)
    return build_section_research_summary_table(section_data)


def build_score_summary_strip_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a compact score strip table for total and category summaries."""

    section_data = build_research_summary_section_data_from_packet(packet)
    return build_section_research_summary_strip_table(section_data)


def build_benchmark_comparison_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a formatted benchmark comparison table across return windows."""

    comparison = packet.benchmark_comparison.copy()
    if comparison.empty:
        return comparison

    comparison["company_return"] = comparison["company_return"].map(_format_percent)
    comparison["benchmark_return"] = comparison["benchmark_return"].map(_format_percent)
    comparison["excess_return"] = comparison["excess_return"].map(_format_percent)
    return comparison.rename(
        columns={
            "benchmark_ticker": "Benchmark",
            "window": "Window",
            "company_return": f"{packet.ticker} Return",
            "benchmark_return": "Benchmark Return",
            "excess_return": "Excess Return",
        }
    )


def build_scorecard_metrics_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a notebook-friendly scorecard metric table."""

    metrics = packet.scorecard.metrics.copy()
    if metrics.empty:
        return metrics

    metrics["raw_value_display"] = metrics.apply(
        lambda row: _format_metric_raw_value(row["metric_id"], row["raw_value"]),
        axis=1,
    )
    metrics["score_display"] = metrics.apply(
        lambda row: f"{row['metric_score']:.2f} / {row['metric_weight']:.0f}",
        axis=1,
    )
    metrics["interpretation"] = metrics["normalized_value"].map(_flag_from_normalized_value)
    return metrics[
        [
            "category",
            "metric_name",
            "raw_value_display",
            "score_display",
            "interpretation",
            "confidence_flag",
            "notes",
        ]
    ].rename(
        columns={
            "category": "Category",
            "metric_name": "Metric",
            "raw_value_display": "Raw Value",
            "score_display": "Score",
            "interpretation": "Interpretation",
            "confidence_flag": "Status",
            "notes": "Notes",
        }
    )


def build_return_windows_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a formatted trailing returns table."""

    returns = packet.return_windows.copy()
    if returns.empty:
        return returns

    returns["start_date"] = pd.to_datetime(returns["start_date"]).dt.date.astype(str)
    returns["end_date"] = pd.to_datetime(returns["end_date"]).dt.date.astype(str)
    returns["start_price"] = returns["start_price"].map(_format_currency)
    returns["end_price"] = returns["end_price"].map(_format_currency)
    returns["total_return"] = returns["total_return"].map(_format_percent)
    return returns.rename(
        columns={
            "window": "Window",
            "trading_days": "Trading Days",
            "start_date": "Start Date",
            "end_date": "End Date",
            "start_price": "Start Price",
            "end_price": "End Price",
            "total_return": "Total Return",
        }
    )


def build_trailing_return_comparison_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a wide trailing return comparison table for company and key benchmarks."""

    comparison = packet.trailing_return_comparison.copy()
    if comparison.empty:
        return comparison

    display = comparison.rename(columns={"company_return": packet.ticker})
    preferred_columns = ["window", packet.ticker]
    for benchmark in [
        packet.scorecard.summary["primary_benchmark"],
        packet.scorecard.summary["secondary_benchmark"],
    ]:
        if benchmark and f"{benchmark}_return" in display.columns:
            display = display.rename(columns={f"{benchmark}_return": benchmark})
            preferred_columns.append(benchmark)

    display = display[preferred_columns].copy()
    for column in display.columns:
        if column != "window":
            display[column] = display[column].map(_format_percent)
    return display.rename(columns={"window": "Window"})


def build_risk_metric_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a compact risk and trend panel."""

    section_data = build_trend_risk_profile_section_data_from_packet(packet)
    summary_table = build_section_trend_risk_profile_table(section_data)
    return summary_table[["Metric", "Value"]].rename(columns={"Metric": "label", "Value": "value"})


def build_balance_sheet_scorecard_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a point-in-time balance sheet panel using scorecard metrics."""

    section_data = build_balance_sheet_strength_section_data_from_packet(packet)
    table = build_section_balance_sheet_strength_table(section_data)
    return table.rename(columns={"Assessment": "Interpretation"})


def build_valuation_summary_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a current-state valuation table using available scorecard metrics."""

    section_data = build_valuation_section_data_from_packet(packet)
    return build_section_valuation_summary_table(section_data).rename(columns={"Assessment": "Interpretation"})


def build_peer_valuation_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a report-friendly peer valuation comparison table."""

    section_data = build_peer_positioning_section_data_from_packet(packet)
    return build_section_peer_positioning_valuation_table(section_data)


def build_peer_return_comparison_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a report-friendly peer return comparison table."""

    section_data = build_peer_positioning_section_data_from_packet(packet)
    return build_section_peer_positioning_return_table(section_data)


def build_sector_snapshot_table(packet: CompanyResearchPacket) -> pd.DataFrame:
    """Build a report-friendly sector snapshot summary table."""

    snapshot = packet.sector_snapshot
    if not snapshot:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    rows = [
        ("Sector", snapshot.get("sector") or "N/A"),
        ("Sector Benchmark", snapshot.get("sector_benchmark") or "N/A"),
        ("Companies In Sector", str(int(snapshot["company_count"])) if pd.notna(snapshot.get("company_count")) else "N/A"),
        (
            "Companies With Fundamentals",
            str(int(snapshot["companies_with_fundamentals"])) if pd.notna(snapshot.get("companies_with_fundamentals")) else "N/A",
        ),
        ("Companies With Prices", str(int(snapshot["companies_with_prices"])) if pd.notna(snapshot.get("companies_with_prices")) else "N/A"),
        ("Median Market Cap", _format_billions(snapshot.get("median_market_cap"))),
        ("Median Forward P/E", _format_ratio(snapshot.get("median_forward_pe"))),
        ("Median EV/EBITDA", _format_ratio(snapshot.get("median_ev_to_ebitda"))),
        ("Median Price/Sales", _format_ratio(snapshot.get("median_price_to_sales"))),
        ("Median FCF Yield", _format_percent(snapshot.get("median_free_cash_flow_yield"))),
        ("Median Operating Margin", _format_percent(snapshot.get("median_operating_margin"))),
        ("Median Gross Margin", _format_percent(snapshot.get("median_gross_margin"))),
        ("Median 6M Return", _format_percent(snapshot.get("median_return_6m"))),
        ("Median 1Y Return", _format_percent(snapshot.get("median_return_1y"))),
    ]
    return pd.DataFrame(rows, columns=REPORT_COLUMNS)


def build_research_summary_text(packet: CompanyResearchPacket, *, short: bool = False) -> str:
    """Build a short memo-style interpretation of the current scorecard."""

    section_data = build_research_summary_section_data_from_packet(packet)
    return build_research_summary_text_from_data(section_data, short=short)


def build_final_research_summary_text(packet: CompanyResearchPacket) -> str:
    """Build a short closing summary for the notebook memo."""

    section_data = build_final_research_summary_section_data_from_packet(packet)
    return build_final_research_summary_text_from_data(section_data)


def build_full_company_report_bundle(
    ticker: str,
    *,
    benchmark_set_id: str | None = "sample_universe",
    settings: AppSettings | None = None,
) -> FullCompanyReportBundle:
    """Build the full Sprint 6 report directly from shared section loaders."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    return FullCompanyReportBundle(
        ticker=normalized_ticker,
        benchmark_set_id=benchmark_set_id,
        market_performance=build_market_performance_section_data(
            normalized_ticker,
            benchmark_set_id=benchmark_set_id or "sample_universe",
            settings=settings,
        ),
        research_summary=build_research_summary_section_data(
            normalized_ticker,
            benchmark_set_id=benchmark_set_id or "sample_universe",
            settings=settings,
        ),
        trend_risk_profile=build_trend_risk_profile_section_data(
            normalized_ticker,
            benchmark_set_id=benchmark_set_id or "sample_universe",
            settings=settings,
        ),
        balance_sheet_strength=build_balance_sheet_strength_section_data(
            normalized_ticker,
            benchmark_set_id=benchmark_set_id,
            settings=settings,
        ),
        valuation=build_valuation_section_data(
            normalized_ticker,
            settings=settings,
        ),
        peer_positioning=build_peer_positioning_section_data(
            normalized_ticker,
            benchmark_set_id=benchmark_set_id,
            settings=settings,
        ),
        score_decomposition=build_score_decomposition_section_data(
            normalized_ticker,
            benchmark_set_id=benchmark_set_id,
            settings=settings,
        ),
        growth_profitability=build_growth_profitability_section_data(
            normalized_ticker,
            benchmark_set_id=benchmark_set_id,
            settings=settings,
        ),
        final_research_summary=build_final_research_summary_section_data(
            normalized_ticker,
            benchmark_set_id=benchmark_set_id,
            settings=settings,
        ),
    )


def build_full_company_report_index(bundle: FullCompanyReportBundle) -> pd.DataFrame:
    """Build a compact ordered section index for the assembled report."""

    return pd.DataFrame(
        [
            {"section_key": section_key, "section_title": section_title, "ticker": bundle.ticker}
            for section_key, section_title in FULL_REPORT_SECTION_ORDER
        ]
    )


def save_full_company_report(
    ticker: str,
    *,
    output_dir: str | Path,
    benchmark_set_id: str | None = "sample_universe",
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Assemble and save the full Sprint 6 report from shared section modules."""

    settings = settings or get_settings()
    bundle = build_full_company_report_bundle(
        ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    sections_dir = destination / "sections"
    sections_dir.mkdir(parents=True, exist_ok=True)

    section_artifacts: dict[str, dict[str, Path]] = {}
    for section_key, _section_title in FULL_REPORT_SECTION_ORDER:
        section_output_dir = sections_dir / section_key
        if section_key == "market_performance":
            section_artifacts[section_key] = save_market_performance_section(
                bundle.ticker,
                output_dir=section_output_dir,
                benchmark_set_id=benchmark_set_id or "sample_universe",
                settings=settings,
            )
        elif section_key == "research_summary":
            section_artifacts[section_key] = save_research_summary_section(
                bundle.ticker,
                output_dir=section_output_dir,
                benchmark_set_id=benchmark_set_id,
                settings=settings,
            )
        elif section_key == "trend_risk_profile":
            section_artifacts[section_key] = save_trend_risk_profile_section(
                bundle.ticker,
                output_dir=section_output_dir,
                benchmark_set_id=benchmark_set_id or "sample_universe",
                settings=settings,
            )
        elif section_key == "balance_sheet_strength":
            section_artifacts[section_key] = save_balance_sheet_strength_section(
                bundle.ticker,
                output_dir=section_output_dir,
                benchmark_set_id=benchmark_set_id,
                settings=settings,
            )
        elif section_key == "valuation":
            section_artifacts[section_key] = save_valuation_section(
                bundle.ticker,
                output_dir=section_output_dir,
                settings=settings,
            )
        elif section_key == "peer_positioning":
            section_artifacts[section_key] = save_peer_positioning_section(
                bundle.ticker,
                output_dir=section_output_dir,
                benchmark_set_id=benchmark_set_id,
                settings=settings,
            )
        elif section_key == "score_decomposition":
            section_artifacts[section_key] = save_score_decomposition_section(
                bundle.ticker,
                output_dir=section_output_dir,
                benchmark_set_id=benchmark_set_id,
                settings=settings,
            )
        elif section_key == "growth_profitability":
            section_artifacts[section_key] = save_growth_profitability_section(
                bundle.ticker,
                output_dir=section_output_dir,
                benchmark_set_id=benchmark_set_id,
                settings=settings,
            )
        elif section_key == "final_research_summary":
            section_artifacts[section_key] = save_final_research_summary_section(
                bundle.ticker,
                output_dir=section_output_dir,
                benchmark_set_id=benchmark_set_id,
                settings=settings,
            )

    pdf_path = destination / f"{bundle.ticker.lower()}_full_report.pdf"
    index_path = destination / f"{bundle.ticker.lower()}_full_report_index.csv"
    summary_path = destination / f"{bundle.ticker.lower()}_full_report_summary.md"

    section_figures = [
        render_market_performance_section(bundle.market_performance),
        render_research_summary_section(bundle.research_summary),
        render_trend_risk_profile_section(bundle.trend_risk_profile),
        render_balance_sheet_strength_section(bundle.balance_sheet_strength),
        render_valuation_section(bundle.valuation),
        render_peer_positioning_section(bundle.peer_positioning),
        render_score_decomposition_section(bundle.score_decomposition),
        render_growth_profitability_section(bundle.growth_profitability),
        render_final_research_summary_section(bundle.final_research_summary),
    ]
    with PdfPages(pdf_path) as pdf:
        for figure in section_figures:
            pdf.savefig(figure, bbox_inches="tight")
            plt.close(figure)

    build_full_company_report_index(bundle).to_csv(index_path, index=False)
    summary_path.write_text(
        "\n".join(
            [
                f"# {bundle.ticker} Full Report",
                "",
                "Section order:",
                *[f"- {section_title}" for _, section_title in FULL_REPORT_SECTION_ORDER],
                "",
                "Final memo:",
                bundle.final_research_summary.final_memo_text,
                "",
                f"Primary benchmark: {bundle.market_performance.primary_benchmark}",
                f"Secondary benchmark: {bundle.market_performance.secondary_benchmark or 'N/A'}",
            ]
        )
        + "\n"
    )

    return {
        "pdf_path": pdf_path,
        "index_path": index_path,
        "summary_path": summary_path,
        "sections_dir": sections_dir,
    }


def render_score_summary_strip(packet: CompanyResearchPacket) -> Figure:
    """Render a compact horizontal score strip for total score and category pillars."""

    section_data = build_research_summary_section_data_from_packet(packet)
    figure = render_research_summary_section(section_data)
    if len(figure.axes) > 1:
        for axis in figure.axes[1:]:
            axis.remove()
        figure.set_size_inches(8.5, 4.8)
        figure.tight_layout()
    return figure


def render_scorecard_category_chart(packet: CompanyResearchPacket) -> Figure:
    """Render a category scorecard bar chart."""

    data = _category_score_frame(packet)

    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.barh(data["category"], data["target"], color="#d9e2ec", label="Max")
    ax.barh(
        data["category"],
        data["category_score"],
        color=[CATEGORY_COLORS.get(category, "#1f5aa6") for category in data["category"]],
        label="Score",
    )
    ax.set_title(f"{packet.ticker} Scorecard Categories")
    ax.set_xlabel("Points")
    ax.legend(loc="lower right")
    ax.grid(axis="x", linestyle=":", alpha=0.4)
    fig.tight_layout()
    return fig


def render_indexed_price_performance_chart(packet: CompanyResearchPacket) -> Figure:
    """Render indexed price performance for the company and key benchmarks."""

    ticker_order = [
        packet.ticker,
        packet.scorecard.summary["primary_benchmark"],
        packet.scorecard.summary["secondary_benchmark"],
    ]
    color_map = {
        packet.ticker: "#111827",
        packet.scorecard.summary["primary_benchmark"]: "#2563eb",
        packet.scorecard.summary["secondary_benchmark"]: "#0f766e",
    }

    fig, ax = plt.subplots(figsize=(9, 4.8))
    for ticker in ticker_order:
        if not ticker:
            continue
        history = packet.company_history if ticker == packet.ticker else packet.benchmark_histories.get(ticker, pd.DataFrame())
        series = build_indexed_price_series(history, ticker=ticker)
        if series.empty:
            continue
        ax.plot(series["date"], series["indexed_price"], label=ticker, linewidth=2.2, color=color_map.get(ticker))

    ax.set_title(f"{packet.ticker} Indexed Price Performance")
    ax.set_ylabel("Indexed to 100")
    ax.grid(axis="y", linestyle=":", alpha=0.35)
    ax.legend(loc="upper left")
    fig.autofmt_xdate()
    fig.tight_layout()
    return fig


def render_trailing_return_comparison_chart(packet: CompanyResearchPacket) -> Figure:
    """Render grouped trailing return bars for the company and primary benchmarks."""

    data = packet.trailing_return_comparison.copy()
    fig, ax = plt.subplots(figsize=(9, 4.8))
    if data.empty:
        ax.set_title(f"{packet.ticker} Trailing Return Comparison")
        ax.text(0.5, 0.5, "No trailing return comparison data available", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        return fig

    series_map = {packet.ticker: "company_return"}
    for benchmark in [
        packet.scorecard.summary["primary_benchmark"],
        packet.scorecard.summary["secondary_benchmark"],
    ]:
        if benchmark and f"{benchmark}_return" in data.columns:
            series_map[benchmark] = f"{benchmark}_return"

    x_positions = range(len(data))
    width = 0.22
    offsets = [-width, 0.0, width]
    colors = ["#111827", "#2563eb", "#0f766e"]
    for offset, (label, column), color in zip(offsets, series_map.items(), colors, strict=False):
        ax.bar([x + offset for x in x_positions], data[column], width=width, label=label, color=color)

    ax.axhline(0, color="#475569", linewidth=1)
    ax.set_xticks(list(x_positions), data["window"])
    ax.set_title(f"{packet.ticker} Trailing Return Comparison")
    ax.set_ylabel("Return")
    ax.yaxis.set_major_formatter(lambda value, _pos: f"{value:.0%}")
    ax.grid(axis="y", linestyle=":", alpha=0.35)
    ax.legend(loc="upper left")
    fig.tight_layout()
    return fig


def render_benchmark_excess_return_chart(packet: CompanyResearchPacket) -> Figure:
    """Render a grouped bar chart of excess returns by benchmark and window."""

    data = packet.benchmark_comparison.copy()
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    if data.empty:
        ax.set_title(f"{packet.ticker} Excess Returns")
        ax.text(0.5, 0.5, "No benchmark comparison data available", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        return fig

    pivoted = data.pivot(index="window", columns="benchmark_ticker", values="excess_return").fillna(0.0)
    pivoted = pivoted.reindex(["1m", "3m", "6m", "1y"])
    selected_columns = [
        benchmark
        for benchmark in [
            packet.scorecard.summary["primary_benchmark"],
            packet.scorecard.summary["secondary_benchmark"],
        ]
        if benchmark in pivoted.columns
    ]
    if selected_columns:
        pivoted = pivoted[selected_columns]
    pivoted.plot(kind="bar", ax=ax, color=["#2563eb", "#0f766e"][: len(pivoted.columns)])
    ax.axhline(0, color="#333333", linewidth=1)
    ax.set_title(f"{packet.ticker} Excess Return vs Benchmarks")
    ax.set_xlabel("Window")
    ax.set_ylabel("Excess Return")
    ax.yaxis.set_major_formatter(lambda value, _pos: f"{value:.0%}")
    ax.grid(axis="y", linestyle=":", alpha=0.4)
    fig.tight_layout()
    return fig


def render_drawdown_chart(packet: CompanyResearchPacket) -> Figure:
    """Render trailing drawdown lines for the company and key benchmarks."""

    ticker_order = [
        packet.ticker,
        packet.scorecard.summary["primary_benchmark"],
        packet.scorecard.summary["secondary_benchmark"],
    ]
    color_map = {
        packet.ticker: "#111827",
        packet.scorecard.summary["primary_benchmark"]: "#2563eb",
        packet.scorecard.summary["secondary_benchmark"]: "#0f766e",
    }

    fig, ax = plt.subplots(figsize=(9, 4.8))
    for ticker in ticker_order:
        if not ticker:
            continue
        history = packet.company_history if ticker == packet.ticker else packet.benchmark_histories.get(ticker, pd.DataFrame())
        series = build_drawdown_series(history, ticker=ticker)
        if series.empty:
            continue
        ax.plot(series["date"], series["drawdown"], label=ticker, linewidth=2, color=color_map.get(ticker))

    ax.axhline(0, color="#94a3b8", linewidth=1)
    ax.set_title(f"{packet.ticker} Drawdown Profile")
    ax.set_ylabel("Drawdown")
    ax.yaxis.set_major_formatter(lambda value, _pos: f"{value:.0%}")
    ax.grid(axis="y", linestyle=":", alpha=0.35)
    ax.legend(loc="lower left")
    fig.autofmt_xdate()
    fig.tight_layout()
    return fig


def render_price_with_moving_averages_chart(packet: CompanyResearchPacket) -> Figure:
    """Render price with 50-day and 200-day moving averages."""

    data = build_moving_average_series(packet.company_history)
    fig, ax = plt.subplots(figsize=(9, 4.8))
    if data.empty:
        ax.set_title(f"{packet.ticker} Price and Moving Averages")
        ax.text(0.5, 0.5, "No price history available", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        return fig

    ax.plot(data["date"], data["adj_close"], label=packet.ticker, linewidth=2.4, color="#111827")
    ax.plot(data["date"], data["sma_50"], label="50D MA", linewidth=1.8, color="#2563eb")
    ax.plot(data["date"], data["sma_200"], label="200D MA", linewidth=1.8, color="#b45309")
    ax.set_title(f"{packet.ticker} Price and Moving Averages")
    ax.set_ylabel("Adjusted Close")
    ax.grid(axis="y", linestyle=":", alpha=0.35)
    ax.legend(loc="upper left")
    fig.autofmt_xdate()
    fig.tight_layout()
    return fig


def render_scorecard_metric_chart(packet: CompanyResearchPacket) -> Figure:
    """Render metric-level score capture versus available weight."""

    data = packet.scorecard.metrics.copy()
    fig, ax = plt.subplots(figsize=(10, 6))
    if data.empty:
        ax.set_title(f"{packet.ticker} Scorecard Metrics")
        ax.text(0.5, 0.5, "No scorecard metrics available", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        return fig

    labels = data["metric_name"]
    ax.barh(labels, data["metric_weight"], color="#d9e2ec", label="Weight")
    ax.barh(
        labels,
        data["metric_score"],
        color=[CATEGORY_COLORS.get(category, "#0f766e") for category in data["category"]],
        label="Score",
    )
    ax.set_title(f"{packet.ticker} Metric-Level Score Capture")
    ax.set_xlabel("Points")
    ax.legend(loc="lower right")
    ax.grid(axis="x", linestyle=":", alpha=0.4)
    fig.tight_layout()
    return fig


def render_score_decomposition_chart(packet: CompanyResearchPacket) -> Figure:
    """Render metric-level score contributions sorted by contribution."""

    metrics = packet.scorecard.metrics.copy()
    fig, ax = plt.subplots(figsize=(10, 6.4))
    if metrics.empty:
        ax.set_title(f"{packet.ticker} Score Decomposition")
        ax.text(0.5, 0.5, "No score decomposition available", ha="center", va="center")
        ax.axis("off")
        fig.tight_layout()
        return fig

    metrics = metrics.sort_values(["metric_score", "metric_weight"], ascending=[True, False]).copy()
    ax.barh(
        metrics["metric_name"],
        metrics["metric_score"],
        color=[CATEGORY_COLORS.get(category, "#1f5aa6") for category in metrics["category"]],
    )
    ax.set_title(f"{packet.ticker} Score Decomposition")
    ax.set_xlabel("Points Contributed")
    ax.grid(axis="x", linestyle=":", alpha=0.35)

    for row in metrics.itertuples():
        ax.text(row.metric_score + 0.12, row.metric_name, f"{row.metric_score:.2f}", va="center", fontsize=8.5)

    fig.tight_layout()
    return fig


def close_figure(figure: Figure) -> None:
    """Close a matplotlib figure after tests or notebook rendering."""

    plt.close(figure)


__all__ = [
    "Axes",
    "Any",
    "Figure",
    "FULL_REPORT_SECTION_ORDER",
    "FullCompanyReportBundle",
    "build_balance_sheet_scorecard_table",
    "build_benchmark_comparison_table",
    "build_company_overview_table",
    "build_final_research_summary_text",
    "build_full_company_report_bundle",
    "build_full_company_report_index",
    "build_sector_snapshot_table",
    "build_peer_return_comparison_table",
    "build_peer_valuation_table",
    "build_research_summary_table",
    "build_research_summary_text",
    "build_return_windows_table",
    "build_risk_metric_table",
    "build_score_summary_strip_table",
    "build_scorecard_metrics_table",
    "build_scorecard_summary_table",
    "build_trailing_return_comparison_table",
    "build_valuation_summary_table",
    "close_figure",
    "render_benchmark_excess_return_chart",
    "render_drawdown_chart",
    "render_indexed_price_performance_chart",
    "render_price_with_moving_averages_chart",
    "render_score_decomposition_chart",
    "render_score_summary_strip",
    "render_scorecard_category_chart",
    "render_scorecard_metric_chart",
    "render_trailing_return_comparison_chart",
    "save_full_company_report",
]
