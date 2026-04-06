"""DuckDB-aware Research Summary section builders and renderers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dumb_money import _matplotlib  # noqa: F401
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from dumb_money.analytics.company import (
    build_fundamentals_summary,
    build_peer_valuation_comparison,
    calculate_return_windows,
    calculate_risk_metrics,
    calculate_trend_metrics,
)
from dumb_money.analytics.scorecard import (
    CATEGORY_TARGET_WEIGHTS,
    build_company_scorecard,
)
from dumb_money.config import AppSettings, get_settings
from dumb_money.outputs.market_performance_section import build_market_performance_section_data
from dumb_money.research.company import (
    CompanyResearchPacket,
    load_benchmark_mappings,
    load_peer_sets,
    load_security_master,
    load_staged_fundamentals,
)

REPORT_COLUMNS = ["label", "value"]

CATEGORY_COLORS: dict[str, str] = {
    "Market Performance": "#1d4ed8",
    "Growth and Profitability": "#047857",
    "Balance Sheet Strength": "#b45309",
    "Valuation": "#7c3aed",
}


@dataclass(slots=True)
class ResearchSummarySectionData:
    """Query-ready Research Summary inputs and standardized derived outputs."""

    ticker: str
    company_name: str | None
    sector: str | None
    industry: str | None
    report_date: str | None
    total_score: float
    confidence_score: float | None
    interpretation_label: str
    strongest_pillar: str
    weakest_pillar: str
    strengths: list[str]
    constraints: list[str]
    category_scores: pd.DataFrame
    summary_table: pd.DataFrame
    score_summary_strip: pd.DataFrame
    summary_text: str


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
    data["assessment"] = data["score_pct"].map(_flag_from_normalized_value)
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


def resolve_research_summary_label(total_score: float) -> str:
    """Map the total score to the standardized Research Summary interpretation label."""

    if total_score >= 80:
        return "High-quality setup with broad support"
    if total_score >= 65:
        return "Quality-led profile with mixed offsets"
    if total_score >= 50:
        return "Mixed setup with visible tradeoffs"
    return "Challenged profile that needs more support"


def build_research_summary_text_from_data(
    data: ResearchSummarySectionData,
    *,
    short: bool = False,
) -> str:
    """Build the reusable memo-style interpretation from section data."""

    if short:
        return data.interpretation_label

    strength_text = data.strengths[0] if data.strengths else data.strongest_pillar
    constraint_text = data.constraints[0] if data.constraints else data.weakest_pillar
    company_label = data.company_name or data.ticker
    return (
        f"{company_label} screens as a {data.interpretation_label.lower()}. "
        f"The score is anchored by {data.strongest_pillar.lower()}, with {strength_text.lower()} standing out in the current data. "
        f"The main drag is {data.weakest_pillar.lower()}, where {constraint_text.lower()} is keeping the memo more cautious."
    )


def build_research_summary_strip_table(data: ResearchSummarySectionData) -> pd.DataFrame:
    """Build the standardized score summary strip table for this section."""

    return data.score_summary_strip.copy()


def build_research_summary_table(data: ResearchSummarySectionData) -> pd.DataFrame:
    """Build the standardized memo-style Research Summary table."""

    return data.summary_table.copy()


def build_research_summary_section_data_from_packet(
    packet: CompanyResearchPacket,
) -> ResearchSummarySectionData:
    """Build standardized Research Summary outputs from the shared research packet."""

    summary = packet.scorecard.summary

    # The section consumes the shared scorecard summary, category scores, and metric
    # rows rather than issuing new ad hoc queries. The packet is already built from
    # canonical staged datasets and DuckDB-backed tables.
    category_scores = _category_score_frame(packet)
    strongest = category_scores.sort_values("score_pct", ascending=False).iloc[0]
    weakest = category_scores.sort_values("score_pct", ascending=True).iloc[0]
    strengths, constraints = _build_strength_constraint_lists(packet)
    interpretation_label = resolve_research_summary_label(float(summary["total_score"]))

    strip_rows = [
        {
            "pillar": "Total Score",
            "score": float(summary["total_score"]),
            "max_score": 100.0,
            "score_pct": float(summary["total_score"]) / 100.0,
            "assessment": _flag_from_normalized_value(float(summary["total_score"]) / 100.0),
        }
    ]
    for row in category_scores.itertuples():
        strip_rows.append(
            {
                "pillar": row.category,
                "score": float(row.category_score),
                "max_score": float(row.target),
                "score_pct": float(row.score_pct),
                "assessment": row.assessment,
            }
        )
    score_summary_strip = pd.DataFrame(strip_rows)
    score_summary_strip["score_display"] = score_summary_strip.apply(
        lambda row: f"{row['score']:.1f} / {row['max_score']:.0f}",
        axis=1,
    )

    summary_table = pd.DataFrame(
        [
            ("Company", packet.company_name or packet.ticker),
            ("Ticker", packet.ticker),
            ("Sector", summary.get("sector") or packet.fundamentals_summary.get("sector") or "N/A"),
            ("Industry", summary.get("industry") or packet.fundamentals_summary.get("industry") or "N/A"),
            ("Report Date", summary.get("score_date") or packet.as_of_date or "N/A"),
            ("Research View", interpretation_label),
            ("Score", f"{float(summary['total_score']):.1f} / 100"),
            ("Confidence", _format_percent(summary.get("confidence_score"))),
            ("Best Supported Pillar", f"{strongest['category']} ({strongest['score_display']})"),
            ("Main Watch Item", f"{weakest['category']} ({weakest['score_display']})"),
            ("Strengths", "; ".join(strengths) if strengths else strongest["category"]),
            ("Constraints", "; ".join(constraints) if constraints else weakest["category"]),
        ],
        columns=REPORT_COLUMNS,
    )

    data = ResearchSummarySectionData(
        ticker=packet.ticker,
        company_name=packet.company_name,
        sector=summary.get("sector") or packet.fundamentals_summary.get("sector"),
        industry=summary.get("industry") or packet.fundamentals_summary.get("industry"),
        report_date=summary.get("score_date") or packet.as_of_date,
        total_score=float(summary["total_score"]),
        confidence_score=summary.get("confidence_score"),
        interpretation_label=interpretation_label,
        strongest_pillar=str(strongest["category"]),
        weakest_pillar=str(weakest["category"]),
        strengths=strengths,
        constraints=constraints,
        category_scores=category_scores,
        summary_table=summary_table,
        score_summary_strip=score_summary_strip,
        summary_text="",
    )
    data.summary_text = build_research_summary_text_from_data(data)
    return data


def build_research_summary_section_data(
    ticker: str,
    *,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> ResearchSummarySectionData:
    """Build the Research Summary section from the shared section inputs only."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    # Reuse the standardized Market Performance section contract for benchmark
    # alignment instead of re-implementing benchmark selection in this section.
    market_data = build_market_performance_section_data(
        normalized_ticker,
        benchmark_set_id=benchmark_set_id or "sample_universe",
        settings=settings,
    )
    company_history = market_data.histories[normalized_ticker]

    fundamentals = load_staged_fundamentals(settings=settings)
    fundamentals_summary = build_fundamentals_summary(fundamentals, normalized_ticker)
    security_master = load_security_master(settings=settings)
    benchmark_mappings = load_benchmark_mappings(settings=settings)
    peer_sets = load_peer_sets(settings=settings)

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

    return_windows = calculate_return_windows(company_history)
    risk_metrics = calculate_risk_metrics(company_history)
    trend_metrics = calculate_trend_metrics(company_history)
    peer_valuation_comparison = build_peer_valuation_comparison(
        normalized_ticker,
        peer_rows,
        fundamentals,
    )

    end_dates = return_windows["end_date"].dropna() if "end_date" in return_windows.columns else pd.Series(dtype=object)
    score_date = (
        str(pd.to_datetime(end_dates.iloc[-1]).date())
        if not return_windows.empty and not end_dates.empty
        else fundamentals_summary.get("as_of_date")
    )
    scorecard = build_company_scorecard(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        sector=fundamentals_summary.get("sector") or security_row.get("sector"),
        industry=fundamentals_summary.get("industry") or security_row.get("industry"),
        score_date=score_date,
        benchmark_comparison=market_data.benchmark_comparison,
        risk_metrics=risk_metrics,
        trend_metrics=trend_metrics,
        fundamentals_summary=fundamentals_summary,
        peer_valuation_comparison=peer_valuation_comparison,
        primary_benchmark=benchmark_mapping_row.get("primary_benchmark") or market_data.primary_benchmark,
        secondary_benchmark=(
            benchmark_mapping_row.get("sector_benchmark")
            or benchmark_mapping_row.get("style_benchmark")
            or benchmark_mapping_row.get("industry_benchmark")
            or benchmark_mapping_row.get("custom_benchmark")
            or market_data.secondary_benchmark
        ),
    )

    packet = CompanyResearchPacket(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        as_of_date=fundamentals_summary.get("as_of_date"),
        company_history=company_history,
        benchmark_histories={},
        return_windows=return_windows,
        trailing_return_comparison=market_data.trailing_return_comparison,
        risk_metrics=risk_metrics,
        trend_metrics=trend_metrics,
        benchmark_comparison=market_data.benchmark_comparison,
        fundamentals_summary=fundamentals_summary,
        peer_return_comparison=pd.DataFrame(),
        peer_return_summary_stats={},
        peer_valuation_comparison=peer_valuation_comparison,
        peer_summary_stats={},
        sector_snapshot={},
        scorecard=scorecard,
    )
    return build_research_summary_section_data_from_packet(packet)


def render_research_summary_section(data: ResearchSummarySectionData) -> Figure:
    """Render the full Research Summary section as one reviewable figure."""

    fig, axes = plt.subplots(
        nrows=3,
        ncols=1,
        figsize=(11.5, 11.0),
        gridspec_kw={"height_ratios": [1.5, 1.7, 0.8]},
    )
    strip_ax, table_ax, text_ax = axes

    strip = data.score_summary_strip.copy()
    y_positions = list(range(len(strip)))
    max_values = strip["max_score"].tolist()
    score_values = strip["score"].tolist()
    colors = [
        "#0f172a" if pillar == "Total Score" else CATEGORY_COLORS.get(pillar, "#1f5aa6")
        for pillar in strip["pillar"]
    ]

    # Plot the shared total and pillar scores against their intended maximums so the
    # section shows both absolute points and relative capture at a glance.
    strip_ax.barh(y_positions, max_values, color="#e2e8f0", height=0.58)
    strip_ax.barh(y_positions, score_values, color=colors, height=0.58)
    strip_ax.set_yticks(y_positions, strip["pillar"])
    strip_ax.invert_yaxis()
    strip_ax.set_xlim(0, 100)
    strip_ax.set_xlabel("Points")
    strip_ax.set_title(f"{data.ticker} Research Summary", loc="left", fontsize=14, fontweight="bold")
    strip_ax.grid(axis="x", linestyle=":", alpha=0.35)
    for y_position, score, score_display in zip(y_positions, score_values, strip["score_display"], strict=False):
        strip_ax.text(min(score + 1.5, 98), y_position, score_display, va="center", ha="left", fontsize=9)

    table_ax.axis("off")
    table_ax.set_title("Summary Table", loc="left")
    summary_table = data.summary_table.copy()
    rendered_table = table_ax.table(
        cellText=summary_table.values,
        colLabels=["Field", "Value"],
        cellLoc="left",
        colLoc="left",
        bbox=[0.0, 0.0, 1.0, 0.95],
    )
    rendered_table.auto_set_font_size(False)
    rendered_table.set_fontsize(9)
    for (row, col), cell in rendered_table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight="bold")
            cell.set_facecolor("#e2e8f0")
        elif col == 0:
            cell.set_text_props(weight="bold")

    text_ax.axis("off")
    text_ax.set_title("Short Interpretation", loc="left")
    text_ax.text(
        0.0,
        0.95,
        data.summary_text,
        va="top",
        ha="left",
        wrap=True,
        fontsize=10,
    )

    fig.tight_layout()
    return fig


def save_research_summary_section(
    ticker: str,
    *,
    output_dir: str | Path,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Save Research Summary review artifacts for one ticker."""

    section_data = build_research_summary_section_data(
        ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    artifact_stem = f"{section_data.ticker.lower()}_research_summary"
    figure_path = destination / f"{artifact_stem}.png"
    table_path = destination / f"{artifact_stem}_table.csv"
    strip_path = destination / f"{artifact_stem}_strip.csv"
    text_path = destination / f"{artifact_stem}_summary.md"

    figure = render_research_summary_section(section_data)
    figure.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(figure)

    section_data.summary_table.to_csv(table_path, index=False)
    section_data.score_summary_strip.to_csv(strip_path, index=False)
    text_path.write_text(section_data.summary_text + "\n")

    return {
        "figure_path": figure_path,
        "table_path": table_path,
        "strip_path": strip_path,
        "text_path": text_path,
    }


__all__ = [
    "ResearchSummarySectionData",
    "build_research_summary_section_data",
    "build_research_summary_section_data_from_packet",
    "build_research_summary_strip_table",
    "build_research_summary_table",
    "build_research_summary_text_from_data",
    "render_research_summary_section",
    "resolve_research_summary_label",
    "save_research_summary_section",
]
