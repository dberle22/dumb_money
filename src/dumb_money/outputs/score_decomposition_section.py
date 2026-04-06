"""DuckDB-aware Score Decomposition section builders and renderers."""

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
    build_peer_summary_stats,
    build_peer_valuation_comparison,
    calculate_risk_metrics,
    calculate_trend_metrics,
    prepare_price_history,
)
from dumb_money.analytics.scorecard import build_company_scorecard
from dumb_money.analytics.scorecard import CATEGORY_TARGET_WEIGHTS
from dumb_money.config import AppSettings, get_settings
from dumb_money.research.company import (
    CompanyResearchPacket,
    load_benchmark_mappings,
    load_benchmark_prices,
    load_benchmark_set,
    load_peer_sets,
    load_security_master,
    load_staged_fundamentals,
    load_staged_prices,
)

CATEGORY_TABLE_COLUMNS = [
    "Category",
    "Category Score",
    "Target",
    "Contribution to Total",
    "Coverage Ratio",
    "Available Weight",
    "Assessment",
]
METRIC_TABLE_COLUMNS = [
    "Category",
    "Metric",
    "Metric ID",
    "Raw Value",
    "Normalized Value",
    "Score Bucket",
    "Metric Score",
    "Metric Weight",
    "Score Capture",
    "Scoring Method",
    "Confidence Flag",
    "Category Coverage",
    "Shared Input",
    "Notes",
]
STRIP_COLUMNS = [
    "signal",
    "score",
    "max_score",
    "contribution_pct",
    "coverage_ratio",
    "score_display",
    "contribution_display",
    "assessment",
]


@dataclass(slots=True)
class ScoreDecompositionSectionData:
    """Query-ready Score Decomposition inputs and standardized outputs."""

    ticker: str
    company_name: str | None
    sector: str | None
    industry: str | None
    report_date: str | None
    total_score: float | None
    total_target: float
    confidence_score: float | None
    category_scores: pd.DataFrame
    metric_scores: pd.DataFrame
    category_contribution_table: pd.DataFrame
    metric_score_table: pd.DataFrame
    summary_strip: pd.DataFrame
    interpretation_text: str


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
        "free_cash_flow_to_debt",
        "cash_to_debt",
    }
    multiple_metric_ids = {
        "net_debt_to_ebitda",
        "current_ratio",
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


def _score_bucket(normalized_value: float | None) -> str:
    if normalized_value is None or pd.isna(normalized_value):
        return "Unavailable"
    if float(normalized_value) >= 0.95:
        return "Strong"
    if float(normalized_value) >= 0.70:
        return "Supportive"
    if float(normalized_value) >= 0.45:
        return "Mixed"
    if float(normalized_value) > 0:
        return "Constraint"
    return "Weak"


def _shared_input_label(metric_id: str, notes: str) -> str:
    mapping = {
        "return_vs_spy_1y": "scorecard.benchmark_comparison[primary_benchmark, 1y].excess_return",
        "return_vs_secondary_1y": "scorecard.benchmark_comparison[secondary_benchmark, 1y].excess_return",
        "max_drawdown_1y": "scorecard.risk_metrics.max_drawdown_1y",
        "price_vs_sma_200": "scorecard.trend_metrics.price_vs_sma_200",
        "operating_margin": "scorecard.fundamentals_summary.operating_margin",
        "free_cash_flow_margin": "scorecard.fundamentals_summary.free_cash_flow_margin",
        "roe": "scorecard.fundamentals_summary.return_on_equity",
        "roa": "scorecard.fundamentals_summary.return_on_assets",
        "gross_margin": "scorecard.fundamentals_summary.gross_margin",
        "net_debt_to_ebitda": "scorecard.fundamentals_summary.net_debt_to_ebitda",
        "current_ratio": "scorecard.fundamentals_summary.current_ratio",
        "debt_to_equity": "scorecard.fundamentals_summary.debt_to_equity",
        "free_cash_flow_to_debt": "scorecard.fundamentals_summary.free_cash_flow_to_debt",
        "cash_to_debt": "scorecard.fundamentals_summary.cash_to_debt",
        "yield_metric": "scorecard.fundamentals_summary.free_cash_flow, market_cap, or dividend_yield",
    }
    if metric_id in {"forward_pe", "ev_to_ebitda", "price_to_sales"}:
        if "peer median" in notes.lower():
            return f"scorecard.fundamentals_summary.{metric_id} + scorecard.peer_valuation_comparison.{metric_id}"
        return f"scorecard.fundamentals_summary.{metric_id}"
    return mapping.get(metric_id, "scorecard.shared_inputs")


def _category_from_summary(summary: dict[str, object]) -> pd.DataFrame:
    rows = [
        {
            "category": "Market Performance",
            "category_score": summary.get("market_performance_score"),
            "available_weight": None,
            "total_intended_weight": CATEGORY_TARGET_WEIGHTS["Market Performance"],
            "coverage_ratio": None,
        },
        {
            "category": "Growth and Profitability",
            "category_score": summary.get("growth_profitability_score"),
            "available_weight": None,
            "total_intended_weight": CATEGORY_TARGET_WEIGHTS["Growth and Profitability"],
            "coverage_ratio": None,
        },
        {
            "category": "Balance Sheet Strength",
            "category_score": summary.get("balance_sheet_score"),
            "available_weight": None,
            "total_intended_weight": CATEGORY_TARGET_WEIGHTS["Balance Sheet Strength"],
            "coverage_ratio": None,
        },
        {
            "category": "Valuation",
            "category_score": summary.get("valuation_score"),
            "available_weight": None,
            "total_intended_weight": CATEGORY_TARGET_WEIGHTS["Valuation"],
            "coverage_ratio": None,
        },
    ]
    return pd.DataFrame(rows)


def _build_category_contribution_table(
    scorecard_summary: dict[str, object],
    category_scores: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    total_score = float(scorecard_summary.get("total_score") or 0.0)
    categories = category_scores.copy()
    if categories.empty:
        categories = _category_from_summary(scorecard_summary)

    categories["target"] = categories["category"].map(CATEGORY_TARGET_WEIGHTS)
    categories["category_score"] = pd.to_numeric(categories["category_score"], errors="coerce")
    categories["coverage_ratio"] = pd.to_numeric(categories["coverage_ratio"], errors="coerce")
    categories["available_weight"] = pd.to_numeric(categories["available_weight"], errors="coerce")
    categories["contribution_pct"] = categories["category_score"].map(
        lambda value: None if total_score <= 0 or pd.isna(value) else float(value) / total_score
    )
    categories["score_pct"] = categories.apply(
        lambda row: None
        if pd.isna(row["category_score"]) or pd.isna(row["target"]) or float(row["target"]) <= 0
        else float(row["category_score"]) / float(row["target"]),
        axis=1,
    )
    categories["assessment"] = categories["score_pct"].map(_score_bucket)
    categories["Category Score"] = categories.apply(
        lambda row: "N/A" if pd.isna(row["category_score"]) or pd.isna(row["target"]) else f"{row['category_score']:.1f} / {row['target']:.0f}",
        axis=1,
    )
    categories["Target"] = categories["target"].map(lambda value: "N/A" if pd.isna(value) else f"{value:.0f}")
    categories["Contribution to Total"] = categories["contribution_pct"].map(_format_percent)
    categories["Coverage Ratio"] = categories["coverage_ratio"].map(_format_percent)
    categories["Available Weight"] = categories["available_weight"].map(
        lambda value: "N/A" if value is None or pd.isna(value) else f"{value:.0f}"
    )
    categories["Assessment"] = categories["assessment"]

    order = pd.Categorical(categories["category"], categories=list(CATEGORY_TARGET_WEIGHTS.keys()), ordered=True)
    categories = categories.assign(category_order=order).sort_values("category_order").drop(columns=["category_order"])

    contribution_table = categories[
        [
            "category",
            "Category Score",
            "Target",
            "Contribution to Total",
            "Coverage Ratio",
            "Available Weight",
            "Assessment",
        ]
    ].rename(columns={"category": "Category"})

    summary_strip = pd.DataFrame(
        [
            {
                "signal": row["category"],
                "score": float(row["category_score"]) if pd.notna(row["category_score"]) else 0.0,
                "max_score": float(row["target"]) if pd.notna(row["target"]) else 0.0,
                "contribution_pct": row["contribution_pct"],
                "coverage_ratio": row["coverage_ratio"],
                "score_display": row["Category Score"],
                "contribution_display": _format_percent(row["contribution_pct"]),
                "assessment": row["assessment"],
            }
            for _, row in categories.iterrows()
        ],
        columns=STRIP_COLUMNS,
    )
    return contribution_table, summary_strip


def _build_metric_score_table(
    metrics: pd.DataFrame,
    category_scores: pd.DataFrame,
) -> pd.DataFrame:
    if metrics.empty:
        return pd.DataFrame(columns=METRIC_TABLE_COLUMNS)

    display = metrics.copy()
    coverage_lookup = {}
    if not category_scores.empty:
        coverage_lookup = (
            category_scores.set_index("category")["coverage_ratio"].to_dict()
            if "coverage_ratio" in category_scores.columns
            else {}
        )

    display["Raw Value"] = display.apply(
        lambda row: _format_metric_raw_value(str(row["metric_id"]), row["raw_value"]),
        axis=1,
    )
    display["Normalized Value"] = display["normalized_value"].map(
        lambda value: "N/A" if value is None or pd.isna(value) else f"{float(value):.2f}"
    )
    display["Score Bucket"] = display["normalized_value"].map(_score_bucket)
    display["Metric Score"] = display.apply(
        lambda row: f"{row['metric_score']:.2f}" if pd.notna(row["metric_score"]) else "N/A",
        axis=1,
    )
    display["Metric Weight"] = display["metric_weight"].map(
        lambda value: "N/A" if value is None or pd.isna(value) else f"{float(value):.0f}"
    )
    display["Score Capture"] = display.apply(
        lambda row: "N/A"
        if pd.isna(row["metric_score"]) or pd.isna(row["metric_weight"]) or float(row["metric_weight"]) <= 0
        else _format_percent(float(row["metric_score"]) / float(row["metric_weight"])),
        axis=1,
    )
    display["Scoring Method"] = display["scoring_method"].fillna("N/A")
    display["Confidence Flag"] = display["confidence_flag"].fillna("N/A")
    display["Category Coverage"] = display["category"].map(
        lambda category: _format_percent(coverage_lookup.get(category))
    )
    display["Shared Input"] = display.apply(
        lambda row: _shared_input_label(str(row["metric_id"]), str(row["notes"] or "")),
        axis=1,
    )
    display["Notes"] = display["notes"].fillna("")

    order = pd.Categorical(display["category"], categories=list(CATEGORY_TARGET_WEIGHTS.keys()), ordered=True)
    display = display.assign(
        category_order=order,
        raw_metric_score=pd.to_numeric(display["metric_score"], errors="coerce"),
        raw_metric_weight=pd.to_numeric(display["metric_weight"], errors="coerce"),
    ).sort_values(["category_order", "raw_metric_score", "raw_metric_weight"], ascending=[True, True, False])

    return display[
        [
            "category",
            "metric_name",
            "metric_id",
            "Raw Value",
            "Normalized Value",
            "Score Bucket",
            "Metric Score",
            "Metric Weight",
            "Score Capture",
            "Scoring Method",
            "Confidence Flag",
            "Category Coverage",
            "Shared Input",
            "Notes",
        ]
    ].rename(
        columns={
            "category": "Category",
            "metric_name": "Metric",
            "metric_id": "Metric ID",
        }
    )


def _build_interpretation_text(data: ScoreDecompositionSectionData) -> str:
    company_label = data.company_name or data.ticker
    if data.summary_strip.empty or data.metric_scores.empty:
        return (
            f"{company_label} does not yet have enough canonical scorecard coverage to build the current "
            "score decomposition section."
        )

    categories = data.category_scores.copy()
    categories["score_pct"] = categories.apply(
        lambda row: None
        if pd.isna(row["category_score"]) or pd.isna(row["target"]) or float(row["target"]) <= 0
        else float(row["category_score"]) / float(row["target"]),
        axis=1,
    )
    strongest_category = categories.sort_values(["score_pct", "category_score"], ascending=[False, False]).iloc[0]
    weakest_category = categories.sort_values(["score_pct", "category_score"], ascending=[True, True]).iloc[0]

    available = data.metric_scores.loc[data.metric_scores["metric_available"]].copy()
    strongest_metric_text = "Metric-level score transparency is limited."
    weakest_metric_text = ""
    if not available.empty:
        available["score_capture"] = available["metric_score"] / available["metric_weight"]
        strongest_metric = available.sort_values(["score_capture", "metric_weight"], ascending=[False, False]).iloc[0]
        weakest_metric = available.sort_values(["score_capture", "metric_weight"], ascending=[True, False]).iloc[0]
        strongest_metric_text = (
            f"The strongest metric contributor is {str(strongest_metric['metric_name']).lower()} at "
            f"{_format_metric_raw_value(str(strongest_metric['metric_id']), strongest_metric['raw_value'])} "
            f"with {float(strongest_metric['metric_score']):.2f} points captured."
        )
        weakest_metric_text = (
            f" The clearest drag is {str(weakest_metric['metric_name']).lower()} at "
            f"{_format_metric_raw_value(str(weakest_metric['metric_id']), weakest_metric['raw_value'])}."
        )

    total_score = float(data.total_score or 0.0)
    strongest_contribution = (
        None
        if total_score <= 0
        else float(strongest_category["category_score"]) / total_score
    )
    balance_text = (
        "The profile is relatively balanced across categories."
        if strongest_contribution is None or strongest_contribution < 0.45
        else f"The score is somewhat concentrated in {str(strongest_category['category']).lower()}."
    )

    return (
        f"{company_label} earns {total_score:.1f} / {data.total_target:.0f}, with "
        f"{str(strongest_category['category']).lower()} contributing the most and "
        f"{str(weakest_category['category']).lower()} contributing the least. "
        f"{strongest_metric_text}{weakest_metric_text} {balance_text}"
    ).strip()


def build_score_decomposition_category_table(data: ScoreDecompositionSectionData) -> pd.DataFrame:
    """Build the standardized category contribution table for this section."""

    return data.category_contribution_table.copy()


def build_score_decomposition_metric_table(data: ScoreDecompositionSectionData) -> pd.DataFrame:
    """Build the standardized metric score table for this section."""

    return data.metric_score_table.copy()


def build_score_decomposition_strip_table(data: ScoreDecompositionSectionData) -> pd.DataFrame:
    """Build the compact score decomposition strip for this section."""

    return data.summary_strip.copy()


def build_score_decomposition_text_from_data(data: ScoreDecompositionSectionData) -> str:
    """Build the reusable short interpretation for this section."""

    return data.interpretation_text


def build_score_decomposition_section_data_from_packet(
    packet: CompanyResearchPacket,
) -> ScoreDecompositionSectionData:
    """Build standardized Score Decomposition outputs from the shared research packet."""

    score_summary = packet.scorecard.summary
    category_scores = packet.scorecard.category_scores.copy()
    metrics = packet.scorecard.metrics.copy()

    # Reuse the canonical category and metric scorecard outputs first, then add
    # only the presentation-specific fields needed for review and export.
    category_contribution_table, summary_strip = _build_category_contribution_table(
        score_summary,
        category_scores,
    )
    if category_scores.empty:
        category_scores = _category_from_summary(score_summary)
    if "target" not in category_scores.columns and "category" in category_scores.columns:
        category_scores["target"] = category_scores["category"].map(CATEGORY_TARGET_WEIGHTS)
    metric_score_table = _build_metric_score_table(metrics, category_scores)

    data = ScoreDecompositionSectionData(
        ticker=packet.ticker,
        company_name=packet.company_name,
        sector=packet.fundamentals_summary.get("sector"),
        industry=packet.fundamentals_summary.get("industry"),
        report_date=score_summary.get("score_date") or packet.as_of_date,
        total_score=score_summary.get("total_score"),
        total_target=float(sum(CATEGORY_TARGET_WEIGHTS.values())),
        confidence_score=score_summary.get("confidence_score"),
        category_scores=category_scores,
        metric_scores=metrics,
        category_contribution_table=category_contribution_table,
        metric_score_table=metric_score_table,
        summary_strip=summary_strip,
        interpretation_text="",
    )
    data.interpretation_text = _build_interpretation_text(data)
    return data


def build_score_decomposition_section_data(
    ticker: str,
    *,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> ScoreDecompositionSectionData:
    """Build the Score Decomposition section from canonical shared inputs."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    prices = load_staged_prices(settings=settings)
    fundamentals = load_staged_fundamentals(settings=settings)
    security_master = load_security_master(settings=settings)
    benchmark_mappings = load_benchmark_mappings(settings=settings)
    benchmark_set = load_benchmark_set(settings=settings, set_id=benchmark_set_id)
    benchmark_prices = load_benchmark_prices(settings=settings)
    peer_sets = load_peer_sets(settings=settings)

    company_history = prepare_price_history(prices, normalized_ticker)
    if company_history.empty:
        raise ValueError(f"no staged price history found for ticker {normalized_ticker}")

    fundamentals_summary = build_fundamentals_summary(fundamentals, normalized_ticker)
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
    security_row = security_rows.iloc[-1].to_dict() if not security_rows.empty else {}
    benchmark_mapping_row = benchmark_mapping_rows.iloc[-1].to_dict() if not benchmark_mapping_rows.empty else {}

    benchmark_histories = {
        benchmark_ticker: prepare_price_history(benchmark_prices, benchmark_ticker)
        for benchmark_ticker in benchmark_set.get("ticker", pd.Series(dtype=str)).tolist()
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

    benchmark_comparison = build_benchmark_comparison(company_history, benchmark_histories)
    risk_metrics = calculate_risk_metrics(company_history, benchmark_history=primary_benchmark_history)
    trend_metrics = calculate_trend_metrics(company_history)

    peer_rows = (
        peer_sets.loc[peer_sets["ticker"] == normalized_ticker].copy()
        if "ticker" in peer_sets.columns
        else pd.DataFrame()
    )
    peer_valuation_comparison = build_peer_valuation_comparison(
        normalized_ticker,
        peer_rows,
        fundamentals,
    )
    peer_summary_stats = build_peer_summary_stats(peer_valuation_comparison)

    scorecard = build_company_scorecard(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        sector=fundamentals_summary.get("sector") or security_row.get("sector"),
        industry=fundamentals_summary.get("industry") or security_row.get("industry"),
        score_date=fundamentals_summary.get("as_of_date"),
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

    packet = CompanyResearchPacket(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        as_of_date=fundamentals_summary.get("as_of_date"),
        company_history=company_history,
        benchmark_histories=benchmark_histories,
        return_windows=pd.DataFrame(),
        trailing_return_comparison=pd.DataFrame(),
        risk_metrics=risk_metrics,
        trend_metrics=trend_metrics,
        benchmark_comparison=benchmark_comparison,
        fundamentals_summary=fundamentals_summary,
        peer_return_comparison=pd.DataFrame(),
        peer_return_summary_stats={},
        peer_valuation_comparison=peer_valuation_comparison,
        peer_summary_stats=peer_summary_stats,
        sector_snapshot={},
        scorecard=scorecard,
    )
    return build_score_decomposition_section_data_from_packet(packet)


def render_score_decomposition_section(data: ScoreDecompositionSectionData) -> Figure:
    """Render the full Score Decomposition section as one reviewable figure."""

    fig, axes = plt.subplots(
        nrows=5,
        ncols=1,
        figsize=(13.5, 19.0),
        gridspec_kw={"height_ratios": [0.9, 1.5, 2.1, 4.1, 0.9]},
    )
    header_ax, strip_ax, category_ax, metric_ax, text_ax = axes

    header_ax.axis("off")
    header_ax.set_title(f"{data.ticker} Score Decomposition", loc="left", fontsize=14, fontweight="bold")
    header_rows = pd.DataFrame(
        [
            ("Company", data.company_name or data.ticker),
            ("Sector", data.sector or "N/A"),
            ("Industry", data.industry or "N/A"),
            ("Report Date", data.report_date or "N/A"),
            ("Total Score", f"{(data.total_score or 0.0):.1f} / {data.total_target:.0f}"),
            ("Confidence", _format_percent(data.confidence_score)),
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

    strip = data.summary_strip.copy()
    if strip.empty:
        strip_ax.text(0.5, 0.5, "No category contribution strip available", ha="center", va="center")
        strip_ax.axis("off")
    else:
        y_positions = list(range(len(strip)))
        strip_ax.barh(y_positions, strip["max_score"], color="#e2e8f0", height=0.55)
        strip_ax.barh(y_positions, strip["score"], color="#2563eb", height=0.55)
        strip_ax.set_yticks(y_positions, strip["signal"])
        strip_ax.invert_yaxis()
        strip_ax.set_xlim(0, max(float(strip["max_score"].max()), 1.0))
        strip_ax.set_xlabel("Points Contributed")
        strip_ax.set_title("Score Contribution Chart", loc="left")
        strip_ax.grid(axis="x", linestyle=":", alpha=0.35)
        for y_position, score, score_display, contribution_display in zip(
            y_positions,
            strip["score"],
            strip["score_display"],
            strip["contribution_display"],
            strict=False,
        ):
            strip_ax.text(
                min(float(score) + 0.5, float(strip["max_score"].max()) + 0.2),
                y_position,
                f"{score_display} | {contribution_display}",
                va="center",
                ha="left",
                fontsize=8.5,
            )

    category_ax.axis("off")
    category_ax.set_title("Category Contribution Table", loc="left")
    if data.category_contribution_table.empty:
        category_ax.text(0.5, 0.5, "No category contribution table available", ha="center", va="center")
    else:
        rendered_table = category_ax.table(
            cellText=data.category_contribution_table.values,
            colLabels=data.category_contribution_table.columns,
            cellLoc="left",
            colLoc="left",
            bbox=[0.0, 0.0, 1.0, 0.97],
        )
        rendered_table.auto_set_font_size(False)
        rendered_table.set_fontsize(8.0)
        for (row, col), cell in rendered_table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight="bold")
                cell.set_facecolor("#e2e8f0")
            elif col == 0:
                cell.set_text_props(weight="bold")

    metric_ax.axis("off")
    metric_ax.set_title("Metric Score Heatmap Table", loc="left")
    if data.metric_score_table.empty:
        metric_ax.text(0.5, 0.5, "No metric score table available", ha="center", va="center")
    else:
        display_table = data.metric_score_table[
            ["Category", "Metric", "Raw Value", "Score Bucket", "Metric Score", "Metric Weight", "Confidence Flag"]
        ].copy()
        for column, width in {"Metric": 26, "Category": 18}.items():
            display_table[column] = display_table[column].map(
                lambda value: textwrap.fill(str(value), width=width) if pd.notna(value) and str(value) else ""
            )
        rendered_table = metric_ax.table(
            cellText=display_table.values,
            colLabels=display_table.columns,
            cellLoc="left",
            colLoc="left",
            bbox=[0.0, 0.0, 1.0, 0.98],
        )
        rendered_table.auto_set_font_size(False)
        rendered_table.set_fontsize(7.2)
        bucket_col = display_table.columns.get_loc("Score Bucket")
        for (row, col), cell in rendered_table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight="bold")
                cell.set_facecolor("#e2e8f0")
                continue
            if col == 0:
                cell.set_text_props(weight="bold")
            if col == bucket_col:
                bucket = str(display_table.iloc[row - 1, bucket_col])
                bucket_color = {
                    "Strong": "#bbf7d0",
                    "Supportive": "#d9f99d",
                    "Mixed": "#fde68a",
                    "Constraint": "#fdba74",
                    "Weak": "#fecaca",
                    "Unavailable": "#e5e7eb",
                }.get(bucket, "#e5e7eb")
                cell.set_facecolor(bucket_color)
            cell.get_text().set_wrap(True)

    text_ax.axis("off")
    text_ax.set_title("Short Interpretation", loc="left")
    text_ax.text(0.0, 0.95, data.interpretation_text, va="top", ha="left", wrap=True, fontsize=10)

    fig.tight_layout()
    return fig


def save_score_decomposition_section(
    ticker: str,
    *,
    output_dir: str | Path,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Save Score Decomposition review artifacts for one ticker."""

    section_data = build_score_decomposition_section_data(
        ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    artifact_stem = f"{section_data.ticker.lower()}_score_decomposition"
    figure_path = destination / f"{artifact_stem}.png"
    category_table_path = destination / f"{artifact_stem}_categories.csv"
    metric_table_path = destination / f"{artifact_stem}_metrics.csv"
    strip_path = destination / f"{artifact_stem}_strip.csv"
    text_path = destination / f"{artifact_stem}_summary.md"

    figure = render_score_decomposition_section(section_data)
    figure.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(figure)

    section_data.category_contribution_table.to_csv(category_table_path, index=False)
    section_data.metric_score_table.to_csv(metric_table_path, index=False)
    section_data.summary_strip.to_csv(strip_path, index=False)
    text_path.write_text(section_data.interpretation_text + "\n")

    return {
        "figure_path": figure_path,
        "category_table_path": category_table_path,
        "metric_table_path": metric_table_path,
        "strip_path": strip_path,
        "text_path": text_path,
    }


__all__ = [
    "ScoreDecompositionSectionData",
    "build_score_decomposition_category_table",
    "build_score_decomposition_metric_table",
    "build_score_decomposition_section_data",
    "build_score_decomposition_section_data_from_packet",
    "build_score_decomposition_strip_table",
    "build_score_decomposition_text_from_data",
    "render_score_decomposition_section",
    "save_score_decomposition_section",
]
