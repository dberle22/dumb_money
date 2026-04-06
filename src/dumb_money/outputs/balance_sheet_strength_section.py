"""DuckDB-aware Balance Sheet Strength section builders and renderers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import textwrap

from dumb_money import _matplotlib  # noqa: F401
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from dumb_money.analytics.scorecard import build_company_scorecard
from dumb_money.config import AppSettings, get_settings
from dumb_money.research.company import (
    CompanyResearchPacket,
    build_fundamentals_summary_from_mart_row,
    load_gold_ticker_metrics_row,
    load_security_master,
    load_staged_fundamentals,
)

SUMMARY_COLUMNS = ["Metric", "Value", "Score Contribution", "Assessment", "Shared Input", "Notes"]
STRIP_COLUMNS = ["signal", "score", "max_score", "score_pct", "score_display", "value_display", "assessment"]


@dataclass(slots=True)
class BalanceSheetStrengthSectionData:
    """Query-ready Balance Sheet Strength inputs and standardized outputs."""

    ticker: str
    company_name: str | None
    report_date: str | None
    balance_sheet_score: float | None
    balance_sheet_target: float | None
    fundamentals_summary: dict[str, object]
    balance_metrics: pd.DataFrame
    summary_table: pd.DataFrame
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


def _format_billions(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value / 1_000_000_000:.{digits}f}B"


def _format_metric_raw_value(metric_id: str, value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"

    if metric_id in {"net_debt_to_ebitda", "current_ratio", "free_cash_flow_to_debt", "cash_to_debt"}:
        return _format_ratio(value)
    if metric_id == "debt_to_equity":
        return _format_number(value)
    return _format_number(value, digits=4)


def _flag_from_normalized_value(normalized_value: float | None) -> str:
    if normalized_value is None or pd.isna(normalized_value):
        return "Unavailable"
    if float(normalized_value) >= 0.95:
        return "Clear strength"
    if float(normalized_value) >= 0.70:
        return "Supportive"
    if float(normalized_value) >= 0.45:
        return "Mixed"
    if float(normalized_value) > 0:
        return "Constraint"
    return "Weak"


def _shared_input_label(metric_id: str) -> str:
    return {
        "net_debt_to_ebitda": "fundamentals_summary.total_debt / total_cash / ebitda",
        "current_ratio": "fundamentals_summary.current_ratio",
        "debt_to_equity": "fundamentals_summary.debt_to_equity",
        "free_cash_flow_to_debt": "fundamentals_summary.free_cash_flow / total_debt",
        "cash_to_debt": "fundamentals_summary.total_cash / total_debt",
    }.get(metric_id, "scorecard.metrics")


def _build_interpretation_text(data: BalanceSheetStrengthSectionData) -> str:
    company_label = data.company_name or data.ticker
    score = data.balance_sheet_score
    score_target = data.balance_sheet_target

    if score is None or score_target is None or not score_target:
        score_text = "with unavailable balance-sheet scoring coverage"
    else:
        score_pct = float(score) / float(score_target)
        if score_pct >= 0.80:
            score_text = "with clearly supportive balance-sheet quality"
        elif score_pct >= 0.60:
            score_text = "with a generally supportive balance-sheet profile"
        elif score_pct >= 0.40:
            score_text = "with a mixed balance-sheet profile"
        else:
            score_text = "with a more constrained balance-sheet profile"

    available = data.balance_metrics.loc[data.balance_metrics["metric_available"]].copy()
    strongest_metric = None
    weakest_metric = None
    if not available.empty:
        available["score_capture"] = available["metric_score"] / available["metric_weight"]
        strongest_metric = available.sort_values(["score_capture", "metric_weight"], ascending=[False, False]).iloc[0]
        weakest_metric = available.sort_values(["score_capture", "metric_weight"], ascending=[True, False]).iloc[0]

    unavailable = data.balance_metrics.loc[~data.balance_metrics["metric_available"]].copy()
    unavailable_notes: list[str] = []
    if "metric_id" in unavailable.columns:
        unavailable_metric_ids = set(unavailable["metric_id"].astype(str))
        if "net_debt_to_ebitda" in unavailable_metric_ids:
            unavailable_notes.append("Net debt to EBITDA is unavailable because EBITDA coverage is missing or non-positive.")

    total_debt = data.fundamentals_summary.get("total_debt")
    if total_debt is not None and not pd.isna(total_debt) and float(total_debt) <= 0:
        unavailable_notes.append("The company currently screens as debt-free in the latest fundamentals snapshot.")

    strength_text = (
        f"The strongest current support is {strongest_metric['metric_name'].lower()} at "
        f"{_format_metric_raw_value(str(strongest_metric['metric_id']), strongest_metric['raw_value'])}."
        if strongest_metric is not None
        else "Available balance-sheet support is limited by missing metric coverage."
    )
    watch_text = (
        f"The main watch item is {weakest_metric['metric_name'].lower()} at "
        f"{_format_metric_raw_value(str(weakest_metric['metric_id']), weakest_metric['raw_value'])}."
        if weakest_metric is not None
        else ""
    )
    missing_text = f" {' '.join(unavailable_notes)}" if unavailable_notes else ""

    return f"{company_label} screens {score_text}. {strength_text} {watch_text}{missing_text}".strip()


def build_balance_sheet_strength_table(data: BalanceSheetStrengthSectionData) -> pd.DataFrame:
    """Build the standardized Balance Sheet Strength summary table."""

    return data.summary_table.copy()


def build_balance_sheet_strength_strip_table(data: BalanceSheetStrengthSectionData) -> pd.DataFrame:
    """Build the compact Balance Sheet Strength summary strip."""

    return data.summary_strip.copy()


def build_balance_sheet_strength_text_from_data(data: BalanceSheetStrengthSectionData) -> str:
    """Build the reusable short interpretation for this section."""

    return data.interpretation_text


def build_balance_sheet_strength_section_data_from_packet(
    packet: CompanyResearchPacket,
) -> BalanceSheetStrengthSectionData:
    """Build standardized Balance Sheet Strength outputs from the shared research packet."""

    metrics = packet.scorecard.metrics.copy()
    balance_metrics = metrics.loc[metrics["category"] == "Balance Sheet Strength"].copy()

    if balance_metrics.empty:
        summary_table = pd.DataFrame(columns=SUMMARY_COLUMNS)
        summary_strip = pd.DataFrame(columns=STRIP_COLUMNS)
    else:
        # This section intentionally reuses the scorecard's shared leverage and
        # liquidity metrics instead of recalculating point-in-time balance-sheet math here.
        balance_metrics["Value"] = balance_metrics.apply(
            lambda row: _format_metric_raw_value(str(row["metric_id"]), row["raw_value"]),
            axis=1,
        )
        balance_metrics["Score Contribution"] = balance_metrics.apply(
            lambda row: f"{row['metric_score']:.2f} / {row['metric_weight']:.0f}",
            axis=1,
        )
        balance_metrics["Assessment"] = balance_metrics["normalized_value"].map(_flag_from_normalized_value)
        balance_metrics["Shared Input"] = balance_metrics["metric_id"].map(_shared_input_label)
        balance_metrics["Notes"] = balance_metrics["notes"].fillna("")

        summary_table = balance_metrics[
            ["metric_name", "Value", "Score Contribution", "Assessment", "Shared Input", "Notes"]
        ].rename(columns={"metric_name": "Metric"})

        summary_strip = pd.DataFrame(
            [
                {
                    "signal": row.metric_name,
                    "score": float(row.metric_score),
                    "max_score": float(row.metric_weight),
                    "score_pct": (
                        float(row.metric_score) / float(row.metric_weight)
                        if row.metric_weight and not pd.isna(row.metric_score)
                        else None
                    ),
                    "score_display": f"{row.metric_score:.2f} / {row.metric_weight:.0f}",
                    "value_display": _format_metric_raw_value(str(row.metric_id), row.raw_value),
                    "assessment": _flag_from_normalized_value(row.normalized_value),
                }
                for row in balance_metrics.itertuples()
            ]
        )

    score_summary = packet.scorecard.summary
    data = BalanceSheetStrengthSectionData(
        ticker=packet.ticker,
        company_name=packet.company_name,
        report_date=score_summary.get("score_date") or packet.as_of_date,
        balance_sheet_score=score_summary.get("balance_sheet_score"),
        balance_sheet_target=score_summary.get("balance_sheet_available_weight") or 25.0,
        fundamentals_summary=packet.fundamentals_summary,
        balance_metrics=balance_metrics,
        summary_table=summary_table,
        summary_strip=summary_strip,
        interpretation_text="",
    )
    data.interpretation_text = _build_interpretation_text(data)
    return data


def build_balance_sheet_strength_section_data(
    ticker: str,
    *,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> BalanceSheetStrengthSectionData:
    """Build the Balance Sheet Strength section from canonical shared inputs."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()
    mart_row = load_gold_ticker_metrics_row(normalized_ticker, settings=settings)

    fundamentals_summary = build_fundamentals_summary_from_mart_row(mart_row)
    if not fundamentals_summary:
        fundamentals = load_staged_fundamentals(settings=settings)
        from dumb_money.analytics.company import build_fundamentals_summary

        fundamentals_summary = build_fundamentals_summary(fundamentals, normalized_ticker)
    security_master = load_security_master(settings=settings)
    security_rows = (
        security_master.loc[security_master["ticker"] == normalized_ticker].copy()
        if "ticker" in security_master.columns
        else pd.DataFrame()
    )
    security_row = security_rows.iloc[-1].to_dict() if not security_rows.empty else {}

    # This section only needs canonical fundamentals plus shared scorecard balance-sheet
    # formulas, so it stays lighter than the full company research packet path.
    scorecard = build_company_scorecard(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        sector=fundamentals_summary.get("sector") or security_row.get("sector"),
        industry=fundamentals_summary.get("industry") or security_row.get("industry"),
        score_date=mart_row.get("score_date") or fundamentals_summary.get("as_of_date"),
        benchmark_comparison=pd.DataFrame(),
        risk_metrics={},
        trend_metrics={},
        fundamentals_summary=fundamentals_summary,
        peer_valuation_comparison=pd.DataFrame(),
    )
    packet = CompanyResearchPacket(
        ticker=normalized_ticker,
        company_name=mart_row.get("company_name") or fundamentals_summary.get("long_name"),
        as_of_date=fundamentals_summary.get("as_of_date"),
        company_history=pd.DataFrame(),
        benchmark_histories={},
        return_windows=pd.DataFrame(),
        trailing_return_comparison=pd.DataFrame(),
        risk_metrics={},
        trend_metrics={},
        benchmark_comparison=pd.DataFrame(),
        fundamentals_summary=fundamentals_summary,
        peer_return_comparison=pd.DataFrame(),
        peer_return_summary_stats={},
        peer_valuation_comparison=pd.DataFrame(),
        peer_summary_stats={},
        sector_snapshot={},
        scorecard=scorecard,
    )
    return build_balance_sheet_strength_section_data_from_packet(packet)


def render_balance_sheet_strength_section(data: BalanceSheetStrengthSectionData) -> Figure:
    """Render the full Balance Sheet Strength section as one reviewable figure."""

    fig, axes = plt.subplots(
        nrows=4,
        ncols=1,
        figsize=(12.0, 15.5),
        gridspec_kw={"height_ratios": [0.9, 1.4, 3.1, 0.8]},
    )
    header_ax, strip_ax, table_ax, text_ax = axes

    header_ax.axis("off")
    header_ax.set_title(f"{data.ticker} Balance Sheet Strength", loc="left", fontsize=14, fontweight="bold")
    header_rows = pd.DataFrame(
        [
            ("Company", data.company_name or data.ticker),
            ("Report Date", data.report_date or "N/A"),
            ("Balance Sheet Score", f"{(data.balance_sheet_score or 0.0):.1f} / {(data.balance_sheet_target or 25.0):.0f}"),
            ("Net Cash", _format_billions(data.fundamentals_summary.get("net_cash"))),
            ("Total Debt", _format_billions(data.fundamentals_summary.get("total_debt"))),
            ("Total Cash", _format_billions(data.fundamentals_summary.get("total_cash"))),
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
        strip_ax.text(0.5, 0.5, "No balance-sheet scorecard metrics available", ha="center", va="center")
        strip_ax.axis("off")
    else:
        y_positions = list(range(len(strip)))
        strip_ax.barh(y_positions, strip["max_score"], color="#e2e8f0", height=0.55)
        strip_ax.barh(y_positions, strip["score"], color="#b45309", height=0.55)
        strip_ax.set_yticks(y_positions, strip["signal"])
        strip_ax.invert_yaxis()
        strip_ax.set_xlim(0, max(float(strip["max_score"].max()), 1.0))
        strip_ax.set_xlabel("Score Contribution")
        strip_ax.grid(axis="x", linestyle=":", alpha=0.35)
        for y_position, score, score_display, value_display in zip(
            y_positions,
            strip["score"],
            strip["score_display"],
            strip["value_display"],
            strict=False,
        ):
            strip_ax.text(
                min(float(score) + 0.08, float(strip["max_score"].max()) + 0.2),
                y_position,
                f"{score_display} | {value_display}",
                va="center",
                ha="left",
                fontsize=8.5,
            )
    strip_ax.set_title("Metric Score Strip", loc="left")

    table_ax.axis("off")
    table_ax.set_title("Balance Sheet Scorecard Table", loc="left")
    if data.summary_table.empty:
        table_ax.text(0.5, 0.5, "No balance-sheet table available", ha="center", va="center")
    else:
        display_table = data.summary_table.copy()
        for column, width in {"Metric": 20, "Shared Input": 28, "Notes": 44}.items():
            if column in display_table.columns:
                display_table[column] = display_table[column].map(
                    lambda value: textwrap.fill(str(value), width=width) if pd.notna(value) and str(value) else ""
                )
        col_widths = [0.18, 0.10, 0.12, 0.12, 0.18, 0.30]
        rendered_table = table_ax.table(
            cellText=display_table.values,
            colLabels=display_table.columns,
            cellLoc="left",
            colLoc="left",
            colWidths=col_widths,
            bbox=[0.0, 0.0, 1.0, 0.95],
        )
        rendered_table.auto_set_font_size(False)
        rendered_table.set_fontsize(8.0)

        line_counts: list[int] = []
        for row in display_table.itertuples(index=False):
            values = [str(value) if value is not None else "" for value in row]
            line_counts.append(max((value.count("\n") + 1 for value in values), default=1))

        header_height = 0.075
        line_height = 0.060
        for (row, col), cell in rendered_table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight="bold")
                cell.set_facecolor("#e2e8f0")
                cell.set_height(header_height)
            elif col == 0:
                cell.set_text_props(weight="bold")
            if row > 0:
                row_height = line_height * line_counts[row - 1]
                cell.set_height(row_height)
            cell.get_text().set_wrap(True)

    text_ax.axis("off")
    text_ax.set_title("Short Interpretation", loc="left")
    text_ax.text(
        0.0,
        0.95,
        data.interpretation_text,
        va="top",
        ha="left",
        wrap=True,
        fontsize=10,
    )
    text_ax.text(
        0.0,
        0.08,
        "Peer leverage comparison remains deferred until canonical peer balance-sheet medians are modeled.",
        va="bottom",
        ha="left",
        fontsize=8.5,
        color="#475569",
    )

    fig.tight_layout()
    return fig


def save_balance_sheet_strength_section(
    ticker: str,
    *,
    output_dir: str | Path,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Save Balance Sheet Strength review artifacts for one ticker."""

    section_data = build_balance_sheet_strength_section_data(
        ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    artifact_stem = f"{section_data.ticker.lower()}_balance_sheet_strength"
    figure_path = destination / f"{artifact_stem}.png"
    table_path = destination / f"{artifact_stem}_table.csv"
    strip_path = destination / f"{artifact_stem}_strip.csv"
    text_path = destination / f"{artifact_stem}_summary.md"

    figure = render_balance_sheet_strength_section(section_data)
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
    "BalanceSheetStrengthSectionData",
    "build_balance_sheet_strength_section_data",
    "build_balance_sheet_strength_section_data_from_packet",
    "build_balance_sheet_strength_strip_table",
    "build_balance_sheet_strength_table",
    "build_balance_sheet_strength_text_from_data",
    "render_balance_sheet_strength_section",
    "save_balance_sheet_strength_section",
]
