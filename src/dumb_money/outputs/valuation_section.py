"""DuckDB-aware Valuation section builders and renderers."""

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
    build_peer_summary_stats,
    build_peer_valuation_comparison,
)
from dumb_money.analytics.scorecard import build_company_scorecard
from dumb_money.config import AppSettings, get_settings
from dumb_money.research.company import (
    CompanyResearchPacket,
    load_peer_sets,
    load_security_master,
    load_staged_fundamentals,
)

SUMMARY_COLUMNS = [
    "Metric",
    "Value",
    "Peer Median",
    "Relative vs Peer",
    "Score Contribution",
    "Assessment",
    "Shared Input",
    "Notes",
]
STRIP_COLUMNS = [
    "signal",
    "score",
    "max_score",
    "score_pct",
    "score_display",
    "value_display",
    "peer_display",
    "assessment",
]

PEER_MEDIAN_STAT_MAP = {
    "forward_pe": "median_forward_pe",
    "ev_to_ebitda": "median_ev_to_ebitda",
    "price_to_sales": "median_price_to_sales",
    "yield_metric": "median_free_cash_flow_yield",
}
LOWER_IS_BETTER_METRICS = {"forward_pe", "ev_to_ebitda", "price_to_sales"}
RATIO_TO_PEER_LABELS = {
    "forward_pe": "Forward P/E",
    "ev_to_ebitda": "EV/EBITDA",
    "price_to_sales": "Price/Sales",
    "yield_metric": "FCF Yield",
}


@dataclass(slots=True)
class ValuationSectionData:
    """Query-ready Valuation inputs and standardized outputs."""

    ticker: str
    company_name: str | None
    report_date: str | None
    valuation_score: float | None
    valuation_target: float | None
    fundamentals_summary: dict[str, object]
    valuation_metrics: pd.DataFrame
    peer_valuation_comparison: pd.DataFrame
    peer_summary_stats: dict[str, object]
    summary_table: pd.DataFrame
    summary_strip: pd.DataFrame
    peer_comparison_table: pd.DataFrame
    interpretation_text: str


def _format_percent(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}%}"


def _format_ratio(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}f}x"


def _format_metric_raw_value(metric_id: str, value: float | None) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    if metric_id == "yield_metric":
        return _format_percent(value)
    return _format_ratio(value)


def _flag_from_normalized_value(normalized_value: float | None) -> str:
    if normalized_value is None or pd.isna(normalized_value):
        return "Unavailable"
    if float(normalized_value) >= 0.95:
        return "Clear support"
    if float(normalized_value) >= 0.70:
        return "Supportive"
    if float(normalized_value) >= 0.45:
        return "Mixed"
    if float(normalized_value) > 0:
        return "Constraint"
    return "Weak"


def _shared_input_label(metric_id: str, notes: str) -> str:
    if metric_id == "forward_pe":
        return "fundamentals_summary.forward_pe + peer_valuation_comparison.forward_pe"
    if metric_id == "ev_to_ebitda":
        return "fundamentals_summary.ev_to_ebitda + peer_valuation_comparison.ev_to_ebitda"
    if metric_id == "price_to_sales":
        return "fundamentals_summary.price_to_sales + peer_valuation_comparison.price_to_sales"
    if "dividend yield because free cash flow yield was unavailable" in notes.lower():
        return "fundamentals_summary.dividend_yield"
    if "free cash flow yield" in notes.lower():
        return "fundamentals_summary.free_cash_flow / market_cap + peer_valuation_comparison.free_cash_flow_yield"
    return "fundamentals_summary.dividend_yield"


def _resolve_peer_median(metric_id: str, notes: str, peer_summary_stats: dict[str, object]) -> float | None:
    if metric_id == "yield_metric" and "dividend yield because free cash flow yield was unavailable" in notes.lower():
        return None
    stat_key = PEER_MEDIAN_STAT_MAP.get(metric_id)
    if stat_key is None:
        return None
    value = peer_summary_stats.get(stat_key)
    if value is None or pd.isna(value):
        return None
    return float(value)


def _format_relative_context(metric_id: str, raw_value: float | None, peer_median: float | None) -> str:
    if raw_value is None or pd.isna(raw_value) or peer_median is None or peer_median == 0:
        return "N/A"

    relative = (float(raw_value) / float(peer_median)) - 1
    if metric_id in LOWER_IS_BETTER_METRICS:
        if relative <= -0.01:
            return f"{abs(relative):.1%} discount"
        if relative >= 0.01:
            return f"{relative:.1%} premium"
        return "Near peer median"

    if relative >= 0.01:
        return f"{relative:.1%} above peer median"
    if relative <= -0.01:
        return f"{abs(relative):.1%} below peer median"
    return "Near peer median"


def _build_peer_comparison_table(peer_valuation: pd.DataFrame) -> pd.DataFrame:
    if peer_valuation.empty:
        return pd.DataFrame()

    display = peer_valuation.copy()
    for column, default in {
        "peer_source": "unknown",
        "relationship_type": "unknown",
        "selection_method": "unknown",
    }.items():
        if column not in display.columns:
            display[column] = default
    display["Role"] = display["is_focal_company"].map(lambda value: "Company" if value else "Peer")
    display["Forward P/E"] = display["forward_pe"].map(_format_ratio)
    display["EV/EBITDA"] = display["ev_to_ebitda"].map(_format_ratio)
    display["Price/Sales"] = display["price_to_sales"].map(_format_ratio)
    display["FCF Yield"] = display["free_cash_flow_yield"].map(_format_percent)
    return display[
        [
            "Role",
            "ticker",
            "company_name",
            "peer_source",
            "relationship_type",
            "selection_method",
            "Forward P/E",
            "EV/EBITDA",
            "Price/Sales",
            "FCF Yield",
        ]
    ].rename(
        columns={
            "ticker": "Ticker",
            "company_name": "Company",
            "peer_source": "Peer Source",
            "relationship_type": "Relationship",
            "selection_method": "Selection Method",
        }
    )


def _build_interpretation_text(data: ValuationSectionData) -> str:
    company_label = data.company_name or data.ticker
    score = data.valuation_score
    score_target = data.valuation_target

    if score is None or score_target is None or not score_target:
        score_text = "with unavailable valuation scoring coverage"
    else:
        score_pct = float(score) / float(score_target)
        if score_pct >= 0.80:
            score_text = "with clearly supportive valuation"
        elif score_pct >= 0.60:
            score_text = "with a generally fair valuation setup"
        elif score_pct >= 0.40:
            score_text = "with mixed valuation support"
        else:
            score_text = "with valuation acting as a clear constraint"

    available = data.valuation_metrics.loc[data.valuation_metrics["metric_available"]].copy()
    strongest_metric = None
    weakest_metric = None
    if not available.empty:
        available["score_capture"] = available["metric_score"] / available["metric_weight"]
        strongest_metric = available.sort_values(["score_capture", "metric_weight"], ascending=[False, False]).iloc[0]
        weakest_metric = available.sort_values(["score_capture", "metric_weight"], ascending=[True, False]).iloc[0]

    strength_text = "Shared valuation coverage is limited in the current packet."
    if strongest_metric is not None:
        peer_median = _resolve_peer_median(
            str(strongest_metric["metric_id"]),
            str(strongest_metric["notes"] or ""),
            data.peer_summary_stats,
        )
        peer_context = _format_relative_context(
            str(strongest_metric["metric_id"]),
            strongest_metric["raw_value"],
            peer_median,
        )
        strength_text = (
            f"The clearest valuation support is {str(strongest_metric['metric_name']).lower()} at "
            f"{_format_metric_raw_value(str(strongest_metric['metric_id']), strongest_metric['raw_value'])}"
            f"{'' if peer_context == 'N/A' else f' ({peer_context})'}."
        )

    watch_text = ""
    if weakest_metric is not None:
        peer_median = _resolve_peer_median(
            str(weakest_metric["metric_id"]),
            str(weakest_metric["notes"] or ""),
            data.peer_summary_stats,
        )
        peer_context = _format_relative_context(
            str(weakest_metric["metric_id"]),
            weakest_metric["raw_value"],
            peer_median,
        )
        watch_text = (
            f" The main watch item is {str(weakest_metric['metric_name']).lower()} at "
            f"{_format_metric_raw_value(str(weakest_metric['metric_id']), weakest_metric['raw_value'])}"
            f"{'' if peer_context == 'N/A' else f' ({peer_context})'}."
        )

    peer_count = int(data.peer_summary_stats.get("peer_count") or 0)
    peer_text = (
        f" Peer valuation context currently covers {peer_count} canonical peers."
        if peer_count > 0
        else " Peer-relative valuation medians are not yet available for this name."
    )
    return f"{company_label} screens {score_text}. {strength_text}{watch_text}{peer_text}".strip()


def build_valuation_summary_table(data: ValuationSectionData) -> pd.DataFrame:
    """Build the standardized Valuation summary table."""

    return data.summary_table.copy()


def build_valuation_strip_table(data: ValuationSectionData) -> pd.DataFrame:
    """Build the compact Valuation score strip."""

    return data.summary_strip.copy()


def build_valuation_peer_comparison_table(data: ValuationSectionData) -> pd.DataFrame:
    """Build the report-friendly peer valuation comparison table."""

    return data.peer_comparison_table.copy()


def build_valuation_text_from_data(data: ValuationSectionData) -> str:
    """Build the reusable short interpretation for this section."""

    return data.interpretation_text


def build_valuation_section_data_from_packet(packet: CompanyResearchPacket) -> ValuationSectionData:
    """Build standardized Valuation outputs from the shared research packet."""

    metrics = packet.scorecard.metrics.copy()
    valuation_metrics = metrics.loc[metrics["category"] == "Valuation"].copy()
    peer_summary_stats = packet.peer_summary_stats or build_peer_summary_stats(packet.peer_valuation_comparison)
    peer_comparison_table = _build_peer_comparison_table(packet.peer_valuation_comparison)

    if valuation_metrics.empty:
        summary_table = pd.DataFrame(columns=SUMMARY_COLUMNS)
        summary_strip = pd.DataFrame(columns=STRIP_COLUMNS)
    else:
        # The section deliberately reuses the scorecard's valuation metrics and the
        # shared peer valuation panel instead of recalculating multiples here.
        valuation_metrics["peer_median_raw"] = valuation_metrics.apply(
            lambda row: _resolve_peer_median(
                str(row["metric_id"]),
                str(row["notes"] or ""),
                peer_summary_stats,
            ),
            axis=1,
        )
        valuation_metrics["Value"] = valuation_metrics.apply(
            lambda row: _format_metric_raw_value(str(row["metric_id"]), row["raw_value"]),
            axis=1,
        )
        valuation_metrics["Peer Median"] = valuation_metrics.apply(
            lambda row: _format_metric_raw_value(str(row["metric_id"]), row["peer_median_raw"]),
            axis=1,
        )
        valuation_metrics["Relative vs Peer"] = valuation_metrics.apply(
            lambda row: _format_relative_context(
                str(row["metric_id"]),
                row["raw_value"],
                row["peer_median_raw"],
            ),
            axis=1,
        )
        valuation_metrics["Score Contribution"] = valuation_metrics.apply(
            lambda row: f"{row['metric_score']:.2f} / {row['metric_weight']:.0f}",
            axis=1,
        )
        valuation_metrics["Assessment"] = valuation_metrics["normalized_value"].map(_flag_from_normalized_value)
        valuation_metrics["Shared Input"] = valuation_metrics.apply(
            lambda row: _shared_input_label(str(row["metric_id"]), str(row["notes"] or "")),
            axis=1,
        )
        valuation_metrics["Notes"] = valuation_metrics["notes"].fillna("")

        summary_table = valuation_metrics[
            [
                "metric_name",
                "Value",
                "Peer Median",
                "Relative vs Peer",
                "Score Contribution",
                "Assessment",
                "Shared Input",
                "Notes",
            ]
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
                    "peer_display": _format_metric_raw_value(str(row.metric_id), row.peer_median_raw),
                    "assessment": _flag_from_normalized_value(row.normalized_value),
                }
                for row in valuation_metrics.itertuples()
            ]
        )

    score_summary = packet.scorecard.summary
    data = ValuationSectionData(
        ticker=packet.ticker,
        company_name=packet.company_name,
        report_date=score_summary.get("score_date") or packet.as_of_date,
        valuation_score=score_summary.get("valuation_score"),
        valuation_target=15.0,
        fundamentals_summary=packet.fundamentals_summary,
        valuation_metrics=valuation_metrics,
        peer_valuation_comparison=packet.peer_valuation_comparison.copy(),
        peer_summary_stats=peer_summary_stats,
        summary_table=summary_table,
        summary_strip=summary_strip,
        peer_comparison_table=peer_comparison_table,
        interpretation_text="",
    )
    data.interpretation_text = _build_interpretation_text(data)
    return data


def build_valuation_section_data(
    ticker: str,
    *,
    settings: AppSettings | None = None,
) -> ValuationSectionData:
    """Build the Valuation section from canonical shared inputs."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    fundamentals = load_staged_fundamentals(settings=settings)
    fundamentals_summary = build_fundamentals_summary(fundamentals, normalized_ticker)
    peer_sets = load_peer_sets(settings=settings)
    security_master = load_security_master(settings=settings)

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

    peer_valuation_comparison = build_peer_valuation_comparison(
        normalized_ticker,
        peer_rows,
        fundamentals,
    )
    peer_summary_stats = build_peer_summary_stats(peer_valuation_comparison)

    # This section only needs the latest fundamentals snapshot plus canonical peer
    # valuation context to reuse the shared valuation scorecard formulas.
    scorecard = build_company_scorecard(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        sector=fundamentals_summary.get("sector") or security_row.get("sector"),
        industry=fundamentals_summary.get("industry") or security_row.get("industry"),
        score_date=fundamentals_summary.get("as_of_date"),
        benchmark_comparison=pd.DataFrame(),
        risk_metrics={},
        trend_metrics={},
        fundamentals_summary=fundamentals_summary,
        peer_valuation_comparison=peer_valuation_comparison,
    )
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
        peer_return_comparison=pd.DataFrame(),
        peer_return_summary_stats={},
        peer_valuation_comparison=peer_valuation_comparison,
        peer_summary_stats=peer_summary_stats,
        sector_snapshot={},
        scorecard=scorecard,
    )
    return build_valuation_section_data_from_packet(packet)


def render_valuation_section(data: ValuationSectionData) -> Figure:
    """Render the full Valuation section as one reviewable figure."""

    fig, axes = plt.subplots(
        nrows=5,
        ncols=1,
        figsize=(12.5, 17.5),
        gridspec_kw={"height_ratios": [0.9, 1.4, 3.1, 2.1, 0.9]},
    )
    header_ax, strip_ax, table_ax, peer_ax, text_ax = axes

    header_ax.axis("off")
    header_ax.set_title(f"{data.ticker} Valuation", loc="left", fontsize=14, fontweight="bold")
    header_rows = pd.DataFrame(
        [
            ("Company", data.company_name or data.ticker),
            ("Report Date", data.report_date or "N/A"),
            ("Valuation Score", f"{(data.valuation_score or 0.0):.1f} / {(data.valuation_target or 15.0):.0f}"),
            ("Forward P/E", _format_ratio(data.fundamentals_summary.get("forward_pe"))),
            ("EV/EBITDA", _format_ratio(data.fundamentals_summary.get("ev_to_ebitda"))),
            ("Price/Sales", _format_ratio(data.fundamentals_summary.get("price_to_sales"))),
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
        strip_ax.text(0.5, 0.5, "No valuation scorecard metrics available", ha="center", va="center")
        strip_ax.axis("off")
    else:
        y_positions = list(range(len(strip)))
        strip_ax.barh(y_positions, strip["max_score"], color="#e2e8f0", height=0.55)
        strip_ax.barh(y_positions, strip["score"], color="#7c3aed", height=0.55)
        strip_ax.set_yticks(y_positions, strip["signal"])
        strip_ax.invert_yaxis()
        strip_ax.set_xlim(0, max(float(strip["max_score"].max()), 1.0))
        strip_ax.set_xlabel("Score Contribution")
        strip_ax.grid(axis="x", linestyle=":", alpha=0.35)
        for y_position, score, score_display, value_display, peer_display in zip(
            y_positions,
            strip["score"],
            strip["score_display"],
            strip["value_display"],
            strip["peer_display"],
            strict=False,
        ):
            peer_suffix = "" if peer_display == "N/A" else f" | peer {peer_display}"
            strip_ax.text(
                min(float(score) + 0.08, float(strip["max_score"].max()) + 0.2),
                y_position,
                f"{score_display} | {value_display}{peer_suffix}",
                va="center",
                ha="left",
                fontsize=8.5,
            )
    strip_ax.set_title("Metric Score Strip", loc="left")

    table_ax.axis("off")
    table_ax.set_title("Valuation Scorecard Table", loc="left")
    if data.summary_table.empty:
        table_ax.text(0.5, 0.5, "No valuation table available", ha="center", va="center")
    else:
        display_table = data.summary_table.copy()
        for column, width in {"Metric": 16, "Relative vs Peer": 18, "Shared Input": 22, "Notes": 30}.items():
            if column in display_table.columns:
                display_table[column] = display_table[column].map(
                    lambda value: textwrap.fill(str(value), width=width) if pd.notna(value) and str(value) else ""
                )
        col_widths = [0.15, 0.09, 0.09, 0.12, 0.10, 0.10, 0.14, 0.21]
        rendered_table = table_ax.table(
            cellText=display_table.values,
            colLabels=display_table.columns,
            cellLoc="left",
            colLoc="left",
            colWidths=col_widths,
            bbox=[0.0, 0.0, 1.0, 0.96],
        )
        rendered_table.auto_set_font_size(False)
        rendered_table.set_fontsize(7.4)

        line_counts: list[int] = []
        for row in display_table.itertuples(index=False):
            values = [str(value) if value is not None else "" for value in row]
            line_counts.append(max((value.count("\n") + 1 for value in values), default=1))

        header_height = 0.082
        line_height = 0.070
        for (row, col), cell in rendered_table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight="bold")
                cell.set_facecolor("#e2e8f0")
                cell.set_height(header_height)
            elif col == 0:
                cell.set_text_props(weight="bold")
            if row > 0:
                cell.set_height(line_height * line_counts[row - 1])
            cell.get_text().set_wrap(True)

    ratio_rows: list[dict[str, object]] = []
    for row in data.valuation_metrics.itertuples():
        peer_median = _resolve_peer_median(str(row.metric_id), str(row.notes or ""), data.peer_summary_stats)
        if peer_median is None or peer_median == 0 or row.raw_value is None or pd.isna(row.raw_value):
            continue
        ratio_rows.append(
            {
                "label": RATIO_TO_PEER_LABELS.get(str(row.metric_id), str(row.metric_name)),
                "metric_id": str(row.metric_id),
                "ratio_to_peer": float(row.raw_value) / float(peer_median),
            }
        )

    if not ratio_rows:
        peer_ax.text(0.5, 0.5, "Peer-relative valuation medians unavailable for this section", ha="center", va="center")
        peer_ax.axis("off")
    else:
        ratio_frame = pd.DataFrame(ratio_rows)
        y_positions = list(range(len(ratio_frame)))
        colors = [
            "#047857" if (row.metric_id == "yield_metric" and row.ratio_to_peer >= 1.0)
            or (row.metric_id in LOWER_IS_BETTER_METRICS and row.ratio_to_peer <= 1.0)
            else "#b91c1c"
            for row in ratio_frame.itertuples()
        ]
        peer_ax.axvline(1.0, color="#94a3b8", linestyle="--", linewidth=1)
        peer_ax.scatter(ratio_frame["ratio_to_peer"], y_positions, s=80, color=colors, zorder=3)
        peer_ax.set_yticks(y_positions, ratio_frame["label"])
        peer_ax.set_xlabel("Company / Peer Median")
        peer_ax.set_title("Valuation Versus Peer Median", loc="left")
        peer_ax.grid(axis="x", linestyle=":", alpha=0.35)
        x_max = max(float(ratio_frame["ratio_to_peer"].max()), 1.0)
        peer_ax.set_xlim(0, max(x_max * 1.25, 1.35))
        for row_index, row in enumerate(ratio_frame.itertuples()):
            peer_ax.text(row.ratio_to_peer + 0.03, row_index, f"{row.ratio_to_peer:.2f}x", va="center", fontsize=9)

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

    fig.tight_layout()
    return fig


def save_valuation_section(
    ticker: str,
    *,
    output_dir: str | Path,
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Save Valuation review artifacts for one ticker."""

    section_data = build_valuation_section_data(
        ticker,
        settings=settings,
    )

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    artifact_stem = f"{section_data.ticker.lower()}_valuation"
    figure_path = destination / f"{artifact_stem}.png"
    table_path = destination / f"{artifact_stem}_table.csv"
    strip_path = destination / f"{artifact_stem}_strip.csv"
    peer_table_path = destination / f"{artifact_stem}_peer_comparison.csv"
    text_path = destination / f"{artifact_stem}_summary.md"

    figure = render_valuation_section(section_data)
    figure.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(figure)

    section_data.summary_table.to_csv(table_path, index=False)
    section_data.summary_strip.to_csv(strip_path, index=False)
    section_data.peer_comparison_table.to_csv(peer_table_path, index=False)
    text_path.write_text(section_data.interpretation_text + "\n")

    return {
        "figure_path": figure_path,
        "table_path": table_path,
        "strip_path": strip_path,
        "peer_table_path": peer_table_path,
        "text_path": text_path,
    }


__all__ = [
    "ValuationSectionData",
    "build_valuation_peer_comparison_table",
    "build_valuation_section_data",
    "build_valuation_section_data_from_packet",
    "build_valuation_strip_table",
    "build_valuation_summary_table",
    "build_valuation_text_from_data",
    "render_valuation_section",
    "save_valuation_section",
]
