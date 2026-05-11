"""DuckDB-aware Growth and Profitability section builders and renderers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dumb_money import _matplotlib  # noqa: F401
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from dumb_money.analytics.company import (
    build_fundamentals_summary,
    prepare_fundamentals_history,
)
from dumb_money.analytics.scorecard import build_company_scorecard
from dumb_money.analytics.scorecard import CATEGORY_TARGET_WEIGHTS
from dumb_money.config import AppSettings, get_settings
from dumb_money.research.company import (
    load_gold_ticker_metrics_row,
    load_peer_sets,
    load_security_master,
    load_staged_fundamentals,
)

GROWTH_TABLE_COLUMNS = [
    "Period",
    "Period Type",
    "Revenue",
    "Revenue Growth",
    "EPS / EPS Proxy",
    "EPS Growth",
    "EPS Basis",
]
MARGIN_TABLE_COLUMNS = [
    "Period",
    "Period Type",
    "Gross Margin",
    "Operating Margin",
    "Free Cash Flow Margin",
]
RETURN_TABLE_COLUMNS = [
    "Metric",
    "Company Value",
    "Peer Median",
    "Assessment",
    "Basis",
    "Shared Input",
    "Notes",
]


@dataclass(slots=True)
class GrowthProfitabilitySectionData:
    """Query-ready Growth and Profitability inputs and standardized outputs."""

    ticker: str
    company_name: str | None
    sector: str | None
    industry: str | None
    report_date: str | None
    growth_profitability_score: float | None
    growth_profitability_target: float
    selected_period_type: str
    selected_period_count: int
    fundamentals_history: pd.DataFrame
    growth_trend_table: pd.DataFrame
    margin_trend_table: pd.DataFrame
    return_on_capital_table: pd.DataFrame
    interpretation_text: str


def _format_percent(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:.{digits}%}"


def _format_number(value: float | None, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{value:,.{digits}f}"


def _format_billions(value: float | None, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value / 1_000_000_000:.{digits}f}B"


def _score_assessment(score: float | None, target: float) -> str:
    if score is None or pd.isna(score) or target <= 0:
        return "with unavailable growth and profitability scoring coverage"
    ratio = float(score) / float(target)
    if ratio >= 0.80:
        return "with clearly supportive growth and business quality"
    if ratio >= 0.60:
        return "with generally constructive growth and profitability"
    if ratio >= 0.40:
        return "with a mixed growth and profitability setup"
    return "with growth and profitability acting as a clear constraint"


def _trend_direction(values: pd.Series, *, neutral_buffer: float = 0.005) -> str:
    series = pd.to_numeric(values, errors="coerce").dropna()
    if len(series) < 2:
        return "limited"
    change = float(series.iloc[-1] - series.iloc[0])
    if abs(change) <= neutral_buffer:
        return "stable"
    return "improving" if change > 0 else "weakening"


def _growth_direction(values: pd.Series, *, neutral_buffer: float = 0.01) -> str:
    series = pd.to_numeric(values, errors="coerce").dropna()
    if len(series) < 2:
        return "limited"
    latest = float(series.iloc[-1])
    prior = float(series.iloc[-2])
    if abs(latest - prior) <= neutral_buffer:
        return "steady"
    return "accelerating" if latest > prior else "decelerating"


def _resolve_period_contract(fundamentals: pd.DataFrame, ticker: str) -> tuple[pd.DataFrame, str]:
    quarterly = prepare_fundamentals_history(fundamentals, ticker, period_type="quarterly")
    if len(quarterly) >= 4:
        return quarterly.tail(6).reset_index(drop=True), "quarterly"

    annual = prepare_fundamentals_history(fundamentals, ticker, period_type="annual")
    if len(annual) >= 2:
        return annual.tail(4).reset_index(drop=True), "annual"

    if not quarterly.empty:
        return quarterly.tail(6).reset_index(drop=True), "quarterly"

    ttm = prepare_fundamentals_history(fundamentals, ticker, period_type="ttm")
    if not ttm.empty:
        return ttm.tail(1).reset_index(drop=True), "ttm"

    if not annual.empty:
        return annual.tail(4).reset_index(drop=True), "annual"

    return pd.DataFrame(), "unavailable"


def _build_growth_trend_table(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(columns=GROWTH_TABLE_COLUMNS)

    return pd.DataFrame(
        {
            "Period": history["period_label"],
            "Period Type": history["period_type"].str.title(),
            "Revenue": history["revenue"].map(_format_billions),
            "Revenue Growth": history["revenue_growth"].map(_format_percent),
            "EPS / EPS Proxy": history["eps_value"].map(lambda value: _format_number(value, digits=2)),
            "EPS Growth": history["eps_growth"].map(_format_percent),
            "EPS Basis": history["eps_basis"],
        }
    )


def _build_margin_trend_table(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(columns=MARGIN_TABLE_COLUMNS)

    return pd.DataFrame(
        {
            "Period": history["period_label"],
            "Period Type": history["period_type"].str.title(),
            "Gross Margin": history["gross_margin_value"].map(_format_percent),
            "Operating Margin": history["operating_margin_value"].map(_format_percent),
            "Free Cash Flow Margin": history["free_cash_flow_margin"].map(_format_percent),
        }
    )


def _build_peer_profitability_snapshot(
    ticker: str,
    *,
    fundamentals: pd.DataFrame,
    peer_sets: pd.DataFrame,
) -> pd.DataFrame:
    if peer_sets.empty:
        return pd.DataFrame(columns=["ticker", "return_on_invested_capital", "return_on_equity", "return_on_assets"])

    normalized_ticker = ticker.strip().upper()
    peer_rows = (
        peer_sets.loc[peer_sets["ticker"].astype(str).str.upper() == normalized_ticker].copy()
        if "ticker" in peer_sets.columns
        else pd.DataFrame()
    )
    peer_tickers = (
        peer_rows.get("peer_ticker", pd.Series(dtype=str)).astype(str).str.upper().drop_duplicates().tolist()
    )
    rows: list[dict[str, object]] = []
    for peer_ticker in peer_tickers:
        summary = build_fundamentals_summary(fundamentals, peer_ticker)
        if len(summary) <= 1:
            continue
        rows.append(
            {
                "ticker": peer_ticker,
                "return_on_invested_capital": summary.get("return_on_invested_capital"),
                "return_on_equity": summary.get("return_on_equity"),
                "return_on_assets": summary.get("return_on_assets"),
            }
        )
    return pd.DataFrame(rows)


def _build_return_on_capital_table(
    latest_row: pd.Series,
    *,
    peer_profitability: pd.DataFrame,
) -> pd.DataFrame:
    if latest_row.empty:
        return pd.DataFrame(columns=RETURN_TABLE_COLUMNS)

    basis_period = str(latest_row.get("period_type") or "latest").title()
    basis = f"{basis_period} row ending {pd.Timestamp(latest_row['period_end_date']).date()}"
    peer_roic = pd.to_numeric(peer_profitability.get("return_on_invested_capital"), errors="coerce").dropna()
    peer_roe = pd.to_numeric(peer_profitability.get("return_on_equity"), errors="coerce").dropna()
    peer_roa = pd.to_numeric(peer_profitability.get("return_on_assets"), errors="coerce").dropna()

    def _assessment(company_value: float | None, peer_values: pd.Series) -> str:
        if company_value is None or pd.isna(company_value):
            return "Unavailable"
        if peer_values.empty:
            return "No peer median"
        peer_median = float(peer_values.median())
        if company_value >= peer_median + 0.01:
            return "Above peer median"
        if company_value <= peer_median - 0.01:
            return "Below peer median"
        return "Near peer median"

    rows: list[dict[str, object]] = []
    if pd.notna(pd.to_numeric(latest_row.get("return_on_invested_capital"), errors="coerce")):
        rows.append(
            {
                "Metric": "Return on invested capital",
                "Company Value": _format_percent(latest_row.get("return_on_invested_capital")),
                "Peer Median": _format_percent(float(peer_roic.median())) if not peer_roic.empty else "N/A",
                "Assessment": _assessment(latest_row.get("return_on_invested_capital"), peer_roic),
                "Basis": basis,
                "Shared Input": "normalized_fundamentals.nopat / invested_capital",
                "Notes": "ROIC is derived from NOPAT divided by average invested capital within the chosen period type.",
            }
        )

    rows.extend([
        {
            "Metric": "Return on equity",
            "Company Value": _format_percent(latest_row.get("return_on_equity")),
            "Peer Median": _format_percent(float(peer_roe.median())) if not peer_roe.empty else "N/A",
            "Assessment": _assessment(latest_row.get("return_on_equity"), peer_roe),
            "Basis": basis,
            "Shared Input": "normalized_fundamentals.return_on_equity",
            "Notes": "ROE is retained as a secondary return-on-capital proxy alongside ROIC when available.",
        },
        {
            "Metric": "Return on assets",
            "Company Value": _format_percent(latest_row.get("return_on_assets")),
            "Peer Median": _format_percent(float(peer_roa.median())) if not peer_roa.empty else "N/A",
            "Assessment": _assessment(latest_row.get("return_on_assets"), peer_roa),
            "Basis": basis,
            "Shared Input": "normalized_fundamentals.return_on_assets",
            "Notes": "Peer medians are based on the latest canonical fundamentals snapshot for available peers.",
        },
    ])
    return pd.DataFrame(rows, columns=RETURN_TABLE_COLUMNS)


def _build_interpretation_text(data: GrowthProfitabilitySectionData) -> str:
    company_label = data.company_name or data.ticker
    if data.fundamentals_history.empty:
        return f"{company_label} has insufficient canonical historical fundamentals coverage for a standardized growth and profitability read."

    history = data.fundamentals_history
    revenue_growth = pd.to_numeric(history["revenue_growth"], errors="coerce").dropna()
    eps_growth = pd.to_numeric(history["eps_growth"], errors="coerce").dropna()
    operating_margin = pd.to_numeric(history["operating_margin_value"], errors="coerce").dropna()
    fcf_margin = pd.to_numeric(history["free_cash_flow_margin"], errors="coerce").dropna()

    latest_eps_basis = str(history["eps_basis"].iloc[-1])
    revenue_text = "Revenue growth coverage is limited."
    if not revenue_growth.empty:
        revenue_text = (
            f"Revenue growth is {_growth_direction(revenue_growth)} at {_format_percent(float(revenue_growth.iloc[-1]))} "
            f"on the latest {data.selected_period_type} period."
        )

    eps_text = "EPS trend coverage is limited."
    if not eps_growth.empty:
        eps_text = (
            f"EPS is {_growth_direction(eps_growth)} at {_format_percent(float(eps_growth.iloc[-1]))}; "
            f"the latest series uses {latest_eps_basis}."
        )
    elif latest_eps_basis != "unavailable":
        eps_text = f"EPS history is partial and currently relies on {latest_eps_basis}."

    margin_text = "Margin trend coverage is limited."
    operating_direction = _trend_direction(operating_margin)
    fcf_direction = _trend_direction(fcf_margin)
    if operating_direction != "limited" or fcf_direction != "limited":
        margin_text = (
            f"Operating margins are {operating_direction} and free-cash-flow margins are {fcf_direction} "
            f"over the selected {data.selected_period_type} history."
        )

    roc_text = "Return-on-capital proxy coverage is limited."
    if not data.return_on_capital_table.empty:
        lead_row = data.return_on_capital_table.iloc[0]
        roc_text = (
            f"Latest return metrics are led by {str(lead_row['Metric']).lower()} at {lead_row['Company Value']} "
            f"with a {str(lead_row['Assessment']).lower()} assessment."
        )

    score_text = _score_assessment(data.growth_profitability_score, data.growth_profitability_target)
    return (
        f"{company_label} screens {score_text}. {revenue_text} {eps_text} {margin_text} {roc_text}"
    ).strip()


def build_growth_profitability_section_data_from_inputs(
    *,
    ticker: str,
    company_name: str | None,
    sector: str | None,
    industry: str | None,
    report_date: str | None,
    growth_profitability_score: float | None,
    growth_profitability_target: float | None = None,
    fundamentals: pd.DataFrame,
    peer_sets: pd.DataFrame | None = None,
) -> GrowthProfitabilitySectionData:
    """Build standardized Growth and Profitability outputs from shared inputs."""

    history, selected_period_type = _resolve_period_contract(fundamentals, ticker)
    peer_profitability = _build_peer_profitability_snapshot(
        ticker,
        fundamentals=fundamentals,
        peer_sets=peer_sets if peer_sets is not None else pd.DataFrame(),
    )

    latest_ttm = prepare_fundamentals_history(fundamentals, ticker, period_type="ttm")
    latest_row = latest_ttm.iloc[-1] if not latest_ttm.empty else (history.iloc[-1] if not history.empty else pd.Series(dtype=object))

    data = GrowthProfitabilitySectionData(
        ticker=ticker.strip().upper(),
        company_name=company_name,
        sector=sector,
        industry=industry,
        report_date=report_date,
        growth_profitability_score=growth_profitability_score,
        growth_profitability_target=growth_profitability_target or CATEGORY_TARGET_WEIGHTS["Growth and Profitability"],
        selected_period_type=selected_period_type,
        selected_period_count=len(history),
        fundamentals_history=history,
        growth_trend_table=_build_growth_trend_table(history),
        margin_trend_table=_build_margin_trend_table(history),
        return_on_capital_table=_build_return_on_capital_table(latest_row, peer_profitability=peer_profitability),
        interpretation_text="",
    )
    data.interpretation_text = _build_interpretation_text(data)
    return data


def build_growth_profitability_section_data(
    ticker: str,
    *,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> GrowthProfitabilitySectionData:
    """Build the Growth and Profitability section from canonical shared inputs."""

    del benchmark_set_id

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()
    fundamentals = load_staged_fundamentals(settings=settings)
    peer_sets = load_peer_sets(settings=settings)
    security_master = load_security_master(settings=settings)
    mart_row = load_gold_ticker_metrics_row(normalized_ticker, settings=settings)
    fundamentals_summary = build_fundamentals_summary(fundamentals, normalized_ticker)
    security_rows = (
        security_master.loc[security_master["ticker"] == normalized_ticker].copy()
        if "ticker" in security_master.columns
        else pd.DataFrame()
    )
    security_row = security_rows.iloc[-1].to_dict() if not security_rows.empty else {}
    mart_growth_score = mart_row.get("growth_profitability_score")
    mart_report_date = mart_row.get("score_date") or mart_row.get("as_of_date")
    if mart_growth_score is None or pd.isna(mart_growth_score):
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
            peer_valuation_comparison=pd.DataFrame(),
        )
        mart_growth_score = scorecard.summary.get("growth_profitability_score")
        mart_report_date = scorecard.summary.get("score_date") or fundamentals_summary.get("as_of_date")

    return build_growth_profitability_section_data_from_inputs(
        ticker=normalized_ticker,
        company_name=mart_row.get("company_name") or fundamentals_summary.get("long_name"),
        sector=mart_row.get("sector") or fundamentals_summary.get("sector") or security_row.get("sector"),
        industry=mart_row.get("industry") or fundamentals_summary.get("industry") or security_row.get("industry"),
        report_date=mart_report_date,
        growth_profitability_score=mart_growth_score,
        fundamentals=fundamentals,
        peer_sets=peer_sets,
    )


def build_growth_profitability_growth_table(data: GrowthProfitabilitySectionData) -> pd.DataFrame:
    """Build the standardized revenue and EPS growth trend output."""

    return data.growth_trend_table.copy()


def build_growth_profitability_margin_table(data: GrowthProfitabilitySectionData) -> pd.DataFrame:
    """Build the standardized margin trend output."""

    return data.margin_trend_table.copy()


def build_growth_profitability_return_on_capital_table(data: GrowthProfitabilitySectionData) -> pd.DataFrame:
    """Build the standardized return-on-capital summary output."""

    return data.return_on_capital_table.copy()


def build_growth_profitability_text_from_data(data: GrowthProfitabilitySectionData) -> str:
    """Build the reusable short interpretation output for this section."""

    return data.interpretation_text


def render_growth_profitability_section(data: GrowthProfitabilitySectionData) -> Figure:
    """Render the full Growth and Profitability section as one reviewable figure."""

    fig, axes = plt.subplots(
        nrows=5,
        ncols=1,
        figsize=(13.0, 18.0),
        gridspec_kw={"height_ratios": [0.9, 2.0, 2.0, 1.7, 0.9]},
    )
    header_ax, growth_ax, margin_ax, return_ax, text_ax = axes

    header_ax.axis("off")
    header_ax.set_title(f"{data.ticker} Growth and Profitability", loc="left", fontsize=14, fontweight="bold")
    header_rows = pd.DataFrame(
        [
            ("Company", data.company_name or data.ticker),
            ("Report Date", data.report_date or "N/A"),
            ("Selected Contract", data.selected_period_type.title()),
            ("Periods Used", f"{data.selected_period_count}"),
            (
                "Growth & Profitability Score",
                f"{(data.growth_profitability_score or 0.0):.1f} / {data.growth_profitability_target:.0f}",
            ),
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

    history = data.fundamentals_history.copy()
    if history.empty:
        growth_ax.text(0.5, 0.5, "No canonical growth history available", ha="center", va="center")
        growth_ax.axis("off")
        margin_ax.text(0.5, 0.5, "No canonical margin history available", ha="center", va="center")
        margin_ax.axis("off")
    else:
        growth_ax.plot(history["period_label"], history["revenue_growth"] * 100, marker="o", linewidth=2, label="Revenue Growth")
        growth_ax.plot(history["period_label"], history["eps_growth"] * 100, marker="o", linewidth=2, label="EPS Growth")
        growth_ax.axhline(0.0, color="#94a3b8", linewidth=1, linestyle=":")
        growth_ax.set_title("Revenue and EPS Growth Trend", loc="left")
        growth_ax.set_ylabel("Growth Rate (%)")
        growth_ax.grid(axis="y", linestyle=":", alpha=0.35)
        growth_ax.legend(loc="best")
        growth_ax.tick_params(axis="x", rotation=25)

        margin_ax.plot(history["period_label"], history["gross_margin_value"] * 100, marker="o", linewidth=2, label="Gross Margin")
        margin_ax.plot(
            history["period_label"],
            history["operating_margin_value"] * 100,
            marker="o",
            linewidth=2,
            label="Operating Margin",
        )
        margin_ax.plot(
            history["period_label"],
            history["free_cash_flow_margin"] * 100,
            marker="o",
            linewidth=2,
            label="FCF Margin",
        )
        margin_ax.set_title("Margin Trend Chart", loc="left")
        margin_ax.set_ylabel("Margin (%)")
        margin_ax.grid(axis="y", linestyle=":", alpha=0.35)
        margin_ax.legend(loc="best")
        margin_ax.tick_params(axis="x", rotation=25)

    return_ax.axis("off")
    return_ax.set_title("Return on Capital Summary", loc="left")
    if data.return_on_capital_table.empty:
        return_ax.text(0.5, 0.5, "No return-on-capital proxy table available", ha="center", va="center")
    else:
        rendered_table = return_ax.table(
            cellText=data.return_on_capital_table.values,
            colLabels=data.return_on_capital_table.columns,
            cellLoc="left",
            colLoc="left",
            colWidths=[0.12, 0.11, 0.11, 0.12, 0.18, 0.18, 0.18],
            bbox=[0.0, 0.0, 1.0, 0.92],
        )
        rendered_table.auto_set_font_size(False)
        rendered_table.set_fontsize(8.2)
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


def save_growth_profitability_section(
    ticker: str,
    *,
    output_dir: str | Path,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> dict[str, Path]:
    """Save Growth and Profitability review artifacts for one ticker."""

    section_data = build_growth_profitability_section_data(
        ticker,
        benchmark_set_id=benchmark_set_id,
        settings=settings,
    )
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    artifact_stem = f"{section_data.ticker.lower()}_growth_profitability"
    figure_path = destination / f"{artifact_stem}.png"
    growth_table_path = destination / f"{artifact_stem}_growth_table.csv"
    margin_table_path = destination / f"{artifact_stem}_margin_table.csv"
    return_table_path = destination / f"{artifact_stem}_return_on_capital.csv"
    text_path = destination / f"{artifact_stem}_summary.md"

    figure = render_growth_profitability_section(section_data)
    figure.savefig(figure_path, dpi=150, bbox_inches="tight")
    plt.close(figure)

    section_data.growth_trend_table.to_csv(growth_table_path, index=False)
    section_data.margin_trend_table.to_csv(margin_table_path, index=False)
    section_data.return_on_capital_table.to_csv(return_table_path, index=False)
    text_path.write_text(section_data.interpretation_text + "\n")

    return {
        "figure_path": figure_path,
        "growth_table_path": growth_table_path,
        "margin_table_path": margin_table_path,
        "return_table_path": return_table_path,
        "text_path": text_path,
    }


__all__ = [
    "GrowthProfitabilitySectionData",
    "build_growth_profitability_growth_table",
    "build_growth_profitability_margin_table",
    "build_growth_profitability_return_on_capital_table",
    "build_growth_profitability_section_data",
    "build_growth_profitability_section_data_from_inputs",
    "build_growth_profitability_text_from_data",
    "render_growth_profitability_section",
    "save_growth_profitability_section",
]
