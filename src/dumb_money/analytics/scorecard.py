"""Company scorecard scoring for the buildable V1 metric set."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

DEFAULT_PRIMARY_BENCHMARK = "SPY"
DEFAULT_SECTOR_BENCHMARK_MAP: dict[str, str] = {
    "Technology": "QQQ",
}

CATEGORY_TARGET_WEIGHTS: dict[str, float] = {
    "Market Performance": 25.0,
    "Growth and Profitability": 35.0,
    "Balance Sheet Strength": 25.0,
    "Valuation": 15.0,
}

METRIC_OUTPUT_COLUMNS = [
    "metric_id",
    "category",
    "metric_name",
    "raw_value",
    "normalized_value",
    "scoring_method",
    "metric_score",
    "metric_weight",
    "metric_available",
    "metric_applicable",
    "confidence_flag",
    "notes",
]

CATEGORY_OUTPUT_COLUMNS = [
    "category",
    "category_score",
    "available_weight",
    "total_intended_weight",
    "coverage_ratio",
]


@dataclass(slots=True)
class CompanyScorecard:
    """Structured stock scorecard output."""

    summary: dict[str, Any]
    category_scores: pd.DataFrame
    metrics: pd.DataFrame


def _score_higher_is_better(value: float, bands: list[tuple[float, float]]) -> float:
    for threshold, normalized_score in bands:
        if value >= threshold:
            return normalized_score
    return 0.0


def _score_lower_is_better(value: float, bands: list[tuple[float, float]]) -> float:
    for threshold, normalized_score in bands:
        if value <= threshold:
            return normalized_score
    return 0.0


def _to_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _lookup_peer_median(
    peer_valuation_comparison: pd.DataFrame | None,
    column: str,
) -> float | None:
    if peer_valuation_comparison is None or peer_valuation_comparison.empty or column not in peer_valuation_comparison.columns:
        return None

    peer_mask = peer_valuation_comparison["is_focal_company"].astype("boolean").fillna(False)
    peers = peer_valuation_comparison.loc[~peer_mask].copy()
    if peers.empty:
        return None
    values = pd.to_numeric(peers[column], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.median())


def _score_relative_discount(
    company_value: float | None,
    peer_median: float | None,
) -> tuple[float | None, str]:
    if company_value is None or peer_median is None or peer_median == 0:
        return None, "threshold"

    premium_to_peer = (company_value / peer_median) - 1
    normalized_value = _score_lower_is_better(
        premium_to_peer,
        [(-0.20, 1.0), (-0.05, 0.75), (0.10, 0.5), (0.25, 0.25)],
    )
    return normalized_value, "peer_relative"


def _lookup_excess_return(
    benchmark_comparison: pd.DataFrame,
    benchmark_ticker: str | None,
    *,
    window: str = "1y",
) -> float | None:
    if benchmark_ticker is None or benchmark_comparison.empty:
        return None

    rows = benchmark_comparison.loc[
        (benchmark_comparison["benchmark_ticker"] == benchmark_ticker) & (benchmark_comparison["window"] == window)
    ]
    if rows.empty:
        return None
    return _to_float(rows.iloc[0]["excess_return"])


def resolve_secondary_benchmark(
    sector: str | None,
    available_benchmarks: set[str],
    *,
    sector_benchmark_map: dict[str, str] | None = None,
) -> str | None:
    """Resolve the current secondary benchmark from a sector mapping."""

    mapping = sector_benchmark_map or DEFAULT_SECTOR_BENCHMARK_MAP
    if sector:
        mapped = mapping.get(sector)
        if mapped and mapped in available_benchmarks:
            return mapped
    if "QQQ" in available_benchmarks:
        return "QQQ"
    if "IWM" in available_benchmarks:
        return "IWM"
    return None


def build_company_scorecard(
    *,
    ticker: str,
    company_name: str | None,
    sector: str | None,
    industry: str | None,
    score_date: str | None,
    benchmark_comparison: pd.DataFrame,
    risk_metrics: dict[str, float | None],
    trend_metrics: dict[str, float | bool | None],
    fundamentals_summary: dict[str, Any],
    peer_valuation_comparison: pd.DataFrame | None = None,
    primary_benchmark: str = DEFAULT_PRIMARY_BENCHMARK,
    secondary_benchmark: str | None = None,
    sector_benchmark_map: dict[str, str] | None = None,
) -> CompanyScorecard:
    """Build the buildable V1 company scorecard from reusable analytics outputs."""

    resolved_primary_benchmark = primary_benchmark or DEFAULT_PRIMARY_BENCHMARK
    available_benchmarks = set(benchmark_comparison.get("benchmark_ticker", pd.Series(dtype=str)).tolist())
    resolved_secondary = secondary_benchmark or resolve_secondary_benchmark(
        sector,
        available_benchmarks,
        sector_benchmark_map=sector_benchmark_map,
    )

    rows: list[dict[str, Any]] = []

    def add_metric(
        *,
        metric_id: str,
        category: str,
        metric_name: str,
        metric_weight: float,
        raw_value: float | None,
        scoring_method: str,
        normalized_value: float | None,
        notes: str = "",
        metric_applicable: bool = True,
    ) -> None:
        metric_available = metric_applicable and raw_value is not None and normalized_value is not None
        metric_score = float(metric_weight * normalized_value) if metric_available else 0.0
        confidence_flag = (
            "ok"
            if metric_available
            else ("not_applicable" if not metric_applicable else "missing_data")
        )
        rows.append(
            {
                "metric_id": metric_id,
                "category": category,
                "metric_name": metric_name,
                "raw_value": raw_value,
                "normalized_value": normalized_value,
                "scoring_method": scoring_method,
                "metric_score": metric_score,
                "metric_weight": metric_weight,
                "metric_available": metric_available,
                "metric_applicable": metric_applicable,
                "confidence_flag": confidence_flag,
                "notes": notes,
            }
        )

    primary_excess_return = _lookup_excess_return(benchmark_comparison, resolved_primary_benchmark)
    secondary_excess_return = _lookup_excess_return(benchmark_comparison, resolved_secondary)
    max_drawdown_1y = _to_float(risk_metrics.get("max_drawdown_1y"))
    price_vs_sma_200 = _to_float(trend_metrics.get("price_vs_sma_200"))

    add_metric(
        metric_id="return_vs_spy_1y",
        category="Market Performance",
        metric_name="12 month return vs SPY",
        metric_weight=10.0,
        raw_value=primary_excess_return,
        scoring_method="threshold",
        normalized_value=None
        if primary_excess_return is None
        else _score_higher_is_better(primary_excess_return, [(0.15, 1.0), (0.05, 0.75), (-0.05, 0.5), (-0.15, 0.25)]),
        notes="Uses trailing 1 year excess return versus the primary benchmark.",
    )
    add_metric(
        metric_id="return_vs_secondary_1y",
        category="Market Performance",
        metric_name="12 month return vs sector or style benchmark",
        metric_weight=7.0,
        raw_value=secondary_excess_return,
        scoring_method="threshold",
        normalized_value=None
        if secondary_excess_return is None
        else _score_higher_is_better(secondary_excess_return, [(0.15, 1.0), (0.05, 0.75), (-0.05, 0.5), (-0.15, 0.25)]),
        notes=f"Uses trailing 1 year excess return versus {resolved_secondary or 'unresolved benchmark'}.",
    )
    add_metric(
        metric_id="max_drawdown_1y",
        category="Market Performance",
        metric_name="Max drawdown over trailing 1 year",
        metric_weight=4.0,
        raw_value=max_drawdown_1y,
        scoring_method="threshold",
        normalized_value=None
        if max_drawdown_1y is None
        else _score_lower_is_better(abs(max_drawdown_1y), [(0.10, 1.0), (0.20, 0.75), (0.30, 0.5), (0.40, 0.25)]),
        notes="Scores smaller one-year drawdowns more favorably.",
    )
    add_metric(
        metric_id="price_vs_sma_200",
        category="Market Performance",
        metric_name="Price vs 200 day moving average",
        metric_weight=4.0,
        raw_value=price_vs_sma_200,
        scoring_method="threshold",
        normalized_value=None
        if price_vs_sma_200 is None
        else _score_higher_is_better(price_vs_sma_200, [(0.10, 1.0), (0.0, 0.75), (-0.10, 0.5), (-0.20, 0.25)]),
        notes="Captures whether price is above or below its long-term trend.",
    )

    operating_margin = _to_float(fundamentals_summary.get("operating_margin"))
    free_cash_flow_margin = _to_float(fundamentals_summary.get("free_cash_flow_margin"))
    roe = _to_float(fundamentals_summary.get("return_on_equity"))
    roa = _to_float(fundamentals_summary.get("return_on_assets"))
    gross_margin = _to_float(fundamentals_summary.get("gross_margin"))

    add_metric(
        metric_id="operating_margin",
        category="Growth and Profitability",
        metric_name="Operating margin",
        metric_weight=8.0,
        raw_value=operating_margin,
        scoring_method="threshold",
        normalized_value=None
        if operating_margin is None
        else _score_higher_is_better(operating_margin, [(0.30, 1.0), (0.20, 0.75), (0.10, 0.5), (0.05, 0.25)]),
        notes="Latest normalized fundamentals snapshot.",
    )
    add_metric(
        metric_id="free_cash_flow_margin",
        category="Growth and Profitability",
        metric_name="Free cash flow margin",
        metric_weight=8.0,
        raw_value=free_cash_flow_margin,
        scoring_method="threshold",
        normalized_value=None
        if free_cash_flow_margin is None
        else _score_higher_is_better(free_cash_flow_margin, [(0.20, 1.0), (0.12, 0.75), (0.05, 0.5), (0.0, 0.25)]),
        notes="Derived from free cash flow divided by revenue TTM.",
    )
    add_metric(
        metric_id="roe",
        category="Growth and Profitability",
        metric_name="Return on equity",
        metric_weight=7.0,
        raw_value=roe,
        scoring_method="threshold",
        normalized_value=None
        if roe is None
        else _score_higher_is_better(roe, [(0.25, 1.0), (0.15, 0.75), (0.10, 0.5), (0.05, 0.25)]),
        notes="ROE is used as the practical V1 capital efficiency metric.",
    )
    add_metric(
        metric_id="roa",
        category="Growth and Profitability",
        metric_name="Return on assets",
        metric_weight=6.0,
        raw_value=roa,
        scoring_method="threshold",
        normalized_value=None
        if roa is None
        else _score_higher_is_better(roa, [(0.12, 1.0), (0.08, 0.75), (0.04, 0.5), (0.01, 0.25)]),
        notes="Latest normalized fundamentals snapshot.",
    )
    add_metric(
        metric_id="gross_margin",
        category="Growth and Profitability",
        metric_name="Gross margin",
        metric_weight=6.0,
        raw_value=gross_margin,
        scoring_method="threshold",
        normalized_value=None
        if gross_margin is None
        else _score_higher_is_better(gross_margin, [(0.60, 1.0), (0.45, 0.75), (0.30, 0.5), (0.15, 0.25)]),
        notes="Latest normalized fundamentals snapshot.",
    )

    total_debt = _to_float(fundamentals_summary.get("total_debt"))
    total_cash = _to_float(fundamentals_summary.get("total_cash"))
    ebitda = _to_float(fundamentals_summary.get("ebitda"))
    current_ratio = _to_float(fundamentals_summary.get("current_ratio"))
    debt_to_equity = _to_float(fundamentals_summary.get("debt_to_equity"))
    free_cash_flow = _to_float(fundamentals_summary.get("free_cash_flow"))

    net_debt_to_ebitda = None
    if total_debt is not None and total_cash is not None and ebitda is not None and ebitda > 0:
        net_debt_to_ebitda = (total_debt - total_cash) / ebitda

    free_cash_flow_to_debt = None
    cash_to_debt = None
    fcf_debt_applicable = True
    cash_debt_applicable = True
    if total_debt is not None:
        if total_debt > 0:
            if free_cash_flow is not None:
                free_cash_flow_to_debt = free_cash_flow / total_debt
            if total_cash is not None:
                cash_to_debt = total_cash / total_debt
        else:
            free_cash_flow_to_debt = 1.0
            cash_to_debt = 1.0
            fcf_debt_applicable = True
            cash_debt_applicable = True

    add_metric(
        metric_id="net_debt_to_ebitda",
        category="Balance Sheet Strength",
        metric_name="Net debt to EBITDA",
        metric_weight=8.0,
        raw_value=net_debt_to_ebitda,
        scoring_method="threshold",
        normalized_value=None
        if net_debt_to_ebitda is None
        else _score_lower_is_better(net_debt_to_ebitda, [(0.0, 1.0), (1.0, 0.75), (2.0, 0.5), (3.0, 0.25)]),
        notes="Derived from total debt less total cash divided by EBITDA.",
        metric_applicable=ebitda is not None and ebitda > 0,
    )
    add_metric(
        metric_id="current_ratio",
        category="Balance Sheet Strength",
        metric_name="Current ratio",
        metric_weight=5.0,
        raw_value=current_ratio,
        scoring_method="threshold",
        normalized_value=None
        if current_ratio is None
        else _score_higher_is_better(current_ratio, [(2.0, 1.0), (1.5, 0.75), (1.0, 0.5), (0.8, 0.25)]),
        notes="Latest normalized fundamentals snapshot.",
    )
    add_metric(
        metric_id="debt_to_equity",
        category="Balance Sheet Strength",
        metric_name="Debt to equity",
        metric_weight=5.0,
        raw_value=debt_to_equity,
        scoring_method="threshold",
        normalized_value=None
        if debt_to_equity is None
        else _score_lower_is_better(debt_to_equity, [(25.0, 1.0), (75.0, 0.75), (125.0, 0.5), (200.0, 0.25)]),
        notes="Latest normalized fundamentals snapshot.",
    )
    add_metric(
        metric_id="free_cash_flow_to_debt",
        category="Balance Sheet Strength",
        metric_name="Free cash flow to debt",
        metric_weight=4.0,
        raw_value=free_cash_flow_to_debt,
        scoring_method="threshold",
        normalized_value=None
        if free_cash_flow_to_debt is None
        else _score_higher_is_better(free_cash_flow_to_debt, [(1.0, 1.0), (0.5, 0.75), (0.2, 0.5), (0.0, 0.25)]),
        notes="Derived from free cash flow divided by total debt.",
        metric_applicable=fcf_debt_applicable,
    )
    add_metric(
        metric_id="cash_to_debt",
        category="Balance Sheet Strength",
        metric_name="Cash to debt",
        metric_weight=3.0,
        raw_value=cash_to_debt,
        scoring_method="threshold",
        normalized_value=None
        if cash_to_debt is None
        else _score_higher_is_better(cash_to_debt, [(1.0, 1.0), (0.75, 0.75), (0.5, 0.5), (0.0, 0.25)]),
        notes="Derived from total cash divided by total debt.",
        metric_applicable=cash_debt_applicable,
    )

    forward_pe = _to_float(fundamentals_summary.get("forward_pe"))
    ev_to_ebitda = _to_float(fundamentals_summary.get("ev_to_ebitda"))
    price_to_sales = _to_float(fundamentals_summary.get("price_to_sales"))
    market_cap = _to_float(fundamentals_summary.get("market_cap"))
    dividend_yield = _to_float(fundamentals_summary.get("dividend_yield"))
    free_cash_flow_yield = (
        (free_cash_flow / market_cap)
        if free_cash_flow is not None and market_cap is not None and market_cap > 0
        else None
    )
    yield_metric_value = free_cash_flow_yield if free_cash_flow_yield is not None else dividend_yield
    yield_notes = (
        "Uses free cash flow yield derived from free cash flow divided by market cap."
        if free_cash_flow_yield is not None
        else "Uses dividend yield because free cash flow yield was unavailable."
    )
    peer_forward_pe = _lookup_peer_median(peer_valuation_comparison, "forward_pe")
    peer_ev_to_ebitda = _lookup_peer_median(peer_valuation_comparison, "ev_to_ebitda")
    peer_price_to_sales = _lookup_peer_median(peer_valuation_comparison, "price_to_sales")
    forward_pe_normalized, forward_pe_method = _score_relative_discount(forward_pe, peer_forward_pe)
    ev_to_ebitda_normalized, ev_to_ebitda_method = _score_relative_discount(ev_to_ebitda, peer_ev_to_ebitda)
    price_to_sales_normalized, price_to_sales_method = _score_relative_discount(price_to_sales, peer_price_to_sales)

    add_metric(
        metric_id="forward_pe",
        category="Valuation",
        metric_name="Forward P/E",
        metric_weight=5.0,
        raw_value=forward_pe,
        scoring_method=forward_pe_method,
        normalized_value=(
            forward_pe_normalized
            if forward_pe_normalized is not None
            else (
                None
                if forward_pe is None
                else _score_lower_is_better(forward_pe, [(15.0, 1.0), (22.0, 0.75), (30.0, 0.5), (40.0, 0.25)])
            )
        ),
        notes=(
            f"Peer-relative scoring versus peer median {peer_forward_pe:.2f}x."
            if forward_pe_normalized is not None and peer_forward_pe is not None
            else "Absolute threshold scoring for V1."
        ),
    )
    add_metric(
        metric_id="ev_to_ebitda",
        category="Valuation",
        metric_name="EV to EBITDA",
        metric_weight=4.0,
        raw_value=ev_to_ebitda,
        scoring_method=ev_to_ebitda_method,
        normalized_value=(
            ev_to_ebitda_normalized
            if ev_to_ebitda_normalized is not None
            else (
                None
                if ev_to_ebitda is None
                else _score_lower_is_better(ev_to_ebitda, [(10.0, 1.0), (15.0, 0.75), (22.0, 0.5), (30.0, 0.25)])
            )
        ),
        notes=(
            f"Peer-relative scoring versus peer median {peer_ev_to_ebitda:.2f}x."
            if ev_to_ebitda_normalized is not None and peer_ev_to_ebitda is not None
            else "Absolute threshold scoring for V1."
        ),
    )
    add_metric(
        metric_id="price_to_sales",
        category="Valuation",
        metric_name="Price to sales",
        metric_weight=3.0,
        raw_value=price_to_sales,
        scoring_method=price_to_sales_method,
        normalized_value=(
            price_to_sales_normalized
            if price_to_sales_normalized is not None
            else (
                None
                if price_to_sales is None
                else _score_lower_is_better(price_to_sales, [(2.0, 1.0), (4.0, 0.75), (7.0, 0.5), (10.0, 0.25)])
            )
        ),
        notes=(
            f"Peer-relative scoring versus peer median {peer_price_to_sales:.2f}x."
            if price_to_sales_normalized is not None and peer_price_to_sales is not None
            else "Absolute threshold scoring for V1."
        ),
    )
    add_metric(
        metric_id="yield_metric",
        category="Valuation",
        metric_name="Dividend yield or free cash flow yield",
        metric_weight=3.0,
        raw_value=yield_metric_value,
        scoring_method="threshold",
        normalized_value=None
        if yield_metric_value is None
        else _score_higher_is_better(yield_metric_value, [(0.08, 1.0), (0.05, 0.75), (0.03, 0.5), (0.0, 0.25)]),
        notes=yield_notes,
    )

    metrics = pd.DataFrame(rows, columns=METRIC_OUTPUT_COLUMNS)

    category_rows: list[dict[str, Any]] = []
    total_score = 0.0
    total_available_weight = 0.0
    total_intended_weight = float(sum(CATEGORY_TARGET_WEIGHTS.values()))

    for category, total_weight in CATEGORY_TARGET_WEIGHTS.items():
        category_metrics = metrics.loc[metrics["category"] == category].copy()
        available_weight = float(category_metrics.loc[category_metrics["metric_available"], "metric_weight"].sum())
        raw_points = float(category_metrics["metric_score"].sum())
        coverage_ratio = (available_weight / total_weight) if total_weight > 0 else 0.0
        category_score = (raw_points / available_weight * total_weight) if available_weight > 0 else 0.0
        total_score += category_score
        total_available_weight += available_weight
        category_rows.append(
            {
                "category": category,
                "category_score": category_score,
                "available_weight": available_weight,
                "total_intended_weight": total_weight,
                "coverage_ratio": coverage_ratio,
            }
        )

    category_scores = pd.DataFrame(category_rows, columns=CATEGORY_OUTPUT_COLUMNS)
    coverage_ratio = (total_available_weight / total_intended_weight) if total_intended_weight > 0 else 0.0

    summary = {
        "ticker": ticker,
        "company_name": company_name,
        "sector": sector,
        "industry": industry,
        "primary_benchmark": resolved_primary_benchmark,
        "secondary_benchmark": resolved_secondary,
        "score_date": score_date,
        "total_score": total_score,
        "market_performance_score": float(
            category_scores.loc[category_scores["category"] == "Market Performance", "category_score"].iloc[0]
        ),
        "growth_profitability_score": float(
            category_scores.loc[category_scores["category"] == "Growth and Profitability", "category_score"].iloc[0]
        ),
        "balance_sheet_score": float(
            category_scores.loc[category_scores["category"] == "Balance Sheet Strength", "category_score"].iloc[0]
        ),
        "valuation_score": float(
            category_scores.loc[category_scores["category"] == "Valuation", "category_score"].iloc[0]
        ),
        "confidence_score": coverage_ratio,
        "available_weight": total_available_weight,
        "total_intended_weight": total_intended_weight,
        "coverage_ratio": coverage_ratio,
    }

    return CompanyScorecard(summary=summary, category_scores=category_scores, metrics=metrics)
