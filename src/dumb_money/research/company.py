"""Company research packet assembly built on shared staged datasets."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

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
from dumb_money.analytics.scorecard import (
    CATEGORY_TARGET_WEIGHTS,
    CompanyScorecard,
    build_company_scorecard,
)
from dumb_money.config import AppSettings, get_settings
from dumb_money.storage import read_canonical_table
from dumb_money.transforms.sector_snapshots import build_sector_snapshots_frame


@dataclass(slots=True)
class CompanyResearchPacket:
    """Structured company research outputs for one ticker."""

    ticker: str
    company_name: str | None
    as_of_date: str | None
    company_history: pd.DataFrame
    benchmark_histories: dict[str, pd.DataFrame]
    return_windows: pd.DataFrame
    trailing_return_comparison: pd.DataFrame
    risk_metrics: dict[str, float | None]
    trend_metrics: dict[str, float | bool | None]
    benchmark_comparison: pd.DataFrame
    fundamentals_summary: dict[str, Any]
    peer_return_comparison: pd.DataFrame
    peer_return_summary_stats: dict[str, Any]
    peer_valuation_comparison: pd.DataFrame
    peer_summary_stats: dict[str, Any]
    sector_snapshot: dict[str, Any]
    scorecard: CompanyScorecard


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def load_staged_prices(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("normalized_prices", settings=settings)


def load_staged_fundamentals(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("normalized_fundamentals", settings=settings)


def load_benchmark_set(*, settings: AppSettings | None = None, set_id: str | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    benchmark_sets = read_canonical_table("benchmark_sets", settings=settings)
    if benchmark_sets.empty:
        return benchmark_sets

    if set_id is not None:
        filtered = benchmark_sets.loc[
            benchmark_sets["set_id"].astype(str).str.lower() == set_id.strip().lower()
        ].copy()
        return filtered.sort_values("member_order").reset_index(drop=True)

    default_sets = benchmark_sets.loc[benchmark_sets["is_default"] == True].copy()  # noqa: E712
    if default_sets.empty:
        return benchmark_sets.sort_values("member_order").reset_index(drop=True)

    chosen_set_id = default_sets["set_id"].iloc[0]
    return default_sets.loc[default_sets["set_id"] == chosen_set_id].sort_values("member_order").reset_index(drop=True)


def load_benchmark_prices(*, settings: AppSettings | None = None) -> pd.DataFrame:
    # Benchmark ETF histories are expected to be canonicalized into DuckDB through
    # the shared price staging flow, not loaded ad hoc from raw benchmark CSVs.
    return load_staged_prices(settings=settings)


def load_security_master(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("security_master", settings=settings)


def load_benchmark_mappings(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("benchmark_mappings", settings=settings)


def load_peer_sets(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("peer_sets", settings=settings)


def load_sector_snapshots(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("sector_snapshots", settings=settings)


def load_gold_ticker_metrics_mart(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("gold_ticker_metrics_mart", settings=settings)


def load_gold_ticker_metrics_row(
    ticker: str,
    *,
    settings: AppSettings | None = None,
) -> dict[str, Any]:
    mart = load_gold_ticker_metrics_mart(settings=settings)
    if mart.empty or "ticker" not in mart.columns:
        return {}

    normalized_ticker = ticker.strip().upper()
    rows = mart.loc[mart["ticker"].astype(str).str.upper() == normalized_ticker].copy()
    if rows.empty:
        return {}
    return rows.iloc[-1].to_dict()


def build_fundamentals_summary_from_mart_row(
    mart_row: dict[str, Any] | None,
) -> dict[str, Any]:
    """Map a gold mart row onto the shared fundamentals summary contract."""

    if not mart_row:
        return {}

    return {
        "as_of_date": mart_row.get("as_of_date"),
        "long_name": mart_row.get("company_name"),
        "sector": mart_row.get("sector"),
        "industry": mart_row.get("industry"),
        "currency": mart_row.get("currency"),
        "market_cap": mart_row.get("market_cap"),
        "enterprise_value": mart_row.get("enterprise_value"),
        "revenue_ttm": mart_row.get("revenue_ttm"),
        "ebitda": mart_row.get("ebitda"),
        "free_cash_flow": mart_row.get("free_cash_flow"),
        "gross_margin": mart_row.get("gross_margin"),
        "operating_margin": mart_row.get("operating_margin"),
        "profit_margin": mart_row.get("profit_margin"),
        "free_cash_flow_margin": mart_row.get("free_cash_flow_margin"),
        "return_on_equity": mart_row.get("return_on_equity"),
        "return_on_assets": mart_row.get("return_on_assets"),
        "return_on_invested_capital": mart_row.get("return_on_invested_capital"),
        "current_ratio": mart_row.get("current_ratio"),
        "debt_to_equity": mart_row.get("debt_to_equity"),
        "total_cash": mart_row.get("total_cash"),
        "total_debt": mart_row.get("total_debt"),
        "net_cash": mart_row.get("net_cash"),
        "trailing_pe": mart_row.get("trailing_pe"),
        "forward_pe": mart_row.get("forward_pe"),
        "ev_to_ebitda": mart_row.get("ev_to_ebitda"),
        "price_to_sales": mart_row.get("price_to_sales"),
        "dividend_yield": mart_row.get("dividend_yield"),
    }


def build_risk_metrics_from_mart_row(
    mart_row: dict[str, Any] | None,
) -> dict[str, float | None]:
    """Map a gold mart row onto the shared risk metrics contract."""

    if not mart_row:
        return {}

    return {
        "annualized_volatility_1m": mart_row.get("annualized_volatility_1m"),
        "annualized_volatility_3m": mart_row.get("annualized_volatility_3m"),
        "annualized_volatility_1y": mart_row.get("annualized_volatility_1y"),
        "downside_volatility_1y": mart_row.get("downside_volatility_1y"),
        "beta_1y": mart_row.get("beta_1y"),
        "current_drawdown": mart_row.get("current_drawdown"),
        "max_drawdown": mart_row.get("max_drawdown"),
        "max_drawdown_1y": mart_row.get("max_drawdown_1y"),
    }


def build_trend_metrics_from_mart_row(
    mart_row: dict[str, Any] | None,
) -> dict[str, float | bool | None]:
    """Map a gold mart row onto the shared trend metrics contract."""

    if not mart_row:
        return {}

    return {
        "sma_50": mart_row.get("sma_50"),
        "sma_200": mart_row.get("sma_200"),
        "price_vs_sma_50": mart_row.get("price_vs_sma_50"),
        "price_vs_sma_200": mart_row.get("price_vs_sma_200"),
        "sma_50_above_sma_200": mart_row.get("sma_50_above_sma_200"),
    }


def build_peer_summary_stats_from_mart_row(
    mart_row: dict[str, Any] | None,
) -> dict[str, Any]:
    """Map a gold mart row onto the peer summary stats contract."""

    if not mart_row:
        return {}

    return {
        "peer_count": mart_row.get("peer_count"),
        "median_forward_pe": mart_row.get("peer_median_forward_pe"),
        "median_ev_to_ebitda": mart_row.get("peer_median_ev_to_ebitda"),
        "median_price_to_sales": mart_row.get("peer_median_price_to_sales"),
        "median_free_cash_flow_yield": mart_row.get("peer_median_free_cash_flow_yield"),
        "median_market_cap": mart_row.get("peer_median_market_cap"),
    }


def build_category_scores_from_mart_row(
    mart_row: dict[str, Any] | None,
) -> pd.DataFrame:
    """Map a gold mart row onto the shared category score contract."""

    if not mart_row:
        return pd.DataFrame(columns=["category", "category_score", "available_weight", "total_intended_weight", "coverage_ratio"])

    rows: list[dict[str, Any]] = []
    category_column_map = {
        "Market Performance": ("market_performance_score", "market_performance_available_weight"),
        "Growth and Profitability": ("growth_profitability_score", "growth_profitability_available_weight"),
        "Balance Sheet Strength": ("balance_sheet_score", "balance_sheet_available_weight"),
        "Valuation": ("valuation_score", "valuation_available_weight"),
    }
    for category, (score_key, weight_key) in category_column_map.items():
        available_weight = mart_row.get(weight_key)
        total_weight = CATEGORY_TARGET_WEIGHTS[category]
        coverage_ratio = (
            None
            if available_weight is None or pd.isna(available_weight) or float(total_weight) <= 0
            else float(available_weight) / float(total_weight)
        )
        rows.append(
            {
                "category": category,
                "category_score": mart_row.get(score_key),
                "available_weight": available_weight,
                "total_intended_weight": total_weight,
                "coverage_ratio": coverage_ratio,
            }
        )
    return pd.DataFrame(rows)


def build_scorecard_summary_from_mart_row(
    ticker: str,
    mart_row: dict[str, Any] | None,
) -> dict[str, Any]:
    """Map a gold mart row onto the shared scorecard summary contract."""

    if not mart_row:
        return {}

    available_weight = sum(
        float(value)
        for value in (
            mart_row.get("market_performance_available_weight"),
            mart_row.get("growth_profitability_available_weight"),
            mart_row.get("balance_sheet_available_weight"),
            mart_row.get("valuation_available_weight"),
        )
        if value is not None and not pd.isna(value)
    )
    total_intended_weight = float(sum(CATEGORY_TARGET_WEIGHTS.values()))
    coverage_ratio = available_weight / total_intended_weight if total_intended_weight else None

    return {
        "ticker": ticker,
        "company_name": mart_row.get("company_name"),
        "sector": mart_row.get("sector"),
        "industry": mart_row.get("industry"),
        "primary_benchmark": mart_row.get("primary_benchmark"),
        "secondary_benchmark": mart_row.get("secondary_benchmark"),
        "score_date": mart_row.get("score_date"),
        "total_score": mart_row.get("total_score"),
        "market_performance_score": mart_row.get("market_performance_score"),
        "growth_profitability_score": mart_row.get("growth_profitability_score"),
        "balance_sheet_score": mart_row.get("balance_sheet_score"),
        "valuation_score": mart_row.get("valuation_score"),
        "confidence_score": mart_row.get("confidence_score"),
        "available_weight": available_weight,
        "total_intended_weight": total_intended_weight,
        "coverage_ratio": coverage_ratio,
    }


def load_gold_scorecard_metric_rows(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("gold_scorecard_metric_rows", settings=settings)


def load_gold_scorecard_metric_rows_for_ticker(
    ticker: str,
    *,
    score_date: str | None = None,
    settings: AppSettings | None = None,
) -> pd.DataFrame:
    metric_rows = load_gold_scorecard_metric_rows(settings=settings)
    if metric_rows.empty or "ticker" not in metric_rows.columns:
        return pd.DataFrame()

    normalized_ticker = ticker.strip().upper()
    filtered = metric_rows.loc[metric_rows["ticker"].astype(str).str.upper() == normalized_ticker].copy()
    if filtered.empty:
        return filtered

    if score_date:
        filtered = filtered.loc[filtered["score_date"].astype(str) == str(score_date)].copy()
    elif "score_date" in filtered.columns:
        latest_score_date = filtered["score_date"].astype(str).max()
        filtered = filtered.loc[filtered["score_date"].astype(str) == latest_score_date].copy()

    return filtered.reset_index(drop=True)


def build_company_scorecard_from_gold_artifacts(
    *,
    ticker: str,
    mart_row: dict[str, Any] | None,
    metric_rows: pd.DataFrame,
) -> CompanyScorecard:
    """Build a shared scorecard object from persisted gold artifacts only."""

    summary = build_scorecard_summary_from_mart_row(ticker, mart_row)
    category_scores = build_category_scores_from_mart_row(mart_row)
    metrics = metric_rows.copy()
    return CompanyScorecard(summary=summary, category_scores=category_scores, metrics=metrics)


def build_company_research_packet(
    ticker: str,
    *,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> CompanyResearchPacket:
    """Assemble a one-ticker company research packet from shared modules only."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    prices = load_staged_prices(settings=settings)
    company_history = prepare_price_history(prices, normalized_ticker)
    if company_history.empty:
        raise ValueError(f"no staged price history found for ticker {normalized_ticker}")

    fundamentals = load_staged_fundamentals(settings=settings)
    fundamentals_summary = build_fundamentals_summary(fundamentals, normalized_ticker)

    benchmark_set = load_benchmark_set(settings=settings, set_id=benchmark_set_id)
    benchmark_prices = load_benchmark_prices(settings=settings)
    benchmark_histories = {
        benchmark_ticker: prepare_price_history(benchmark_prices, benchmark_ticker)
        for benchmark_ticker in benchmark_set.get("ticker", pd.Series(dtype=str)).tolist()
    }
    benchmark_histories = {
        benchmark_ticker: history
        for benchmark_ticker, history in benchmark_histories.items()
        if not history.empty
    }
    security_master = load_security_master(settings=settings)
    benchmark_mappings = load_benchmark_mappings(settings=settings)
    peer_sets = load_peer_sets(settings=settings)
    sector_snapshots = load_sector_snapshots(settings=settings)
    if sector_snapshots.empty:
        sector_snapshots = build_sector_snapshots_frame(
            security_master,
            fundamentals,
            prices,
            benchmark_mappings,
        )
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
    peer_return_comparison = build_peer_return_comparison(
        normalized_ticker,
        peer_rows,
        prices,
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

    return CompanyResearchPacket(
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
