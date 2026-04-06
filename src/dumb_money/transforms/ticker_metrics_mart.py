"""Gold-layer ticker metrics mart builders for reusable Sprint 6 section inputs."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dumb_money.analytics.company import (
    build_benchmark_comparison,
    build_fundamentals_summary,
    build_peer_summary_stats,
    build_peer_valuation_comparison,
    build_trailing_return_comparison,
    calculate_return_windows,
    calculate_risk_metrics,
    calculate_trend_metrics,
    prepare_fundamentals_history,
    prepare_price_history,
)
from dumb_money.analytics.scorecard import CompanyScorecard, build_company_scorecard
from dumb_money.config import AppSettings, get_settings
from dumb_money.storage import (
    GOLD_TICKER_METRICS_MART_COLUMNS,
    export_table_csv,
    read_canonical_table,
    write_canonical_table,
)

DEFAULT_BENCHMARK_SET_ID = "sample_universe"


def _safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _select_latest_row(frame: pd.DataFrame, *, ticker: str) -> dict[str, Any]:
    if frame.empty or "ticker" not in frame.columns:
        return {}

    rows = frame.loc[frame["ticker"].astype(str).str.upper() == ticker].copy()
    if rows.empty:
        return {}

    sort_columns: list[str] = []
    for column in ("priority", "peer_order", "member_order", "as_of_date", "period_end_date"):
        if column in rows.columns:
            rows[column] = pd.to_datetime(rows[column], errors="ignore") if "date" in column else rows[column]
            sort_columns.append(column)

    if sort_columns:
        rows = rows.sort_values(sort_columns)
    return rows.iloc[-1].to_dict()


def _resolve_default_benchmark_set(benchmark_sets: pd.DataFrame) -> list[str]:
    if benchmark_sets.empty or "ticker" not in benchmark_sets.columns:
        return []

    frame = benchmark_sets.copy()
    if "is_default" in frame.columns:
        default_rows = frame.loc[frame["is_default"] == True].copy()  # noqa: E712
        if not default_rows.empty:
            frame = default_rows
    if "set_id" in frame.columns:
        first_set_id = str(frame.iloc[0]["set_id"])
        frame = frame.loc[frame["set_id"].astype(str) == first_set_id].copy()
    if "member_order" in frame.columns:
        frame = frame.sort_values("member_order")
    return frame["ticker"].astype(str).str.upper().drop_duplicates().tolist()


def _resolve_history_contract(
    fundamentals: pd.DataFrame,
    ticker: str,
) -> tuple[pd.DataFrame, str]:
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


def _build_category_lookup(scorecard: CompanyScorecard) -> dict[str, dict[str, float | None]]:
    if scorecard.category_scores.empty:
        return {}
    frame = scorecard.category_scores.copy()
    frame["category_score"] = pd.to_numeric(frame["category_score"], errors="coerce")
    frame["available_weight"] = pd.to_numeric(frame["available_weight"], errors="coerce")
    return {
        str(row["category"]): {
            "category_score": _safe_float(row["category_score"]),
            "available_weight": _safe_float(row["available_weight"]),
        }
        for _, row in frame.iterrows()
    }


def build_gold_ticker_metrics_mart_frame(
    *,
    settings: AppSettings | None = None,
    tickers: list[str] | None = None,
) -> pd.DataFrame:
    """Build the canonical one-row-per-ticker section-input mart from canonical tables.

    The mart intentionally stores ticker-level snapshot fields and score-ready rollups
    while leaving time series and detailed peer panels in their existing canonical
    tables. This keeps section loaders thin without making the table contract brittle.
    """

    settings = settings or get_settings()
    prices = read_canonical_table("normalized_prices", settings=settings)
    fundamentals = read_canonical_table("normalized_fundamentals", settings=settings)
    security_master = read_canonical_table("security_master", settings=settings)
    benchmark_mappings = read_canonical_table("benchmark_mappings", settings=settings)
    benchmark_sets = read_canonical_table("benchmark_sets", settings=settings)
    peer_sets = read_canonical_table("peer_sets", settings=settings)
    sector_snapshots = read_canonical_table("sector_snapshots", settings=settings)

    if tickers:
        mart_tickers = sorted({ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()})
    else:
        mart_tickers = set()
        for frame in (security_master, prices, fundamentals, benchmark_mappings, peer_sets):
            if "ticker" in frame.columns:
                mart_tickers.update(frame["ticker"].astype(str).str.upper().tolist())
        mart_tickers = sorted(value for value in mart_tickers if value)

    default_benchmark_tickers = _resolve_default_benchmark_set(benchmark_sets)
    rows: list[dict[str, Any]] = []

    for ticker in mart_tickers:
        price_history = prepare_price_history(prices, ticker)
        fundamentals_summary = build_fundamentals_summary(fundamentals, ticker)
        security_row = _select_latest_row(security_master, ticker=ticker)
        mapping_row = _select_latest_row(benchmark_mappings, ticker=ticker)
        peer_rows = (
            peer_sets.loc[peer_sets["ticker"].astype(str).str.upper() == ticker].copy()
            if "ticker" in peer_sets.columns
            else pd.DataFrame()
        )

        candidate_benchmarks = [
            str(mapping_row.get("primary_benchmark") or "").upper(),
            str(mapping_row.get("sector_benchmark") or "").upper(),
            str(mapping_row.get("industry_benchmark") or "").upper(),
            str(mapping_row.get("style_benchmark") or "").upper(),
            str(mapping_row.get("custom_benchmark") or "").upper(),
            *default_benchmark_tickers,
        ]
        benchmark_histories = {
            benchmark_ticker: prepare_price_history(prices, benchmark_ticker)
            for benchmark_ticker in dict.fromkeys(value for value in candidate_benchmarks if value)
        }
        benchmark_histories = {
            benchmark_ticker: history
            for benchmark_ticker, history in benchmark_histories.items()
            if not history.empty
        }

        primary_benchmark = str(mapping_row.get("primary_benchmark") or "").upper()
        if not primary_benchmark:
            primary_benchmark = next(iter(benchmark_histories), "")

        secondary_benchmark = next(
            (
                value
                for value in (
                    str(mapping_row.get("sector_benchmark") or "").upper(),
                    str(mapping_row.get("style_benchmark") or "").upper(),
                    str(mapping_row.get("industry_benchmark") or "").upper(),
                    str(mapping_row.get("custom_benchmark") or "").upper(),
                )
                if value and value != primary_benchmark and value in benchmark_histories
            ),
            None,
        )
        if secondary_benchmark is None:
            secondary_benchmark = next(
                (
                    benchmark_ticker
                    for benchmark_ticker in benchmark_histories
                    if benchmark_ticker != primary_benchmark
                ),
                None,
            )

        comparison_histories = {
            benchmark_ticker: history
            for benchmark_ticker, history in benchmark_histories.items()
            if benchmark_ticker in {primary_benchmark, secondary_benchmark}
        }
        return_windows = calculate_return_windows(price_history)
        benchmark_comparison = build_benchmark_comparison(price_history, comparison_histories)
        trailing_return_comparison = build_trailing_return_comparison(price_history, comparison_histories)
        primary_benchmark_history = comparison_histories.get(primary_benchmark) if primary_benchmark else None
        risk_metrics = calculate_risk_metrics(price_history, benchmark_history=primary_benchmark_history)
        trend_metrics = calculate_trend_metrics(price_history)
        peer_valuation_comparison = build_peer_valuation_comparison(ticker, peer_rows, fundamentals)
        peer_summary_stats = build_peer_summary_stats(peer_valuation_comparison)

        end_dates = return_windows["end_date"].dropna() if "end_date" in return_windows.columns else pd.Series(dtype=object)
        score_date = (
            str(pd.to_datetime(end_dates.iloc[-1]).date())
            if not return_windows.empty and not end_dates.empty
            else fundamentals_summary.get("as_of_date")
        )

        scorecard = build_company_scorecard(
            ticker=ticker,
            company_name=fundamentals_summary.get("long_name"),
            sector=fundamentals_summary.get("sector") or security_row.get("sector"),
            industry=fundamentals_summary.get("industry") or security_row.get("industry"),
            score_date=score_date,
            benchmark_comparison=benchmark_comparison,
            risk_metrics=risk_metrics,
            trend_metrics=trend_metrics,
            fundamentals_summary=fundamentals_summary,
            peer_valuation_comparison=peer_valuation_comparison,
            primary_benchmark=primary_benchmark,
            secondary_benchmark=secondary_benchmark,
        )
        category_lookup = _build_category_lookup(scorecard)
        history, selected_period_type = _resolve_history_contract(fundamentals, ticker)
        latest_history_row = history.iloc[-1].to_dict() if not history.empty else {}
        sector_snapshot_row = {}
        resolved_sector = fundamentals_summary.get("sector") or security_row.get("sector")
        if resolved_sector and "sector" in sector_snapshots.columns:
            sector_rows = sector_snapshots.loc[sector_snapshots["sector"] == resolved_sector].copy()
            if not sector_rows.empty:
                sector_snapshot_row = sector_rows.iloc[-1].to_dict()

        trailing_lookup = {}
        if not trailing_return_comparison.empty:
            trailing_indexed = trailing_return_comparison.set_index("window")
            for window in ("1m", "3m", "6m", "1y"):
                if window in trailing_indexed.index:
                    trailing_lookup[window] = trailing_indexed.loc[window].to_dict()

        benchmark_lookup = {}
        if not benchmark_comparison.empty:
            for _, benchmark_row in benchmark_comparison.iterrows():
                benchmark_lookup[(str(benchmark_row["benchmark_ticker"]), str(benchmark_row["window"]))] = benchmark_row

        rows.append(
            {
                "mart_id": f"gold_ticker_metrics::{ticker}::{score_date or fundamentals_summary.get('as_of_date') or 'unavailable'}",
                "ticker": ticker,
                "as_of_date": fundamentals_summary.get("as_of_date"),
                "score_date": score_date,
                "company_name": fundamentals_summary.get("long_name") or security_row.get("name"),
                "sector": resolved_sector,
                "industry": fundamentals_summary.get("industry") or security_row.get("industry"),
                "currency": fundamentals_summary.get("currency") or security_row.get("currency"),
                "primary_benchmark": primary_benchmark or None,
                "secondary_benchmark": secondary_benchmark or None,
                "sector_benchmark": mapping_row.get("sector_benchmark"),
                "industry_benchmark": mapping_row.get("industry_benchmark"),
                "style_benchmark": mapping_row.get("style_benchmark"),
                "custom_benchmark": mapping_row.get("custom_benchmark"),
                "market_cap": fundamentals_summary.get("market_cap"),
                "enterprise_value": fundamentals_summary.get("enterprise_value"),
                "revenue_ttm": fundamentals_summary.get("revenue_ttm"),
                "ebitda": fundamentals_summary.get("ebitda"),
                "free_cash_flow": fundamentals_summary.get("free_cash_flow"),
                "gross_margin": fundamentals_summary.get("gross_margin"),
                "operating_margin": fundamentals_summary.get("operating_margin"),
                "profit_margin": fundamentals_summary.get("profit_margin"),
                "free_cash_flow_margin": fundamentals_summary.get("free_cash_flow_margin"),
                "return_on_equity": fundamentals_summary.get("return_on_equity"),
                "return_on_assets": fundamentals_summary.get("return_on_assets"),
                "return_on_invested_capital": fundamentals_summary.get("return_on_invested_capital"),
                "current_ratio": fundamentals_summary.get("current_ratio"),
                "debt_to_equity": fundamentals_summary.get("debt_to_equity"),
                "total_cash": fundamentals_summary.get("total_cash"),
                "total_debt": fundamentals_summary.get("total_debt"),
                "net_cash": fundamentals_summary.get("net_cash"),
                "trailing_pe": fundamentals_summary.get("trailing_pe"),
                "forward_pe": fundamentals_summary.get("forward_pe"),
                "ev_to_ebitda": fundamentals_summary.get("ev_to_ebitda"),
                "price_to_sales": fundamentals_summary.get("price_to_sales"),
                "dividend_yield": fundamentals_summary.get("dividend_yield"),
                "return_1m": trailing_lookup.get("1m", {}).get("company_return"),
                "return_3m": trailing_lookup.get("3m", {}).get("company_return"),
                "return_6m": trailing_lookup.get("6m", {}).get("company_return"),
                "return_1y": trailing_lookup.get("1y", {}).get("company_return"),
                "primary_benchmark_return_1y": benchmark_lookup.get((primary_benchmark, "1y"), {}).get("benchmark_return"),
                "secondary_benchmark_return_1y": benchmark_lookup.get((secondary_benchmark, "1y"), {}).get("benchmark_return"),
                "excess_return_primary_1y": benchmark_lookup.get((primary_benchmark, "1y"), {}).get("excess_return"),
                "excess_return_secondary_1y": benchmark_lookup.get((secondary_benchmark, "1y"), {}).get("excess_return"),
                "annualized_volatility_1m": risk_metrics.get("annualized_volatility_1m"),
                "annualized_volatility_3m": risk_metrics.get("annualized_volatility_3m"),
                "annualized_volatility_1y": risk_metrics.get("annualized_volatility_1y"),
                "downside_volatility_1y": risk_metrics.get("downside_volatility_1y"),
                "beta_1y": risk_metrics.get("beta_1y"),
                "current_drawdown": risk_metrics.get("current_drawdown"),
                "max_drawdown": risk_metrics.get("max_drawdown"),
                "max_drawdown_1y": risk_metrics.get("max_drawdown_1y"),
                "sma_50": trend_metrics.get("sma_50"),
                "sma_200": trend_metrics.get("sma_200"),
                "price_vs_sma_50": trend_metrics.get("price_vs_sma_50"),
                "price_vs_sma_200": trend_metrics.get("price_vs_sma_200"),
                "sma_50_above_sma_200": trend_metrics.get("sma_50_above_sma_200"),
                "peer_count": peer_summary_stats.get("peer_count"),
                "peer_median_forward_pe": peer_summary_stats.get("median_forward_pe"),
                "peer_median_ev_to_ebitda": peer_summary_stats.get("median_ev_to_ebitda"),
                "peer_median_price_to_sales": peer_summary_stats.get("median_price_to_sales"),
                "peer_median_free_cash_flow_yield": peer_summary_stats.get("median_free_cash_flow_yield"),
                "peer_median_market_cap": peer_summary_stats.get("median_market_cap"),
                "selected_history_period_type": selected_period_type,
                "selected_history_period_count": len(history),
                "latest_period_end_date": (
                    str(pd.Timestamp(latest_history_row["period_end_date"]).date())
                    if latest_history_row.get("period_end_date") is not None and not pd.isna(latest_history_row.get("period_end_date"))
                    else None
                ),
                "latest_revenue_growth": latest_history_row.get("revenue_growth"),
                "latest_eps_growth": latest_history_row.get("eps_growth"),
                "latest_gross_margin_value": latest_history_row.get("gross_margin_value"),
                "latest_operating_margin_value": latest_history_row.get("operating_margin_value"),
                "latest_free_cash_flow_margin": latest_history_row.get("free_cash_flow_margin"),
                "total_score": scorecard.summary.get("total_score"),
                "confidence_score": scorecard.summary.get("confidence_score"),
                "market_performance_score": category_lookup.get("Market Performance", {}).get("category_score"),
                "market_performance_available_weight": category_lookup.get("Market Performance", {}).get("available_weight"),
                "growth_profitability_score": category_lookup.get("Growth and Profitability", {}).get("category_score"),
                "growth_profitability_available_weight": category_lookup.get("Growth and Profitability", {}).get("available_weight"),
                "balance_sheet_score": category_lookup.get("Balance Sheet Strength", {}).get("category_score"),
                "balance_sheet_available_weight": category_lookup.get("Balance Sheet Strength", {}).get("available_weight"),
                "valuation_score": category_lookup.get("Valuation", {}).get("category_score"),
                "valuation_available_weight": category_lookup.get("Valuation", {}).get("available_weight"),
                "sector_company_count": sector_snapshot_row.get("company_count"),
                "sector_median_return_6m": sector_snapshot_row.get("median_return_6m"),
                "sector_median_return_1y": sector_snapshot_row.get("median_return_1y"),
                "sector_median_forward_pe": sector_snapshot_row.get("median_forward_pe"),
                "sector_median_ev_to_ebitda": sector_snapshot_row.get("median_ev_to_ebitda"),
                "sector_median_price_to_sales": sector_snapshot_row.get("median_price_to_sales"),
            }
        )

    if not rows:
        return pd.DataFrame(columns=GOLD_TICKER_METRICS_MART_COLUMNS)
    return pd.DataFrame(rows, columns=GOLD_TICKER_METRICS_MART_COLUMNS)


def stage_gold_ticker_metrics_mart(
    *,
    settings: AppSettings | None = None,
    tickers: list[str] | None = None,
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Build and persist the canonical gold-layer ticker metrics mart."""

    settings = settings or get_settings()
    mart = build_gold_ticker_metrics_mart_frame(settings=settings, tickers=tickers)
    if write_warehouse:
        mart = write_canonical_table(mart, "gold_ticker_metrics_mart", settings=settings)
    if write_csv:
        export_table_csv(mart, "gold_ticker_metrics_mart", settings=settings)
    return mart
