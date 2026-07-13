"""Portfolio-fit analytics helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dumb_money.analytics.company import (
    STANDARD_RETURN_WINDOWS,
    calculate_return_windows,
    prepare_price_history,
)


def _safe_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def enrich_portfolio_holdings(
    holdings: pd.DataFrame,
    security_master: pd.DataFrame,
) -> pd.DataFrame:
    """Join holdings to security metadata and normalize weight fields."""

    if holdings.empty:
        return holdings.copy()

    enriched = holdings.copy()
    if "market_value" in enriched.columns:
        enriched["market_value"] = pd.to_numeric(enriched["market_value"], errors="coerce")
    if "weight" in enriched.columns:
        enriched["weight"] = pd.to_numeric(enriched["weight"], errors="coerce")

    if enriched["weight"].isna().all() and "market_value" in enriched.columns:
        totals = enriched.groupby(["portfolio_id", "as_of_date"])["market_value"].transform("sum")
        enriched["weight"] = enriched["market_value"] / totals.where(totals != 0)

    if security_master.empty:
        return enriched

    metadata_columns = [
        column
        for column in ("ticker", "name", "sector", "industry", "exchange", "asset_type")
        if column in security_master.columns
    ]
    metadata = security_master[metadata_columns].drop_duplicates(subset=["ticker"], keep="last")
    return enriched.merge(metadata, on="ticker", how="left")


def build_portfolio_concentration_metrics(holdings: pd.DataFrame) -> dict[str, float | int | None]:
    """Summarize basic portfolio concentration metrics."""

    if holdings.empty:
        return {
            "holding_count": 0,
            "total_market_value": None,
            "weight_sum": None,
            "top_1_weight": None,
            "top_3_weight": None,
            "top_5_weight": None,
        }

    ordered = holdings.sort_values("weight", ascending=False, na_position="last")
    weights = pd.to_numeric(ordered["weight"], errors="coerce").fillna(0.0)
    market_values = pd.to_numeric(ordered.get("market_value"), errors="coerce")
    return {
        "holding_count": int(len(ordered)),
        "total_market_value": _safe_float(market_values.sum()) if market_values.notna().any() else None,
        "weight_sum": _safe_float(weights.sum()),
        "top_1_weight": _safe_float(weights.head(1).sum()),
        "top_3_weight": _safe_float(weights.head(3).sum()),
        "top_5_weight": _safe_float(weights.head(5).sum()),
    }


def build_portfolio_exposure(
    holdings: pd.DataFrame,
    *,
    by: str,
) -> pd.DataFrame:
    """Aggregate holdings by sector or industry."""

    if holdings.empty:
        return pd.DataFrame(columns=[by, "holding_count", "market_value", "weight"])
    if by not in holdings.columns:
        raise ValueError(f"holdings frame does not contain grouping column {by!r}")

    exposure = (
        holdings.assign(
            market_value=pd.to_numeric(holdings.get("market_value"), errors="coerce"),
            weight=pd.to_numeric(holdings.get("weight"), errors="coerce"),
        )
        .fillna({by: "Unclassified"})
        .groupby(by, dropna=False)
        .agg(
            holding_count=("ticker", "count"),
            market_value=("market_value", "sum"),
            weight=("weight", "sum"),
        )
        .reset_index()
        .sort_values(["weight", "market_value", by], ascending=[False, False, True], na_position="last")
        .reset_index(drop=True)
    )
    return exposure


def build_candidate_fit_summary(
    candidate_ticker: str,
    holdings: pd.DataFrame,
    security_master: pd.DataFrame,
) -> dict[str, Any]:
    """Build a simple inspectable candidate-fit summary against current holdings."""

    normalized_ticker = candidate_ticker.strip().upper()
    candidate_rows = security_master.loc[
        security_master["ticker"].astype(str).str.upper() == normalized_ticker
    ].copy()
    candidate_row = candidate_rows.iloc[-1].to_dict() if not candidate_rows.empty else {}

    enriched = enrich_portfolio_holdings(holdings, security_master)
    sector = candidate_row.get("sector")
    industry = candidate_row.get("industry")
    sector_weight = None
    industry_weight = None
    if sector and "sector" in enriched.columns:
        sector_weight = _safe_float(
            pd.to_numeric(
                enriched.loc[enriched["sector"] == sector, "weight"],
                errors="coerce",
            ).sum()
        )
    if industry and "industry" in enriched.columns:
        industry_weight = _safe_float(
            pd.to_numeric(
                enriched.loc[enriched["industry"] == industry, "weight"],
                errors="coerce",
            ).sum()
        )

    existing_holding = enriched.loc[
        enriched["ticker"].astype(str).str.upper() == normalized_ticker
    ].copy()
    existing_weight = None
    if not existing_holding.empty:
        existing_weight = _safe_float(pd.to_numeric(existing_holding["weight"], errors="coerce").sum())

    diversification_role = "new_exposure"
    if existing_weight not in (None, 0):
        diversification_role = "already_held"
    elif sector_weight not in (None, 0):
        diversification_role = "adds_to_existing_sector"

    concentration = build_portfolio_concentration_metrics(enriched)
    return {
        "candidate_ticker": normalized_ticker,
        "candidate_name": candidate_row.get("name"),
        "candidate_sector": sector,
        "candidate_industry": industry,
        "already_held": not existing_holding.empty,
        "existing_weight": existing_weight,
        "sector_weight_before": sector_weight,
        "industry_weight_before": industry_weight,
        "diversification_role": diversification_role,
        "largest_position_weight": concentration.get("top_1_weight"),
        "top_3_weight": concentration.get("top_3_weight"),
    }


def build_portfolio_benchmark_comparison(
    holdings: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    benchmark_tickers: list[str],
) -> pd.DataFrame:
    """Compare weighted portfolio trailing returns against benchmark ETFs."""

    columns = ["window", "portfolio_return", *[f"{ticker}_return" for ticker in benchmark_tickers]]
    if holdings.empty or prices.empty:
        return pd.DataFrame(columns=columns)

    weighted_frames: list[pd.DataFrame] = []
    for row in holdings.to_dict(orient="records"):
        ticker = str(row.get("ticker") or "").upper()
        weight = _safe_float(row.get("weight"))
        if not ticker or weight in (None, 0):
            continue
        history = prepare_price_history(prices, ticker)
        if history.empty:
            continue
        returns = calculate_return_windows(history)[["window", "total_return"]].copy()
        returns["weighted_return"] = pd.to_numeric(returns["total_return"], errors="coerce") * float(weight)
        weighted_frames.append(returns[["window", "weighted_return"]])

    if not weighted_frames:
        return pd.DataFrame(columns=columns)

    portfolio_returns = (
        pd.concat(weighted_frames, ignore_index=True)
        .groupby("window", as_index=False)["weighted_return"]
        .sum()
        .rename(columns={"weighted_return": "portfolio_return"})
    )

    comparison = portfolio_returns.copy()
    for benchmark_ticker in benchmark_tickers:
        benchmark_history = prepare_price_history(prices, benchmark_ticker)
        benchmark_returns = calculate_return_windows(benchmark_history)[["window", "total_return"]].rename(
            columns={"total_return": f"{benchmark_ticker}_return"}
        )
        comparison = comparison.merge(benchmark_returns, on="window", how="left")

    ordered_windows = pd.Categorical(comparison["window"], categories=list(STANDARD_RETURN_WINDOWS.keys()), ordered=True)
    comparison["window"] = ordered_windows
    comparison = comparison.sort_values("window").reset_index(drop=True)
    comparison["window"] = comparison["window"].astype(str)
    return comparison


def build_watchlist_decision_table(
    candidate_tickers: list[str],
    holdings: pd.DataFrame,
    security_master: pd.DataFrame,
    *,
    gold_snapshot: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a reusable decision-support table for portfolio watchlist review."""

    rows: list[dict[str, Any]] = []
    normalized_candidates = [ticker.strip().upper() for ticker in candidate_tickers if ticker and ticker.strip()]
    for ticker in normalized_candidates:
        fit = build_candidate_fit_summary(ticker, holdings, security_master)
        gold_row: dict[str, Any] = {}
        if gold_snapshot is not None and not gold_snapshot.empty and "ticker" in gold_snapshot.columns:
            matches = gold_snapshot.loc[gold_snapshot["ticker"].astype(str).str.upper() == ticker].copy()
            if not matches.empty:
                gold_row = matches.iloc[-1].to_dict()
        rows.append(
            {
                "candidate_ticker": ticker,
                "candidate_name": fit.get("candidate_name"),
                "candidate_sector": fit.get("candidate_sector"),
                "candidate_industry": fit.get("candidate_industry"),
                "diversification_role": fit.get("diversification_role"),
                "already_held": fit.get("already_held"),
                "existing_weight": fit.get("existing_weight"),
                "sector_weight_before": fit.get("sector_weight_before"),
                "industry_weight_before": fit.get("industry_weight_before"),
                "total_score": gold_row.get("total_score"),
                "valuation_score": gold_row.get("valuation_score"),
                "market_performance_score": gold_row.get("market_performance_score"),
                "forward_pe": gold_row.get("forward_pe"),
                "return_1y": gold_row.get("return_1y"),
            }
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "candidate_ticker",
                "candidate_name",
                "candidate_sector",
                "candidate_industry",
                "diversification_role",
                "already_held",
                "existing_weight",
                "sector_weight_before",
                "industry_weight_before",
                "total_score",
                "valuation_score",
                "market_performance_score",
                "forward_pe",
                "return_1y",
            ]
        )

    table = pd.DataFrame(rows)
    return table.sort_values(
        ["already_held", "sector_weight_before", "total_score", "candidate_ticker"],
        ascending=[True, True, False, True],
        na_position="last",
    ).reset_index(drop=True)
