"""Portfolio loaders and summary helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dumb_money.analytics.portfolio import (
    build_candidate_fit_summary,
    build_portfolio_benchmark_comparison,
    build_portfolio_concentration_metrics,
    build_portfolio_exposure,
    build_watchlist_decision_table,
    enrich_portfolio_holdings,
)
from dumb_money.config import AppSettings, get_settings
from dumb_money.storage import read_canonical_table


def load_portfolio_holdings(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("portfolio_holdings", settings=settings)


def load_portfolio_holdings_for_portfolio(
    portfolio_id: str = "default",
    *,
    settings: AppSettings | None = None,
) -> pd.DataFrame:
    holdings = load_portfolio_holdings(settings=settings)
    if holdings.empty:
        return holdings
    normalized_portfolio_id = portfolio_id.strip()
    return holdings.loc[holdings["portfolio_id"].astype(str) == normalized_portfolio_id].copy()


def build_portfolio_summary(
    portfolio_id: str = "default",
    *,
    candidate_ticker: str | None = None,
    settings: AppSettings | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    holdings = load_portfolio_holdings_for_portfolio(portfolio_id, settings=settings)
    security_master = read_canonical_table("security_master", settings=settings)
    prices = read_canonical_table("normalized_prices", settings=settings)
    enriched = enrich_portfolio_holdings(holdings, security_master)

    summary: dict[str, Any] = {
        "portfolio_id": portfolio_id,
        "holdings": enriched,
        "concentration_metrics": build_portfolio_concentration_metrics(enriched),
        "sector_exposure": build_portfolio_exposure(enriched, by="sector")
        if "sector" in enriched.columns
        else pd.DataFrame(),
        "industry_exposure": build_portfolio_exposure(enriched, by="industry")
        if "industry" in enriched.columns
        else pd.DataFrame(),
        "benchmark_comparison": build_portfolio_benchmark_comparison(
            enriched,
            prices,
            benchmark_tickers=list(settings.default_benchmarks),
        ),
    }
    if candidate_ticker:
        summary["candidate_fit"] = build_candidate_fit_summary(candidate_ticker, enriched, security_master)
    return summary


def build_watchlist_summary(
    candidate_tickers: list[str],
    *,
    portfolio_id: str = "default",
    settings: AppSettings | None = None,
) -> pd.DataFrame:
    settings = settings or get_settings()
    holdings = load_portfolio_holdings_for_portfolio(portfolio_id, settings=settings)
    security_master = read_canonical_table("security_master", settings=settings)
    gold_snapshot = read_canonical_table("gold_ticker_metrics_mart", settings=settings)
    return build_watchlist_decision_table(
        candidate_tickers,
        holdings,
        security_master,
        gold_snapshot=gold_snapshot,
    )
