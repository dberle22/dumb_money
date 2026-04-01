"""Shared ticker-universe selection helpers."""

from __future__ import annotations

from collections.abc import Iterable

from dumb_money.config import AppSettings, get_settings
from dumb_money.ingestion.prices import normalize_tickers
from dumb_money.storage import query_canonical_data


def build_benchmark_member_ticker_sql(benchmark_ticker: str) -> str:
    """Return SQL for real security tickers from benchmark membership data."""

    normalized = benchmark_ticker.strip().upper().replace("'", "''")
    return f"""
        select distinct member_ticker as ticker
        from benchmark_memberships
        where benchmark_ticker = '{normalized}'
          and lower(coalesce(asset_class, '')) = 'equity'
          and regexp_matches(member_ticker, '^[A-Z][A-Z0-9.\\-]*$')
          and member_ticker not in ('', '-', 'NAN', 'NONE', 'CASH', 'USD')
        order by member_ticker
    """


def resolve_ticker_universe(
    *,
    tickers: Iterable[str] | None = None,
    ticker_query_sql: str | None = None,
    settings: AppSettings | None = None,
) -> list[str]:
    """Resolve a ticker universe from either an explicit list or DuckDB SQL."""

    if tickers is not None and ticker_query_sql is not None:
        raise ValueError("pass either tickers or ticker_query_sql, not both")

    if tickers is not None:
        return normalize_tickers(tickers)

    if ticker_query_sql is None:
        raise ValueError("ticker universe requires tickers or ticker_query_sql")

    settings = settings or get_settings()
    frame = query_canonical_data(ticker_query_sql, settings=settings)
    if frame.empty:
        return []
    if "ticker" not in frame.columns:
        raise ValueError("ticker_query_sql must return a 'ticker' column")
    return normalize_tickers(frame["ticker"].tolist())
