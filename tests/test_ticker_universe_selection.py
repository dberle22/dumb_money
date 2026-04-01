from __future__ import annotations

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.ingestion.fundamentals import ingest_benchmark_member_fundamentals
from dumb_money.ingestion.prices import ingest_benchmark_member_prices
from dumb_money.storage import write_canonical_table
from dumb_money.universe import build_benchmark_member_ticker_sql, resolve_ticker_universe


def test_resolve_ticker_universe_normalizes_static_tickers() -> None:
    tickers = resolve_ticker_universe(tickers=[" aapl ", "MSFT", "aapl"])

    assert tickers == ["AAPL", "MSFT"]


def test_resolve_ticker_universe_from_duckdb_sql(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    memberships = pd.DataFrame(
        {
            "benchmark_id": ["DIA", "DIA", "DIA"],
            "benchmark_ticker": ["DIA", "DIA", "DIA"],
            "member_ticker": ["AAPL", "-", "MSFT"],
            "member_name": ["Apple Inc.", "US Dollar", "Microsoft Corp."],
            "member_weight": [3.0, 0.1, 5.0],
            "member_sector": ["Technology", None, "Technology"],
            "asset_class": ["Equity", "Equity", "Equity"],
            "exchange": ["NASDAQ", None, "NASDAQ"],
            "currency": ["USD", "USD", "USD"],
            "as_of_date": ["Mar 30, 2026"] * 3,
            "source": ["benchmark_holdings_snapshot"] * 3,
            "source_file": ["dia_holdings.xlsx"] * 3,
        }
    )
    write_canonical_table(memberships, "benchmark_memberships", settings=settings)

    tickers = resolve_ticker_universe(
        ticker_query_sql=build_benchmark_member_ticker_sql("DIA"),
        settings=settings,
    )

    assert tickers == ["AAPL", "MSFT"]


def test_ingest_benchmark_member_prices_uses_query_selected_tickers(monkeypatch, tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    calls: dict[str, object] = {}

    def fake_ingest_selected_prices(**kwargs):
        calls.update(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr("dumb_money.ingestion.prices.ingest_selected_prices", fake_ingest_selected_prices)

    ingest_benchmark_member_prices(
        "DIA",
        start_date="2024-01-01",
        end_date="2024-01-31",
        settings=settings,
    )

    assert "from benchmark_memberships" in str(calls["ticker_query_sql"]).lower()
    assert calls.get("tickers") is None


def test_ingest_benchmark_member_fundamentals_uses_query_selected_tickers(monkeypatch, tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    calls: dict[str, object] = {}

    def fake_ingest_selected_fundamentals(**kwargs):
        calls.update(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(
        "dumb_money.ingestion.fundamentals.ingest_selected_fundamentals",
        fake_ingest_selected_fundamentals,
    )

    ingest_benchmark_member_fundamentals(
        "DIA",
        as_of_date="2026-04-01",
        settings=settings,
    )

    assert "from benchmark_memberships" in str(calls["ticker_query_sql"]).lower()
    assert calls.get("tickers") is None
