from __future__ import annotations

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.ingestion.fundamentals import ingest_benchmark_member_fundamentals
from dumb_money.ingestion.prices import ingest_benchmark_member_prices
from dumb_money.storage import write_canonical_table
from dumb_money.transforms.ingestion_status import stage_security_ingestion_status
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


def test_benchmark_member_sql_can_exclude_fully_ingested_tickers(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    memberships = pd.DataFrame(
        {
            "benchmark_id": ["XSD", "XSD"],
            "benchmark_ticker": ["XSD", "XSD"],
            "member_ticker": ["AAPL", "MSFT"],
            "member_name": ["Apple Inc.", "Microsoft Corp."],
            "member_weight": [3.0, 5.0],
            "member_sector": ["Technology", "Technology"],
            "asset_class": ["Equity", "Equity"],
            "exchange": ["NASDAQ", "NASDAQ"],
            "currency": ["USD", "USD"],
            "as_of_date": ["Mar 30, 2026"] * 2,
            "source": ["benchmark_holdings_snapshot"] * 2,
            "source_file": ["xsd_holdings.xlsx"] * 2,
        }
    )
    security_master = pd.DataFrame(
        {
            "security_id": ["sec_aapl", "sec_msft"],
            "ticker": ["AAPL", "MSFT"],
            "name": ["Apple Inc.", "Microsoft Corp."],
            "asset_type": ["common_stock", "common_stock"],
            "exchange": ["Nasdaq", "Nasdaq"],
            "primary_listing": ["Nasdaq", "Nasdaq"],
            "currency": ["USD", "USD"],
            "sector": ["Technology", "Technology"],
            "industry": ["Consumer Electronics", "Software"],
            "country": [None, None],
            "cik": [None, None],
            "is_benchmark": [False, False],
            "is_active": [True, True],
            "is_eligible_research_universe": [True, True],
            "source": ["test", "test"],
            "source_id": ["AAPL", "MSFT"],
            "first_seen_at": [None, None],
            "last_updated_at": [None, None],
            "notes": [None, None],
        }
    )
    prices = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": ["2024-01-02"],
            "interval": ["1d"],
            "source": ["yfinance"],
            "currency": ["USD"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "adj_close": [100.2],
            "volume": [1000],
        }
    )
    fundamentals = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "as_of_date": ["2024-06-30"],
            "period_end_date": ["2024-03-30"],
            "report_date": ["2024-05-02"],
            "fiscal_year": [2024],
            "fiscal_quarter": [1],
            "fiscal_period": ["Q1"],
            "period_type": ["quarterly"],
            "source": ["yfinance"],
            "currency": ["USD"],
            "long_name": ["Apple Inc."],
            "sector": ["Technology"],
            "industry": ["Consumer Electronics"],
            "website": ["https://www.apple.com"],
            "market_cap": [1.0],
            "enterprise_value": [1.0],
            "revenue": [1.0],
            "revenue_ttm": [1.0],
            "gross_profit": [1.0],
            "operating_income": [1.0],
            "net_income": [1.0],
            "ebitda": [1.0],
            "free_cash_flow": [1.0],
            "total_debt": [1.0],
            "total_cash": [1.0],
            "current_assets": [1.0],
            "current_liabilities": [1.0],
            "shares_outstanding": [1.0],
            "eps_trailing": [1.0],
            "eps_forward": [1.0],
            "gross_margin": [1.0],
            "operating_margin": [1.0],
            "profit_margin": [1.0],
            "return_on_equity": [1.0],
            "return_on_assets": [1.0],
            "debt_to_equity": [1.0],
            "current_ratio": [1.0],
            "trailing_pe": [1.0],
            "forward_pe": [1.0],
            "price_to_sales": [1.0],
            "ev_to_ebitda": [1.0],
            "dividend_yield": [1.0],
            "raw_payload_path": ["x"],
            "pulled_at": ["2024-06-30T12:00:00Z"],
        }
    )
    write_canonical_table(memberships, "benchmark_memberships", settings=settings)
    write_canonical_table(security_master, "security_master", settings=settings)
    write_canonical_table(prices, "normalized_prices", settings=settings)
    write_canonical_table(fundamentals, "normalized_fundamentals", settings=settings)
    stage_security_ingestion_status(settings=settings)

    tickers = resolve_ticker_universe(
        ticker_query_sql=build_benchmark_member_ticker_sql("XSD", exclude_fully_ingested=True),
        settings=settings,
    )

    assert tickers == ["MSFT"]


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
