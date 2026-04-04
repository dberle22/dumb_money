from __future__ import annotations

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from dumb_money.config import AppSettings
import duckdb

from dumb_money.storage import read_canonical_table, upsert_canonical_table, write_canonical_table
from dumb_money.transforms.ingestion_status import stage_security_ingestion_status


def test_write_and_read_canonical_table_round_trip(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    prices = pd.read_csv("tests/fixtures/prices/aapl_daily.csv").rename(columns={"adjclose": "adj_close"})
    prices["ticker"] = "AAPL"
    prices["interval"] = "1d"
    prices["source"] = "yfinance"
    prices["currency"] = "USD"
    prices = prices[
        ["ticker", "date", "interval", "source", "currency", "open", "high", "low", "close", "adj_close", "volume"]
    ]

    write_canonical_table(prices, "normalized_prices", settings=settings)
    loaded = read_canonical_table("normalized_prices", settings=settings)
    loaded["date"] = loaded["date"].astype(str)

    assert settings.warehouse_path.exists()
    assert_frame_equal(loaded, prices, check_dtype=False)


def test_write_canonical_table_overwrites_existing_table(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    base = pd.read_csv("tests/fixtures/prices/aapl_daily.csv").rename(columns={"adjclose": "adj_close"})
    base["ticker"] = "AAPL"
    base["interval"] = "1d"
    base["source"] = "yfinance"
    base["currency"] = "USD"
    prices = base[
        ["ticker", "date", "interval", "source", "currency", "open", "high", "low", "close", "adj_close", "volume"]
    ]

    write_canonical_table(prices, "normalized_prices", settings=settings)
    replacement = prices.iloc[[0]].copy()
    replacement.loc[:, "close"] = 999.0
    replacement.loc[:, "adj_close"] = 999.0

    write_canonical_table(replacement, "normalized_prices", settings=settings)
    loaded = read_canonical_table("normalized_prices", settings=settings)

    assert loaded.shape == (1, len(replacement.columns))
    assert loaded.loc[0, "close"] == 999.0


def test_write_canonical_table_rejects_schema_mismatch(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    invalid = pd.DataFrame({"ticker": ["AAPL"], "date": ["2024-01-02"]})

    with pytest.raises(ValueError):
        write_canonical_table(invalid, "normalized_prices", settings=settings)


def test_upsert_canonical_table_merges_and_replaces_by_natural_key(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    base = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT"],
            "date": ["2024-01-02", "2024-01-02"],
            "interval": ["1d", "1d"],
            "source": ["yfinance", "yfinance"],
            "currency": ["USD", "USD"],
            "open": [100.0, 200.0],
            "high": [101.0, 201.0],
            "low": [99.0, 199.0],
            "close": [100.5, 200.5],
            "adj_close": [100.2, 200.2],
            "volume": [1000, 2000],
        }
    )
    incoming = pd.DataFrame(
        {
            "ticker": ["AAPL", "NVDA"],
            "date": ["2024-01-02", "2024-01-03"],
            "interval": ["1d", "1d"],
            "source": ["yfinance", "yfinance"],
            "currency": ["USD", "USD"],
            "open": [111.0, 300.0],
            "high": [112.0, 301.0],
            "low": [110.0, 299.0],
            "close": [111.5, 300.5],
            "adj_close": [111.2, 300.2],
            "volume": [1111, 3000],
        }
    )

    write_canonical_table(base, "normalized_prices", settings=settings)
    merged = upsert_canonical_table(incoming, "normalized_prices", settings=settings)

    assert merged.shape == (3, len(base.columns))
    assert merged.loc[
        (merged["ticker"] == "AAPL") & (merged["date"].astype(str) == "2024-01-02"),
        "close",
    ].iloc[0] == 111.5
    assert "NVDA" in merged["ticker"].tolist()


def test_duckdb_tables_support_membership_price_and_fundamentals_joins(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    memberships = pd.DataFrame(
        {
            "benchmark_id": ["DIA"],
            "benchmark_ticker": ["DIA"],
            "member_ticker": ["AAPL"],
            "member_name": ["Apple Inc."],
            "member_weight": [3.0],
            "member_sector": ["Technology"],
            "asset_class": ["Equity"],
            "exchange": ["NASDAQ"],
            "currency": ["USD"],
            "as_of_date": ["Mar 30, 2026"],
            "source": ["benchmark_holdings_snapshot"],
            "source_file": ["dia_holdings.xlsx"],
        }
    )
    security_master = pd.DataFrame(
        {
            "security_id": ["sec_aapl"],
            "ticker": ["AAPL"],
            "name": ["Apple Inc."],
            "asset_type": ["common_stock"],
            "exchange": ["Nasdaq"],
            "primary_listing": ["Nasdaq"],
            "currency": ["USD"],
            "sector": ["Technology"],
            "industry": ["Consumer Electronics"],
            "country": [None],
            "cik": [None],
            "is_benchmark": [False],
            "is_active": [True],
            "is_eligible_research_universe": [True],
            "source": ["test"],
            "source_id": ["AAPL"],
            "first_seen_at": [None],
            "last_updated_at": [None],
            "notes": [None],
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
            "market_cap": [3000000000.0],
            "enterprise_value": [3200000000.0],
            "revenue": [900000.0],
            "revenue_ttm": [None],
            "gross_profit": [380000.0],
            "operating_income": [210000.0],
            "net_income": [170000.0],
            "ebitda": [250000.0],
            "free_cash_flow": [120000.0],
            "total_debt": [75000.0],
            "total_cash": [60000.0],
            "current_assets": [145000.0],
            "current_liabilities": [98000.0],
            "shares_outstanding": [1000.0],
            "eps_trailing": [6.12],
            "eps_forward": [6.45],
            "gross_margin": [0.42],
            "operating_margin": [0.31],
            "profit_margin": [0.24],
            "return_on_equity": [1.52],
            "return_on_assets": [0.22],
            "debt_to_equity": [180.0],
            "current_ratio": [1.1],
            "trailing_pe": [30.5],
            "forward_pe": [28.1],
            "price_to_sales": [7.2],
            "ev_to_ebitda": [18.8],
            "dividend_yield": [0.005],
            "raw_payload_path": ["data/raw/fundamentals/aapl.json"],
            "pulled_at": ["2024-06-30T12:00:00Z"],
        }
    )

    write_canonical_table(memberships, "benchmark_memberships", settings=settings)
    write_canonical_table(security_master, "security_master", settings=settings)
    write_canonical_table(prices, "normalized_prices", settings=settings)
    write_canonical_table(fundamentals, "normalized_fundamentals", settings=settings)

    with duckdb.connect(str(settings.warehouse_path), read_only=True) as connection:
        joined = connection.execute(
            """
            select
              bm.member_ticker,
              sm.security_id,
              count(distinct np.date) as price_rows,
              count(distinct nf.period_end_date) as fundamentals_rows
            from benchmark_memberships bm
            left join security_master sm on sm.ticker = bm.member_ticker
            left join normalized_prices np on np.ticker = bm.member_ticker
            left join normalized_fundamentals nf on nf.ticker = bm.member_ticker
            where bm.benchmark_ticker = 'DIA'
            group by 1, 2
            """
        ).fetchdf()

    assert joined.loc[0, "member_ticker"] == "AAPL"
    assert joined.loc[0, "security_id"] == "sec_aapl"
    assert joined.loc[0, "price_rows"] == 1
    assert joined.loc[0, "fundamentals_rows"] == 1


def test_write_and_read_peer_sets_round_trip(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    peer_sets = pd.DataFrame(
        {
            "peer_set_id": ["peer_set::AAPL", "peer_set::AAPL"],
            "ticker": ["AAPL", "AAPL"],
            "peer_ticker": ["MSFT", "DELL"],
            "relationship_type": ["sector", "sector"],
            "sector": ["Technology", "Technology"],
            "industry": ["Consumer Electronics", "Consumer Electronics"],
            "selection_method": ["sector_market_cap_proximity", "sector_market_cap_proximity"],
            "peer_order": [1, 2],
        }
    )

    write_canonical_table(peer_sets, "peer_sets", settings=settings)
    loaded = read_canonical_table("peer_sets", settings=settings)

    assert_frame_equal(loaded, peer_sets, check_dtype=False)


def test_write_and_read_sector_snapshots_round_trip(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    sector_snapshots = pd.DataFrame(
        {
            "sector": ["Technology"],
            "sector_benchmark": ["XLK"],
            "company_count": [2],
            "companies_with_fundamentals": [2],
            "companies_with_prices": [2],
            "median_market_cap": [3100.0],
            "median_forward_pe": [27.5],
            "median_ev_to_ebitda": [19.0],
            "median_price_to_sales": [8.0],
            "median_free_cash_flow_yield": [0.036],
            "median_operating_margin": [0.36],
            "median_gross_margin": [0.565],
            "median_return_6m": [0.25],
            "median_return_1y": [0.60],
        }
    )

    write_canonical_table(sector_snapshots, "sector_snapshots", settings=settings)
    loaded = read_canonical_table("sector_snapshots", settings=settings)

    assert_frame_equal(loaded, sector_snapshots, check_dtype=False)


def test_stage_security_ingestion_status_summarizes_coverage(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
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
            "as_of_date": ["Mar 30, 2026", "Mar 30, 2026"],
            "source": ["benchmark_holdings_snapshot", "benchmark_holdings_snapshot"],
            "source_file": ["xsd_holdings.xlsx", "xsd_holdings.xlsx"],
        }
    )

    write_canonical_table(security_master, "security_master", settings=settings)
    write_canonical_table(prices, "normalized_prices", settings=settings)
    write_canonical_table(fundamentals, "normalized_fundamentals", settings=settings)
    write_canonical_table(memberships, "benchmark_memberships", settings=settings)

    status = stage_security_ingestion_status(settings=settings)

    assert status["ticker"].tolist() == ["AAPL", "MSFT"]
    assert bool(status.loc[status["ticker"] == "AAPL", "is_fully_ingested"].iloc[0])
    assert not bool(status.loc[status["ticker"] == "MSFT", "has_any_ingestion"].iloc[0])
    assert (settings.security_ingestion_status_dir / "security_ingestion_status.csv").exists()
    loaded = read_canonical_table("security_ingestion_status", settings=settings)
    assert set(loaded["ticker"]) == {"AAPL", "MSFT"}
