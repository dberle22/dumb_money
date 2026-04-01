from datetime import date

import pytest

from dumb_money.ingestion.fundamentals import (
    collect_fundamentals_payload,
    flatten_dict,
    normalize_historical_fundamentals_payload,
    normalize_fundamentals_payload,
)
from dumb_money.models import DataSource


def test_flatten_dict_creates_dotted_keys() -> None:
    flattened = flatten_dict({"price": {"marketCap": 10}, "asset_profile": {"sector": "Tech"}})

    assert flattened == {"price.marketCap": 10, "asset_profile.sector": "Tech"}


def test_normalize_fundamentals_payload_maps_core_fields() -> None:
    raw_payload = {
        "price": {
            "longName": "Apple Inc.",
            "marketCap": 3000000000,
            "currency": "usd",
            "sharesOutstanding": 1000,
        },
        "summary_detail": {
            "trailingPE": 30.5,
            "forwardPE": 28.1,
            "dividendYield": 0.5,
            "dividendRate": 1.0,
            "currentPrice": 200.0,
        },
        "financial_data": {
            "financialCurrency": "usd",
            "totalRevenue": 500000,
            "grossProfits": 210000,
            "ebitda": 170000,
            "freeCashflow": 120000,
            "totalDebt": 75000,
            "totalCash": 60000,
            "grossMargins": 0.42,
            "operatingMargins": 0.31,
            "profitMargins": 0.24,
            "returnOnEquity": 1.52,
            "returnOnAssets": 0.22,
            "debtToEquity": 180.0,
            "currentRatio": 1.1,
        },
        "asset_profile": {
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "website": "https://www.apple.com",
        },
        "key_stats": {
            "enterpriseValue": 3200000000,
            "enterpriseToEbitda": 18.8,
        },
    }

    snapshot = normalize_fundamentals_payload("aapl", raw_payload, as_of_date=date(2024, 6, 30))

    assert snapshot.ticker == "AAPL"
    assert snapshot.currency == "USD"
    assert snapshot.long_name == "Apple Inc."
    assert snapshot.market_cap == 3000000000
    assert snapshot.enterprise_value == 3200000000
    assert snapshot.revenue_ttm == 500000
    assert snapshot.trailing_pe == 30.5
    assert snapshot.sector == "Technology"
    assert snapshot.dividend_yield == 0.005


def test_normalize_fundamentals_payload_defaults_to_yfinance_source() -> None:
    snapshot = normalize_fundamentals_payload("msft", {"price": {"currency": "usd"}}, as_of_date=date(2024, 6, 30))

    assert snapshot.source == DataSource.YFINANCE


def test_normalize_fundamentals_payload_normalizes_yfinance_dividend_yield_percent_units() -> None:
    raw_payload = {
        "price": {"currency": "USD", "currentPrice": 250.0},
        "summary_detail": {"dividendYield": 0.42, "dividendRate": 1.05, "currentPrice": 250.0},
    }

    snapshot = normalize_fundamentals_payload(
        "aapl",
        raw_payload,
        as_of_date=date(2024, 6, 30),
        source=DataSource.YFINANCE,
    )

    assert snapshot.dividend_yield == pytest.approx(0.0042)


def test_normalize_historical_fundamentals_payload_maps_mixed_period_history() -> None:
    raw_payload = {
        "price": {"currency": "usd", "longName": "Apple Inc.", "marketCap": 3000000000},
        "summary_detail": {"trailingPE": 30.5, "forwardPE": 28.1, "dividendYield": 0.5},
        "financial_data": {"financialCurrency": "usd", "totalRevenue": 1100000},
        "asset_profile": {"sector": "Technology", "industry": "Consumer Electronics"},
        "historical_fundamentals": [
            {
                "period_end_date": "2024-03-30",
                "report_date": "2024-05-02",
                "period_type": "quarterly",
                "revenue": 900000,
                "gross_profit": 380000,
                "free_cash_flow": 120000,
                "total_cash": 60000,
            },
            {
                "period_end_date": "2023-09-30",
                "report_date": "2023-11-02",
                "period_type": "annual",
                "revenue": 1500000,
                "gross_profit": 620000,
                "free_cash_flow": 210000,
                "total_cash": 55000,
            },
            {
                "period_end_date": "2024-03-30",
                "period_type": "ttm",
                "revenue": 1100000,
                "revenue_ttm": 1100000,
                "gross_profit": 470000,
                "free_cash_flow": 140000,
                "total_cash": 60000,
            },
        ],
    }

    snapshots = normalize_historical_fundamentals_payload(
        "aapl",
        raw_payload,
        as_of_date=date(2024, 6, 30),
    )

    assert len(snapshots) == 3
    period_types = {snapshot.period_type for snapshot in snapshots}
    assert period_types == {"quarterly", "annual", "ttm"}
    quarterly = next(snapshot for snapshot in snapshots if snapshot.period_type == "quarterly")
    ttm = next(snapshot for snapshot in snapshots if snapshot.period_type == "ttm")
    assert quarterly.fiscal_period == "Q1"
    assert quarterly.revenue == 900000
    assert ttm.revenue_ttm == 1100000
    assert ttm.long_name == "Apple Inc."


def test_collect_fundamentals_payload_falls_back_to_yfinance(monkeypatch) -> None:
    def fake_yahooquery(_ticker: str) -> dict[str, object]:
        raise ModuleNotFoundError("yahooquery")

    def fake_yfinance(_ticker: str) -> dict[str, object]:
        return {"price": {"currency": "USD", "longName": "Fallback Co"}}

    monkeypatch.setattr(
        "dumb_money.ingestion.fundamentals.collect_yahooquery_fundamentals",
        fake_yahooquery,
    )
    monkeypatch.setattr(
        "dumb_money.ingestion.fundamentals.collect_yfinance_fundamentals",
        fake_yfinance,
    )

    payload, source = collect_fundamentals_payload("AAPL", provider=DataSource.YAHOOQUERY)

    assert payload["price"]["longName"] == "Fallback Co"
    assert source == DataSource.YFINANCE
