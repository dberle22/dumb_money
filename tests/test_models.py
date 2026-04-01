from datetime import date

import pytest

from dumb_money.models import BenchmarkDefinition, FundamentalSnapshot, Holding, PriceBar, Security


def test_security_uppercases_ticker_and_currency() -> None:
    security = Security(ticker=" msft ", currency="usd", name="Microsoft")

    assert security.ticker == "MSFT"
    assert security.currency == "USD"
    assert security.is_active is True


def test_price_bar_validates_price_range() -> None:
    with pytest.raises(ValueError):
        PriceBar(
            ticker="AAPL",
            date=date(2024, 1, 2),
            open=100,
            high=90,
            low=95,
            close=98,
            adj_close=98,
            volume=10,
        )


def test_fundamental_snapshot_uppercases_identifiers() -> None:
    snapshot = FundamentalSnapshot(
        ticker=" aapl ",
        as_of_date=date(2024, 6, 30),
        currency="usd",
        period_type="TTM",
        fiscal_period="ttm",
    )

    assert snapshot.ticker == "AAPL"
    assert snapshot.currency == "USD"
    assert snapshot.period_type == "ttm"
    assert snapshot.fiscal_period == "TTM"


def test_benchmark_and_holding_models_accept_mvp_payloads() -> None:
    benchmark = BenchmarkDefinition(
        benchmark_id="spy",
        ticker="spy",
        name="SPDR S&P 500 ETF Trust",
    )
    holding = Holding(
        ticker="voo",
        as_of_date=date(2024, 7, 1),
        quantity=12.5,
        market_value=6000,
        weight=0.25,
    )

    assert benchmark.benchmark_id == "SPY"
    assert holding.ticker == "VOO"
