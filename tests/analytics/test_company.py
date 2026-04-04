from __future__ import annotations

import pandas as pd

from dumb_money.analytics.company import (
    build_benchmark_comparison,
    build_peer_return_comparison,
    build_peer_return_summary_stats,
    build_fundamentals_summary,
    build_peer_summary_stats,
    build_peer_valuation_comparison,
    calculate_return_windows,
    calculate_risk_metrics,
    calculate_trend_metrics,
    prepare_price_history,
)


def _price_history_frame(*, ticker: str, periods: int, start_price: float, step: float) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-01", periods=periods)
    prices = [start_price + (step * index) for index in range(periods)]
    return pd.DataFrame(
        {
            "ticker": [ticker] * periods,
            "date": dates,
            "adj_close": prices,
        }
    )


def test_calculate_return_windows_uses_trailing_trading_days() -> None:
    history = prepare_price_history(_price_history_frame(ticker="AAPL", periods=260, start_price=100, step=1), "AAPL")

    returns = calculate_return_windows(history)

    one_month = returns.loc[returns["window"] == "1m"].iloc[0]
    one_year = returns.loc[returns["window"] == "1y"].iloc[0]

    assert round(one_month["total_return"], 6) == round((359 / 338) - 1, 6)
    assert round(one_year["total_return"], 6) == round((359 / 107) - 1, 6)


def test_calculate_risk_and_trend_metrics_on_uptrend() -> None:
    history = prepare_price_history(_price_history_frame(ticker="AAPL", periods=260, start_price=100, step=1), "AAPL")

    risk = calculate_risk_metrics(history)
    trend = calculate_trend_metrics(history)

    assert risk["annualized_volatility_1m"] is not None
    assert risk["max_drawdown"] == 0.0
    assert trend["sma_20_above_sma_50"] is True
    assert trend["sma_50_above_sma_200"] is True
    assert trend["price_vs_sma_20"] is not None
    assert trend["price_vs_sma_20"] > 0


def test_build_benchmark_comparison_and_fundamentals_summary() -> None:
    company_history = prepare_price_history(_price_history_frame(ticker="AAPL", periods=260, start_price=100, step=1), "AAPL")
    benchmark_history = prepare_price_history(_price_history_frame(ticker="SPY", periods=260, start_price=100, step=0.5), "SPY")
    fundamentals = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "as_of_date": ["2026-03-30"],
            "long_name": ["Apple Inc."],
            "sector": ["Technology"],
            "industry": ["Consumer Electronics"],
            "currency": ["USD"],
            "market_cap": [3_000_000_000],
            "enterprise_value": [3_100_000_000],
            "revenue_ttm": [1_000_000_000],
            "gross_margin": [0.45],
            "operating_margin": [0.3],
            "profit_margin": [0.2],
            "free_cash_flow": [250_000_000],
            "return_on_equity": [0.8],
            "return_on_assets": [0.2],
            "trailing_pe": [30],
            "forward_pe": [25],
            "ev_to_ebitda": [18],
            "price_to_sales": [6],
            "dividend_yield": [0.005],
            "total_cash": [80_000_000],
            "total_debt": [50_000_000],
            "current_ratio": [1.1],
            "debt_to_equity": [80],
        }
    )

    comparison = build_benchmark_comparison(company_history, {"SPY": benchmark_history})
    summary = build_fundamentals_summary(fundamentals, "AAPL")

    one_month = comparison.loc[
        (comparison["benchmark_ticker"] == "SPY") & (comparison["window"] == "1m")
    ].iloc[0]

    assert one_month["excess_return"] > 0
    assert summary["long_name"] == "Apple Inc."
    assert summary["net_cash"] == 30_000_000
    assert summary["free_cash_flow_margin"] == 0.25


def test_build_peer_valuation_comparison_and_summary_stats() -> None:
    peer_sets = pd.DataFrame(
        [
            {
                "peer_set_id": "peer_set::AAPL",
                "ticker": "AAPL",
                "peer_ticker": "MSFT",
                "relationship_type": "sector",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "selection_method": "sector_market_cap_proximity",
                "peer_order": 1,
            },
            {
                "peer_set_id": "peer_set::AAPL",
                "ticker": "AAPL",
                "peer_ticker": "DELL",
                "relationship_type": "sector",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "selection_method": "sector_market_cap_proximity",
                "peer_order": 2,
            },
        ]
    )
    fundamentals = pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "DELL"],
            "as_of_date": ["2026-03-30", "2026-03-30", "2026-03-30"],
            "period_type": ["ttm", "ttm", "ttm"],
            "long_name": ["Apple Inc.", "Microsoft Corp.", "Dell Technologies"],
            "sector": ["Technology", "Technology", "Technology"],
            "industry": ["Consumer Electronics", "Software", "Computer Hardware"],
            "market_cap": [3000.0, 3100.0, 120.0],
            "free_cash_flow": [100.0, 120.0, 10.0],
            "forward_pe": [25.0, 30.0, 12.0],
            "ev_to_ebitda": [18.0, 20.0, 8.0],
            "price_to_sales": [6.0, 10.0, 1.5],
        }
    )

    comparison = build_peer_valuation_comparison("AAPL", peer_sets, fundamentals)
    summary = build_peer_summary_stats(comparison)

    assert comparison["ticker"].tolist() == ["AAPL", "MSFT", "DELL"]
    assert bool(comparison.loc[0, "is_focal_company"])
    assert comparison.loc[1, "relationship_type"] == "sector"
    assert round(float(summary["median_forward_pe"]), 2) == 21.0
    assert summary["peer_count"] == 2


def test_build_peer_return_comparison_and_summary_stats() -> None:
    peer_sets = pd.DataFrame(
        [
            {
                "peer_set_id": "peer_set::AAPL",
                "ticker": "AAPL",
                "peer_ticker": "MSFT",
                "relationship_type": "sector",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "selection_method": "sector_market_cap_proximity",
                "peer_order": 1,
            },
            {
                "peer_set_id": "peer_set::AAPL",
                "ticker": "AAPL",
                "peer_ticker": "DELL",
                "relationship_type": "sector",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "selection_method": "sector_market_cap_proximity",
                "peer_order": 2,
            },
        ]
    )
    prices = pd.concat(
        [
            _price_history_frame(ticker="AAPL", periods=260, start_price=100, step=1),
            _price_history_frame(ticker="MSFT", periods=260, start_price=100, step=0.8),
            _price_history_frame(ticker="DELL", periods=260, start_price=100, step=0.2),
        ],
        ignore_index=True,
    )

    comparison = build_peer_return_comparison("AAPL", peer_sets, prices)
    summary = build_peer_return_summary_stats(comparison)

    one_year = comparison.loc[comparison["window"] == "1y"].copy()
    assert one_year["ticker"].tolist() == ["AAPL", "MSFT", "DELL"]
    assert float(one_year.loc[one_year["ticker"] == "AAPL", "excess_return"].iloc[0]) == 0.0
    assert summary["peer_count"] == 2
    assert summary["best_peer_ticker"] == "MSFT"
