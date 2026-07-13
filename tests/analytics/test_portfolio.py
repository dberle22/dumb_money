import pandas as pd

from dumb_money.analytics.portfolio import (
    build_candidate_fit_summary,
    build_portfolio_benchmark_comparison,
    build_portfolio_concentration_metrics,
    build_portfolio_exposure,
    build_watchlist_decision_table,
    enrich_portfolio_holdings,
)


def _holdings_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "portfolio_id": ["default", "default", "default"],
            "ticker": ["AAPL", "MSFT", "XOM"],
            "as_of_date": ["2024-07-01", "2024-07-01", "2024-07-01"],
            "quantity": [10, 5, 4],
            "average_cost": [None, None, None],
            "market_value": [5000.0, 3000.0, 2000.0],
            "weight": [0.5, 0.3, 0.2],
            "account_name": [None, None, None],
            "notes": [None, None, None],
        }
    )


def _security_master_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "XOM", "NVDA"],
            "name": ["Apple", "Microsoft", "Exxon", "NVIDIA"],
            "sector": ["Technology", "Technology", "Energy", "Technology"],
            "industry": ["Consumer Electronics", "Software", "Oil & Gas", "Semiconductors"],
            "exchange": ["NASDAQ", "NASDAQ", "NYSE", "NASDAQ"],
            "asset_type": ["common_stock", "common_stock", "common_stock", "common_stock"],
        }
    )


def _prices_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=260)

    def _series(ticker: str, base: float, slope: float) -> pd.DataFrame:
        values = [base + slope * idx for idx in range(len(dates))]
        return pd.DataFrame(
            {
                "ticker": [ticker] * len(dates),
                "date": dates,
                "interval": ["1d"] * len(dates),
                "source": ["yfinance"] * len(dates),
                "currency": ["USD"] * len(dates),
                "open": values,
                "high": [value * 1.01 for value in values],
                "low": [value * 0.99 for value in values],
                "close": values,
                "adj_close": values,
                "volume": [1_000_000] * len(dates),
            }
        )

    return pd.concat(
        [
            _series("AAPL", 100, 0.4),
            _series("MSFT", 90, 0.3),
            _series("XOM", 70, 0.1),
            _series("SPY", 95, 0.2),
            _series("QQQ", 96, 0.25),
            _series("IWM", 80, 0.15),
        ],
        ignore_index=True,
    )


def test_enrich_portfolio_holdings_preserves_weights_and_adds_metadata() -> None:
    enriched = enrich_portfolio_holdings(_holdings_frame(), _security_master_frame())

    assert "sector" in enriched.columns
    assert enriched.loc[enriched["ticker"] == "AAPL", "sector"].iloc[0] == "Technology"


def test_build_portfolio_concentration_metrics_summarizes_top_weights() -> None:
    metrics = build_portfolio_concentration_metrics(_holdings_frame())

    assert metrics["holding_count"] == 3
    assert metrics["top_1_weight"] == 0.5
    assert metrics["top_3_weight"] == 1.0


def test_build_portfolio_exposure_groups_by_sector() -> None:
    enriched = enrich_portfolio_holdings(_holdings_frame(), _security_master_frame())
    exposure = build_portfolio_exposure(enriched, by="sector")

    assert exposure.loc[exposure["sector"] == "Technology", "weight"].iloc[0] == 0.8


def test_build_candidate_fit_summary_reports_overlap() -> None:
    summary = build_candidate_fit_summary("NVDA", _holdings_frame(), _security_master_frame())

    assert summary["candidate_sector"] == "Technology"
    assert summary["sector_weight_before"] == 0.8
    assert summary["diversification_role"] == "adds_to_existing_sector"


def test_build_portfolio_benchmark_comparison_produces_portfolio_and_benchmark_returns() -> None:
    comparison = build_portfolio_benchmark_comparison(
        _holdings_frame(),
        _prices_frame(),
        benchmark_tickers=["SPY", "QQQ"],
    )

    assert not comparison.empty
    assert {"portfolio_return", "SPY_return", "QQQ_return"}.issubset(comparison.columns)


def test_build_watchlist_decision_table_includes_gold_fields_when_available() -> None:
    gold_snapshot = pd.DataFrame(
        {
            "ticker": ["NVDA"],
            "total_score": [84.0],
            "valuation_score": [18.0],
            "market_performance_score": [20.0],
            "forward_pe": [32.0],
            "return_1y": [0.24],
        }
    )

    watchlist = build_watchlist_decision_table(
        ["NVDA"],
        _holdings_frame(),
        _security_master_frame(),
        gold_snapshot=gold_snapshot,
    )

    assert watchlist.loc[0, "candidate_ticker"] == "NVDA"
    assert watchlist.loc[0, "total_score"] == 84.0
