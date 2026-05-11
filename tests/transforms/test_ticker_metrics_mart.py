from __future__ import annotations

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.models import Security
from dumb_money.research.company import (
    load_gold_scorecard_metric_rows_for_ticker,
    load_gold_ticker_metrics_row,
)
from dumb_money.storage import read_canonical_table, write_canonical_table
from dumb_money.transforms import stage_gold_ticker_metrics_mart
from dumb_money.transforms.scorecard_metric_rows_mart import stage_gold_scorecard_metric_rows


def _price_frame(ticker: str, dates: pd.DatetimeIndex, base_price: float, slope: float) -> pd.DataFrame:
    values = [base_price + slope * index for index in range(len(dates))]
    return pd.DataFrame(
        {
            "ticker": ticker,
            "date": dates.date,
            "interval": "1d",
            "source": "yfinance",
            "currency": "USD",
            "open": values,
            "high": [value * 1.01 for value in values],
            "low": [value * 0.99 for value in values],
            "close": values,
            "adj_close": values,
            "volume": [1_000_000] * len(values),
        }
    )


def _write_mart_inputs(settings: AppSettings) -> None:
    dates = pd.bdate_range("2024-01-02", periods=320)
    prices = pd.concat(
        [
            _price_frame("AAPL", dates, base_price=100.0, slope=0.40),
            _price_frame("QQQ", dates, base_price=95.0, slope=0.28),
            _price_frame("SPY", dates, base_price=94.0, slope=0.22),
            _price_frame("MSFT", dates, base_price=102.0, slope=0.32),
            _price_frame("DELL", dates, base_price=70.0, slope=0.18),
        ],
        ignore_index=True,
    )
    write_canonical_table(prices, "normalized_prices", settings=settings)

    fundamentals = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-03-31",
                "report_date": "2024-05-02",
                "fiscal_year": 2024,
                "fiscal_quarter": 1,
                "fiscal_period": "Q1",
                "period_type": "quarterly",
                "source": "fixture",
                "currency": "USD",
                "long_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "market_cap": 3_000_000_000_000.0,
                "enterprise_value": 3_100_000_000_000.0,
                "revenue": 90_000_000_000.0,
                "revenue_ttm": None,
                "gross_profit": 40_000_000_000.0,
                "operating_income": 28_000_000_000.0,
                "net_income": 22_000_000_000.0,
                "ebitda": 32_000_000_000.0,
                "free_cash_flow": 24_000_000_000.0,
                "total_debt": 108_000_000_000.0,
                "total_cash": 62_000_000_000.0,
                "current_assets": 140_000_000_000.0,
                "current_liabilities": 95_000_000_000.0,
                "shares_outstanding": 15_500_000_000.0,
                "eps_trailing": 6.1,
                "eps_forward": 6.4,
                "gross_margin": 0.444,
                "operating_margin": 0.311,
                "profit_margin": 0.245,
                "return_on_equity": 1.45,
                "return_on_assets": 0.23,
                "debt_to_equity": 1.7,
                "current_ratio": 1.47,
                "trailing_pe": 31.0,
                "forward_pe": 28.0,
                "price_to_sales": 7.5,
                "ev_to_ebitda": 18.2,
                "dividend_yield": 0.005,
                "pretax_income": 27_000_000_000.0,
                "tax_provision": 5_000_000_000.0,
                "tax_rate_for_calcs": 0.19,
                "effective_tax_rate": 0.185,
                "nopat": 21_870_000_000.0,
                "interest_expense": 1_100_000_000.0,
                "total_assets": 360_000_000_000.0,
                "total_equity_gross_minority_interest": 65_000_000_000.0,
                "stockholders_equity": 64_000_000_000.0,
                "invested_capital": 170_000_000_000.0,
                "working_capital": 45_000_000_000.0,
                "basic_eps": 1.40,
                "diluted_eps": 1.38,
                "return_on_invested_capital": 0.129,
                "raw_payload_path": "data/raw/fundamentals/aapl_q1.json",
                "pulled_at": "2024-12-31T00:00:00Z",
            },
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-06-30",
                "report_date": "2024-08-01",
                "fiscal_year": 2024,
                "fiscal_quarter": 2,
                "fiscal_period": "Q2",
                "period_type": "quarterly",
                "source": "fixture",
                "currency": "USD",
                "long_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "revenue": 94_000_000_000.0,
                "gross_profit": 42_000_000_000.0,
                "operating_income": 29_000_000_000.0,
                "net_income": 23_000_000_000.0,
                "free_cash_flow": 25_000_000_000.0,
                "shares_outstanding": 15_500_000_000.0,
                "gross_margin": 0.447,
                "operating_margin": 0.309,
                "return_on_equity": 1.46,
                "return_on_assets": 0.23,
                "return_on_invested_capital": 0.130,
                "raw_payload_path": "data/raw/fundamentals/aapl_q2.json",
                "pulled_at": "2024-12-31T00:00:00Z",
            },
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-09-30",
                "report_date": "2024-11-01",
                "fiscal_year": 2024,
                "fiscal_quarter": 3,
                "fiscal_period": "Q3",
                "period_type": "quarterly",
                "source": "fixture",
                "currency": "USD",
                "long_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "revenue": 97_000_000_000.0,
                "gross_profit": 43_800_000_000.0,
                "operating_income": 30_000_000_000.0,
                "net_income": 24_000_000_000.0,
                "free_cash_flow": 26_500_000_000.0,
                "shares_outstanding": 15_500_000_000.0,
                "gross_margin": 0.451,
                "operating_margin": 0.309,
                "return_on_equity": 1.48,
                "return_on_assets": 0.24,
                "return_on_invested_capital": 0.132,
                "raw_payload_path": "data/raw/fundamentals/aapl_q3.json",
                "pulled_at": "2024-12-31T00:00:00Z",
            },
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-12-31",
                "report_date": "2025-02-01",
                "fiscal_year": 2024,
                "fiscal_quarter": 4,
                "fiscal_period": "Q4",
                "period_type": "quarterly",
                "source": "fixture",
                "currency": "USD",
                "long_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "revenue": 100_000_000_000.0,
                "gross_profit": 45_200_000_000.0,
                "operating_income": 31_800_000_000.0,
                "net_income": 25_500_000_000.0,
                "free_cash_flow": 28_000_000_000.0,
                "shares_outstanding": 15_500_000_000.0,
                "gross_margin": 0.452,
                "operating_margin": 0.318,
                "return_on_equity": 1.50,
                "return_on_assets": 0.245,
                "return_on_invested_capital": 0.134,
                "raw_payload_path": "data/raw/fundamentals/aapl_q4.json",
                "pulled_at": "2024-12-31T00:00:00Z",
            },
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-12-31",
                "report_date": "2025-02-01",
                "fiscal_year": 2024,
                "fiscal_quarter": 4,
                "fiscal_period": "TTM",
                "period_type": "ttm",
                "source": "fixture",
                "currency": "USD",
                "long_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "market_cap": 3_050_000_000_000.0,
                "enterprise_value": 3_120_000_000_000.0,
                "revenue": 381_000_000_000.0,
                "revenue_ttm": 381_000_000_000.0,
                "gross_profit": 171_000_000_000.0,
                "operating_income": 118_000_000_000.0,
                "net_income": 94_500_000_000.0,
                "ebitda": 131_000_000_000.0,
                "free_cash_flow": 103_000_000_000.0,
                "total_debt": 108_000_000_000.0,
                "total_cash": 62_000_000_000.0,
                "current_assets": 140_000_000_000.0,
                "current_liabilities": 95_000_000_000.0,
                "shares_outstanding": 15_500_000_000.0,
                "eps_trailing": 6.1,
                "eps_forward": 6.4,
                "gross_margin": 0.449,
                "operating_margin": 0.310,
                "profit_margin": 0.248,
                "return_on_equity": 1.52,
                "return_on_assets": 0.24,
                "debt_to_equity": 1.7,
                "current_ratio": 1.47,
                "trailing_pe": 31.0,
                "forward_pe": 28.0,
                "price_to_sales": 7.4,
                "ev_to_ebitda": 18.1,
                "dividend_yield": 0.005,
                "pretax_income": 116_000_000_000.0,
                "tax_provision": 21_500_000_000.0,
                "tax_rate_for_calcs": 0.185,
                "effective_tax_rate": 0.185,
                "nopat": 94_540_000_000.0,
                "interest_expense": 4_200_000_000.0,
                "total_assets": 360_000_000_000.0,
                "total_equity_gross_minority_interest": 65_000_000_000.0,
                "stockholders_equity": 64_000_000_000.0,
                "invested_capital": 170_000_000_000.0,
                "working_capital": 45_000_000_000.0,
                "basic_eps": 6.1,
                "diluted_eps": 6.0,
                "return_on_invested_capital": 0.556,
                "raw_payload_path": "data/raw/fundamentals/aapl_ttm.json",
                "pulled_at": "2024-12-31T00:00:00Z",
            },
            {
                "ticker": "MSFT",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-12-31",
                "report_date": "2025-02-01",
                "fiscal_year": 2024,
                "fiscal_quarter": 4,
                "fiscal_period": "TTM",
                "period_type": "ttm",
                "source": "fixture",
                "currency": "USD",
                "long_name": "Microsoft Corp.",
                "sector": "Technology",
                "industry": "Software",
                "market_cap": 2_800_000_000_000.0,
                "enterprise_value": 2_850_000_000_000.0,
                "revenue": 245_000_000_000.0,
                "revenue_ttm": 245_000_000_000.0,
                "ebitda": 125_000_000_000.0,
                "free_cash_flow": 89_000_000_000.0,
                "gross_margin": 0.690,
                "operating_margin": 0.450,
                "profit_margin": 0.360,
                "return_on_equity": 0.35,
                "return_on_assets": 0.16,
                "return_on_invested_capital": 0.26,
                "current_ratio": 1.8,
                "debt_to_equity": 0.4,
                "trailing_pe": 34.0,
                "forward_pe": 30.0,
                "price_to_sales": 11.0,
                "ev_to_ebitda": 22.8,
                "dividend_yield": 0.007,
                "raw_payload_path": "data/raw/fundamentals/msft_ttm.json",
                "pulled_at": "2024-12-31T00:00:00Z",
            },
            {
                "ticker": "DELL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-12-31",
                "report_date": "2025-02-01",
                "fiscal_year": 2024,
                "fiscal_quarter": 4,
                "fiscal_period": "TTM",
                "period_type": "ttm",
                "source": "fixture",
                "currency": "USD",
                "long_name": "Dell Technologies",
                "sector": "Technology",
                "industry": "Computer Hardware",
                "market_cap": 80_000_000_000.0,
                "enterprise_value": 95_000_000_000.0,
                "revenue": 91_000_000_000.0,
                "revenue_ttm": 91_000_000_000.0,
                "ebitda": 8_000_000_000.0,
                "free_cash_flow": 4_500_000_000.0,
                "gross_margin": 0.235,
                "operating_margin": 0.081,
                "profit_margin": 0.050,
                "return_on_equity": 0.52,
                "return_on_assets": 0.06,
                "return_on_invested_capital": 0.11,
                "current_ratio": 0.9,
                "debt_to_equity": 3.1,
                "trailing_pe": 18.0,
                "forward_pe": 15.0,
                "price_to_sales": 0.9,
                "ev_to_ebitda": 11.9,
                "dividend_yield": 0.019,
                "raw_payload_path": "data/raw/fundamentals/dell_ttm.json",
                "pulled_at": "2024-12-31T00:00:00Z",
            },
        ]
    )
    write_canonical_table(fundamentals, "normalized_fundamentals", settings=settings)

    benchmark_sets = pd.DataFrame(
        {
            "set_id": ["sample_universe", "sample_universe"],
            "benchmark_id": ["QQQ", "SPY"],
            "ticker": ["QQQ", "SPY"],
            "name": ["Invesco QQQ Trust", "SPDR S&P 500 ETF Trust"],
            "category": ["style", "market"],
            "scope": ["us_large_cap_growth", "us_large_cap"],
            "currency": ["USD", "USD"],
            "description": ["Growth benchmark", "Market benchmark"],
            "member_order": [1, 2],
            "is_default": [True, True],
        }
    )
    write_canonical_table(benchmark_sets, "benchmark_sets", settings=settings)

    benchmark_mappings = pd.DataFrame(
        {
            "mapping_id": ["benchmark_mapping::AAPL"],
            "ticker": ["AAPL"],
            "sector": ["Technology"],
            "industry": ["Consumer Electronics"],
            "primary_benchmark": ["QQQ"],
            "sector_benchmark": ["SPY"],
            "industry_benchmark": [None],
            "style_benchmark": [None],
            "custom_benchmark": [None],
            "assignment_method": ["fixture"],
            "priority": [1],
            "is_active": [True],
            "notes": [None],
        }
    )
    write_canonical_table(benchmark_mappings, "benchmark_mappings", settings=settings)

    peer_sets = pd.DataFrame(
        {
            "peer_set_id": ["peer_set::AAPL", "peer_set::AAPL"],
            "ticker": ["AAPL", "AAPL"],
            "peer_ticker": ["MSFT", "DELL"],
            "peer_source": ["automatic", "automatic"],
            "relationship_type": ["sector", "sector"],
            "sector": ["Technology", "Technology"],
            "industry": ["Consumer Electronics", "Consumer Electronics"],
            "selection_method": ["sector_market_cap_proximity", "sector_market_cap_proximity"],
            "peer_order": [1, 2],
        }
    )
    write_canonical_table(peer_sets, "peer_sets", settings=settings)

    sector_snapshots = pd.DataFrame(
        {
            "sector": ["Technology"],
            "sector_benchmark": ["XLK"],
            "company_count": [3],
            "companies_with_fundamentals": [3],
            "companies_with_prices": [3],
            "median_market_cap": [2_800_000_000_000.0],
            "median_forward_pe": [28.0],
            "median_ev_to_ebitda": [18.1],
            "median_price_to_sales": [7.4],
            "median_free_cash_flow_yield": [0.031],
            "median_operating_margin": [0.31],
            "median_gross_margin": [0.449],
            "median_return_6m": [0.17],
            "median_return_1y": [0.29],
        }
    )
    write_canonical_table(sector_snapshots, "sector_snapshots", settings=settings)

    security_master = pd.DataFrame(
        [
            Security(
                security_id="sec_aapl",
                ticker="AAPL",
                name="Apple Inc.",
                asset_type="common_stock",
                exchange="Nasdaq",
                primary_listing="Nasdaq",
                currency="USD",
                sector="Technology",
                industry="Consumer Electronics",
                is_benchmark=False,
                is_active=True,
                is_eligible_research_universe=True,
                source="fixture",
                source_id="AAPL",
                first_seen_at="2024-12-31",
                last_updated_at="2024-12-31",
            ).model_dump(mode="json"),
            Security(
                security_id="sec_msft",
                ticker="MSFT",
                name="Microsoft Corp.",
                asset_type="common_stock",
                exchange="Nasdaq",
                primary_listing="Nasdaq",
                currency="USD",
                sector="Technology",
                industry="Software",
                is_benchmark=False,
                is_active=True,
                is_eligible_research_universe=True,
                source="fixture",
                source_id="MSFT",
                first_seen_at="2024-12-31",
                last_updated_at="2024-12-31",
            ).model_dump(mode="json"),
            Security(
                security_id="sec_dell",
                ticker="DELL",
                name="Dell Technologies",
                asset_type="common_stock",
                exchange="NYSE",
                primary_listing="NYSE",
                currency="USD",
                sector="Technology",
                industry="Computer Hardware",
                is_benchmark=False,
                is_active=True,
                is_eligible_research_universe=True,
                source="fixture",
                source_id="DELL",
                first_seen_at="2024-12-31",
                last_updated_at="2024-12-31",
            ).model_dump(mode="json"),
        ]
    )
    write_canonical_table(security_master, "security_master", settings=settings)


def test_stage_gold_ticker_metrics_mart_builds_reusable_section_snapshot(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()
    _write_mart_inputs(settings)

    mart = stage_gold_ticker_metrics_mart(settings=settings, tickers=["AAPL"])
    loaded = read_canonical_table("gold_ticker_metrics_mart", settings=settings)
    row = load_gold_ticker_metrics_row("AAPL", settings=settings)

    assert mart.shape[0] == 1
    assert loaded.shape[0] == 1
    assert row["ticker"] == "AAPL"
    assert row["primary_benchmark"] == "QQQ"
    assert row["secondary_benchmark"] == "SPY"
    assert row["peer_count"] == 2
    assert row["selected_history_period_type"] == "quarterly"
    assert row["selected_history_period_count"] == 4
    assert row["latest_period_end_date"] == "2024-12-31"
    assert row["company_name"] == "Apple Inc."
    assert row["total_score"] is not None
    assert row["growth_profitability_score"] is not None
    assert row["return_1y"] is not None
    assert row["price_vs_sma_200"] is not None


def test_stage_gold_scorecard_metric_rows_builds_reusable_metric_artifact(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()
    _write_mart_inputs(settings)

    stage_gold_ticker_metrics_mart(settings=settings, tickers=["AAPL"])
    metric_rows = stage_gold_scorecard_metric_rows(settings=settings, tickers=["AAPL"])
    loaded = read_canonical_table("gold_scorecard_metric_rows", settings=settings)
    ticker_rows = load_gold_scorecard_metric_rows_for_ticker(
        "AAPL",
        score_date=str(metric_rows["score_date"].iloc[0]),
        settings=settings,
    )

    assert not metric_rows.empty
    assert not loaded.empty
    assert not ticker_rows.empty
    assert {"metric_id", "category", "metric_score", "metric_weight"}.issubset(ticker_rows.columns)
    assert "forward_pe" in ticker_rows["metric_id"].tolist()
    assert "return_vs_spy_1y" in ticker_rows["metric_id"].tolist()
