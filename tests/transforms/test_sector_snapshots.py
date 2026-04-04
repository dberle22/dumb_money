from __future__ import annotations

import pandas as pd

from dumb_money.transforms.sector_snapshots import build_sector_snapshots_frame


def test_build_sector_snapshots_frame_summarizes_sector_medians_and_benchmark() -> None:
    security_master = pd.DataFrame(
        [
            {"ticker": "AAPL", "sector": "Technology", "industry": "Consumer Electronics", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "MSFT", "sector": "Technology", "industry": "Software", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "JPM", "sector": "Financials", "industry": "Banks", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
        ]
    )
    fundamentals = pd.DataFrame(
        [
            {"ticker": "AAPL", "as_of_date": "2026-03-31", "period_type": "ttm", "long_name": "Apple Inc.", "sector": "Technology", "industry": "Consumer Electronics", "market_cap": 3000.0, "free_cash_flow": 100.0, "forward_pe": 25.0, "ev_to_ebitda": 18.0, "price_to_sales": 6.0, "operating_margin": 0.30, "gross_margin": 0.45},
            {"ticker": "MSFT", "as_of_date": "2026-03-31", "period_type": "ttm", "long_name": "Microsoft Corp.", "sector": "Technology", "industry": "Software", "market_cap": 3200.0, "free_cash_flow": 120.0, "forward_pe": 30.0, "ev_to_ebitda": 20.0, "price_to_sales": 10.0, "operating_margin": 0.42, "gross_margin": 0.68},
            {"ticker": "JPM", "as_of_date": "2026-03-31", "period_type": "ttm", "long_name": "JPMorgan Chase", "sector": "Financials", "industry": "Banks", "market_cap": 600.0, "free_cash_flow": 20.0, "forward_pe": 12.0, "ev_to_ebitda": 8.0, "price_to_sales": 2.0, "operating_margin": 0.28, "gross_margin": 0.50},
        ]
    )
    prices = pd.DataFrame(
        [
            *[
                {"ticker": "AAPL", "date": date, "adj_close": 100 + index}
                for index, date in enumerate(pd.bdate_range("2024-01-01", periods=260))
            ],
            *[
                {"ticker": "MSFT", "date": date, "adj_close": 100 + (index * 0.8)}
                for index, date in enumerate(pd.bdate_range("2024-01-01", periods=260))
            ],
            *[
                {"ticker": "JPM", "date": date, "adj_close": 100 + (index * 0.4)}
                for index, date in enumerate(pd.bdate_range("2024-01-01", periods=260))
            ],
        ]
    )
    benchmark_mappings = pd.DataFrame(
        [
            {"ticker": "AAPL", "sector": "Technology", "sector_benchmark": "XLK"},
            {"ticker": "MSFT", "sector": "Technology", "sector_benchmark": "XLK"},
            {"ticker": "JPM", "sector": "Financials", "sector_benchmark": "XLF"},
        ]
    )

    snapshots = build_sector_snapshots_frame(security_master, fundamentals, prices, benchmark_mappings)

    technology = snapshots.loc[snapshots["sector"] == "Technology"].iloc[0]
    assert technology["sector_benchmark"] == "XLK"
    assert technology["company_count"] == 2
    assert technology["companies_with_fundamentals"] == 2
    assert technology["companies_with_prices"] == 2
    assert round(float(technology["median_forward_pe"]), 2) == 27.5
