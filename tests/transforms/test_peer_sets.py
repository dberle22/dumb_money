from __future__ import annotations

import pandas as pd

from dumb_money.transforms.peer_sets import build_peer_sets_frame


def test_build_peer_sets_frame_prefers_industry_then_market_cap_proximity() -> None:
    security_master = pd.DataFrame(
        [
            {"ticker": "AAPL", "sector": "Technology", "industry": "Consumer Electronics", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "SONY", "sector": "Technology", "industry": "Consumer Electronics", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "HPQ", "sector": "Technology", "industry": "Consumer Electronics", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "DELL", "sector": "Technology", "industry": "Consumer Electronics", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "MSFT", "sector": "Technology", "industry": "Software", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
        ]
    )
    fundamentals = pd.DataFrame(
        [
            {"ticker": "AAPL", "as_of_date": "2026-03-31", "market_cap": 3000.0, "period_type": "ttm"},
            {"ticker": "SONY", "as_of_date": "2026-03-31", "market_cap": 1500.0, "period_type": "ttm"},
            {"ticker": "HPQ", "as_of_date": "2026-03-31", "market_cap": 120.0, "period_type": "ttm"},
            {"ticker": "DELL", "as_of_date": "2026-03-31", "market_cap": 110.0, "period_type": "ttm"},
            {"ticker": "MSFT", "as_of_date": "2026-03-31", "market_cap": 3100.0, "period_type": "ttm"},
        ]
    )

    peer_sets = build_peer_sets_frame(security_master, fundamentals)

    aapl = peer_sets.loc[peer_sets["ticker"] == "AAPL"].copy()
    assert aapl["relationship_type"].tolist() == ["industry", "industry", "industry"]
    assert aapl["selection_method"].tolist() == ["industry_market_cap_proximity"] * 3
    assert aapl["peer_ticker"].tolist() == ["SONY", "HPQ", "DELL"]


def test_build_peer_sets_frame_falls_back_to_sector_and_excludes_ineligible_names() -> None:
    security_master = pd.DataFrame(
        [
            {"ticker": "CRM", "sector": "Technology", "industry": "Software", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "MSFT", "sector": "Technology", "industry": "Software", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "ADBE", "sector": "Technology", "industry": "Software", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": False, "is_benchmark": False},
            {"ticker": "ORCL", "sector": "Technology", "industry": "Software - Infrastructure", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "SNOW", "sector": "Technology", "industry": "Software - Infrastructure", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "QQQ", "sector": "Technology", "industry": None, "asset_type": "etf", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": True},
        ]
    )
    fundamentals = pd.DataFrame(
        [
            {"ticker": "CRM", "as_of_date": "2026-03-31", "market_cap": 250.0, "period_type": "ttm"},
            {"ticker": "MSFT", "as_of_date": "2026-03-31", "market_cap": 3000.0, "period_type": "ttm"},
            {"ticker": "ORCL", "as_of_date": "2026-03-31", "market_cap": 400.0, "period_type": "ttm"},
            {"ticker": "SNOW", "as_of_date": "2026-03-31", "market_cap": 80.0, "period_type": "ttm"},
        ]
    )

    peer_sets = build_peer_sets_frame(security_master, fundamentals)

    crm = peer_sets.loc[peer_sets["ticker"] == "CRM"].copy()
    assert crm["relationship_type"].tolist() == ["sector", "sector", "sector"]
    assert crm["selection_method"].tolist() == ["sector_market_cap_proximity"] * 3
    assert crm["peer_ticker"].tolist() == ["ORCL", "SNOW", "MSFT"]
    assert "ADBE" not in crm["peer_ticker"].tolist()
    assert "QQQ" not in crm["peer_ticker"].tolist()
