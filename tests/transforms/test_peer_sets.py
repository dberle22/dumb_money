from __future__ import annotations

import pandas as pd
from pandas.testing import assert_frame_equal

from dumb_money.config import AppSettings
from dumb_money.transforms.peer_sets import build_peer_sets_frame, normalize_curated_peer_sets
from dumb_money.transforms.peer_sets import stage_peer_sets


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
    assert aapl["peer_source"].tolist() == ["automatic"] * 3
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
    assert crm["peer_source"].tolist() == ["automatic"] * 3
    assert crm["relationship_type"].tolist() == ["sector", "sector", "sector"]
    assert crm["selection_method"].tolist() == ["sector_market_cap_proximity"] * 3
    assert crm["peer_ticker"].tolist() == ["ORCL", "SNOW", "MSFT"]
    assert "ADBE" not in crm["peer_ticker"].tolist()
    assert "QQQ" not in crm["peer_ticker"].tolist()


def test_build_peer_sets_frame_appends_curated_peers_with_source_flag() -> None:
    security_master = pd.DataFrame(
        [
            {"ticker": "AAPL", "sector": "Technology", "industry": "Consumer Electronics", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "SONY", "sector": "Technology", "industry": "Consumer Electronics", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "HPQ", "sector": "Technology", "industry": "Consumer Electronics", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "DELL", "sector": "Technology", "industry": "Consumer Electronics", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "MSFT", "sector": "Technology", "industry": "Software", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
            {"ticker": "META", "sector": "Technology", "industry": "Internet Content & Information", "asset_type": "common_stock", "is_active": True, "is_eligible_research_universe": True, "is_benchmark": False},
        ]
    )
    fundamentals = pd.DataFrame(
        [
            {"ticker": "AAPL", "as_of_date": "2026-03-31", "market_cap": 3000.0, "period_type": "ttm"},
            {"ticker": "SONY", "as_of_date": "2026-03-31", "market_cap": 1500.0, "period_type": "ttm"},
            {"ticker": "HPQ", "as_of_date": "2026-03-31", "market_cap": 120.0, "period_type": "ttm"},
            {"ticker": "DELL", "as_of_date": "2026-03-31", "market_cap": 110.0, "period_type": "ttm"},
            {"ticker": "MSFT", "as_of_date": "2026-03-31", "market_cap": 3100.0, "period_type": "ttm"},
            {"ticker": "META", "as_of_date": "2026-03-31", "market_cap": 1700.0, "period_type": "ttm"},
        ]
    )
    curated = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "peer_ticker": "MSFT",
                "relationship_type": "curated",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "selection_method": "curated_mega_cap_tech_peer_list",
                "peer_order": 1,
            },
            {
                "ticker": "AAPL",
                "peer_ticker": "META",
                "relationship_type": "curated",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "selection_method": "curated_mega_cap_tech_peer_list",
                "peer_order": 2,
            },
        ]
    )

    peer_sets = build_peer_sets_frame(security_master, fundamentals, curated_peer_sets=curated)

    aapl = peer_sets.loc[peer_sets["ticker"] == "AAPL"].copy()
    assert aapl["peer_source"].tolist() == ["automatic", "automatic", "automatic", "curated", "curated"]
    assert aapl.loc[aapl["peer_source"] == "curated", "peer_ticker"].tolist() == ["MSFT", "META"]


def test_normalize_curated_peer_sets_sets_canonical_columns() -> None:
    curated = pd.DataFrame(
        [
            {
                "ticker": "aapl",
                "peer_ticker": "msft",
                "relationship_type": "Curated",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "selection_method": "manual_list",
                "peer_order": 1,
            }
        ]
    )

    normalized = normalize_curated_peer_sets(curated)

    assert normalized.loc[0, "peer_set_id"] == "peer_set::AAPL"
    assert normalized.loc[0, "peer_source"] == "curated"
    assert normalized.loc[0, "ticker"] == "AAPL"
    assert normalized.loc[0, "peer_ticker"] == "MSFT"


def test_stage_peer_sets_merges_curated_reference_rows(tmp_path, monkeypatch) -> None:
    settings = AppSettings(project_root=tmp_path)
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
    curated_path = settings.reference_dir / "curated_peer_sets.csv"
    settings.ensure_directories()
    curated_path.write_text(
        "\n".join(
            [
                "ticker,peer_ticker,relationship_type,sector,industry,selection_method,peer_order",
                "AAPL,MSFT,curated,Technology,Consumer Electronics,curated_mega_cap_tech_peer_list,1",
            ]
        )
            + "\n"
        )

    captured: dict[str, pd.DataFrame] = {}

    def _read_table(table_name: str, *, settings: AppSettings | None = None) -> pd.DataFrame:
        if table_name == "security_master":
            return security_master
        if table_name == "normalized_fundamentals":
            return fundamentals
        raise AssertionError(table_name)

    def _write_table(frame: pd.DataFrame, table_name: str, *, settings: AppSettings | None = None) -> pd.DataFrame:
        captured[table_name] = frame.copy()
        return frame

    monkeypatch.setattr("dumb_money.transforms.peer_sets.read_canonical_table", _read_table)
    monkeypatch.setattr("dumb_money.transforms.peer_sets.write_canonical_table", _write_table)

    peer_sets = stage_peer_sets(settings=settings, write_csv=False)
    loaded = captured["peer_sets"]

    assert not peer_sets.empty
    assert "curated" in peer_sets["peer_source"].tolist()
    assert "automatic" in peer_sets["peer_source"].tolist()
    assert_frame_equal(loaded, peer_sets, check_dtype=False)
