from __future__ import annotations

from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.models import Security
from dumb_money.research.company import build_company_research_packet
from dumb_money.storage import write_canonical_table


def test_build_company_research_packet_from_shared_datasets(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()

    prices = pd.read_csv("tests/fixtures/prices/aapl_daily.csv").rename(columns={"adjclose": "adj_close"})
    prices["ticker"] = "AAPL"
    prices["interval"] = "1d"
    prices["source"] = "yfinance"
    prices["currency"] = "USD"
    prices = pd.concat(
        [
            prices,
            prices.assign(ticker="SPY", adj_close=prices["adj_close"] * 0.98, close=prices["close"] * 0.98),
            prices.assign(ticker="QQQ", adj_close=prices["adj_close"] * 1.01, close=prices["close"] * 1.01),
            prices.assign(ticker="MSFT", adj_close=prices["adj_close"] * 1.03, close=prices["close"] * 1.03),
            prices.assign(ticker="DELL", adj_close=prices["adj_close"] * 0.95, close=prices["close"] * 0.95),
        ],
        ignore_index=True,
    )
    write_canonical_table(
        prices[
            ["ticker", "date", "interval", "source", "currency", "open", "high", "low", "close", "adj_close", "volume"]
        ],
        "normalized_prices",
        settings=settings,
    )

    fundamentals = pd.read_csv("tests/fixtures/fundamentals/aapl_fundamentals_flat_2024-06-30.csv")
    write_canonical_table(fundamentals, "normalized_fundamentals", settings=settings)

    benchmark_sets = pd.DataFrame(
        {
            "set_id": ["sample_universe", "sample_universe"],
            "benchmark_id": ["SPY", "QQQ"],
            "ticker": ["SPY", "QQQ"],
            "name": ["SPDR S&P 500 ETF Trust", "Invesco QQQ Trust"],
            "category": ["market", "style"],
            "scope": ["us_large_cap", "us_large_cap_growth"],
            "currency": ["USD", "USD"],
            "description": ["Core US large-cap equity benchmark ETF.", "Nasdaq-100 large-cap growth benchmark ETF."],
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
            "assignment_method": ["test_fixture"],
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
            "median_market_cap": [2200.0],
            "median_forward_pe": [22.0],
            "median_ev_to_ebitda": [16.0],
            "median_price_to_sales": [5.0],
            "median_free_cash_flow_yield": [0.03],
            "median_operating_margin": [0.25],
            "median_gross_margin": [0.48],
            "median_return_6m": [0.12],
            "median_return_1y": [0.30],
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
                primary_listing="Nasdaq Global Select Market",
                currency="USD",
                sector="Technology",
                industry="Consumer Electronics",
                country=None,
                is_benchmark=False,
                is_active=True,
                is_eligible_research_universe=True,
                source="test",
                source_id="AAPL",
                first_seen_at="2026-03-31",
                last_updated_at="2026-03-31",
            ).model_dump(mode="json"),
            Security(
                security_id="sec_msft",
                ticker="MSFT",
                name="Microsoft Corp.",
                asset_type="common_stock",
                exchange="Nasdaq",
                primary_listing="Nasdaq Global Select Market",
                currency="USD",
                sector="Technology",
                industry="Software",
                country=None,
                is_benchmark=False,
                is_active=True,
                is_eligible_research_universe=True,
                source="test",
                source_id="MSFT",
                first_seen_at="2026-03-31",
                last_updated_at="2026-03-31",
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
                country=None,
                is_benchmark=False,
                is_active=True,
                is_eligible_research_universe=True,
                source="test",
                source_id="DELL",
                first_seen_at="2026-03-31",
                last_updated_at="2026-03-31",
            ).model_dump(mode="json"),
            Security(
                security_id="sec_spy",
                ticker="SPY",
                name="SPDR S&P 500 ETF Trust",
                asset_type="etf",
                exchange="NYSE Arca",
                primary_listing="NYSE Arca",
                currency="USD",
                is_benchmark=True,
                is_active=True,
                is_eligible_research_universe=False,
                source="test",
                source_id="SPY",
                first_seen_at="2026-03-31",
                last_updated_at="2026-03-31",
            ).model_dump(mode="json"),
            Security(
                security_id="sec_qqq",
                ticker="QQQ",
                name="Invesco QQQ Trust",
                asset_type="etf",
                exchange="Nasdaq",
                primary_listing="Nasdaq Global Market",
                currency="USD",
                is_benchmark=True,
                is_active=True,
                is_eligible_research_universe=False,
                source="test",
                source_id="QQQ",
                first_seen_at="2026-03-31",
                last_updated_at="2026-03-31",
            ).model_dump(mode="json"),
        ]
    )
    write_canonical_table(security_master, "security_master", settings=settings)

    packet = build_company_research_packet("AAPL", benchmark_set_id="sample_universe", settings=settings)

    assert packet.ticker == "AAPL"
    assert packet.company_name == "Apple Inc."
    assert packet.as_of_date == "2024-06-30"
    assert not packet.company_history.empty
    assert set(packet.benchmark_histories) == {"SPY", "QQQ"}
    assert packet.return_windows["window"].tolist() == ["1m", "3m", "6m", "1y"]
    assert packet.trailing_return_comparison["window"].tolist() == ["1m", "3m", "6m", "1y"]
    assert packet.benchmark_comparison["benchmark_ticker"].tolist() == ["SPY", "SPY", "SPY", "SPY", "QQQ", "QQQ", "QQQ", "QQQ"]
    assert "net_cash" in packet.fundamentals_summary
    assert packet.scorecard.summary["primary_benchmark"] == "QQQ"
    assert packet.scorecard.summary["secondary_benchmark"] == "SPY"
    assert packet.peer_summary_stats["peer_count"] == 2
    assert packet.peer_return_summary_stats["peer_count"] == 2
    assert packet.peer_return_comparison["window"].tolist()[:4] == ["1m", "3m", "6m", "1y"]
    assert packet.peer_valuation_comparison["ticker"].tolist()[0] == "AAPL"
    assert packet.sector_snapshot["sector_benchmark"] == "XLK"
    assert packet.scorecard.summary["total_score"] >= 0


def test_build_company_research_packet_requires_company_price_history(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()
    Path(settings.normalized_prices_dir / "normalized_prices.csv").write_text("ticker,date,interval,source,currency,open,high,low,close,adj_close,volume\n")

    try:
        build_company_research_packet("AAPL", settings=settings)
    except ValueError as exc:
        assert "AAPL" in str(exc)
    else:
        raise AssertionError("expected build_company_research_packet to require staged price history")
