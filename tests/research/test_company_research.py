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

    benchmark_prices = prices.loc[prices["ticker"].isin(["SPY", "QQQ"])].copy()
    benchmark_prices.to_csv(
        settings.raw_benchmarks_dir / "sample_universe_benchmark_prices_20240102_20240103_1d.csv",
        index=False,
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
    assert packet.scorecard.summary["primary_benchmark"] == "SPY"
    assert packet.scorecard.summary["secondary_benchmark"] == "QQQ"
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
