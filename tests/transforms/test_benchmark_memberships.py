from __future__ import annotations

from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.transforms.benchmark_memberships import (
    build_benchmark_definitions_from_mapping,
    build_benchmark_membership_coverage_frame,
    build_benchmark_memberships_frame,
    filter_real_security_members,
    get_real_benchmark_member_tickers,
    normalize_benchmark_mapping_frame,
    stage_benchmark_definition_refresh,
    stage_benchmark_membership_coverage,
    stage_benchmark_memberships,
)
from dumb_money.transforms.security_master import stage_security_master
from dumb_money.transforms.security_universe import stage_listed_security_seed, stage_security_master_overrides


def _write_spdr_fixture(path: Path, ticker: str, name: str, holdings: pd.DataFrame) -> None:
    metadata = pd.DataFrame(
        [
            ["Fund Name:", name],
            ["Ticker Symbol:", ticker],
            ["Holdings:", "As of 30-Mar-2026"],
            [None, None],
        ]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        metadata.to_excel(writer, sheet_name="holdings", index=False, header=False)
        holdings.to_excel(writer, sheet_name="holdings", index=False, startrow=4)


def test_build_benchmark_definitions_and_memberships_from_mapping(tmp_path) -> None:
    holdings_dir = tmp_path / "benchmark_holdings"
    holdings_dir.mkdir(parents=True)
    mapping_path = holdings_dir / "etf_benchmark_mapping.csv"

    _write_spdr_fixture(
        holdings_dir / "spy_holdings.xlsx",
        "SPY",
        "State Street SPDR S&P 500 ETF Trust",
        pd.DataFrame(
            {
                "Name": ["Apple Inc.", "Microsoft Corp"],
                "Ticker": ["AAPL", "MSFT"],
                "Identifier": ["037833100", "594918104"],
                "SEDOL": ["2046251", "2588173"],
                "Weight": [7.0, 6.0],
                "Sector": ["-", "-"],
                "Shares Held": [1, 1],
                "Local Currency": ["USD", "USD"],
            }
        ),
    )
    (holdings_dir / "russell_2000_holdings.csv").write_text(
        "iShares Russell 2000 ETF\n"
        "Fund Holdings as of,\"Mar 30, 2026\"\n"
        "Inception Date,\"May 22, 2000\"\n"
        "Shares Outstanding,\"100\"\n"
        "Stock,\"-\"\n"
        "Bond,\"-\"\n"
        "Cash,\"-\"\n"
        "Other,\"-\"\n"
        "\n"
        "Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Quantity,Price,Location,Exchange,Currency,FX Rate,Market Currency,Accrual Date\n"
        "BE,BLOOM ENERGY CLASS A CORP,Industrials,Equity,1,0.92,1,1,1,United States,NYSE,USD,1,USD,-\n"
        "FN,FABRINET,Information Technology,Equity,1,0.63,1,1,1,United States,NYSE,USD,1,USD,-\n"
    )
    mapping_path.write_text(
        "ticker,name,path,benchmark,sector,industry\n"
        "SPY,State Street SPDR S&P 500 ETF Trust,spy_holdings.xlsx,S&P 500,,\n"
        "IWM,iShares Russell 2000 ETF,russell_2000_holdings.csv,Russell 2000,,\n"
    )

    mapping = normalize_benchmark_mapping_frame(pd.read_csv(mapping_path))
    definitions = build_benchmark_definitions_from_mapping(mapping)
    memberships = build_benchmark_memberships_frame(mapping, base_dir=holdings_dir)

    assert definitions["benchmark_id"].tolist() == ["IWM", "SPY"]
    assert sorted(memberships["benchmark_id"].unique().tolist()) == ["IWM", "SPY"]
    assert sorted(memberships["member_ticker"].tolist()) == ["AAPL", "BE", "FN", "MSFT"]


def test_stage_benchmark_memberships_and_coverage_write_outputs(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    holdings_dir = settings.raw_benchmark_holdings_dir
    holdings_dir.mkdir(parents=True)
    mapping_path = holdings_dir / "etf_benchmark_mapping.csv"

    _write_spdr_fixture(
        holdings_dir / "spy_holdings.xlsx",
        "SPY",
        "State Street SPDR S&P 500 ETF Trust",
        pd.DataFrame(
            {
                "Name": ["Apple Inc.", "Microsoft Corp"],
                "Ticker": ["AAPL", "MSFT"],
                "Identifier": ["037833100", "594918104"],
                "SEDOL": ["2046251", "2588173"],
                "Weight": [7.0, 6.0],
                "Sector": ["-", "-"],
                "Shares Held": [1, 1],
                "Local Currency": ["USD", "USD"],
            }
        ),
    )
    mapping_path.write_text(
        "ticker,name,path,benchmark,sector,industry\n"
        "SPY,State Street SPDR S&P 500 ETF Trust,spy_holdings.xlsx,S&P 500,,\n"
    )

    stage_listed_security_seed(
        nasdaq_listed_paths=["tests/fixtures/universe/nasdaqlisted_sample.txt"],
        other_listed_paths=["tests/fixtures/universe/otherlisted_sample.txt"],
        settings=settings,
        as_of_date="2026-03-31",
    )
    stage_security_master_overrides(settings=settings)
    stage_security_master(settings=settings, write_csv=False)

    definitions = stage_benchmark_definition_refresh(mapping_path=mapping_path, settings=settings)
    memberships = stage_benchmark_memberships(mapping_path=mapping_path, settings=settings)
    coverage = stage_benchmark_membership_coverage(mapping_path=mapping_path, settings=settings)

    assert definitions["benchmark_id"].tolist() == ["SPY"]
    assert memberships["member_ticker"].tolist() == ["AAPL", "MSFT"]
    assert coverage["member_ticker"].tolist() == ["AAPL", "MSFT"]
    assert coverage["is_in_security_master"].tolist() == [False, True]
    assert (settings.benchmark_definitions_dir / "benchmark_definitions.csv").exists()
    assert (settings.benchmark_memberships_dir / "benchmark_memberships.csv").exists()
    assert (settings.benchmark_membership_coverage_dir / "benchmark_membership_coverage.csv").exists()


def test_build_benchmark_membership_coverage_frame_flags_missing_members() -> None:
    definitions = pd.DataFrame(
        [
            {
                "benchmark_id": "SPY",
                "ticker": "SPY",
                "name": "SPY",
                "category": "market",
                "scope": "us_large_cap",
                "currency": "USD",
                "inception_date": None,
                "description": "S&P 500",
            }
        ]
    )
    memberships = pd.DataFrame(
        [
            {
                "benchmark_id": "SPY",
                "benchmark_ticker": "SPY",
                "member_ticker": "AAPL",
                "member_name": "Apple Inc.",
                "member_weight": 7.0,
                "member_sector": "-",
                "asset_class": "Equity",
                "exchange": "NASDAQ",
                "currency": "USD",
                "as_of_date": "Mar 30, 2026",
                "source": "benchmark_holdings_snapshot",
                "source_file": "spy_holdings.xlsx",
            },
            {
                "benchmark_id": "SPY",
                "benchmark_ticker": "SPY",
                "member_ticker": "ZZZZ",
                "member_name": "Missing Co",
                "member_weight": 0.1,
                "member_sector": "-",
                "asset_class": "Equity",
                "exchange": "NASDAQ",
                "currency": "USD",
                "as_of_date": "Mar 30, 2026",
                "source": "benchmark_holdings_snapshot",
                "source_file": "spy_holdings.xlsx",
            },
        ]
    )
    security_master = pd.DataFrame(
        [
            {
                "security_id": "sec_aapl",
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "asset_type": "common_stock",
                "exchange": "Nasdaq",
                "primary_listing": "Nasdaq",
                "currency": "USD",
                "sector": "Technology",
                "industry": None,
                "country": None,
                "cik": None,
                "is_benchmark": False,
                "is_active": True,
                "is_eligible_research_universe": True,
                "source": "test",
                "source_id": "AAPL",
                "first_seen_at": None,
                "last_updated_at": None,
                "notes": None,
            }
        ]
    )
    mapping = pd.DataFrame(
        [{"ticker": "SPY", "name": "SPY", "path": "spy_holdings.xlsx", "benchmark": "S&P 500", "sector": None, "industry": None}]
    )

    coverage = build_benchmark_membership_coverage_frame(definitions, memberships, security_master, mapping)

    assert coverage["is_in_security_master"].tolist() == [True, False]


def test_get_real_benchmark_member_tickers_filters_cash_and_footer_rows() -> None:
    memberships = pd.DataFrame(
        [
            {
                "benchmark_id": "DIA",
                "benchmark_ticker": "DIA",
                "member_ticker": "AAPL",
                "member_name": "Apple Inc.",
                "member_weight": 3.0,
                "member_sector": "Technology",
                "asset_class": "Equity",
                "exchange": "NASDAQ",
                "currency": "USD",
                "as_of_date": "Mar 30, 2026",
                "source": "benchmark_holdings_snapshot",
                "source_file": "dia_holdings.xlsx",
            },
            {
                "benchmark_id": "DIA",
                "benchmark_ticker": "DIA",
                "member_ticker": "-",
                "member_name": "US Dollar",
                "member_weight": 0.1,
                "member_sector": None,
                "asset_class": "Equity",
                "exchange": None,
                "currency": "USD",
                "as_of_date": "Mar 30, 2026",
                "source": "benchmark_holdings_snapshot",
                "source_file": "dia_holdings.xlsx",
            },
            {
                "benchmark_id": "DIA",
                "benchmark_ticker": "DIA",
                "member_ticker": "NAN",
                "member_name": "Footer artifact",
                "member_weight": 0.0,
                "member_sector": None,
                "asset_class": "Equity",
                "exchange": None,
                "currency": "USD",
                "as_of_date": "Mar 30, 2026",
                "source": "benchmark_holdings_snapshot",
                "source_file": "dia_holdings.xlsx",
            },
            {
                "benchmark_id": "DIA",
                "benchmark_ticker": "DIA",
                "member_ticker": "MSFT",
                "member_name": "Microsoft Corp.",
                "member_weight": 5.0,
                "member_sector": "Technology",
                "asset_class": "Equity",
                "exchange": "NASDAQ",
                "currency": "USD",
                "as_of_date": "Mar 30, 2026",
                "source": "benchmark_holdings_snapshot",
                "source_file": "dia_holdings.xlsx",
            },
        ]
    )

    filtered = filter_real_security_members(memberships)

    assert filtered["member_ticker"].tolist() == ["AAPL", "MSFT"]
    assert get_real_benchmark_member_tickers(memberships, benchmark_ticker="dia") == ["AAPL", "MSFT"]
