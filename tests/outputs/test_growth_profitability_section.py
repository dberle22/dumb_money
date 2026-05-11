from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from dumb_money.config import AppSettings
from dumb_money.outputs import (
    build_growth_profitability_section_data,
    render_growth_profitability_section,
    save_growth_profitability_section,
)
from dumb_money.outputs.growth_profitability_section import (
    build_growth_profitability_growth_table,
    build_growth_profitability_margin_table,
    build_growth_profitability_return_on_capital_table,
    build_growth_profitability_section_data_from_inputs,
)
from dumb_money.storage import GOLD_TICKER_METRICS_MART_COLUMNS, write_canonical_table


def test_growth_profitability_section_uses_quarterly_history_and_peer_return_proxies() -> None:
    fundamentals = pd.DataFrame(
        [
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-03-31",
                "fiscal_year": 2025,
                "fiscal_quarter": 1,
                "fiscal_period": "Q1",
                "period_type": "quarterly",
                "long_name": "Test Co",
                "sector": "Technology",
                "industry": "Software",
                "revenue": 100.0,
                "gross_profit": 45.0,
                "operating_income": 18.0,
                "net_income": 10.0,
                "free_cash_flow": 14.0,
                "shares_outstanding": 5.0,
                "eps_trailing": None,
                "gross_margin": None,
                "operating_margin": None,
                "return_on_equity": 0.24,
                "return_on_assets": 0.10,
                "return_on_invested_capital": 0.16,
            },
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-06-30",
                "fiscal_year": 2025,
                "fiscal_quarter": 2,
                "fiscal_period": "Q2",
                "period_type": "quarterly",
                "long_name": "Test Co",
                "sector": "Technology",
                "industry": "Software",
                "revenue": 110.0,
                "gross_profit": 52.8,
                "operating_income": 22.0,
                "net_income": 11.0,
                "free_cash_flow": 17.6,
                "shares_outstanding": 5.0,
                "eps_trailing": None,
                "gross_margin": None,
                "operating_margin": None,
                "return_on_equity": 0.25,
                "return_on_assets": 0.11,
                "return_on_invested_capital": 0.18,
            },
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-09-30",
                "fiscal_year": 2025,
                "fiscal_quarter": 3,
                "fiscal_period": "Q3",
                "period_type": "quarterly",
                "long_name": "Test Co",
                "sector": "Technology",
                "industry": "Software",
                "revenue": 124.0,
                "gross_profit": 60.8,
                "operating_income": 26.0,
                "net_income": 12.4,
                "free_cash_flow": 21.1,
                "shares_outstanding": 5.0,
                "eps_trailing": None,
                "gross_margin": None,
                "operating_margin": None,
                "return_on_equity": 0.27,
                "return_on_assets": 0.12,
                "return_on_invested_capital": 0.20,
            },
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-12-31",
                "fiscal_year": 2025,
                "fiscal_quarter": 4,
                "fiscal_period": "Q4",
                "period_type": "quarterly",
                "long_name": "Test Co",
                "sector": "Technology",
                "industry": "Software",
                "revenue": 140.0,
                "gross_profit": 70.0,
                "operating_income": 31.5,
                "net_income": 14.0,
                "free_cash_flow": 25.2,
                "shares_outstanding": 5.0,
                "eps_trailing": None,
                "gross_margin": None,
                "operating_margin": None,
                "return_on_equity": 0.30,
                "return_on_assets": 0.14,
                "return_on_invested_capital": 0.22,
            },
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-12-31",
                "fiscal_year": 2025,
                "fiscal_period": "TTM",
                "period_type": "ttm",
                "long_name": "Test Co",
                "sector": "Technology",
                "industry": "Software",
                "revenue": 474.0,
                "revenue_ttm": 474.0,
                "gross_profit": 228.6,
                "operating_income": 97.5,
                "net_income": 47.4,
                "free_cash_flow": 77.9,
                "shares_outstanding": 5.0,
                "eps_trailing": 9.48,
                "gross_margin": 0.482,
                "operating_margin": 0.206,
                "return_on_equity": 0.32,
                "return_on_assets": 0.15,
                "return_on_invested_capital": 0.24,
            },
            {
                "ticker": "PEER1",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-12-31",
                "fiscal_period": "TTM",
                "period_type": "ttm",
                "return_on_equity": 0.25,
                "return_on_assets": 0.11,
                "return_on_invested_capital": 0.18,
            },
            {
                "ticker": "PEER2",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-12-31",
                "fiscal_period": "TTM",
                "period_type": "ttm",
                "return_on_equity": 0.29,
                "return_on_assets": 0.13,
                "return_on_invested_capital": 0.20,
            },
        ]
    )
    peer_sets = pd.DataFrame(
        [
            {"ticker": "TEST", "peer_ticker": "PEER1"},
            {"ticker": "TEST", "peer_ticker": "PEER2"},
        ]
    )

    data = build_growth_profitability_section_data_from_inputs(
        ticker="TEST",
        company_name="Test Co",
        sector="Technology",
        industry="Software",
        report_date="2026-04-05",
        growth_profitability_score=28.0,
        fundamentals=fundamentals,
        peer_sets=peer_sets,
    )

    growth_table = build_growth_profitability_growth_table(data)
    margin_table = build_growth_profitability_margin_table(data)
    return_table = build_growth_profitability_return_on_capital_table(data)

    assert data.selected_period_type == "quarterly"
    assert growth_table["Period"].tolist() == ["Q1 2025", "Q2 2025", "Q3 2025", "Q4 2025"]
    assert growth_table.loc[1, "Revenue Growth"] == "10.0%"
    assert growth_table.loc[3, "EPS Basis"] == "net_income / shares_outstanding"
    assert margin_table.loc[3, "Operating Margin"] == "22.5%"
    assert return_table.loc[0, "Metric"] == "Return on invested capital"
    assert return_table.loc[0, "Peer Median"] == "19.0%"
    assert return_table.loc[0, "Assessment"] == "Above peer median"
    assert "growth and business quality" in data.interpretation_text.lower()


def test_growth_profitability_section_falls_back_to_annual_history_when_quarterly_is_sparse() -> None:
    fundamentals = pd.DataFrame(
        [
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2022-12-31",
                "fiscal_year": 2022,
                "fiscal_period": "FY",
                "period_type": "annual",
                "revenue": 300.0,
                "gross_profit": 120.0,
                "operating_income": 45.0,
                "net_income": 30.0,
                "free_cash_flow": 36.0,
                "shares_outstanding": 10.0,
                "return_on_equity": 0.15,
                "return_on_assets": 0.07,
                "return_on_invested_capital": 0.11,
            },
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2023-12-31",
                "fiscal_year": 2023,
                "fiscal_period": "FY",
                "period_type": "annual",
                "revenue": 330.0,
                "gross_profit": 138.6,
                "operating_income": 56.1,
                "net_income": 36.3,
                "free_cash_flow": 42.9,
                "shares_outstanding": 10.0,
                "return_on_equity": 0.17,
                "return_on_assets": 0.08,
                "return_on_invested_capital": 0.12,
            },
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2024-12-31",
                "fiscal_year": 2024,
                "fiscal_period": "FY",
                "period_type": "annual",
                "revenue": 360.0,
                "gross_profit": 158.4,
                "operating_income": 68.4,
                "net_income": 43.2,
                "free_cash_flow": 50.4,
                "shares_outstanding": 10.0,
                "return_on_equity": 0.19,
                "return_on_assets": 0.09,
                "return_on_invested_capital": 0.14,
            },
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-12-31",
                "fiscal_year": 2025,
                "fiscal_period": "FY",
                "period_type": "annual",
                "revenue": 396.0,
                "gross_profit": 182.2,
                "operating_income": 79.2,
                "net_income": 47.5,
                "free_cash_flow": 57.4,
                "shares_outstanding": 10.0,
                "return_on_equity": 0.21,
                "return_on_assets": 0.10,
                "return_on_invested_capital": 0.16,
            },
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-06-30",
                "fiscal_year": 2025,
                "fiscal_quarter": 2,
                "fiscal_period": "Q2",
                "period_type": "quarterly",
                "revenue": 90.0,
                "gross_profit": 39.6,
                "operating_income": 16.2,
                "net_income": 9.9,
                "free_cash_flow": 12.6,
                "shares_outstanding": 10.0,
                "return_on_equity": 0.19,
                "return_on_assets": 0.09,
                "return_on_invested_capital": 0.14,
            },
            {
                "ticker": "TEST",
                "as_of_date": "2026-04-05",
                "period_end_date": "2025-09-30",
                "fiscal_year": 2025,
                "fiscal_quarter": 3,
                "fiscal_period": "Q3",
                "period_type": "quarterly",
                "revenue": 96.0,
                "gross_profit": 43.2,
                "operating_income": 18.2,
                "net_income": 10.5,
                "free_cash_flow": 13.4,
                "shares_outstanding": 10.0,
                "return_on_equity": 0.20,
                "return_on_assets": 0.10,
                "return_on_invested_capital": 0.15,
            },
        ]
    )

    data = build_growth_profitability_section_data_from_inputs(
        ticker="TEST",
        company_name="Test Co",
        sector="Technology",
        industry="Software",
        report_date="2026-04-05",
        growth_profitability_score=22.0,
        fundamentals=fundamentals,
        peer_sets=pd.DataFrame(),
    )

    assert data.selected_period_type == "annual"
    assert data.growth_trend_table["Period"].tolist() == ["FY 2022", "FY 2023", "FY 2024", "FY 2025"]
    assert "annual history" in data.interpretation_text.lower()


def test_growth_profitability_section_builds_from_aapl_and_saves(tmp_path) -> None:
    data = build_growth_profitability_section_data("AAPL")

    assert data.ticker == "AAPL"
    assert data.selected_period_type in {"quarterly", "annual", "ttm"}
    assert not data.growth_trend_table.empty
    assert not data.margin_trend_table.empty
    assert not data.return_on_capital_table.empty

    figure = render_growth_profitability_section(data)
    assert len(figure.axes) == 5
    figure.clear()

    artifacts = save_growth_profitability_section(
        "AAPL",
        output_dir=Path(tmp_path) / "growth_profitability",
    )
    assert artifacts["figure_path"].exists()
    assert artifacts["growth_table_path"].exists()
    assert artifacts["margin_table_path"].exists()
    assert artifacts["return_table_path"].exists()
    assert artifacts["text_path"].exists()


def test_growth_profitability_section_prefers_gold_mart_snapshot_when_available(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()

    fundamentals = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-03-31",
                "fiscal_year": 2024,
                "fiscal_quarter": 1,
                "fiscal_period": "Q1",
                "period_type": "quarterly",
                "long_name": "Apple Fundamentals Name",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "revenue": 90.0,
                "gross_profit": 40.0,
                "operating_income": 28.0,
                "net_income": 22.0,
                "free_cash_flow": 24.0,
                "shares_outstanding": 10.0,
                "gross_margin": 0.44,
                "operating_margin": 0.31,
                "return_on_equity": 1.45,
                "return_on_assets": 0.23,
                "return_on_invested_capital": 0.13,
            },
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-06-30",
                "fiscal_year": 2024,
                "fiscal_quarter": 2,
                "fiscal_period": "Q2",
                "period_type": "quarterly",
                "long_name": "Apple Fundamentals Name",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "revenue": 94.0,
                "gross_profit": 42.0,
                "operating_income": 29.0,
                "net_income": 23.0,
                "free_cash_flow": 25.0,
                "shares_outstanding": 10.0,
                "gross_margin": 0.45,
                "operating_margin": 0.31,
                "return_on_equity": 1.46,
                "return_on_assets": 0.23,
                "return_on_invested_capital": 0.13,
            },
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-09-30",
                "fiscal_year": 2024,
                "fiscal_quarter": 3,
                "fiscal_period": "Q3",
                "period_type": "quarterly",
                "long_name": "Apple Fundamentals Name",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "revenue": 97.0,
                "gross_profit": 44.0,
                "operating_income": 30.0,
                "net_income": 24.0,
                "free_cash_flow": 26.0,
                "shares_outstanding": 10.0,
                "gross_margin": 0.45,
                "operating_margin": 0.31,
                "return_on_equity": 1.47,
                "return_on_assets": 0.24,
                "return_on_invested_capital": 0.13,
            },
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-12-31",
                "fiscal_year": 2024,
                "fiscal_quarter": 4,
                "fiscal_period": "Q4",
                "period_type": "quarterly",
                "long_name": "Apple Fundamentals Name",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "revenue": 100.0,
                "gross_profit": 45.0,
                "operating_income": 32.0,
                "net_income": 25.0,
                "free_cash_flow": 28.0,
                "shares_outstanding": 10.0,
                "gross_margin": 0.45,
                "operating_margin": 0.32,
                "return_on_equity": 1.48,
                "return_on_assets": 0.24,
                "return_on_invested_capital": 0.14,
            },
        ]
    )
    write_canonical_table(fundamentals, "normalized_fundamentals", settings=settings)
    write_canonical_table(pd.DataFrame(columns=["peer_set_id", "ticker", "peer_ticker", "peer_source", "relationship_type", "sector", "industry", "selection_method", "peer_order"]), "peer_sets", settings=settings)
    write_canonical_table(
        pd.DataFrame(
            {
                "security_id": ["sec_aapl"],
                "ticker": ["AAPL"],
                "name": ["Apple Security Name"],
                "asset_type": ["common_stock"],
                "exchange": ["Nasdaq"],
                "primary_listing": ["Nasdaq"],
                "currency": ["USD"],
                "sector": ["Technology"],
                "industry": ["Consumer Electronics"],
                "country": [None],
                "cik": [None],
                "is_benchmark": [False],
                "is_active": [True],
                "is_eligible_research_universe": [True],
                "source": ["fixture"],
                "source_id": ["AAPL"],
                "first_seen_at": ["2024-12-31"],
                "last_updated_at": ["2024-12-31"],
                "notes": [None],
            }
        ),
        "security_master",
        settings=settings,
    )

    mart_row = {column: None for column in GOLD_TICKER_METRICS_MART_COLUMNS}
    mart_row.update(
        {
            "mart_id": "gold_ticker_metrics::AAPL::2025-01-15",
            "ticker": "AAPL",
            "as_of_date": "2024-12-31",
            "score_date": "2025-01-15",
            "company_name": "Apple Mart Name",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "growth_profitability_score": 33.0,
        }
    )
    write_canonical_table(pd.DataFrame([mart_row]), "gold_ticker_metrics_mart", settings=settings)

    data = build_growth_profitability_section_data("AAPL", settings=settings)

    assert data.company_name == "Apple Mart Name"
    assert data.report_date == "2025-01-15"
    assert data.growth_profitability_score == 33.0
