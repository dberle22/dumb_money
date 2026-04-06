from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from dumb_money.analytics.scorecard import CompanyScorecard, build_company_scorecard
from dumb_money.config import AppSettings
from dumb_money.outputs import (
    build_balance_sheet_strength_section_data,
    build_balance_sheet_strength_table,
    render_balance_sheet_strength_section,
    save_balance_sheet_strength_section,
)
from dumb_money.outputs.balance_sheet_strength_section import (
    build_balance_sheet_strength_section_data_from_packet,
)
from dumb_money.research.company import CompanyResearchPacket
from dumb_money.storage import GOLD_TICKER_METRICS_MART_COLUMNS, write_canonical_table


def _build_packet_from_scorecard(scorecard: CompanyScorecard) -> CompanyResearchPacket:
    return CompanyResearchPacket(
        ticker="TEST",
        company_name="Test Co",
        as_of_date="2026-04-05",
        company_history=pd.DataFrame(),
        benchmark_histories={},
        return_windows=pd.DataFrame(),
        trailing_return_comparison=pd.DataFrame(),
        risk_metrics={},
        trend_metrics={},
        benchmark_comparison=pd.DataFrame(),
        fundamentals_summary={
            "sector": "Technology",
            "industry": "Software",
            "net_cash": 8_000_000_000,
            "total_debt": 4_000_000_000,
            "total_cash": 12_000_000_000,
        },
        peer_return_comparison=pd.DataFrame(),
        peer_return_summary_stats={},
        peer_valuation_comparison=pd.DataFrame(),
        peer_summary_stats={},
        sector_snapshot={},
        scorecard=scorecard,
    )


def _build_manual_packet() -> CompanyResearchPacket:
    scorecard = CompanyScorecard(
        summary={
            "ticker": "TEST",
            "company_name": "Test Co",
            "sector": "Technology",
            "industry": "Software",
            "primary_benchmark": "SPY",
            "secondary_benchmark": "QQQ",
            "score_date": "2026-04-05",
            "total_score": 72.0,
            "market_performance_score": 20.0,
            "growth_profitability_score": 30.0,
            "balance_sheet_score": 17.0,
            "valuation_score": 5.0,
            "confidence_score": 1.0,
            "available_weight": 100.0,
            "total_intended_weight": 100.0,
            "coverage_ratio": 1.0,
        },
        category_scores=pd.DataFrame(),
        metrics=pd.DataFrame(
            [
                {
                    "metric_id": "net_debt_to_ebitda",
                    "category": "Balance Sheet Strength",
                    "metric_name": "Net debt to EBITDA",
                    "raw_value": 0.4,
                    "normalized_value": 0.75,
                    "scoring_method": "threshold",
                    "metric_score": 6.0,
                    "metric_weight": 8.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Derived from total debt less total cash divided by EBITDA.",
                },
                {
                    "metric_id": "debt_to_equity",
                    "category": "Balance Sheet Strength",
                    "metric_name": "Debt to equity",
                    "raw_value": 95.0,
                    "normalized_value": 0.5,
                    "scoring_method": "threshold",
                    "metric_score": 2.5,
                    "metric_weight": 5.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Latest normalized fundamentals snapshot.",
                },
            ]
        ),
    )
    return _build_packet_from_scorecard(scorecard)


def test_balance_sheet_strength_section_formats_shared_scorecard_metrics() -> None:
    packet = _build_manual_packet()

    data = build_balance_sheet_strength_section_data_from_packet(packet)
    table = build_balance_sheet_strength_table(data)

    assert data.ticker == "TEST"
    assert table["Metric"].tolist() == ["Net debt to EBITDA", "Debt to equity"]
    assert "Shared Input" in table.columns
    assert table.loc[0, "Assessment"] == "Supportive"
    assert "balance-sheet profile" in data.interpretation_text.lower()


def test_balance_sheet_strength_section_handles_zero_debt_and_unavailable_ebitda() -> None:
    scorecard = build_company_scorecard(
        ticker="TEST",
        company_name="Test Co",
        sector="Technology",
        industry="Software",
        score_date="2026-04-05",
        benchmark_comparison=pd.DataFrame(),
        risk_metrics={},
        trend_metrics={},
        fundamentals_summary={
            "total_debt": 0.0,
            "total_cash": 5_000_000_000.0,
            "ebitda": None,
            "current_ratio": 1.9,
            "debt_to_equity": 0.0,
            "free_cash_flow": 2_000_000_000.0,
        },
        peer_valuation_comparison=pd.DataFrame(),
        primary_benchmark="SPY",
        secondary_benchmark="QQQ",
    )
    packet = _build_packet_from_scorecard(scorecard)
    packet.fundamentals_summary.update(
        {
            "net_cash": 5_000_000_000.0,
            "total_debt": 0.0,
            "total_cash": 5_000_000_000.0,
        }
    )

    data = build_balance_sheet_strength_section_data_from_packet(packet)
    table = build_balance_sheet_strength_table(data)

    net_debt_row = table.loc[table["Metric"] == "Net debt to EBITDA"].iloc[0]
    fcf_row = table.loc[table["Metric"] == "Free cash flow to debt"].iloc[0]

    assert net_debt_row["Assessment"] == "Unavailable"
    assert fcf_row["Value"] == "1.00x"
    assert "debt-free" in data.interpretation_text.lower()
    assert "ebitda coverage is missing or non-positive" in data.interpretation_text.lower()


def test_balance_sheet_strength_section_builds_from_aapl_and_saves(tmp_path) -> None:
    data = build_balance_sheet_strength_section_data("AAPL", benchmark_set_id="sample_universe")

    assert data.ticker == "AAPL"
    assert not data.summary_table.empty
    assert not data.summary_strip.empty

    figure = render_balance_sheet_strength_section(data)
    assert len(figure.axes) == 4
    figure.clear()

    artifacts = save_balance_sheet_strength_section(
        "AAPL",
        output_dir=Path(tmp_path) / "balance_sheet_strength",
        benchmark_set_id="sample_universe",
    )
    assert artifacts["figure_path"].exists()
    assert artifacts["table_path"].exists()
    assert artifacts["strip_path"].exists()
    assert artifacts["text_path"].exists()


def test_balance_sheet_strength_section_can_build_from_mart_snapshot_only(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()

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
            "ebitda": 120_000_000_000.0,
            "free_cash_flow": 95_000_000_000.0,
            "current_ratio": 1.5,
            "debt_to_equity": 1.2,
            "total_cash": 60_000_000_000.0,
            "total_debt": 100_000_000_000.0,
        }
    )
    write_canonical_table(pd.DataFrame([mart_row]), "gold_ticker_metrics_mart", settings=settings)

    data = build_balance_sheet_strength_section_data("AAPL", settings=settings)

    assert data.company_name == "Apple Mart Name"
    assert data.report_date == "2025-01-15"
    assert "Net debt to EBITDA" in data.summary_table["Metric"].tolist()
