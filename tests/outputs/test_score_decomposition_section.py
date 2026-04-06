from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from dumb_money.analytics.scorecard import CompanyScorecard
from dumb_money.config import AppSettings
from dumb_money.outputs import (
    build_score_decomposition_section_data,
    render_score_decomposition_section,
    save_score_decomposition_section,
)
from dumb_money.outputs.score_decomposition_section import (
    build_score_decomposition_category_table,
    build_score_decomposition_metric_table,
    build_score_decomposition_section_data_from_packet,
    build_score_decomposition_strip_table,
)
from dumb_money.research.company import CompanyResearchPacket
from dumb_money.storage import (
    GOLD_SCORECARD_METRIC_ROWS_COLUMNS,
    GOLD_TICKER_METRICS_MART_COLUMNS,
    write_canonical_table,
)


def _build_packet(
    *,
    category_scores: pd.DataFrame | None = None,
    metrics: pd.DataFrame | None = None,
) -> CompanyResearchPacket:
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
            "market_performance_score": 18.0,
            "growth_profitability_score": 28.0,
            "balance_sheet_score": 16.0,
            "valuation_score": 10.0,
            "confidence_score": 0.94,
            "available_weight": 94.0,
            "total_intended_weight": 100.0,
            "coverage_ratio": 0.94,
        },
        category_scores=category_scores if category_scores is not None else pd.DataFrame(),
        metrics=metrics if metrics is not None else pd.DataFrame(),
    )
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
        fundamentals_summary={"sector": "Technology", "industry": "Software"},
        peer_return_comparison=pd.DataFrame(),
        peer_return_summary_stats={},
        peer_valuation_comparison=pd.DataFrame(),
        peer_summary_stats={},
        sector_snapshot={},
        scorecard=scorecard,
    )


def _build_manual_packet() -> CompanyResearchPacket:
    return _build_packet(
        category_scores=pd.DataFrame(
            [
                {
                    "category": "Market Performance",
                    "category_score": 18.0,
                    "available_weight": 21.0,
                    "total_intended_weight": 25.0,
                    "coverage_ratio": 0.84,
                },
                {
                    "category": "Growth and Profitability",
                    "category_score": 28.0,
                    "available_weight": 35.0,
                    "total_intended_weight": 35.0,
                    "coverage_ratio": 1.0,
                },
                {
                    "category": "Balance Sheet Strength",
                    "category_score": 16.0,
                    "available_weight": 23.0,
                    "total_intended_weight": 25.0,
                    "coverage_ratio": 0.92,
                },
                {
                    "category": "Valuation",
                    "category_score": 10.0,
                    "available_weight": 15.0,
                    "total_intended_weight": 15.0,
                    "coverage_ratio": 1.0,
                },
            ]
        ),
        metrics=pd.DataFrame(
            [
                {
                    "metric_id": "return_vs_spy_1y",
                    "category": "Market Performance",
                    "metric_name": "12 month return vs SPY",
                    "raw_value": 0.11,
                    "normalized_value": 0.75,
                    "scoring_method": "threshold",
                    "metric_score": 7.5,
                    "metric_weight": 10.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Uses trailing 1 year excess return versus the primary benchmark.",
                },
                {
                    "metric_id": "operating_margin",
                    "category": "Growth and Profitability",
                    "metric_name": "Operating margin",
                    "raw_value": 0.32,
                    "normalized_value": 1.0,
                    "scoring_method": "threshold",
                    "metric_score": 8.0,
                    "metric_weight": 8.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Latest normalized fundamentals snapshot.",
                },
                {
                    "metric_id": "debt_to_equity",
                    "category": "Balance Sheet Strength",
                    "metric_name": "Debt to equity",
                    "raw_value": 1.8,
                    "normalized_value": 0.25,
                    "scoring_method": "threshold",
                    "metric_score": 1.0,
                    "metric_weight": 4.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Latest normalized fundamentals snapshot.",
                },
                {
                    "metric_id": "forward_pe",
                    "category": "Valuation",
                    "metric_name": "Forward P/E",
                    "raw_value": 18.0,
                    "normalized_value": 0.75,
                    "scoring_method": "peer_relative",
                    "metric_score": 3.75,
                    "metric_weight": 5.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Peer-relative scoring versus peer median 22.00x.",
                },
            ]
        ),
    )


def test_score_decomposition_section_formats_category_and_metric_transparency_tables() -> None:
    packet = _build_manual_packet()

    data = build_score_decomposition_section_data_from_packet(packet)
    category_table = build_score_decomposition_category_table(data)
    metric_table = build_score_decomposition_metric_table(data)
    strip = build_score_decomposition_strip_table(data)

    assert data.ticker == "TEST"
    assert category_table["Category"].tolist() == [
        "Market Performance",
        "Growth and Profitability",
        "Balance Sheet Strength",
        "Valuation",
    ]
    assert category_table.loc[1, "Assessment"] == "Supportive"
    assert strip.loc[1, "contribution_display"] == "38.9%"
    assert metric_table["Metric ID"].tolist()[0] == "return_vs_spy_1y"
    assert metric_table.loc[3, "Shared Input"] == "scorecard.fundamentals_summary.forward_pe + scorecard.peer_valuation_comparison.forward_pe"
    assert "growth and profitability" in data.interpretation_text.lower()


def test_score_decomposition_section_handles_missing_scorecard_coverage() -> None:
    packet = _build_packet()

    data = build_score_decomposition_section_data_from_packet(packet)

    assert data.category_contribution_table.shape[0] == 4
    assert data.metric_score_table.empty
    assert "does not yet have enough canonical scorecard coverage" in data.interpretation_text


def test_score_decomposition_section_builds_from_aapl_and_saves(tmp_path) -> None:
    data = build_score_decomposition_section_data("AAPL", benchmark_set_id="sample_universe")

    assert data.ticker == "AAPL"
    assert not data.category_contribution_table.empty
    assert not data.metric_score_table.empty
    assert not data.summary_strip.empty
    assert data.interpretation_text

    figure = render_score_decomposition_section(data)
    assert len(figure.axes) == 5
    figure.clear()

    artifacts = save_score_decomposition_section(
        "AAPL",
        benchmark_set_id="sample_universe",
        output_dir=Path(tmp_path) / "score_decomposition",
    )
    assert artifacts["figure_path"].exists()


def test_score_decomposition_section_prefers_gold_score_artifacts(tmp_path) -> None:
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
            "total_score": 74.0,
            "confidence_score": 0.95,
            "market_performance_score": 18.0,
            "market_performance_available_weight": 25.0,
            "growth_profitability_score": 30.0,
            "growth_profitability_available_weight": 35.0,
            "balance_sheet_score": 16.0,
            "balance_sheet_available_weight": 25.0,
            "valuation_score": 10.0,
            "valuation_available_weight": 15.0,
        }
    )
    write_canonical_table(pd.DataFrame([mart_row]), "gold_ticker_metrics_mart", settings=settings)

    metric_rows = pd.DataFrame(
        [
            {
                "scorecard_metric_row_id": "gold_scorecard_metric::AAPL::2025-01-15::return_vs_spy_1y",
                "ticker": "AAPL",
                "score_date": "2025-01-15",
                "metric_id": "return_vs_spy_1y",
                "category": "Market Performance",
                "metric_name": "12 month return vs SPY",
                "raw_value": 0.11,
                "normalized_value": 0.75,
                "scoring_method": "threshold",
                "metric_score": 7.5,
                "metric_weight": 10.0,
                "metric_available": True,
                "metric_applicable": True,
                "confidence_flag": "ok",
                "notes": "Uses trailing 1 year excess return versus the primary benchmark.",
                "company_name": "Apple Mart Name",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "primary_benchmark": "SPY",
                "secondary_benchmark": "QQQ",
            },
            {
                "scorecard_metric_row_id": "gold_scorecard_metric::AAPL::2025-01-15::forward_pe",
                "ticker": "AAPL",
                "score_date": "2025-01-15",
                "metric_id": "forward_pe",
                "category": "Valuation",
                "metric_name": "Forward P/E",
                "raw_value": 18.0,
                "normalized_value": 0.75,
                "scoring_method": "peer_relative",
                "metric_score": 3.75,
                "metric_weight": 5.0,
                "metric_available": True,
                "metric_applicable": True,
                "confidence_flag": "ok",
                "notes": "Peer-relative scoring versus peer median 22.00x.",
                "company_name": "Apple Mart Name",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "primary_benchmark": "SPY",
                "secondary_benchmark": "QQQ",
            },
        ]
    )
    metric_rows = metric_rows.reindex(columns=GOLD_SCORECARD_METRIC_ROWS_COLUMNS)
    write_canonical_table(metric_rows, "gold_scorecard_metric_rows", settings=settings)

    data = build_score_decomposition_section_data("AAPL", settings=settings)

    assert data.company_name == "Apple Mart Name"
    assert data.report_date == "2025-01-15"
    assert "forward_pe" in data.metric_scores["metric_id"].tolist()
