from __future__ import annotations

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from dumb_money.analytics.scorecard import CompanyScorecard
from dumb_money.outputs import (
    build_research_summary_table as build_legacy_research_summary_table,
    build_research_summary_section_data,
    build_research_summary_text as build_legacy_research_summary_text,
    build_score_summary_strip_table as build_legacy_score_summary_strip_table,
    render_research_summary_section,
    resolve_research_summary_label,
)
from dumb_money.outputs.research_summary_section import (
    build_research_summary_section_data_from_packet,
)
from dumb_money.research.company import CompanyResearchPacket


def _build_packet(
    *,
    total_score: float = 72.0,
    category_scores: pd.DataFrame | None = None,
    metrics: pd.DataFrame | None = None,
) -> CompanyResearchPacket:
    category_scores = category_scores if category_scores is not None else pd.DataFrame(
        [
            {
                "category": "Market Performance",
                "category_score": 20.0,
                "available_weight": 25.0,
                "total_intended_weight": 25.0,
                "coverage_ratio": 1.0,
            },
            {
                "category": "Growth and Profitability",
                "category_score": 30.0,
                "available_weight": 35.0,
                "total_intended_weight": 35.0,
                "coverage_ratio": 1.0,
            },
            {
                "category": "Balance Sheet Strength",
                "category_score": 16.0,
                "available_weight": 25.0,
                "total_intended_weight": 25.0,
                "coverage_ratio": 1.0,
            },
            {
                "category": "Valuation",
                "category_score": 6.0,
                "available_weight": 15.0,
                "total_intended_weight": 15.0,
                "coverage_ratio": 1.0,
            },
        ]
    )
    metrics = metrics if metrics is not None else pd.DataFrame(
        [
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
                "notes": "",
            },
            {
                "metric_id": "forward_pe",
                "category": "Valuation",
                "metric_name": "Forward P/E",
                "raw_value": 32.0,
                "normalized_value": 0.25,
                "scoring_method": "threshold",
                "metric_score": 1.25,
                "metric_weight": 5.0,
                "metric_available": True,
                "metric_applicable": True,
                "confidence_flag": "ok",
                "notes": "",
            },
        ]
    )

    scorecard = CompanyScorecard(
        summary={
            "ticker": "TEST",
            "company_name": "Test Co",
            "sector": "Technology",
            "industry": "Software",
            "primary_benchmark": "SPY",
            "secondary_benchmark": "QQQ",
            "score_date": "2026-04-05",
            "total_score": total_score,
            "market_performance_score": 20.0,
            "growth_profitability_score": 30.0,
            "balance_sheet_score": 16.0,
            "valuation_score": 6.0,
            "confidence_score": 1.0,
            "available_weight": 100.0,
            "total_intended_weight": 100.0,
            "coverage_ratio": 1.0,
        },
        category_scores=category_scores,
        metrics=metrics,
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


def test_research_summary_section_builds_from_aapl_packet() -> None:
    data = build_research_summary_section_data("AAPL", benchmark_set_id="sample_universe")

    assert data.ticker == "AAPL"
    assert data.summary_table["label"].tolist()[:4] == ["Company", "Ticker", "Sector", "Industry"]
    assert "Total Score" in data.score_summary_strip["pillar"].tolist()
    assert data.summary_text

    figure = render_research_summary_section(data)
    assert len(figure.axes) == 3
    figure.clear()


def test_research_summary_section_selects_strongest_and_weakest_pillars() -> None:
    packet = _build_packet()

    data = build_research_summary_section_data_from_packet(packet)

    assert data.strongest_pillar == "Growth and Profitability"
    assert data.weakest_pillar == "Valuation"
    assert data.interpretation_label == "Quality-led profile with mixed offsets"
    assert "growth and profitability" in data.summary_text
    assert "valuation" in data.summary_text


def test_resolve_research_summary_label_score_bands() -> None:
    assert resolve_research_summary_label(82.0) == "High-quality setup with broad support"
    assert resolve_research_summary_label(65.0) == "Quality-led profile with mixed offsets"
    assert resolve_research_summary_label(50.0) == "Mixed setup with visible tradeoffs"
    assert resolve_research_summary_label(49.9) == "Challenged profile that needs more support"


def test_research_summary_section_handles_empty_metrics() -> None:
    packet = _build_packet(metrics=pd.DataFrame())

    data = build_research_summary_section_data_from_packet(packet)

    assert data.strengths == []
    assert data.constraints == []
    strengths_value = data.summary_table.loc[data.summary_table["label"] == "Strengths", "value"].iloc[0]
    constraints_value = data.summary_table.loc[data.summary_table["label"] == "Constraints", "value"].iloc[0]
    assert strengths_value == "Growth and Profitability"
    assert constraints_value == "Valuation"


def test_company_report_research_summary_helpers_delegate_to_section_module() -> None:
    packet = _build_packet()

    table = build_legacy_research_summary_table(packet)
    strip = build_legacy_score_summary_strip_table(packet)
    summary_text = build_legacy_research_summary_text(packet, short=True)

    assert table["label"].tolist()[:3] == ["Company", "Ticker", "Sector"]
    assert strip["pillar"].tolist()[0] == "Total Score"
    assert summary_text == "Quality-led profile with mixed offsets"
