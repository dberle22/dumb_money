from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from dumb_money.analytics.scorecard import CompanyScorecard, build_company_scorecard
from dumb_money.outputs import (
    build_valuation_section_data,
    render_valuation_section,
    save_valuation_section,
)
from dumb_money.outputs.valuation_section import (
    build_valuation_peer_comparison_table,
    build_valuation_section_data_from_packet,
    build_valuation_summary_table,
)
from dumb_money.research.company import CompanyResearchPacket


def _build_packet_from_scorecard(
    scorecard: CompanyScorecard,
    *,
    peer_valuation_comparison: pd.DataFrame | None = None,
    peer_summary_stats: dict[str, object] | None = None,
) -> CompanyResearchPacket:
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
            "forward_pe": 18.0,
            "ev_to_ebitda": 12.0,
            "price_to_sales": 3.5,
            "market_cap": 1_000.0,
            "free_cash_flow": 60.0,
        },
        peer_return_comparison=pd.DataFrame(),
        peer_return_summary_stats={},
        peer_valuation_comparison=peer_valuation_comparison if peer_valuation_comparison is not None else pd.DataFrame(),
        peer_summary_stats=peer_summary_stats if peer_summary_stats is not None else {},
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
            "total_score": 70.0,
            "market_performance_score": 18.0,
            "growth_profitability_score": 30.0,
            "balance_sheet_score": 15.0,
            "valuation_score": 7.25,
            "confidence_score": 1.0,
            "available_weight": 100.0,
            "total_intended_weight": 100.0,
            "coverage_ratio": 1.0,
        },
        category_scores=pd.DataFrame(),
        metrics=pd.DataFrame(
            [
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
                {
                    "metric_id": "yield_metric",
                    "category": "Valuation",
                    "metric_name": "Dividend yield or free cash flow yield",
                    "raw_value": 0.06,
                    "normalized_value": 0.75,
                    "scoring_method": "threshold",
                    "metric_score": 2.25,
                    "metric_weight": 3.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Uses free cash flow yield derived from free cash flow divided by market cap.",
                },
            ]
        ),
    )
    return _build_packet_from_scorecard(
        scorecard,
        peer_valuation_comparison=pd.DataFrame(
            [
                {
                    "ticker": "TEST",
                    "company_name": "Test Co",
                    "relationship_type": "focal_company",
                    "selection_method": "self",
                    "peer_order": 0,
                    "is_focal_company": True,
                    "forward_pe": 18.0,
                    "ev_to_ebitda": 12.0,
                    "price_to_sales": 3.5,
                    "free_cash_flow_yield": 0.06,
                },
                {
                    "ticker": "PEER1",
                    "company_name": "Peer One",
                    "relationship_type": "sector",
                    "selection_method": "sector_market_cap_proximity",
                    "peer_order": 1,
                    "is_focal_company": False,
                    "forward_pe": 22.0,
                    "ev_to_ebitda": 14.0,
                    "price_to_sales": 4.0,
                    "free_cash_flow_yield": 0.04,
                },
            ]
        ),
        peer_summary_stats={
            "peer_count": 1,
            "available_peer_count": 1,
            "median_forward_pe": 22.0,
            "median_ev_to_ebitda": 14.0,
            "median_price_to_sales": 4.0,
            "median_free_cash_flow_yield": 0.04,
        },
    )


def test_valuation_section_formats_shared_scorecard_metrics_and_peer_context() -> None:
    packet = _build_manual_packet()

    data = build_valuation_section_data_from_packet(packet)
    table = build_valuation_summary_table(data)
    peer_table = build_valuation_peer_comparison_table(data)

    assert data.ticker == "TEST"
    assert table["Metric"].tolist() == ["Forward P/E", "Dividend yield or free cash flow yield"]
    assert table.loc[0, "Peer Median"] == "22.00x"
    assert table.loc[0, "Relative vs Peer"] == "18.2% discount"
    assert table.loc[1, "Relative vs Peer"] == "50.0% above peer median"
    assert "Shared Input" in table.columns
    assert peer_table["Role"].tolist() == ["Company", "Peer"]
    assert "valuation" in data.interpretation_text.lower()


def test_valuation_section_handles_missing_peer_coverage_and_dividend_yield_fallback() -> None:
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
            "forward_pe": 32.0,
            "ev_to_ebitda": 20.0,
            "price_to_sales": 8.0,
            "market_cap": 1_000.0,
            "free_cash_flow": None,
            "dividend_yield": 0.012,
        },
        peer_valuation_comparison=pd.DataFrame(),
        primary_benchmark="SPY",
        secondary_benchmark="QQQ",
    )
    packet = _build_packet_from_scorecard(scorecard)
    packet.fundamentals_summary["dividend_yield"] = 0.012

    data = build_valuation_section_data_from_packet(packet)
    table = build_valuation_summary_table(data)

    yield_row = table.loc[table["Metric"] == "Dividend yield or free cash flow yield"].iloc[0]

    assert yield_row["Peer Median"] == "N/A"
    assert yield_row["Relative vs Peer"] == "N/A"
    assert "dividend_yield" in yield_row["Shared Input"]
    assert "not yet available" in data.interpretation_text.lower()


def test_valuation_section_builds_from_aapl_and_saves(tmp_path) -> None:
    data = build_valuation_section_data("AAPL")

    assert data.ticker == "AAPL"
    assert not data.summary_table.empty
    assert not data.summary_strip.empty
    assert data.interpretation_text

    figure = render_valuation_section(data)
    assert len(figure.axes) == 5
    figure.clear()

    artifacts = save_valuation_section(
        "AAPL",
        output_dir=Path(tmp_path) / "valuation",
    )
    assert artifacts["figure_path"].exists()
    assert artifacts["table_path"].exists()
    assert artifacts["strip_path"].exists()
    assert artifacts["peer_table_path"].exists()
    assert artifacts["text_path"].exists()
