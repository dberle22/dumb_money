from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from dumb_money.analytics.scorecard import CompanyScorecard
from dumb_money.outputs import (
    build_final_research_summary_section_data,
    build_final_research_summary_text,
    render_final_research_summary_section,
    save_final_research_summary_section,
)
from dumb_money.outputs.final_research_summary_section import (
    build_final_research_summary_section_data_from_packet,
    build_final_research_summary_table,
)
from dumb_money.research.company import CompanyResearchPacket


def _history_frame(ticker: str, start_price: float) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=260, freq="B")
    prices = [start_price + (idx * 0.4) for idx in range(len(dates))]
    return pd.DataFrame(
        {
            "ticker": ticker,
            "date": dates,
            "adj_close": prices,
            "close": prices,
        }
    )


def _build_packet(
    *,
    with_peer_context: bool = True,
) -> CompanyResearchPacket:
    scorecard = CompanyScorecard(
        summary={
            "ticker": "TEST",
            "company_name": "Test Co",
            "sector": "Technology",
            "industry": "Software",
            "primary_benchmark": "SPY",
            "secondary_benchmark": "XLK",
            "score_date": "2026-04-05",
            "total_score": 72.0,
            "market_performance_score": 18.0,
            "growth_profitability_score": 30.0,
            "balance_sheet_score": 16.0,
            "valuation_score": 8.0,
            "confidence_score": 0.96,
            "available_weight": 100.0,
            "total_intended_weight": 100.0,
            "coverage_ratio": 1.0,
        },
        category_scores=pd.DataFrame(
            [
                {
                    "category": "Market Performance",
                    "category_score": 18.0,
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
                    "category_score": 8.0,
                    "available_weight": 15.0,
                    "total_intended_weight": 15.0,
                    "coverage_ratio": 1.0,
                },
            ]
        ),
        metrics=pd.DataFrame(
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
                    "notes": "Latest normalized fundamentals snapshot.",
                },
                {
                    "metric_id": "free_cash_flow_margin",
                    "category": "Growth and Profitability",
                    "metric_name": "Free cash flow margin",
                    "raw_value": 0.24,
                    "normalized_value": 0.75,
                    "scoring_method": "threshold",
                    "metric_score": 4.5,
                    "metric_weight": 6.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Latest normalized fundamentals snapshot.",
                },
                {
                    "metric_id": "max_drawdown_1y",
                    "category": "Market Performance",
                    "metric_name": "Max drawdown (1Y)",
                    "raw_value": -0.23,
                    "normalized_value": 0.25,
                    "scoring_method": "threshold",
                    "metric_score": 1.25,
                    "metric_weight": 5.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Uses trailing one-year max drawdown.",
                },
                {
                    "metric_id": "forward_pe",
                    "category": "Valuation",
                    "metric_name": "Forward P/E",
                    "raw_value": 28.0,
                    "normalized_value": 0.25,
                    "scoring_method": "peer_relative",
                    "metric_score": 1.25,
                    "metric_weight": 5.0,
                    "metric_available": True,
                    "metric_applicable": True,
                    "confidence_flag": "ok",
                    "notes": "Peer-relative scoring versus peer median 24.00x.",
                },
            ]
        ),
    )

    peer_return_comparison = pd.DataFrame()
    peer_return_summary_stats: dict[str, object] = {}
    peer_valuation_comparison = pd.DataFrame()
    peer_summary_stats: dict[str, object] = {}
    sector_snapshot = {"sector": "Technology", "sector_benchmark": "XLK", "median_return_1y": 0.18}

    if with_peer_context:
        peer_return_comparison = pd.DataFrame(
            [
                {
                    "ticker": "TEST",
                    "window": "1y",
                    "company_return": 0.24,
                    "peer_return": pd.NA,
                    "excess_return": 0.0,
                    "is_focal_company": True,
                    "peer_source": "curated",
                    "relationship_type": "focal_company",
                    "selection_method": "manual",
                    "peer_order": 0,
                },
                {
                    "ticker": "PEER1",
                    "window": "1y",
                    "company_return": 0.24,
                    "peer_return": 0.18,
                    "excess_return": 0.06,
                    "is_focal_company": False,
                    "peer_source": "curated",
                    "relationship_type": "same_industry",
                    "selection_method": "manual",
                    "peer_order": 1,
                },
                {
                    "ticker": "PEER2",
                    "window": "1y",
                    "company_return": 0.24,
                    "peer_return": 0.12,
                    "excess_return": 0.12,
                    "is_focal_company": False,
                    "peer_source": "automatic",
                    "relationship_type": "same_sector",
                    "selection_method": "industry_then_sector",
                    "peer_order": 2,
                },
            ]
        )
        peer_return_summary_stats = {"focus_window": "1y", "median_peer_return": 0.15, "peer_count": 2}
        peer_valuation_comparison = pd.DataFrame(
            [
                {
                    "ticker": "TEST",
                    "company_name": "Test Co",
                    "is_focal_company": True,
                    "market_cap": 500_000_000_000,
                    "forward_pe": 28.0,
                    "ev_to_ebitda": 18.0,
                    "price_to_sales": 7.0,
                    "free_cash_flow_yield": 0.028,
                    "peer_source": "curated",
                    "relationship_type": "focal_company",
                    "selection_method": "manual",
                    "peer_order": 0,
                },
                {
                    "ticker": "PEER1",
                    "company_name": "Peer One",
                    "is_focal_company": False,
                    "market_cap": 420_000_000_000,
                    "forward_pe": 32.0,
                    "ev_to_ebitda": 20.0,
                    "price_to_sales": 8.0,
                    "free_cash_flow_yield": 0.024,
                    "peer_source": "curated",
                    "relationship_type": "same_industry",
                    "selection_method": "manual",
                    "peer_order": 1,
                },
                {
                    "ticker": "PEER2",
                    "company_name": "Peer Two",
                    "is_focal_company": False,
                    "market_cap": 300_000_000_000,
                    "forward_pe": 24.0,
                    "ev_to_ebitda": 16.0,
                    "price_to_sales": 5.0,
                    "free_cash_flow_yield": 0.031,
                    "peer_source": "automatic",
                    "relationship_type": "same_sector",
                    "selection_method": "industry_then_sector",
                    "peer_order": 2,
                },
            ]
        )
        peer_summary_stats = {
            "peer_count": 2,
            "median_forward_pe": 28.0,
            "median_ev_to_ebitda": 18.0,
            "median_price_to_sales": 6.5,
            "median_free_cash_flow_yield": 0.0275,
        }

    return CompanyResearchPacket(
        ticker="TEST",
        company_name="Test Co",
        as_of_date="2026-04-05",
        company_history=_history_frame("TEST", 100.0),
        benchmark_histories={"SPY": _history_frame("SPY", 90.0), "XLK": _history_frame("XLK", 95.0)},
        return_windows=pd.DataFrame(),
        trailing_return_comparison=pd.DataFrame(),
        risk_metrics={
            "annualized_volatility_1m": 0.18,
            "annualized_volatility_1y": 0.24,
            "downside_volatility_1y": 0.16,
            "beta_1y": 1.05,
            "current_drawdown": -0.08,
            "max_drawdown_1y": -0.23,
        },
        trend_metrics={
            "price_vs_sma_50": 0.04,
            "price_vs_sma_200": 0.11,
            "sma_50_above_sma_200": True,
        },
        benchmark_comparison=pd.DataFrame(),
        fundamentals_summary={
            "sector": "Technology",
            "industry": "Software",
            "forward_pe": 28.0,
            "ev_to_ebitda": 18.0,
            "price_to_sales": 7.0,
            "free_cash_flow": 14_000_000_000,
            "market_cap": 500_000_000_000,
            "long_name": "Test Co",
        },
        peer_return_comparison=peer_return_comparison,
        peer_return_summary_stats=peer_return_summary_stats,
        peer_valuation_comparison=peer_valuation_comparison,
        peer_summary_stats=peer_summary_stats,
        sector_snapshot=sector_snapshot,
        scorecard=scorecard,
    )


def test_final_research_summary_section_assembles_structured_closing_fields() -> None:
    packet = _build_packet()

    data = build_final_research_summary_section_data_from_packet(packet)
    table = build_final_research_summary_table(data)

    assert data.strongest_pillar == "Growth and Profitability"
    assert data.weakest_pillar == "Valuation"
    assert table["Component"].tolist() == [
        "What is working",
        "What is not working",
        "Bottom line",
        "What to watch",
    ]
    assert any("growth and profitability" in item.lower() for item in data.what_is_working)
    assert any("valuation" in item.lower() for item in data.what_is_not_working)
    assert any("watch" in item.lower() for item in data.what_to_watch)
    assert "What is working:" in data.final_memo_text


def test_final_research_summary_section_handles_missing_peer_context() -> None:
    packet = _build_packet(with_peer_context=False)

    data = build_final_research_summary_section_data_from_packet(packet)

    assert data.peer_positioning.ranking_panel.empty
    assert any("richer canonical peer or catalyst fields" in item for item in data.what_to_watch)
    assert "What to watch:" in build_final_research_summary_text(packet)


def test_final_research_summary_section_builds_from_aapl_and_saves(tmp_path) -> None:
    data = build_final_research_summary_section_data("AAPL", benchmark_set_id="sample_universe")

    assert data.ticker == "AAPL"
    assert not data.closing_summary_table.empty
    assert data.bottom_line
    assert data.final_memo_text

    figure = render_final_research_summary_section(data)
    assert len(figure.axes) == 3
    figure.clear()

    artifacts = save_final_research_summary_section(
        "AAPL",
        benchmark_set_id="sample_universe",
        output_dir=Path(tmp_path) / "final_research_summary",
    )
    assert artifacts["figure_path"].exists()
    assert artifacts["table_path"].exists()
    assert artifacts["text_path"].exists()
