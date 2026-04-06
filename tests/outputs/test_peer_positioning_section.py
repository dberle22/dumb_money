from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from dumb_money.analytics.scorecard import CompanyScorecard
from dumb_money.outputs import (
    build_peer_positioning_section_data,
    render_peer_positioning_section,
    save_peer_positioning_section,
)
from dumb_money.outputs.peer_positioning_section import (
    build_peer_positioning_ranking_panel,
    build_peer_positioning_return_table,
    build_peer_positioning_section_data_from_packet,
    build_peer_positioning_valuation_table,
)
from dumb_money.research.company import CompanyResearchPacket


def _build_packet(
    *,
    peer_return_comparison: pd.DataFrame | None = None,
    peer_return_summary_stats: dict[str, object] | None = None,
    peer_valuation_comparison: pd.DataFrame | None = None,
    peer_summary_stats: dict[str, object] | None = None,
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
            "total_score": 70.0,
            "market_performance_score": 18.0,
            "growth_profitability_score": 30.0,
            "balance_sheet_score": 15.0,
            "valuation_score": 7.0,
            "confidence_score": 1.0,
            "available_weight": 100.0,
            "total_intended_weight": 100.0,
            "coverage_ratio": 1.0,
        },
        category_scores=pd.DataFrame(),
        metrics=pd.DataFrame(),
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
        peer_return_comparison=peer_return_comparison if peer_return_comparison is not None else pd.DataFrame(),
        peer_return_summary_stats=peer_return_summary_stats if peer_return_summary_stats is not None else {},
        peer_valuation_comparison=peer_valuation_comparison if peer_valuation_comparison is not None else pd.DataFrame(),
        peer_summary_stats=peer_summary_stats if peer_summary_stats is not None else {},
        sector_snapshot={
            "sector": "Technology",
            "sector_benchmark": "XLK",
            "median_return_1y": 0.18,
        },
        scorecard=scorecard,
    )


def _build_manual_packet() -> CompanyResearchPacket:
    return _build_packet(
        peer_return_comparison=pd.DataFrame(
            [
                {
                    "ticker": "TEST",
                    "relationship_type": "focal_company",
                    "peer_source": "self",
                    "selection_method": "self",
                    "peer_order": 0,
                    "window": "1y",
                    "company_return": 0.24,
                    "peer_return": 0.24,
                    "excess_return": 0.0,
                    "is_focal_company": True,
                },
                {
                    "ticker": "PEER1",
                    "relationship_type": "industry",
                    "peer_source": "automatic",
                    "selection_method": "industry_market_cap_proximity",
                    "peer_order": 1,
                    "window": "1y",
                    "company_return": 0.24,
                    "peer_return": 0.19,
                    "excess_return": 0.05,
                    "is_focal_company": False,
                },
                {
                    "ticker": "PEER2",
                    "relationship_type": "sector",
                    "peer_source": "curated",
                    "selection_method": "curated_research_list",
                    "peer_order": 2,
                    "window": "1y",
                    "company_return": 0.24,
                    "peer_return": 0.11,
                    "excess_return": 0.13,
                    "is_focal_company": False,
                },
            ]
        ),
        peer_return_summary_stats={
            "peer_count": 2,
            "available_peer_count": 2,
            "focus_window": "1y",
            "median_peer_return": 0.15,
            "median_excess_return": 0.09,
            "best_peer_ticker": "PEER1",
            "worst_peer_ticker": "PEER2",
        },
        peer_valuation_comparison=pd.DataFrame(
            [
                {
                    "ticker": "TEST",
                    "company_name": "Test Co",
                    "peer_source": "self",
                    "relationship_type": "focal_company",
                    "selection_method": "self",
                    "peer_order": 0,
                    "is_focal_company": True,
                    "sector": "Technology",
                    "industry": "Software",
                    "market_cap": 2_000_000_000_000.0,
                    "forward_pe": 22.0,
                    "ev_to_ebitda": 16.0,
                    "price_to_sales": 6.5,
                    "free_cash_flow_yield": 0.045,
                },
                {
                    "ticker": "PEER1",
                    "company_name": "Peer One",
                    "peer_source": "automatic",
                    "relationship_type": "industry",
                    "selection_method": "industry_market_cap_proximity",
                    "peer_order": 1,
                    "is_focal_company": False,
                    "sector": "Technology",
                    "industry": "Software",
                    "market_cap": 1_800_000_000_000.0,
                    "forward_pe": 25.0,
                    "ev_to_ebitda": 18.0,
                    "price_to_sales": 7.0,
                    "free_cash_flow_yield": 0.04,
                },
                {
                    "ticker": "PEER2",
                    "company_name": "Peer Two",
                    "peer_source": "curated",
                    "relationship_type": "sector",
                    "selection_method": "curated_research_list",
                    "peer_order": 2,
                    "is_focal_company": False,
                    "sector": "Technology",
                    "industry": "Software",
                    "market_cap": 900_000_000_000.0,
                    "forward_pe": 28.0,
                    "ev_to_ebitda": 19.0,
                    "price_to_sales": 8.0,
                    "free_cash_flow_yield": 0.03,
                },
            ]
        ),
        peer_summary_stats={
            "peer_count": 2,
            "available_peer_count": 2,
            "median_forward_pe": 26.5,
            "median_ev_to_ebitda": 18.5,
            "median_price_to_sales": 7.5,
            "median_free_cash_flow_yield": 0.035,
        },
    )


def test_peer_positioning_section_formats_shared_peer_tables_and_rankings() -> None:
    packet = _build_manual_packet()

    data = build_peer_positioning_section_data_from_packet(packet)
    return_table = build_peer_positioning_return_table(data)
    valuation_table = build_peer_positioning_valuation_table(data)
    ranking_panel = build_peer_positioning_ranking_panel(data)

    assert data.ticker == "TEST"
    assert return_table["Peer Source"].tolist() == ["self", "automatic", "curated"]
    assert return_table["Return Rank"].tolist() == ["1 / 3", "2 / 3", "3 / 3"]
    assert valuation_table["Forward P/E Rank"].tolist() == ["1 / 3", "2 / 3", "3 / 3"]
    assert ranking_panel["signal"].tolist() == ["1Y Return", "Forward P/E", "EV/EBITDA", "FCF Yield"]
    assert ranking_panel.loc[0, "assessment"] == "Leader"
    assert "canonical comparison is Peer One" in data.interpretation_text


def test_peer_positioning_section_handles_missing_peer_coverage() -> None:
    packet = _build_packet()

    data = build_peer_positioning_section_data_from_packet(packet)

    assert data.return_context_table.empty
    assert data.valuation_context_table.empty
    assert data.ranking_panel.empty
    assert "does not yet have enough canonical peer coverage" in data.interpretation_text


def test_peer_positioning_section_builds_from_aapl_and_saves(tmp_path) -> None:
    data = build_peer_positioning_section_data("AAPL", benchmark_set_id="sample_universe")

    assert data.ticker == "AAPL"
    assert not data.return_context_table.empty
    assert not data.valuation_context_table.empty
    assert not data.ranking_panel.empty
    assert data.interpretation_text

    figure = render_peer_positioning_section(data)
    assert len(figure.axes) == 5
    figure.clear()

    artifacts = save_peer_positioning_section(
        "AAPL",
        benchmark_set_id="sample_universe",
        output_dir=Path(tmp_path) / "peer_positioning",
    )
    assert artifacts["figure_path"].exists()
    assert artifacts["return_table_path"].exists()
    assert artifacts["valuation_table_path"].exists()
    assert artifacts["ranking_path"].exists()
    assert artifacts["text_path"].exists()
