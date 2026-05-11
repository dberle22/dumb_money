from __future__ import annotations

import matplotlib
import pandas as pd
from types import SimpleNamespace

matplotlib.use("Agg")

from dumb_money.analytics.scorecard import CompanyScorecard
from dumb_money.config import AppSettings
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
from dumb_money.storage import GOLD_TICKER_METRICS_MART_COLUMNS, write_canonical_table


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


def test_research_summary_section_prefers_mart_snapshot_fields(tmp_path, monkeypatch) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()

    fundamentals = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-12-31",
                "fiscal_period": "TTM",
                "period_type": "ttm",
                "long_name": "Apple Fundamentals Name",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "market_cap": 1_000.0,
                "free_cash_flow": 60.0,
                "gross_profit": 42.0,
                "operating_income": 31.0,
                "net_income": 24.0,
                "shares_outstanding": 10.0,
                "gross_margin": 0.42,
                "operating_margin": 0.31,
                "return_on_equity": 0.28,
                "return_on_assets": 0.12,
                "total_cash": 60.0,
                "total_debt": 100.0,
                "current_ratio": 1.5,
                "debt_to_equity": 1.2,
                "forward_pe": 18.0,
                "ev_to_ebitda": 12.0,
                "price_to_sales": 3.5,
            },
            {
                "ticker": "MSFT",
                "as_of_date": "2024-12-31",
                "period_end_date": "2024-12-31",
                "fiscal_period": "TTM",
                "period_type": "ttm",
                "long_name": "Microsoft Corp.",
                "sector": "Technology",
                "industry": "Software",
                "market_cap": 1_200.0,
                "free_cash_flow": 36.0,
                "gross_profit": 50.0,
                "operating_income": 35.0,
                "net_income": 27.0,
                "shares_outstanding": 8.0,
                "forward_pe": 22.0,
                "ev_to_ebitda": 14.0,
                "price_to_sales": 4.0,
            },
        ]
    )
    write_canonical_table(fundamentals, "normalized_fundamentals", settings=settings)
    write_canonical_table(
        pd.DataFrame(
            {
                "mapping_id": ["benchmark_mapping::AAPL"],
                "ticker": ["AAPL"],
                "sector": ["Technology"],
                "industry": ["Consumer Electronics"],
                "primary_benchmark": ["SPY"],
                "sector_benchmark": ["QQQ"],
                "industry_benchmark": [None],
                "style_benchmark": [None],
                "custom_benchmark": [None],
                "assignment_method": ["test_fixture"],
                "priority": [1],
                "is_active": [True],
                "notes": [None],
            }
        ),
        "benchmark_mappings",
        settings=settings,
    )
    write_canonical_table(
        pd.DataFrame(
            {
                "peer_set_id": ["peer_set::AAPL"],
                "ticker": ["AAPL"],
                "peer_ticker": ["MSFT"],
                "peer_source": ["automatic"],
                "relationship_type": ["sector"],
                "sector": ["Technology"],
                "industry": ["Consumer Electronics"],
                "selection_method": ["sector_market_cap_proximity"],
                "peer_order": [1],
            }
        ),
        "peer_sets",
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
            "primary_benchmark": "SPY",
            "secondary_benchmark": "QQQ",
            "gross_margin": 0.42,
            "operating_margin": 0.31,
            "free_cash_flow_margin": 0.20,
            "return_on_equity": 0.28,
            "return_on_assets": 0.12,
            "current_ratio": 1.5,
            "debt_to_equity": 1.2,
            "total_cash": 60.0,
            "total_debt": 100.0,
            "free_cash_flow": 60.0,
            "market_cap": 1_000.0,
            "ebitda": 120.0,
            "forward_pe": 18.0,
            "ev_to_ebitda": 12.0,
            "price_to_sales": 3.5,
            "max_drawdown_1y": -0.18,
            "price_vs_sma_200": 0.08,
        }
    )
    write_canonical_table(pd.DataFrame([mart_row]), "gold_ticker_metrics_mart", settings=settings)

    market_data = SimpleNamespace(
        histories={
            "AAPL": pd.DataFrame(
                {
                    "ticker": ["AAPL", "AAPL"],
                    "date": pd.to_datetime(["2024-12-30", "2024-12-31"]),
                    "adj_close": [100.0, 101.0],
                }
            )
        },
        trailing_return_comparison=pd.DataFrame(),
        benchmark_comparison=pd.DataFrame(
            [
                {"benchmark_ticker": "SPY", "window": "1y", "excess_return": 0.12},
                {"benchmark_ticker": "QQQ", "window": "1y", "excess_return": 0.04},
            ]
        ),
        primary_benchmark="SPY",
        secondary_benchmark="QQQ",
    )
    monkeypatch.setattr(
        "dumb_money.outputs.research_summary_section.build_market_performance_section_data",
        lambda *args, **kwargs: market_data,
    )

    data = build_research_summary_section_data("AAPL", settings=settings)

    assert data.company_name == "Apple Mart Name"
    report_date = data.summary_table.loc[data.summary_table["label"] == "Report Date", "value"].iloc[0]
    assert report_date == "2025-01-15"
