from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from dumb_money.analytics.scorecard import CompanyScorecard
from dumb_money.config import AppSettings
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
from dumb_money.storage import (
    GOLD_SCORECARD_METRIC_ROWS_COLUMNS,
    GOLD_TICKER_METRICS_MART_COLUMNS,
    write_canonical_table,
)


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


def test_final_research_summary_section_uses_gold_score_artifact_when_available(tmp_path, monkeypatch) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()

    dates = pd.bdate_range("2025-01-02", periods=260)
    prices = pd.concat(
        [
            pd.DataFrame(
                {
                    "ticker": ticker,
                    "date": dates.date,
                    "interval": "1d",
                    "source": "fixture",
                    "currency": "USD",
                    "open": values,
                    "high": [value * 1.01 for value in values],
                    "low": [value * 0.99 for value in values],
                    "close": values,
                    "adj_close": values,
                    "volume": [1_000_000] * len(values),
                }
            )
            for ticker, values in {
                "AAPL": [100.0 + (idx * 0.45) for idx in range(len(dates))],
                "SPY": [95.0 + (idx * 0.20) for idx in range(len(dates))],
                "QQQ": [98.0 + (idx * 0.28) for idx in range(len(dates))],
            }.items()
        ],
        ignore_index=True,
    )
    write_canonical_table(prices, "normalized_prices", settings=settings)

    fundamentals = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "as_of_date": "2025-12-31",
                "period_end_date": "2025-12-31",
                "report_date": "2026-01-30",
                "fiscal_year": 2025,
                "fiscal_quarter": 4,
                "fiscal_period": "TTM",
                "period_type": "ttm",
                "source": "fixture",
                "currency": "USD",
                "long_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "market_cap": 1000.0,
                "enterprise_value": 1100.0,
                "revenue": 500.0,
                "revenue_ttm": 500.0,
                "gross_profit": 220.0,
                "operating_income": 150.0,
                "net_income": 120.0,
                "ebitda": 170.0,
                "free_cash_flow": 100.0,
                "total_debt": 90.0,
                "total_cash": 60.0,
                "current_assets": 140.0,
                "current_liabilities": 95.0,
                "shares_outstanding": 10.0,
                "gross_margin": 0.44,
                "operating_margin": 0.30,
                "profit_margin": 0.24,
                "return_on_equity": 0.28,
                "return_on_assets": 0.12,
                "debt_to_equity": 1.2,
                "current_ratio": 1.47,
                "trailing_pe": 30.0,
                "forward_pe": 28.0,
                "price_to_sales": 7.4,
                "ev_to_ebitda": 18.0,
                "dividend_yield": 0.005,
                "raw_payload_path": "fixture",
                "pulled_at": "2026-01-31T00:00:00Z",
            }
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
                "assignment_method": ["fixture"],
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
                "set_id": ["sample_universe", "sample_universe"],
                "benchmark_id": ["SPY", "QQQ"],
                "ticker": ["SPY", "QQQ"],
                "name": ["SPY", "QQQ"],
                "category": ["market", "style"],
                "scope": ["us_large_cap", "us_large_cap_growth"],
                "currency": ["USD", "USD"],
                "description": [None, None],
                "member_order": [1, 2],
                "is_default": [True, True],
            }
        ),
        "benchmark_sets",
        settings=settings,
    )
    write_canonical_table(
        pd.DataFrame(columns=["peer_set_id", "ticker", "peer_ticker", "peer_source", "relationship_type", "sector", "industry", "selection_method", "peer_order"]),
        "peer_sets",
        settings=settings,
    )

    mart_row = {column: None for column in GOLD_TICKER_METRICS_MART_COLUMNS}
    mart_row.update(
        {
            "mart_id": "gold_ticker_metrics::AAPL::2026-01-15",
            "ticker": "AAPL",
            "as_of_date": "2025-12-31",
            "score_date": "2026-01-15",
            "company_name": "Apple Mart Name",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "primary_benchmark": "SPY",
            "secondary_benchmark": "QQQ",
            "market_cap": 1000.0,
            "free_cash_flow": 100.0,
            "ebitda": 170.0,
            "gross_margin": 0.44,
            "operating_margin": 0.30,
            "free_cash_flow_margin": 0.20,
            "return_on_equity": 0.28,
            "return_on_assets": 0.12,
            "current_ratio": 1.47,
            "debt_to_equity": 1.2,
            "total_cash": 60.0,
            "total_debt": 90.0,
            "forward_pe": 28.0,
            "ev_to_ebitda": 18.0,
            "price_to_sales": 7.4,
            "return_1y": 0.22,
            "primary_benchmark_return_1y": 0.14,
            "secondary_benchmark_return_1y": 0.18,
            "excess_return_primary_1y": 0.08,
            "excess_return_secondary_1y": 0.04,
            "annualized_volatility_1y": 0.24,
            "downside_volatility_1y": 0.16,
            "beta_1y": 1.05,
            "current_drawdown": -0.08,
            "max_drawdown_1y": -0.23,
            "price_vs_sma_50": 0.04,
            "price_vs_sma_200": 0.11,
            "sma_50_above_sma_200": True,
            "total_score": 72.0,
            "confidence_score": 0.96,
            "market_performance_score": 18.0,
            "market_performance_available_weight": 25.0,
            "growth_profitability_score": 30.0,
            "growth_profitability_available_weight": 35.0,
            "balance_sheet_score": 16.0,
            "balance_sheet_available_weight": 25.0,
            "valuation_score": 8.0,
            "valuation_available_weight": 15.0,
        }
    )
    write_canonical_table(pd.DataFrame([mart_row]), "gold_ticker_metrics_mart", settings=settings)

    metric_rows = pd.DataFrame(
        [
            {
                "scorecard_metric_row_id": "gold_scorecard_metric::AAPL::2026-01-15::operating_margin",
                "ticker": "AAPL",
                "score_date": "2026-01-15",
                "metric_id": "operating_margin",
                "category": "Growth and Profitability",
                "metric_name": "Operating margin",
                "raw_value": 0.30,
                "normalized_value": 1.0,
                "scoring_method": "threshold",
                "metric_score": 8.0,
                "metric_weight": 8.0,
                "metric_available": True,
                "metric_applicable": True,
                "confidence_flag": "ok",
                "notes": "Latest normalized fundamentals snapshot.",
                "company_name": "Apple Mart Name",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "primary_benchmark": "SPY",
                "secondary_benchmark": "QQQ",
            },
            {
                "scorecard_metric_row_id": "gold_scorecard_metric::AAPL::2026-01-15::forward_pe",
                "ticker": "AAPL",
                "score_date": "2026-01-15",
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
                "company_name": "Apple Mart Name",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "primary_benchmark": "SPY",
                "secondary_benchmark": "QQQ",
            },
        ]
    ).reindex(columns=GOLD_SCORECARD_METRIC_ROWS_COLUMNS)
    write_canonical_table(metric_rows, "gold_scorecard_metric_rows", settings=settings)

    monkeypatch.setattr(
        "dumb_money.outputs.final_research_summary_section.build_company_scorecard",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("fallback scorecard builder should not be used")),
    )

    data = build_final_research_summary_section_data("AAPL", benchmark_set_id="sample_universe", settings=settings)

    assert data.ticker == "AAPL"
    assert data.report_date == "2026-01-15"
    assert data.final_memo_text
