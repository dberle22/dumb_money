from __future__ import annotations

import pandas as pd

from dumb_money.analytics.scorecard import build_company_scorecard, resolve_secondary_benchmark


def test_resolve_secondary_benchmark_uses_sector_mapping() -> None:
    resolved = resolve_secondary_benchmark("Technology", {"SPY", "QQQ", "IWM"})

    assert resolved == "QQQ"


def test_build_company_scorecard_scores_and_tracks_coverage() -> None:
    benchmark_comparison = pd.DataFrame(
        [
            {"benchmark_ticker": "SPY", "window": "1y", "company_return": 0.20, "benchmark_return": 0.10, "excess_return": 0.10},
            {"benchmark_ticker": "QQQ", "window": "1y", "company_return": 0.20, "benchmark_return": 0.15, "excess_return": 0.05},
        ]
    )
    risk_metrics = {"max_drawdown_1y": -0.12}
    trend_metrics = {"price_vs_sma_200": 0.08}
    fundamentals_summary = {
        "operating_margin": 0.31,
        "free_cash_flow_margin": 0.18,
        "return_on_equity": 0.22,
        "return_on_assets": 0.14,
        "gross_margin": 0.55,
        "total_debt": 50.0,
        "total_cash": 70.0,
        "ebitda": 100.0,
        "current_ratio": 1.7,
        "debt_to_equity": 40.0,
        "free_cash_flow": 60.0,
        "forward_pe": 18.0,
        "ev_to_ebitda": 12.0,
        "price_to_sales": 3.5,
        "market_cap": 1000.0,
        "dividend_yield": 0.01,
    }

    scorecard = build_company_scorecard(
        ticker="AAPL",
        company_name="Apple Inc.",
        sector="Technology",
        industry="Consumer Electronics",
        score_date="2026-03-27",
        benchmark_comparison=benchmark_comparison,
        risk_metrics=risk_metrics,
        trend_metrics=trend_metrics,
        fundamentals_summary=fundamentals_summary,
    )

    summary = scorecard.summary
    metrics = scorecard.metrics.set_index("metric_id")

    assert summary["ticker"] == "AAPL"
    assert summary["primary_benchmark"] == "SPY"
    assert summary["secondary_benchmark"] == "QQQ"
    assert round(summary["confidence_score"], 4) == 1.0
    assert summary["total_score"] > 70
    assert metrics.loc["return_vs_spy_1y", "metric_score"] == 7.5
    assert metrics.loc["net_debt_to_ebitda", "metric_score"] == 8.0
    assert "free cash flow yield" in metrics.loc["yield_metric", "notes"].lower()


def test_build_company_scorecard_uses_peer_relative_valuation_when_available() -> None:
    benchmark_comparison = pd.DataFrame(
        [
            {"benchmark_ticker": "SPY", "window": "1y", "company_return": 0.20, "benchmark_return": 0.10, "excess_return": 0.10},
            {"benchmark_ticker": "QQQ", "window": "1y", "company_return": 0.20, "benchmark_return": 0.15, "excess_return": 0.05},
        ]
    )
    fundamentals_summary = {
        "operating_margin": 0.31,
        "free_cash_flow_margin": 0.18,
        "return_on_equity": 0.22,
        "return_on_assets": 0.14,
        "gross_margin": 0.55,
        "total_debt": 50.0,
        "total_cash": 70.0,
        "ebitda": 100.0,
        "current_ratio": 1.7,
        "debt_to_equity": 40.0,
        "free_cash_flow": 60.0,
        "forward_pe": 18.0,
        "ev_to_ebitda": 12.0,
        "price_to_sales": 3.5,
        "market_cap": 1000.0,
        "dividend_yield": 0.01,
    }
    peer_valuation_comparison = pd.DataFrame(
        [
            {"ticker": "AAPL", "forward_pe": 18.0, "ev_to_ebitda": 12.0, "price_to_sales": 3.5, "is_focal_company": True},
            {"ticker": "MSFT", "forward_pe": 24.0, "ev_to_ebitda": 16.0, "price_to_sales": 6.0, "is_focal_company": False},
            {"ticker": "DELL", "forward_pe": 20.0, "ev_to_ebitda": 14.0, "price_to_sales": 4.5, "is_focal_company": False},
        ]
    )

    scorecard = build_company_scorecard(
        ticker="AAPL",
        company_name="Apple Inc.",
        sector="Technology",
        industry="Consumer Electronics",
        score_date="2026-03-27",
        benchmark_comparison=benchmark_comparison,
        risk_metrics={"max_drawdown_1y": -0.12},
        trend_metrics={"price_vs_sma_200": 0.08},
        fundamentals_summary=fundamentals_summary,
        peer_valuation_comparison=peer_valuation_comparison,
    )

    metrics = scorecard.metrics.set_index("metric_id")
    assert metrics.loc["forward_pe", "scoring_method"] == "peer_relative"
    assert "peer median" in metrics.loc["forward_pe", "notes"].lower()
    assert metrics.loc["ev_to_ebitda", "scoring_method"] == "peer_relative"
    assert metrics.loc["price_to_sales", "scoring_method"] == "peer_relative"


def test_build_company_scorecard_falls_back_to_absolute_valuation_thresholds_without_peers() -> None:
    scorecard = build_company_scorecard(
        ticker="AAPL",
        company_name="Apple Inc.",
        sector="Technology",
        industry="Consumer Electronics",
        score_date="2026-03-27",
        benchmark_comparison=pd.DataFrame(
            [{"benchmark_ticker": "SPY", "window": "1y", "company_return": 0.20, "benchmark_return": 0.10, "excess_return": 0.10}]
        ),
        risk_metrics={"max_drawdown_1y": -0.12},
        trend_metrics={"price_vs_sma_200": 0.08},
        fundamentals_summary={
            "operating_margin": 0.31,
            "free_cash_flow_margin": 0.18,
            "return_on_equity": 0.22,
            "return_on_assets": 0.14,
            "gross_margin": 0.55,
            "total_debt": 50.0,
            "total_cash": 70.0,
            "ebitda": 100.0,
            "current_ratio": 1.7,
            "debt_to_equity": 40.0,
            "free_cash_flow": 60.0,
            "forward_pe": 18.0,
            "ev_to_ebitda": 12.0,
            "price_to_sales": 3.5,
            "market_cap": 1000.0,
            "dividend_yield": 0.01,
        },
        peer_valuation_comparison=pd.DataFrame(),
    )

    metrics = scorecard.metrics.set_index("metric_id")
    assert metrics.loc["forward_pe", "scoring_method"] == "threshold"
    assert metrics.loc["ev_to_ebitda", "scoring_method"] == "threshold"
    assert metrics.loc["price_to_sales", "scoring_method"] == "threshold"
