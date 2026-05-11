"""Gold-layer scorecard metric row builders for reusable score transparency sections."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dumb_money.analytics.scorecard import build_company_scorecard
from dumb_money.config import AppSettings, get_settings
from dumb_money.research.company import (
    build_fundamentals_summary_from_mart_row,
    build_peer_summary_stats_from_mart_row,
    build_risk_metrics_from_mart_row,
    build_trend_metrics_from_mart_row,
    load_gold_ticker_metrics_mart,
)
from dumb_money.storage import (
    GOLD_SCORECARD_METRIC_ROWS_COLUMNS,
    export_table_csv,
    write_canonical_table,
)
from dumb_money.transforms.ticker_metrics_mart import stage_gold_ticker_metrics_mart


def _build_benchmark_comparison_from_mart_row(mart_row: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for benchmark_key, excess_key, return_key in (
        ("primary_benchmark", "excess_return_primary_1y", "primary_benchmark_return_1y"),
        ("secondary_benchmark", "excess_return_secondary_1y", "secondary_benchmark_return_1y"),
    ):
        benchmark_ticker = mart_row.get(benchmark_key)
        if not benchmark_ticker:
            continue
        rows.append(
            {
                "benchmark_ticker": benchmark_ticker,
                "window": "1y",
                "benchmark_return": mart_row.get(return_key),
                "excess_return": mart_row.get(excess_key),
            }
        )
    return pd.DataFrame(rows)


def _build_peer_valuation_comparison_from_mart_row(mart_row: dict[str, Any]) -> pd.DataFrame:
    peer_summary_stats = build_peer_summary_stats_from_mart_row(mart_row)
    if not peer_summary_stats:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "ticker": mart_row.get("ticker"),
                "company_name": mart_row.get("company_name"),
                "is_focal_company": True,
                "forward_pe": mart_row.get("forward_pe"),
                "ev_to_ebitda": mart_row.get("ev_to_ebitda"),
                "price_to_sales": mart_row.get("price_to_sales"),
                "free_cash_flow_yield": (
                    None
                    if mart_row.get("market_cap") in (None, 0) or pd.isna(mart_row.get("market_cap"))
                    else float(mart_row.get("free_cash_flow") or 0.0) / float(mart_row.get("market_cap"))
                ),
            },
            {
                "ticker": f"{mart_row.get('ticker')}_PEER_MEDIAN",
                "company_name": "Peer Median",
                "is_focal_company": False,
                "forward_pe": peer_summary_stats.get("median_forward_pe"),
                "ev_to_ebitda": peer_summary_stats.get("median_ev_to_ebitda"),
                "price_to_sales": peer_summary_stats.get("median_price_to_sales"),
                "free_cash_flow_yield": peer_summary_stats.get("median_free_cash_flow_yield"),
            },
        ]
    )


def build_gold_scorecard_metric_rows_frame(
    *,
    settings: AppSettings | None = None,
    tickers: list[str] | None = None,
) -> pd.DataFrame:
    """Build the canonical one-row-per-ticker-score-date-metric artifact."""

    settings = settings or get_settings()
    mart = load_gold_ticker_metrics_mart(settings=settings)
    if mart.empty:
        mart = stage_gold_ticker_metrics_mart(
            settings=settings,
            tickers=tickers,
            write_warehouse=True,
            write_csv=True,
        )
    if mart.empty:
        return pd.DataFrame(columns=GOLD_SCORECARD_METRIC_ROWS_COLUMNS)

    mart_frame = mart.copy()
    if tickers:
        normalized_tickers = {ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()}
        mart_frame = mart_frame.loc[mart_frame["ticker"].astype(str).str.upper().isin(normalized_tickers)].copy()

    rows: list[dict[str, Any]] = []
    for mart_row in mart_frame.to_dict(orient="records"):
        ticker = str(mart_row.get("ticker") or "").upper()
        score_date = mart_row.get("score_date")
        if not ticker or not score_date:
            continue

        scorecard = build_company_scorecard(
            ticker=ticker,
            company_name=mart_row.get("company_name"),
            sector=mart_row.get("sector"),
            industry=mart_row.get("industry"),
            score_date=score_date,
            benchmark_comparison=_build_benchmark_comparison_from_mart_row(mart_row),
            risk_metrics=build_risk_metrics_from_mart_row(mart_row),
            trend_metrics=build_trend_metrics_from_mart_row(mart_row),
            fundamentals_summary=build_fundamentals_summary_from_mart_row(mart_row),
            peer_valuation_comparison=_build_peer_valuation_comparison_from_mart_row(mart_row),
            primary_benchmark=mart_row.get("primary_benchmark"),
            secondary_benchmark=mart_row.get("secondary_benchmark"),
        )

        metric_rows = scorecard.metrics.copy()
        if metric_rows.empty:
            continue

        for metric_row in metric_rows.to_dict(orient="records"):
            rows.append(
                {
                    "scorecard_metric_row_id": f"gold_scorecard_metric::{ticker}::{score_date}::{metric_row['metric_id']}",
                    "ticker": ticker,
                    "score_date": score_date,
                    "metric_id": metric_row.get("metric_id"),
                    "category": metric_row.get("category"),
                    "metric_name": metric_row.get("metric_name"),
                    "raw_value": metric_row.get("raw_value"),
                    "normalized_value": metric_row.get("normalized_value"),
                    "scoring_method": metric_row.get("scoring_method"),
                    "metric_score": metric_row.get("metric_score"),
                    "metric_weight": metric_row.get("metric_weight"),
                    "metric_available": metric_row.get("metric_available"),
                    "metric_applicable": metric_row.get("metric_applicable"),
                    "confidence_flag": metric_row.get("confidence_flag"),
                    "notes": metric_row.get("notes"),
                    "company_name": mart_row.get("company_name"),
                    "sector": mart_row.get("sector"),
                    "industry": mart_row.get("industry"),
                    "primary_benchmark": mart_row.get("primary_benchmark"),
                    "secondary_benchmark": mart_row.get("secondary_benchmark"),
                }
            )

    if not rows:
        return pd.DataFrame(columns=GOLD_SCORECARD_METRIC_ROWS_COLUMNS)
    return pd.DataFrame(rows, columns=GOLD_SCORECARD_METRIC_ROWS_COLUMNS)


def stage_gold_scorecard_metric_rows(
    *,
    settings: AppSettings | None = None,
    tickers: list[str] | None = None,
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Build and persist the canonical gold-layer scorecard metric rows artifact."""

    settings = settings or get_settings()
    metric_rows = build_gold_scorecard_metric_rows_frame(settings=settings, tickers=tickers)
    if write_warehouse:
        metric_rows = write_canonical_table(metric_rows, "gold_scorecard_metric_rows", settings=settings)
    if write_csv:
        export_table_csv(metric_rows, "gold_scorecard_metric_rows", settings=settings)
    return metric_rows
