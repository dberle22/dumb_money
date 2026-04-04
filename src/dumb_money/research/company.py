"""Company research packet assembly built on shared staged datasets."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from dumb_money.analytics.company import (
    build_benchmark_comparison,
    build_fundamentals_summary,
    build_peer_return_comparison,
    build_peer_return_summary_stats,
    build_peer_summary_stats,
    build_peer_valuation_comparison,
    build_trailing_return_comparison,
    calculate_return_windows,
    calculate_risk_metrics,
    calculate_trend_metrics,
    prepare_price_history,
)
from dumb_money.analytics.scorecard import (
    CompanyScorecard,
    build_company_scorecard,
)
from dumb_money.config import AppSettings, get_settings
from dumb_money.storage import read_canonical_table
from dumb_money.transforms.sector_snapshots import build_sector_snapshots_frame


@dataclass(slots=True)
class CompanyResearchPacket:
    """Structured company research outputs for one ticker."""

    ticker: str
    company_name: str | None
    as_of_date: str | None
    company_history: pd.DataFrame
    benchmark_histories: dict[str, pd.DataFrame]
    return_windows: pd.DataFrame
    trailing_return_comparison: pd.DataFrame
    risk_metrics: dict[str, float | None]
    trend_metrics: dict[str, float | bool | None]
    benchmark_comparison: pd.DataFrame
    fundamentals_summary: dict[str, Any]
    peer_return_comparison: pd.DataFrame
    peer_return_summary_stats: dict[str, Any]
    peer_valuation_comparison: pd.DataFrame
    peer_summary_stats: dict[str, Any]
    sector_snapshot: dict[str, Any]
    scorecard: CompanyScorecard


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def load_staged_prices(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("normalized_prices", settings=settings)


def load_staged_fundamentals(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("normalized_fundamentals", settings=settings)


def load_benchmark_set(*, settings: AppSettings | None = None, set_id: str | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    benchmark_sets = read_canonical_table("benchmark_sets", settings=settings)
    if benchmark_sets.empty:
        return benchmark_sets

    if set_id is not None:
        filtered = benchmark_sets.loc[
            benchmark_sets["set_id"].astype(str).str.lower() == set_id.strip().lower()
        ].copy()
        return filtered.sort_values("member_order").reset_index(drop=True)

    default_sets = benchmark_sets.loc[benchmark_sets["is_default"] == True].copy()  # noqa: E712
    if default_sets.empty:
        return benchmark_sets.sort_values("member_order").reset_index(drop=True)

    chosen_set_id = default_sets["set_id"].iloc[0]
    return default_sets.loc[default_sets["set_id"] == chosen_set_id].sort_values("member_order").reset_index(drop=True)


def load_benchmark_prices(*, settings: AppSettings | None = None) -> pd.DataFrame:
    # Benchmark ETF histories are expected to be canonicalized into DuckDB through
    # the shared price staging flow, not loaded ad hoc from raw benchmark CSVs.
    return load_staged_prices(settings=settings)


def load_security_master(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("security_master", settings=settings)


def load_benchmark_mappings(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("benchmark_mappings", settings=settings)


def load_peer_sets(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("peer_sets", settings=settings)


def load_sector_snapshots(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("sector_snapshots", settings=settings)


def build_company_research_packet(
    ticker: str,
    *,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> CompanyResearchPacket:
    """Assemble a one-ticker company research packet from shared modules only."""

    settings = settings or get_settings()
    normalized_ticker = ticker.strip().upper()

    prices = load_staged_prices(settings=settings)
    company_history = prepare_price_history(prices, normalized_ticker)
    if company_history.empty:
        raise ValueError(f"no staged price history found for ticker {normalized_ticker}")

    fundamentals = load_staged_fundamentals(settings=settings)
    fundamentals_summary = build_fundamentals_summary(fundamentals, normalized_ticker)

    benchmark_set = load_benchmark_set(settings=settings, set_id=benchmark_set_id)
    benchmark_prices = load_benchmark_prices(settings=settings)
    benchmark_histories = {
        benchmark_ticker: prepare_price_history(benchmark_prices, benchmark_ticker)
        for benchmark_ticker in benchmark_set.get("ticker", pd.Series(dtype=str)).tolist()
    }
    benchmark_histories = {
        benchmark_ticker: history
        for benchmark_ticker, history in benchmark_histories.items()
        if not history.empty
    }
    return_windows = calculate_return_windows(company_history)
    risk_metrics = calculate_risk_metrics(company_history)
    trend_metrics = calculate_trend_metrics(company_history)
    benchmark_comparison = build_benchmark_comparison(company_history, benchmark_histories)
    trailing_return_comparison = build_trailing_return_comparison(company_history, benchmark_histories)
    security_master = load_security_master(settings=settings)
    benchmark_mappings = load_benchmark_mappings(settings=settings)
    peer_sets = load_peer_sets(settings=settings)
    sector_snapshots = load_sector_snapshots(settings=settings)
    if sector_snapshots.empty:
        sector_snapshots = build_sector_snapshots_frame(
            security_master,
            fundamentals,
            prices,
            benchmark_mappings,
        )
    security_rows = (
        security_master.loc[security_master["ticker"] == normalized_ticker].copy()
        if "ticker" in security_master.columns
        else pd.DataFrame()
    )
    benchmark_mapping_rows = (
        benchmark_mappings.loc[benchmark_mappings["ticker"] == normalized_ticker].copy()
        if "ticker" in benchmark_mappings.columns
        else pd.DataFrame()
    )
    security_row = security_rows.iloc[-1].to_dict() if not security_rows.empty else {}
    benchmark_mapping_row = benchmark_mapping_rows.iloc[-1].to_dict() if not benchmark_mapping_rows.empty else {}
    end_dates = return_windows["end_date"].dropna() if "end_date" in return_windows.columns else pd.Series(dtype=object)
    score_date = (
        str(pd.to_datetime(end_dates.iloc[-1]).date())
        if not return_windows.empty and not end_dates.empty
        else fundamentals_summary.get("as_of_date")
    )
    peer_rows = (
        peer_sets.loc[peer_sets["ticker"] == normalized_ticker].copy()
        if "ticker" in peer_sets.columns
        else pd.DataFrame()
    )
    peer_valuation_comparison = build_peer_valuation_comparison(
        normalized_ticker,
        peer_rows,
        fundamentals,
    )
    peer_return_comparison = build_peer_return_comparison(
        normalized_ticker,
        peer_rows,
        prices,
    )
    peer_return_summary_stats = build_peer_return_summary_stats(peer_return_comparison)
    peer_summary_stats = build_peer_summary_stats(peer_valuation_comparison)
    scorecard = build_company_scorecard(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        sector=fundamentals_summary.get("sector") or security_row.get("sector"),
        industry=fundamentals_summary.get("industry") or security_row.get("industry"),
        score_date=score_date,
        benchmark_comparison=benchmark_comparison,
        risk_metrics=risk_metrics,
        trend_metrics=trend_metrics,
        fundamentals_summary=fundamentals_summary,
        peer_valuation_comparison=peer_valuation_comparison,
        primary_benchmark=benchmark_mapping_row.get("primary_benchmark"),
        secondary_benchmark=(
            benchmark_mapping_row.get("sector_benchmark")
            or benchmark_mapping_row.get("style_benchmark")
            or benchmark_mapping_row.get("industry_benchmark")
            or benchmark_mapping_row.get("custom_benchmark")
        ),
    )
    company_sector = fundamentals_summary.get("sector") or security_row.get("sector")
    sector_snapshot_rows = (
        sector_snapshots.loc[sector_snapshots["sector"] == company_sector].copy()
        if company_sector and "sector" in sector_snapshots.columns
        else pd.DataFrame()
    )
    sector_snapshot = sector_snapshot_rows.iloc[-1].to_dict() if not sector_snapshot_rows.empty else {}

    return CompanyResearchPacket(
        ticker=normalized_ticker,
        company_name=fundamentals_summary.get("long_name"),
        as_of_date=fundamentals_summary.get("as_of_date"),
        company_history=company_history,
        benchmark_histories=benchmark_histories,
        return_windows=return_windows,
        trailing_return_comparison=trailing_return_comparison,
        risk_metrics=risk_metrics,
        trend_metrics=trend_metrics,
        benchmark_comparison=benchmark_comparison,
        fundamentals_summary=fundamentals_summary,
        peer_return_comparison=peer_return_comparison,
        peer_return_summary_stats=peer_return_summary_stats,
        peer_valuation_comparison=peer_valuation_comparison,
        peer_summary_stats=peer_summary_stats,
        sector_snapshot=sector_snapshot,
        scorecard=scorecard,
    )
