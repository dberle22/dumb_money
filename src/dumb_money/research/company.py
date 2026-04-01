"""Company research packet assembly built on shared staged datasets."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from dumb_money.analytics.company import (
    build_benchmark_comparison,
    build_fundamentals_summary,
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
from dumb_money.transforms.prices import normalize_prices_frame


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
    settings = settings or get_settings()
    combined_paths = sorted(settings.raw_benchmarks_dir.glob("*_benchmark_prices_*.csv"))
    if combined_paths:
        frames = [pd.read_csv(path) for path in combined_paths]
        return normalize_prices_frame(pd.concat(frames, ignore_index=True))

    individual_paths = sorted(settings.raw_benchmarks_dir.glob("*.csv"))
    price_paths = [path for path in individual_paths if "benchmark_definitions" not in path.name]
    if not price_paths:
        return pd.DataFrame()
    frames = [pd.read_csv(path) for path in price_paths]
    return normalize_prices_frame(pd.concat(frames, ignore_index=True))


def load_security_master(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("security_master", settings=settings)


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
    security_rows = (
        security_master.loc[security_master["ticker"] == normalized_ticker].copy()
        if "ticker" in security_master.columns
        else pd.DataFrame()
    )
    security_row = security_rows.iloc[-1].to_dict() if not security_rows.empty else {}
    end_dates = return_windows["end_date"].dropna() if "end_date" in return_windows.columns else pd.Series(dtype=object)
    score_date = (
        str(pd.to_datetime(end_dates.iloc[-1]).date())
        if not return_windows.empty and not end_dates.empty
        else fundamentals_summary.get("as_of_date")
    )
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
    )

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
        scorecard=scorecard,
    )
