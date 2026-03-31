"""Reusable company analytics built on normalized price and fundamentals data."""

from __future__ import annotations

from collections.abc import Mapping
from math import sqrt
from typing import Any

import pandas as pd

STANDARD_RETURN_WINDOWS: dict[str, int] = {
    "1m": 21,
    "3m": 63,
    "6m": 126,
    "1y": 252,
}

STANDARD_MOVING_AVERAGES: tuple[int, ...] = (20, 50, 200)


def prepare_price_history(prices: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Filter and normalize one ticker's price history for downstream metrics."""

    if prices.empty:
        return pd.DataFrame(columns=["ticker", "date", "adj_close", "daily_return"])

    history = prices.loc[prices["ticker"].astype(str).str.upper() == ticker.strip().upper()].copy()
    if history.empty:
        return pd.DataFrame(columns=["ticker", "date", "adj_close", "daily_return"])

    history["date"] = pd.to_datetime(history["date"])
    history["adj_close"] = pd.to_numeric(history["adj_close"], errors="coerce")
    history = history.dropna(subset=["date", "adj_close"]).sort_values("date").reset_index(drop=True)
    history["daily_return"] = history["adj_close"].pct_change()
    return history


def calculate_return_windows(
    price_history: pd.DataFrame,
    *,
    windows: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """Calculate trailing total returns over standard trading-day windows."""

    windows = windows or STANDARD_RETURN_WINDOWS
    rows: list[dict[str, Any]] = []

    if price_history.empty:
        return pd.DataFrame(
            columns=[
                "window",
                "trading_days",
                "start_date",
                "end_date",
                "start_price",
                "end_price",
                "total_return",
            ]
        )

    history = price_history.sort_values("date").reset_index(drop=True)

    for label, trading_days in windows.items():
        row: dict[str, Any] = {
            "window": label,
            "trading_days": trading_days,
            "start_date": pd.NaT,
            "end_date": history["date"].iloc[-1],
            "start_price": pd.NA,
            "end_price": history["adj_close"].iloc[-1],
            "total_return": pd.NA,
        }
        if len(history) > trading_days:
            start_row = history.iloc[-(trading_days + 1)]
            end_row = history.iloc[-1]
            row.update(
                {
                    "start_date": start_row["date"],
                    "start_price": start_row["adj_close"],
                    "total_return": (end_row["adj_close"] / start_row["adj_close"]) - 1,
                }
            )
        rows.append(row)

    return pd.DataFrame(rows)


def calculate_risk_metrics(price_history: pd.DataFrame) -> dict[str, float | None]:
    """Calculate trailing volatility and drawdown metrics."""

    if price_history.empty:
        return {
            "annualized_volatility_1m": None,
            "annualized_volatility_3m": None,
            "annualized_volatility_1y": None,
            "current_drawdown": None,
            "max_drawdown": None,
            "max_drawdown_1y": None,
        }

    history = price_history.sort_values("date").reset_index(drop=True)
    daily_returns = history["daily_return"].dropna()

    def annualized_volatility(window: int) -> float | None:
        if len(daily_returns) < window:
            return None
        return float(daily_returns.tail(window).std(ddof=1) * sqrt(252))

    def max_drawdown(series: pd.Series) -> float | None:
        if series.empty:
            return None
        running_peak = series.cummax()
        drawdown = (series / running_peak) - 1
        return float(drawdown.min())

    prices = history["adj_close"]
    trailing_prices = prices.tail(252)
    running_peak = prices.cummax()
    current_drawdown = float(((prices / running_peak) - 1).iloc[-1])

    return {
        "annualized_volatility_1m": annualized_volatility(21),
        "annualized_volatility_3m": annualized_volatility(63),
        "annualized_volatility_1y": annualized_volatility(252),
        "current_drawdown": current_drawdown,
        "max_drawdown": max_drawdown(prices),
        "max_drawdown_1y": max_drawdown(trailing_prices),
    }


def calculate_trend_metrics(
    price_history: pd.DataFrame,
    *,
    moving_averages: tuple[int, ...] = STANDARD_MOVING_AVERAGES,
) -> dict[str, float | bool | None]:
    """Calculate simple moving-average and price-trend metrics."""

    metrics: dict[str, float | bool | None] = {}
    if price_history.empty:
        for window in moving_averages:
            metrics[f"sma_{window}"] = None
            metrics[f"price_vs_sma_{window}"] = None
        metrics["sma_20_above_sma_50"] = None
        metrics["sma_50_above_sma_200"] = None
        return metrics

    history = price_history.sort_values("date").reset_index(drop=True)
    prices = history["adj_close"]
    latest_price = float(prices.iloc[-1])

    for window in moving_averages:
        sma = prices.rolling(window=window).mean().iloc[-1]
        if pd.isna(sma):
            metrics[f"sma_{window}"] = None
            metrics[f"price_vs_sma_{window}"] = None
            continue
        metrics[f"sma_{window}"] = float(sma)
        metrics[f"price_vs_sma_{window}"] = float((latest_price / sma) - 1)

    sma_20 = metrics.get("sma_20")
    sma_50 = metrics.get("sma_50")
    sma_200 = metrics.get("sma_200")
    metrics["sma_20_above_sma_50"] = bool(sma_20 > sma_50) if sma_20 is not None and sma_50 is not None else None
    metrics["sma_50_above_sma_200"] = bool(sma_50 > sma_200) if sma_50 is not None and sma_200 is not None else None
    return metrics


def build_indexed_price_series(
    price_history: pd.DataFrame,
    *,
    ticker: str | None = None,
    trading_days: int = 252,
    base_value: float = 100.0,
) -> pd.DataFrame:
    """Build an indexed price series over a trailing display window."""

    if price_history.empty:
        return pd.DataFrame(columns=["date", "ticker", "adj_close", "indexed_price"])

    history = price_history.sort_values("date").reset_index(drop=True).copy()
    window_history = history.tail(trading_days + 1).copy()
    if window_history.empty:
        return pd.DataFrame(columns=["date", "ticker", "adj_close", "indexed_price"])

    start_price = float(window_history["adj_close"].iloc[0])
    if start_price == 0:
        return pd.DataFrame(columns=["date", "ticker", "adj_close", "indexed_price"])

    label = ticker or str(window_history.get("ticker", pd.Series(dtype=str)).iloc[-1])
    window_history["ticker"] = label
    window_history["indexed_price"] = (window_history["adj_close"] / start_price) * base_value
    return window_history[["date", "ticker", "adj_close", "indexed_price"]].reset_index(drop=True)


def build_drawdown_series(
    price_history: pd.DataFrame,
    *,
    ticker: str | None = None,
    trading_days: int = 252,
) -> pd.DataFrame:
    """Build a trailing drawdown time series from adjusted close history."""

    if price_history.empty:
        return pd.DataFrame(columns=["date", "ticker", "adj_close", "running_peak", "drawdown"])

    history = price_history.sort_values("date").reset_index(drop=True).copy()
    window_history = history.tail(trading_days + 1).copy()
    if window_history.empty:
        return pd.DataFrame(columns=["date", "ticker", "adj_close", "running_peak", "drawdown"])

    label = ticker or str(window_history.get("ticker", pd.Series(dtype=str)).iloc[-1])
    window_history["ticker"] = label
    window_history["running_peak"] = window_history["adj_close"].cummax()
    window_history["drawdown"] = (window_history["adj_close"] / window_history["running_peak"]) - 1
    return window_history[["date", "ticker", "adj_close", "running_peak", "drawdown"]].reset_index(drop=True)


def build_moving_average_series(
    price_history: pd.DataFrame,
    *,
    trading_days: int = 252,
    moving_averages: tuple[int, ...] = (50, 200),
) -> pd.DataFrame:
    """Build price history with trailing moving averages for charting."""

    if price_history.empty:
        columns = ["date", "ticker", "adj_close", *[f"sma_{window}" for window in moving_averages]]
        return pd.DataFrame(columns=columns)

    history = price_history.sort_values("date").reset_index(drop=True).copy()
    for window in moving_averages:
        history[f"sma_{window}"] = history["adj_close"].rolling(window=window).mean()
    window_history = history.tail(trading_days + 1).copy()
    selected_columns = ["date", "ticker", "adj_close", *[f"sma_{window}" for window in moving_averages]]
    return window_history[selected_columns].reset_index(drop=True)


def build_trailing_return_comparison(
    company_history: pd.DataFrame,
    benchmark_histories: Mapping[str, pd.DataFrame],
    *,
    windows: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """Build a wide trailing return panel for company and selected benchmarks."""

    windows = windows or STANDARD_RETURN_WINDOWS
    company_returns = calculate_return_windows(company_history, windows=windows)[["window", "total_return"]].rename(
        columns={"total_return": "company_return"}
    )
    if company_returns.empty:
        return pd.DataFrame(columns=["window", "company_return"])

    comparison = company_returns.copy()
    for benchmark_ticker, history in benchmark_histories.items():
        benchmark_returns = calculate_return_windows(history, windows=windows)[["window", "total_return"]].rename(
            columns={"total_return": f"{benchmark_ticker}_return"}
        )
        comparison = comparison.merge(benchmark_returns, on="window", how="left")

    return comparison


def build_fundamentals_summary(fundamentals: pd.DataFrame, ticker: str) -> dict[str, Any]:
    """Build a latest-snapshot fundamentals summary with a few derived fields."""

    if fundamentals.empty:
        return {"ticker": ticker.strip().upper()}

    rows = fundamentals.loc[
        fundamentals["ticker"].astype(str).str.upper() == ticker.strip().upper()
    ].copy()
    if rows.empty:
        return {"ticker": ticker.strip().upper()}

    rows["as_of_date"] = pd.to_datetime(rows["as_of_date"])
    latest = rows.sort_values("as_of_date").iloc[-1]

    total_cash = latest.get("total_cash")
    total_debt = latest.get("total_debt")
    revenue_ttm = latest.get("revenue_ttm")
    free_cash_flow = latest.get("free_cash_flow")

    net_cash = (
        float(total_cash) - float(total_debt)
        if pd.notna(total_cash) and pd.notna(total_debt)
        else None
    )
    free_cash_flow_margin = (
        float(free_cash_flow) / float(revenue_ttm)
        if pd.notna(free_cash_flow) and pd.notna(revenue_ttm) and float(revenue_ttm) != 0
        else None
    )

    summary_fields = [
        "ticker",
        "as_of_date",
        "long_name",
        "sector",
        "industry",
        "currency",
        "market_cap",
        "enterprise_value",
        "revenue_ttm",
        "ebitda",
        "gross_margin",
        "operating_margin",
        "profit_margin",
        "free_cash_flow",
        "return_on_equity",
        "return_on_assets",
        "trailing_pe",
        "forward_pe",
        "ev_to_ebitda",
        "price_to_sales",
        "dividend_yield",
        "total_cash",
        "total_debt",
        "current_ratio",
        "debt_to_equity",
    ]
    summary = {field: latest.get(field) for field in summary_fields}
    summary["as_of_date"] = latest["as_of_date"].date().isoformat()
    summary["net_cash"] = net_cash
    summary["free_cash_flow_margin"] = free_cash_flow_margin
    return summary


def build_benchmark_comparison(
    company_history: pd.DataFrame,
    benchmark_histories: Mapping[str, pd.DataFrame],
    *,
    windows: Mapping[str, int] | None = None,
) -> pd.DataFrame:
    """Compare company returns against benchmark returns across standard windows."""

    windows = windows or STANDARD_RETURN_WINDOWS
    company_returns = calculate_return_windows(company_history, windows=windows)
    if company_returns.empty:
        return pd.DataFrame(
            columns=[
                "benchmark_ticker",
                "window",
                "company_return",
                "benchmark_return",
                "excess_return",
            ]
        )

    rows: list[dict[str, Any]] = []
    for benchmark_ticker, history in benchmark_histories.items():
        benchmark_returns = calculate_return_windows(history, windows=windows).set_index("window")
        for company_row in company_returns.to_dict(orient="records"):
            benchmark_return = benchmark_returns.loc[company_row["window"], "total_return"]
            company_return = company_row["total_return"]
            excess_return = (
                company_return - benchmark_return
                if pd.notna(company_return) and pd.notna(benchmark_return)
                else pd.NA
            )
            rows.append(
                {
                    "benchmark_ticker": benchmark_ticker,
                    "window": company_row["window"],
                    "company_return": company_return,
                    "benchmark_return": benchmark_return,
                    "excess_return": excess_return,
                }
            )

    return pd.DataFrame(rows)
