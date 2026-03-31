"""Benchmark ingestion helpers for definitions and raw benchmark price extracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.ingestion.prices import (
    build_price_filename,
    fetch_prices,
    normalize_tickers,
    save_price_frame,
)
from dumb_money.models import BenchmarkCategory, BenchmarkDefinition

BENCHMARK_COLUMNS = list(BenchmarkDefinition.model_fields.keys())

DEFAULT_BENCHMARK_METADATA: dict[str, dict[str, str]] = {
    "SPY": {
        "name": "SPDR S&P 500 ETF Trust",
        "category": BenchmarkCategory.MARKET.value,
        "scope": "us_large_cap",
        "description": "Core US large-cap equity benchmark ETF.",
    },
    "QQQ": {
        "name": "Invesco QQQ Trust",
        "category": BenchmarkCategory.STYLE.value,
        "scope": "us_large_cap_growth",
        "description": "Nasdaq-100 large-cap growth benchmark ETF.",
    },
    "IWM": {
        "name": "iShares Russell 2000 ETF",
        "category": BenchmarkCategory.STYLE.value,
        "scope": "us_small_cap",
        "description": "US small-cap equity benchmark ETF.",
    },
}


def build_benchmark_definitions_filename(label: str, as_of_date: date | str) -> str:
    clean_date = str(as_of_date).replace("-", "")
    clean_label = str(label).strip().lower().replace(" ", "_")
    return f"{clean_label}_benchmark_definitions_{clean_date}.csv"


def _coerce_benchmark_definition(item: str | Mapping[str, Any] | BenchmarkDefinition) -> BenchmarkDefinition:
    if isinstance(item, BenchmarkDefinition):
        return item

    if isinstance(item, str):
        ticker = item.strip().upper()
        metadata = DEFAULT_BENCHMARK_METADATA.get(ticker, {})
        return BenchmarkDefinition(
            benchmark_id=ticker,
            ticker=ticker,
            name=metadata.get("name", ticker),
            category=metadata.get("category", BenchmarkCategory.MARKET.value),
            scope=metadata.get("scope"),
            description=metadata.get("description"),
        )

    payload = dict(item)
    ticker = str(payload.get("ticker", payload.get("benchmark_id", ""))).strip().upper()
    payload.setdefault("benchmark_id", ticker)
    payload.setdefault("ticker", ticker)
    if ticker in DEFAULT_BENCHMARK_METADATA:
        for key, value in DEFAULT_BENCHMARK_METADATA[ticker].items():
            payload.setdefault(key, value)
    payload.setdefault("name", ticker)
    return BenchmarkDefinition.model_validate(payload)


def normalize_benchmark_definitions(
    definitions: Sequence[str | Mapping[str, Any] | BenchmarkDefinition],
) -> list[BenchmarkDefinition]:
    """Normalize benchmark input definitions while preserving order."""

    normalized: list[BenchmarkDefinition] = []
    seen: set[str] = set()

    for item in definitions:
        definition = _coerce_benchmark_definition(item)
        if definition.benchmark_id in seen:
            continue
        normalized.append(definition)
        seen.add(definition.benchmark_id)

    return normalized


def benchmark_definitions_to_frame(
    definitions: Sequence[str | Mapping[str, Any] | BenchmarkDefinition],
) -> pd.DataFrame:
    normalized = normalize_benchmark_definitions(definitions)
    records = [definition.model_dump(mode="json") for definition in normalized]
    return pd.DataFrame(records, columns=BENCHMARK_COLUMNS)


def save_benchmark_definitions_frame(frame: pd.DataFrame, *, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path


def default_benchmark_definitions(settings: AppSettings | None = None) -> list[BenchmarkDefinition]:
    settings = settings or get_settings()
    return normalize_benchmark_definitions(settings.default_benchmarks)


def ingest_benchmark_definitions(
    definitions: Sequence[str | Mapping[str, Any] | BenchmarkDefinition] | None = None,
    *,
    settings: AppSettings | None = None,
    as_of_date: date | str | None = None,
    label: str = "default",
    save_csv: bool = True,
) -> pd.DataFrame:
    """Persist benchmark definitions to the raw benchmark directory."""

    settings = settings or get_settings()
    settings.ensure_directories()

    resolved_as_of = as_of_date or date.today()
    resolved_definitions = definitions or default_benchmark_definitions(settings)
    frame = benchmark_definitions_to_frame(resolved_definitions)

    if save_csv and not frame.empty:
        save_benchmark_definitions_frame(
            frame,
            output_path=settings.raw_benchmarks_dir
            / build_benchmark_definitions_filename(label, resolved_as_of),
        )

    return frame


def ingest_benchmark_prices(
    definitions: Sequence[str | Mapping[str, Any] | BenchmarkDefinition] | None = None,
    *,
    tickers: Sequence[str] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    interval: str | None = None,
    settings: AppSettings | None = None,
    label: str = "default",
    save_definitions: bool = True,
    save_individual: bool = True,
    save_combined: bool = True,
) -> pd.DataFrame:
    """Download benchmark prices and persist them under the benchmark raw path."""

    settings = settings or get_settings()
    settings.ensure_directories()

    resolved_interval = interval or settings.default_price_interval
    resolved_start, resolved_end = settings.default_price_window()
    resolved_start = start_date or resolved_start
    resolved_end = end_date or resolved_end

    resolved_inputs: Sequence[str | Mapping[str, Any] | BenchmarkDefinition]
    if definitions is not None:
        resolved_inputs = definitions
    elif tickers is not None:
        resolved_inputs = normalize_tickers(tickers)
    else:
        resolved_inputs = settings.default_benchmarks

    definitions_frame = benchmark_definitions_to_frame(resolved_inputs)
    benchmark_tickers = definitions_frame["ticker"].tolist() if not definitions_frame.empty else []

    if save_definitions and not definitions_frame.empty:
        save_benchmark_definitions_frame(
            definitions_frame,
            output_path=settings.raw_benchmarks_dir
            / build_benchmark_definitions_filename(label, resolved_end),
        )

    prices = fetch_prices(
        benchmark_tickers,
        start_date=resolved_start,
        end_date=resolved_end,
        interval=resolved_interval,
    )

    if prices.empty:
        return prices

    if save_individual:
        for ticker, frame in prices.groupby("ticker", sort=True):
            save_price_frame(
                frame,
                output_path=settings.raw_benchmarks_dir
                / build_price_filename(ticker, resolved_start, resolved_end, resolved_interval),
            )

    if save_combined:
        save_price_frame(
            prices,
            output_path=settings.raw_benchmarks_dir
            / build_price_filename(
                f"{label}_benchmark_prices",
                resolved_start,
                resolved_end,
                resolved_interval,
            ),
        )

    return prices
