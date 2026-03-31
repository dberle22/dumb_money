"""Ingestion entry points."""

from dumb_money.ingestion.benchmarks import (
    ingest_benchmark_definitions,
    ingest_benchmark_prices,
    normalize_benchmark_definitions,
)
from dumb_money.ingestion.fundamentals import ingest_fundamentals, normalize_fundamentals_payload
from dumb_money.ingestion.prices import ingest_prices, normalize_price_history_frame

__all__ = [
    "ingest_benchmark_definitions",
    "ingest_benchmark_prices",
    "ingest_fundamentals",
    "ingest_prices",
    "normalize_benchmark_definitions",
    "normalize_fundamentals_payload",
    "normalize_price_history_frame",
]
