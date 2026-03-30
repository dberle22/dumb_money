"""Ingestion entry points."""

from dumb_money.ingestion.fundamentals import ingest_fundamentals, normalize_fundamentals_payload
from dumb_money.ingestion.prices import ingest_prices, normalize_price_history_frame

__all__ = [
    "ingest_fundamentals",
    "ingest_prices",
    "normalize_fundamentals_payload",
    "normalize_price_history_frame",
]
