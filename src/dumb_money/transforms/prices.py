"""Normalize raw price extracts into canonical staging tables."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.ingestion.prices import PRICE_COLUMNS, to_price_models
from dumb_money.storage import export_table_csv, upsert_canonical_table, write_canonical_table

NUMERIC_PRICE_COLUMNS = ["open", "high", "low", "close", "adj_close", "volume"]


def _resolve_price_input_paths(
    input_paths: Sequence[str | Path] | None,
    *,
    settings: AppSettings,
) -> list[Path]:
    if input_paths:
        return [Path(path) for path in input_paths]

    individual_paths = sorted(
        path
        for path in settings.raw_prices_dir.glob("*.csv")
        if not path.name.startswith("combined_prices_")
    )
    if individual_paths:
        return individual_paths

    return sorted(settings.raw_prices_dir.glob("combined_prices_*.csv"))


def normalize_prices_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Coerce raw price extracts into the canonical staged schema."""

    if frame.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]

    rename_map = {
        "Date": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "adjclose": "adj_close",
        "Volume": "volume",
    }
    normalized = normalized.rename(columns=rename_map)

    if "adj_close" not in normalized.columns and "close" in normalized.columns:
        normalized["adj_close"] = normalized["close"]

    defaults = {
        "interval": get_settings().default_price_interval,
        "source": "yfinance",
        "currency": get_settings().default_currency,
    }
    for column, default in defaults.items():
        if column not in normalized.columns:
            normalized[column] = default

    missing = [column for column in PRICE_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"price frame is missing required canonical columns: {missing}")

    normalized["date"] = pd.to_datetime(normalized["date"]).dt.date
    normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
    normalized["currency"] = normalized["currency"].astype(str).str.strip().str.upper()
    normalized["source"] = normalized["source"].astype(str).str.strip().str.lower()
    normalized["interval"] = normalized["interval"].astype(str).str.strip()

    for column in NUMERIC_PRICE_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["volume"] = normalized["volume"].fillna(0).astype(int)
    normalized = normalized.dropna(subset=["ticker", "date", "open", "high", "low", "close", "adj_close"])
    normalized = normalized[PRICE_COLUMNS].drop_duplicates(
        subset=["ticker", "date", "interval", "source"],
        keep="last",
    )
    normalized = normalized.sort_values(["ticker", "date"]).reset_index(drop=True)

    return pd.DataFrame(
        [model.model_dump(mode="json") for model in to_price_models(normalized)],
        columns=PRICE_COLUMNS,
    )


def stage_prices(
    *,
    input_paths: Sequence[str | Path] | None = None,
    settings: AppSettings | None = None,
    output_name: str = "normalized_prices.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
    incremental: bool = True,
) -> pd.DataFrame:
    """Build the normalized price staging dataset from raw CSV extracts."""

    settings = settings or get_settings()
    settings.ensure_directories()

    paths = _resolve_price_input_paths(input_paths, settings=settings)
    if not paths:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    frames = [pd.read_csv(path) for path in paths]
    normalized = normalize_prices_frame(pd.concat(frames, ignore_index=True))

    materialized = normalized
    if write_warehouse:
        materialized = (
            upsert_canonical_table(normalized, "normalized_prices", settings=settings)
            if incremental
            else write_canonical_table(normalized, "normalized_prices", settings=settings)
        )

    if write_csv and not materialized.empty:
        if output_name == "normalized_prices.csv":
            export_table_csv(materialized, "normalized_prices", settings=settings)
        else:
            output_path = settings.normalized_prices_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            materialized.to_csv(output_path, index=False)

    return materialized
