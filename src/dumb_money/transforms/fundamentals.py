"""Normalize raw fundamentals extracts into canonical staging tables."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.ingestion.fundamentals import SNAPSHOT_COLUMNS
from dumb_money.models import FundamentalSnapshot

NUMERIC_FUNDAMENTAL_COLUMNS = [
    "market_cap",
    "enterprise_value",
    "revenue_ttm",
    "gross_profit",
    "operating_income",
    "net_income",
    "ebitda",
    "free_cash_flow",
    "total_debt",
    "total_cash",
    "shares_outstanding",
    "eps_trailing",
    "eps_forward",
    "gross_margin",
    "operating_margin",
    "profit_margin",
    "return_on_equity",
    "return_on_assets",
    "debt_to_equity",
    "current_ratio",
    "trailing_pe",
    "forward_pe",
    "price_to_sales",
    "ev_to_ebitda",
    "dividend_yield",
]


def _resolve_fundamentals_input_paths(
    input_paths: Sequence[str | Path] | None,
    *,
    settings: AppSettings,
) -> list[Path]:
    if input_paths:
        return [Path(path) for path in input_paths]
    return sorted(settings.raw_fundamentals_dir.glob("*_fundamentals_flat_*.csv"))


def normalize_fundamentals_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Coerce raw fundamentals snapshots into the canonical staged schema."""

    if frame.empty:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]

    missing = [column for column in SNAPSHOT_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"fundamentals frame is missing required canonical columns: {missing}")

    normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
    normalized["currency"] = normalized["currency"].fillna(get_settings().default_currency)
    normalized["currency"] = normalized["currency"].astype(str).str.strip().str.upper()
    normalized["source"] = normalized["source"].astype(str).str.strip().str.lower()
    normalized["as_of_date"] = pd.to_datetime(normalized["as_of_date"]).dt.date

    if "pulled_at" in normalized.columns:
        normalized["pulled_at"] = pd.to_datetime(normalized["pulled_at"], errors="coerce")

    for column in NUMERIC_FUNDAMENTAL_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized.dropna(subset=["ticker", "as_of_date"])
    normalized = normalized.drop_duplicates(subset=["ticker", "as_of_date"], keep="last")
    normalized = normalized.sort_values(["ticker", "as_of_date"]).reset_index(drop=True)

    records = [FundamentalSnapshot.model_validate(record).model_dump(mode="json") for record in normalized.to_dict(orient="records")]
    return pd.DataFrame(records, columns=SNAPSHOT_COLUMNS)


def stage_fundamentals(
    *,
    input_paths: Sequence[str | Path] | None = None,
    settings: AppSettings | None = None,
    output_name: str = "normalized_fundamentals.csv",
    write_csv: bool = True,
) -> pd.DataFrame:
    """Build the normalized fundamentals staging dataset from raw snapshot CSVs."""

    settings = settings or get_settings()
    settings.ensure_directories()

    paths = _resolve_fundamentals_input_paths(input_paths, settings=settings)
    if not paths:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    frames = [pd.read_csv(path) for path in paths]
    normalized = normalize_fundamentals_frame(pd.concat(frames, ignore_index=True))

    if write_csv and not normalized.empty:
        output_path = settings.normalized_fundamentals_dir / output_name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        normalized.to_csv(output_path, index=False)

    return normalized
