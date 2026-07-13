"""Portfolio holdings ingestion helpers."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings, get_settings
from dumb_money.models import Holding
from dumb_money.storage.warehouse import export_table_csv, upsert_canonical_table

HOLDING_COLUMNS = list(Holding.model_fields.keys())

HOLDING_COLUMN_ALIASES: dict[str, str] = {
    "symbol": "ticker",
    "shares": "quantity",
    "qty": "quantity",
    "avg_cost": "average_cost",
    "cost_basis": "average_cost",
    "value": "market_value",
    "market value": "market_value",
    "account": "account_name",
    "portfolio": "portfolio_id",
    "date": "as_of_date",
}


def _replace_missing(record: dict[str, object]) -> dict[str, object]:
    return {key: (None if pd.isna(value) else value) for key, value in record.items()}


def _normalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    normalized.columns = [
        HOLDING_COLUMN_ALIASES.get(str(column).strip().lower(), str(column).strip())
        for column in normalized.columns
    ]
    return normalized


def normalize_holdings_frame(
    frame: pd.DataFrame,
    *,
    portfolio_id: str | None = None,
    as_of_date: date | None = None,
) -> pd.DataFrame:
    """Normalize a holdings input frame into the canonical portfolio schema."""

    if frame.empty:
        return pd.DataFrame(columns=HOLDING_COLUMNS)

    normalized = _normalize_columns(frame)

    if "ticker" not in normalized.columns or "quantity" not in normalized.columns:
        raise ValueError("holdings frame must include at least ticker and quantity columns")

    if "portfolio_id" not in normalized.columns:
        normalized["portfolio_id"] = portfolio_id or "default"
    elif portfolio_id is not None:
        normalized["portfolio_id"] = portfolio_id

    if "as_of_date" not in normalized.columns:
        if as_of_date is None:
            raise ValueError("holdings frame must include as_of_date or an as_of_date override")
        normalized["as_of_date"] = as_of_date
    elif as_of_date is not None:
        normalized["as_of_date"] = as_of_date

    for column in HOLDING_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = None

    normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
    normalized["portfolio_id"] = normalized["portfolio_id"].fillna("default").astype(str).str.strip()
    normalized["account_name"] = normalized["account_name"].replace({"": None})
    normalized["notes"] = normalized["notes"].replace({"": None})
    normalized["as_of_date"] = pd.to_datetime(normalized["as_of_date"], errors="coerce").dt.date

    for numeric_column in ("quantity", "average_cost", "market_value", "weight"):
        normalized[numeric_column] = pd.to_numeric(normalized[numeric_column], errors="coerce")

    if normalized["weight"].isna().all() and normalized["market_value"].notna().any():
        group_keys = ["portfolio_id", "as_of_date"]
        totals = normalized.groupby(group_keys)["market_value"].transform("sum")
        normalized["weight"] = normalized["market_value"] / totals.where(totals != 0)

    normalized = normalized.dropna(subset=["ticker", "quantity", "as_of_date"]).reset_index(drop=True)
    normalized = normalized.drop_duplicates(
        subset=["portfolio_id", "ticker", "as_of_date", "account_name"],
        keep="last",
    )
    normalized = normalized.sort_values(
        ["portfolio_id", "as_of_date", "ticker", "account_name"],
        na_position="last",
    ).reset_index(drop=True)

    records = [
        Holding.model_validate(_replace_missing(record)).model_dump(mode="json")
        for record in normalized[HOLDING_COLUMNS].to_dict(orient="records")
    ]
    return pd.DataFrame(records, columns=HOLDING_COLUMNS)


def ingest_portfolio_holdings(
    input_path: str | Path,
    *,
    settings: AppSettings | None = None,
    portfolio_id: str | None = None,
    as_of_date: date | None = None,
    copy_to_raw: bool = True,
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Ingest a file-based holdings extract into the canonical portfolio table."""

    settings = settings or get_settings()
    settings.ensure_directories()

    source_path = Path(input_path)
    frame = pd.read_csv(source_path)
    normalized = normalize_holdings_frame(frame, portfolio_id=portfolio_id, as_of_date=as_of_date)

    if copy_to_raw:
        suffix_date = as_of_date.isoformat() if as_of_date else "as_of_input"
        target_path = settings.raw_portfolios_dir / f"{source_path.stem}_{suffix_date}{source_path.suffix}"
        shutil.copy2(source_path, target_path)

    materialized = normalized
    if write_warehouse:
        materialized = upsert_canonical_table(normalized, "portfolio_holdings", settings=settings)
    if write_csv:
        export_table_csv(materialized, "portfolio_holdings", settings=settings)
    return materialized
