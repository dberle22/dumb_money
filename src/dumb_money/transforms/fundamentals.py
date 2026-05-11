"""Normalize raw fundamentals extracts into canonical staging tables."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.ingestion.fundamentals import FUNDAMENTAL_COLUMNS
from dumb_money.models import FundamentalSnapshot
from dumb_money.storage import export_table_csv, upsert_canonical_table, write_canonical_table

NUMERIC_FUNDAMENTAL_COLUMNS = [
    "market_cap",
    "enterprise_value",
    "revenue",
    "revenue_ttm",
    "gross_profit",
    "operating_income",
    "net_income",
    "ebitda",
    "free_cash_flow",
    "total_debt",
    "total_cash",
    "current_assets",
    "current_liabilities",
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


def _normalize_period_defaults(normalized: pd.DataFrame) -> pd.DataFrame:
    if "period_end_date" not in normalized.columns:
        normalized["period_end_date"] = normalized["as_of_date"]
    if "report_date" not in normalized.columns:
        normalized["report_date"] = None
    if "fiscal_year" not in normalized.columns:
        normalized["fiscal_year"] = pd.to_datetime(normalized["period_end_date"], errors="coerce").dt.year
    if "fiscal_quarter" not in normalized.columns:
        normalized["fiscal_quarter"] = None
    if "fiscal_period" not in normalized.columns:
        normalized["fiscal_period"] = "TTM"
    if "period_type" not in normalized.columns:
        normalized["period_type"] = "ttm"
    if "revenue" not in normalized.columns:
        normalized["revenue"] = normalized.get("revenue_ttm")
    if "current_assets" not in normalized.columns:
        normalized["current_assets"] = None
    if "current_liabilities" not in normalized.columns:
        normalized["current_liabilities"] = None
    return normalized


def _nullable_int(value: object) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _replace_missing(record: dict[str, object]) -> dict[str, object]:
    return {key: (None if pd.isna(value) else value) for key, value in record.items()}


def normalize_fundamentals_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Coerce raw fundamentals snapshots into the canonical staged schema."""

    if frame.empty:
        return pd.DataFrame(columns=FUNDAMENTAL_COLUMNS)

    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    normalized = _normalize_period_defaults(normalized)

    missing = [column for column in FUNDAMENTAL_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"fundamentals frame is missing required canonical columns: {missing}")

    normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
    normalized["currency"] = normalized["currency"].fillna(get_settings().default_currency)
    normalized["currency"] = normalized["currency"].astype(str).str.strip().str.upper()
    normalized["source"] = normalized["source"].astype(str).str.strip().str.lower()
    normalized["as_of_date"] = pd.to_datetime(normalized["as_of_date"]).dt.date
    normalized["period_end_date"] = pd.to_datetime(normalized["period_end_date"], errors="coerce").dt.date
    normalized["report_date"] = pd.to_datetime(normalized["report_date"], errors="coerce").dt.date
    normalized["fiscal_year"] = pd.to_numeric(normalized["fiscal_year"], errors="coerce")
    normalized["fiscal_quarter"] = pd.to_numeric(normalized["fiscal_quarter"], errors="coerce")
    normalized["fiscal_period"] = normalized["fiscal_period"].astype(str).str.strip().replace({"": None})
    normalized["period_type"] = normalized["period_type"].astype(str).str.strip().str.lower().replace({"": None})

    if "pulled_at" in normalized.columns:
        normalized["pulled_at"] = pd.to_datetime(normalized["pulled_at"], errors="coerce")

    for column in NUMERIC_FUNDAMENTAL_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized["fiscal_year"] = normalized["fiscal_year"].map(_nullable_int)
    normalized["fiscal_quarter"] = normalized["fiscal_quarter"].map(_nullable_int)

    normalized = normalized.dropna(subset=["ticker", "as_of_date"])
    normalized = normalized.dropna(subset=["period_end_date"], how="any")
    normalized = normalized.drop_duplicates(
        subset=["ticker", "period_end_date", "period_type", "as_of_date"],
        keep="last",
    )
    normalized = normalized.sort_values(
        ["ticker", "period_end_date", "period_type", "as_of_date"]
    ).reset_index(drop=True)

    records = [
        FundamentalSnapshot.model_validate(_replace_missing(record)).model_dump(mode="json")
        for record in normalized.to_dict(orient="records")
    ]
    return pd.DataFrame(records, columns=FUNDAMENTAL_COLUMNS)


def stage_fundamentals(
    *,
    input_paths: Sequence[str | Path] | None = None,
    settings: AppSettings | None = None,
    output_name: str = "normalized_fundamentals.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
    incremental: bool = True,
) -> pd.DataFrame:
    """Build the normalized fundamentals staging dataset from raw snapshot CSVs."""

    settings = settings or get_settings()
    settings.ensure_directories()

    paths = _resolve_fundamentals_input_paths(input_paths, settings=settings)
    if not paths:
        return pd.DataFrame(columns=FUNDAMENTAL_COLUMNS)

    frames = [pd.read_csv(path) for path in paths]
    normalized = normalize_fundamentals_frame(pd.concat(frames, ignore_index=True))

    materialized = normalized
    if write_warehouse:
        materialized = (
            upsert_canonical_table(normalized, "normalized_fundamentals", settings=settings)
            if incremental
            else write_canonical_table(normalized, "normalized_fundamentals", settings=settings)
        )

    if write_csv and not materialized.empty:
        if output_name == "normalized_fundamentals.csv":
            export_table_csv(materialized, "normalized_fundamentals", settings=settings)
        else:
            output_path = settings.normalized_fundamentals_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            materialized.to_csv(output_path, index=False)

    return materialized
