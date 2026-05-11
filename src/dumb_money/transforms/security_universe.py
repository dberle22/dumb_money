"""Normalize listed-security universe inputs into reusable staging tables."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings, get_settings
from dumb_money.ingestion.universe import load_nasdaq_listed_frame, load_other_listed_frame
from dumb_money.storage import (
    LISTED_SECURITY_SEED_COLUMNS,
    SECURITY_MASTER_OVERRIDE_COLUMNS,
    export_table_csv,
    read_canonical_table,
    write_canonical_table,
)
from dumb_money.validation import validate_listed_security_seed_frame

OTHER_LISTED_EXCHANGE_MAP = {
    "A": "NYSE American",
    "N": "NYSE",
    "P": "NYSE Arca",
    "Q": "Nasdaq",
    "V": "IEX",
    "Z": "Cboe BZX",
}

NASDAQ_MARKET_CATEGORY_MAP = {
    "Q": "Nasdaq Global Select Market",
    "G": "Nasdaq Global Market",
    "S": "Nasdaq Capital Market",
}

ELIGIBLE_ASSET_TYPES = {"common_stock", "adr"}


def _resolve_input_paths(
    input_paths: Sequence[str | Path] | None,
    fallback_glob: str,
    *,
    base_dir: Path,
) -> list[Path]:
    if input_paths:
        return [Path(path) for path in input_paths]
    return sorted(base_dir.glob(fallback_glob))


def _flag_to_bool(value: object) -> bool:
    return str(value).strip().upper() in {"Y", "YES", "TRUE", "1"}


def _infer_asset_type_raw(name: str, *, is_etf: bool) -> str:
    lowered = name.lower()
    if is_etf:
        return "etf"
    if "adr" in lowered or "american depositary" in lowered or "ads" in lowered:
        return "adr"
    if "warrant" in lowered:
        return "warrant"
    if " right" in lowered or lowered.endswith(" rights"):
        return "right"
    if " unit" in lowered or lowered.endswith(" units"):
        return "unit"
    if "preferred" in lowered or "depositary share" in lowered:
        return "preferred_stock"
    if "fund" in lowered or "portfolio" in lowered:
        return "fund"
    return "common_stock"


def _eligibility(asset_type_raw: str, *, is_test_issue: bool, is_active: bool) -> tuple[bool, str]:
    if is_test_issue:
        return False, "test_issue"
    if not is_active:
        return False, "inactive_listing"
    if asset_type_raw not in ELIGIBLE_ASSET_TYPES:
        return False, f"excluded_{asset_type_raw}"
    return True, "eligible"


def _normalize_nasdaq_listed_frame(frame: pd.DataFrame, *, source_file: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=LISTED_SECURITY_SEED_COLUMNS)

    normalized = pd.DataFrame(
        {
            "ticker": frame["Symbol"],
            "name": frame["Security Name"],
            "exchange": "Nasdaq",
            "listing_market": frame["Market Category"].map(NASDAQ_MARKET_CATEGORY_MAP).fillna("Nasdaq"),
            "is_etf": frame["ETF"].map(_flag_to_bool),
            "is_test_issue": frame["Test Issue"].map(_flag_to_bool),
            "is_active": ~frame["Financial Status"].astype(str).str.strip().str.upper().eq("D"),
            "round_lot_size": pd.to_numeric(frame["Round Lot Size"], errors="coerce").fillna(100).astype(int),
        }
    )
    normalized["asset_type_raw"] = normalized.apply(
        lambda row: _infer_asset_type_raw(row["name"], is_etf=bool(row["is_etf"])),
        axis=1,
    )
    eligibility = normalized.apply(
        lambda row: _eligibility(
            str(row["asset_type_raw"]),
            is_test_issue=bool(row["is_test_issue"]),
            is_active=bool(row["is_active"]),
        ),
        axis=1,
    )
    normalized["is_eligible_research_universe"] = [item[0] for item in eligibility]
    normalized["eligibility_reason"] = [item[1] for item in eligibility]
    normalized["source"] = "nasdaq_trader"
    normalized["source_file"] = source_file
    normalized["as_of_date"] = None
    return normalized[LISTED_SECURITY_SEED_COLUMNS]


def _normalize_other_listed_frame(frame: pd.DataFrame, *, source_file: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=LISTED_SECURITY_SEED_COLUMNS)

    normalized = pd.DataFrame(
        {
            "ticker": frame["ACT Symbol"],
            "name": frame["Security Name"],
            "exchange": frame["Exchange"].map(OTHER_LISTED_EXCHANGE_MAP).fillna("Other Listed"),
            "listing_market": frame["Exchange"].map(OTHER_LISTED_EXCHANGE_MAP).fillna("Other Listed"),
            "is_etf": frame["ETF"].map(_flag_to_bool),
            "is_test_issue": frame["Test Issue"].map(_flag_to_bool),
            "is_active": True,
            "round_lot_size": pd.to_numeric(frame["Round Lot Size"], errors="coerce").fillna(100).astype(int),
        }
    )
    normalized["asset_type_raw"] = normalized.apply(
        lambda row: _infer_asset_type_raw(row["name"], is_etf=bool(row["is_etf"])),
        axis=1,
    )
    eligibility = normalized.apply(
        lambda row: _eligibility(
            str(row["asset_type_raw"]),
            is_test_issue=bool(row["is_test_issue"]),
            is_active=bool(row["is_active"]),
        ),
        axis=1,
    )
    normalized["is_eligible_research_universe"] = [item[0] for item in eligibility]
    normalized["eligibility_reason"] = [item[1] for item in eligibility]
    normalized["source"] = "nasdaq_trader"
    normalized["source_file"] = source_file
    normalized["as_of_date"] = None
    return normalized[LISTED_SECURITY_SEED_COLUMNS]


def normalize_listed_security_seed_frame(
    nasdaq_listed: pd.DataFrame,
    other_listed: pd.DataFrame,
    *,
    as_of_date: date | str | None = None,
    nasdaq_source_file: str = "nasdaqlisted.txt",
    other_source_file: str = "otherlisted.txt",
) -> pd.DataFrame:
    """Combine listed-security directories into one canonical seed table."""

    frames = [
        _normalize_nasdaq_listed_frame(nasdaq_listed, source_file=nasdaq_source_file),
        _normalize_other_listed_frame(other_listed, source_file=other_source_file),
    ]
    combined = pd.concat(frames, ignore_index=True) if any(not frame.empty for frame in frames) else pd.DataFrame(columns=LISTED_SECURITY_SEED_COLUMNS)
    if combined.empty:
        return combined

    combined["ticker"] = combined["ticker"].astype(str).str.strip().str.upper()
    combined["as_of_date"] = str(as_of_date or date.today())
    combined = combined.drop_duplicates(subset=["ticker"], keep="first")
    return combined.sort_values(["ticker"]).reset_index(drop=True)[LISTED_SECURITY_SEED_COLUMNS]


def normalize_security_master_overrides(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize field-level security master overrides."""

    if frame.empty:
        return pd.DataFrame(columns=SECURITY_MASTER_OVERRIDE_COLUMNS)

    normalized = frame.copy()
    missing = [column for column in SECURITY_MASTER_OVERRIDE_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"override frame is missing required columns: {missing}")

    normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
    normalized["field_name"] = normalized["field_name"].astype(str).str.strip()
    normalized["override_value"] = normalized["override_value"].where(
        normalized["override_value"].notna(),
        None,
    )
    normalized["updated_at"] = normalized["updated_at"].astype(str).str.strip()
    normalized = normalized.drop_duplicates(subset=["ticker", "field_name"], keep="last")
    return normalized[SECURITY_MASTER_OVERRIDE_COLUMNS].sort_values(["ticker", "field_name"]).reset_index(drop=True)


def normalize_existing_listed_security_seed(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a previously materialized listed-security seed frame."""

    if frame.empty:
        return pd.DataFrame(columns=LISTED_SECURITY_SEED_COLUMNS)

    normalized = frame.copy()
    missing = [column for column in LISTED_SECURITY_SEED_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"listed security seed frame is missing required columns: {missing}")

    normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
    normalized["name"] = normalized["name"].astype(str).str.strip()
    normalized["exchange"] = normalized["exchange"].astype(str).str.strip()
    normalized["listing_market"] = normalized["listing_market"].astype(str).str.strip()
    normalized["asset_type_raw"] = normalized["asset_type_raw"].astype(str).str.strip().str.lower()
    for column in ["is_etf", "is_test_issue", "is_active", "is_eligible_research_universe"]:
        normalized[column] = normalized[column].map(_flag_to_bool) if normalized[column].dtype == object else normalized[column].astype(bool)
    normalized["round_lot_size"] = pd.to_numeric(normalized["round_lot_size"], errors="coerce").fillna(100).astype(int)
    normalized["source"] = normalized["source"].astype(str).str.strip()
    normalized["source_file"] = normalized["source_file"].astype(str).str.strip()
    normalized["as_of_date"] = normalized["as_of_date"].astype(str).str.strip()
    normalized = normalized.drop_duplicates(subset=["ticker"], keep="first")
    return normalized[LISTED_SECURITY_SEED_COLUMNS].sort_values(["ticker"]).reset_index(drop=True)


def stage_listed_security_seed(
    *,
    nasdaq_listed_paths: Sequence[str | Path] | None = None,
    other_listed_paths: Sequence[str | Path] | None = None,
    settings: AppSettings | None = None,
    as_of_date: date | str | None = None,
    output_name: str = "listed_security_seed.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Build the canonical listed-security seed staging table."""

    settings = settings or get_settings()
    settings.ensure_directories()

    nasdaq_paths = _resolve_input_paths(
        nasdaq_listed_paths,
        "nasdaqlisted_*.txt",
        base_dir=settings.raw_universe_dir,
    )
    other_paths = _resolve_input_paths(
        other_listed_paths,
        "otherlisted_*.txt",
        base_dir=settings.raw_universe_dir,
    )

    if not nasdaq_paths or not other_paths:
        return pd.DataFrame(columns=LISTED_SECURITY_SEED_COLUMNS)

    nasdaq_frame = pd.concat(
        [load_nasdaq_listed_frame(path) for path in nasdaq_paths],
        ignore_index=True,
    )
    other_frame = pd.concat(
        [load_other_listed_frame(path) for path in other_paths],
        ignore_index=True,
    )
    seed = normalize_listed_security_seed_frame(
        nasdaq_frame,
        other_frame,
        as_of_date=as_of_date,
        nasdaq_source_file=nasdaq_paths[-1].name,
        other_source_file=other_paths[-1].name,
    )
    validate_listed_security_seed_frame(seed)

    if write_warehouse:
        write_canonical_table(seed, "listed_security_seed", settings=settings)

    if write_csv and not seed.empty:
        if output_name == "listed_security_seed.csv":
            export_table_csv(seed, "listed_security_seed", settings=settings)
        else:
            output_path = settings.listed_security_seed_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            seed.to_csv(output_path, index=False)

    return seed


def stage_security_master_overrides(
    *,
    input_paths: Sequence[str | Path] | None = None,
    settings: AppSettings | None = None,
    output_name: str = "security_master_overrides.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Materialize the manual security master override table."""

    settings = settings or get_settings()
    settings.ensure_directories()
    paths = _resolve_input_paths(input_paths, "security_master_overrides*.csv", base_dir=settings.reference_dir)
    if not paths:
        overrides = pd.DataFrame(columns=SECURITY_MASTER_OVERRIDE_COLUMNS)
    else:
        overrides = normalize_security_master_overrides(
            pd.concat([pd.read_csv(path) for path in paths], ignore_index=True)
        )

    if write_warehouse:
        write_canonical_table(overrides, "security_master_overrides", settings=settings)

    if write_csv:
        if output_name == "security_master_overrides.csv":
            export_table_csv(overrides, "security_master_overrides", settings=settings)
        else:
            output_path = settings.reference_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            overrides.to_csv(output_path, index=False)

    return overrides


def load_listed_security_seed(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return normalize_existing_listed_security_seed(
        read_canonical_table("listed_security_seed", settings=settings)
    )


def load_security_master_overrides(*, settings: AppSettings | None = None) -> pd.DataFrame:
    settings = settings or get_settings()
    return read_canonical_table("security_master_overrides", settings=settings)
