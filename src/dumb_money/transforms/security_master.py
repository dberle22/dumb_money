"""Build the canonical security master for downstream joins and universe management."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.models import AssetType, Security
from dumb_money.storage import export_table_csv, read_canonical_table, write_canonical_table
from dumb_money.transforms.benchmark_sets import normalize_benchmark_definition_frame
from dumb_money.transforms.fundamentals import normalize_fundamentals_frame
from dumb_money.transforms.security_universe import (
    load_listed_security_seed,
    load_security_master_overrides,
    normalize_existing_listed_security_seed,
    normalize_security_master_overrides,
)
from dumb_money.validation import validate_security_master_frame

SECURITY_COLUMNS = list(Security.model_fields.keys())


def _resolve_paths(
    input_paths: Sequence[str | Path] | None,
    fallback_glob: str,
    *,
    base_dir: Path,
) -> list[Path]:
    if input_paths:
        return [Path(path) for path in input_paths]
    return sorted(base_dir.glob(fallback_glob))


def _benchmark_asset_type(row: pd.Series) -> AssetType:
    name = str(row.get("name", "")).lower()
    ticker = str(row.get("ticker", "")).strip()
    if ticker.startswith("^") or "index" in name:
        return AssetType.INDEX
    return AssetType.ETF


def _seed_asset_type(row: pd.Series) -> AssetType:
    asset_type_raw = str(row.get("asset_type_raw", "")).strip().lower()
    if asset_type_raw == "etf":
        return AssetType.ETF
    if asset_type_raw == "adr":
        return AssetType.ADR
    if asset_type_raw == "fund":
        return AssetType.MUTUAL_FUND
    if asset_type_raw == "common_stock":
        return AssetType.COMMON_STOCK
    return AssetType.OTHER


def _coerce_override_value(field_name: str, value: object) -> object:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None

    lowered = str(value).strip().lower()
    if field_name in {"is_benchmark", "is_active", "is_eligible_research_universe"}:
        return lowered in {"true", "1", "yes", "y"}
    if field_name in {"first_seen_at", "last_updated_at"}:
        return pd.to_datetime(value).date()
    return value


def _apply_overrides(frame: pd.DataFrame, overrides: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or overrides.empty:
        return frame

    normalized = frame.copy().set_index("ticker", drop=False)
    for row in overrides.to_dict(orient="records"):
        ticker = str(row["ticker"]).strip().upper()
        field_name = str(row["field_name"]).strip()
        if field_name not in normalized.columns:
            raise ValueError(f"override field {field_name!r} is not a security master column")

        if ticker not in normalized.index:
            empty_record = Security(
                security_id=f"sec_{ticker.lower()}",
                ticker=ticker,
                source="manual_override",
                source_id=ticker,
                notes=row.get("reason"),
            ).model_dump(mode="json")
            normalized.loc[ticker] = empty_record

        normalized.at[ticker, field_name] = _coerce_override_value(field_name, row.get("override_value"))
        if row.get("reason"):
            normalized.at[ticker, "notes"] = row["reason"]

    return normalized.reset_index(drop=True)


def build_security_master_frame(
    listed_security_seed: pd.DataFrame,
    fundamentals: pd.DataFrame,
    benchmark_definitions: pd.DataFrame,
    overrides: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Combine seed universe, latest fundamentals, benchmarks, and overrides."""

    records: dict[str, Security] = {}

    normalized_seed = normalize_existing_listed_security_seed(listed_security_seed)

    if not normalized_seed.empty:
        for row in normalized_seed.to_dict(orient="records"):
            as_of_date = pd.to_datetime(row.get("as_of_date"), errors="coerce")
            seen_at = as_of_date.date() if not pd.isna(as_of_date) else None
            security = Security(
                security_id=f"sec_{row['ticker'].lower()}",
                ticker=row["ticker"],
                name=row.get("name"),
                asset_type=_seed_asset_type(pd.Series(row)),
                exchange=row.get("exchange"),
                primary_listing=row.get("listing_market") or row.get("exchange"),
                currency="USD",
                is_active=bool(row.get("is_active", True)),
                is_eligible_research_universe=bool(row.get("is_eligible_research_universe", False)),
                source=row.get("source") or "nasdaq_trader",
                source_id=row.get("ticker"),
                first_seen_at=seen_at,
                last_updated_at=seen_at,
                notes=None if row.get("eligibility_reason") == "eligible" else row.get("eligibility_reason"),
            )
            records[security.ticker] = security

    normalized_fundamentals = normalize_fundamentals_frame(fundamentals)
    if not normalized_fundamentals.empty:
        if "period_end_date" in normalized_fundamentals.columns:
            normalized_fundamentals["period_end_date"] = pd.to_datetime(
                normalized_fundamentals["period_end_date"],
                errors="coerce",
            )
        if "period_type" not in normalized_fundamentals.columns:
            normalized_fundamentals["period_type"] = None
        latest = (
            normalized_fundamentals.assign(
                _period_rank=normalized_fundamentals["period_type"].map(
                    {"quarterly": 0, "annual": 1, "ttm": 2}
                ).fillna(-1)
            )
            .sort_values(["ticker", "as_of_date", "period_end_date", "_period_rank"])
            .groupby("ticker", as_index=False)
            .tail(1)
            .drop(columns="_period_rank")
        )
        for row in latest.to_dict(orient="records"):
            existing = records.get(row["ticker"])
            as_of_date = pd.to_datetime(row.get("as_of_date"), errors="coerce")
            refreshed_at = as_of_date.date() if not pd.isna(as_of_date) else date.today()
            update = {
                "security_id": existing.security_id if existing else f"sec_{row['ticker'].lower()}",
                "ticker": row["ticker"],
                "name": row.get("long_name") or (existing.name if existing else None),
                "asset_type": existing.asset_type if existing else AssetType.COMMON_STOCK,
                "exchange": existing.exchange if existing else None,
                "primary_listing": existing.primary_listing if existing else None,
                "currency": row.get("currency") or (existing.currency if existing else "USD"),
                "sector": row.get("sector") or (existing.sector if existing else None),
                "industry": row.get("industry") or (existing.industry if existing else None),
                "country": existing.country if existing else None,
                "cik": existing.cik if existing else None,
                "is_benchmark": existing.is_benchmark if existing else False,
                "is_active": existing.is_active if existing else True,
                "is_eligible_research_universe": existing.is_eligible_research_universe if existing else True,
                "source": existing.source if existing else "fundamentals_snapshot",
                "source_id": existing.source_id if existing else row["ticker"],
                "first_seen_at": existing.first_seen_at if existing else refreshed_at,
                "last_updated_at": refreshed_at,
                "notes": existing.notes if existing else None,
            }
            records[row["ticker"]] = Security(**update)

    normalized_benchmarks = normalize_benchmark_definition_frame(benchmark_definitions)
    for row in normalized_benchmarks.to_dict(orient="records"):
        existing = records.get(row["ticker"])
        asset_type = _benchmark_asset_type(pd.Series(row))
        if existing is None:
            records[row["ticker"]] = Security(
                security_id=f"sec_{row['ticker'].lower()}",
                ticker=row["ticker"],
                name=row.get("name"),
                asset_type=asset_type,
                primary_listing=None,
                currency=row.get("currency") or "USD",
                is_benchmark=True,
                is_active=True,
                is_eligible_research_universe=False,
                source="benchmark_definition",
                source_id=row.get("benchmark_id") or row["ticker"],
            )
            continue

        records[row["ticker"]] = existing.model_copy(
            update={
                "name": existing.name or row.get("name"),
                "currency": existing.currency or row.get("currency") or "USD",
                "is_benchmark": True,
                "asset_type": asset_type if existing.asset_type == AssetType.OTHER else existing.asset_type,
                "last_updated_at": existing.last_updated_at or date.today(),
            }
        )

    frame = pd.DataFrame(
        [record.model_dump(mode="json") for record in records.values()],
        columns=SECURITY_COLUMNS,
    ).sort_values(["ticker"]).reset_index(drop=True) if records else pd.DataFrame(columns=SECURITY_COLUMNS)

    normalized_overrides = (
        normalize_security_master_overrides(overrides)
        if overrides is not None and not overrides.empty
        else pd.DataFrame(columns=["ticker", "field_name", "override_value", "reason", "updated_at"])
    )
    frame = _apply_overrides(frame, normalized_overrides)
    validated = [
        Security.model_validate(record).model_dump(mode="json")
        for record in frame.to_dict(orient="records")
    ]
    return pd.DataFrame(validated, columns=SECURITY_COLUMNS).sort_values(["ticker"]).reset_index(drop=True)


def stage_security_master(
    *,
    listed_security_paths: Sequence[str | Path] | None = None,
    fundamentals_paths: Sequence[str | Path] | None = None,
    benchmark_definition_paths: Sequence[str | Path] | None = None,
    override_paths: Sequence[str | Path] | None = None,
    settings: AppSettings | None = None,
    output_name: str = "security_master.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    settings = settings or get_settings()
    settings.ensure_directories()

    listed_security_files = _resolve_paths(
        listed_security_paths,
        "*.csv",
        base_dir=settings.listed_security_seed_dir,
    )
    fundamentals_files = _resolve_paths(
        fundamentals_paths,
        "*.csv",
        base_dir=settings.normalized_fundamentals_dir,
    )
    benchmark_files = _resolve_paths(
        benchmark_definition_paths,
        "*_benchmark_definitions_*.csv",
        base_dir=settings.raw_benchmarks_dir,
    )
    override_files = _resolve_paths(
        override_paths,
        "security_master_overrides*.csv",
        base_dir=settings.reference_dir,
    )

    if listed_security_files:
        listed_security_seed = pd.concat([pd.read_csv(path) for path in listed_security_files], ignore_index=True)
    else:
        listed_security_seed = load_listed_security_seed(settings=settings)

    if fundamentals_files:
        fundamentals = pd.concat([pd.read_csv(path) for path in fundamentals_files], ignore_index=True)
    else:
        fundamentals = read_canonical_table("normalized_fundamentals", settings=settings)

    benchmark_definitions = (
        pd.concat([pd.read_csv(path) for path in benchmark_files], ignore_index=True)
        if benchmark_files
        else pd.DataFrame(columns=["benchmark_id", "ticker", "name", "category", "scope", "currency", "inception_date", "description"])
    )
    overrides = (
        pd.concat([pd.read_csv(path) for path in override_files], ignore_index=True)
        if override_files
        else load_security_master_overrides(settings=settings)
    )

    security_master = build_security_master_frame(
        listed_security_seed,
        fundamentals,
        benchmark_definitions,
        overrides=overrides,
    )
    validate_security_master_frame(security_master)

    if write_warehouse:
        write_canonical_table(security_master, "security_master", settings=settings)

    if write_csv and not security_master.empty:
        if output_name == "security_master.csv":
            export_table_csv(security_master, "security_master", settings=settings)
        else:
            output_path = settings.security_master_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            security_master.to_csv(output_path, index=False)

    return security_master
