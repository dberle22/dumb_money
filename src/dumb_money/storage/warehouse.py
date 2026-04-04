"""DuckDB-backed storage helpers for canonical analytical tables."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from pathlib import Path

import duckdb
import pandas as pd

from dumb_money.config import AppSettings, get_settings
from dumb_money.ingestion.benchmarks import BENCHMARK_COLUMNS
from dumb_money.ingestion.fundamentals import SNAPSHOT_COLUMNS
from dumb_money.ingestion.prices import PRICE_COLUMNS
from dumb_money.models import Security

BENCHMARK_SET_COLUMNS = [
    "set_id",
    "benchmark_id",
    "ticker",
    "name",
    "category",
    "scope",
    "currency",
    "description",
    "member_order",
    "is_default",
]

BENCHMARK_MAPPING_COLUMNS = [
    "mapping_id",
    "ticker",
    "sector",
    "industry",
    "primary_benchmark",
    "sector_benchmark",
    "industry_benchmark",
    "style_benchmark",
    "custom_benchmark",
    "assignment_method",
    "priority",
    "is_active",
    "notes",
]

PEER_SET_COLUMNS = [
    "peer_set_id",
    "ticker",
    "peer_ticker",
    "relationship_type",
    "sector",
    "industry",
    "selection_method",
    "peer_order",
]

SECTOR_SNAPSHOT_COLUMNS = [
    "sector",
    "sector_benchmark",
    "company_count",
    "companies_with_fundamentals",
    "companies_with_prices",
    "median_market_cap",
    "median_forward_pe",
    "median_ev_to_ebitda",
    "median_price_to_sales",
    "median_free_cash_flow_yield",
    "median_operating_margin",
    "median_gross_margin",
    "median_return_6m",
    "median_return_1y",
]

SECURITY_INGESTION_STATUS_COLUMNS = [
    "ticker",
    "security_id",
    "name",
    "asset_type",
    "is_eligible_research_universe",
    "benchmark_membership_count",
    "first_price_date",
    "latest_price_date",
    "price_record_count",
    "has_price_history",
    "first_fundamental_period_end_date",
    "latest_fundamental_period_end_date",
    "fundamentals_record_count",
    "has_historical_fundamentals",
    "has_any_ingestion",
    "is_fully_ingested",
    "updated_at",
]

BENCHMARK_MEMBERSHIP_COLUMNS = [
    "benchmark_id",
    "benchmark_ticker",
    "member_ticker",
    "member_name",
    "member_weight",
    "member_sector",
    "asset_class",
    "exchange",
    "currency",
    "as_of_date",
    "source",
    "source_file",
]

BENCHMARK_MEMBERSHIP_COVERAGE_COLUMNS = [
    "benchmark_id",
    "benchmark_ticker",
    "benchmark_name",
    "benchmark_category",
    "benchmark_scope",
    "mapped_sector",
    "mapped_industry",
    "member_ticker",
    "member_name",
    "member_weight",
    "member_sector",
    "member_exchange",
    "is_in_security_master",
    "security_id",
    "security_name",
    "security_asset_type",
    "security_exchange",
    "is_eligible_research_universe",
]

LISTED_SECURITY_SEED_COLUMNS = [
    "ticker",
    "name",
    "exchange",
    "listing_market",
    "asset_type_raw",
    "is_etf",
    "is_test_issue",
    "is_active",
    "round_lot_size",
    "is_eligible_research_universe",
    "eligibility_reason",
    "source",
    "source_file",
    "as_of_date",
]

SECURITY_MASTER_OVERRIDE_COLUMNS = [
    "ticker",
    "field_name",
    "override_value",
    "reason",
    "updated_at",
]

SECURITY_COLUMNS = list(Security.model_fields.keys())

LEGACY_CSV_DEFAULTS: dict[str, dict[str, object]] = {
    "normalized_fundamentals": {
        "period_end_date": None,
        "report_date": None,
        "fiscal_year": None,
        "fiscal_quarter": None,
        "fiscal_period": "TTM",
        "period_type": "ttm",
        "revenue": None,
        "current_assets": None,
        "current_liabilities": None,
    },
    "security_master": {
        "security_id": None,
        "primary_listing": None,
        "cik": None,
        "is_active": True,
        "is_eligible_research_universe": False,
        "source": None,
        "source_id": None,
        "first_seen_at": None,
        "last_updated_at": None,
        "notes": None,
    }
}


@dataclass(frozen=True, slots=True)
class WarehouseTableSpec:
    """Warehouse metadata for one canonical analytical table."""

    table_name: str
    columns: tuple[str, ...]
    csv_dir_attr: str
    csv_file_name: str
    description: str

    def csv_path(self, settings: AppSettings) -> Path:
        return getattr(settings, self.csv_dir_attr) / self.csv_file_name


CANONICAL_DUCKDB_TABLES: dict[str, WarehouseTableSpec] = {
    "normalized_prices": WarehouseTableSpec(
        table_name="normalized_prices",
        columns=tuple(PRICE_COLUMNS),
        csv_dir_attr="normalized_prices_dir",
        csv_file_name="normalized_prices.csv",
        description="Canonical normalized daily or interval price observations.",
    ),
    "normalized_fundamentals": WarehouseTableSpec(
        table_name="normalized_fundamentals",
        columns=tuple(SNAPSHOT_COLUMNS),
        csv_dir_attr="normalized_fundamentals_dir",
        csv_file_name="normalized_fundamentals.csv",
        description="Canonical period-aware fundamentals staging table.",
    ),
    "security_master": WarehouseTableSpec(
        table_name="security_master",
        columns=tuple(SECURITY_COLUMNS),
        csv_dir_attr="security_master_dir",
        csv_file_name="security_master.csv",
        description="Canonical shared security metadata table for joins.",
    ),
    "benchmark_definitions": WarehouseTableSpec(
        table_name="benchmark_definitions",
        columns=tuple(BENCHMARK_COLUMNS),
        csv_dir_attr="benchmark_definitions_dir",
        csv_file_name="benchmark_definitions.csv",
        description="Canonical benchmark definition registry.",
    ),
    "benchmark_memberships": WarehouseTableSpec(
        table_name="benchmark_memberships",
        columns=tuple(BENCHMARK_MEMBERSHIP_COLUMNS),
        csv_dir_attr="benchmark_memberships_dir",
        csv_file_name="benchmark_memberships.csv",
        description="Canonical current-snapshot benchmark constituent membership table.",
    ),
    "benchmark_membership_coverage": WarehouseTableSpec(
        table_name="benchmark_membership_coverage",
        columns=tuple(BENCHMARK_MEMBERSHIP_COVERAGE_COLUMNS),
        csv_dir_attr="benchmark_membership_coverage_dir",
        csv_file_name="benchmark_membership_coverage.csv",
        description="Join-ready view of benchmark members against security master coverage.",
    ),
    "listed_security_seed": WarehouseTableSpec(
        table_name="listed_security_seed",
        columns=tuple(LISTED_SECURITY_SEED_COLUMNS),
        csv_dir_attr="listed_security_seed_dir",
        csv_file_name="listed_security_seed.csv",
        description="Normalized listed-security universe seed derived from source directories.",
    ),
    "security_master_overrides": WarehouseTableSpec(
        table_name="security_master_overrides",
        columns=tuple(SECURITY_MASTER_OVERRIDE_COLUMNS),
        csv_dir_attr="reference_dir",
        csv_file_name="security_master_overrides.csv",
        description="Field-level manual overrides for the canonical security master.",
    ),
    "benchmark_sets": WarehouseTableSpec(
        table_name="benchmark_sets",
        columns=tuple(BENCHMARK_SET_COLUMNS),
        csv_dir_attr="benchmark_sets_dir",
        csv_file_name="benchmark_sets.csv",
        description="Canonical reusable benchmark set membership table.",
    ),
    "benchmark_mappings": WarehouseTableSpec(
        table_name="benchmark_mappings",
        columns=tuple(BENCHMARK_MAPPING_COLUMNS),
        csv_dir_attr="benchmark_mappings_dir",
        csv_file_name="benchmark_mappings.csv",
        description="Canonical benchmark assignment table for shared company and sector workflows.",
    ),
    "peer_sets": WarehouseTableSpec(
        table_name="peer_sets",
        columns=tuple(PEER_SET_COLUMNS),
        csv_dir_attr="staging_dir",
        csv_file_name="peer_sets/peer_sets.csv",
        description="Canonical peer-group membership table for shared peer-relative research workflows.",
    ),
    "sector_snapshots": WarehouseTableSpec(
        table_name="sector_snapshots",
        columns=tuple(SECTOR_SNAPSHOT_COLUMNS),
        csv_dir_attr="staging_dir",
        csv_file_name="sector_snapshots/sector_snapshots.csv",
        description="Canonical per-sector summary table for reusable sector context in company research and reporting.",
    ),
    "security_ingestion_status": WarehouseTableSpec(
        table_name="security_ingestion_status",
        columns=tuple(SECURITY_INGESTION_STATUS_COLUMNS),
        csv_dir_attr="security_ingestion_status_dir",
        csv_file_name="security_ingestion_status.csv",
        description="Coverage control table showing which securities already have staged prices and fundamentals.",
    ),
}

CANONICAL_INCREMENTAL_KEYS: dict[str, tuple[str, ...]] = {
    "normalized_prices": ("ticker", "date", "interval", "source"),
    "normalized_fundamentals": ("ticker", "period_end_date", "period_type", "as_of_date"),
}


def get_table_spec(table_name: str) -> WarehouseTableSpec:
    """Return the canonical table spec for a known warehouse table."""

    try:
        return CANONICAL_DUCKDB_TABLES[table_name]
    except KeyError as exc:
        known = ", ".join(sorted(CANONICAL_DUCKDB_TABLES))
        raise KeyError(f"unknown canonical warehouse table {table_name!r}; expected one of: {known}") from exc


def _empty_frame(spec: WarehouseTableSpec) -> pd.DataFrame:
    return pd.DataFrame(columns=list(spec.columns))


def _validate_frame_columns(frame: pd.DataFrame, spec: WarehouseTableSpec) -> pd.DataFrame:
    actual = list(frame.columns)
    expected = list(spec.columns)
    missing = [column for column in expected if column not in actual]
    extra = [column for column in actual if column not in expected]
    if missing or extra:
        raise ValueError(
            f"{spec.table_name} frame does not match canonical schema; missing={missing}, extra={extra}"
        )
    return frame[expected].copy()


def _coerce_csv_fallback_frame(frame: pd.DataFrame, spec: WarehouseTableSpec) -> pd.DataFrame:
    actual = list(frame.columns)
    expected = list(spec.columns)
    defaults = LEGACY_CSV_DEFAULTS.get(spec.table_name, {})
    missing = [column for column in expected if column not in actual]
    extra = [column for column in actual if column not in expected]
    unsupported_missing = [column for column in missing if column not in defaults]
    if extra or unsupported_missing:
        raise ValueError(
            f"{spec.table_name} frame does not match canonical schema; missing={unsupported_missing}, extra={extra}"
        )

    normalized = frame.copy()
    for column in expected:
        if column in normalized.columns:
            continue
        normalized[column] = defaults.get(column)

    return normalized[expected]


def _connect(settings: AppSettings, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    if not read_only:
        settings.ensure_directories()
    return duckdb.connect(str(settings.warehouse_path), read_only=read_only)


def warehouse_table_exists(
    table_name: str,
    *,
    settings: AppSettings | None = None,
) -> bool:
    """Return whether the requested canonical table exists in the warehouse."""

    settings = settings or get_settings()
    if not settings.warehouse_path.exists():
        return False

    spec = get_table_spec(table_name)
    with _connect(settings, read_only=True) as connection:
        rows = connection.execute(
            "select 1 from information_schema.tables where table_name = ? limit 1",
            [spec.table_name],
        ).fetchone()
    return rows is not None


def write_canonical_table(
    frame: pd.DataFrame,
    table_name: str,
    *,
    settings: AppSettings | None = None,
) -> pd.DataFrame:
    """Replace a canonical warehouse table with validated frame contents."""

    settings = settings or get_settings()
    spec = get_table_spec(table_name)
    try:
        validated = _validate_frame_columns(frame, spec)
    except ValueError:
        validated = _coerce_csv_fallback_frame(frame, spec)

    with _connect(settings) as connection:
        connection.register("_canonical_frame", validated)
        connection.execute(
            f'create or replace table "{spec.table_name}" as select * from _canonical_frame'
        )
        connection.unregister("_canonical_frame")

    return validated


def upsert_canonical_table(
    frame: pd.DataFrame,
    table_name: str,
    *,
    settings: AppSettings | None = None,
    key_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Merge new rows into a canonical table, replacing existing rows on natural keys."""

    settings = settings or get_settings()
    spec = get_table_spec(table_name)
    try:
        validated = _validate_frame_columns(frame, spec)
    except ValueError:
        validated = _coerce_csv_fallback_frame(frame, spec)

    resolved_key_columns = tuple(key_columns or CANONICAL_INCREMENTAL_KEYS.get(table_name, ()))
    if not resolved_key_columns:
        raise ValueError(f"no incremental key columns configured for canonical table {table_name!r}")

    existing = read_canonical_table(
        table_name,
        settings=settings,
        prefer_duckdb=True,
        allow_csv_fallback=True,
    )
    if existing.empty:
        return write_canonical_table(validated, table_name, settings=settings)

    merged = (
        pd.concat([existing, validated], ignore_index=True)
        .drop_duplicates(subset=list(resolved_key_columns), keep="last")
        .reset_index(drop=True)
    )
    if all(column in merged.columns for column in resolved_key_columns):
        merged = merged.sort_values(list(resolved_key_columns)).reset_index(drop=True)

    return write_canonical_table(merged, table_name, settings=settings)


def export_table_csv(
    frame: pd.DataFrame,
    table_name: str,
    *,
    settings: AppSettings | None = None,
) -> Path:
    """Persist a canonical table frame to its conventional CSV export path."""

    settings = settings or get_settings()
    spec = get_table_spec(table_name)
    validated = _validate_frame_columns(frame, spec)
    output_path = spec.csv_path(settings)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    validated.to_csv(output_path, index=False)
    return output_path


def read_canonical_table(
    table_name: str,
    *,
    settings: AppSettings | None = None,
    prefer_duckdb: bool = True,
    allow_csv_fallback: bool = True,
) -> pd.DataFrame:
    """Load a canonical table from DuckDB first, then CSV if available."""

    settings = settings or get_settings()
    spec = get_table_spec(table_name)

    if prefer_duckdb and warehouse_table_exists(table_name, settings=settings):
        with _connect(settings, read_only=True) as connection:
            return connection.execute(f'select * from "{spec.table_name}"').df()

    if allow_csv_fallback:
        csv_path = spec.csv_path(settings)
        if csv_path.exists():
            frame = pd.read_csv(csv_path)
            return _coerce_csv_fallback_frame(frame, spec)

    return _empty_frame(spec)


def query_canonical_data(
    sql: str,
    *,
    settings: AppSettings | None = None,
    parameters: Sequence[object] | None = None,
) -> pd.DataFrame:
    """Run a read-only DuckDB query against the project warehouse."""

    settings = settings or get_settings()
    if not settings.warehouse_path.exists():
        raise FileNotFoundError(f"warehouse not found at {settings.warehouse_path}")

    with _connect(settings, read_only=True) as connection:
        if parameters is None:
            return connection.execute(sql).df()
        return connection.execute(sql, list(parameters)).df()
