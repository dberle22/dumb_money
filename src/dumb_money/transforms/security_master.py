"""Build an initial stitchable security master for downstream joins."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.models import AssetType, Security
from dumb_money.transforms.benchmark_sets import normalize_benchmark_definition_frame
from dumb_money.transforms.fundamentals import normalize_fundamentals_frame

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


def build_security_master_frame(
    fundamentals: pd.DataFrame,
    benchmark_definitions: pd.DataFrame,
) -> pd.DataFrame:
    """Combine latest issuer metadata and benchmark definitions into one table."""

    records: dict[str, Security] = {}

    normalized_fundamentals = normalize_fundamentals_frame(fundamentals)
    if not normalized_fundamentals.empty:
        latest = normalized_fundamentals.sort_values(["ticker", "as_of_date"]).groupby("ticker", as_index=False).tail(1)
        for row in latest.to_dict(orient="records"):
            security = Security(
                ticker=row["ticker"],
                name=row.get("long_name"),
                asset_type=AssetType.COMMON_STOCK,
                currency=row.get("currency") or "USD",
                sector=row.get("sector"),
                industry=row.get("industry"),
                is_benchmark=False,
            )
            records[security.ticker] = security

    normalized_benchmarks = normalize_benchmark_definition_frame(benchmark_definitions)
    for row in normalized_benchmarks.to_dict(orient="records"):
        existing = records.get(row["ticker"])
        asset_type = _benchmark_asset_type(pd.Series(row))
        if existing is None:
            records[row["ticker"]] = Security(
                ticker=row["ticker"],
                name=row.get("name"),
                asset_type=asset_type,
                currency=row.get("currency") or "USD",
                is_benchmark=True,
            )
            continue

        records[row["ticker"]] = existing.model_copy(
            update={
                "name": existing.name or row.get("name"),
                "currency": existing.currency or row.get("currency") or "USD",
                "is_benchmark": True,
                "asset_type": asset_type if existing.asset_type == AssetType.OTHER else existing.asset_type,
            }
        )

    return pd.DataFrame(
        [record.model_dump(mode="json") for record in records.values()],
        columns=SECURITY_COLUMNS,
    ).sort_values(["ticker"]).reset_index(drop=True) if records else pd.DataFrame(columns=SECURITY_COLUMNS)


def stage_security_master(
    *,
    fundamentals_paths: Sequence[str | Path] | None = None,
    benchmark_definition_paths: Sequence[str | Path] | None = None,
    settings: AppSettings | None = None,
    output_name: str = "security_master.csv",
    write_csv: bool = True,
) -> pd.DataFrame:
    settings = settings or get_settings()
    settings.ensure_directories()

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

    fundamentals = (
        pd.concat([pd.read_csv(path) for path in fundamentals_files], ignore_index=True)
        if fundamentals_files
        else pd.DataFrame(columns=list(Security.model_fields.keys()))
    )
    benchmark_definitions = (
        pd.concat([pd.read_csv(path) for path in benchmark_files], ignore_index=True)
        if benchmark_files
        else pd.DataFrame(columns=["benchmark_id", "ticker", "name", "category", "scope", "currency", "inception_date", "description"])
    )

    security_master = build_security_master_frame(fundamentals, benchmark_definitions)

    if write_csv and not security_master.empty:
        output_path = settings.security_master_dir / output_name
        output_path.parent.mkdir(parents=True, exist_ok=True)
        security_master.to_csv(output_path, index=False)

    return security_master
