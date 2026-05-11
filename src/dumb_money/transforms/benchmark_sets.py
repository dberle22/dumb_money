"""Build reusable benchmark set definitions for staging."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.ingestion.benchmarks import BENCHMARK_COLUMNS
from dumb_money.storage import BENCHMARK_SET_COLUMNS, export_table_csv, write_canonical_table


def _resolve_benchmark_definition_paths(
    input_paths: Sequence[str | Path] | None,
    *,
    settings: AppSettings,
) -> list[Path]:
    if input_paths:
        return [Path(path) for path in input_paths]
    return sorted(settings.raw_benchmarks_dir.glob("*_benchmark_definitions_*.csv"))


def normalize_benchmark_definition_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=BENCHMARK_COLUMNS)

    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    missing = [column for column in BENCHMARK_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"benchmark definition frame is missing required columns: {missing}")

    normalized["benchmark_id"] = normalized["benchmark_id"].astype(str).str.strip().str.upper()
    normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
    normalized["currency"] = normalized["currency"].astype(str).str.strip().str.upper()
    normalized = normalized.drop_duplicates(subset=["benchmark_id"], keep="last")
    normalized = normalized.sort_values(["benchmark_id"]).reset_index(drop=True)
    return normalized[BENCHMARK_COLUMNS]


def build_benchmark_sets_frame(
    definitions: pd.DataFrame,
    *,
    set_id: str = "default_benchmarks",
    is_default: bool = True,
) -> pd.DataFrame:
    """Expand benchmark definitions into a reusable benchmark set table."""

    normalized = normalize_benchmark_definition_frame(definitions)
    if normalized.empty:
        return pd.DataFrame(columns=BENCHMARK_SET_COLUMNS)

    benchmark_sets = normalized[
        ["benchmark_id", "ticker", "name", "category", "scope", "currency", "description"]
    ].copy()
    benchmark_sets.insert(0, "set_id", set_id.strip().lower().replace(" ", "_"))
    benchmark_sets["member_order"] = range(1, len(benchmark_sets) + 1)
    benchmark_sets["is_default"] = is_default
    return benchmark_sets[BENCHMARK_SET_COLUMNS]


def stage_benchmark_sets(
    *,
    input_paths: Sequence[str | Path] | None = None,
    settings: AppSettings | None = None,
    set_id: str = "default_benchmarks",
    output_name: str = "benchmark_sets.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    settings = settings or get_settings()
    settings.ensure_directories()

    paths = _resolve_benchmark_definition_paths(input_paths, settings=settings)
    if not paths:
        return pd.DataFrame(columns=BENCHMARK_SET_COLUMNS)

    frames = [pd.read_csv(path) for path in paths]
    benchmark_sets = build_benchmark_sets_frame(pd.concat(frames, ignore_index=True), set_id=set_id)

    if write_warehouse:
        write_canonical_table(benchmark_sets, "benchmark_sets", settings=settings)

    if write_csv and not benchmark_sets.empty:
        if output_name == "benchmark_sets.csv":
            export_table_csv(benchmark_sets, "benchmark_sets", settings=settings)
        else:
            output_path = settings.benchmark_sets_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            benchmark_sets.to_csv(output_path, index=False)

    return benchmark_sets
