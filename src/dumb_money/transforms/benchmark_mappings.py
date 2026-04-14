"""Build shared benchmark assignment mappings for maintained securities."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings, get_settings
from dumb_money.storage import (
    BENCHMARK_MAPPING_COLUMNS,
    export_table_csv,
    read_canonical_table,
    write_canonical_table,
)
from dumb_money.transforms.benchmark_memberships import (
    normalize_benchmark_mapping_frame,
    stage_benchmark_definition_refresh,
    stage_benchmark_memberships,
)

DEFAULT_PRIMARY_BENCHMARK = "SPY"
STYLE_PROXY_BENCHMARK = "QQQ"
SECTOR_LABEL_ALIASES = {
    "healthcare": "health_care",
}
INDUSTRY_LABEL_ALIASES = {
    "drug_manufacturers_general": "pharmaceuticals",
    "medical_instruments_supplies": "medical_devices",
}


def _normalize_label(value: object, *, aliases: dict[str, str] | None = None) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text or text == "none":
        return None

    normalized = "".join(character if character.isalnum() else "_" for character in text)
    normalized = "_".join(part for part in normalized.split("_") if part)
    if not normalized:
        return None
    if aliases:
        normalized = aliases.get(normalized, normalized)
    return normalized


def _resolve_mapping_path(
    mapping_path: str | Path | None,
    *,
    settings: AppSettings,
) -> Path:
    if mapping_path is not None:
        return Path(mapping_path)
    return settings.raw_benchmark_holdings_dir / "etf_benchmark_mapping.csv"


def _membership_sets(benchmark_memberships: pd.DataFrame) -> dict[str, set[str]]:
    if benchmark_memberships.empty:
        return {}
    memberships = benchmark_memberships.copy()
    memberships["benchmark_ticker"] = memberships["benchmark_ticker"].astype(str).str.strip().str.upper()
    memberships["member_ticker"] = memberships["member_ticker"].astype(str).str.strip().str.upper()
    return {
        benchmark_ticker: set(frame["member_ticker"].tolist())
        for benchmark_ticker, frame in memberships.groupby("benchmark_ticker", sort=True)
    }


def _first_lookup(frame: pd.DataFrame, key_column: str) -> dict[str, str]:
    if frame.empty:
        return {}
    normalized = frame.copy()
    aliases = SECTOR_LABEL_ALIASES if key_column == "sector" else INDUSTRY_LABEL_ALIASES
    normalized[key_column] = normalized[key_column].map(
        lambda value: _normalize_label(value, aliases=aliases)
    )
    normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
    normalized = normalized.loc[normalized[key_column].notna()].sort_values([key_column, "ticker"])
    return (
        normalized.drop_duplicates(subset=[key_column], keep="first")
        .set_index(key_column)["ticker"]
        .to_dict()
    )


def _resolve_primary_benchmark(
    ticker: str,
    memberships: dict[str, set[str]],
    market_benchmarks: set[str],
) -> tuple[str | None, str]:
    ticker = ticker.strip().upper()
    if "IWM" in market_benchmarks and ticker in memberships.get("IWM", set()) and ticker not in memberships.get("SPY", set()):
        return "IWM", "market_membership_small_cap"
    for candidate in ["SPY", "QQQ", "DIA"]:
        if candidate in market_benchmarks and ticker in memberships.get(candidate, set()):
            return candidate, "market_membership"
    if DEFAULT_PRIMARY_BENCHMARK in market_benchmarks:
        return DEFAULT_PRIMARY_BENCHMARK, "default_primary_fallback"
    if market_benchmarks:
        return sorted(market_benchmarks)[0], "default_primary_fallback"
    return None, "no_market_benchmark_available"


def _resolve_style_benchmark(ticker: str, memberships: dict[str, set[str]]) -> tuple[str | None, str | None]:
    ticker = ticker.strip().upper()
    if ticker in memberships.get(STYLE_PROXY_BENCHMARK, set()):
        return STYLE_PROXY_BENCHMARK, "style_membership"
    return None, None


def build_benchmark_mappings_frame(
    security_master: pd.DataFrame,
    benchmark_definitions: pd.DataFrame,
    benchmark_memberships: pd.DataFrame,
    benchmark_mapping_reference: pd.DataFrame,
) -> pd.DataFrame:
    """Build canonical benchmark assignments for maintained securities."""

    if security_master.empty:
        return pd.DataFrame(columns=BENCHMARK_MAPPING_COLUMNS)

    securities = security_master.copy()
    securities["ticker"] = securities["ticker"].astype(str).str.strip().str.upper()
    securities["sector"] = securities["sector"].where(securities["sector"].notna(), None)
    securities["industry"] = securities["industry"].where(securities["industry"].notna(), None)

    if "is_eligible_research_universe" in securities.columns:
        securities = securities.loc[securities["is_eligible_research_universe"].fillna(False)].copy()
    if "is_benchmark" in securities.columns:
        securities = securities.loc[~securities["is_benchmark"].fillna(False)].copy()

    if securities.empty:
        return pd.DataFrame(columns=BENCHMARK_MAPPING_COLUMNS)

    definitions = benchmark_definitions.copy()
    if definitions.empty:
        definitions = pd.DataFrame(columns=["ticker", "category"])
    definitions["ticker"] = definitions["ticker"].astype(str).str.strip().str.upper()
    definitions["category"] = definitions["category"].astype(str).str.strip().str.lower()

    reference = normalize_benchmark_mapping_frame(benchmark_mapping_reference)
    memberships = _membership_sets(benchmark_memberships)
    market_benchmarks = set(
        definitions.loc[definitions["category"] == "market", "ticker"].astype(str).str.strip().str.upper().tolist()
    )
    sector_lookup = _first_lookup(reference.loc[reference["sector"].notna(), ["ticker", "sector"]], "sector")
    industry_lookup = _first_lookup(reference.loc[reference["industry"].notna(), ["ticker", "industry"]], "industry")

    rows: list[dict[str, object]] = []
    for record in securities.sort_values("ticker").to_dict(orient="records"):
        ticker = str(record["ticker"]).strip().upper()
        sector = record.get("sector")
        industry = record.get("industry")
        primary_benchmark, primary_method = _resolve_primary_benchmark(ticker, memberships, market_benchmarks)
        style_benchmark, style_method = _resolve_style_benchmark(ticker, memberships)
        sector_benchmark = (
            sector_lookup.get(_normalize_label(sector, aliases=SECTOR_LABEL_ALIASES))
            if sector
            else None
        )
        industry_benchmark = (
            industry_lookup.get(_normalize_label(industry, aliases=INDUSTRY_LABEL_ALIASES))
            if industry
            else None
        )

        method_parts = [f"primary:{primary_method}"]
        if sector_benchmark:
            method_parts.append("sector:reference_mapping")
        if industry_benchmark:
            method_parts.append("industry:reference_mapping")
        if style_method:
            method_parts.append(f"style:{style_method}")

        notes: list[str] = []
        if primary_method == "default_primary_fallback":
            notes.append("defaulted primary benchmark because no explicit market membership matched")
        if sector and sector_benchmark is None:
            notes.append(f"no sector benchmark mapping found for sector {sector}")
        if industry and industry_benchmark is None:
            notes.append(f"no industry benchmark mapping found for industry {industry}")

        rows.append(
            {
                "mapping_id": f"benchmark_mapping::{ticker}",
                "ticker": ticker,
                "sector": sector,
                "industry": industry,
                "primary_benchmark": primary_benchmark,
                "sector_benchmark": sector_benchmark,
                "industry_benchmark": industry_benchmark,
                "style_benchmark": style_benchmark,
                "custom_benchmark": None,
                "assignment_method": ";".join(method_parts),
                "priority": 1,
                "is_active": True,
                "notes": " | ".join(notes) if notes else None,
            }
        )

    return pd.DataFrame(rows, columns=BENCHMARK_MAPPING_COLUMNS).sort_values("ticker").reset_index(drop=True)


def stage_benchmark_mappings(
    *,
    mapping_path: str | Path | None = None,
    settings: AppSettings | None = None,
    output_name: str = "benchmark_mappings.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Build canonical benchmark assignment mappings for maintained securities."""

    settings = settings or get_settings()
    reference = pd.read_csv(_resolve_mapping_path(mapping_path, settings=settings))

    definitions = read_canonical_table("benchmark_definitions", settings=settings)
    if definitions.empty:
        definitions = stage_benchmark_definition_refresh(
            mapping_path=mapping_path,
            settings=settings,
            write_warehouse=write_warehouse,
            write_csv=write_csv,
        )

    memberships = read_canonical_table("benchmark_memberships", settings=settings)
    if memberships.empty:
        memberships = stage_benchmark_memberships(
            mapping_path=mapping_path,
            settings=settings,
            write_warehouse=write_warehouse,
            write_csv=write_csv,
        )

    security_master = read_canonical_table("security_master", settings=settings)
    mappings = build_benchmark_mappings_frame(
        security_master,
        definitions,
        memberships,
        reference,
    )

    if write_warehouse:
        write_canonical_table(mappings, "benchmark_mappings", settings=settings)

    if write_csv and not mappings.empty:
        if output_name == "benchmark_mappings.csv":
            export_table_csv(mappings, "benchmark_mappings", settings=settings)
        else:
            output_path = settings.benchmark_mappings_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            mappings.to_csv(output_path, index=False)

    return mappings
