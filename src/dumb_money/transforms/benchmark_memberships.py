"""Normalize benchmark definitions and constituent memberships from holdings files."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings, get_settings
from dumb_money.ingestion.benchmarks import BENCHMARK_COLUMNS
from dumb_money.models import BenchmarkCategory
from dumb_money.storage import (
    BENCHMARK_MEMBERSHIP_COLUMNS,
    BENCHMARK_MEMBERSHIP_COVERAGE_COLUMNS,
    export_table_csv,
    read_canonical_table,
    write_canonical_table,
)
from dumb_money.transforms.benchmark_sets import normalize_benchmark_definition_frame

BENCHMARK_HOLDINGS_MAPPING_COLUMNS = ["ticker", "name", "path", "benchmark", "sector", "industry"]
CUSTOM_BENCHMARK_MEMBERSHIP_INPUT_COLUMNS = [
    "benchmark_id",
    "benchmark_ticker",
    "benchmark_name",
    "benchmark_category",
    "benchmark_scope",
    "benchmark_description",
    "member_ticker",
    "member_name",
    "member_weight",
    "member_sector",
    "asset_class",
    "exchange",
    "currency",
    "as_of_date",
]
NON_SECURITY_MEMBER_TICKERS = {"", "-", "NAN", "NONE", "CASH", "USD"}
FUTURES_LIKE_TICKER_PATTERN = r"[A-Z]{2,5}[FGHJKMNQUVXZ]\d{1,2}"
FUTURES_LIKE_NAME_PATTERN = r"\b(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}\b"


def _resolve_mapping_path(
    mapping_path: str | Path | None,
    *,
    settings: AppSettings,
) -> Path:
    if mapping_path is not None:
        return Path(mapping_path)
    return settings.raw_benchmark_holdings_dir / "etf_benchmark_mapping.csv"


def _resolve_custom_membership_path(
    custom_membership_path: str | Path | None,
    *,
    settings: AppSettings,
) -> Path:
    if custom_membership_path is not None:
        return Path(custom_membership_path)
    return settings.reference_dir / "custom_benchmark_memberships.csv"


def normalize_benchmark_mapping_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize the benchmark holdings mapping file."""

    if frame.empty:
        return pd.DataFrame(columns=BENCHMARK_HOLDINGS_MAPPING_COLUMNS)

    normalized = frame.copy()
    missing = [column for column in BENCHMARK_HOLDINGS_MAPPING_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"benchmark mapping frame is missing required columns: {missing}")

    normalized["ticker"] = normalized["ticker"].astype(str).str.strip().str.upper()
    normalized["name"] = normalized["name"].astype(str).str.strip()
    normalized["path"] = normalized["path"].astype(str).str.strip()
    for column in ["benchmark", "sector", "industry"]:
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
        normalized.loc[normalized[column] == "", column] = None

    return (
        normalized[BENCHMARK_HOLDINGS_MAPPING_COLUMNS]
        .drop_duplicates(subset=["ticker"], keep="last")
        .sort_values(["ticker"])
        .reset_index(drop=True)
    )


def normalize_custom_benchmark_memberships_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize custom benchmark basket membership input rows."""

    if frame.empty:
        return pd.DataFrame(columns=CUSTOM_BENCHMARK_MEMBERSHIP_INPUT_COLUMNS)

    normalized = frame.copy()
    missing = [column for column in CUSTOM_BENCHMARK_MEMBERSHIP_INPUT_COLUMNS if column not in normalized.columns]
    if missing:
        raise ValueError(f"custom benchmark memberships frame is missing required columns: {missing}")

    uppercase_columns = ["benchmark_id", "benchmark_ticker", "member_ticker", "currency"]
    text_columns = [
        "benchmark_name",
        "benchmark_category",
        "benchmark_scope",
        "benchmark_description",
        "member_name",
        "member_sector",
        "asset_class",
        "exchange",
        "as_of_date",
    ]
    for column in uppercase_columns:
        normalized[column] = normalized[column].astype(str).str.strip().str.upper()
    for column in text_columns:
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
        normalized.loc[normalized[column] == "", column] = None

    normalized["member_weight"] = pd.to_numeric(normalized["member_weight"], errors="coerce")
    normalized["asset_class"] = normalized["asset_class"].fillna("Custom").astype(str).str.strip()

    return normalized[CUSTOM_BENCHMARK_MEMBERSHIP_INPUT_COLUMNS].drop_duplicates().reset_index(drop=True)


def filter_real_security_members(frame: pd.DataFrame) -> pd.DataFrame:
    """Remove cash rows and footer artifacts from benchmark membership data."""

    if frame.empty:
        return pd.DataFrame(columns=BENCHMARK_MEMBERSHIP_COLUMNS)

    filtered = frame.copy()
    filtered["member_ticker"] = filtered["member_ticker"].astype(str).str.strip().str.upper()
    filtered["member_name"] = filtered["member_name"].fillna("").astype(str).str.strip()
    filtered["asset_class"] = filtered["asset_class"].fillna("").astype(str).str.strip()
    filtered = filtered.loc[filtered["asset_class"].str.lower().eq("equity")].copy()
    filtered = filtered.loc[~filtered["member_ticker"].isin(NON_SECURITY_MEMBER_TICKERS)].copy()
    filtered = filtered.loc[
        filtered["member_ticker"].str.fullmatch(r"[A-Z][A-Z0-9.\-]*", na=False)
    ].copy()
    filtered = filtered.loc[
        ~filtered["member_ticker"].str.fullmatch(FUTURES_LIKE_TICKER_PATTERN, na=False)
    ].copy()
    filtered = filtered.loc[
        ~filtered["member_name"].str.contains(FUTURES_LIKE_NAME_PATTERN, case=False, na=False, regex=True)
    ].copy()
    return filtered.reset_index(drop=True)


def get_real_benchmark_member_tickers(
    frame: pd.DataFrame,
    *,
    benchmark_ticker: str,
) -> list[str]:
    """Return clean security tickers for one benchmark membership set."""

    if frame.empty:
        return []

    benchmark_rows = frame.loc[
        frame["benchmark_ticker"].astype(str).str.strip().str.upper()
        == benchmark_ticker.strip().upper()
    ].copy()
    if benchmark_rows.empty:
        return []

    return filter_real_security_members(benchmark_rows)["member_ticker"].drop_duplicates().tolist()


def _definition_category(row: pd.Series) -> BenchmarkCategory:
    if row.get("benchmark"):
        return BenchmarkCategory.MARKET
    if row.get("sector"):
        return BenchmarkCategory.SECTOR
    if row.get("industry"):
        return BenchmarkCategory.INDUSTRY
    return BenchmarkCategory.CUSTOM


def _definition_scope(row: pd.Series) -> str | None:
    if row.get("benchmark"):
        label = str(row["benchmark"]).strip().lower()
        mapping = {
            "s&p 500": "us_large_cap",
            "dow jones industrial average": "us_large_cap_blue_chip",
            "russell 2000": "us_small_cap",
        }
        return mapping.get(label, label.replace(" ", "_"))
    if row.get("sector"):
        return str(row["sector"]).strip().lower().replace(" ", "_")
    if row.get("industry"):
        return str(row["industry"]).strip().lower().replace(" ", "_")
    return None


def build_benchmark_definitions_from_mapping(mapping: pd.DataFrame) -> pd.DataFrame:
    """Build canonical benchmark definitions from the holdings mapping file."""

    normalized = normalize_benchmark_mapping_frame(mapping)
    if normalized.empty:
        return pd.DataFrame(columns=BENCHMARK_COLUMNS)

    definitions = pd.DataFrame(
        {
            "benchmark_id": normalized["ticker"],
            "ticker": normalized["ticker"],
            "name": normalized["name"],
            "category": normalized.apply(_definition_category, axis=1).astype(str),
            "scope": normalized.apply(_definition_scope, axis=1),
            "currency": "USD",
            "inception_date": None,
            "description": normalized.apply(
                lambda row: row["benchmark"] or row["sector"] or row["industry"],
                axis=1,
            ),
        }
    )
    return normalize_benchmark_definition_frame(definitions)


def build_custom_benchmark_definitions(custom_memberships: pd.DataFrame) -> pd.DataFrame:
    """Build canonical custom benchmark definitions from custom membership rows."""

    normalized = normalize_custom_benchmark_memberships_frame(custom_memberships)
    if normalized.empty:
        return pd.DataFrame(columns=BENCHMARK_COLUMNS)

    definitions = (
        normalized[
            [
                "benchmark_id",
                "benchmark_ticker",
                "benchmark_name",
                "benchmark_category",
                "benchmark_scope",
                "benchmark_description",
            ]
        ]
        .drop_duplicates(subset=["benchmark_id"], keep="first")
        .rename(
            columns={
                "benchmark_ticker": "ticker",
                "benchmark_name": "name",
                "benchmark_category": "category",
                "benchmark_scope": "scope",
                "benchmark_description": "description",
            }
        )
    )
    definitions["category"] = definitions["category"].fillna("custom").astype(str).str.strip().str.lower()
    definitions["currency"] = "USD"
    definitions["inception_date"] = None
    return normalize_benchmark_definition_frame(definitions[BENCHMARK_COLUMNS])


def _parse_spdr_holdings(path: Path) -> tuple[pd.DataFrame, str]:
    header_frame = pd.read_excel(path, sheet_name="holdings", nrows=3, header=None)
    holdings = pd.read_excel(path, sheet_name="holdings", skiprows=4)
    holdings = holdings.dropna(how="all")
    as_of_value = str(header_frame.iloc[2, 1]) if header_frame.shape[0] > 2 and header_frame.shape[1] > 1 else ""
    as_of_date = as_of_value.replace("As of", "").strip()
    frame = pd.DataFrame(
        {
            "member_ticker": holdings["Ticker"].astype(str).str.strip().str.upper(),
            "member_name": holdings["Name"].astype(str).str.strip(),
            "member_weight": pd.to_numeric(holdings["Weight"], errors="coerce"),
            "member_sector": holdings["Sector"].where(holdings["Sector"].notna(), None),
            "asset_class": "Equity",
            "exchange": None,
            "currency": holdings["Local Currency"].fillna("USD").astype(str).str.strip().str.upper(),
        }
    )
    return frame.dropna(subset=["member_ticker"]).reset_index(drop=True), as_of_date


def _parse_russell_holdings(path: Path) -> tuple[pd.DataFrame, str]:
    header_idx = 0
    as_of_date = ""
    with path.open() as handle:
        for idx, line in enumerate(handle):
            if idx == 1:
                parts = [part.strip().strip('"') for part in line.strip().split(",", maxsplit=1)]
                as_of_date = parts[1] if len(parts) > 1 else ""
            if "Ticker" in line and "Name" in line and "Weight (%)" in line:
                header_idx = idx
                break

    holdings = pd.read_csv(path, skiprows=header_idx).dropna(how="all")
    frame = pd.DataFrame(
        {
            "member_ticker": holdings["Ticker"].astype(str).str.strip().str.upper(),
            "member_name": holdings["Name"].astype(str).str.strip(),
            "member_weight": pd.to_numeric(holdings["Weight (%)"], errors="coerce"),
            "member_sector": holdings["Sector"].astype(str).str.strip(),
            "asset_class": holdings["Asset Class"].astype(str).str.strip(),
            "exchange": holdings["Exchange"].astype(str).str.strip(),
            "currency": holdings["Currency"].fillna("USD").astype(str).str.strip().str.upper(),
        }
    )
    return frame.dropna(subset=["member_ticker"]).reset_index(drop=True), as_of_date


def _parse_holdings_file(path: Path) -> tuple[pd.DataFrame, str]:
    if path.suffix.lower() == ".csv":
        return _parse_russell_holdings(path)
    return _parse_spdr_holdings(path)


def build_benchmark_memberships_frame(
    mapping: pd.DataFrame,
    *,
    base_dir: str | Path,
) -> pd.DataFrame:
    """Build canonical current-snapshot benchmark memberships from mapped holdings files."""

    normalized = normalize_benchmark_mapping_frame(mapping)
    base_dir = Path(base_dir)
    frames: list[pd.DataFrame] = []

    for row in normalized.to_dict(orient="records"):
        holdings, as_of_date = _parse_holdings_file(base_dir / str(row["path"]))
        if holdings.empty:
            continue
        holdings.insert(0, "benchmark_ticker", row["ticker"])
        holdings.insert(0, "benchmark_id", row["ticker"])
        holdings["as_of_date"] = as_of_date
        holdings["source"] = "benchmark_holdings_snapshot"
        holdings["source_file"] = str(row["path"])
        frames.append(holdings[BENCHMARK_MEMBERSHIP_COLUMNS])

    if not frames:
        return pd.DataFrame(columns=BENCHMARK_MEMBERSHIP_COLUMNS)

    memberships = filter_real_security_members(pd.concat(frames, ignore_index=True))
    memberships = memberships.drop_duplicates(subset=["benchmark_id", "member_ticker"], keep="first")
    return memberships.sort_values(["benchmark_id", "member_ticker"]).reset_index(drop=True)


def build_custom_benchmark_memberships_frame(custom_memberships: pd.DataFrame) -> pd.DataFrame:
    """Build canonical membership rows for custom benchmark baskets."""

    normalized = normalize_custom_benchmark_memberships_frame(custom_memberships)
    if normalized.empty:
        return pd.DataFrame(columns=BENCHMARK_MEMBERSHIP_COLUMNS)

    memberships = normalized.rename(columns={"benchmark_ticker": "benchmark_ticker"})[
        [
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
        ]
    ].copy()
    memberships["source"] = "custom_benchmark_membership"
    memberships["source_file"] = "custom_benchmark_memberships.csv"
    return memberships[BENCHMARK_MEMBERSHIP_COLUMNS].drop_duplicates(
        subset=["benchmark_id", "member_ticker"], keep="first"
    ).sort_values(["benchmark_id", "member_ticker"]).reset_index(drop=True)


def build_benchmark_membership_coverage_frame(
    benchmark_definitions: pd.DataFrame,
    benchmark_memberships: pd.DataFrame,
    security_master: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Build a join-ready coverage table against security master."""

    definitions = normalize_benchmark_definition_frame(benchmark_definitions)
    memberships = benchmark_memberships.copy()
    normalized_mapping = normalize_benchmark_mapping_frame(mapping)

    if memberships.empty:
        return pd.DataFrame(columns=BENCHMARK_MEMBERSHIP_COVERAGE_COLUMNS)

    coverage = memberships.merge(
        definitions.rename(
            columns={
                "ticker": "definition_ticker",
                "name": "benchmark_name",
                "category": "benchmark_category",
                "scope": "benchmark_scope",
            }
        ),
        on="benchmark_id",
        how="left",
    ).merge(
        normalized_mapping.rename(
            columns={"ticker": "mapping_ticker", "sector": "mapped_sector", "industry": "mapped_industry"}
        )[["mapping_ticker", "mapped_sector", "mapped_industry"]],
        left_on="benchmark_id",
        right_on="mapping_ticker",
        how="left",
    ).merge(
        security_master.rename(
            columns={
                "ticker": "member_ticker_join",
                "name": "security_name",
                "asset_type": "security_asset_type",
                "exchange": "security_exchange",
            }
        ),
        left_on="member_ticker",
        right_on="member_ticker_join",
        how="left",
    )

    coverage["is_in_security_master"] = coverage["security_id"].notna()
    coverage["is_eligible_research_universe"] = coverage["is_eligible_research_universe"].where(
        coverage["is_eligible_research_universe"].notna(),
        False,
    ).astype(bool)

    frame = pd.DataFrame(
        {
            "benchmark_id": coverage["benchmark_id"],
            "benchmark_ticker": coverage["benchmark_ticker"],
            "benchmark_name": coverage["benchmark_name"],
            "benchmark_category": coverage["benchmark_category"],
            "benchmark_scope": coverage["benchmark_scope"],
            "mapped_sector": coverage["mapped_sector"],
            "mapped_industry": coverage["mapped_industry"],
            "member_ticker": coverage["member_ticker"],
            "member_name": coverage["member_name"],
            "member_weight": coverage["member_weight"],
            "member_sector": coverage["member_sector"],
            "member_exchange": coverage["exchange"],
            "is_in_security_master": coverage["is_in_security_master"],
            "security_id": coverage["security_id"],
            "security_name": coverage["security_name"],
            "security_asset_type": coverage["security_asset_type"],
            "security_exchange": coverage["security_exchange"],
            "is_eligible_research_universe": coverage["is_eligible_research_universe"],
        }
    )
    return frame[BENCHMARK_MEMBERSHIP_COVERAGE_COLUMNS].sort_values(["benchmark_id", "member_ticker"]).reset_index(drop=True)


def stage_benchmark_definition_refresh(
    *,
    mapping_path: str | Path | None = None,
    custom_membership_path: str | Path | None = None,
    settings: AppSettings | None = None,
    output_name: str = "benchmark_definitions.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Refresh canonical benchmark definitions from the benchmark holdings mapping file."""

    settings = settings or get_settings()
    mapping = pd.read_csv(_resolve_mapping_path(mapping_path, settings=settings))
    definitions = build_benchmark_definitions_from_mapping(mapping)
    custom_path = _resolve_custom_membership_path(custom_membership_path, settings=settings)
    if custom_path.exists():
        custom_memberships = pd.read_csv(custom_path)
        custom_definitions = build_custom_benchmark_definitions(custom_memberships)
        if not custom_definitions.empty:
            definitions = normalize_benchmark_definition_frame(
                pd.concat([definitions, custom_definitions], ignore_index=True)
            )

    if write_warehouse:
        write_canonical_table(definitions, "benchmark_definitions", settings=settings)

    if write_csv and not definitions.empty:
        if output_name == "benchmark_definitions.csv":
            export_table_csv(definitions, "benchmark_definitions", settings=settings)
        else:
            output_path = settings.benchmark_definitions_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            definitions.to_csv(output_path, index=False)

    return definitions


def stage_benchmark_memberships(
    *,
    mapping_path: str | Path | None = None,
    custom_membership_path: str | Path | None = None,
    settings: AppSettings | None = None,
    output_name: str = "benchmark_memberships.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Stage canonical current-snapshot benchmark memberships from holdings files."""

    settings = settings or get_settings()
    mapping = pd.read_csv(_resolve_mapping_path(mapping_path, settings=settings))
    memberships = build_benchmark_memberships_frame(mapping, base_dir=settings.raw_benchmark_holdings_dir)
    custom_path = _resolve_custom_membership_path(custom_membership_path, settings=settings)
    if custom_path.exists():
        custom_memberships = build_custom_benchmark_memberships_frame(pd.read_csv(custom_path))
        if not custom_memberships.empty:
            memberships = pd.concat([memberships, custom_memberships], ignore_index=True)
            memberships = memberships.drop_duplicates(subset=["benchmark_id", "member_ticker"], keep="first")
            memberships = memberships.sort_values(["benchmark_id", "member_ticker"]).reset_index(drop=True)

    if write_warehouse:
        write_canonical_table(memberships, "benchmark_memberships", settings=settings)

    if write_csv and not memberships.empty:
        if output_name == "benchmark_memberships.csv":
            export_table_csv(memberships, "benchmark_memberships", settings=settings)
        else:
            output_path = settings.benchmark_memberships_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            memberships.to_csv(output_path, index=False)

    return memberships


def stage_benchmark_membership_coverage(
    *,
    mapping_path: str | Path | None = None,
    custom_membership_path: str | Path | None = None,
    settings: AppSettings | None = None,
    output_name: str = "benchmark_membership_coverage.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Build a join-ready benchmark membership coverage table against security master."""

    settings = settings or get_settings()
    mapping = pd.read_csv(_resolve_mapping_path(mapping_path, settings=settings))
    definitions = read_canonical_table("benchmark_definitions", settings=settings)
    if definitions.empty:
        definitions = stage_benchmark_definition_refresh(
            mapping_path=mapping_path,
            custom_membership_path=custom_membership_path,
            settings=settings,
            write_warehouse=write_warehouse,
            write_csv=write_csv,
        )

    memberships = read_canonical_table("benchmark_memberships", settings=settings)
    if memberships.empty:
        memberships = stage_benchmark_memberships(
            mapping_path=mapping_path,
            custom_membership_path=custom_membership_path,
            settings=settings,
            write_warehouse=write_warehouse,
            write_csv=write_csv,
        )

    security_master = read_canonical_table("security_master", settings=settings)
    coverage = build_benchmark_membership_coverage_frame(definitions, memberships, security_master, mapping)

    if write_warehouse:
        write_canonical_table(coverage, "benchmark_membership_coverage", settings=settings)

    if write_csv and not coverage.empty:
        if output_name == "benchmark_membership_coverage.csv":
            export_table_csv(coverage, "benchmark_membership_coverage", settings=settings)
        else:
            output_path = settings.benchmark_membership_coverage_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            coverage.to_csv(output_path, index=False)

    return coverage
