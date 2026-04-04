"""Build canonical sector snapshot summaries for downstream research context."""

from __future__ import annotations

from typing import Any

import pandas as pd

from dumb_money.analytics.company import build_fundamentals_summary, calculate_return_windows, prepare_price_history
from dumb_money.config import AppSettings, get_settings
from dumb_money.storage import (
    SECTOR_SNAPSHOT_COLUMNS,
    export_table_csv,
    read_canonical_table,
    write_canonical_table,
)


def _eligible_sector_universe(security_master: pd.DataFrame) -> pd.DataFrame:
    if security_master.empty:
        return pd.DataFrame(columns=["ticker", "sector"])

    securities = security_master.copy()
    securities["ticker"] = securities["ticker"].astype(str).str.strip().str.upper()
    if "is_active" in securities.columns:
        securities = securities.loc[securities["is_active"].fillna(False)].copy()
    if "is_eligible_research_universe" in securities.columns:
        securities = securities.loc[securities["is_eligible_research_universe"].fillna(False)].copy()
    if "is_benchmark" in securities.columns:
        securities = securities.loc[~securities["is_benchmark"].fillna(False)].copy()
    securities = securities.loc[securities["sector"].notna()].copy()
    return securities.sort_values(["sector", "ticker"]).reset_index(drop=True)


def _sector_benchmark_lookup(benchmark_mappings: pd.DataFrame) -> dict[str, str]:
    if benchmark_mappings.empty:
        return {}
    mappings = benchmark_mappings.copy()
    mappings = mappings.loc[mappings["sector"].notna() & mappings["sector_benchmark"].notna()].copy()
    if mappings.empty:
        return {}
    mappings["sector"] = mappings["sector"].astype(str).str.strip()
    mappings["sector_benchmark"] = mappings["sector_benchmark"].astype(str).str.strip().str.upper()
    return (
        mappings.groupby(["sector", "sector_benchmark"])
        .size()
        .reset_index(name="mapping_count")
        .sort_values(["sector", "mapping_count", "sector_benchmark"], ascending=[True, False, True])
        .drop_duplicates(subset=["sector"], keep="first")
        .set_index("sector")["sector_benchmark"]
        .to_dict()
    )


def _latest_fundamental_rows(
    tickers: list[str],
    fundamentals: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ticker in tickers:
        summary = build_fundamentals_summary(fundamentals, ticker)
        if len(summary) > 1:
            free_cash_flow = summary.get("free_cash_flow")
            market_cap = summary.get("market_cap")
            summary["free_cash_flow_yield"] = (
                float(free_cash_flow) / float(market_cap)
                if pd.notna(free_cash_flow) and pd.notna(market_cap) and float(market_cap) > 0
                else None
            )
            rows.append(summary)
    return rows


def _latest_return_rows(
    tickers: list[str],
    prices: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ticker in tickers:
        history = prepare_price_history(prices, ticker)
        if history.empty:
            continue
        returns = calculate_return_windows(history)
        if returns.empty:
            continue
        return_row: dict[str, Any] = {"ticker": ticker}
        for window in ["6m", "1y"]:
            matched = returns.loc[returns["window"] == window, "total_return"]
            return_row[f"return_{window}"] = matched.iloc[0] if not matched.empty else None
        rows.append(return_row)
    return rows


def build_sector_snapshots_frame(
    security_master: pd.DataFrame,
    fundamentals: pd.DataFrame,
    prices: pd.DataFrame,
    benchmark_mappings: pd.DataFrame,
) -> pd.DataFrame:
    """Build canonical per-sector summary rows for reusable research context."""

    eligible = _eligible_sector_universe(security_master)
    if eligible.empty:
        return pd.DataFrame(columns=SECTOR_SNAPSHOT_COLUMNS)

    sector_benchmarks = _sector_benchmark_lookup(benchmark_mappings)
    rows: list[dict[str, Any]] = []
    for sector, sector_frame in eligible.groupby("sector", sort=True):
        tickers = sector_frame["ticker"].astype(str).str.upper().tolist()
        fundamental_rows = pd.DataFrame(_latest_fundamental_rows(tickers, fundamentals))
        return_rows = pd.DataFrame(_latest_return_rows(tickers, prices))

        rows.append(
            {
                "sector": sector,
                "sector_benchmark": sector_benchmarks.get(str(sector).strip()),
                "company_count": int(len(tickers)),
                "companies_with_fundamentals": int(len(fundamental_rows)),
                "companies_with_prices": int(len(return_rows)),
                "median_market_cap": fundamental_rows["market_cap"].dropna().median() if "market_cap" in fundamental_rows else None,
                "median_forward_pe": fundamental_rows["forward_pe"].dropna().median() if "forward_pe" in fundamental_rows else None,
                "median_ev_to_ebitda": fundamental_rows["ev_to_ebitda"].dropna().median() if "ev_to_ebitda" in fundamental_rows else None,
                "median_price_to_sales": fundamental_rows["price_to_sales"].dropna().median() if "price_to_sales" in fundamental_rows else None,
                "median_free_cash_flow_yield": (
                    fundamental_rows["free_cash_flow_yield"].dropna().median()
                    if "free_cash_flow_yield" in fundamental_rows
                    else None
                ),
                "median_operating_margin": (
                    fundamental_rows["operating_margin"].dropna().median()
                    if "operating_margin" in fundamental_rows
                    else None
                ),
                "median_gross_margin": (
                    fundamental_rows["gross_margin"].dropna().median()
                    if "gross_margin" in fundamental_rows
                    else None
                ),
                "median_return_6m": return_rows["return_6m"].dropna().median() if "return_6m" in return_rows else None,
                "median_return_1y": return_rows["return_1y"].dropna().median() if "return_1y" in return_rows else None,
            }
        )

    return pd.DataFrame(rows, columns=SECTOR_SNAPSHOT_COLUMNS).sort_values("sector").reset_index(drop=True)


def stage_sector_snapshots(
    *,
    settings: AppSettings | None = None,
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Build and materialize canonical sector snapshot summaries."""

    settings = settings or get_settings()
    security_master = read_canonical_table("security_master", settings=settings)
    fundamentals = read_canonical_table("normalized_fundamentals", settings=settings)
    prices = read_canonical_table("normalized_prices", settings=settings)
    benchmark_mappings = read_canonical_table("benchmark_mappings", settings=settings)
    sector_snapshots = build_sector_snapshots_frame(
        security_master,
        fundamentals,
        prices,
        benchmark_mappings,
    )

    if write_warehouse:
        write_canonical_table(sector_snapshots, "sector_snapshots", settings=settings)

    if write_csv and not sector_snapshots.empty:
        export_table_csv(sector_snapshots, "sector_snapshots", settings=settings)

    return sector_snapshots
