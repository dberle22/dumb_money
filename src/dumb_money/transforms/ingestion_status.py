"""Build ingestion coverage controls for maintained securities."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from dumb_money.config import AppSettings, get_settings
from dumb_money.storage import (
    SECURITY_INGESTION_STATUS_COLUMNS,
    export_table_csv,
    read_canonical_table,
    write_canonical_table,
)


def build_security_ingestion_status_frame(
    security_master: pd.DataFrame,
    normalized_prices: pd.DataFrame,
    normalized_fundamentals: pd.DataFrame,
    benchmark_memberships: pd.DataFrame,
) -> pd.DataFrame:
    """Summarize staged ingestion coverage by security ticker."""

    if security_master.empty:
        return pd.DataFrame(columns=SECURITY_INGESTION_STATUS_COLUMNS)

    base = security_master[
        ["ticker", "security_id", "name", "asset_type", "is_eligible_research_universe"]
    ].copy()
    base["ticker"] = base["ticker"].astype(str).str.strip().str.upper()

    if benchmark_memberships.empty:
        benchmark_counts = pd.DataFrame(columns=["ticker", "benchmark_membership_count"])
    else:
        benchmark_counts = (
            benchmark_memberships.assign(
                member_ticker=benchmark_memberships["member_ticker"].astype(str).str.strip().str.upper()
            )
            .groupby("member_ticker", as_index=False)
            .size()
            .rename(columns={"member_ticker": "ticker", "size": "benchmark_membership_count"})
        )

    if normalized_prices.empty:
        price_status = pd.DataFrame(
            columns=["ticker", "first_price_date", "latest_price_date", "price_record_count"]
        )
    else:
        prices = normalized_prices.copy()
        prices["ticker"] = prices["ticker"].astype(str).str.strip().str.upper()
        prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
        price_status = (
            prices.groupby("ticker", as_index=False)
            .agg(
                first_price_date=("date", "min"),
                latest_price_date=("date", "max"),
                price_record_count=("date", "count"),
            )
        )
        price_status["first_price_date"] = price_status["first_price_date"].dt.date
        price_status["latest_price_date"] = price_status["latest_price_date"].dt.date

    if normalized_fundamentals.empty:
        fundamentals_status = pd.DataFrame(
            columns=[
                "ticker",
                "first_fundamental_period_end_date",
                "latest_fundamental_period_end_date",
                "fundamentals_record_count",
            ]
        )
    else:
        fundamentals = normalized_fundamentals.copy()
        fundamentals["ticker"] = fundamentals["ticker"].astype(str).str.strip().str.upper()
        fundamentals["period_end_date"] = pd.to_datetime(
            fundamentals["period_end_date"],
            errors="coerce",
        )
        fundamentals_status = (
            fundamentals.groupby("ticker", as_index=False)
            .agg(
                first_fundamental_period_end_date=("period_end_date", "min"),
                latest_fundamental_period_end_date=("period_end_date", "max"),
                fundamentals_record_count=("period_end_date", "count"),
            )
        )
        fundamentals_status["first_fundamental_period_end_date"] = fundamentals_status[
            "first_fundamental_period_end_date"
        ].dt.date
        fundamentals_status["latest_fundamental_period_end_date"] = fundamentals_status[
            "latest_fundamental_period_end_date"
        ].dt.date

    status = (
        base.merge(benchmark_counts, on="ticker", how="left")
        .merge(price_status, on="ticker", how="left")
        .merge(fundamentals_status, on="ticker", how="left")
    )
    status["benchmark_membership_count"] = (
        pd.to_numeric(status["benchmark_membership_count"], errors="coerce").fillna(0).astype(int)
    )
    status["price_record_count"] = (
        pd.to_numeric(status["price_record_count"], errors="coerce").fillna(0).astype(int)
    )
    status["fundamentals_record_count"] = (
        pd.to_numeric(status["fundamentals_record_count"], errors="coerce").fillna(0).astype(int)
    )
    status["has_price_history"] = status["price_record_count"] > 0
    status["has_historical_fundamentals"] = status["fundamentals_record_count"] > 0
    status["has_any_ingestion"] = status["has_price_history"] | status["has_historical_fundamentals"]
    status["is_fully_ingested"] = status["has_price_history"] & status["has_historical_fundamentals"]
    status["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0)

    return status[SECURITY_INGESTION_STATUS_COLUMNS].sort_values(["ticker"]).reset_index(drop=True)


def stage_security_ingestion_status(
    *,
    settings: AppSettings | None = None,
    output_name: str = "security_ingestion_status.csv",
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Materialize the security ingestion control table from canonical datasets."""

    settings = settings or get_settings()
    settings.ensure_directories()

    frame = build_security_ingestion_status_frame(
        read_canonical_table("security_master", settings=settings),
        read_canonical_table("normalized_prices", settings=settings),
        read_canonical_table("normalized_fundamentals", settings=settings),
        read_canonical_table("benchmark_memberships", settings=settings),
    )

    if write_warehouse:
        write_canonical_table(frame, "security_ingestion_status", settings=settings)

    if write_csv and not frame.empty:
        if output_name == "security_ingestion_status.csv":
            export_table_csv(frame, "security_ingestion_status", settings=settings)
        else:
            output_path = settings.security_ingestion_status_dir / output_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(output_path, index=False)

    return frame
