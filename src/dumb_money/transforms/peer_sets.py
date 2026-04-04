"""Build canonical peer-group memberships for maintained securities."""

from __future__ import annotations

from math import inf, log10

import pandas as pd

from dumb_money.config import AppSettings, get_settings
from dumb_money.storage import PEER_SET_COLUMNS, export_table_csv, read_canonical_table, write_canonical_table

MIN_INDUSTRY_PEERS = 3
MAX_PEERS = 8
ELIGIBLE_ASSET_TYPES = {"common_stock", "adr"}


def _latest_market_caps(fundamentals: pd.DataFrame) -> dict[str, float]:
    if fundamentals.empty:
        return {}

    rows = fundamentals.copy()
    rows["ticker"] = rows["ticker"].astype(str).str.strip().str.upper()
    rows["as_of_date"] = pd.to_datetime(rows["as_of_date"], errors="coerce")
    if "period_end_date" in rows.columns:
        rows["period_end_date"] = pd.to_datetime(rows["period_end_date"], errors="coerce")
    else:
        rows["period_end_date"] = pd.NaT
    if "period_type" not in rows.columns:
        rows["period_type"] = None

    latest = (
        rows.assign(
            _period_rank=rows["period_type"].map({"quarterly": 0, "annual": 1, "ttm": 2}).fillna(-1)
        )
        .sort_values(["ticker", "as_of_date", "period_end_date", "_period_rank"])
        .groupby("ticker", as_index=False)
        .tail(1)
    )
    market_cap_series = pd.to_numeric(latest["market_cap"], errors="coerce")
    latest = latest.assign(market_cap=market_cap_series)
    return latest.set_index("ticker")["market_cap"].dropna().to_dict()


def _eligible_securities(security_master: pd.DataFrame) -> pd.DataFrame:
    if security_master.empty:
        return pd.DataFrame(columns=["ticker", "sector", "industry", "asset_type"])

    securities = security_master.copy()
    securities["ticker"] = securities["ticker"].astype(str).str.strip().str.upper()
    securities["asset_type"] = securities["asset_type"].astype(str).str.strip().str.lower()
    if "is_active" in securities.columns:
        securities = securities.loc[securities["is_active"].fillna(False)].copy()
    if "is_eligible_research_universe" in securities.columns:
        securities = securities.loc[securities["is_eligible_research_universe"].fillna(False)].copy()
    if "is_benchmark" in securities.columns:
        securities = securities.loc[~securities["is_benchmark"].fillna(False)].copy()
    securities = securities.loc[securities["asset_type"].isin(ELIGIBLE_ASSET_TYPES)].copy()
    return securities.sort_values("ticker").reset_index(drop=True)


def _market_cap_sort_key(
    candidate_ticker: str,
    candidate_market_cap: float | None,
    focal_market_cap: float | None,
) -> tuple[float, float, str]:
    if candidate_market_cap is None or pd.isna(candidate_market_cap) or focal_market_cap is None or pd.isna(focal_market_cap):
        return (1.0, inf, candidate_ticker)
    return (
        0.0,
        abs(log10(float(candidate_market_cap)) - log10(float(focal_market_cap))),
        candidate_ticker,
    )


def build_peer_sets_frame(
    security_master: pd.DataFrame,
    fundamentals: pd.DataFrame,
    *,
    min_industry_peers: int = MIN_INDUSTRY_PEERS,
    max_peers: int = MAX_PEERS,
) -> pd.DataFrame:
    """Build canonical peer-group memberships for eligible maintained securities."""

    eligible = _eligible_securities(security_master)
    if eligible.empty:
        return pd.DataFrame(columns=PEER_SET_COLUMNS)

    market_caps = _latest_market_caps(fundamentals)
    rows: list[dict[str, object]] = []

    for record in eligible.to_dict(orient="records"):
        ticker = str(record["ticker"]).strip().upper()
        sector = record.get("sector")
        industry = record.get("industry")
        focal_market_cap = market_caps.get(ticker)

        candidates = eligible.loc[eligible["ticker"] != ticker].copy()
        relationship_type = "sector"
        selection_method = "sector_fallback"

        industry_candidates = (
            candidates.loc[candidates["industry"].astype(str).str.strip() == str(industry).strip()].copy()
            if industry
            else pd.DataFrame(columns=candidates.columns)
        )
        sector_candidates = (
            candidates.loc[candidates["sector"].astype(str).str.strip() == str(sector).strip()].copy()
            if sector
            else pd.DataFrame(columns=candidates.columns)
        )

        if industry and len(industry_candidates) >= min_industry_peers:
            chosen = industry_candidates
            relationship_type = "industry"
            selection_method = "industry_market_cap_proximity"
        elif sector and not sector_candidates.empty:
            chosen = sector_candidates
            selection_method = "sector_market_cap_proximity"
        else:
            chosen = candidates
            relationship_type = "universe"
            selection_method = "eligible_universe_fallback"

        chosen = chosen.assign(
            _sort_key=chosen["ticker"].map(
                lambda candidate_ticker: _market_cap_sort_key(
                    candidate_ticker,
                    market_caps.get(str(candidate_ticker).strip().upper()),
                    focal_market_cap,
                )
            )
        ).sort_values("_sort_key").head(max_peers)

        for peer_order, peer_row in enumerate(chosen.to_dict(orient="records"), start=1):
            rows.append(
                {
                    "peer_set_id": f"peer_set::{ticker}",
                    "ticker": ticker,
                    "peer_ticker": str(peer_row["ticker"]).strip().upper(),
                    "relationship_type": relationship_type,
                    "sector": sector,
                    "industry": industry,
                    "selection_method": selection_method,
                    "peer_order": peer_order,
                }
            )

    return pd.DataFrame(rows, columns=PEER_SET_COLUMNS).sort_values(["ticker", "peer_order"]).reset_index(drop=True)


def stage_peer_sets(
    *,
    settings: AppSettings | None = None,
    write_warehouse: bool = True,
    write_csv: bool = True,
) -> pd.DataFrame:
    """Build and materialize canonical peer-group memberships."""

    settings = settings or get_settings()
    security_master = read_canonical_table("security_master", settings=settings)
    fundamentals = read_canonical_table("normalized_fundamentals", settings=settings)
    peer_sets = build_peer_sets_frame(security_master, fundamentals)

    if write_warehouse:
        write_canonical_table(peer_sets, "peer_sets", settings=settings)

    if write_csv and not peer_sets.empty:
        export_table_csv(peer_sets, "peer_sets", settings=settings)

    return peer_sets
