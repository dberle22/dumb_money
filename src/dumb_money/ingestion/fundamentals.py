"""Fundamentals ingestion helpers with a flattened snapshot contract."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.models import DataSource, FundamentalSnapshot

SNAPSHOT_COLUMNS = list(FundamentalSnapshot.model_fields.keys())


def _safe_get_block(block: Any, ticker: str) -> dict[str, Any]:
    if block is None:
        return {}
    if isinstance(block, pd.DataFrame):
        if block.empty:
            return {}
        if ticker in block.index:
            row = block.loc[ticker]
            return row.to_dict() if hasattr(row, "to_dict") else dict(row)
        return block.iloc[0].to_dict()
    if isinstance(block, Mapping):
        value = block.get(ticker, block)
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def flatten_dict(payload: Mapping[str, Any], *, parent_key: str = "", separator: str = ".") -> dict[str, Any]:
    """Flatten nested dict payloads into dotted keys."""

    flat: dict[str, Any] = {}
    for key, value in payload.items():
        child_key = f"{parent_key}{separator}{key}" if parent_key else str(key)
        if isinstance(value, Mapping):
            flat.update(flatten_dict(value, parent_key=child_key, separator=separator))
        else:
            flat[child_key] = value
    return flat


def _first_number(raw: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = raw.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _normalize_dividend_yield(
    raw: Mapping[str, Any],
    *,
    source: DataSource | str,
) -> float | None:
    """Return dividend yield as a fraction, regardless of provider conventions."""

    resolved_source = DataSource(str(source))
    dividend_rate = _first_number(raw, "summary_detail.dividendRate", "price.dividendRate")
    current_price = _first_number(raw, "summary_detail.currentPrice", "price.currentPrice")
    if dividend_rate is not None and current_price not in (None, 0):
        return dividend_rate / current_price

    raw_yield = _first_number(raw, "summary_detail.dividendYield")
    if raw_yield is None:
        return None

    if resolved_source is DataSource.YFINANCE:
        return raw_yield / 100 if raw_yield > 0 else raw_yield

    return raw_yield


def collect_yahooquery_fundamentals(ticker: str) -> dict[str, Any]:
    """Pull the core YahooQuery fundamentals blocks for one ticker."""

    from yahooquery import Ticker

    client = Ticker(ticker)
    blocks = {
        "price": _safe_get_block(getattr(client, "price", None), ticker),
        "summary_detail": _safe_get_block(getattr(client, "summary_detail", None), ticker),
        "key_stats": _safe_get_block(getattr(client, "key_stats", None), ticker),
        "financial_data": _safe_get_block(getattr(client, "financial_data", None), ticker),
        "asset_profile": _safe_get_block(getattr(client, "asset_profile", None), ticker),
    }
    return {name: block for name, block in blocks.items() if block}


def collect_yfinance_fundamentals(ticker: str) -> dict[str, Any]:
    """Pull a best-effort fundamentals payload from yfinance."""

    import yfinance as yf

    client = yf.Ticker(ticker)
    info = client.info or {}
    if not isinstance(info, Mapping) or not info:
        return {}

    price = {
        "currency": info.get("currency"),
        "longName": info.get("longName") or info.get("shortName"),
        "shortName": info.get("shortName"),
        "marketCap": info.get("marketCap"),
        "sharesOutstanding": info.get("sharesOutstanding"),
        "currentPrice": info.get("currentPrice"),
    }
    summary_detail = {
        "trailingPE": info.get("trailingPE"),
        "forwardPE": info.get("forwardPE"),
        "dividendYield": info.get("dividendYield"),
        "dividendRate": info.get("dividendRate"),
        "currentPrice": info.get("currentPrice"),
        "marketCap": info.get("marketCap"),
        "priceToSalesTrailing12Months": info.get("priceToSalesTrailing12Months"),
        "trailingEps": info.get("trailingEps"),
        "forwardEps": info.get("forwardEps"),
    }
    key_stats = {
        "enterpriseValue": info.get("enterpriseValue"),
        "enterpriseToEbitda": info.get("enterpriseToEbitda"),
        "sharesOutstanding": info.get("sharesOutstanding"),
    }
    financial_data = {
        "financialCurrency": info.get("financialCurrency") or info.get("currency"),
        "totalRevenue": info.get("totalRevenue"),
        "grossProfits": info.get("grossProfits"),
        "operatingCashflow": info.get("operatingCashflow"),
        "ebitda": info.get("ebitda"),
        "freeCashflow": info.get("freeCashflow"),
        "totalDebt": info.get("totalDebt"),
        "totalCash": info.get("totalCash"),
        "grossMargins": info.get("grossMargins"),
        "operatingMargins": info.get("operatingMargins"),
        "profitMargins": info.get("profitMargins"),
        "returnOnEquity": info.get("returnOnEquity"),
        "returnOnAssets": info.get("returnOnAssets"),
        "debtToEquity": info.get("debtToEquity"),
        "currentRatio": info.get("currentRatio"),
        "netIncomeToCommon": info.get("netIncomeToCommon"),
    }
    asset_profile = {
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "website": info.get("website"),
    }

    payload = {
        "price": {key: value for key, value in price.items() if value is not None},
        "summary_detail": {
            key: value for key, value in summary_detail.items() if value is not None
        },
        "key_stats": {key: value for key, value in key_stats.items() if value is not None},
        "financial_data": {
            key: value for key, value in financial_data.items() if value is not None
        },
        "asset_profile": {key: value for key, value in asset_profile.items() if value is not None},
    }
    return {name: block for name, block in payload.items() if block}


def collect_fundamentals_payload(
    ticker: str,
    *,
    provider: DataSource | str = DataSource.YFINANCE,
) -> tuple[dict[str, Any], DataSource]:
    """Collect fundamentals from the requested provider with graceful fallback."""

    resolved_provider = DataSource(str(provider))

    if resolved_provider is DataSource.YAHOOQUERY:
        try:
            payload = collect_yahooquery_fundamentals(ticker)
        except ModuleNotFoundError:
            payload = {}
        if payload:
            return payload, DataSource.YAHOOQUERY
        fallback_payload = collect_yfinance_fundamentals(ticker)
        return fallback_payload, DataSource.YFINANCE

    payload = collect_yfinance_fundamentals(ticker)
    return payload, DataSource.YFINANCE


def normalize_fundamentals_payload(
    ticker: str,
    raw_payload: Mapping[str, Any],
    *,
    as_of_date: date | str,
    source: DataSource | str = DataSource.YFINANCE,
    raw_payload_path: str | None = None,
    pulled_at: datetime | None = None,
) -> FundamentalSnapshot:
    """Map provider-specific fundamentals payloads to the canonical snapshot."""

    flat = flatten_dict(raw_payload)
    return FundamentalSnapshot(
        ticker=ticker,
        as_of_date=as_of_date,
        source=source,
        currency=str(
            raw_payload.get("price", {}).get("currency")
            or raw_payload.get("financial_data", {}).get("financialCurrency")
            or "USD"
        ),
        long_name=raw_payload.get("price", {}).get("longName")
        or raw_payload.get("price", {}).get("shortName"),
        sector=raw_payload.get("asset_profile", {}).get("sector"),
        industry=raw_payload.get("asset_profile", {}).get("industry"),
        website=raw_payload.get("asset_profile", {}).get("website"),
        market_cap=_first_number(flat, "price.marketCap", "summary_detail.marketCap"),
        enterprise_value=_first_number(flat, "key_stats.enterpriseValue"),
        revenue_ttm=_first_number(flat, "financial_data.totalRevenue"),
        gross_profit=_first_number(flat, "financial_data.grossProfits"),
        operating_income=_first_number(flat, "financial_data.operatingCashflow"),
        net_income=_first_number(flat, "financial_data.netIncomeToCommon"),
        ebitda=_first_number(flat, "financial_data.ebitda"),
        free_cash_flow=_first_number(flat, "financial_data.freeCashflow"),
        total_debt=_first_number(flat, "financial_data.totalDebt"),
        total_cash=_first_number(flat, "financial_data.totalCash"),
        shares_outstanding=_first_number(
            flat, "price.sharesOutstanding", "key_stats.sharesOutstanding"
        ),
        eps_trailing=_first_number(flat, "summary_detail.trailingEps", "key_stats.trailingEps"),
        eps_forward=_first_number(flat, "summary_detail.forwardEps", "key_stats.forwardEps"),
        gross_margin=_first_number(flat, "financial_data.grossMargins"),
        operating_margin=_first_number(flat, "financial_data.operatingMargins"),
        profit_margin=_first_number(flat, "financial_data.profitMargins"),
        return_on_equity=_first_number(flat, "financial_data.returnOnEquity"),
        return_on_assets=_first_number(flat, "financial_data.returnOnAssets"),
        debt_to_equity=_first_number(flat, "financial_data.debtToEquity"),
        current_ratio=_first_number(flat, "financial_data.currentRatio"),
        trailing_pe=_first_number(flat, "summary_detail.trailingPE"),
        forward_pe=_first_number(flat, "summary_detail.forwardPE"),
        price_to_sales=_first_number(flat, "summary_detail.priceToSalesTrailing12Months"),
        ev_to_ebitda=_first_number(flat, "key_stats.enterpriseToEbitda"),
        dividend_yield=_normalize_dividend_yield(flat, source=source),
        raw_payload_path=raw_payload_path,
        pulled_at=pulled_at,
    )


def snapshot_to_frame(snapshot: FundamentalSnapshot) -> pd.DataFrame:
    return pd.DataFrame([snapshot.model_dump(mode="json")], columns=SNAPSHOT_COLUMNS)


def save_json_payload(payload: Mapping[str, Any], *, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, default=str))
    return output_path


def save_snapshot(snapshot: FundamentalSnapshot, *, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_to_frame(snapshot).to_csv(output_path, index=False)
    return output_path


def ingest_fundamentals(
    tickers: Sequence[str],
    *,
    as_of_date: date | str | None = None,
    settings: AppSettings | None = None,
    save_raw_json: bool = True,
    save_flat_csv: bool = True,
    provider: DataSource | str = DataSource.YFINANCE,
) -> pd.DataFrame:
    """Download and persist fundamentals snapshots for a set of tickers."""

    settings = settings or get_settings()
    settings.ensure_directories()

    resolved_as_of = as_of_date or date.today()
    snapshots: list[FundamentalSnapshot] = []

    for ticker in dict.fromkeys(str(item).strip().upper() for item in tickers if str(item).strip()):
        pulled_at = datetime.utcnow()
        payload, resolved_source = collect_fundamentals_payload(ticker, provider=provider)
        if not payload:
            continue

        raw_path: str | None = None
        if save_raw_json:
            raw_file = (
                settings.raw_fundamentals_dir
                / f"{ticker.lower()}_fundamentals_raw_{resolved_as_of}.json"
            )
            save_json_payload(payload, output_path=raw_file)
            raw_path = str(raw_file)

        snapshot = normalize_fundamentals_payload(
            ticker,
            payload,
            as_of_date=resolved_as_of,
            source=resolved_source,
            raw_payload_path=raw_path,
            pulled_at=pulled_at,
        )
        snapshots.append(snapshot)

        if save_flat_csv:
            snapshot_file = (
                settings.raw_fundamentals_dir
                / f"{ticker.lower()}_fundamentals_flat_{resolved_as_of}.csv"
            )
            save_snapshot(snapshot, output_path=snapshot_file)

    if not snapshots:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    return pd.concat([snapshot_to_frame(snapshot) for snapshot in snapshots], ignore_index=True)
