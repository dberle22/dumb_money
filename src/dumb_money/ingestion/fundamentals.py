"""Fundamentals ingestion helpers with a period-aware historical contract."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.models import DataSource, FundamentalSnapshot

FUNDAMENTAL_COLUMNS = list(FundamentalSnapshot.model_fields.keys())
SNAPSHOT_COLUMNS = FUNDAMENTAL_COLUMNS

PERIOD_TYPES = ("quarterly", "annual", "ttm")

STATEMENT_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "revenue": ("Total Revenue", "Operating Revenue"),
    "gross_profit": ("Gross Profit",),
    "operating_income": ("Operating Income",),
    "net_income": ("Net Income", "Net Income Common Stockholders"),
    "pretax_income": ("Pretax Income",),
    "tax_provision": ("Tax Provision",),
    "tax_rate_for_calcs": ("Tax Rate For Calcs",),
    "ebitda": ("EBITDA", "Normalized EBITDA"),
    "free_cash_flow": ("Free Cash Flow",),
    "interest_expense": ("Interest Expense", "Interest Expense Non Operating"),
    "total_debt": ("Total Debt",),
    "total_cash": (
        "Cash Cash Equivalents And Short Term Investments",
        "Cash And Cash Equivalents",
        "Cash Equivalents",
        "Cash",
    ),
    "total_assets": ("Total Assets",),
    "current_assets": ("Current Assets",),
    "current_liabilities": ("Current Liabilities",),
    "total_equity_gross_minority_interest": ("Total Equity Gross Minority Interest",),
    "stockholders_equity": ("Stockholders Equity", "Common Stock Equity"),
    "invested_capital": ("Invested Capital",),
    "working_capital": ("Working Capital",),
    "basic_eps": ("Basic EPS",),
    "diluted_eps": ("Diluted EPS",),
}


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
        if value is None or isinstance(value, bool):
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


def _statement_to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []

    normalized = frame.copy()
    normalized.columns = [str(pd.Timestamp(column).date()) for column in normalized.columns]
    records: list[dict[str, Any]] = []

    for label, row in normalized.iterrows():
        record = {"line_item": str(label)}
        record.update({column: row[column] for column in normalized.columns})
        records.append(record)

    return records


def _extract_statement_metric(
    statement: pd.DataFrame | None,
    aliases: Sequence[str],
) -> dict[date, float]:
    if statement is None or statement.empty:
        return {}

    normalized = statement.copy()
    normalized.index = normalized.index.map(str)
    matched_label = next((label for label in aliases if label in normalized.index), None)
    if matched_label is None:
        return {}

    series = normalized.loc[matched_label]
    if isinstance(series, pd.DataFrame):
        series = series.iloc[0]

    values: dict[date, float] = {}
    for column, value in series.items():
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            continue
        period_end = pd.to_datetime(column, errors="coerce")
        if pd.isna(period_end):
            continue
        values[period_end.date()] = float(numeric)

    return values


def _infer_period_metadata(period_end_date: date, period_type: str) -> dict[str, Any]:
    if period_type == "ttm":
        return {
            "fiscal_year": period_end_date.year,
            "fiscal_quarter": None,
            "fiscal_period": "TTM",
            "period_type": "ttm",
        }
    if period_type == "annual":
        return {
            "fiscal_year": period_end_date.year,
            "fiscal_quarter": None,
            "fiscal_period": "FY",
            "period_type": "annual",
        }

    quarter = ((period_end_date.month - 1) // 3) + 1
    return {
        "fiscal_year": period_end_date.year,
        "fiscal_quarter": quarter,
        "fiscal_period": f"Q{quarter}",
        "period_type": "quarterly",
    }


def _base_record(
    ticker: str,
    raw_payload: Mapping[str, Any],
    *,
    as_of_date: date | str,
    source: DataSource | str,
    raw_payload_path: str | None,
    pulled_at: datetime | None,
) -> dict[str, Any]:
    flat = flatten_dict(raw_payload)
    return {
        "ticker": ticker,
        "as_of_date": as_of_date,
        "source": source,
        "currency": str(
            raw_payload.get("price", {}).get("currency")
            or raw_payload.get("financial_data", {}).get("financialCurrency")
            or "USD"
        ),
        "long_name": raw_payload.get("price", {}).get("longName")
        or raw_payload.get("price", {}).get("shortName"),
        "sector": raw_payload.get("asset_profile", {}).get("sector"),
        "industry": raw_payload.get("asset_profile", {}).get("industry"),
        "website": raw_payload.get("asset_profile", {}).get("website"),
        "market_cap": _first_number(flat, "price.marketCap", "summary_detail.marketCap"),
        "enterprise_value": _first_number(flat, "key_stats.enterpriseValue"),
        "revenue_ttm": _first_number(flat, "financial_data.totalRevenue"),
        "shares_outstanding": _first_number(
            flat, "price.sharesOutstanding", "key_stats.sharesOutstanding"
        ),
        "eps_trailing": _first_number(flat, "summary_detail.trailingEps", "key_stats.trailingEps"),
        "eps_forward": _first_number(flat, "summary_detail.forwardEps", "key_stats.forwardEps"),
        "gross_margin": _first_number(flat, "financial_data.grossMargins"),
        "operating_margin": _first_number(flat, "financial_data.operatingMargins"),
        "profit_margin": _first_number(flat, "financial_data.profitMargins"),
        "return_on_equity": _first_number(flat, "financial_data.returnOnEquity"),
        "return_on_assets": _first_number(flat, "financial_data.returnOnAssets"),
        "debt_to_equity": _first_number(flat, "financial_data.debtToEquity"),
        "current_ratio": _first_number(flat, "financial_data.currentRatio"),
        "trailing_pe": _first_number(flat, "summary_detail.trailingPE"),
        "forward_pe": _first_number(flat, "summary_detail.forwardPE"),
        "price_to_sales": _first_number(flat, "summary_detail.priceToSalesTrailing12Months"),
        "ev_to_ebitda": _first_number(flat, "key_stats.enterpriseToEbitda"),
        "dividend_yield": _normalize_dividend_yield(flat, source=source),
        "raw_payload_path": raw_payload_path,
        "pulled_at": pulled_at,
    }


def _legacy_snapshot_record(
    ticker: str,
    raw_payload: Mapping[str, Any],
    *,
    as_of_date: date | str,
    source: DataSource | str,
    raw_payload_path: str | None,
    pulled_at: datetime | None,
) -> dict[str, Any]:
    flat = flatten_dict(raw_payload)
    record = _base_record(
        ticker,
        raw_payload,
        as_of_date=as_of_date,
        source=source,
        raw_payload_path=raw_payload_path,
        pulled_at=pulled_at,
    )
    as_of = pd.to_datetime(as_of_date).date()
    record.update(
        {
            "period_end_date": as_of,
            "report_date": None,
            "fiscal_year": as_of.year,
            "fiscal_quarter": None,
            "fiscal_period": "TTM",
            "period_type": "ttm",
            "revenue": _first_number(flat, "financial_data.totalRevenue"),
            "gross_profit": _first_number(flat, "financial_data.grossProfits"),
            "operating_income": _first_number(flat, "financial_data.operatingCashflow"),
            "net_income": _first_number(flat, "financial_data.netIncomeToCommon"),
            "ebitda": _first_number(flat, "financial_data.ebitda"),
            "free_cash_flow": _first_number(flat, "financial_data.freeCashflow"),
            "total_debt": _first_number(flat, "financial_data.totalDebt"),
            "total_cash": _first_number(flat, "financial_data.totalCash"),
            "current_assets": None,
            "current_liabilities": None,
        }
    )
    return record


def _build_historical_records(
    ticker: str,
    raw_payload: Mapping[str, Any],
    *,
    as_of_date: date | str,
    source: DataSource | str,
    raw_payload_path: str | None,
    pulled_at: datetime | None,
) -> list[dict[str, Any]]:
    base = _base_record(
        ticker,
        raw_payload,
        as_of_date=as_of_date,
        source=source,
        raw_payload_path=raw_payload_path,
        pulled_at=pulled_at,
    )
    supplied = raw_payload.get("historical_fundamentals", [])
    if not isinstance(supplied, list) or not supplied:
        return [_legacy_snapshot_record(
            ticker,
            raw_payload,
            as_of_date=as_of_date,
            source=source,
            raw_payload_path=raw_payload_path,
            pulled_at=pulled_at,
        )]

    records: list[dict[str, Any]] = []
    for row in supplied:
        if not isinstance(row, Mapping):
            continue
        period_end = pd.to_datetime(row.get("period_end_date"), errors="coerce")
        if pd.isna(period_end):
            continue
        metadata = _infer_period_metadata(period_end.date(), str(row.get("period_type", "")))
        record = {**base, **metadata}
        record.update(
            {
                "period_end_date": period_end.date(),
                "report_date": (
                    pd.to_datetime(row.get("report_date"), errors="coerce").date()
                    if pd.notna(pd.to_datetime(row.get("report_date"), errors="coerce"))
                    else None
                ),
                "revenue": row.get("revenue"),
                "revenue_ttm": row.get("revenue_ttm", base.get("revenue_ttm")),
                "gross_profit": row.get("gross_profit"),
                "operating_income": row.get("operating_income"),
                "net_income": row.get("net_income"),
                "pretax_income": row.get("pretax_income"),
                "tax_provision": row.get("tax_provision"),
                "tax_rate_for_calcs": row.get("tax_rate_for_calcs"),
                "nopat": row.get("nopat"),
                "ebitda": row.get("ebitda"),
                "free_cash_flow": row.get("free_cash_flow"),
                "interest_expense": row.get("interest_expense"),
                "total_debt": row.get("total_debt"),
                "total_cash": row.get("total_cash"),
                "total_assets": row.get("total_assets"),
                "current_assets": row.get("current_assets"),
                "current_liabilities": row.get("current_liabilities"),
                "total_equity_gross_minority_interest": row.get("total_equity_gross_minority_interest"),
                "stockholders_equity": row.get("stockholders_equity"),
                "invested_capital": row.get("invested_capital"),
                "working_capital": row.get("working_capital"),
                "basic_eps": row.get("basic_eps"),
                "diluted_eps": row.get("diluted_eps"),
                "effective_tax_rate": row.get("effective_tax_rate"),
                "return_on_invested_capital": row.get("return_on_invested_capital"),
            }
        )
        if record["period_type"] != "ttm":
            record["revenue_ttm"] = base.get("revenue_ttm") if record["period_type"] == "annual" else None
        records.append(record)

    return _apply_historical_metric_derivations(records) or [_legacy_snapshot_record(
        ticker,
        raw_payload,
        as_of_date=as_of_date,
        source=source,
        raw_payload_path=raw_payload_path,
        pulled_at=pulled_at,
    )]


def _safe_ratio(numerator: Any, denominator: Any) -> float | None:
    numerator_value = pd.to_numeric(numerator, errors="coerce")
    denominator_value = pd.to_numeric(denominator, errors="coerce")
    if pd.isna(numerator_value) or pd.isna(denominator_value) or float(denominator_value) == 0:
        return None
    return float(numerator_value) / float(denominator_value)


def _average_period_denominator(current: Any, prior: Any) -> float | None:
    current_value = pd.to_numeric(current, errors="coerce")
    prior_value = pd.to_numeric(prior, errors="coerce")
    if pd.isna(current_value):
        return None
    if pd.isna(prior_value):
        return float(current_value)
    return (float(current_value) + float(prior_value)) / 2.0


def _resolve_effective_tax_rate(record: Mapping[str, Any]) -> float | None:
    tax_rate_for_calcs = pd.to_numeric(record.get("tax_rate_for_calcs"), errors="coerce")
    if pd.notna(tax_rate_for_calcs) and 0 <= float(tax_rate_for_calcs) <= 1:
        return float(tax_rate_for_calcs)

    pretax_income = pd.to_numeric(record.get("pretax_income"), errors="coerce")
    tax_provision = pd.to_numeric(record.get("tax_provision"), errors="coerce")
    if pd.notna(pretax_income) and pd.notna(tax_provision) and float(pretax_income) > 0:
        derived = float(tax_provision) / float(pretax_income)
        if 0 <= derived <= 1:
            return derived
    return None


def _apply_historical_metric_derivations(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not records:
        return records

    ordered = sorted(records, key=lambda row: (str(row.get("period_type")), str(row.get("period_end_date"))))
    prior_by_period_type: dict[str, dict[str, Any]] = {}

    for record in ordered:
        period_type = str(record.get("period_type") or "")
        prior = prior_by_period_type.get(period_type, {})

        gross_margin = _safe_ratio(record.get("gross_profit"), record.get("revenue"))
        if gross_margin is not None:
            record["gross_margin"] = gross_margin
        operating_margin = _safe_ratio(record.get("operating_income"), record.get("revenue"))
        if operating_margin is not None:
            record["operating_margin"] = operating_margin
        profit_margin = _safe_ratio(record.get("net_income"), record.get("revenue"))
        if profit_margin is not None:
            record["profit_margin"] = profit_margin
        current_ratio = _safe_ratio(record.get("current_assets"), record.get("current_liabilities"))
        if current_ratio is not None:
            record["current_ratio"] = current_ratio

        equity_base = record.get("stockholders_equity")
        if equity_base is None or pd.isna(pd.to_numeric(equity_base, errors="coerce")):
            equity_base = record.get("total_equity_gross_minority_interest")
        debt_to_equity = _safe_ratio(record.get("total_debt"), equity_base)
        if debt_to_equity is not None:
            # Keep provider convention aligned with Yahoo's debt-to-equity percent-like scale.
            record["debt_to_equity"] = debt_to_equity * 100.0

        record["effective_tax_rate"] = _resolve_effective_tax_rate(record)
        operating_income = pd.to_numeric(record.get("operating_income"), errors="coerce")
        effective_tax_rate = pd.to_numeric(record.get("effective_tax_rate"), errors="coerce")
        if pd.notna(operating_income) and pd.notna(effective_tax_rate):
            record["nopat"] = float(operating_income) * (1.0 - float(effective_tax_rate))

        prior_equity = prior.get("stockholders_equity")
        if prior_equity is None or pd.isna(pd.to_numeric(prior_equity, errors="coerce")):
            prior_equity = prior.get("total_equity_gross_minority_interest")
        roe_denominator = _average_period_denominator(equity_base, prior_equity)
        roa_denominator = _average_period_denominator(record.get("total_assets"), prior.get("total_assets"))
        roic_denominator = _average_period_denominator(record.get("invested_capital"), prior.get("invested_capital"))

        roe = _safe_ratio(record.get("net_income"), roe_denominator)
        roa = _safe_ratio(record.get("net_income"), roa_denominator)
        roic = _safe_ratio(record.get("nopat"), roic_denominator)
        if roe is not None:
            record["return_on_equity"] = roe
        if roa is not None:
            record["return_on_assets"] = roa
        if roic is not None:
            record["return_on_invested_capital"] = roic

        prior_by_period_type[period_type] = record.copy()

    return ordered


def _collect_statement_history(
    statement: pd.DataFrame | None,
    *,
    period_type: str,
    field_alias_map: Mapping[str, Sequence[str]],
) -> dict[date, dict[str, Any]]:
    rows: dict[date, dict[str, Any]] = {}
    for field_name, aliases in field_alias_map.items():
        metric_values = _extract_statement_metric(statement, aliases)
        for period_end_date, value in metric_values.items():
            row = rows.setdefault(period_end_date, {})
            row[field_name] = value
            row["period_end_date"] = period_end_date.isoformat()
            row["period_type"] = period_type
    return rows


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
    """Pull a best-effort period-aware fundamentals payload from yfinance."""

    import yfinance as yf

    client = yf.Ticker(ticker)
    info = client.info or {}
    if not isinstance(info, Mapping) or not info:
        return {}

    payload = {
        "price": {
            key: value
            for key, value in {
                "currency": info.get("currency"),
                "longName": info.get("longName") or info.get("shortName"),
                "shortName": info.get("shortName"),
                "marketCap": info.get("marketCap"),
                "sharesOutstanding": info.get("sharesOutstanding"),
                "currentPrice": info.get("currentPrice"),
            }.items()
            if value is not None
        },
        "summary_detail": {
            key: value
            for key, value in {
                "trailingPE": info.get("trailingPE"),
                "forwardPE": info.get("forwardPE"),
                "dividendYield": info.get("dividendYield"),
                "dividendRate": info.get("dividendRate"),
                "currentPrice": info.get("currentPrice"),
                "marketCap": info.get("marketCap"),
                "priceToSalesTrailing12Months": info.get("priceToSalesTrailing12Months"),
                "trailingEps": info.get("trailingEps"),
                "forwardEps": info.get("forwardEps"),
            }.items()
            if value is not None
        },
        "key_stats": {
            key: value
            for key, value in {
                "enterpriseValue": info.get("enterpriseValue"),
                "enterpriseToEbitda": info.get("enterpriseToEbitda"),
                "sharesOutstanding": info.get("sharesOutstanding"),
            }.items()
            if value is not None
        },
        "financial_data": {
            key: value
            for key, value in {
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
            }.items()
            if value is not None
        },
        "asset_profile": {
            key: value
            for key, value in {
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "website": info.get("website"),
            }.items()
            if value is not None
        },
    }

    annual_income_stmt = client.income_stmt
    quarterly_income_stmt = client.quarterly_income_stmt
    ttm_income_stmt = client.ttm_income_stmt
    annual_balance_sheet = client.balance_sheet
    quarterly_balance_sheet = client.quarterly_balance_sheet
    annual_cash_flow = client.cash_flow
    quarterly_cash_flow = client.quarterly_cash_flow
    ttm_cash_flow = client.ttm_cash_flow

    historical_rows: dict[tuple[str, str], dict[str, Any]] = {}

    statement_sets = [
        (annual_income_stmt, "annual"),
        (quarterly_income_stmt, "quarterly"),
        (ttm_income_stmt, "ttm"),
    ]
    for statement, period_type in statement_sets:
        for period_end, values in _collect_statement_history(
            statement,
            period_type=period_type,
            field_alias_map={
                key: value
                for key, value in STATEMENT_FIELD_ALIASES.items()
                if key
                in {
                    "revenue",
                    "gross_profit",
                    "operating_income",
                    "net_income",
                    "pretax_income",
                    "tax_provision",
                    "tax_rate_for_calcs",
                    "interest_expense",
                    "ebitda",
                    "basic_eps",
                    "diluted_eps",
                }
            },
        ).items():
            historical_rows[(period_type, period_end.isoformat())] = values

    for statement, period_type in [
        (annual_cash_flow, "annual"),
        (quarterly_cash_flow, "quarterly"),
        (ttm_cash_flow, "ttm"),
    ]:
        for period_end, values in _collect_statement_history(
            statement,
            period_type=period_type,
            field_alias_map={"free_cash_flow": STATEMENT_FIELD_ALIASES["free_cash_flow"]},
        ).items():
            historical_rows.setdefault((period_type, period_end.isoformat()), {}).update(values)

    for statement, period_type in [
        (annual_balance_sheet, "annual"),
        (quarterly_balance_sheet, "quarterly"),
    ]:
        for period_end, values in _collect_statement_history(
            statement,
            period_type=period_type,
            field_alias_map={
                key: STATEMENT_FIELD_ALIASES[key]
                for key in (
                    "total_debt",
                    "total_cash",
                    "total_assets",
                    "current_assets",
                    "current_liabilities",
                    "total_equity_gross_minority_interest",
                    "stockholders_equity",
                    "invested_capital",
                    "working_capital",
                )
            },
        ).items():
            historical_rows.setdefault((period_type, period_end.isoformat()), {}).update(values)

    payload["historical_fundamentals"] = [
        historical_rows[key]
        for key in sorted(
            historical_rows,
            key=lambda item: (item[0], item[1]),
        )
    ]
    payload["statements"] = {
        "annual_income_stmt": _statement_to_records(annual_income_stmt),
        "quarterly_income_stmt": _statement_to_records(quarterly_income_stmt),
        "ttm_income_stmt": _statement_to_records(ttm_income_stmt),
        "annual_balance_sheet": _statement_to_records(annual_balance_sheet),
        "quarterly_balance_sheet": _statement_to_records(quarterly_balance_sheet),
        "annual_cash_flow": _statement_to_records(annual_cash_flow),
        "quarterly_cash_flow": _statement_to_records(quarterly_cash_flow),
        "ttm_cash_flow": _statement_to_records(ttm_cash_flow),
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


def normalize_historical_fundamentals_payload(
    ticker: str,
    raw_payload: Mapping[str, Any],
    *,
    as_of_date: date | str,
    source: DataSource | str = DataSource.YFINANCE,
    raw_payload_path: str | None = None,
    pulled_at: datetime | None = None,
) -> list[FundamentalSnapshot]:
    """Map provider payloads to canonical historical fundamentals rows."""

    records = _build_historical_records(
        ticker,
        raw_payload,
        as_of_date=as_of_date,
        source=source,
        raw_payload_path=raw_payload_path,
        pulled_at=pulled_at,
    )
    return [FundamentalSnapshot.model_validate(record) for record in records]


def normalize_fundamentals_payload(
    ticker: str,
    raw_payload: Mapping[str, Any],
    *,
    as_of_date: date | str,
    source: DataSource | str = DataSource.YFINANCE,
    raw_payload_path: str | None = None,
    pulled_at: datetime | None = None,
) -> FundamentalSnapshot:
    """Return the latest available normalized fundamentals row for compatibility."""

    snapshots = normalize_historical_fundamentals_payload(
        ticker,
        raw_payload,
        as_of_date=as_of_date,
        source=source,
        raw_payload_path=raw_payload_path,
        pulled_at=pulled_at,
    )
    snapshots.sort(
        key=lambda snapshot: (
            pd.to_datetime(snapshot.period_end_date or snapshot.as_of_date),
            snapshot.period_type != "ttm",
            snapshot.period_type != "annual",
        )
    )
    return snapshots[-1]


def snapshots_to_frame(snapshots: Sequence[FundamentalSnapshot]) -> pd.DataFrame:
    return pd.DataFrame(
        [snapshot.model_dump(mode="json") for snapshot in snapshots],
        columns=FUNDAMENTAL_COLUMNS,
    )


def snapshot_to_frame(snapshot: FundamentalSnapshot) -> pd.DataFrame:
    return snapshots_to_frame([snapshot])


def save_json_payload(payload: Mapping[str, Any], *, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, default=str))
    return output_path


def save_snapshot(snapshot: FundamentalSnapshot, *, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_to_frame(snapshot).to_csv(output_path, index=False)
    return output_path


def save_snapshots(snapshots: Sequence[FundamentalSnapshot], *, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    snapshots_to_frame(snapshots).to_csv(output_path, index=False)
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
    """Download and persist period-aware fundamentals for a set of tickers."""

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
            raw_file = settings.raw_fundamentals_dir / f"{ticker.lower()}_fundamentals_raw_{resolved_as_of}.json"
            save_json_payload(payload, output_path=raw_file)
            raw_path = str(raw_file)

        ticker_snapshots = normalize_historical_fundamentals_payload(
            ticker,
            payload,
            as_of_date=resolved_as_of,
            source=resolved_source,
            raw_payload_path=raw_path,
            pulled_at=pulled_at,
        )
        snapshots.extend(ticker_snapshots)

        if save_flat_csv:
            snapshot_file = settings.raw_fundamentals_dir / f"{ticker.lower()}_fundamentals_flat_{resolved_as_of}.csv"
            save_snapshots(ticker_snapshots, output_path=snapshot_file)

    if not snapshots:
        return pd.DataFrame(columns=FUNDAMENTAL_COLUMNS)

    return snapshots_to_frame(snapshots)


def ingest_selected_fundamentals(
    *,
    tickers: Sequence[str] | None = None,
    ticker_query_sql: str | None = None,
    as_of_date: date | str | None = None,
    settings: AppSettings | None = None,
    provider: DataSource | str = DataSource.YFINANCE,
    save_raw_json: bool = True,
    save_flat_csv: bool = True,
) -> pd.DataFrame:
    """Ingest fundamentals for a resolved ticker universe."""

    settings = settings or get_settings()
    from dumb_money.universe import resolve_ticker_universe

    resolved_tickers = resolve_ticker_universe(
        tickers=tickers,
        ticker_query_sql=ticker_query_sql,
        settings=settings,
    )
    return ingest_fundamentals(
        resolved_tickers,
        as_of_date=as_of_date,
        settings=settings,
        save_raw_json=save_raw_json,
        save_flat_csv=save_flat_csv,
        provider=provider,
    )


def ingest_benchmark_member_fundamentals(
    benchmark_ticker: str,
    *,
    as_of_date: date | str | None = None,
    settings: AppSettings | None = None,
    provider: DataSource | str = DataSource.YFINANCE,
    save_raw_json: bool = True,
    save_flat_csv: bool = True,
) -> pd.DataFrame:
    """Ingest fundamentals for the real-security members of a staged benchmark."""

    settings = settings or get_settings()
    from dumb_money.universe import build_benchmark_member_ticker_sql

    return ingest_selected_fundamentals(
        ticker_query_sql=build_benchmark_member_ticker_sql(benchmark_ticker),
        as_of_date=as_of_date,
        settings=settings,
        provider=provider,
        save_raw_json=save_raw_json,
        save_flat_csv=save_flat_csv,
    )
