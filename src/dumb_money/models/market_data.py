"""Canonical price and fundamentals schemas."""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class DataSource(StrEnum):
    YAHOOQUERY = "yahooquery"
    YFINANCE = "yfinance"
    MANUAL = "manual"


class PriceBar(BaseModel):
    """Normalized daily or periodic OHLCV price record."""

    model_config = ConfigDict(str_strip_whitespace=True)

    ticker: str = Field(min_length=1)
    date: date
    interval: str = "1d"
    source: DataSource = DataSource.YFINANCE
    currency: str = "USD"
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int = Field(ge=0)

    @field_validator("ticker", "currency")
    @classmethod
    def uppercase_identifiers(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def validate_ohlc_range(self) -> "PriceBar":
        if self.low > self.high:
            raise ValueError("low cannot be greater than high")
        if self.open < 0 or self.high < 0 or self.low < 0 or self.close < 0 or self.adj_close < 0:
            raise ValueError("price fields must be non-negative")
        return self


class FundamentalSnapshot(BaseModel):
    """Normalized period-aware fundamentals used by research modules."""

    model_config = ConfigDict(str_strip_whitespace=True)

    ticker: str = Field(min_length=1)
    as_of_date: date
    period_end_date: date | None = None
    report_date: date | None = None
    fiscal_year: int | None = None
    fiscal_quarter: int | None = Field(default=None, ge=1, le=4)
    fiscal_period: str | None = None
    period_type: str | None = None
    source: DataSource = DataSource.YFINANCE
    currency: str = "USD"
    long_name: str | None = None
    sector: str | None = None
    industry: str | None = None
    website: str | None = None
    market_cap: float | None = None
    enterprise_value: float | None = None
    revenue: float | None = None
    revenue_ttm: float | None = None
    gross_profit: float | None = None
    operating_income: float | None = None
    net_income: float | None = None
    ebitda: float | None = None
    free_cash_flow: float | None = None
    total_debt: float | None = None
    total_cash: float | None = None
    current_assets: float | None = None
    current_liabilities: float | None = None
    shares_outstanding: float | None = None
    eps_trailing: float | None = None
    eps_forward: float | None = None
    gross_margin: float | None = None
    operating_margin: float | None = None
    profit_margin: float | None = None
    return_on_equity: float | None = None
    return_on_assets: float | None = None
    debt_to_equity: float | None = None
    current_ratio: float | None = None
    trailing_pe: float | None = None
    forward_pe: float | None = None
    price_to_sales: float | None = None
    ev_to_ebitda: float | None = None
    dividend_yield: float | None = None
    raw_payload_path: str | None = None
    pulled_at: datetime | None = None

    @field_validator("ticker", "currency")
    @classmethod
    def uppercase_identifiers(cls, value: str) -> str:
        return value.upper()

    @field_validator("fiscal_period")
    @classmethod
    def normalize_fiscal_period(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized.upper() if normalized.lower() in {"q1", "q2", "q3", "q4", "fy", "ttm"} else normalized

    @field_validator("period_type")
    @classmethod
    def normalize_period_type(cls, value: str | None) -> str | None:
        return value.strip().lower() if value is not None else None
