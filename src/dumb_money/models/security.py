"""Security and benchmark data contracts."""

from __future__ import annotations

from datetime import date
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class AssetType(StrEnum):
    COMMON_STOCK = "common_stock"
    ETF = "etf"
    INDEX = "index"
    ADR = "adr"
    MUTUAL_FUND = "mutual_fund"
    CRYPTO = "crypto"
    CASH = "cash"
    OTHER = "other"


class Security(BaseModel):
    """Shared security master record for research workflows and universe management."""

    model_config = ConfigDict(str_strip_whitespace=True)

    security_id: str | None = None
    ticker: str = Field(min_length=1)
    name: str | None = None
    asset_type: AssetType = AssetType.COMMON_STOCK
    exchange: str | None = None
    primary_listing: str | None = None
    currency: str = "USD"
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    cik: str | None = None
    is_benchmark: bool = False
    is_active: bool = True
    is_eligible_research_universe: bool = False
    source: str | None = None
    source_id: str | None = None
    first_seen_at: date | None = None
    last_updated_at: date | None = None
    notes: str | None = None

    @field_validator("ticker", "currency")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        return value.upper()


class BenchmarkCategory(StrEnum):
    MARKET = "market"
    SECTOR = "sector"
    INDUSTRY = "industry"
    STYLE = "style"
    CUSTOM = "custom"


class BenchmarkDefinition(BaseModel):
    """Definition for a benchmark or benchmark set member."""

    model_config = ConfigDict(str_strip_whitespace=True)

    benchmark_id: str = Field(min_length=1)
    ticker: str = Field(min_length=1)
    name: str
    category: BenchmarkCategory = BenchmarkCategory.MARKET
    scope: str | None = None
    currency: str = "USD"
    inception_date: date | None = None
    description: str | None = None

    @field_validator("benchmark_id", "ticker", "currency")
    @classmethod
    def uppercase_identifiers(cls, value: str) -> str:
        return value.upper()
