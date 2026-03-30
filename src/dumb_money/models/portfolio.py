"""Portfolio and holdings schemas."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Holding(BaseModel):
    """Point-in-time holding for portfolio-fit workflows."""

    model_config = ConfigDict(str_strip_whitespace=True)

    portfolio_id: str = "default"
    ticker: str = Field(min_length=1)
    as_of_date: date
    quantity: float = Field(ge=0)
    average_cost: float | None = Field(default=None, ge=0)
    market_value: float | None = Field(default=None, ge=0)
    weight: float | None = Field(default=None, ge=0, le=1)
    account_name: str | None = None
    notes: str | None = None

    @field_validator("ticker")
    @classmethod
    def uppercase_ticker(cls, value: str) -> str:
        return value.upper()
