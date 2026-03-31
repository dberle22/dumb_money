"""Shared application settings for local-first data workflows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable


VALID_PRICE_INTERVALS = {
    "1m",
    "2m",
    "5m",
    "15m",
    "30m",
    "60m",
    "90m",
    "1h",
    "1d",
    "5d",
    "1wk",
    "1mo",
    "3mo",
}


@dataclass(slots=True)
class AppSettings:
    """Project-level paths and default ingestion behavior."""

    project_root: Path = field(default_factory=lambda: Path(__file__).resolve().parents[3])
    default_price_interval: str = "1d"
    default_lookback_days: int = 365 * 5
    default_currency: str = "USD"
    default_benchmarks: tuple[str, ...] = ("SPY", "QQQ", "IWM")

    def __post_init__(self) -> None:
        self.project_root = Path(self.project_root).resolve()
        self.validate()

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def marts_dir(self) -> Path:
        return self.data_dir / "marts"

    @property
    def raw_prices_dir(self) -> Path:
        return self.raw_dir / "prices"

    @property
    def raw_fundamentals_dir(self) -> Path:
        return self.raw_dir / "fundamentals"

    @property
    def raw_benchmarks_dir(self) -> Path:
        return self.raw_dir / "benchmarks"

    @property
    def raw_portfolios_dir(self) -> Path:
        return self.raw_dir / "portfolios"

    @property
    def raw_watchlists_dir(self) -> Path:
        return self.raw_dir / "watchlists"

    @property
    def normalized_prices_dir(self) -> Path:
        return self.staging_dir / "normalized_prices"

    @property
    def normalized_fundamentals_dir(self) -> Path:
        return self.staging_dir / "normalized_fundamentals"

    @property
    def security_master_dir(self) -> Path:
        return self.staging_dir / "security_master"

    @property
    def benchmark_sets_dir(self) -> Path:
        return self.staging_dir / "benchmark_sets"

    def validate(self) -> None:
        if self.default_price_interval not in VALID_PRICE_INTERVALS:
            raise ValueError(
                f"default_price_interval must be one of {sorted(VALID_PRICE_INTERVALS)}"
            )
        if self.default_lookback_days <= 0:
            raise ValueError("default_lookback_days must be positive")

    def ensure_directories(self) -> None:
        for path in self.all_directories():
            path.mkdir(parents=True, exist_ok=True)

    def all_directories(self) -> Iterable[Path]:
        return (
            self.data_dir,
            self.raw_dir,
            self.staging_dir,
            self.marts_dir,
            self.raw_prices_dir,
            self.raw_fundamentals_dir,
            self.raw_benchmarks_dir,
            self.raw_portfolios_dir,
            self.raw_watchlists_dir,
            self.normalized_prices_dir,
            self.normalized_fundamentals_dir,
            self.security_master_dir,
            self.benchmark_sets_dir,
        )

    def default_price_window(self) -> tuple[date, date]:
        end_date = date.today()
        start_date = end_date - timedelta(days=self.default_lookback_days)
        return start_date, end_date


_SETTINGS = AppSettings()


def get_settings() -> AppSettings:
    """Return the shared singleton settings instance."""

    return _SETTINGS
