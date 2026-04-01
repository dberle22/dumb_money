"""Listed-security ingestion helpers for building the shared research universe."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings, get_settings


def _load_symbol_directory(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    rows: list[list[str]] = []
    header: list[str] | None = None

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("File Creation Time"):
            continue

        parts = line.split("|")
        if parts and parts[-1] == "":
            parts = parts[:-1]

        if header is None:
            header = parts
            continue

        if len(parts) != len(header):
            raise ValueError(f"unexpected column count in symbol directory row for {path}")
        rows.append(parts)

    if header is None:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=header)


def load_nasdaq_listed_frame(path: str | Path) -> pd.DataFrame:
    """Load a Nasdaq Trader `nasdaqlisted.txt` file into a DataFrame."""

    return _load_symbol_directory(path)


def load_other_listed_frame(path: str | Path) -> pd.DataFrame:
    """Load a Nasdaq Trader `otherlisted.txt` file into a DataFrame."""

    return _load_symbol_directory(path)


def _build_universe_filename(label: str, as_of_date: date | str | None, suffix: str = ".txt") -> str:
    clean_date = str(as_of_date or date.today()).replace("-", "")
    return f"{label}_{clean_date}{suffix}"


def ingest_listed_security_sources(
    *,
    nasdaq_listed_path: str | Path,
    other_listed_path: str | Path,
    settings: AppSettings | None = None,
    as_of_date: date | str | None = None,
) -> dict[str, Path]:
    """Copy listed-security source files into the project's raw universe directory."""

    settings = settings or get_settings()
    settings.ensure_directories()

    destinations = {
        "nasdaq_listed": settings.raw_universe_dir / _build_universe_filename("nasdaqlisted", as_of_date),
        "other_listed": settings.raw_universe_dir / _build_universe_filename("otherlisted", as_of_date),
    }

    shutil.copyfile(nasdaq_listed_path, destinations["nasdaq_listed"])
    shutil.copyfile(other_listed_path, destinations["other_listed"])
    return destinations
