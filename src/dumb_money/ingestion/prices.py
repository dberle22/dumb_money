"""Price ingestion helpers built around a normalized OHLCV contract."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date
from pathlib import Path

import pandas as pd

from dumb_money.config.settings import AppSettings, get_settings
from dumb_money.models import DataSource, PriceBar

PRICE_COLUMNS = [
    "ticker",
    "date",
    "interval",
    "source",
    "currency",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
]


def _flatten_price_column_name(column: object) -> str:
    if isinstance(column, tuple):
        parts = [str(part).strip() for part in column if str(part).strip()]
        if not parts:
            return ""
        if parts[0].lower() == "date":
            return "Date"
        return parts[0]
    return str(column)


def normalize_tickers(tickers: Iterable[str]) -> list[str]:
    """Deduplicate and normalize ticker input while preserving order."""

    normalized: list[str] = []
    seen: set[str] = set()
    for ticker in tickers:
        clean = str(ticker).strip().upper()
        if clean and clean not in seen:
            normalized.append(clean)
            seen.add(clean)
    return normalized


def build_price_filename(label: str, start_date: date | str, end_date: date | str, interval: str) -> str:
    clean_start = str(start_date).replace("-", "")
    clean_end = str(end_date).replace("-", "")
    return f"{label.lower()}_{clean_start}_{clean_end}_{interval}.csv"


def normalize_price_history_frame(
    frame: pd.DataFrame,
    ticker: str,
    *,
    source: DataSource | str,
    interval: str = "1d",
    currency: str = "USD",
) -> pd.DataFrame:
    """Map provider-specific price history columns to the canonical schema."""

    if frame.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    normalized = frame.copy()
    normalized.columns = [_flatten_price_column_name(column) for column in normalized.columns]

    if "symbol" in normalized.columns and "ticker" not in normalized.columns:
        normalized["ticker"] = normalized["symbol"]
    normalized["ticker"] = ticker.upper()

    column_mapping = {
        "date": "date",
        "Date": "date",
        "open": "open",
        "Open": "open",
        "high": "high",
        "High": "high",
        "low": "low",
        "Low": "low",
        "close": "close",
        "Close": "close",
        "adjclose": "adj_close",
        "Adj Close": "adj_close",
        "adj_close": "adj_close",
        "volume": "volume",
        "Volume": "volume",
    }
    normalized = normalized.rename(columns=column_mapping)

    if "date" not in normalized.columns:
        raise ValueError("price history frame must include a date column")

    if "adj_close" not in normalized.columns and "close" in normalized.columns:
        normalized["adj_close"] = normalized["close"]

    normalized["date"] = pd.to_datetime(normalized["date"]).dt.date
    normalized["interval"] = interval
    normalized["source"] = str(source)
    normalized["currency"] = currency.upper()

    missing = [
        column
        for column in ("open", "high", "low", "close", "adj_close", "volume")
        if column not in normalized.columns
    ]
    if missing:
        raise ValueError(f"price history frame is missing required columns: {missing}")

    normalized = normalized[PRICE_COLUMNS].sort_values(["ticker", "date"]).reset_index(drop=True)
    normalized["volume"] = normalized["volume"].fillna(0).astype(int)
    return normalized


def to_price_models(frame: pd.DataFrame) -> list[PriceBar]:
    """Convert a normalized DataFrame into validated model instances."""

    return [PriceBar.model_validate(record) for record in frame.to_dict(orient="records")]


def download_prices_yahooquery(
    ticker: str,
    *,
    start_date: date | str,
    end_date: date | str,
    interval: str = "1d",
) -> pd.DataFrame:
    from yahooquery import Ticker

    ticker_client = Ticker(ticker)
    history = ticker_client.history(start=str(start_date), end=str(end_date), interval=interval)
    if history is None or history.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)
    history = history.reset_index()
    return normalize_price_history_frame(
        history,
        ticker,
        source=DataSource.YAHOOQUERY,
        interval=interval,
    )


def download_prices_yfinance(
    ticker: str,
    *,
    start_date: date | str,
    end_date: date | str,
    interval: str = "1d",
) -> pd.DataFrame:
    import yfinance as yf

    history = yf.download(
        ticker,
        start=str(start_date),
        end=str(end_date),
        interval=interval,
        progress=False,
        auto_adjust=False,
    )
    if history is None or history.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)
    history = history.reset_index()
    return normalize_price_history_frame(
        history,
        ticker,
        source=DataSource.YFINANCE,
        interval=interval,
    )


def fetch_prices(
    tickers: Sequence[str],
    *,
    start_date: date | str,
    end_date: date | str,
    interval: str = "1d",
    use_yfinance_fallback: bool = True,
) -> pd.DataFrame:
    """Fetch price history for one or more tickers using Yahoo providers."""

    frames: list[pd.DataFrame] = []
    for ticker in normalize_tickers(tickers):
        try:
            frame = download_prices_yahooquery(
                ticker,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
            )
            if frame.empty and use_yfinance_fallback:
                frame = download_prices_yfinance(
                    ticker,
                    start_date=start_date,
                    end_date=end_date,
                    interval=interval,
                )
            if not frame.empty:
                frames.append(frame)
        except Exception:
            if not use_yfinance_fallback:
                raise
            fallback = download_prices_yfinance(
                ticker,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
            )
            if not fallback.empty:
                frames.append(fallback)

    if not frames:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    return pd.concat(frames, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)


def save_price_frame(
    frame: pd.DataFrame,
    *,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return output_path


def ingest_prices(
    tickers: Sequence[str],
    *,
    start_date: date | str,
    end_date: date | str,
    interval: str = "1d",
    settings: AppSettings | None = None,
    save_individual: bool = True,
    save_combined: bool = True,
) -> pd.DataFrame:
    """Download prices and persist raw CSV extracts using repo conventions."""

    settings = settings or get_settings()
    settings.ensure_directories()

    combined = fetch_prices(
        tickers,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
    )

    if combined.empty:
        return combined

    if save_individual:
        for ticker, frame in combined.groupby("ticker", sort=True):
            save_price_frame(
                frame,
                output_path=settings.raw_prices_dir
                / build_price_filename(ticker, start_date, end_date, interval),
            )

    if save_combined:
        save_price_frame(
            combined,
            output_path=settings.raw_prices_dir
            / build_price_filename("combined_prices", start_date, end_date, interval),
        )

    return combined


def ingest_selected_prices(
    *,
    start_date: date | str,
    end_date: date | str,
    tickers: Sequence[str] | None = None,
    ticker_query_sql: str | None = None,
    interval: str = "1d",
    settings: AppSettings | None = None,
    save_individual: bool = True,
    save_combined: bool = True,
) -> pd.DataFrame:
    """Ingest prices for a resolved ticker universe."""

    settings = settings or get_settings()
    from dumb_money.universe import resolve_ticker_universe

    resolved_tickers = resolve_ticker_universe(
        tickers=tickers,
        ticker_query_sql=ticker_query_sql,
        settings=settings,
    )
    return ingest_prices(
        resolved_tickers,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        settings=settings,
        save_individual=save_individual,
        save_combined=save_combined,
    )


def ingest_benchmark_member_prices(
    benchmark_ticker: str,
    *,
    start_date: date | str,
    end_date: date | str,
    interval: str = "1d",
    settings: AppSettings | None = None,
    save_individual: bool = True,
    save_combined: bool = True,
) -> pd.DataFrame:
    """Ingest prices for the real-security members of a staged benchmark."""

    settings = settings or get_settings()
    from dumb_money.universe import build_benchmark_member_ticker_sql

    return ingest_selected_prices(
        start_date=start_date,
        end_date=end_date,
        ticker_query_sql=build_benchmark_member_ticker_sql(benchmark_ticker),
        interval=interval,
        settings=settings,
        save_individual=save_individual,
        save_combined=save_combined,
    )
