from datetime import date

import pandas as pd

from dumb_money.ingestion.prices import (
    normalize_price_history_frame,
    normalize_tickers,
    to_price_models,
    to_yahoo_symbol,
)


def test_normalize_tickers_deduplicates_and_uppercases() -> None:
    assert normalize_tickers([" aapl ", "MSFT", "aapl", "", " msft "]) == ["AAPL", "MSFT"]


def test_to_yahoo_symbol_maps_dotted_share_classes() -> None:
    assert to_yahoo_symbol("UHAL.B") == "UHAL-B"
    assert to_yahoo_symbol("BRK.B") == "BRK-B"
    assert to_yahoo_symbol("BF.B") == "BF-B"
    assert to_yahoo_symbol("AAPL") == "AAPL"


def test_normalize_price_history_frame_maps_provider_columns() -> None:
    raw = pd.DataFrame(
        {
            "date": ["2024-01-03", "2024-01-02"],
            "open": [101.0, 100.0],
            "high": [103.0, 102.0],
            "low": [99.5, 98.5],
            "close": [102.0, 101.0],
            "adjclose": [101.8, 100.8],
            "volume": [1200, 1000],
        }
    )

    normalized = normalize_price_history_frame(raw, "aapl", source="yahooquery", interval="1d")

    assert normalized.columns.tolist() == [
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
    assert normalized["date"].tolist() == [date(2024, 1, 2), date(2024, 1, 3)]
    assert normalized["ticker"].unique().tolist() == ["AAPL"]
    assert len(to_price_models(normalized)) == 2


def test_normalize_price_history_frame_fills_adj_close_from_close() -> None:
    raw = pd.DataFrame(
        {
            "Date": ["2024-01-02"],
            "Open": [10.0],
            "High": [11.0],
            "Low": [9.5],
            "Close": [10.5],
            "Volume": [100],
        }
    )

    normalized = normalize_price_history_frame(raw, "spy", source="yfinance")

    assert normalized.loc[0, "adj_close"] == 10.5


def test_normalize_price_history_frame_handles_yfinance_multiindex_columns() -> None:
    raw = pd.DataFrame(
        {
            ("Date", ""): ["2024-01-02"],
            ("Open", "AAPL"): [10.0],
            ("High", "AAPL"): [11.0],
            ("Low", "AAPL"): [9.5],
            ("Close", "AAPL"): [10.5],
            ("Adj Close", "AAPL"): [10.4],
            ("Volume", "AAPL"): [100],
        }
    )

    normalized = normalize_price_history_frame(raw, "aapl", source="yfinance")

    assert normalized.loc[0, "ticker"] == "AAPL"
    assert normalized.loc[0, "date"] == date(2024, 1, 2)
