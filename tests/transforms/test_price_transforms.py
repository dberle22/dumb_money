import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.transforms.prices import normalize_prices_frame, stage_prices


def test_normalize_prices_frame_enforces_adj_close_fallback() -> None:
    raw = pd.DataFrame(
        {
            "ticker": ["aapl"],
            "date": ["2024-01-02"],
            "interval": ["1d"],
            "source": ["YFINANCE"],
            "currency": ["usd"],
            "open": [10],
            "high": [11],
            "low": [9],
            "close": [10.5],
            "volume": [100],
        }
    )

    normalized = normalize_prices_frame(raw)

    assert normalized.loc[0, "ticker"] == "AAPL"
    assert normalized.loc[0, "currency"] == "USD"
    assert normalized.loc[0, "source"] == "yfinance"
    assert normalized.loc[0, "adj_close"] == 10.5


def test_stage_prices_writes_staging_csv(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    raw_path = tmp_path / "raw_prices.csv"
    frame = pd.read_csv("tests/fixtures/prices/aapl_daily.csv")
    frame["ticker"] = "AAPL"
    frame["interval"] = "1d"
    frame["source"] = "yfinance"
    frame["currency"] = "USD"
    frame.to_csv(raw_path, index=False)

    frame = stage_prices(input_paths=[raw_path], settings=settings)

    assert frame["ticker"].tolist() == ["AAPL", "AAPL"]
    assert (settings.normalized_prices_dir / "normalized_prices.csv").exists()
    assert settings.warehouse_path.exists()
