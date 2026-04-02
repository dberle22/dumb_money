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


def test_stage_prices_incrementally_preserves_existing_rows(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    initial_path = tmp_path / "initial_prices.csv"
    initial = pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": ["2024-01-02"],
            "interval": ["1d"],
            "source": ["yfinance"],
            "currency": ["USD"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "adj_close": [100.2],
            "volume": [1000],
        }
    )
    initial.to_csv(initial_path, index=False)
    stage_prices(input_paths=[initial_path], settings=settings)

    incoming_path = tmp_path / "incoming_prices.csv"
    incoming = pd.DataFrame(
        {
            "ticker": ["MSFT"],
            "date": ["2024-01-03"],
            "interval": ["1d"],
            "source": ["yfinance"],
            "currency": ["USD"],
            "open": [200.0],
            "high": [201.0],
            "low": [199.0],
            "close": [200.5],
            "adj_close": [200.2],
            "volume": [2000],
        }
    )
    incoming.to_csv(incoming_path, index=False)

    frame = stage_prices(input_paths=[incoming_path], settings=settings)

    assert sorted(frame["ticker"].tolist()) == ["AAPL", "MSFT"]


def test_stage_prices_prefers_individual_raw_files_over_combined_snapshots(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    combined = settings.raw_prices_dir / "combined_prices_20240101_20240102_1d.csv"
    combined.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": ["2024-01-02"],
            "interval": ["1d"],
            "source": ["yfinance"],
            "currency": ["USD"],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "adj_close": [10.4],
            "volume": [100],
        }
    ).to_csv(combined, index=False)
    pd.DataFrame(
        {
            "ticker": ["MSFT"],
            "date": ["2024-01-03"],
            "interval": ["1d"],
            "source": ["yfinance"],
            "currency": ["USD"],
            "open": [20.0],
            "high": [21.0],
            "low": [19.0],
            "close": [20.5],
            "adj_close": [20.4],
            "volume": [200],
        }
    ).to_csv(settings.raw_prices_dir / "msft_20240101_20240103_1d.csv", index=False)

    frame = stage_prices(settings=settings)

    assert frame["ticker"].tolist() == ["MSFT"]
