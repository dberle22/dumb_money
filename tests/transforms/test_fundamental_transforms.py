from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.transforms.fundamentals import normalize_fundamentals_frame, stage_fundamentals


def test_normalize_fundamentals_frame_coerces_types() -> None:
    raw = Path("tests/fixtures/fundamentals/aapl_fundamentals_flat_2024-06-30.csv")
    frame = normalize_fundamentals_frame(pd.read_csv(raw))

    assert frame.loc[0, "ticker"] == "AAPL"
    assert frame.loc[0, "currency"] == "USD"
    assert frame.loc[0, "source"] == "yfinance"
    assert frame.loc[0, "market_cap"] == 3000000000.0
    assert frame.loc[0, "period_type"] == "ttm"
    assert frame.loc[0, "fiscal_period"] == "TTM"


def test_normalize_fundamentals_frame_preserves_historical_rows() -> None:
    raw = Path("tests/fixtures/fundamentals/aapl_fundamentals_history_2024-06-30.csv")
    frame = normalize_fundamentals_frame(pd.read_csv(raw))

    assert frame["period_type"].tolist() == ["annual", "quarterly", "ttm"]
    assert frame.loc[frame["period_type"] == "quarterly", "fiscal_quarter"].iloc[0] == 2
    assert frame.loc[frame["period_type"] == "ttm", "revenue_ttm"].iloc[0] == 1100000.0


def test_stage_fundamentals_writes_staging_csv(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    raw_path = tmp_path / "aapl_fundamentals_flat_2024-06-30.csv"
    raw_path.write_text(Path("tests/fixtures/fundamentals/aapl_fundamentals_history_2024-06-30.csv").read_text())

    frame = stage_fundamentals(input_paths=[raw_path], settings=settings)

    assert frame["ticker"].unique().tolist() == ["AAPL"]
    assert sorted(frame["period_type"].tolist()) == ["annual", "quarterly", "ttm"]
    assert (settings.normalized_fundamentals_dir / "normalized_fundamentals.csv").exists()
    assert settings.warehouse_path.exists()
