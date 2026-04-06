from __future__ import annotations

from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.outputs import (
    build_market_performance_section_data,
    build_market_performance_table,
    render_market_performance_section,
    save_market_performance_section,
)
from dumb_money.storage import GOLD_TICKER_METRICS_MART_COLUMNS, write_canonical_table
from dumb_money.transforms.prices import stage_prices


def _price_frame(ticker: str, dates: pd.DatetimeIndex, base_price: float, slope: float) -> pd.DataFrame:
    values = [base_price + slope * index for index in range(len(dates))]
    return pd.DataFrame(
        {
            "ticker": ticker,
            "date": dates.date,
            "interval": "1d",
            "source": "yfinance",
            "currency": "USD",
            "open": values,
            "high": [value * 1.01 for value in values],
            "low": [value * 0.99 for value in values],
            "close": values,
            "adj_close": values,
            "volume": [1_000_000] * len(values),
        }
    )


def test_market_performance_section_queries_duckdb_and_saves(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()

    dates = pd.bdate_range("2024-01-02", periods=320)

    # Only the company is written to canonical DuckDB prices here. The shared price
    # staging flow should materialize raw benchmark files before the section runs.
    aapl_prices = _price_frame("AAPL", dates, base_price=100.0, slope=0.4)
    write_canonical_table(aapl_prices, "normalized_prices", settings=settings)

    benchmark_prices = pd.concat(
        [
            _price_frame("SPY", dates, base_price=95.0, slope=0.22),
            _price_frame("QQQ", dates, base_price=98.0, slope=0.3),
        ],
        ignore_index=True,
    )
    benchmark_prices.to_csv(
        settings.raw_benchmarks_dir / "sample_universe_benchmark_prices_20240102_20250331_1d.csv",
        index=False,
    )
    pd.DataFrame(
        {
            "benchmark_id": ["SPY", "QQQ"],
            "ticker": ["SPY", "QQQ"],
            "name": ["SPDR S&P 500 ETF Trust", "Invesco QQQ Trust"],
            "category": ["market", "style"],
            "scope": ["us_large_cap", "us_large_cap_growth"],
            "currency": ["USD", "USD"],
            "inception_date": ["1993-01-22", "1999-03-10"],
            "description": ["Core US large-cap equity benchmark ETF.", "Nasdaq-100 benchmark ETF."],
        }
    ).to_csv(
        settings.raw_benchmarks_dir / "sample_universe_benchmark_definitions_20250331.csv",
        index=False,
    )

    write_canonical_table(
        pd.DataFrame(
            {
                "mapping_id": ["benchmark_mapping::AAPL"],
                "ticker": ["AAPL"],
                "sector": ["Technology"],
                "industry": ["Consumer Electronics"],
                "primary_benchmark": ["SPY"],
                "sector_benchmark": ["XLK"],
                "industry_benchmark": [None],
                "style_benchmark": [None],
                "custom_benchmark": [None],
                "assignment_method": ["test_fixture"],
                "priority": [1],
                "is_active": [True],
                "notes": [None],
            }
        ),
        "benchmark_mappings",
        settings=settings,
    )

    stage_prices(settings=settings)

    data = build_market_performance_section_data("AAPL", benchmark_set_id="sample_universe", settings=settings)

    assert data.primary_benchmark == "SPY"
    assert data.secondary_benchmark == "QQQ"
    assert set(["AAPL", "SPY", "QQQ"]).issubset(data.histories)

    table = build_market_performance_table(data)
    assert table["Window"].tolist() == ["1m", "3m", "6m", "1y"]
    assert table.iloc[-1]["AAPL"] != "N/A"
    assert table.iloc[-1]["SPY"] != "N/A"
    assert table.iloc[-1]["QQQ"] != "N/A"

    figure = render_market_performance_section(data)
    assert len(figure.axes) == 3
    figure.clf()

    artifacts = save_market_performance_section(
        "AAPL",
        output_dir=Path(tmp_path) / "reports",
        benchmark_set_id="sample_universe",
        settings=settings,
    )
    assert artifacts["figure_path"].exists()
    assert artifacts["table_path"].exists()


def test_market_performance_section_prefers_gold_mart_benchmark_choices(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()

    dates = pd.bdate_range("2024-01-02", periods=320)
    prices = pd.concat(
        [
            _price_frame("AAPL", dates, base_price=100.0, slope=0.4),
            _price_frame("SPY", dates, base_price=95.0, slope=0.22),
            _price_frame("QQQ", dates, base_price=98.0, slope=0.3),
        ],
        ignore_index=True,
    )
    write_canonical_table(prices, "normalized_prices", settings=settings)
    write_canonical_table(
        pd.DataFrame(
            {
                "set_id": ["sample_universe", "sample_universe"],
                "benchmark_id": ["SPY", "QQQ"],
                "ticker": ["SPY", "QQQ"],
                "name": ["SPDR S&P 500 ETF Trust", "Invesco QQQ Trust"],
                "category": ["market", "style"],
                "scope": ["us_large_cap", "us_large_cap_growth"],
                "currency": ["USD", "USD"],
                "description": ["Core US benchmark", "Nasdaq benchmark"],
                "member_order": [1, 2],
                "is_default": [True, True],
            }
        ),
        "benchmark_sets",
        settings=settings,
    )
    write_canonical_table(
        pd.DataFrame(
            {
                "mapping_id": ["benchmark_mapping::AAPL"],
                "ticker": ["AAPL"],
                "sector": ["Technology"],
                "industry": ["Consumer Electronics"],
                "primary_benchmark": ["SPY"],
                "sector_benchmark": ["SPY"],
                "industry_benchmark": [None],
                "style_benchmark": [None],
                "custom_benchmark": [None],
                "assignment_method": ["fixture"],
                "priority": [1],
                "is_active": [True],
                "notes": [None],
            }
        ),
        "benchmark_mappings",
        settings=settings,
    )

    mart_row = {column: None for column in GOLD_TICKER_METRICS_MART_COLUMNS}
    mart_row.update(
        {
            "mart_id": "gold_ticker_metrics::AAPL::2024-12-31",
            "ticker": "AAPL",
            "primary_benchmark": "QQQ",
            "secondary_benchmark": "SPY",
        }
    )
    write_canonical_table(pd.DataFrame([mart_row]), "gold_ticker_metrics_mart", settings=settings)

    data = build_market_performance_section_data("AAPL", benchmark_set_id="sample_universe", settings=settings)

    assert data.primary_benchmark == "QQQ"
    assert data.secondary_benchmark == "SPY"
