from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")

from dumb_money.analytics.company import calculate_risk_metrics, calculate_trend_metrics, prepare_price_history
from dumb_money.analytics.scorecard import CompanyScorecard
from dumb_money.config import AppSettings
from dumb_money.outputs import (
    build_risk_metric_table,
    build_trend_risk_profile_section_data,
    build_trend_risk_profile_strip_table,
    build_trend_risk_profile_table,
    render_trend_risk_profile_section,
    save_trend_risk_profile_section,
)
from dumb_money.research.company import CompanyResearchPacket
from dumb_money.storage import write_canonical_table
from dumb_money.transforms.prices import stage_prices


def _price_frame(ticker: str, dates: pd.DatetimeIndex, base_price: float, slope: float) -> pd.DataFrame:
    values = [base_price + slope * index + ((index % 9) - 4) * 0.9 for index in range(len(dates))]
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


def _build_packet() -> CompanyResearchPacket:
    dates = pd.bdate_range("2024-01-02", periods=320)
    combined_prices = pd.concat(
        [
            _price_frame("TEST", dates, base_price=100.0, slope=0.45),
            _price_frame("SPY", dates, base_price=95.0, slope=0.20),
            _price_frame("QQQ", dates, base_price=98.0, slope=0.30),
        ],
        ignore_index=True,
    )
    company_history = prepare_price_history(combined_prices, "TEST")
    benchmark_histories = {
        "SPY": prepare_price_history(combined_prices, "SPY"),
        "QQQ": prepare_price_history(combined_prices, "QQQ"),
    }
    scorecard = CompanyScorecard(
        summary={
            "ticker": "TEST",
            "company_name": "Test Co",
            "primary_benchmark": "SPY",
            "secondary_benchmark": "QQQ",
        },
        category_scores=pd.DataFrame(),
        metrics=pd.DataFrame(),
    )
    return CompanyResearchPacket(
        ticker="TEST",
        company_name="Test Co",
        as_of_date="2026-04-05",
        company_history=company_history,
        benchmark_histories=benchmark_histories,
        return_windows=pd.DataFrame(),
        trailing_return_comparison=pd.DataFrame(),
        risk_metrics=calculate_risk_metrics(company_history, benchmark_history=benchmark_histories["SPY"]),
        trend_metrics=calculate_trend_metrics(company_history),
        benchmark_comparison=pd.DataFrame(),
        fundamentals_summary={},
        peer_return_comparison=pd.DataFrame(),
        peer_return_summary_stats={},
        peer_valuation_comparison=pd.DataFrame(),
        peer_summary_stats={},
        sector_snapshot={},
        scorecard=scorecard,
    )


def test_trend_risk_profile_section_queries_duckdb_and_saves(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.ensure_directories()

    dates = pd.bdate_range("2024-01-02", periods=320)

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

    data = build_trend_risk_profile_section_data("AAPL", benchmark_set_id="sample_universe", settings=settings)

    assert data.primary_benchmark == "SPY"
    assert data.secondary_benchmark == "QQQ"
    assert "Trend Structure" in data.summary_table["Metric"].tolist()
    assert "Beta vs Primary Benchmark" in data.summary_table["Metric"].tolist()
    assert data.summary_table.loc[data.summary_table["Metric"] == "Beta vs Primary Benchmark", "Value"].iloc[0] != "N/A"
    assert data.summary_table.loc[data.summary_table["Metric"] == "Downside Volatility", "Value"].iloc[0] != "N/A"
    assert "Max Drawdown (1Y)" in data.summary_strip["signal"].tolist()
    assert "Beta vs Benchmark" in data.summary_strip["signal"].tolist()

    table = build_trend_risk_profile_table(data)
    strip = build_trend_risk_profile_strip_table(data)
    assert table.columns.tolist() == ["Metric", "Value", "Assessment", "Shared Input"]
    assert strip.columns.tolist() == ["signal", "band_score", "band_score_display", "value_display", "assessment"]

    figure = render_trend_risk_profile_section(data)
    assert len(figure.axes) == 4
    figure.clf()

    artifacts = save_trend_risk_profile_section(
        "AAPL",
        output_dir=Path(tmp_path) / "reports",
        benchmark_set_id="sample_universe",
        settings=settings,
    )
    assert artifacts["figure_path"].exists()
    assert artifacts["table_path"].exists()
    assert artifacts["strip_path"].exists()
    assert artifacts["text_path"].exists()


def test_company_report_risk_table_delegates_to_trend_risk_section() -> None:
    packet = _build_packet()

    table = build_risk_metric_table(packet)

    assert table.columns.tolist() == ["label", "value"]
    assert "Current Drawdown" in table["label"].tolist()
    assert "Beta vs Primary Benchmark" in table["label"].tolist()
