from datetime import date

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.ingestion.benchmarks import (
    benchmark_definitions_to_frame,
    build_benchmark_definitions_filename,
    default_benchmark_price_definitions,
    ingest_benchmark_definitions,
    ingest_benchmark_prices,
    normalize_benchmark_definitions,
)


def test_normalize_benchmark_definitions_uses_defaults_and_deduplicates() -> None:
    definitions = normalize_benchmark_definitions(["spy", "QQQ", "SPY"])

    assert [item.benchmark_id for item in definitions] == ["SPY", "QQQ"]
    assert definitions[0].name == "SPDR S&P 500 ETF Trust"


def test_benchmark_definitions_to_frame_matches_fixture() -> None:
    expected = pd.read_csv("tests/fixtures/benchmarks/default_benchmarks.csv").fillna("")

    actual = benchmark_definitions_to_frame(["SPY", "QQQ"]).fillna("")

    assert actual.to_dict(orient="records") == expected.to_dict(orient="records")


def test_ingest_benchmark_definitions_writes_expected_csv(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)

    frame = ingest_benchmark_definitions(
        ["SPY"],
        settings=settings,
        as_of_date=date(2024, 7, 1),
        label="core",
    )

    output_path = settings.raw_benchmarks_dir / build_benchmark_definitions_filename(
        "core",
        date(2024, 7, 1),
    )

    assert frame["ticker"].tolist() == ["SPY"]
    assert output_path.exists()


def test_ingest_benchmark_prices_saves_definitions_and_combined_prices(
    monkeypatch,
    tmp_path,
) -> None:
    settings = AppSettings(project_root=tmp_path)
    fixture = pd.read_csv("tests/fixtures/prices/aapl_daily.csv")

    def fake_fetch_prices(*_args, **_kwargs) -> pd.DataFrame:
        normalized = fixture.rename(columns={"adjclose": "adj_close"}).copy()
        normalized["ticker"] = "SPY"
        normalized["interval"] = "1d"
        normalized["source"] = "yfinance"
        normalized["currency"] = "USD"
        normalized["date"] = pd.to_datetime(normalized["date"]).dt.date
        return normalized[
            [
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
        ]

    monkeypatch.setattr("dumb_money.ingestion.benchmarks.fetch_prices", fake_fetch_prices)

    frame = ingest_benchmark_prices(
        tickers=["SPY"],
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        settings=settings,
        label="core",
    )

    definitions_path = settings.raw_benchmarks_dir / "core_benchmark_definitions_20240103.csv"
    combined_path = settings.raw_benchmarks_dir / "core_benchmark_prices_20240102_20240103_1d.csv"
    individual_path = settings.raw_benchmarks_dir / "spy_20240102_20240103_1d.csv"

    assert frame["ticker"].unique().tolist() == ["SPY"]
    assert definitions_path.exists()
    assert combined_path.exists()
    assert individual_path.exists()


def test_default_benchmark_price_definitions_include_mapping_benchmarks(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.raw_benchmark_holdings_dir.mkdir(parents=True, exist_ok=True)
    (settings.raw_benchmark_holdings_dir / "etf_benchmark_mapping.csv").write_text(
        "ticker,name,path,benchmark,sector,industry\n"
        "SPY,State Street SPDR S&P 500 ETF Trust,spy_holdings.xlsx,S&P 500,,\n"
        "XLK,Technology Select Sector SPDR Fund,xlk_holdings.xlsx,,Technology,\n"
        "XSD,SPDR S&P Semiconductor ETF,xsd_holdings.xlsx,,,Semiconductor\n"
    )

    definitions = default_benchmark_price_definitions(settings)

    assert [definition.ticker for definition in definitions] == ["SPY", "XLK", "XSD", "QQQ", "IWM"]
    assert definitions[1].category == "sector"
    assert definitions[2].category == "industry"


def test_ingest_benchmark_prices_defaults_to_mapping_backed_universe(monkeypatch, tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    settings.raw_benchmark_holdings_dir.mkdir(parents=True, exist_ok=True)
    (settings.raw_benchmark_holdings_dir / "etf_benchmark_mapping.csv").write_text(
        "ticker,name,path,benchmark,sector,industry\n"
        "SPY,State Street SPDR S&P 500 ETF Trust,spy_holdings.xlsx,S&P 500,,\n"
        "XLK,Technology Select Sector SPDR Fund,xlk_holdings.xlsx,,Technology,\n"
        "XSD,SPDR S&P Semiconductor ETF,xsd_holdings.xlsx,,,Semiconductor\n"
    )

    captured: dict[str, object] = {}

    def fake_fetch_prices(tickers, **_kwargs) -> pd.DataFrame:
        captured["tickers"] = list(tickers)
        return pd.DataFrame(
            {
                "ticker": ["SPY"],
                "date": [date(2024, 1, 2)],
                "interval": ["1d"],
                "source": ["yfinance"],
                "currency": ["USD"],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "adj_close": [100.4],
                "volume": [1000],
            }
        )

    monkeypatch.setattr("dumb_money.ingestion.benchmarks.fetch_prices", fake_fetch_prices)

    ingest_benchmark_prices(
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        settings=settings,
        save_individual=False,
        save_combined=False,
        save_definitions=False,
    )

    assert captured["tickers"] == ["SPY", "XLK", "XSD", "QQQ", "IWM"]
