from datetime import date
import importlib

cli_main_module = importlib.import_module("dumb_money.cli.main")


def test_prices_command_dispatches_to_ingestion(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_ingest_selected_prices(*, tickers=None, ticker_query_sql=None, start_date, end_date, interval) -> None:
        calls["tickers"] = tickers
        calls["ticker_query_sql"] = ticker_query_sql
        calls["start_date"] = start_date
        calls["end_date"] = end_date
        calls["interval"] = interval

    monkeypatch.setattr(cli_main_module, "ingest_selected_prices", fake_ingest_selected_prices)

    exit_code = cli_main_module.main(
        [
            "prices",
            "--tickers",
            "AAPL,MSFT",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-31",
        ]
    )

    assert exit_code == 0
    assert calls == {
        "tickers": ["AAPL", "MSFT"],
        "ticker_query_sql": None,
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 31),
        "interval": "1d",
    }


def test_prices_command_accepts_ticker_query_sql(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_ingest_selected_prices(*, tickers=None, ticker_query_sql=None, start_date, end_date, interval) -> None:
        calls["tickers"] = tickers
        calls["ticker_query_sql"] = ticker_query_sql
        calls["start_date"] = start_date
        calls["end_date"] = end_date
        calls["interval"] = interval

    monkeypatch.setattr(cli_main_module, "ingest_selected_prices", fake_ingest_selected_prices)

    exit_code = cli_main_module.main(
        [
            "prices",
            "--ticker-query-sql",
            "select 'AAPL' as ticker",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-01-31",
        ]
    )

    assert exit_code == 0
    assert calls == {
        "tickers": None,
        "ticker_query_sql": "select 'AAPL' as ticker",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 1, 31),
        "interval": "1d",
    }


def test_benchmarks_command_can_run_definitions_only(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_ingest_benchmark_definitions(definitions, *, as_of_date, label) -> None:
        calls["definitions"] = definitions
        calls["as_of_date"] = as_of_date
        calls["label"] = label

    monkeypatch.setattr(cli_main_module, "ingest_benchmark_definitions", fake_ingest_benchmark_definitions)

    exit_code = cli_main_module.main(
        [
            "benchmarks",
            "--tickers",
            "SPY,QQQ",
            "--definitions-only",
            "--label",
            "core",
            "--as-of-date",
            "2024-07-01",
        ]
    )

    assert exit_code == 0
    assert calls == {
        "definitions": ["SPY", "QQQ"],
        "as_of_date": date(2024, 7, 1),
        "label": "core",
    }


def test_fundamentals_command_accepts_ticker_query_sql(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_ingest_selected_fundamentals(*, tickers=None, ticker_query_sql=None, as_of_date=None) -> None:
        calls["tickers"] = tickers
        calls["ticker_query_sql"] = ticker_query_sql
        calls["as_of_date"] = as_of_date

    monkeypatch.setattr(
        cli_main_module,
        "ingest_selected_fundamentals",
        fake_ingest_selected_fundamentals,
    )

    exit_code = cli_main_module.main(
        [
            "fundamentals",
            "--ticker-query-sql",
            "select 'MSFT' as ticker",
            "--as-of-date",
            "2024-07-01",
        ]
    )

    assert exit_code == 0
    assert calls == {
        "tickers": None,
        "ticker_query_sql": "select 'MSFT' as ticker",
        "as_of_date": date(2024, 7, 1),
    }


def test_universe_command_dispatches_to_ingestion(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_ingest_listed_security_sources(*, nasdaq_listed_path, other_listed_path, as_of_date) -> None:
        calls["nasdaq_listed_path"] = nasdaq_listed_path
        calls["other_listed_path"] = other_listed_path
        calls["as_of_date"] = as_of_date

    monkeypatch.setattr(cli_main_module, "ingest_listed_security_sources", fake_ingest_listed_security_sources)

    exit_code = cli_main_module.main(
        [
            "universe",
            "--nasdaq-listed-path",
            "nasdaqlisted.txt",
            "--other-listed-path",
            "otherlisted.txt",
            "--as-of-date",
            "2026-03-31",
        ]
    )

    assert exit_code == 0
    assert calls == {
        "nasdaq_listed_path": "nasdaqlisted.txt",
        "other_listed_path": "otherlisted.txt",
        "as_of_date": date(2026, 3, 31),
    }
