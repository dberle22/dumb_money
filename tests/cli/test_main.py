from datetime import date
import importlib

cli_main_module = importlib.import_module("dumb_money.cli.main")


def test_prices_command_dispatches_to_ingestion(monkeypatch) -> None:
    calls: dict[str, object] = {}

    def fake_ingest_prices(tickers, *, start_date, end_date, interval) -> None:
        calls["tickers"] = tickers
        calls["start_date"] = start_date
        calls["end_date"] = end_date
        calls["interval"] = interval

    monkeypatch.setattr(cli_main_module, "ingest_prices", fake_ingest_prices)

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
