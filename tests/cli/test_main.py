from datetime import date
import importlib
from types import SimpleNamespace

import pandas as pd

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


def test_portfolio_import_command_dispatches_to_ingestion(monkeypatch, capsys) -> None:
    calls: dict[str, object] = {}

    def fake_ingest_portfolio_holdings(input_path, *, portfolio_id=None, as_of_date=None):
        calls["input_path"] = input_path
        calls["portfolio_id"] = portfolio_id
        calls["as_of_date"] = as_of_date
        return pd.DataFrame(
            {
                "portfolio_id": ["default"],
                "ticker": ["AAPL"],
            }
        )

    monkeypatch.setattr(cli_main_module, "ingest_portfolio_holdings", fake_ingest_portfolio_holdings)

    exit_code = cli_main_module.main(
        [
            "portfolio-import",
            "--input-path",
            "holdings.csv",
            "--portfolio-id",
            "default",
            "--as-of-date",
            "2024-07-01",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert calls == {
        "input_path": "holdings.csv",
        "portfolio_id": "default",
        "as_of_date": date(2024, 7, 1),
    }
    assert "rows=1" in captured.out


def test_portfolio_summary_command_prints_candidate_fit(monkeypatch, capsys) -> None:
    def fake_build_portfolio_summary(portfolio_id="default", *, candidate_ticker=None):
        assert portfolio_id == "default"
        assert candidate_ticker == "NVDA"
        return {
            "portfolio_id": "default",
            "concentration_metrics": {"holding_count": 3, "top_1_weight": 0.5, "top_3_weight": 1.0},
            "sector_exposure": pd.DataFrame([{"sector": "Technology", "holding_count": 2, "market_value": 8000, "weight": 0.8}]),
            "benchmark_comparison": pd.DataFrame([{"window": "1y", "portfolio_return": 0.15, "SPY_return": 0.12}]),
            "candidate_fit": {
                "candidate_ticker": "NVDA",
                "diversification_role": "adds_to_existing_sector",
                "sector_weight_before": 0.8,
                "industry_weight_before": 0.0,
            },
        }

    monkeypatch.setattr(cli_main_module, "build_portfolio_summary", fake_build_portfolio_summary)

    exit_code = cli_main_module.main(["portfolio-summary", "--portfolio-id", "default", "--candidate-ticker", "NVDA"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "candidate_ticker=NVDA" in captured.out
    assert "diversification_role=adds_to_existing_sector" in captured.out
    assert "portfolio_return" in captured.out


def test_watchlist_summary_command_prints_table(monkeypatch, capsys) -> None:
    def fake_build_watchlist_summary(candidate_tickers, *, portfolio_id="default"):
        assert candidate_tickers == ["NVDA", "AMZN"]
        assert portfolio_id == "default"
        return pd.DataFrame(
            [
                {"candidate_ticker": "NVDA", "diversification_role": "adds_to_existing_sector", "total_score": 84.0},
                {"candidate_ticker": "AMZN", "diversification_role": "new_exposure", "total_score": 78.0},
            ]
        )

    monkeypatch.setattr(cli_main_module, "build_watchlist_summary", fake_build_watchlist_summary)

    exit_code = cli_main_module.main(["watchlist-summary", "--portfolio-id", "default", "--tickers", "NVDA,AMZN"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "candidate_ticker" in captured.out
    assert "NVDA" in captured.out


def test_ingest_basket_command_dispatches_to_shared_workflow(monkeypatch, capsys) -> None:
    calls: dict[str, object] = {}

    def fake_ingest_basket(benchmark_ticker, *, start_date=None, end_date=None, as_of_date=None, interval="1d"):
        calls["benchmark_ticker"] = benchmark_ticker
        calls["start_date"] = start_date
        calls["end_date"] = end_date
        calls["as_of_date"] = as_of_date
        calls["interval"] = interval
        return SimpleNamespace(
            benchmark_ticker=benchmark_ticker,
            target_tickers=["AAPL", "MSFT"],
            unresolved_tickers=["AAPL"],
            skipped_already_ingested_tickers=["MSFT"],
            price_input_paths=["aapl.csv"],
            fundamental_input_paths=["aapl_fundamentals.csv"],
            raw_price_rows=10,
            raw_fundamental_rows=3,
            canonical_price_rows=100,
            canonical_fundamental_rows=20,
            validation_summary=pd.DataFrame(
                [{"target_tickers": 2, "fully_ingested_tickers": 2, "in_security_master": 2, "in_prices": 2, "in_fundamentals": 2}]
            ),
            period_type_summary=pd.DataFrame(
                [{"period_type": "annual", "rows": 1}, {"period_type": "ttm", "rows": 1}]
            ),
        )

    monkeypatch.setattr(cli_main_module, "ingest_basket", fake_ingest_basket)

    exit_code = cli_main_module.main(
        [
            "ingest-basket",
            "--ticker",
            "XBI",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--as-of-date",
            "2024-12-31",
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert calls == {
        "benchmark_ticker": "XBI",
        "start_date": date(2024, 1, 1),
        "end_date": date(2024, 12, 31),
        "as_of_date": date(2024, 12, 31),
        "interval": "1d",
    }
    assert "basket=XBI" in captured.out
    assert "raw_price_rows=10" in captured.out


def test_plan_basket_command_dispatches_to_manifest_workflow(monkeypatch, capsys) -> None:
    calls: dict[str, object] = {}

    def fake_plan_basket_ingestion(
        benchmark_ticker,
        *,
        batch_size,
        start_date=None,
        end_date=None,
        as_of_date=None,
        interval="1d",
    ):
        calls["benchmark_ticker"] = benchmark_ticker
        calls["batch_size"] = batch_size
        calls["start_date"] = start_date
        calls["end_date"] = end_date
        calls["as_of_date"] = as_of_date
        calls["interval"] = interval
        return SimpleNamespace(
            basket_ticker=benchmark_ticker,
            batch_size=batch_size,
            total_target_tickers=200,
            total_unresolved_tickers=150,
            batches=[SimpleNamespace(batch_index=0), SimpleNamespace(batch_index=1)],
        )

    monkeypatch.setattr(cli_main_module, "plan_basket_ingestion", fake_plan_basket_ingestion)

    exit_code = cli_main_module.main(["plan-basket", "--ticker", "IWM", "--batch-size", "100"])

    captured = capsys.readouterr()

    assert exit_code == 0
    assert calls == {
        "benchmark_ticker": "IWM",
        "batch_size": 100,
        "start_date": None,
        "end_date": None,
        "as_of_date": None,
        "interval": "1d",
    }
    assert "planned_batches=2" in captured.out


def test_ingest_basket_batch_command_dispatches_to_batch_workflow(monkeypatch, capsys) -> None:
    calls: dict[str, object] = {}

    def fake_ingest_basket_batch(benchmark_ticker, *, batch_index):
        calls["benchmark_ticker"] = benchmark_ticker
        calls["batch_index"] = batch_index
        return SimpleNamespace(
            basket_ticker=benchmark_ticker,
            batch_index=batch_index,
            status="partial",
            attempt_count=2,
            batch_tickers=["AAPL", "MSFT"],
            fully_ingested_tickers=["AAPL"],
            missing_price_tickers=["MSFT"],
            missing_fundamental_tickers=["MSFT"],
            failures=[{"ticker": "MSFT"}],
        )

    monkeypatch.setattr(cli_main_module, "ingest_basket_batch", fake_ingest_basket_batch)

    exit_code = cli_main_module.main(["ingest-basket", "--ticker", "IWM", "--batch-index", "3"])

    captured = capsys.readouterr()

    assert exit_code == 0
    assert calls == {"benchmark_ticker": "IWM", "batch_index": 3}
    assert "status=partial" in captured.out
    assert "failures=1" in captured.out


def test_basket_status_command_dispatches_to_status_workflow(monkeypatch, capsys) -> None:
    def fake_build_basket_status_summary(benchmark_ticker):
        assert benchmark_ticker == "IWM"
        return SimpleNamespace(
            basket_ticker="IWM",
            total_planned_batches=5,
            completed_batches=2,
            partial_batches=1,
            failed_batches=1,
            remaining_batches=3,
            cumulative_target_tickers=500,
            cumulative_fully_ingested_tickers=280,
            cumulative_missing_price_tickers=20,
            cumulative_missing_fundamentals_tickers=40,
        )

    monkeypatch.setattr(cli_main_module, "build_basket_status_summary", fake_build_basket_status_summary)

    exit_code = cli_main_module.main(["basket-status", "--ticker", "IWM"])

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "completed_batches=2" in captured.out
    assert "remaining_batches=3" in captured.out


def test_basket_validate_command_dispatches_to_validation_workflow(monkeypatch, capsys) -> None:
    def fake_validate_basket_ingestion(benchmark_ticker):
        assert benchmark_ticker == "IWM"
        return SimpleNamespace(
            basket_ticker="IWM",
            target_tickers=["AAPL", "MSFT"],
            fully_ingested_tickers=["AAPL"],
            missing_from_security_master=["MSFT"],
            missing_from_normalized_prices=["MSFT"],
            missing_from_normalized_fundamentals=["MSFT"],
            period_types_present=["annual", "quarterly", "ttm"],
        )

    monkeypatch.setattr(cli_main_module, "validate_basket_ingestion", fake_validate_basket_ingestion)

    exit_code = cli_main_module.main(["basket-validate", "--ticker", "IWM"])

    captured = capsys.readouterr()

    assert exit_code == 0
    assert "target_tickers=2" in captured.out
    assert "period_types_present=annual,quarterly,ttm" in captured.out
