from __future__ import annotations

import json
from datetime import date

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.ingestion.baskets import (
    BATCH_STATUS_COMPLETED,
    BATCH_STATUS_PARTIAL,
    BasketBatchResult,
    batch_result_path,
    build_basket_status_summary,
    ingest_basket_batch,
    load_basket_manifest,
    plan_basket_ingestion,
)
from dumb_money.storage import write_canonical_table
from dumb_money.transforms.ingestion_status import stage_security_ingestion_status


def _seed_benchmark_memberships(settings: AppSettings, tickers: list[str], benchmark_ticker: str = "IWM") -> None:
    memberships = pd.DataFrame(
        {
            "benchmark_id": [benchmark_ticker] * len(tickers),
            "benchmark_ticker": [benchmark_ticker] * len(tickers),
            "member_ticker": tickers,
            "member_name": [f"{ticker} Inc." for ticker in tickers],
            "member_weight": [1.0] * len(tickers),
            "member_sector": ["Technology"] * len(tickers),
            "asset_class": ["Equity"] * len(tickers),
            "exchange": ["NASDAQ"] * len(tickers),
            "currency": ["USD"] * len(tickers),
            "as_of_date": ["Apr 01, 2026"] * len(tickers),
            "source": ["benchmark_holdings_snapshot"] * len(tickers),
            "source_file": ["iwm_holdings.csv"] * len(tickers),
        }
    )
    write_canonical_table(memberships, "benchmark_memberships", settings=settings)


def _seed_security_master(
    settings: AppSettings,
    tickers: list[str],
) -> None:
    security_master = pd.DataFrame(
        {
            "security_id": [f"sec_{ticker.lower()}" for ticker in tickers],
            "ticker": tickers,
            "name": [f"{ticker} Inc." for ticker in tickers],
            "asset_type": ["common_stock"] * len(tickers),
            "exchange": ["Nasdaq"] * len(tickers),
            "primary_listing": ["Nasdaq"] * len(tickers),
            "currency": ["USD"] * len(tickers),
            "sector": ["Technology"] * len(tickers),
            "industry": ["Software"] * len(tickers),
            "country": [None] * len(tickers),
            "cik": [None] * len(tickers),
            "is_benchmark": [False] * len(tickers),
            "is_active": [True] * len(tickers),
            "is_eligible_research_universe": [True] * len(tickers),
            "source": ["test"] * len(tickers),
            "source_id": tickers,
            "first_seen_at": [None] * len(tickers),
            "last_updated_at": [None] * len(tickers),
            "notes": [None] * len(tickers),
        }
    )
    write_canonical_table(security_master, "security_master", settings=settings)


def _seed_prices(settings: AppSettings, tickers: list[str]) -> None:
    prices = pd.DataFrame(
        {
            "ticker": tickers,
            "date": ["2024-01-02"] * len(tickers),
            "interval": ["1d"] * len(tickers),
            "source": ["yfinance"] * len(tickers),
            "currency": ["USD"] * len(tickers),
            "open": [100.0] * len(tickers),
            "high": [101.0] * len(tickers),
            "low": [99.0] * len(tickers),
            "close": [100.5] * len(tickers),
            "adj_close": [100.2] * len(tickers),
            "volume": [1000] * len(tickers),
        }
    )
    write_canonical_table(prices, "normalized_prices", settings=settings)


def _seed_fundamentals(settings: AppSettings, tickers: list[str]) -> None:
    fundamentals = pd.DataFrame(
        {
            "ticker": tickers,
            "as_of_date": ["2024-06-30"] * len(tickers),
            "period_end_date": ["2024-03-31"] * len(tickers),
            "report_date": ["2024-05-01"] * len(tickers),
            "fiscal_year": [2024] * len(tickers),
            "fiscal_quarter": [1] * len(tickers),
            "fiscal_period": ["Q1"] * len(tickers),
            "period_type": ["quarterly"] * len(tickers),
            "source": ["yfinance"] * len(tickers),
            "currency": ["USD"] * len(tickers),
            "long_name": [f"{ticker} Inc." for ticker in tickers],
            "sector": ["Technology"] * len(tickers),
            "industry": ["Software"] * len(tickers),
            "website": ["https://example.com"] * len(tickers),
            "market_cap": [1.0] * len(tickers),
            "enterprise_value": [1.0] * len(tickers),
            "revenue": [1.0] * len(tickers),
            "revenue_ttm": [1.0] * len(tickers),
            "gross_profit": [1.0] * len(tickers),
            "operating_income": [1.0] * len(tickers),
            "net_income": [1.0] * len(tickers),
            "ebitda": [1.0] * len(tickers),
            "free_cash_flow": [1.0] * len(tickers),
            "total_debt": [1.0] * len(tickers),
            "total_cash": [1.0] * len(tickers),
            "current_assets": [1.0] * len(tickers),
            "current_liabilities": [1.0] * len(tickers),
            "shares_outstanding": [1.0] * len(tickers),
            "eps_trailing": [1.0] * len(tickers),
            "eps_forward": [1.0] * len(tickers),
            "gross_margin": [1.0] * len(tickers),
            "operating_margin": [1.0] * len(tickers),
            "profit_margin": [1.0] * len(tickers),
            "return_on_equity": [1.0] * len(tickers),
            "return_on_assets": [1.0] * len(tickers),
            "debt_to_equity": [1.0] * len(tickers),
            "current_ratio": [1.0] * len(tickers),
            "trailing_pe": [1.0] * len(tickers),
            "forward_pe": [1.0] * len(tickers),
            "price_to_sales": [1.0] * len(tickers),
            "ev_to_ebitda": [1.0] * len(tickers),
            "dividend_yield": [1.0] * len(tickers),
            "raw_payload_path": ["x.json"] * len(tickers),
            "pulled_at": ["2024-06-30T12:00:00Z"] * len(tickers),
        }
    )
    write_canonical_table(fundamentals, "normalized_fundamentals", settings=settings)


def test_plan_basket_ingestion_writes_deterministic_manifest(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    _seed_benchmark_memberships(settings, ["MSFT", "AAPL", "NVDA", "AMD", "TSLA"])

    manifest = plan_basket_ingestion(
        "IWM",
        batch_size=2,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        as_of_date=date(2024, 12, 31),
        settings=settings,
    )

    loaded = load_basket_manifest("IWM", settings=settings)

    assert manifest.total_target_tickers == 5
    assert manifest.total_unresolved_tickers == 5
    assert [batch.tickers for batch in manifest.batches] == [
        ["AAPL", "AMD"],
        ["MSFT", "NVDA"],
        ["TSLA"],
    ]
    assert loaded.batch_size == 2
    assert loaded.selector_sql == manifest.selector_sql


def test_plan_basket_ingestion_uses_unresolved_tickers_for_batches(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    _seed_benchmark_memberships(settings, ["AAPL", "MSFT", "NVDA"])
    _seed_security_master(settings, ["AAPL", "MSFT", "NVDA"])
    _seed_prices(settings, ["AAPL"])
    _seed_fundamentals(settings, ["AAPL"])
    stage_security_ingestion_status(settings=settings)

    manifest = plan_basket_ingestion("IWM", batch_size=2, settings=settings)

    assert manifest.total_target_tickers == 3
    assert manifest.total_unresolved_tickers == 2
    assert [batch.tickers for batch in manifest.batches] == [["MSFT", "NVDA"]]


def test_build_basket_status_summary_counts_partial_batches(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    _seed_benchmark_memberships(settings, ["AAPL", "MSFT", "NVDA"])
    plan_basket_ingestion("IWM", batch_size=2, settings=settings)

    batch_one = BasketBatchResult(
        basket_ticker="IWM",
        batch_index=0,
        batch_size=2,
        batch_tickers=["AAPL", "MSFT"],
        status=BATCH_STATUS_PARTIAL,
        attempt_count=1,
        skipped=False,
        start_date=date(2024, 4, 2),
        end_date=date(2024, 4, 2),
        as_of_date=date(2024, 4, 2),
        interval="1d",
        raw_price_rows=2,
        raw_fundamental_rows=1,
        canonical_price_rows=2,
        canonical_fundamental_rows=1,
        fully_ingested_tickers=["AAPL"],
        missing_security_master_tickers=[],
        missing_price_tickers=["MSFT"],
        missing_fundamental_tickers=["MSFT"],
        period_types_present=["quarterly"],
        failures=[{"ticker": "MSFT", "classification": "provider_or_transient_failure"}],
        notes=[],
        started_at="2026-04-02T12:00:00+00:00",
        completed_at="2026-04-02T12:05:00+00:00",
    )
    batch_two = BasketBatchResult(
        basket_ticker="IWM",
        batch_index=1,
        batch_size=2,
        batch_tickers=["NVDA"],
        status=BATCH_STATUS_COMPLETED,
        attempt_count=1,
        skipped=False,
        start_date=date(2024, 4, 2),
        end_date=date(2024, 4, 2),
        as_of_date=date(2024, 4, 2),
        interval="1d",
        raw_price_rows=1,
        raw_fundamental_rows=1,
        canonical_price_rows=1,
        canonical_fundamental_rows=1,
        fully_ingested_tickers=["NVDA"],
        missing_security_master_tickers=[],
        missing_price_tickers=[],
        missing_fundamental_tickers=[],
        period_types_present=["annual", "quarterly"],
        failures=[],
        notes=[],
        started_at="2026-04-02T12:10:00+00:00",
        completed_at="2026-04-02T12:15:00+00:00",
    )
    batch_result_path("IWM", 0, settings=settings).write_text(json.dumps(batch_one.to_dict(), indent=2))
    batch_result_path("IWM", 1, settings=settings).write_text(json.dumps(batch_two.to_dict(), indent=2))

    summary = build_basket_status_summary("IWM", settings=settings)

    assert summary.total_planned_batches == 2
    assert summary.partial_batches == 1
    assert summary.completed_batches == 1
    assert summary.remaining_batches == 1
    assert summary.cumulative_fully_ingested_tickers == 2
    assert summary.cumulative_missing_price_tickers == 1
    assert summary.cumulative_missing_fundamentals_tickers == 1


def test_ingest_basket_batch_skips_completed_batch_rerun_safely(monkeypatch, tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    _seed_benchmark_memberships(settings, ["AAPL", "MSFT"])
    plan_basket_ingestion("IWM", batch_size=2, settings=settings)

    existing = BasketBatchResult(
        basket_ticker="IWM",
        batch_index=0,
        batch_size=2,
        batch_tickers=["AAPL", "MSFT"],
        status=BATCH_STATUS_COMPLETED,
        attempt_count=1,
        skipped=False,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 31),
        as_of_date=date(2024, 12, 31),
        interval="1d",
        raw_price_rows=2,
        raw_fundamental_rows=2,
        canonical_price_rows=2,
        canonical_fundamental_rows=2,
        fully_ingested_tickers=["AAPL", "MSFT"],
        missing_security_master_tickers=[],
        missing_price_tickers=[],
        missing_fundamental_tickers=[],
        period_types_present=["quarterly"],
        failures=[],
        notes=[],
        started_at="2026-04-02T12:00:00+00:00",
        completed_at="2026-04-02T12:05:00+00:00",
    )
    result_path = batch_result_path("IWM", 0, settings=settings)
    result_path.write_text(json.dumps(existing.to_dict(), indent=2))

    def fail_if_called(**kwargs):
        raise AssertionError("ingestion should not run for an already-completed batch")

    monkeypatch.setattr("dumb_money.ingestion.baskets.ingest_selected_prices", fail_if_called)
    monkeypatch.setattr("dumb_money.ingestion.baskets.ingest_selected_fundamentals", fail_if_called)

    result = ingest_basket_batch("IWM", batch_index=0, settings=settings)

    assert result.status == "skipped"
    assert result.skipped is True
    persisted = json.loads(result_path.read_text())
    assert persisted["status"] == BATCH_STATUS_COMPLETED

