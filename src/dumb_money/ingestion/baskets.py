"""Shared basket-ingestion workflow helpers."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings, get_settings
from dumb_money.ingestion.fundamentals import ingest_selected_fundamentals
from dumb_money.ingestion.prices import build_price_filename, ingest_selected_prices
from dumb_money.storage.warehouse import query_canonical_data, warehouse_table_exists
from dumb_money.transforms.fundamentals import stage_fundamentals
from dumb_money.transforms.ingestion_status import stage_security_ingestion_status
from dumb_money.transforms.prices import stage_prices
from dumb_money.transforms.security_master import stage_security_master
from dumb_money.universe import build_benchmark_member_ticker_sql

DEFAULT_BASKET_BATCH_SIZE = 100

BATCH_STATUS_PLANNED = "planned"
BATCH_STATUS_RUNNING = "running"
BATCH_STATUS_COMPLETED = "completed"
BATCH_STATUS_PARTIAL = "partial"
BATCH_STATUS_FAILED = "failed"
BATCH_STATUS_SKIPPED = "skipped"


@dataclass(slots=True)
class BasketBatchDefinition:
    batch_index: int
    tickers: list[str]

    def to_dict(self) -> dict[str, object]:
        return {"batch_index": self.batch_index, "tickers": list(self.tickers)}


@dataclass(slots=True)
class BasketManifest:
    basket_ticker: str
    created_at: str
    selector_sql: str
    batch_size: int
    total_target_tickers: int
    total_unresolved_tickers: int
    start_date: date
    end_date: date
    as_of_date: date
    interval: str
    batches: list[BasketBatchDefinition]

    def to_dict(self) -> dict[str, object]:
        return {
            "basket_ticker": self.basket_ticker,
            "created_at": self.created_at,
            "selector_sql": self.selector_sql,
            "batch_size": self.batch_size,
            "total_target_tickers": self.total_target_tickers,
            "total_unresolved_tickers": self.total_unresolved_tickers,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "as_of_date": self.as_of_date.isoformat(),
            "interval": self.interval,
            "batches": [batch.to_dict() for batch in self.batches],
        }


@dataclass(slots=True)
class BasketBatchResult:
    basket_ticker: str
    batch_index: int
    batch_size: int
    batch_tickers: list[str]
    status: str
    attempt_count: int
    skipped: bool
    start_date: date
    end_date: date
    as_of_date: date
    interval: str
    raw_price_rows: int
    raw_fundamental_rows: int
    canonical_price_rows: int
    canonical_fundamental_rows: int
    fully_ingested_tickers: list[str]
    missing_security_master_tickers: list[str]
    missing_price_tickers: list[str]
    missing_fundamental_tickers: list[str]
    period_types_present: list[str]
    failures: list[dict[str, object]]
    notes: list[str]
    started_at: str
    completed_at: str | None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["start_date"] = self.start_date.isoformat()
        payload["end_date"] = self.end_date.isoformat()
        payload["as_of_date"] = self.as_of_date.isoformat()
        return payload


@dataclass(slots=True)
class BasketStatusSummary:
    basket_ticker: str
    total_planned_batches: int
    completed_batches: int
    partial_batches: int
    failed_batches: int
    remaining_batches: int
    cumulative_target_tickers: int
    cumulative_fully_ingested_tickers: int
    cumulative_missing_price_tickers: int
    cumulative_missing_fundamentals_tickers: int
    batch_statuses: list[dict[str, object]]


@dataclass(slots=True)
class BasketValidationSummary:
    basket_ticker: str
    target_tickers: list[str]
    fully_ingested_tickers: list[str]
    missing_from_security_master: list[str]
    missing_from_normalized_prices: list[str]
    missing_from_normalized_fundamentals: list[str]
    period_types_present: list[str]


@dataclass(slots=True)
class BasketIngestionResult:
    benchmark_ticker: str
    start_date: date
    end_date: date
    as_of_date: date
    target_tickers: list[str]
    unresolved_tickers: list[str]
    skipped_already_ingested_tickers: list[str]
    price_input_paths: list[Path]
    fundamental_input_paths: list[Path]
    raw_price_rows: int
    raw_fundamental_rows: int
    canonical_price_rows: int
    canonical_fundamental_rows: int
    validation_summary: pd.DataFrame
    period_type_summary: pd.DataFrame


def basket_run_dir(benchmark_ticker: str, *, settings: AppSettings | None = None) -> Path:
    settings = settings or get_settings()
    return settings.data_dir / "runs" / "basket_ingestion" / benchmark_ticker.strip().upper()


def basket_manifest_path(benchmark_ticker: str, *, settings: AppSettings | None = None) -> Path:
    return basket_run_dir(benchmark_ticker, settings=settings) / "manifest.json"


def batch_result_path(
    benchmark_ticker: str,
    batch_index: int,
    *,
    settings: AppSettings | None = None,
) -> Path:
    return basket_run_dir(benchmark_ticker, settings=settings) / "results" / f"batch_{batch_index:04d}.json"


def _resolve_target_tickers(benchmark_ticker: str, *, settings: AppSettings) -> list[str]:
    frame = query_canonical_data(
        build_benchmark_member_ticker_sql(benchmark_ticker),
        settings=settings,
    )
    if frame.empty:
        return []
    return sorted(frame["ticker"].astype(str).str.strip().str.upper().tolist())


def _resolve_unresolved_tickers(benchmark_ticker: str, *, settings: AppSettings) -> list[str]:
    if not warehouse_table_exists("security_ingestion_status", settings=settings):
        return _resolve_target_tickers(benchmark_ticker, settings=settings)
    frame = query_canonical_data(
        build_benchmark_member_ticker_sql(benchmark_ticker, exclude_fully_ingested=True),
        settings=settings,
    )
    if frame.empty:
        return []
    return sorted(frame["ticker"].astype(str).str.strip().str.upper().tolist())


def _chunk_tickers(tickers: list[str], *, batch_size: int) -> list[BasketBatchDefinition]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    return [
        BasketBatchDefinition(batch_index=index // batch_size, tickers=tickers[index : index + batch_size])
        for index in range(0, len(tickers), batch_size)
    ]


def _date_to_iso(value: date) -> str:
    return value.isoformat()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def _manifest_from_payload(payload: dict[str, object]) -> BasketManifest:
    batches = [
        BasketBatchDefinition(
            batch_index=int(batch["batch_index"]),
            tickers=[str(ticker) for ticker in batch["tickers"]],
        )
        for batch in payload.get("batches", [])
    ]
    return BasketManifest(
        basket_ticker=str(payload["basket_ticker"]),
        created_at=str(payload["created_at"]),
        selector_sql=str(payload["selector_sql"]),
        batch_size=int(payload["batch_size"]),
        total_target_tickers=int(payload["total_target_tickers"]),
        total_unresolved_tickers=int(payload["total_unresolved_tickers"]),
        start_date=date.fromisoformat(str(payload["start_date"])),
        end_date=date.fromisoformat(str(payload["end_date"])),
        as_of_date=date.fromisoformat(str(payload["as_of_date"])),
        interval=str(payload["interval"]),
        batches=batches,
    )


def _batch_result_from_payload(payload: dict[str, object]) -> BasketBatchResult:
    return BasketBatchResult(
        basket_ticker=str(payload["basket_ticker"]),
        batch_index=int(payload["batch_index"]),
        batch_size=int(payload["batch_size"]),
        batch_tickers=[str(ticker) for ticker in payload.get("batch_tickers", [])],
        status=str(payload["status"]),
        attempt_count=int(payload["attempt_count"]),
        skipped=bool(payload.get("skipped", False)),
        start_date=date.fromisoformat(str(payload["start_date"])),
        end_date=date.fromisoformat(str(payload["end_date"])),
        as_of_date=date.fromisoformat(str(payload["as_of_date"])),
        interval=str(payload["interval"]),
        raw_price_rows=int(payload.get("raw_price_rows", 0)),
        raw_fundamental_rows=int(payload.get("raw_fundamental_rows", 0)),
        canonical_price_rows=int(payload.get("canonical_price_rows", 0)),
        canonical_fundamental_rows=int(payload.get("canonical_fundamental_rows", 0)),
        fully_ingested_tickers=[str(ticker) for ticker in payload.get("fully_ingested_tickers", [])],
        missing_security_master_tickers=[
            str(ticker) for ticker in payload.get("missing_security_master_tickers", [])
        ],
        missing_price_tickers=[str(ticker) for ticker in payload.get("missing_price_tickers", [])],
        missing_fundamental_tickers=[
            str(ticker) for ticker in payload.get("missing_fundamental_tickers", [])
        ],
        period_types_present=[str(period) for period in payload.get("period_types_present", [])],
        failures=[dict(item) for item in payload.get("failures", [])],
        notes=[str(note) for note in payload.get("notes", [])],
        started_at=str(payload["started_at"]),
        completed_at=str(payload["completed_at"]) if payload.get("completed_at") else None,
    )


def load_basket_manifest(
    benchmark_ticker: str,
    *,
    settings: AppSettings | None = None,
) -> BasketManifest:
    path = basket_manifest_path(benchmark_ticker, settings=settings)
    if not path.exists():
        raise FileNotFoundError(f"basket manifest not found: {path}")
    return _manifest_from_payload(_read_json(path))


def load_batch_result(
    benchmark_ticker: str,
    batch_index: int,
    *,
    settings: AppSettings | None = None,
) -> BasketBatchResult | None:
    path = batch_result_path(benchmark_ticker, batch_index, settings=settings)
    if not path.exists():
        return None
    return _batch_result_from_payload(_read_json(path))


def plan_basket_ingestion(
    benchmark_ticker: str,
    *,
    batch_size: int = DEFAULT_BASKET_BATCH_SIZE,
    settings: AppSettings | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of_date: date | None = None,
    interval: str = "1d",
) -> BasketManifest:
    settings = settings or get_settings()
    settings.ensure_directories()
    benchmark_ticker = benchmark_ticker.strip().upper()
    resolved_start, resolved_end = settings.default_price_window()
    start_date = start_date or resolved_start
    end_date = end_date or resolved_end
    as_of_date = as_of_date or date.today()
    selector_sql = build_benchmark_member_ticker_sql(benchmark_ticker, exclude_fully_ingested=True)
    target_tickers = _resolve_target_tickers(benchmark_ticker, settings=settings)
    unresolved_tickers = _resolve_unresolved_tickers(benchmark_ticker, settings=settings)
    manifest = BasketManifest(
        basket_ticker=benchmark_ticker,
        created_at=_utc_now_iso(),
        selector_sql=selector_sql,
        batch_size=batch_size,
        total_target_tickers=len(target_tickers),
        total_unresolved_tickers=len(unresolved_tickers),
        start_date=start_date,
        end_date=end_date,
        as_of_date=as_of_date,
        interval=interval,
        batches=_chunk_tickers(unresolved_tickers, batch_size=batch_size),
    )
    basket_run_dir(benchmark_ticker, settings=settings).mkdir(parents=True, exist_ok=True)
    (basket_run_dir(benchmark_ticker, settings=settings) / "results").mkdir(parents=True, exist_ok=True)
    _write_json(basket_manifest_path(benchmark_ticker, settings=settings), manifest.to_dict())
    return manifest


def _price_input_paths(
    tickers: list[str],
    *,
    start_date: date,
    end_date: date,
    interval: str,
    settings: AppSettings,
) -> list[Path]:
    return [
        settings.raw_prices_dir / build_price_filename(ticker, start_date, end_date, interval)
        for ticker in tickers
        if (settings.raw_prices_dir / build_price_filename(ticker, start_date, end_date, interval)).exists()
    ]


def _fundamental_input_paths(
    tickers: list[str],
    *,
    as_of_date: date,
    settings: AppSettings,
) -> list[Path]:
    return [
        settings.raw_fundamentals_dir / f"{ticker.lower()}_fundamentals_flat_{as_of_date}.csv"
        for ticker in tickers
        if (settings.raw_fundamentals_dir / f"{ticker.lower()}_fundamentals_flat_{as_of_date}.csv").exists()
    ]


def _values_clause(tickers: list[str]) -> str:
    if not tickers:
        return "select null::varchar as ticker where false"
    values = ", ".join("('" + ticker.replace("'", "''") + "')" for ticker in tickers)
    return f"select * from (values {values}) as batch(ticker)"


def _batch_validation_frame(tickers: list[str], *, settings: AppSettings) -> pd.DataFrame:
    return query_canonical_data(
        f"""
        with batch as (
          {_values_clause(tickers)}
        )
        select
          batch.ticker,
          sm.ticker is not null as in_security_master,
          p.ticker is not null as in_prices,
          f.ticker is not null as in_fundamentals
        from batch
        left join (select distinct ticker from security_master) sm on sm.ticker = batch.ticker
        left join (select distinct ticker from normalized_prices) p on p.ticker = batch.ticker
        left join (select distinct ticker from normalized_fundamentals) f on f.ticker = batch.ticker
        order by batch.ticker
        """,
        settings=settings,
    )


def _period_types_for_tickers(tickers: list[str], *, settings: AppSettings) -> list[str]:
    if not tickers:
        return []
    frame = query_canonical_data(
        f"""
        with batch as (
          {_values_clause(tickers)}
        )
        select distinct period_type
        from normalized_fundamentals
        where ticker in (select ticker from batch)
          and period_type is not null
        order by period_type
        """,
        settings=settings,
    )
    if frame.empty:
        return []
    return frame["period_type"].astype(str).tolist()


def _classify_failures(
    tickers: list[str],
    price_input_paths: list[Path],
    fundamental_input_paths: list[Path],
    validation: pd.DataFrame,
) -> list[dict[str, object]]:
    raw_price_tickers = {path.name.split("_")[0].upper() for path in price_input_paths}
    raw_fundamental_tickers = {path.name.split("_")[0].upper() for path in fundamental_input_paths}
    failures: list[dict[str, object]] = []
    for ticker in tickers:
        row = validation.loc[validation["ticker"] == ticker]
        if row.empty:
            failures.append({"ticker": ticker, "classification": "provider_or_transient_failure"})
            continue
        record = row.iloc[0]
        missing_price = not bool(record["in_prices"])
        missing_fundamentals = not bool(record["in_fundamentals"])
        missing_security_master = not bool(record["in_security_master"])
        if not any([missing_price, missing_fundamentals, missing_security_master]):
            continue
        if ticker not in raw_price_tickers and ticker not in raw_fundamental_tickers:
            classification = "provider_or_transient_failure"
        elif missing_security_master:
            classification = "known_symbol_or_holdings_artifact"
        else:
            classification = "provider_or_transient_failure"
        failures.append(
            {
                "ticker": ticker,
                "classification": classification,
                "missing_security_master": missing_security_master,
                "missing_prices": missing_price,
                "missing_fundamentals": missing_fundamentals,
            }
        )
    return failures


def _status_from_validation(validation: pd.DataFrame) -> str:
    if validation.empty:
        return BATCH_STATUS_FAILED
    fully_ingested = validation["in_security_master"] & validation["in_prices"] & validation["in_fundamentals"]
    if bool(fully_ingested.all()):
        return BATCH_STATUS_COMPLETED
    if bool(fully_ingested.any()):
        return BATCH_STATUS_PARTIAL
    return BATCH_STATUS_FAILED


def _validation_lists(validation: pd.DataFrame, column: str, *, expect_present: bool) -> list[str]:
    if validation.empty:
        return []
    mask = validation[column].astype(bool)
    if expect_present:
        return validation.loc[mask, "ticker"].astype(str).tolist()
    return validation.loc[~mask, "ticker"].astype(str).tolist()


def ingest_basket_batch(
    benchmark_ticker: str,
    *,
    batch_index: int,
    settings: AppSettings | None = None,
) -> BasketBatchResult:
    settings = settings or get_settings()
    manifest = load_basket_manifest(benchmark_ticker, settings=settings)
    batch_map = {batch.batch_index: batch for batch in manifest.batches}
    if batch_index not in batch_map:
        raise ValueError(f"batch_index {batch_index} is not present in manifest for {manifest.basket_ticker}")

    prior_result = load_batch_result(manifest.basket_ticker, batch_index, settings=settings)
    batch = batch_map[batch_index]
    if prior_result is not None and prior_result.status == BATCH_STATUS_COMPLETED:
        return BasketBatchResult(
            basket_ticker=manifest.basket_ticker,
            batch_index=batch_index,
            batch_size=manifest.batch_size,
            batch_tickers=list(batch.tickers),
            status=BATCH_STATUS_SKIPPED,
            attempt_count=prior_result.attempt_count,
            skipped=True,
            start_date=manifest.start_date,
            end_date=manifest.end_date,
            as_of_date=manifest.as_of_date,
            interval=manifest.interval,
            raw_price_rows=prior_result.raw_price_rows,
            raw_fundamental_rows=prior_result.raw_fundamental_rows,
            canonical_price_rows=prior_result.canonical_price_rows,
            canonical_fundamental_rows=prior_result.canonical_fundamental_rows,
            fully_ingested_tickers=list(prior_result.fully_ingested_tickers),
            missing_security_master_tickers=list(prior_result.missing_security_master_tickers),
            missing_price_tickers=list(prior_result.missing_price_tickers),
            missing_fundamental_tickers=list(prior_result.missing_fundamental_tickers),
            period_types_present=list(prior_result.period_types_present),
            failures=list(prior_result.failures),
            notes=["batch already completed; rerun skipped safely"],
            started_at=_utc_now_iso(),
            completed_at=_utc_now_iso(),
        )

    started_at = _utc_now_iso()
    attempt_count = 1 if prior_result is None else prior_result.attempt_count + 1
    tickers = list(batch.tickers)
    raw_price_rows = 0
    raw_fundamental_rows = 0
    canonical_price_rows = 0
    canonical_fundamental_rows = 0
    notes: list[str] = []
    status = BATCH_STATUS_RUNNING
    failures: list[dict[str, object]] = []
    fully_ingested_tickers: list[str] = []
    missing_security_master_tickers: list[str] = []
    missing_price_tickers: list[str] = []
    missing_fundamental_tickers: list[str] = []
    period_types_present: list[str] = []

    try:
        raw_price_rows = len(
            ingest_selected_prices(
                tickers=tickers,
                start_date=manifest.start_date,
                end_date=manifest.end_date,
                interval=manifest.interval,
                settings=settings,
            )
        )
        raw_fundamental_rows = len(
            ingest_selected_fundamentals(
                tickers=tickers,
                as_of_date=manifest.as_of_date,
                settings=settings,
            )
        )

        price_input_paths = _price_input_paths(
            tickers,
            start_date=manifest.start_date,
            end_date=manifest.end_date,
            interval=manifest.interval,
            settings=settings,
        )
        fundamental_input_paths = _fundamental_input_paths(
            tickers,
            as_of_date=manifest.as_of_date,
            settings=settings,
        )

        staged_prices = stage_prices(input_paths=price_input_paths, settings=settings, incremental=True)
        staged_fundamentals = stage_fundamentals(
            input_paths=fundamental_input_paths,
            settings=settings,
            incremental=True,
        )
        canonical_price_rows = len(staged_prices)
        canonical_fundamental_rows = len(staged_fundamentals)

        stage_security_master(settings=settings)
        stage_security_ingestion_status(settings=settings)

        validation = _batch_validation_frame(tickers, settings=settings)
        fully_ingested_tickers = _validation_lists(validation, "in_security_master", expect_present=True)
        fully_ingested_tickers = sorted(
            ticker
            for ticker in fully_ingested_tickers
            if ticker not in _validation_lists(validation, "in_prices", expect_present=False)
            and ticker not in _validation_lists(validation, "in_fundamentals", expect_present=False)
        )
        missing_security_master_tickers = _validation_lists(
            validation,
            "in_security_master",
            expect_present=False,
        )
        missing_price_tickers = _validation_lists(validation, "in_prices", expect_present=False)
        missing_fundamental_tickers = _validation_lists(
            validation,
            "in_fundamentals",
            expect_present=False,
        )
        period_types_present = _period_types_for_tickers(tickers, settings=settings)
        failures = _classify_failures(tickers, price_input_paths, fundamental_input_paths, validation)
        status = _status_from_validation(validation)
        if status == BATCH_STATUS_PARTIAL:
            notes.append("batch completed with at least one ticker still missing staged coverage")
    except Exception as exc:
        status = BATCH_STATUS_FAILED
        failures.append({"ticker": None, "classification": "workflow_error", "error": str(exc)})
        notes.append("batch workflow raised an exception before validation completed")

    result = BasketBatchResult(
        basket_ticker=manifest.basket_ticker,
        batch_index=batch_index,
        batch_size=manifest.batch_size,
        batch_tickers=tickers,
        status=status,
        attempt_count=attempt_count,
        skipped=False,
        start_date=manifest.start_date,
        end_date=manifest.end_date,
        as_of_date=manifest.as_of_date,
        interval=manifest.interval,
        raw_price_rows=raw_price_rows,
        raw_fundamental_rows=raw_fundamental_rows,
        canonical_price_rows=canonical_price_rows,
        canonical_fundamental_rows=canonical_fundamental_rows,
        fully_ingested_tickers=fully_ingested_tickers,
        missing_security_master_tickers=missing_security_master_tickers,
        missing_price_tickers=missing_price_tickers,
        missing_fundamental_tickers=missing_fundamental_tickers,
        period_types_present=period_types_present,
        failures=failures,
        notes=notes,
        started_at=started_at,
        completed_at=_utc_now_iso(),
    )
    _write_json(
        batch_result_path(manifest.basket_ticker, batch_index, settings=settings),
        result.to_dict(),
    )
    return result


def build_basket_status_summary(
    benchmark_ticker: str,
    *,
    settings: AppSettings | None = None,
) -> BasketStatusSummary:
    settings = settings or get_settings()
    manifest = load_basket_manifest(benchmark_ticker, settings=settings)
    statuses: list[dict[str, object]] = []
    fully_ingested: set[str] = set()
    missing_prices: set[str] = set()
    missing_fundamentals: set[str] = set()
    completed = 0
    partial = 0
    failed = 0
    for batch in manifest.batches:
        result = load_batch_result(manifest.basket_ticker, batch.batch_index, settings=settings)
        status = BATCH_STATUS_PLANNED if result is None else result.status
        statuses.append(
            {
                "batch_index": batch.batch_index,
                "status": status,
                "ticker_count": len(batch.tickers),
                "tickers": list(batch.tickers),
            }
        )
        if result is None:
            continue
        fully_ingested.update(result.fully_ingested_tickers)
        missing_prices.update(result.missing_price_tickers)
        missing_fundamentals.update(result.missing_fundamental_tickers)
        if status == BATCH_STATUS_COMPLETED:
            completed += 1
        elif status == BATCH_STATUS_PARTIAL:
            partial += 1
        elif status == BATCH_STATUS_FAILED:
            failed += 1

    return BasketStatusSummary(
        basket_ticker=manifest.basket_ticker,
        total_planned_batches=len(manifest.batches),
        completed_batches=completed,
        partial_batches=partial,
        failed_batches=failed,
        remaining_batches=max(len(manifest.batches) - completed, 0),
        cumulative_target_tickers=manifest.total_target_tickers,
        cumulative_fully_ingested_tickers=len(fully_ingested),
        cumulative_missing_price_tickers=len(missing_prices),
        cumulative_missing_fundamentals_tickers=len(missing_fundamentals),
        batch_statuses=statuses,
    )


def validate_basket_ingestion(
    benchmark_ticker: str,
    *,
    settings: AppSettings | None = None,
) -> BasketValidationSummary:
    settings = settings or get_settings()
    target_tickers = _resolve_target_tickers(benchmark_ticker, settings=settings)
    validation = _batch_validation_frame(target_tickers, settings=settings)
    fully_ingested = [
        row["ticker"]
        for row in validation.to_dict(orient="records")
        if row["in_security_master"] and row["in_prices"] and row["in_fundamentals"]
    ]
    return BasketValidationSummary(
        basket_ticker=benchmark_ticker.strip().upper(),
        target_tickers=target_tickers,
        fully_ingested_tickers=sorted(fully_ingested),
        missing_from_security_master=_validation_lists(
            validation,
            "in_security_master",
            expect_present=False,
        ),
        missing_from_normalized_prices=_validation_lists(
            validation,
            "in_prices",
            expect_present=False,
        ),
        missing_from_normalized_fundamentals=_validation_lists(
            validation,
            "in_fundamentals",
            expect_present=False,
        ),
        period_types_present=_period_types_for_tickers(target_tickers, settings=settings),
    )


def _validation_summary(benchmark_ticker: str, *, settings: AppSettings) -> pd.DataFrame:
    safe_ticker = benchmark_ticker.strip().upper().replace("'", "''")
    return query_canonical_data(
        f"""
        with target as (
          select distinct bm.member_ticker as ticker
          from benchmark_memberships bm
          where bm.benchmark_ticker = '{safe_ticker}'
            and lower(coalesce(bm.asset_class, '')) = 'equity'
            and regexp_matches(bm.member_ticker, '^[A-Z][A-Z0-9.\\-]*$')
            and bm.member_ticker not in ('', '-', 'NAN', 'NONE', 'CASH', 'USD')
        )
        select
          count(*) as target_tickers,
          count(*) filter (where sis.is_fully_ingested) as fully_ingested_tickers,
          count(*) filter (where sm.ticker is not null) as in_security_master,
          count(*) filter (where p.ticker is not null) as in_prices,
          count(*) filter (where f.ticker is not null) as in_fundamentals
        from target t
        left join security_ingestion_status sis on sis.ticker = t.ticker
        left join (select distinct ticker from security_master) sm on sm.ticker = t.ticker
        left join (select distinct ticker from normalized_prices) p on p.ticker = t.ticker
        left join (select distinct ticker from normalized_fundamentals) f on f.ticker = t.ticker
        """,
        settings=settings,
    )


def _period_type_summary(benchmark_ticker: str, *, settings: AppSettings) -> pd.DataFrame:
    safe_ticker = benchmark_ticker.strip().upper().replace("'", "''")
    return query_canonical_data(
        f"""
        with target as (
          select distinct bm.member_ticker as ticker
          from benchmark_memberships bm
          where bm.benchmark_ticker = '{safe_ticker}'
            and lower(coalesce(bm.asset_class, '')) = 'equity'
            and regexp_matches(bm.member_ticker, '^[A-Z][A-Z0-9.\\-]*$')
            and bm.member_ticker not in ('', '-', 'NAN', 'NONE', 'CASH', 'USD')
        )
        select period_type, count(*) as rows
        from normalized_fundamentals
        where ticker in (select ticker from target)
        group by 1
        order by 1
        """,
        settings=settings,
    )


def ingest_basket(
    benchmark_ticker: str,
    *,
    settings: AppSettings | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    as_of_date: date | None = None,
    interval: str = "1d",
) -> BasketIngestionResult:
    """Ingest unresolved members for one benchmark basket and stage canonical tables."""

    settings = settings or get_settings()
    resolved_start, resolved_end = settings.default_price_window()
    start_date = start_date or resolved_start
    end_date = end_date or resolved_end
    as_of_date = as_of_date or date.today()
    benchmark_ticker = benchmark_ticker.strip().upper()

    target_tickers = _resolve_target_tickers(benchmark_ticker, settings=settings)
    unresolved_tickers = _resolve_unresolved_tickers(benchmark_ticker, settings=settings)
    skipped_already_ingested_tickers = [ticker for ticker in target_tickers if ticker not in set(unresolved_tickers)]

    raw_price_rows = 0
    raw_fundamental_rows = 0
    if unresolved_tickers:
        unresolved_sql = build_benchmark_member_ticker_sql(benchmark_ticker, exclude_fully_ingested=True)
        raw_price_rows = len(
            ingest_selected_prices(
                ticker_query_sql=unresolved_sql,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
                settings=settings,
            )
        )
        raw_fundamental_rows = len(
            ingest_selected_fundamentals(
                ticker_query_sql=unresolved_sql,
                as_of_date=as_of_date,
                settings=settings,
            )
        )

    price_input_paths = _price_input_paths(
        target_tickers,
        start_date=start_date,
        end_date=end_date,
        interval=interval,
        settings=settings,
    )
    fundamental_input_paths = _fundamental_input_paths(
        target_tickers,
        as_of_date=as_of_date,
        settings=settings,
    )

    prices = stage_prices(input_paths=price_input_paths, settings=settings, incremental=True)
    fundamentals = stage_fundamentals(
        input_paths=fundamental_input_paths,
        settings=settings,
        incremental=True,
    )
    stage_security_master(settings=settings)
    stage_security_ingestion_status(settings=settings)

    return BasketIngestionResult(
        benchmark_ticker=benchmark_ticker,
        start_date=start_date,
        end_date=end_date,
        as_of_date=as_of_date,
        target_tickers=target_tickers,
        unresolved_tickers=unresolved_tickers,
        skipped_already_ingested_tickers=skipped_already_ingested_tickers,
        price_input_paths=price_input_paths,
        fundamental_input_paths=fundamental_input_paths,
        raw_price_rows=raw_price_rows,
        raw_fundamental_rows=raw_fundamental_rows,
        canonical_price_rows=len(prices),
        canonical_fundamental_rows=len(fundamentals),
        validation_summary=_validation_summary(benchmark_ticker, settings=settings),
        period_type_summary=_period_type_summary(benchmark_ticker, settings=settings),
    )
