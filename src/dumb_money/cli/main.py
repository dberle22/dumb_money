"""Command-line entry points for local ingestion workflows."""

from __future__ import annotations

import argparse
from datetime import date
from typing import Sequence

from dumb_money.ingestion import (
    ingest_benchmark_definitions,
    ingest_benchmark_prices,
    ingest_selected_fundamentals,
    ingest_selected_prices,
    ingest_listed_security_sources,
)
from dumb_money.ingestion.baskets import (
    DEFAULT_BASKET_BATCH_SIZE,
    build_basket_status_summary,
    ingest_basket,
    ingest_basket_batch,
    plan_basket_ingestion,
    validate_basket_ingestion,
)
from dumb_money.transforms import (
    stage_benchmark_definition_refresh,
    stage_benchmark_mappings,
    stage_benchmark_membership_coverage,
    stage_benchmark_memberships,
    stage_benchmark_sets,
    stage_fundamentals,
    stage_security_ingestion_status,
    stage_listed_security_seed,
    stage_prices,
    stage_security_master_overrides,
    stage_security_master,
)


def _csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _date_value(value: str) -> date:
    return date.fromisoformat(value)


def _path_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dumb-money", description="Dumb Money ingestion CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prices_parser = subparsers.add_parser("prices", help="Ingest raw price history.")
    prices_group = prices_parser.add_mutually_exclusive_group(required=True)
    prices_group.add_argument("--tickers", type=_csv_list)
    prices_group.add_argument("--ticker-query-sql")
    prices_parser.add_argument("--start-date", required=True, type=_date_value)
    prices_parser.add_argument("--end-date", required=True, type=_date_value)
    prices_parser.add_argument("--interval", default="1d")

    fundamentals_parser = subparsers.add_parser("fundamentals", help="Ingest fundamentals.")
    fundamentals_group = fundamentals_parser.add_mutually_exclusive_group(required=True)
    fundamentals_group.add_argument("--tickers", type=_csv_list)
    fundamentals_group.add_argument("--ticker-query-sql")
    fundamentals_parser.add_argument("--as-of-date", type=_date_value)

    basket_parser = subparsers.add_parser(
        "ingest-basket",
        help="Ingest unresolved members for one staged benchmark basket and stage canonical tables.",
    )
    basket_parser.add_argument("--ticker", required=True)
    basket_parser.add_argument("--start-date", type=_date_value)
    basket_parser.add_argument("--end-date", type=_date_value)
    basket_parser.add_argument("--as-of-date", type=_date_value)
    basket_parser.add_argument("--interval", default="1d")
    basket_parser.add_argument("--batch-index", type=int)

    plan_basket_parser = subparsers.add_parser(
        "plan-basket",
        help="Plan a deterministic resumable basket-ingestion run without ingesting data.",
    )
    plan_basket_parser.add_argument("--ticker", required=True)
    plan_basket_parser.add_argument("--batch-size", type=int, default=DEFAULT_BASKET_BATCH_SIZE)
    plan_basket_parser.add_argument("--start-date", type=_date_value)
    plan_basket_parser.add_argument("--end-date", type=_date_value)
    plan_basket_parser.add_argument("--as-of-date", type=_date_value)
    plan_basket_parser.add_argument("--interval", default="1d")

    basket_status_parser = subparsers.add_parser(
        "basket-status",
        help="Report manifest and per-batch execution status for a planned basket run.",
    )
    basket_status_parser.add_argument("--ticker", required=True)

    basket_validate_parser = subparsers.add_parser(
        "basket-validate",
        help="Validate basket ingestion coverage from DuckDB for the full basket target set.",
    )
    basket_validate_parser.add_argument("--ticker", required=True)

    benchmarks_parser = subparsers.add_parser(
        "benchmarks",
        help="Persist benchmark definitions and optionally benchmark prices.",
    )
    benchmarks_parser.add_argument("--tickers", type=_csv_list)
    benchmarks_parser.add_argument("--label", default="default")
    benchmarks_parser.add_argument("--definitions-only", action="store_true")
    benchmarks_parser.add_argument("--start-date", type=_date_value)
    benchmarks_parser.add_argument("--end-date", type=_date_value)
    benchmarks_parser.add_argument("--interval", default="1d")
    benchmarks_parser.add_argument("--as-of-date", type=_date_value)

    universe_parser = subparsers.add_parser(
        "universe",
        help="Copy listed-security source files into the raw universe directory.",
    )
    universe_parser.add_argument("--nasdaq-listed-path", required=True)
    universe_parser.add_argument("--other-listed-path", required=True)
    universe_parser.add_argument("--as-of-date", type=_date_value)

    stage_parser = subparsers.add_parser("stage", help="Build normalized staging datasets.")
    stage_parser.add_argument(
        "target",
        choices=[
            "prices",
            "fundamentals",
            "listed-security-seed",
            "benchmark-definitions",
            "benchmark-memberships",
            "benchmark-membership-coverage",
            "benchmark-mappings",
            "benchmark-sets",
            "security-master",
            "security-ingestion-status",
            "all",
        ],
    )
    stage_parser.add_argument("--price-paths", type=_path_list)
    stage_parser.add_argument("--fundamental-paths", type=_path_list)
    stage_parser.add_argument("--nasdaq-listed-paths", type=_path_list)
    stage_parser.add_argument("--other-listed-paths", type=_path_list)
    stage_parser.add_argument("--benchmark-definition-paths", type=_path_list)
    stage_parser.add_argument("--override-paths", type=_path_list)
    stage_parser.add_argument("--benchmark-mapping-path", type=str)
    stage_parser.add_argument("--custom-benchmark-membership-path", type=str)
    stage_parser.add_argument("--set-id", default="default_benchmarks")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "prices":
        ingest_selected_prices(
            tickers=args.tickers,
            ticker_query_sql=args.ticker_query_sql,
            start_date=args.start_date,
            end_date=args.end_date,
            interval=args.interval,
        )
        return 0

    if args.command == "fundamentals":
        ingest_selected_fundamentals(
            tickers=args.tickers,
            ticker_query_sql=args.ticker_query_sql,
            as_of_date=args.as_of_date,
        )
        return 0

    if args.command == "ingest-basket":
        if args.batch_index is not None:
            result = ingest_basket_batch(
                args.ticker,
                batch_index=args.batch_index,
            )
            print(f"basket={result.basket_ticker}")
            print(f"batch_index={result.batch_index}")
            print(f"status={result.status}")
            print(f"attempt_count={result.attempt_count}")
            print(f"batch_tickers={len(result.batch_tickers)}")
            print(f"fully_ingested_tickers={len(result.fully_ingested_tickers)}")
            print(f"missing_price_tickers={len(result.missing_price_tickers)}")
            print(f"missing_fundamental_tickers={len(result.missing_fundamental_tickers)}")
            print(f"failures={len(result.failures)}")
            return 0
        result = ingest_basket(
            args.ticker,
            start_date=args.start_date,
            end_date=args.end_date,
            as_of_date=args.as_of_date,
            interval=args.interval,
        )
        print(f"basket={result.benchmark_ticker}")
        print(f"target_tickers={len(result.target_tickers)}")
        print(f"unresolved_tickers={len(result.unresolved_tickers)}")
        print(f"skipped_already_ingested_tickers={len(result.skipped_already_ingested_tickers)}")
        print(f"price_input_paths={len(result.price_input_paths)}")
        print(f"fundamental_input_paths={len(result.fundamental_input_paths)}")
        print(f"raw_price_rows={result.raw_price_rows}")
        print(f"raw_fundamental_rows={result.raw_fundamental_rows}")
        print(f"canonical_price_rows={result.canonical_price_rows}")
        print(f"canonical_fundamental_rows={result.canonical_fundamental_rows}")
        if not result.validation_summary.empty:
            print(result.validation_summary.to_string(index=False))
        if not result.period_type_summary.empty:
            print(result.period_type_summary.to_string(index=False))
        return 0

    if args.command == "plan-basket":
        manifest = plan_basket_ingestion(
            args.ticker,
            batch_size=args.batch_size,
            start_date=args.start_date,
            end_date=args.end_date,
            as_of_date=args.as_of_date,
            interval=args.interval,
        )
        print(f"basket={manifest.basket_ticker}")
        print(f"batch_size={manifest.batch_size}")
        print(f"total_target_tickers={manifest.total_target_tickers}")
        print(f"total_unresolved_tickers={manifest.total_unresolved_tickers}")
        print(f"planned_batches={len(manifest.batches)}")
        return 0

    if args.command == "basket-status":
        summary = build_basket_status_summary(args.ticker)
        print(f"basket={summary.basket_ticker}")
        print(f"total_planned_batches={summary.total_planned_batches}")
        print(f"completed_batches={summary.completed_batches}")
        print(f"partial_batches={summary.partial_batches}")
        print(f"failed_batches={summary.failed_batches}")
        print(f"remaining_batches={summary.remaining_batches}")
        print(f"cumulative_target_tickers={summary.cumulative_target_tickers}")
        print(f"cumulative_fully_ingested_tickers={summary.cumulative_fully_ingested_tickers}")
        print(f"cumulative_missing_price_tickers={summary.cumulative_missing_price_tickers}")
        print(
            "cumulative_missing_fundamentals_tickers="
            f"{summary.cumulative_missing_fundamentals_tickers}"
        )
        return 0

    if args.command == "basket-validate":
        summary = validate_basket_ingestion(args.ticker)
        print(f"basket={summary.basket_ticker}")
        print(f"target_tickers={len(summary.target_tickers)}")
        print(f"fully_ingested_tickers={len(summary.fully_ingested_tickers)}")
        print(f"missing_from_security_master={','.join(summary.missing_from_security_master) or '-'}")
        print(f"missing_from_normalized_prices={','.join(summary.missing_from_normalized_prices) or '-'}")
        print(
            "missing_from_normalized_fundamentals="
            f"{','.join(summary.missing_from_normalized_fundamentals) or '-'}"
        )
        print(f"period_types_present={','.join(summary.period_types_present) or '-'}")
        return 0

    if args.command == "benchmarks":
        if args.definitions_only:
            ingest_benchmark_definitions(
                args.tickers,
                as_of_date=args.as_of_date,
                label=args.label,
            )
            return 0

        ingest_benchmark_prices(
            tickers=args.tickers,
            start_date=args.start_date,
            end_date=args.end_date,
            interval=args.interval,
            label=args.label,
        )
        return 0

    if args.command == "universe":
        ingest_listed_security_sources(
            nasdaq_listed_path=args.nasdaq_listed_path,
            other_listed_path=args.other_listed_path,
            as_of_date=args.as_of_date,
        )
        return 0

    if args.command == "stage":
        if args.target in {"prices", "all"}:
            stage_prices(input_paths=args.price_paths)
        if args.target in {"fundamentals", "all"}:
            stage_fundamentals(input_paths=args.fundamental_paths)
        if args.target in {"listed-security-seed", "all"}:
            stage_listed_security_seed(
                nasdaq_listed_paths=args.nasdaq_listed_paths,
                other_listed_paths=args.other_listed_paths,
            )
            stage_security_master_overrides(input_paths=args.override_paths)
        if args.target in {"benchmark-definitions", "all"}:
            stage_benchmark_definition_refresh(
                mapping_path=args.benchmark_mapping_path,
                custom_membership_path=args.custom_benchmark_membership_path,
            )
        if args.target in {"benchmark-memberships", "all"}:
            stage_benchmark_memberships(
                mapping_path=args.benchmark_mapping_path,
                custom_membership_path=args.custom_benchmark_membership_path,
            )
        if args.target in {"benchmark-sets", "all"}:
            stage_benchmark_sets(
                input_paths=args.benchmark_definition_paths,
                set_id=args.set_id,
            )
        if args.target in {"security-master", "all"}:
            stage_security_master(
                listed_security_paths=None,
                fundamentals_paths=args.fundamental_paths,
                benchmark_definition_paths=args.benchmark_definition_paths,
                override_paths=args.override_paths,
            )
        if args.target in {"benchmark-mappings", "all"}:
            stage_benchmark_mappings(mapping_path=args.benchmark_mapping_path)
        if args.target in {"security-ingestion-status", "all"}:
            stage_security_ingestion_status()
        if args.target in {"benchmark-membership-coverage", "all"}:
            stage_benchmark_membership_coverage(
                mapping_path=args.benchmark_mapping_path,
                custom_membership_path=args.custom_benchmark_membership_path,
            )
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2
