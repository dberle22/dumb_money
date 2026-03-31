"""Command-line entry points for local ingestion workflows."""

from __future__ import annotations

import argparse
from datetime import date
from typing import Sequence

from dumb_money.ingestion import (
    ingest_benchmark_definitions,
    ingest_benchmark_prices,
    ingest_fundamentals,
    ingest_prices,
)
from dumb_money.transforms import (
    stage_benchmark_sets,
    stage_fundamentals,
    stage_prices,
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
    prices_parser.add_argument("--tickers", required=True, type=_csv_list)
    prices_parser.add_argument("--start-date", required=True, type=_date_value)
    prices_parser.add_argument("--end-date", required=True, type=_date_value)
    prices_parser.add_argument("--interval", default="1d")

    fundamentals_parser = subparsers.add_parser("fundamentals", help="Ingest fundamentals.")
    fundamentals_parser.add_argument("--tickers", required=True, type=_csv_list)
    fundamentals_parser.add_argument("--as-of-date", type=_date_value)

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

    stage_parser = subparsers.add_parser("stage", help="Build normalized staging datasets.")
    stage_parser.add_argument(
        "target",
        choices=["prices", "fundamentals", "benchmark-sets", "security-master", "all"],
    )
    stage_parser.add_argument("--price-paths", type=_path_list)
    stage_parser.add_argument("--fundamental-paths", type=_path_list)
    stage_parser.add_argument("--benchmark-definition-paths", type=_path_list)
    stage_parser.add_argument("--set-id", default="default_benchmarks")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "prices":
        ingest_prices(
            args.tickers,
            start_date=args.start_date,
            end_date=args.end_date,
            interval=args.interval,
        )
        return 0

    if args.command == "fundamentals":
        ingest_fundamentals(
            args.tickers,
            as_of_date=args.as_of_date,
        )
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

    if args.command == "stage":
        if args.target in {"prices", "all"}:
            stage_prices(input_paths=args.price_paths)
        if args.target in {"fundamentals", "all"}:
            stage_fundamentals(input_paths=args.fundamental_paths)
        if args.target in {"benchmark-sets", "all"}:
            stage_benchmark_sets(
                input_paths=args.benchmark_definition_paths,
                set_id=args.set_id,
            )
        if args.target in {"security-master", "all"}:
            stage_security_master(
                fundamentals_paths=args.fundamental_paths,
                benchmark_definition_paths=args.benchmark_definition_paths,
            )
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2
