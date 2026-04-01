from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.transforms.fundamentals import stage_fundamentals
from dumb_money.transforms.security_universe import (
    stage_listed_security_seed,
    stage_security_master_overrides,
)
from dumb_money.transforms.benchmark_sets import build_benchmark_sets_frame, stage_benchmark_sets
from dumb_money.transforms.security_master import build_security_master_frame, stage_security_master


def test_build_security_master_frame_stitches_benchmark_flags() -> None:
    listed_security_seed = stage_listed_security_seed(
        nasdaq_listed_paths=["tests/fixtures/universe/nasdaqlisted_sample.txt"],
        other_listed_paths=["tests/fixtures/universe/otherlisted_sample.txt"],
        settings=AppSettings(project_root=Path("tests").resolve().parents[0]),
        as_of_date="2026-03-31",
        write_csv=False,
        write_warehouse=False,
    )
    fundamentals = pd.read_csv("tests/fixtures/fundamentals/aapl_fundamentals_flat_2024-06-30.csv")
    benchmarks = pd.read_csv("tests/fixtures/benchmarks/default_benchmarks.csv")

    frame = build_security_master_frame(listed_security_seed, fundamentals, benchmarks)

    aapl = frame.loc[frame["ticker"] == "AAPL"].iloc[0].to_dict()
    spy = frame.loc[frame["ticker"] == "SPY"].iloc[0].to_dict()
    msft = frame.loc[frame["ticker"] == "MSFT"].iloc[0].to_dict()

    assert aapl["name"] == "Apple Inc."
    assert aapl["is_benchmark"] is False
    assert spy["is_benchmark"] is True
    assert msft["is_eligible_research_universe"] is True


def test_build_benchmark_sets_frame_creates_reusable_membership_table() -> None:
    definitions = pd.read_csv("tests/fixtures/benchmarks/default_benchmarks.csv")

    frame = build_benchmark_sets_frame(definitions, set_id="core_us")

    assert frame["set_id"].unique().tolist() == ["core_us"]
    assert frame["member_order"].tolist() == [1, 2]


def test_stage_security_master_and_benchmark_sets_write_outputs(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    raw_fundamentals_path = tmp_path / "aapl_fundamentals_flat_2024-06-30.csv"
    benchmark_defs_path = tmp_path / "default_benchmark_definitions_20240630.csv"
    overrides_path = tmp_path / "security_master_overrides.csv"

    raw_fundamentals_path.write_text(
        Path("tests/fixtures/fundamentals/aapl_fundamentals_flat_2024-06-30.csv").read_text()
    )
    benchmark_defs_path.write_text(Path("tests/fixtures/benchmarks/default_benchmarks.csv").read_text())
    overrides_path.write_text(
        "ticker,field_name,override_value,reason,updated_at\n"
        "IBM,is_eligible_research_universe,true,include core industrial,2026-03-31\n"
    )

    stage_listed_security_seed(
        nasdaq_listed_paths=["tests/fixtures/universe/nasdaqlisted_sample.txt"],
        other_listed_paths=["tests/fixtures/universe/otherlisted_sample.txt"],
        settings=settings,
        as_of_date="2026-03-31",
    )
    stage_fundamentals(
        input_paths=[raw_fundamentals_path],
        settings=settings,
        write_csv=False,
    )
    stage_security_master_overrides(input_paths=[overrides_path], settings=settings)
    stage_benchmark_sets(input_paths=[benchmark_defs_path], settings=settings, set_id="core_us")
    frame = stage_security_master(
        benchmark_definition_paths=[benchmark_defs_path],
        settings=settings,
    )

    assert "AAPL" in frame["ticker"].tolist()
    assert "IBM" in frame["ticker"].tolist()
    assert bool(frame.loc[frame["ticker"] == "IBM", "is_eligible_research_universe"].iloc[0])
    assert (settings.security_master_dir / "security_master.csv").exists()
    assert (settings.benchmark_sets_dir / "benchmark_sets.csv").exists()
    assert settings.warehouse_path.exists()
