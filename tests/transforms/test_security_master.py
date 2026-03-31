from pathlib import Path

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.transforms.benchmark_sets import build_benchmark_sets_frame, stage_benchmark_sets
from dumb_money.transforms.security_master import build_security_master_frame, stage_security_master


def test_build_security_master_frame_stitches_benchmark_flags() -> None:
    fundamentals = pd.read_csv("tests/fixtures/fundamentals/aapl_fundamentals_flat_2024-06-30.csv")
    benchmarks = pd.read_csv("tests/fixtures/benchmarks/default_benchmarks.csv")

    frame = build_security_master_frame(fundamentals, benchmarks)

    aapl = frame.loc[frame["ticker"] == "AAPL"].iloc[0].to_dict()
    spy = frame.loc[frame["ticker"] == "SPY"].iloc[0].to_dict()

    assert aapl["name"] == "Apple Inc."
    assert aapl["is_benchmark"] is False
    assert spy["is_benchmark"] is True


def test_build_benchmark_sets_frame_creates_reusable_membership_table() -> None:
    definitions = pd.read_csv("tests/fixtures/benchmarks/default_benchmarks.csv")

    frame = build_benchmark_sets_frame(definitions, set_id="core_us")

    assert frame["set_id"].unique().tolist() == ["core_us"]
    assert frame["member_order"].tolist() == [1, 2]


def test_stage_security_master_and_benchmark_sets_write_outputs(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    normalized_fundamentals_path = tmp_path / "normalized_fundamentals.csv"
    benchmark_defs_path = tmp_path / "default_benchmark_definitions_20240630.csv"

    normalized_fundamentals_path.write_text(
        Path("tests/fixtures/fundamentals/aapl_fundamentals_flat_2024-06-30.csv").read_text()
    )
    benchmark_defs_path.write_text(Path("tests/fixtures/benchmarks/default_benchmarks.csv").read_text())

    stage_benchmark_sets(input_paths=[benchmark_defs_path], settings=settings, set_id="core_us")
    frame = stage_security_master(
        fundamentals_paths=[normalized_fundamentals_path],
        benchmark_definition_paths=[benchmark_defs_path],
        settings=settings,
    )

    assert sorted(frame["ticker"].tolist()) == ["AAPL", "QQQ", "SPY"]
    assert (settings.security_master_dir / "security_master.csv").exists()
    assert (settings.benchmark_sets_dir / "benchmark_sets.csv").exists()
