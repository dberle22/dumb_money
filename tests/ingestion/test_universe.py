from __future__ import annotations

from dumb_money.config import AppSettings
from dumb_money.ingestion.universe import (
    ingest_listed_security_sources,
    load_nasdaq_listed_frame,
    load_other_listed_frame,
)


def test_load_symbol_directory_frames() -> None:
    nasdaq = load_nasdaq_listed_frame("tests/fixtures/universe/nasdaqlisted_sample.txt")
    other = load_other_listed_frame("tests/fixtures/universe/otherlisted_sample.txt")

    assert nasdaq["Symbol"].tolist() == ["MSFT", "QQQ", "ZVZZT"]
    assert other["ACT Symbol"].tolist() == ["IBM", "SPY", "BABA", "XYZU"]


def test_ingest_listed_security_sources_copies_files(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    outputs = ingest_listed_security_sources(
        nasdaq_listed_path="tests/fixtures/universe/nasdaqlisted_sample.txt",
        other_listed_path="tests/fixtures/universe/otherlisted_sample.txt",
        settings=settings,
        as_of_date="2026-03-31",
    )

    assert outputs["nasdaq_listed"].exists()
    assert outputs["other_listed"].exists()
