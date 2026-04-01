from __future__ import annotations

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.ingestion.universe import load_nasdaq_listed_frame, load_other_listed_frame
from dumb_money.transforms.security_universe import (
    normalize_listed_security_seed_frame,
    normalize_security_master_overrides,
    stage_listed_security_seed,
    stage_security_master_overrides,
)


def test_normalize_listed_security_seed_frame_derives_eligibility() -> None:
    nasdaq = load_nasdaq_listed_frame("tests/fixtures/universe/nasdaqlisted_sample.txt")
    other = load_other_listed_frame("tests/fixtures/universe/otherlisted_sample.txt")

    frame = normalize_listed_security_seed_frame(nasdaq, other, as_of_date="2026-03-31")

    assert bool(frame.loc[frame["ticker"] == "MSFT", "is_eligible_research_universe"].iloc[0])
    assert frame.loc[frame["ticker"] == "QQQ", "eligibility_reason"].iloc[0] == "excluded_etf"
    assert frame.loc[frame["ticker"] == "ZVZZT", "eligibility_reason"].iloc[0] == "test_issue"
    assert frame.loc[frame["ticker"] == "BABA", "asset_type_raw"].iloc[0] == "adr"
    assert frame.loc[frame["ticker"] == "XYZU", "asset_type_raw"].iloc[0] == "unit"


def test_stage_listed_security_seed_and_overrides_write_outputs(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    overrides_path = tmp_path / "security_master_overrides.csv"
    overrides_path.write_text(
        "ticker,field_name,override_value,reason,updated_at\n"
        "BABA,is_eligible_research_universe,true,include adr,2026-03-31\n"
    )

    frame = stage_listed_security_seed(
        nasdaq_listed_paths=["tests/fixtures/universe/nasdaqlisted_sample.txt"],
        other_listed_paths=["tests/fixtures/universe/otherlisted_sample.txt"],
        settings=settings,
        as_of_date="2026-03-31",
    )
    overrides = stage_security_master_overrides(input_paths=[overrides_path], settings=settings)

    assert sorted(frame["ticker"].tolist()) == ["BABA", "IBM", "MSFT", "QQQ", "SPY", "XYZU", "ZVZZT"]
    assert overrides.loc[0, "ticker"] == "BABA"
    assert (settings.listed_security_seed_dir / "listed_security_seed.csv").exists()
    assert (settings.reference_dir / "security_master_overrides.csv").exists()


def test_normalize_security_master_overrides_requires_columns() -> None:
    invalid = pd.DataFrame({"ticker": ["AAPL"]})

    try:
        normalize_security_master_overrides(invalid)
    except ValueError as exc:
        assert "required columns" in str(exc)
    else:
        raise AssertionError("expected override normalization to validate columns")
