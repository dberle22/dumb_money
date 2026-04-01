from __future__ import annotations

import pandas as pd
import pytest

from dumb_money.validation.security_master import (
    build_security_master_validation_issues,
    validate_security_master_frame,
)


def test_validate_security_master_frame_rejects_duplicates() -> None:
    frame = pd.DataFrame(
        [
            {"ticker": "AAPL", "asset_type": "common_stock", "exchange": "Nasdaq", "is_eligible_research_universe": True},
            {"ticker": "AAPL", "asset_type": "common_stock", "exchange": "Nasdaq", "is_eligible_research_universe": True},
        ]
    )

    with pytest.raises(ValueError):
        validate_security_master_frame(frame)


def test_build_security_master_validation_issues_flags_missing_exchange() -> None:
    frame = pd.DataFrame(
        [
            {"ticker": "AAPL", "asset_type": "common_stock", "exchange": None, "is_eligible_research_universe": True},
        ]
    )

    issues = build_security_master_validation_issues(frame)

    assert issues["issue_type"].tolist() == ["missing_exchange"]
