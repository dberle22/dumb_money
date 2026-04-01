"""Validation checks for listed-security seed and security master tables."""

from __future__ import annotations

import pandas as pd

from dumb_money.models import AssetType


def _issue_rows(
    rows: pd.DataFrame,
    *,
    severity: str,
    issue_type: str,
    message: str,
    ticker_column: str = "ticker",
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    for row in rows.to_dict(orient="records"):
        issues.append(
            {
                "severity": severity,
                "issue_type": issue_type,
                "ticker": str(row.get(ticker_column, "")),
                "message": message,
            }
        )
    return issues


def build_seed_validation_issues(frame: pd.DataFrame) -> pd.DataFrame:
    """Return validation issues for a listed-security seed frame."""

    if frame.empty:
        return pd.DataFrame(columns=["severity", "issue_type", "ticker", "message"])

    issues: list[dict[str, str]] = []
    issues.extend(
        _issue_rows(
            frame.loc[frame["ticker"].astype(str).str.strip().eq("")],
            severity="error",
            issue_type="blank_ticker",
            message="listed security seed contains a blank ticker",
        )
    )
    duplicated = frame.loc[frame.duplicated(subset=["ticker"], keep=False)]
    issues.extend(
        _issue_rows(
            duplicated,
            severity="error",
            issue_type="duplicate_ticker",
            message="listed security seed contains duplicate tickers",
        )
    )
    unknown_exchange = frame.loc[frame["exchange"].isna() | frame["exchange"].astype(str).str.strip().eq("")]
    issues.extend(
        _issue_rows(
            unknown_exchange,
            severity="warning",
            issue_type="missing_exchange",
            message="listed security seed is missing exchange metadata",
        )
    )
    return pd.DataFrame(issues, columns=["severity", "issue_type", "ticker", "message"])


def validate_listed_security_seed_frame(frame: pd.DataFrame) -> None:
    """Raise if a listed-security seed frame has validation errors."""

    issues = build_seed_validation_issues(frame)
    errors = issues.loc[issues["severity"] == "error"]
    if not errors.empty:
        sample = errors.head(5).to_dict(orient="records")
        raise ValueError(f"listed security seed validation failed: {sample}")


def build_security_master_validation_issues(frame: pd.DataFrame) -> pd.DataFrame:
    """Return validation issues for a security master frame."""

    if frame.empty:
        return pd.DataFrame(columns=["severity", "issue_type", "ticker", "message"])

    issues: list[dict[str, str]] = []
    issues.extend(
        _issue_rows(
            frame.loc[frame["ticker"].astype(str).str.strip().eq("")],
            severity="error",
            issue_type="blank_ticker",
            message="security master contains a blank ticker",
        )
    )
    duplicated = frame.loc[frame.duplicated(subset=["ticker"], keep=False)]
    issues.extend(
        _issue_rows(
            duplicated,
            severity="error",
            issue_type="duplicate_ticker",
            message="security master contains duplicate tickers",
        )
    )
    invalid_asset_type = frame.loc[~frame["asset_type"].astype(str).isin({item.value for item in AssetType})]
    issues.extend(
        _issue_rows(
            invalid_asset_type,
            severity="error",
            issue_type="invalid_asset_type",
            message="security master contains an unsupported asset_type",
        )
    )
    missing_exchange = frame.loc[
        frame["is_eligible_research_universe"].fillna(False)
        & (frame["exchange"].isna() | frame["exchange"].astype(str).str.strip().eq(""))
    ]
    issues.extend(
        _issue_rows(
            missing_exchange,
            severity="warning",
            issue_type="missing_exchange",
            message="eligible security is missing exchange metadata",
        )
    )
    return pd.DataFrame(issues, columns=["severity", "issue_type", "ticker", "message"])


def validate_security_master_frame(frame: pd.DataFrame) -> None:
    """Raise if a security master frame has validation errors."""

    issues = build_security_master_validation_issues(frame)
    errors = issues.loc[issues["severity"] == "error"]
    if not errors.empty:
        sample = errors.head(5).to_dict(orient="records")
        raise ValueError(f"security master validation failed: {sample}")
