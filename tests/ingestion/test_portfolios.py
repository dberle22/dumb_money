from datetime import date

import pandas as pd

from dumb_money.config import AppSettings
from dumb_money.ingestion.portfolios import ingest_portfolio_holdings, normalize_holdings_frame
from dumb_money.storage import read_canonical_table


def test_normalize_holdings_frame_supports_common_aliases_and_derives_weights() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["aapl", "msft"],
            "shares": [10, 5],
            "value": [2000, 1000],
            "account": ["Brokerage", "Brokerage"],
        }
    )

    normalized = normalize_holdings_frame(frame, portfolio_id="taxable", as_of_date=date(2024, 7, 1))

    assert normalized["ticker"].tolist() == ["AAPL", "MSFT"]
    assert normalized["portfolio_id"].tolist() == ["taxable", "taxable"]
    assert normalized["weight"].tolist() == [2 / 3, 1 / 3]


def test_ingest_portfolio_holdings_writes_canonical_table(tmp_path) -> None:
    settings = AppSettings(project_root=tmp_path)
    input_path = tmp_path / "holdings.csv"
    pd.DataFrame(
        {
            "ticker": ["VOO", "AAPL"],
            "quantity": [12.5, 8],
            "market_value": [6000, 4000],
            "as_of_date": ["2024-07-01", "2024-07-01"],
        }
    ).to_csv(input_path, index=False)

    holdings = ingest_portfolio_holdings(input_path, settings=settings, portfolio_id="default")
    loaded = read_canonical_table("portfolio_holdings", settings=settings)

    assert len(holdings) == 2
    assert loaded["ticker"].tolist() == ["AAPL", "VOO"]
    assert settings.raw_portfolios_dir.exists()

