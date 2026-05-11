# Function Guide and Implementation

## Overview

This document explains how to use the current repository functions, outlines the canonical data models, and provides high-level implementation guidance for five priority use cases.

---

## Data Models

### Security Models

#### `dumb_money.models.security.Security`

Represents research universe entities such as stocks, ETFs, indexes, and other securities.

Fields:
- `security_id`: optional stable internal identifier
- `ticker`: primary identifier, uppercase
- `name`: company or fund name
- `asset_type`: one of `common_stock`, `etf`, `index`, `adr`, `mutual_fund`, `crypto`, `cash`, `other`
- `exchange`: listing exchange
- `primary_listing`: primary market listing
- `currency`: trading currency (default `USD`)
- `sector`: normalized sector label
- `industry`: normalized industry label
- `country`: issuer country
- `cik`: SEC CIK identifier
- `is_benchmark`: benchmark flag
- `is_active`: active universe flag
- `is_eligible_research_universe`: research eligibility
- `source`: metadata source
- `source_id`: provider identifier
- `first_seen_at`: first observed date
- `last_updated_at`: last update date
- `notes`: free-text notes

Usage example:
```python
from dumb_money.models.security import Security

security = Security(
    ticker="AAPL",
    name="Apple Inc.",
    asset_type="common_stock",
    exchange="NASDAQ",
    sector="Technology",
    industry="Consumer Electronics",
    is_eligible_research_universe=True
)
```

#### `dumb_money.models.security.BenchmarkDefinition`

Defines a benchmark reference item used by research workflows.

Fields:
- `benchmark_id`: stable benchmark identifier
- `ticker`: benchmark ticker
- `name`: display name
- `category`: one of `market`, `sector`, `industry`, `style`, `custom`
- `scope`: optional grouping tag
- `currency`: benchmark currency
- `inception_date`: optional metadata date
- `description`: optional notes

Usage example:
```python
from dumb_money.models.security import BenchmarkDefinition, BenchmarkCategory

benchmark = BenchmarkDefinition(
    benchmark_id="US_MARKET",
    ticker="SPY",
    name="S&P 500",
    category=BenchmarkCategory.MARKET,
    scope="us_large_cap"
)
```

### Market Data Models

#### `dumb_money.models.market_data.PriceBar`

Canonical OHLCV price record.

Fields:
- `ticker`: security symbol
- `date`: calendar date
- `interval`: data interval, default `1d`
- `source`: data provider enum (`yahooquery`, `yfinance`, `manual`)
- `currency`: currency code
- `open`, `high`, `low`, `close`, `adj_close`: price fields
- `volume`: volume value

Usage example:
```python
from datetime import date
from dumb_money.models.market_data import PriceBar, DataSource

price_bar = PriceBar(
    ticker="AAPL",
    date=date(2026, 4, 6),
    open=195.50,
    high=198.25,
    low=195.00,
    close=197.75,
    adj_close=197.75,
    volume=52000000,
    source=DataSource.YFINANCE
)
```

#### `dumb_money.models.market_data.FundamentalSnapshot`

Normalized point-in-time company fundamental snapshot.

Common fields:
- `ticker`
- `as_of_date`
- `period_end_date`, `report_date`, `fiscal_year`, `fiscal_quarter`, `fiscal_period`, `period_type`
- `source`, `currency`, `long_name`, `sector`, `industry`, `website`
- `market_cap`, `enterprise_value`, `total_debt`, `total_cash`, `shares_outstanding`
- `revenue`, `revenue_ttm`, `gross_profit`, `operating_income`, `net_income`, `ebitda`, `free_cash_flow`
- `basic_eps`, `diluted_eps`, `eps_trailing`, `eps_forward`
- `gross_margin`, `operating_margin`, `profit_margin`
- `return_on_equity`, `return_on_assets`, `return_on_invested_capital`
- `debt_to_equity`, `current_ratio`
- `trailing_pe`, `forward_pe`, `price_to_sales`, `ev_to_ebitda`, `dividend_yield`
- `raw_payload_path`, `pulled_at`

Usage example:
```python
from datetime import date
from dumb_money.models.market_data import FundamentalSnapshot, DataSource

snapshot = FundamentalSnapshot(
    ticker="AAPL",
    as_of_date=date(2026, 4, 6),
    revenue_ttm=383285000000,
    net_income=96995000000,
    market_cap=3000000000000,
    trailing_pe=32.5,
    return_on_equity=0.85,
    source=DataSource.YFINANCE
)
```

### Portfolio Model

#### `dumb_money.models.portfolio.Holding`

Point-in-time portfolio position record for portfolio fit workflows.

Fields:
- `portfolio_id`
- `ticker`
- `as_of_date`
- `quantity`
- `average_cost`
- `market_value`
- `weight`
- `account_name`
- `notes`

Usage example:
```python
from datetime import date
from dumb_money.models.portfolio import Holding

holding = Holding(
    portfolio_id="default",
    ticker="AAPL",
    as_of_date=date(2026, 4, 6),
    quantity=100,
    average_cost=150.0,
    market_value=19775.0,
    weight=0.25
)
```

---

## Core Functions & Workflows

### Company Research Workflow

#### `dumb_money.research.company.build_company_research_packet`

Builds the company research packet for one ticker.

Signature:
```python
build_company_research_packet(
    ticker: str,
    *,
    benchmark_set_id: str | None = None,
    settings: AppSettings | None = None,
) -> CompanyResearchPacket
```

Returned object: `CompanyResearchPacket` contains:
- `ticker`
- `company_name`
- `as_of_date`
- `company_history`
- `benchmark_histories`
- `return_windows`
- `trailing_return_comparison`
- `risk_metrics`
- `trend_metrics`
- `benchmark_comparison`
- `fundamentals_summary`
- `peer_return_comparison`
- `peer_return_summary_stats`
- `peer_valuation_comparison`
- `peer_summary_stats`
- `sector_snapshot`
- `scorecard`

Example usage:
```python
from dumb_money.research import build_company_research_packet

packet = build_company_research_packet("AAPL", benchmark_set_id="sample_universe")
print(packet.ticker, packet.company_name)
print(packet.scorecard.summary.overall_score)
```

### Analytics Helpers

#### `dumb_money.analytics.company.prepare_price_history`

Normalizes raw price history for a single ticker.

Inputs:
- `prices`: master price DataFrame
- `ticker`: ticker symbol

Outputs:
- DataFrame with `ticker`, `date`, `adj_close`, `daily_return`

#### `dumb_money.analytics.company.calculate_return_windows`

Calculates trailing total returns over standard windows.

Default windows:
- `1m`: 21 trading days
- `3m`: 63 trading days
- `6m`: 126 trading days
- `1y`: 252 trading days

#### `dumb_money.analytics.company.calculate_risk_metrics`

Computes risk metrics such as:
- annualized volatility
- maximum drawdown
- current drawdown from peak
- downside deviation
- beta vs. primary benchmark

#### `dumb_money.analytics.company.calculate_trend_metrics`

Computes trend and momentum indicators such as:
- `sma_20`, `sma_50`, `sma_200`
- `price_vs_sma_50`, `price_vs_sma_200`
- boolean trend flags

### Output Helpers

The `dumb_money.outputs` package exposes a set of table and chart builders.

Examples:
- `build_company_overview_table(packet)`
- `build_research_summary_table(packet)`
- `build_trailing_return_comparison_table(packet)`
- `build_risk_metric_table(packet)`
- `build_valuation_summary_table(packet)`
- `build_scorecard_metrics_table(packet)`
- `render_indexed_price_performance_chart(packet)`
- `render_drawdown_chart(packet)`
- `render_price_with_moving_averages_chart(packet)`
- `render_trailing_return_comparison_chart(packet)`
- `render_benchmark_excess_return_chart(packet)`
- `render_score_summary_strip(packet)`
- `render_score_decomposition_chart(packet)`
- `build_research_summary_text(packet)`
- `build_final_research_summary_text(packet)`
- `close_figure(figure)`

Example usage:
```python
from dumb_money.outputs import (
    build_company_overview_table,
    build_research_summary_table,
    render_indexed_price_performance_chart,
    close_figure,
)
from dumb_money.research import build_company_research_packet

packet = build_company_research_packet("AAPL")
print(build_company_overview_table(packet))
fig = render_indexed_price_performance_chart(packet)
close_figure(fig)
```

### Storage and Canonical Tables

#### `dumb_money.storage.read_canonical_table`

Loads standardized datasets from the DuckDB warehouse.

Common canonical table names:
- `normalized_prices`
- `normalized_fundamentals`
- `security_master`
- `benchmark_sets`
- `benchmark_mappings`
- `peer_sets`
- `sector_snapshots`
- `gold_ticker_metrics_mart`

Example usage:
```python
from dumb_money.storage import read_canonical_table

prices = read_canonical_table("normalized_prices")
fundamentals = read_canonical_table("normalized_fundamentals")
security_master = read_canonical_table("security_master")
```

#### `dumb_money.research.company.load_*` helpers

Convenience loaders defined in `src/dumb_money/research/company.py`:
- `load_staged_prices`
- `load_staged_fundamentals`
- `load_benchmark_set`
- `load_benchmark_prices`
- `load_security_master`
- `load_benchmark_mappings`
- `load_peer_sets`
- `load_sector_snapshots`
- `load_gold_ticker_metrics_mart`
- `load_gold_ticker_metrics_row`

These helpers read from canonical tables and support packet assembly.

---

## Use Case Implementation Guide

### Use Case 1: Automated Stock Screening and Alerts

Goal: identify candidates with attractive valuation and quality signals, then generate alerts.

Implementation steps:
1. Load the security universe from `security_master`.
2. Iterate tickers and build `CompanyResearchPacket` for each.
3. Extract screening metrics from `packet.fundamentals_summary`, `packet.scorecard`, and `packet.risk_metrics`.
4. Save candidates to CSV or a tracking mart.
5. Send alerts when candidates meet desired thresholds.

Example pattern:
```python
from dumb_money.research import build_company_research_packet
from dumb_money.storage import read_canonical_table
import pandas as pd

security_master = read_canonical_table("security_master")
eligible = security_master[security_master["is_eligible_research_universe"] == True]["ticker"].tolist()

results = []
for ticker in eligible[:50]:
    try:
        packet = build_company_research_packet(ticker)
        pe = packet.fundamentals_summary.get("trailing_pe")
        roe = packet.fundamentals_summary.get("return_on_equity")
        growth = packet.fundamentals_summary.get("revenue_growth")
        if pe and pe < 20 and roe and roe > 0.15 and growth and growth > 0.1:
            results.append({
                "ticker": ticker,
                "pe": pe,
                "roe": roe,
                "growth": growth,
                "overall_score": packet.scorecard.summary.overall_score,
            })
    except Exception:
        continue

screen_df = pd.DataFrame(results).sort_values("overall_score", ascending=False)
print(screen_df.head())
```

Suggested playlist:
- schedule the screener daily after market close
- write results to `data/runs/` or a DuckDB mart
- compare candidates against historical screening outcomes
- add notification logic for email / Slack alerts

Data sources:
- `normalized_fundamentals`
- `normalized_prices`
- `gold_ticker_metrics_mart`

---

### Use Case 2: Sector Rotation Strategy Backtesting

Goal: analyze sector ETF returns and test rotation rules.

Implementation steps:
1. Load price history from `normalized_prices`.
2. Define sector ETF tickers or use `benchmark_sets`.
3. Calculate trailing returns by window with `calculate_return_windows`.
4. Rank sectors and simulate monthly or quarterly rebalancing.
5. Visualize cumulative returns and compare to market.

Example pattern:
```python
from dumb_money.storage import read_canonical_table
from dumb_money.analytics.company import prepare_price_history, calculate_return_windows
import pandas as pd

prices = read_canonical_table("normalized_prices")
sector_etfs = {"Technology": "XLK", "Healthcare": "XLV", "Energy": "XLE"}

sector_returns = []
for sector, ticker in sector_etfs.items():
    history = prepare_price_history(prices, ticker)
    if history.empty:
        continue
    returns = calculate_return_windows(history)
    returns["sector"] = sector
    sector_returns.append(returns)

sector_df = pd.concat(sector_returns, ignore_index=True)
print(sector_df[sector_df["window"] == "1m"].sort_values("total_return", ascending=False))
```

Suggested extensions:
- apply equal-weight or top-N allocation rules
- calculate benchmark-relative performance
- track drawdown and volatility for each rotation basket
- compare rebalancing frequencies

Data sources:
- `normalized_prices`
- `benchmark_sets`

---

### Use Case 3: Portfolio Optimization Dashboard

Goal: build an interactive dashboard to evaluate portfolio fit and candidate impact.

Implementation steps:
1. Build a Streamlit app under `app/streamlit/`.
2. Allow upload of current holdings and candidate tickers.
3. Compute exposures, concentration, and correlation using normalized prices.
4. Show candidate metrics from `build_company_research_packet()`.
5. Display before/after sector exposure and fit indicators.

Example pattern:
```python
import streamlit as st
import pandas as pd
from dumb_money.research import build_company_research_packet

st.title("Portfolio Fit Analyzer")
file = st.file_uploader("Upload holdings CSV")
if file:
    holdings = pd.read_csv(file)
    st.dataframe(holdings)

candidate = st.text_input("Candidate ticker", "AAPL")
if st.button("Analyze"):
    packet = build_company_research_packet(candidate)
    st.write(packet.scorecard.summary)
```

Suggested capabilities:
- correlation heatmaps between candidate and holdings
- portfolio concentration metrics
- sector exposure before/after candidate addition
- candidate ranking vs. watchlist

Data sources:
- `normalized_prices`
- `gold_ticker_metrics_mart`
- user portfolio input

---

### Use Case 4: Sentiment-Driven Research Reports

Goal: weave news sentiment and event tagging into company research.

Implementation steps:
1. Ingest news into a table such as `news_articles`.
2. Score sentiment and tag event types.
3. Aggregate article counts and sentiment over trailing windows.
4. Add sentiment context to the company research packet.
5. Generate narrative summaries based on sentiment and key events.

Example pattern:
```python
from dumb_money.storage import read_canonical_table
from dumb_money.research import build_company_research_packet

news = read_canonical_table("news_articles")
packet = build_company_research_packet("AAPL")
aapl_news = news[news["ticker"] == "AAPL"].sort_values("publish_date", ascending=False)
print(aapl_news.head())
```

Suggested outputs:
- sentiment trend chart
- event counts by category
- sentiment input into scorecard
- summary paragraphs describing recent catalyst tone

Data sources:
- self-hosted news ingestion
- `normalized_prices`
- company research packet

---

### Use Case 5: Custom Benchmark Construction

Goal: build and compare candidate stocks against a strategy-specific benchmark.

Implementation steps:
1. Define a custom benchmark as a curated ticker set.
2. Use normalized prices to calculate constituent returns.
3. Build an equal-weight or custom-weight composite.
4. Store benchmark definitions in a DuckDB canonical reference table.
5. Use the custom benchmark in research comparisons.

Example pattern:
```python
from dumb_money.models.security import BenchmarkDefinition, BenchmarkCategory
from dumb_money.storage import read_canonical_table
from dumb_money.analytics.company import prepare_price_history, calculate_return_windows
import pandas as pd

benchmark_tickers = ["AAPL", "MSFT", "NVDA"]
prices = read_canonical_table("normalized_prices")

comps = []
for ticker in benchmark_tickers:
    history = prepare_price_history(prices, ticker)
    if not history.empty:
        comp = calculate_return_windows(history)
        comp["ticker"] = ticker
        comps.append(comp)

composite = pd.concat(comps, ignore_index=True)
print(composite.groupby("window")["total_return"].mean())
```

Suggested additions:
- custom benchmark metadata table
- periodic reconstitution rules
- benchmark scorecard for strategy tracking

Data sources:
- `normalized_prices`
- `security_master`
- curated reference lists

---

## Quick Start Examples

### Generate a Stock Research Report

```python
from dumb_money.research import build_company_research_packet
from dumb_money.outputs import (
    build_company_overview_table,
    build_research_summary_table,
    render_indexed_price_performance_chart,
    close_figure,
)

packet = build_company_research_packet("AAPL")
print(build_company_overview_table(packet))
print(build_research_summary_table(packet))
fig = render_indexed_price_performance_chart(packet)
close_figure(fig)
```

### Batch Process Multiple Tickers

```python
from dumb_money.research import build_company_research_packet
import pandas as pd

results = []
for ticker in ["AAPL", "MSFT", "GOOGL", "TSLA"]:
    try:
        packet = build_company_research_packet(ticker)
        results.append({
            "ticker": ticker,
            "company_name": packet.company_name,
            "score": packet.scorecard.summary.overall_score,
            "sector": packet.fundamentals_summary.get("sector"),
        })
    except Exception as exc:
        print(f"{ticker} failed: {exc}")

print(pd.DataFrame(results))
```

### Access Raw Canonical Data

```python
from dumb_money.storage import read_canonical_table

prices = read_canonical_table("normalized_prices")
fundamentals = read_canonical_table("normalized_fundamentals")
security_master = read_canonical_table("security_master")
```

---

## Configuration

The package uses `dumb_money.config.AppSettings` and `get_settings()` for environment-specific values.

Common settings include:
- `warehouse_path`
- `data_dir`
- `normalized_prices_table`
- `normalized_fundamentals_table`
- `security_master_table`

Example:
```python
from dumb_money.config import get_settings

settings = get_settings()
print(settings.warehouse_path)
```

---

## Notes

- `build_company_research_packet()` is the primary reusable entry point for company-level analysis.
- The `outputs` package is intended for notebooks and report rendering.
- The canonical data models are built on Pydantic to enforce schema consistency.
- The `storage` layer is the place to read shared datasets and to extend support for new canonical tables.

---

## Next Steps

1. Add news ingestion and sentiment tables.
2. Expand the sector and portfolio workflows with explicit `sector_snapshots` and `portfolio_fit` mart outputs.
3. Build a `Streamlit` app under `app/streamlit/` for interactive portfolio analysis.
4. Add benchmark sets and custom benchmark metadata as canonical tables.
5. Use this guide as the reference for applying the current repo functions and data models.
