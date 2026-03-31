# Data Models

This document defines the first canonical schemas for the shared data foundation. The goal is to keep the MVP local-first and file-based while giving downstream analytics a stable contract.

## Design Rules

- Raw provider payloads stay in `data/raw/...`
- Canonical tabular outputs use snake_case field names
- Tickers and currencies are stored uppercase
- Prices are stored one row per ticker-date observation
- Fundamentals are stored as point-in-time snapshots, one row per ticker and `as_of_date`
- Holdings are stored as portfolio positions, one row per ticker and `as_of_date`

## Security

Canonical model: `dumb_money.models.security.Security`

Fields:

- `ticker`: primary research identifier, uppercase
- `name`: company or fund name
- `asset_type`: `common_stock`, `etf`, `index`, `adr`, `mutual_fund`, `crypto`, `cash`, `other`
- `exchange`: listing exchange when known
- `currency`: trading currency
- `sector`: normalized sector label when known
- `industry`: normalized industry label when known
- `country`: issuer country when known
- `is_benchmark`: marks records that primarily serve as benchmark members

Usage:

- forms the initial security master
- supports company, peer, sector, and benchmark joins

## Benchmarks

Canonical model: `dumb_money.models.security.BenchmarkDefinition`

Fields:

- `benchmark_id`: stable identifier for the benchmark definition
- `ticker`: tradeable symbol or index symbol
- `name`: display name
- `category`: `market`, `sector`, `industry`, `style`, `custom`
- `scope`: optional free-text grouping such as `us_large_cap`
- `currency`: benchmark currency
- `inception_date`: optional metadata field
- `description`: optional notes

Usage:

- supports benchmark set definitions for company and portfolio comparison
- allows later grouping logic without hard-coding symbols deep inside analytics modules

## Prices

Canonical model: `dumb_money.models.market_data.PriceBar`

Stored columns:

- `ticker`
- `date`
- `interval`
- `source`
- `currency`
- `open`
- `high`
- `low`
- `close`
- `adj_close`
- `volume`

Conventions:

- one row per ticker-date
- sorted by `ticker`, `date`
- `adj_close` is required; if a provider does not supply it, MVP logic falls back to `close`
- schema is source-agnostic after normalization, even if raw provider columns differ

Expected raw file layout:

- individual extracts: `data/raw/prices/{ticker}_{start}_{end}_{interval}.csv`
- combined extract: `data/raw/prices/combined_prices_{start}_{end}_{interval}.csv`

## Fundamentals

Canonical model: `dumb_money.models.market_data.FundamentalSnapshot`

Stored columns:

- identifiers and metadata:
  `ticker`, `as_of_date`, `source`, `currency`, `long_name`, `sector`, `industry`, `website`
- size and capital structure:
  `market_cap`, `enterprise_value`, `total_debt`, `total_cash`, `shares_outstanding`
- income and cash flow:
  `revenue_ttm`, `gross_profit`, `operating_income`, `net_income`, `ebitda`, `free_cash_flow`
- per-share and return metrics:
  `eps_trailing`, `eps_forward`, `return_on_equity`, `return_on_assets`
- margins and leverage:
  `gross_margin`, `operating_margin`, `profit_margin`, `debt_to_equity`, `current_ratio`
- valuation:
  `trailing_pe`, `forward_pe`, `price_to_sales`, `ev_to_ebitda`, `dividend_yield`
- lineage:
  `raw_payload_path`, `pulled_at`

Conventions:

- one row per ticker per pull date
- flattened from provider payload blocks such as `price`, `summary_detail`, `key_stats`, `financial_data`, and `asset_profile`
- preserves a pointer to the raw JSON payload so later transforms remain auditable

Expected raw file layout:

- raw payload: `data/raw/fundamentals/{ticker}_fundamentals_raw_{as_of_date}.json`
- flattened snapshot: `data/raw/fundamentals/{ticker}_fundamentals_flat_{as_of_date}.csv`

## Holdings

Canonical model: `dumb_money.models.portfolio.Holding`

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

Conventions:

- one row per held security and as-of date
- `weight` is expressed as a fraction between `0` and `1`
- cost basis and market value are optional because early imports may only provide positions

## Phase 2 Follow-On Outputs

These schemas are enough to begin the earliest normalization layer:

- `normalized_prices`: canonical price tables derived from raw extracts
- `normalized_fundamentals`: canonical fundamentals snapshots derived from raw payloads
- `security_master`: initial stitched security metadata table
- `benchmark_sets`: grouped benchmark membership definitions

These outputs should remain file-based for the MVP before introducing DuckDB or a heavier persistence layer.
