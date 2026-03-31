# Data Models

This document defines the first canonical schemas for the shared data foundation. The goal is to keep the MVP local-first while giving downstream analytics a stable contract and a storage path that can scale beyond CSV-only workflows.

## Design Rules

- Raw provider payloads stay in `data/raw/...`
- Canonical tabular outputs use snake_case field names
- Tickers and currencies are stored uppercase
- Prices are stored one row per ticker-date observation
- Fundamentals are stored as point-in-time snapshots, one row per ticker and `as_of_date`
- Holdings are stored as portfolio positions, one row per ticker and `as_of_date`
- CSV extracts remain useful for inspection and interchange, but DuckDB should become the canonical analytical storage layer for normalized and derived datasets

## Storage Strategy

Near-term storage should follow a layered pattern:

- raw provider payloads and source extracts stay on disk under `data/raw/...`
- normalized and derived analytical tables should be loadable into a project DuckDB database such as `data/warehouse/dumb_money.duckdb`
- CSV outputs can remain as optional materialized exports for debugging, fixtures, and manual inspection

Recommended near-term rule:

- treat DuckDB tables as the canonical store for scaled staging and research-ready marts
- treat CSVs as convenience outputs, not the long-term system of record for broad ticker coverage

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

### Security Master Expansion

The current `security_master` is sufficient for the first single-ticker workflow, but the next iteration should make it the broad reusable universe table for the repo.

Recommended next-step fields:

- `security_id`: internal stable identifier if we create one
- `ticker`
- `name`
- `asset_type`
- `exchange`
- `currency`
- `sector`
- `industry`
- `country`
- `is_benchmark`
- `is_active`
- `primary_listing`
- `source`
- `source_id`
- `first_seen_at`
- `last_updated_at`
- `notes`

Recommended source strategy:

- primary coverage source for broad symbol lists and listing metadata
- provider-enriched metadata source for sector, industry, country, and company descriptors
- manual override layer for aliases, benchmark exceptions, ADR handling, and classification cleanup

Purpose of the expanded `security_master`:

- define the eligible research universe
- drive which tickers should receive price and fundamentals ingestion
- support benchmark assignment, peer grouping, and future app search or filtering

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

### Near-Term Benchmark Mapping Follow-On

The current benchmark layer is sufficient for reusable default benchmark sets, but the first company research workflow has shown that we also need a lightweight benchmark assignment layer for reporting and scorecards.

Recommended near-term additions:

- a reusable company-to-benchmark mapping output that distinguishes:
  `primary_benchmark`, `sector_benchmark`, `style_benchmark`, and optional `custom_benchmark`
- a mapping table that can assign benchmark ETFs from company sector, industry, or a manual override
- explicit support for temporary proxies such as `Technology -> QQQ` during early report development

This should remain a shared data or transform output rather than notebook-only logic.

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

### Price Coverage Expansion

`normalized_prices` does not need a schema redesign right now, but it should expand in lockstep with `security_master`.

Recommended operating rule:

- the maintained security universe should determine which tickers are eligible for recurring price ingestion
- benchmark and custom basket constituents should use the same canonical price storage contract as company tickers

### Company Research Follow-On Price Outputs

The first company research workflow uses the normalized company price series together with raw benchmark price history. For reporting and charting, the repo will benefit from a unified research-ready market series contract.

Recommended derived outputs:

- indexed price series for company and benchmark comparisons
- drawdown series
- moving-average series such as `sma_50` and `sma_200`
- benchmark-aligned return panels for charting and table output

These do not necessarily need new raw ingestion, but they should become stable shared analytics or mart outputs rather than ad hoc notebook calculations.

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

### Historical Fundamentals Expansion

The current latest-snapshot model is enough for point-in-time scorecards, but not enough for historical balance sheet, income statement, or growth analysis.

Recommended next-step fields for a time-aware fundamentals contract:

- `ticker`
- `as_of_date`
- `period_end_date`
- `report_date`
- `fiscal_year`
- `fiscal_quarter`
- `fiscal_period`: values such as `Q1`, `Q2`, `Q3`, `Q4`, `FY`, `TTM`
- `period_type`: `quarterly`, `annual`, `ttm`
- existing metadata, operating, balance sheet, and valuation fields from `FundamentalSnapshot`
- lineage fields such as `raw_payload_path`, `pulled_at`, and provider source identifiers

Recommended conventions:

- preserve one row per ticker-period snapshot rather than one row per pull event only
- allow both quarterly and annual rows in the same canonical table
- keep `TTM` values explicit rather than blending them silently with quarterly or annual rows
- add balance-sheet-friendly fields when providers support them reliably, especially `current_assets`, `current_liabilities`, and any annualized balance sheet items needed for historical trend tables

Potential future field additions if providers support them reliably:

- `interest_expense`
- `current_assets`
- `current_liabilities`
- `invested_capital` or fields sufficient to derive ROIC
- any additional fields needed for historical valuation bands

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
- `benchmark_mappings`: default benchmark assignments by ticker, sector, industry, or rule
- `benchmark_memberships`: basket membership rows for custom benchmarks

Suggested stored columns for the Phase 2 staging outputs:

### normalized_prices

- same canonical columns as `PriceBar`
- produced from raw price extracts after type coercion, ticker and currency normalization, deduplication, and `adj_close` fallback to `close` when needed

### normalized_fundamentals

- same canonical columns as `FundamentalSnapshot`
- plus the recommended time-aware period fields:
  `period_end_date`, `report_date`, `fiscal_year`, `fiscal_quarter`, `fiscal_period`, `period_type`
- produced from raw flattened snapshots after type coercion, ticker and currency normalization, and one-row-per-`ticker`-and-`period snapshot` deduplication

### security_master

- same canonical columns as `Security`
- plus expansion fields such as:
  `security_id`, `is_active`, `primary_listing`, `source`, `source_id`, `first_seen_at`, `last_updated_at`, `notes`
- built from a broader universe source plus provider-enriched metadata and benchmark definitions
- intended to become the central eligible-universe table rather than a lightweight temporary join table

### security_master Follow-On Role

The current `security_master` already provides enough sector and industry metadata to support early company report assembly. Near-term reporting work suggests extending its practical role to support:

- benchmark assignment joins
- peer grouping seeds using sector and industry
- report metadata such as display names and classification context

### benchmark_sets

Canonical MVP columns:

- `set_id`: stable grouping identifier for a reusable benchmark set
- `benchmark_id`: stable benchmark definition identifier
- `ticker`: benchmark symbol
- `name`: display name
- `category`: benchmark category
- `scope`: optional grouping scope
- `currency`: benchmark currency
- `description`: free-text notes
- `member_order`: explicit display or processing order inside the set
- `is_default`: marks default packaged sets versus ad hoc sets

### benchmark_mapping

Recommended columns:

- `mapping_id`
- `ticker`
- `sector`
- `industry`
- `primary_benchmark`
- `sector_benchmark`
- `industry_benchmark`
- `style_benchmark`
- `custom_benchmark`
- `assignment_method`
- `priority`
- `is_active`
- `notes`

Purpose:

- standard benchmark assignment for company research and scorecards
- sector and industry benchmark coverage without notebook-specific rules

### benchmark_memberships

Recommended columns:

- `benchmark_id`
- `member_ticker`
- `member_type`: `security`, `etf`, `index`, `cash`, `custom_benchmark`
- `weight`
- `member_order`
- `start_date`
- `end_date`
- `notes`

Purpose:

- define custom benchmark baskets and reusable benchmark composites
- support single-stock benchmarks, ETF baskets, or mixed custom baskets through one contract

## DuckDB Analytical Storage

CSV-only staging will become difficult to manage as coverage expands. The repo should add a DuckDB-backed analytical layer for normalized and reusable derived outputs.

Recommended near-term canonical DuckDB tables:

- `normalized_prices`
- `normalized_fundamentals`
- `security_master`
- `benchmark_definitions`
- `benchmark_sets`
- `benchmark_mappings`
- `benchmark_memberships`

Recommended implementation conventions:

- raw source payloads remain on disk for auditability
- transforms should be able to materialize outputs both to DuckDB and to optional CSV exports
- tests can continue to use small CSV fixtures while validating DuckDB write and read paths
- notebook and analytics code should prefer shared loaders that can read from DuckDB first, with CSV fallback only where needed

## Company Research and Reporting Follow-On Outputs

The first end-to-end company workflow has identified a small set of reusable outputs that sit between staging and fully polished reports.

### benchmark_mapping

Suggested columns:

- `ticker`
- `primary_benchmark`
- `sector_benchmark`
- `industry_benchmark`
- `style_benchmark`
- `custom_benchmark`
- `assignment_method`
- `sector`
- `industry`
- `notes`

Purpose:

- keeps company-to-benchmark assignment out of notebook code
- supports scorecards, performance comparisons, and later sector research

### company_research_packet

This is currently a shared code artifact, not a stored tabular dataset. A future stored output could include:

- score summary
- category scores
- metric-level scores
- trailing return comparison table
- risk summary table
- benchmark assignments

Purpose:

- supports report generation, notebook review, and later app reuse

### peer_sets

Not required for the first `AAPL` workflow, but likely needed next for the fuller narrative and scorecard vision.

Suggested columns:

- `peer_set_id`
- `ticker`
- `peer_ticker`
- `relationship_type`
- `sector`
- `industry`
- `selection_method`
- `peer_order`

Purpose:

- supports peer-relative valuation, positioning tables, and percentile scoring
