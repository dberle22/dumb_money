# End-to-End Flow: Dumb Money Research Pipeline

This document walks through the complete pipeline from raw data ingestion to a finished research
report, maps every function call to the file that contains it, explains what each layer does and
why it exists, and marks what is built versus what is still to come.

---

## Mental Model: The Five Layers

```
Raw Provider Data  (yfinance API + local files)
      ↓  [Ingestion]
data/raw/           (JSON / CSV files on disk)
      ↓  [Transforms / Staging]
DuckDB Warehouse    (normalized, analytical tables)
      ↓  [Research + Analytics]
CompanyResearchPacket  (in-memory, per-ticker struct)
      ↓  [Outputs]
Report sections + saved artifacts  (CSV / PNG / HTML)
```

Each layer has a single job. You never skip layers going down, and you never reach up to a
lower layer from above (analytics does not write to DuckDB; ingestion does not call scorecard logic).

---

## Layer 1: Ingestion — Raw Data → `data/raw/`

**What it does:** calls a market data provider (yfinance by default), saves the response to disk
as-is so we have a reproducible audit trail.

**Where it lives:** `src/dumb_money/ingestion/`

| File | What it owns |
|---|---|
| `prices.py` | Daily OHLCV price history per ticker |
| `fundamentals.py` | Income statement, balance sheet, valuation multiples |
| `benchmarks.py` | Benchmark ETF definitions and benchmark price history |
| `universe.py` | Listed-security source files from NASDAQ/FINRA |
| `portfolios.py` | Holdings CSV ingestion into the canonical holdings table |

### Key functions

**`ingest_prices(ticker, start_date, end_date, interval, settings)`**
→ `ingestion/prices.py`
Calls yfinance for one ticker, normalizes to `PRICE_COLUMNS`, writes a CSV to
`data/raw/prices/{ticker}_{start}_{end}_{interval}.csv`, and upserts into the `normalized_prices`
DuckDB table. Returns a normalized DataFrame.

**`ingest_selected_prices(tickers, ticker_query_sql, start_date, end_date, interval)`**
→ `ingestion/prices.py`
Batch wrapper. Accepts either an explicit list of tickers or a SQL query against the DuckDB
warehouse (e.g. `SELECT ticker FROM benchmark_memberships WHERE benchmark_ticker = 'IWM'`).
Loops over resolved tickers and calls `ingest_prices` for each.

**`ingest_fundamentals(ticker, as_of_date, settings)`**
→ `ingestion/fundamentals.py`
Calls yfinance for income statement, balance sheet, cashflow, and key stats. Saves raw JSON to
`data/raw/fundamentals/` and a flat CSV. Normalizes into period-aware rows
(`quarterly`, `annual`, `ttm`) and upserts into `normalized_fundamentals`.

**`ingest_selected_fundamentals(tickers, ticker_query_sql, as_of_date)`**
→ `ingestion/fundamentals.py`
Batch wrapper for fundamentals, same pattern as `ingest_selected_prices`.

**`ingest_benchmark_definitions(tickers, label, as_of_date)`**
→ `ingestion/benchmarks.py`
Fetches metadata for benchmark ETFs (SPY, QQQ, XLK, etc.) and writes to
`data/raw/benchmark_definitions/`. Normalizes into `BenchmarkDefinition` rows and upserts
into the `benchmark_definitions` DuckDB table.

**`ingest_benchmark_prices(tickers, start_date, end_date, interval, label)`**
→ `ingestion/benchmarks.py`
Fetches price history for benchmark ETFs. Follows the same pattern as `ingest_prices` but writes
into `data/raw/benchmark_prices/` and upserts into `normalized_prices` (same table, same schema —
benchmarks are just tickers).

**`ingest_portfolio_holdings(input_path, portfolio_id, as_of_date)`**
→ `ingestion/portfolios.py`
Reads a holdings CSV, normalizes columns via `HOLDING_COLUMN_ALIASES`, validates with the
`Holding` Pydantic model, copies the source file to `data/raw/portfolios/`, and upserts into
`portfolio_holdings`.

### CLI entry points

All ingestion is reachable through `python -m dumb_money.cli.main` (or `dumb-money` if installed):

```bash
# Ingest prices for specific tickers
dumb-money prices --tickers TEAM,AAPL --start-date 2021-01-01 --end-date 2026-07-13

# Ingest prices for an entire benchmark universe via SQL
dumb-money prices --ticker-query-sql "SELECT ticker FROM benchmark_memberships WHERE benchmark_ticker='IWM'" \
  --start-date 2021-01-01 --end-date 2026-07-13

# Ingest fundamentals
dumb-money fundamentals --tickers TEAM --as-of-date 2026-07-13

# Ingest benchmark definitions and prices
dumb-money benchmarks --tickers SPY,QQQ,XLK --start-date 2021-01-01 --end-date 2026-07-13

# Import a holdings CSV
dumb-money portfolio-import --input-path ~/holdings.csv --portfolio-id main --as-of-date 2026-07-13
```

There is also a `ingest-basket` command for ingesting an entire ETF's constituent universe in
batches (used for IWM, SPY, etc.). It uses `ingestion/universe.py` to plan batches,
track which have been ingested, and resume incomplete runs.

### Output on disk after ingestion

```
data/raw/
  prices/
    team_20210402_20260401_1d.csv
  fundamentals/
    team_fundamentals_raw_2026-04-01.json
    team_fundamentals_flat_2026-04-01.csv
  benchmark_prices/
    spy_20210101_20260713_1d.csv
  benchmark_definitions/
    benchmark_definitions_default_2026-04-01.csv
  portfolios/
    holdings_main_2026-07-13.csv
```

---

## Layer 2: Transforms / Staging — `data/raw/` → DuckDB Warehouse

**What it does:** reads raw files, applies normalization rules, and writes canonical tables into
the DuckDB warehouse at `data/warehouse/dumb_money.duckdb`. This is where schema enforcement,
deduplication, and derived field calculation happen.

**Where it lives:** `src/dumb_money/transforms/`

| File | What it owns |
|---|---|
| `prices.py` | Normalize and stage `normalized_prices` |
| `fundamentals.py` | Normalize and stage `normalized_fundamentals` |
| `security_master.py` | Build and stage `security_master` |
| `benchmark_sets.py` | Build and stage `benchmark_sets` |
| `benchmark_mappings.py` | Build and stage `benchmark_mappings` |
| `benchmark_memberships.py` | Build and stage `benchmark_memberships` |
| `peer_sets.py` | Build and stage `peer_sets` |
| `sector_snapshots.py` | Build and stage `sector_snapshots` |
| `ticker_metrics_mart.py` | Build and stage `gold_ticker_metrics_mart` (gold layer) |
| `scorecard_metric_rows_mart.py` | Build and stage `gold_scorecard_metric_rows` (gold layer) |
| `security_universe.py` | Build and stage `security_ingestion_status` |
| `ingestion_status.py` | Track ingestion coverage per ticker |

### Key functions

**`stage_prices(input_paths)`**
→ `transforms/prices.py`
Reads raw price CSVs from `data/raw/prices/`, deduplicates on `(ticker, date, interval)`,
enforces `PRICE_COLUMNS` schema, and upserts into `normalized_prices`.

**`stage_fundamentals(input_paths)`**
→ `transforms/fundamentals.py`
Reads raw fundamentals CSVs, applies period-type logic (`quarterly`/`annual`/`ttm`),
deduplicates on `(ticker, period_end_date, period_type)`, and upserts into
`normalized_fundamentals`. This table is the single source of truth for all historical
income statement, balance sheet, and ratio data.

**`stage_security_master(listed_security_paths, fundamentals_paths, ...)`**
→ `transforms/security_master.py`
Joins `listed_security_seed`, fundamentals metadata, and `security_master_overrides` to produce
the master registry of known securities. Each row is one ticker with `sector`, `industry`,
`is_active`, `is_eligible_research_universe`, and lineage fields.

**`build_benchmark_mappings_frame(security_master, benchmark_memberships, ...)`**
→ `transforms/benchmark_mappings.py`
For each ticker in `security_master`, resolves `primary_benchmark`, `sector_benchmark`,
`industry_benchmark`, `style_benchmark`, and `custom_benchmark` using membership lookups and
reference mapping rules. Encodes the assignment method (how each benchmark was resolved) for
auditability.

**`stage_benchmark_mappings(mapping_path)`**
→ `transforms/benchmark_mappings.py`
Orchestrates the full mapping build: loads canonical tables from DuckDB, calls
`build_benchmark_mappings_frame`, and upserts results into `benchmark_mappings`.

**`build_peer_sets_frame(security_master, fundamentals)`**
→ `transforms/peer_sets.py`
For each eligible ticker, selects industry-first peers from `security_master`, falls back to
sector peers when industry coverage is thin, excludes the focal ticker from its own peer rows,
and sorts by market cap. Records `relationship_type` (`industry`/`sector`), `selection_method`,
`peer_order`, and `peer_source` (`automatic`/`curated`) on every row.

**`stage_peer_sets(settings, write_warehouse, write_csv)`**
→ `transforms/peer_sets.py`
Loads canonical tables from DuckDB, calls `build_peer_sets_frame`, merges with any curated peer
overrides from `data/raw/peer_sets/`, and upserts into the `peer_sets` DuckDB table.

**`build_gold_ticker_metrics_mart_frame(ticker, ...)`**
→ `transforms/ticker_metrics_mart.py`
The most important transform function. Takes a ticker and all canonical tables as inputs.
Runs the full analytics pipeline for that ticker — return windows, risk metrics, trend metrics,
fundamentals summary, peer summary stats, and scorecard — and collapses everything into a single
wide row. This is what populates `gold_ticker_metrics_mart`.

**`stage_gold_ticker_metrics_mart(tickers, settings)`**
→ `transforms/ticker_metrics_mart.py`
Loops over a ticker list, calls `build_gold_ticker_metrics_mart_frame` for each, and upserts
all rows into `gold_ticker_metrics_mart`. This is the step that is missing for TEAM right now
(the table exists but has 0 TEAM rows).

### CLI entry point

```bash
# Stage one or all targets
dumb-money stage prices
dumb-money stage fundamentals
dumb-money stage security-master
dumb-money stage benchmark-mappings
dumb-money stage peer-sets
dumb-money stage all
```

Note: `stage all` does not include `gold_ticker_metrics_mart`. That step is currently run
directly in Python because it requires a ticker list argument.

### DuckDB tables after staging

| Table | Content |
|---|---|
| `normalized_prices` | Daily OHLCV for every ticker, deduped |
| `normalized_fundamentals` | Period-aware fundamentals, one row per `(ticker, period_end_date, period_type)` |
| `security_master` | Master ticker registry with sector, industry, eligibility flags |
| `benchmark_definitions` | Metadata for each benchmark ETF/index |
| `benchmark_memberships` | Constituent rows per benchmark ETF (from holdings files) |
| `benchmark_membership_coverage` | Coverage join between memberships and security_master |
| `benchmark_mappings` | One row per ticker with resolved benchmark assignments |
| `benchmark_sets` | Named sets of benchmarks (e.g. `default_benchmarks`) |
| `peer_sets` | Peer rows per ticker with relationship type and source |
| `sector_snapshots` | Sector-level aggregate metrics |
| `listed_security_seed` | Raw universe seed from NASDAQ/FINRA listing files |
| `security_master_overrides` | Manual ticker classification corrections |
| `security_ingestion_status` | Per-ticker coverage flags across all tables |
| `gold_ticker_metrics_mart` | **Gold layer** — one wide row per ticker with all section inputs |
| `gold_scorecard_metric_rows` | **Gold layer** — one row per `(ticker, score_date, metric_id)` |

---

## Layer 3: Storage — DuckDB Access Helpers

**What it does:** provides a clean read/write API so every other layer talks to DuckDB through
consistent helpers rather than raw SQL.

**Where it lives:** `src/dumb_money/storage/warehouse.py`

### Key functions

**`read_canonical_table(table_name, settings)`**
Reads an entire canonical table from DuckDB as a DataFrame. Falls back to CSV if the table
does not exist.

**`query_canonical_data(table_name, sql, settings)`**
Runs arbitrary SQL against the warehouse and returns a DataFrame. Used for filtered reads
(e.g. `WHERE ticker = 'TEAM'`).

**`upsert_canonical_table(frame, table_name, settings)`**
Writes a DataFrame into DuckDB using the table's registered incremental keys. Replaces
matching rows (by key) rather than appending everything.

**`write_canonical_table(frame, table_name, settings)`**
Full overwrite of a table. Used for tables that are always rebuilt from scratch
(e.g. `security_master`).

**`export_table_csv(frame, table_name, settings)`**
Writes a canonical table to its registered CSV export path under `data/staging/` or the
appropriate subdirectory. Every canonical table has a registered CSV path in
`CANONICAL_DUCKDB_TABLES`.

**`WarehouseTableSpec`**
A frozen dataclass registered in `CANONICAL_DUCKDB_TABLES` for each table. Holds the table name,
column schema, CSV directory attribute name, and CSV filename. This is how the system knows
where to write the CSV export for any given table without hard-coding paths in callers.

### Config and paths

**`src/dumb_money/config/settings.py`** — `AppSettings` (a Pydantic `BaseSettings` model)

Holds all path configuration. Key attributes:

| Attribute | Default path |
|---|---|
| `warehouse_path` | `data/warehouse/dumb_money.duckdb` |
| `raw_prices_dir` | `data/raw/prices/` |
| `raw_fundamentals_dir` | `data/raw/fundamentals/` |
| `normalized_prices_dir` | `data/staging/normalized_prices/` |
| `normalized_fundamentals_dir` | `data/staging/normalized_fundamentals/` |
| `security_master_dir` | `data/staging/security_master/` |
| `raw_portfolios_dir` | `data/raw/portfolios/` |

All paths are relative to `project_root` which defaults to the repo root. Call `get_settings()`
anywhere to get the singleton.

---

## Layer 4: Research — Building the Per-Ticker Packet

**What it does:** loads all canonical data needed for one ticker from the warehouse and assembles
it into a `CompanyResearchPacket` — an in-memory container that holds everything the analytics
and output layers need.

**Where it lives:** `src/dumb_money/research/company.py`

### `CompanyResearchPacket` dataclass

```python
@dataclass
class CompanyResearchPacket:
    ticker: str
    metadata: dict[str, Any]           # name, sector, industry, currency
    prices: pd.DataFrame               # filtered from normalized_prices
    fundamentals: pd.DataFrame         # filtered from normalized_fundamentals
    benchmark_prices: pd.DataFrame     # prices for assigned benchmarks
    benchmark_set: pd.DataFrame        # benchmark_sets rows
    benchmark_mappings: dict           # resolved benchmark tickers for this ticker
    peer_sets: pd.DataFrame | None     # peer rows for this ticker
    sector_snapshots: pd.DataFrame | None
    scorecard: CompanyScorecard | None
    mart_row: dict | None              # single row from gold_ticker_metrics_mart (if materialized)
    scorecard_metric_rows: pd.DataFrame | None  # from gold_scorecard_metric_rows
```

### Key functions

**`build_company_research_packet(ticker, settings)`**
→ `research/company.py:354`
The main entry point for building a packet. Loads data from DuckDB via the loader functions
below, runs `build_company_scorecard` from `analytics/scorecard.py`, and returns a populated
`CompanyResearchPacket`. This function is what you call before building any report section.

**Loader functions** (all called by `build_company_research_packet`):

| Function | Reads from |
|---|---|
| `load_staged_prices()` | `normalized_prices` DuckDB table |
| `load_staged_fundamentals()` | `normalized_fundamentals` DuckDB table |
| `load_benchmark_set()` | `benchmark_sets` DuckDB table |
| `load_benchmark_prices()` | `normalized_prices` filtered to benchmark tickers |
| `load_security_master()` | `security_master` DuckDB table |
| `load_benchmark_mappings()` | `benchmark_mappings` DuckDB table |
| `load_peer_sets()` | `peer_sets` DuckDB table |
| `load_sector_snapshots()` | `sector_snapshots` DuckDB table |
| `load_gold_ticker_metrics_mart()` | `gold_ticker_metrics_mart` DuckDB table |
| `load_gold_scorecard_metric_rows()` | `gold_scorecard_metric_rows` DuckDB table |

**`build_company_scorecard_from_gold_artifacts(ticker, mart_row, scorecard_metric_rows)`**
→ `research/company.py:340`
Reconstructs a `CompanyScorecard` from the pre-computed gold mart and scorecard metric rows.
Used when the gold mart has been materialized (the fast, preferred path). Falls back to
computing the scorecard live from the packet when mart data is not available.

---

## Layer 5a: Analytics — Calculations

**What it does:** pure computation on DataFrames. No DuckDB reads or writes. Takes raw price and
fundamentals data as inputs and returns calculated metrics.

**Where it lives:** `src/dumb_money/analytics/company.py` and `analytics/scorecard.py`

### Key functions in `analytics/company.py`

**`prepare_price_history(prices, ticker)`**
Filters to one ticker, ensures date ordering, forward-fills missing adj_close values.

**`calculate_return_windows(prices, windows)`**
Computes trailing total returns over specified windows (1m, 3m, 6m, 1y, 3y, 5y) from the
adj_close series.

**`calculate_risk_metrics(prices)`**
Annualized volatility (1m, 3m, 1y windows), downside volatility, max drawdown, current drawdown,
beta vs benchmark (requires benchmark price series).

**`calculate_trend_metrics(prices)`**
50-day and 200-day simple moving averages, price vs SMA ratios, golden cross flag
(SMA50 > SMA200).

**`build_drawdown_series(prices)`**
Full time-series of rolling drawdown from peak. Used by the Trend & Risk section chart.

**`build_moving_average_series(prices, windows)`**
Full time-series of SMAs. Used by the Trend & Risk section chart.

**`prepare_fundamentals_history(fundamentals, ticker)`**
Filters and cleans the fundamentals DataFrame for one ticker. Handles period-type priority
(TTM > quarterly > annual for flow metrics).

**`build_fundamentals_summary(fundamentals, ticker)`**
Extracts the latest snapshot values for all key metrics (margins, ratios, balance sheet) into
a flat dict. Used when the gold mart is not yet available for a ticker.

**`build_peer_valuation_comparison(ticker, peer_sets, fundamentals)`**
Joins peer tickers from `peer_sets` to their latest fundamentals and builds a comparison
DataFrame with valuation multiples for the focal ticker vs each peer.

**`build_peer_return_comparison(ticker, peer_prices, peer_sets, return_windows)`**
Builds a return comparison DataFrame for the focal ticker vs peers across standard return windows.

### Key functions in `analytics/scorecard.py`

**`build_company_scorecard(ticker, packet)`**
→ `analytics/scorecard.py:144`
The main scoring function. Takes a `CompanyResearchPacket` and scores the ticker across six
categories: Momentum, Relative Performance, Valuation, Profitability & Quality, Growth, and
Balance Sheet Strength. Returns a `CompanyScorecard` with total score (0–100), category scores,
and a per-metric breakdown (metric id, raw value, normalized score, weight).

The scorecard is the thing that eventually feeds `gold_scorecard_metric_rows` and the Score
Decomposition report section.

---

## Layer 5b: Outputs — Report Sections

**What it does:** takes a `CompanyResearchPacket` (or data from the gold mart) and produces
formatted DataFrames and chart-ready series for each report section. Each section has its own
file and can be built and reviewed independently.

**Where it lives:** `src/dumb_money/outputs/`

| File | Report section |
|---|---|
| `market_performance_section.py` | Returns vs benchmarks, indexed price chart |
| `research_summary_section.py` | Score strip, strengths/constraints summary |
| `trend_risk_profile_section.py` | Drawdown chart, SMA chart, risk panel |
| `balance_sheet_strength_section.py` | Leverage, liquidity, financial resilience |
| `valuation_section.py` | Multiples, FCF yield, peer comparison |
| `peer_positioning_section.py` | Peer return table, peer valuation table |
| `score_decomposition_section.py` | Category and metric-level score transparency |
| `growth_profitability_section.py` | Revenue, EPS, margin trends, ROIC |
| `final_research_summary_section.py` | Closing memo and summary card |
| `company_report.py` | Assembly — combines all sections into one bundle |

### The assembly function

**`build_full_company_report_bundle(ticker, settings)`**
→ `outputs/company_report.py:509`
Calls `build_company_research_packet(ticker)` from the research layer, then calls each section
builder in order, and returns a `FullCompanyReportBundle` dataclass holding all section outputs
(tables, chart series, text memos).

**`save_full_company_report(bundle, output_dir)`**
→ `outputs/company_report.py:581`
Writes the full bundle to disk as CSVs per section. Saved under
`reports/templates/full_company_report/` for review.

**`build_full_company_report_index(bundle)`**
→ `outputs/company_report.py:570`
Returns a single-row summary DataFrame with key metrics from the bundle — useful for
side-by-side comparison across multiple tickers.

---

## Running TEAM End-to-End Today

Here is the exact sequence to go from nothing to a full TEAM report:

### Step 1: Fix the circular import (already done in this session)
The `portfolios.py` → `storage` → `warehouse` → `ingestion/__init__` → `portfolios.py` cycle
was broken by importing directly from `dumb_money.storage.warehouse` instead of the package,
and by deriving column constants from `dumb_money.models` instead of ingestion submodules.

### Step 2: Refresh TEAM data (data is ~3.5 months stale)

```python
from dumb_money.ingestion.prices import ingest_prices
from dumb_money.ingestion.fundamentals import ingest_fundamentals
from datetime import date

ingest_prices("TEAM", start_date=date(2021, 1, 1), end_date=date(2026, 7, 13))
ingest_fundamentals("TEAM", as_of_date=date(2026, 7, 13))
```

Or via CLI:
```bash
dumb-money prices --tickers TEAM --start-date 2021-01-01 --end-date 2026-07-13
dumb-money fundamentals --tickers TEAM --as-of-date 2026-07-13
```

### Step 3: Rebuild staging tables that depend on fundamentals

```bash
dumb-money stage security-master
dumb-money stage benchmark-mappings
dumb-money stage peer-sets
```

### Step 4: Build the gold mart for TEAM

```python
from dumb_money.transforms.ticker_metrics_mart import stage_gold_ticker_metrics_mart

stage_gold_ticker_metrics_mart(tickers=["TEAM"])
```

This is the step currently missing. Without it, `mart_row` in the packet is None, and report
sections that prefer the gold mart path fall back to slower live computation.

### Step 5: Build and save the report

```python
from dumb_money.outputs.company_report import (
    build_full_company_report_bundle,
    save_full_company_report,
)

bundle = build_full_company_report_bundle("TEAM")
save_full_company_report(bundle, output_dir="reports/generated/TEAM/")
```

---

## What is NOT Yet Built (Sprints 8–12)

### Sprint 8: App Layer (Not Started)

A local Streamlit app at `app/streamlit/` that exposes company research, sector research, and
portfolio fit through a UI. The analytics and output modules are designed to support this —
each section can already be called independently — but no UI shell exists yet.

**What is needed:**
- `app/streamlit/app.py` — main Streamlit entry point
- Page for company research (calls `build_full_company_report_bundle`)
- Page for portfolio fit (calls analytics/portfolio.py)
- Shared UI helpers (chart rendering, table formatting)

### Sprint 9: LLM Summaries (Not Started)

Narrative summaries generated from structured scorecard and section outputs. The plan is to
feed scorecard verdicts, section evidence, and metric values into a structured prompt rather
than passing raw provider data to the LLM.

**What is needed:**
- Summary input schema (structured fields extracted from `CompanyResearchPacket`)
- Prompt assembly pipeline
- API call wrapper (Claude API)
- Summary validation checks against source metrics

### Sprint 10: Forward Estimates, Short Interest, DCF Inputs (Not Started)

New data sources needed before the three-lens framework can run.

**What is needed:**
- `ingestion/estimates.py` — yfinance estimate endpoints (`earnings_estimate`,
  `revenue_estimate`, `eps_revisions`, `eps_trend`, `earnings_history`, `analyst_price_targets`)
- `transforms/estimates.py` — normalize into `forward_estimates` DuckDB table
- `ingestion/short_interest.py` — local FINRA source file ingestion
- `transforms/short_interest.py` — normalize into `short_interest` DuckDB table
- DCF config defaults in `AppSettings` (`dcf_discount_rate`, `dcf_terminal_growth_rate`)
- `net_debt` derived metric from existing balance sheet fields

### Sprint 11: Three-Lens Evaluation Framework (Not Started, depends on Sprint 10)

The core analytical upgrade. Replaces the single composite scorecard with three independent
lenses that can agree or conflict.

**What is needed:**
- `analytics/value_lens.py` — DCF intrinsic value (bear/base/bull), EV/EBITDA vs peers,
  P/FCF, margin of safety
- `analytics/growth_lens.py` — revenue CAGR, PEG, earnings revision direction, margin trends
- `analytics/momentum_lens.py` — relative strength, MA signals, short interest, volume trend
- `LensVerdict` dataclass (in `models/`)
- `analytics/synthesis.py` — narrative synthesis from three verdicts

### Sprint 12: Decision Brief Report (Not Started, depends on Sprints 7 + 11)

A two-page PDF decision brief as a standalone output, separate from the existing full company
report. Verdict-first format with price target range from DCF.

**What is needed:**
- `outputs/decision_brief.py` — assembles the brief from lens verdicts + DCF + portfolio overlay
- PDF rendering (weasyprint or reportlab)
- Price target range generated from DCF scenarios (bear/base/bull)
- Portfolio overlay section (requires Sprint 7 holdings data)
- 90-day revisit date auto-population

---

## Current State Summary

| Layer | Status | Notes |
|---|---|---|
| Ingestion (prices, fundamentals, benchmarks) | ✅ Built | CLI + Python API working |
| Ingestion (portfolio holdings) | ✅ Built | Sprint 7 addition; circular import now fixed |
| Transforms (prices, fundamentals, security master) | ✅ Built | |
| Transforms (benchmark mappings, peer sets) | ✅ Built | TEAM: no industry benchmark mapped |
| Transforms (gold ticker metrics mart) | ✅ Built | TEAM: 0 rows — needs to be run |
| Storage (DuckDB helpers) | ✅ Built | Circular import fixed in this session |
| Research (packet assembly) | ✅ Built | |
| Analytics (returns, risk, trend, fundamentals) | ✅ Built | |
| Analytics (scorecard) | ✅ Built | Single composite score model |
| Outputs (all 9 report sections) | ✅ Built | Based on gold mart when available |
| Portfolio analytics | ✅ Built | `analytics/portfolio.py`, `research/portfolio.py` |
| App layer (Streamlit) | ❌ Not started | Sprint 8 |
| LLM summaries | ❌ Not started | Sprint 9 |
| Forward estimates ingestion | ❌ Not started | Sprint 10 |
| Short interest ingestion | ❌ Not started | Sprint 10 |
| DCF config + value lens | ❌ Not started | Sprints 10–11 |
| Growth lens | ❌ Not started | Sprint 11 |
| Momentum lens | ❌ Not started | Sprint 11 |
| Lens synthesis layer | ❌ Not started | Sprint 11 |
| Decision brief report | ❌ Not started | Sprint 12 |
