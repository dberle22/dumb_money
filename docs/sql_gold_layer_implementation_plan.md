# SQL-First Gold Layer Implementation Plan

## Goal

Implement the proposed stock-analysis gold layer as DuckDB SQL models that run after ingestion has materialized the canonical input tables.

For now, we are not solving orchestration, scheduling, or automation. The objective is:

- define the SQL model surface
- translate the current Python-derived gold logic into DuckDB SQL
- validate the tables manually through repeated use
- only later decide how to orchestrate and automate builds

## Scope

In scope now:

- SQL model design
- model dependency order
- table contracts and grains
- validation approach
- phased implementation plan

Out of scope for now:

- job runners
- CLI orchestration
- incremental scheduling
- alerting
- full production automation

## Recommended SQL Project Shape

Add a dedicated SQL area in the repo so we can build and inspect the models directly:

- `sql/gold/00_gold_stock_snapshot.sql`
- `sql/gold/01_gold_scorecard_metric_rows.sql`
- `sql/gold/02_gold_stock_history_snapshot.sql`
- `sql/gold/03_gold_relative_performance_rows.sql`
- `sql/gold/04_gold_peer_comparison_rows.sql`
- `sql/gold/05_gold_sector_context_snapshot.sql`
- `sql/gold/06_gold_universe_rankings.sql`
- `sql/gold/README.md`

Recommended execution pattern for now:

- each file uses `create or replace table ... as`
- each file is runnable by itself once its dependencies exist
- each file contains a short header comment naming:
  - table grain
  - required upstream tables
  - key business rules

## Canonical Inputs

These are the core tables the gold SQL layer should read from:

- `normalized_prices`
- `normalized_fundamentals`
- `security_master`
- `benchmark_sets`
- `benchmark_mappings`
- `peer_sets`
- `sector_snapshots`

These should remain canonical lower-layer inputs, not be redefined in gold.

## Guiding SQL Principles

- Keep one SQL file per gold table.
- Prefer CTE-driven SQL so logic is inspectable and easy to test.
- Make every gold table fully rebuildable from canonical inputs.
- Use explicit as-of logic in every model.
- Keep table grains strict and documented.
- Avoid storing chart-only series in gold.
- Use SQL window functions for ranking, lag, percentiles, and latest-row selection.

## Phase 1: Establish SQL Foundation

### Objective

Set up the SQL conventions and implement the current gold surface in SQL before expanding the model set.

### Deliverables

1. Create `sql/gold/README.md`
2. Define naming conventions:
   - final tables are named `gold_*`
   - helper CTE names are descriptive and stable
   - date fields use `as_of_date`, `score_date`, or `snapshot_date` intentionally
3. Decide the manual execution order for SQL files
4. Add a short schema section to each file header

### Decisions To Lock Early

1. `gold_ticker_metrics_mart` should be replaced or aliased by `gold_stock_snapshot`
2. gold builds should target broad-universe runs, not single-ticker runs
3. every gold table should be rebuildable from DuckDB canonical tables only

## Phase 2: Implement `gold_stock_snapshot`

### Objective

Build the main one-row-per-`ticker x as_of_date` stock snapshot table in SQL.

### Why First

This is the anchor table for almost every downstream gold output:

- scorecard rows
- historical snapshots
- rankings
- report summary inputs

### Grain

- one row per `ticker x as_of_date`

### Upstream dependencies

- `security_master`
- latest usable rows from `normalized_fundamentals`
- latest usable rows and return windows from `normalized_prices`
- `benchmark_mappings`
- `peer_sets`
- `sector_snapshots`

### Main SQL workstreams

1. Build a latest-fundamentals CTE
   - choose the latest valid row per ticker
   - preserve period metadata
   - standardize fields used in scoring and analysis

2. Build a latest-price-history CTE family
   - latest price date per ticker
   - trailing window returns for `1m`, `3m`, `6m`, `1y`
   - moving averages for `50` and `200`
   - price vs moving-average signals

3. Build risk metrics in SQL
   - annualized volatility windows
   - downside volatility
   - current drawdown
   - max drawdown
   - max drawdown over trailing 1 year
   - beta versus primary benchmark

4. Build peer summary medians in SQL
   - peer count
   - peer median forward P/E
   - peer median EV/EBITDA
   - peer median price-to-sales
   - peer median free-cash-flow yield
   - peer median market cap

5. Join benchmark assignment and sector context

6. Translate scorecard category and total score logic into SQL

### Important implementation note

This is the first place where we need a careful Python-to-SQL translation.

The current scorecard logic in [scorecard.py](dumb_money/src/dumb_money/analytics/scorecard.py) uses:

- threshold bands
- peer-relative fallback logic for valuation
- availability and applicability flags
- category-level rollups using available-weight logic

That logic should be re-expressed in SQL explicitly with:

- `case when` banding
- nullable score inputs
- explicit applicability flags
- category aggregation CTEs

We should not approximate this loosely. The SQL result should match the Python result for the same fixtures.

### Validation targets

- row count equals eligible universe coverage for the chosen run
- no duplicate `ticker x as_of_date`
- score outputs match current Python mart for fixture tickers
- benchmark and peer fields are populated where upstream coverage exists

## Phase 3: Implement `gold_scorecard_metric_rows`

### Objective

Build the score transparency table directly in SQL, rather than regenerating it through Python scorecard code.

### Grain

- one row per `ticker x score_date x metric_id`

### Upstream dependencies

- `gold_stock_snapshot`
- helper SQL logic for score metric expansion

### Main SQL workstreams

1. Recreate each scorecard metric as an explicit metric row
   - market performance metrics
   - growth and profitability metrics
   - balance sheet metrics
   - valuation metrics

2. Emit metric-level fields
   - raw value
   - normalized value
   - scoring method
   - metric score
   - metric weight
   - metric availability
   - applicability
   - confidence flag
   - notes

### Suggested implementation pattern

Use a `union all` pattern:

- one CTE or block per metric
- consistent output columns across all blocks
- final `union all` into the table contract

This is more verbose than a compact macro-like approach, but much easier to audit at first.

### Validation targets

- metric row count equals expected `tickers x score metrics`
- total category and total scores re-aggregate back to `gold_stock_snapshot`
- output matches current Python scorecard rows for fixture tickers

## Phase 4: Implement `gold_stock_history_snapshot`

### Objective

Store compact historical analytical states so we can study changes over time without rebuilding everything from raw prices and fundamentals for each question.

### Grain

- one row per `ticker x snapshot_date`

### Upstream dependencies

- `normalized_prices`
- `normalized_fundamentals`
- `benchmark_mappings`
- `peer_sets`
- `sector_snapshots` or sector history equivalent

### Main SQL design choice

We need to choose initial snapshot cadence:

- monthly is recommended first
- weekly is a reasonable second option
- daily is not recommended for the first implementation

### Initial recommended contract

Store a compact subset of `gold_stock_snapshot` fields:

- score totals and category scores
- key valuation metrics
- key quality metrics
- key leverage metrics
- return windows
- risk metrics
- trend metrics
- freshness markers

### Why not first

This table is high value, but it depends on the snapshot logic being trustworthy first.

### Validation targets

- one row per `ticker x snapshot_date`
- stable cadence
- reasonable score evolution over time for a few manually reviewed tickers
- no lookahead leakage in historical fields

## Phase 5: Implement `gold_relative_performance_rows`

### Objective

Materialize compact relative-return facts for repeated analytical use.

### Grain

- one row per `ticker x as_of_date x comparison_type x comparison_ticker x window`

### Upstream dependencies

- `normalized_prices`
- `benchmark_mappings`
- `peer_sets`

### Main SQL workstreams

1. Materialize company trailing returns
2. Materialize benchmark trailing returns
3. Join company returns to relevant benchmark returns
4. Optionally add peer-median return comparisons
5. Standardize `comparison_type`

### Recommended windows

- `1m`
- `3m`
- `6m`
- `1y`

### Validation targets

- excess-return math reconciles to underlying return fields
- no duplicate comparison rows at the same grain
- benchmark assignment aligns with `benchmark_mappings`

## Phase 6: Implement `gold_peer_comparison_rows`

### Objective

Create a stable peer-comparison surface for peer valuation and positioning analysis.

### Grain

- one row per `ticker x as_of_date x peer_set_id x peer_ticker`

### Upstream dependencies

- `peer_sets`
- latest fundamentals and price-derived fields
- `gold_stock_snapshot`

### Main SQL workstreams

1. Resolve active peer memberships
2. pull peer stock snapshots
3. emit focal and peer metrics side by side
4. compute premium/discount versus peer median
5. compute within-peer-set ranking for key fields

### Suggested metric set for v1

- market cap
- forward P/E
- EV/EBITDA
- price-to-sales
- free-cash-flow yield
- operating margin
- gross margin
- 1-year return
- total score

### Validation targets

- peer rows reconcile to `peer_sets`
- peer medians reconcile to snapshot-level peer median fields
- no self-join duplication beyond intentional focal-row logic

## Phase 7: Implement `gold_sector_context_snapshot`

### Objective

Formalize the reusable sector context surface in SQL.

### Grain

- one row per `sector x as_of_date`

### Upstream dependencies

- `security_master`
- latest fundamentals by ticker
- latest returns by ticker
- `benchmark_mappings`

### Main SQL workstreams

1. define eligible sector universe
2. aggregate latest stock snapshot fields by sector
3. add sector benchmark
4. optionally add sector-level score medians once broad score coverage is stable

### Recommendation

This may replace or evolve today’s `sector_snapshots` table. We should avoid keeping two overlapping sector-summary contracts long term.

### Validation targets

- one row per sector and date
- median fields reconcile to constituent stock snapshots
- sector coverage counts make sense

## Phase 8: Implement `gold_universe_rankings`

### Objective

Create reusable universe, sector, and industry ranks/percentiles.

### Grain

- one row per `ticker x as_of_date x ranking_family`

### Upstream dependencies

- `gold_stock_snapshot`

### Main SQL workstreams

1. assign ranking families
   - overall universe
   - within sector
   - within industry

2. compute:
   - total score rank and percentile
   - category score ranks
   - valuation percentile
   - profitability percentile
   - volatility percentile

3. store ranking context
   - population size
   - market-cap bucket if desired

### Recommended SQL functions

- `row_number()`
- `rank()`
- `dense_rank()`
- `percent_rank()`
- `ntile()`

### Validation targets

- percentile ranges stay within `0-1`
- within-group counts match the expected populations
- ranks are stable and interpretable for sample sectors

## Testing And Validation Plan

We are not orchestrating yet, so validation should be manual and deliberate.

### 1. SQL correctness checks

For every table, add a short validation query set in the SQL README:

- duplicate grain check
- null checks on critical keys
- row-count sanity checks
- upstream reconciliation checks

### 2. Python parity checks

For the first two tables, compare SQL outputs to current Python outputs:

- `gold_stock_snapshot` versus current `gold_ticker_metrics_mart`
- `gold_scorecard_metric_rows` versus current Python scorecard rows

The goal is not permanent dual implementation. The goal is to verify the SQL translation.

### 3. Analyst usability checks

For each table, answer a few real questions using only SQL:

- What are the top 20 stocks by total score?
- Which stocks outperform both their primary and sector benchmark?
- Which stocks are cheap versus peers but strong on margins?
- Which sectors have the strongest median score?
- Which watchlist names improved most over the last 6 months?

If a table does not make these questions easy, the contract likely needs adjustment.

## Translation Risks To Handle Carefully

### 1. Latest-row selection rules

`normalized_fundamentals` has period-aware history. We need one explicit SQL policy for:

- which period types are preferred
- how ties are broken
- whether `TTM`, quarterly, and annual rows are mixed or prioritized

### 2. Beta and downside volatility

These are feasible in DuckDB SQL, but they are the most math-sensitive part of the migration. Build and test them carefully.

### 3. Score applicability versus availability

The Python scorecard distinguishes:

- missing data
- not applicable metrics
- available metrics

That distinction must survive in SQL or the category rollups will drift.

### 4. Peer-relative valuation fallback

The current scorecard uses peer-relative scoring when peer medians exist, otherwise absolute thresholds. That branch logic needs exact SQL treatment.

### 5. Historical leakage

When we build `gold_stock_history_snapshot`, we must make sure each historical row only uses information available as of that snapshot date.

## Recommended Implementation Order

1. Create SQL directory and SQL README
2. Implement `gold_stock_snapshot`
3. Validate against the current Python mart
4. Implement `gold_scorecard_metric_rows`
5. Validate score re-aggregation and parity
6. Implement `gold_universe_rankings`
7. Implement `gold_relative_performance_rows`
8. Implement `gold_peer_comparison_rows`
9. Implement `gold_sector_context_snapshot`
10. Implement `gold_stock_history_snapshot`

## Why This Order

- `gold_stock_snapshot` is the hub table
- `gold_scorecard_metric_rows` depends on trustworthy score translation
- rankings are cheap once the snapshot is stable
- relative and peer comparison tables depend on stable snapshot logic
- historical snapshots should come after the current-state logic is trusted

## Suggested Near-Term Milestone

The best first milestone is:

- one SQL-built `gold_stock_snapshot`
- one SQL-built `gold_scorecard_metric_rows`
- one small validation pack that proves parity with current Python outputs for a handful of tickers

If those two tables feel good in real analysis work, the rest of the gold layer becomes much easier and much safer to add.
