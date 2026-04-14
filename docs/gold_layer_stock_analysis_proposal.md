# Gold Layer Proposal For Broad Stock Analysis

## Why This Needs To Change

The current gold layer is useful for single-ticker report assembly, but it is still shaped around the most recently run stock:

- `gold_ticker_metrics_mart` gives one reusable ticker-level snapshot
- `gold_scorecard_metric_rows` gives one reusable score-transparency row set

That is enough for report sections, but it is not yet enough for broader analysis such as:

- cross-universe screening
- ranking all eligible stocks on the same as-of date
- tracking how a company's score and factor profile change over time
- comparing a company to peers and sectors without rebuilding ad hoc joins
- building watchlists and recurring research monitors

The next gold layer should stay opinionated and analysis-ready, while still keeping detailed raw and normalized series in the canonical lower-layer tables.

## Design Principles

- Gold tables should be analysis-ready, not raw or lightly normalized.
- Gold tables should have a clear grain and a stable downstream use case.
- Gold tables should support both one-ticker report generation and many-ticker research.
- Gold should prefer snapshot facts, compact histories, and reusable comparisons over full raw time series.
- Daily price bars, full fundamentals history, benchmark memberships, and other detailed source-like tables should stay below gold.

## Recommended Gold Layer

### 1. `gold_stock_snapshot`

Grain:
- one row per `ticker x as_of_date`

This should be the main stock-level screening and ranking table. It is the natural evolution of `gold_ticker_metrics_mart`.

Suggested contents:
- company identity and classification
- benchmark assignment fields
- current valuation, profitability, balance-sheet, risk, and trend metrics
- latest growth fields
- score totals and category scores
- peer summary medians
- sector summary context
- data freshness and coverage flags

How it will be used:
- universe screening
- sort/filter tables in notebooks or apps
- ranking stocks by score, valuation, quality, or risk
- supplying the top summary card for company reports
- powering "what looks interesting today?" workflows

Why it belongs in gold:
- it is compact
- stable
- directly useful to analysts without rebuilding joins

Notes:
- rename or version `gold_ticker_metrics_mart` into this contract rather than introducing a redundant second snapshot table
- this table should contain all eligible tickers for a given run, not only the focal ticker

### 2. `gold_scorecard_metric_rows`

Grain:
- one row per `ticker x score_date x metric_id`

This table already exists and should remain. It is the narrow transparency layer behind total and category scores.

Suggested contents:
- metric id and display name
- category
- raw metric value
- normalized/scaled value
- score contribution
- weight
- availability/applicability flags
- benchmark context and company metadata

How it will be used:
- score decomposition views
- "why did this ticker score well or poorly?" analysis
- identifying repeated weak spots across the universe
- QA for scoring logic changes
- feeding explanation layers or narrative generation

Why it belongs in gold:
- it exposes the business-ready scoring contract, not raw inputs
- it is reusable across reports, audits, and ranking diagnostics

### 3. `gold_stock_history_snapshot`

Grain:
- one row per `ticker x snapshot_date`

This is the most important missing table for broader analysis. It stores the compact historical evolution of each stock's analysis-ready metrics at regular observation points.

Suggested contents:
- the same core fields as `gold_stock_snapshot`, but recorded historically
- total score and category scores
- key valuation metrics
- key profitability and balance-sheet metrics
- trailing returns, volatility, drawdown, beta
- data freshness fields

How it will be used:
- trend analysis of score migration over time
- backtesting screen rules using frozen historical snapshots
- seeing whether a stock is improving or deteriorating fundamentally
- comparing today's rank versus rank 3, 6, or 12 months ago
- building watchlist change reports

Why it belongs in gold:
- analysts often need compact historical states, not full raw daily rebuilds
- it avoids recomputing all downstream metrics every time we ask a historical question

Notes:
- use monthly or weekly snapshot cadence first, not daily, to keep the table useful and affordable
- this is the table that turns the repo from "current scorecard" into "research system"

### 4. `gold_relative_performance_rows`

Grain:
- one row per `ticker x as_of_date x comparison_type x comparison_ticker x window`

This table standardizes relative-return facts that currently have to be rebuilt inside report logic or from multiple staged tables.

Suggested contents:
- company return
- benchmark/peer/sector ETF return
- excess return
- window
- comparison type such as `primary_benchmark`, `sector_benchmark`, `style_benchmark`, `peer_median`

How it will be used:
- leaderboard views for outperformance
- identifying stocks beating both the market and their sector
- reusable market-performance sections
- validating that scorecard market-performance signals match underlying return comparisons

Why it belongs in gold:
- relative performance is a common analytical output, not a raw input
- the grain is standardized and reusable across many downstream questions

Notes:
- keep full indexed price series and chart-ready lines below gold
- gold should store the compact comparison facts, not every plotting series

### 5. `gold_peer_comparison_rows`

Grain:
- one row per `ticker x as_of_date x peer_set_id x peer_ticker`

This table should hold reusable peer comparison rows for the specific metrics analysts repeatedly inspect.

Suggested contents:
- focal ticker and peer ticker
- peer source and relationship type
- market cap
- valuation metrics
- selected profitability metrics
- selected return metrics
- rank within peer set for key metrics
- premium/discount versus peer median fields

How it will be used:
- peer positioning tables
- peer-relative valuation work
- finding stocks that are quality leaders but valuation laggards, or vice versa
- reusable peer panels in reports and notebooks

Why it belongs in gold:
- peer comparison is already a curated analytical product, not just a normalized source join
- it removes repeated on-the-fly peer assembly

Notes:
- `peer_sets` should remain the canonical membership table below gold
- this table is for the derived comparison output

### 6. `gold_sector_context_snapshot`

Grain:
- one row per `sector x as_of_date`

The repo already has `sector_snapshots`. For broader stock analysis, we should either promote that contract into gold or create a gold-facing sector snapshot with explicit as-of dating and a stable analytical surface.

Suggested contents:
- company count and coverage counts
- sector benchmark
- sector median valuation metrics
- sector median margin metrics
- sector median return windows
- optional sector score medians once universe-wide score coverage exists

How it will be used:
- contextualizing whether a stock is cheap or expensive relative to its sector
- sector-relative ranking
- sector dashboards
- determining whether a stock's improvement is company-specific or sector-wide

Why it belongs in gold:
- sector context is an analysis-ready rollup that many stock-level workflows reuse

### 7. `gold_universe_rankings`

Grain:
- one row per `ticker x as_of_date x ranking_family`

This table provides reusable percentiles and ranks across the eligible universe and within sector or industry.

Suggested contents:
- total score rank and percentile
- category score ranks
- valuation percentile
- profitability percentile
- volatility percentile
- market-cap bucket
- within-sector and within-industry ranks

How it will be used:
- screen for "top decile quality, bottom half valuation"
- build watchlists and candidate lists
- power ranking dashboards without recomputing percentiles everywhere
- compare a stock's absolute metrics and relative standing together

Why it belongs in gold:
- ranks and percentiles are directly consumed analytical outputs
- they are expensive and repetitive enough to merit one canonical table

Notes:
- keep the raw metrics in `gold_stock_snapshot`
- keep the derived relative standing here

## Recommended Boundaries

Should stay below gold:

- `normalized_prices`
- `normalized_fundamentals`
- `benchmark_memberships`
- `benchmark_mappings`
- `peer_sets`
- full chart-ready daily series
- full statement history and raw provider payloads

Belong in gold:

- stock-level snapshots
- compact historical snapshots
- score transparency rows
- reusable relative-comparison facts
- reusable peer comparison rows
- sector rollups
- reusable universe ranks

## Recommended Build Order

### Phase 1: Fix current gold coverage

1. Expand `gold_ticker_metrics_mart` / `gold_stock_snapshot` so one run materializes many tickers, not only the last ticker worked on.
2. Expand `gold_scorecard_metric_rows` to the same universe coverage and same scoring date discipline.
3. Add freshness fields and coverage flags so analysts can trust broad screens.

### Phase 2: Add historical and relative analysis support

4. Add `gold_stock_history_snapshot`.
5. Add `gold_relative_performance_rows`.
6. Add `gold_peer_comparison_rows`.

### Phase 3: Add ranking and context products

7. Promote or formalize `gold_sector_context_snapshot`.
8. Add `gold_universe_rankings`.

## Minimum Viable Gold Layer

If we want the smallest useful target, the MVP gold layer should be:

- `gold_stock_snapshot`
- `gold_scorecard_metric_rows`
- `gold_stock_history_snapshot`
- `gold_universe_rankings`

That combination would already support:

- current scorecard reports
- broad stock screening
- rank-based idea generation
- historical score tracking
- watchlist monitoring

## Recommendation

The cleanest gold-layer direction for this repo is:

- keep one primary stock-level snapshot table
- keep one score-transparency row table
- add one compact historical snapshot table
- add comparison and ranking tables only where many downstream consumers would otherwise rebuild the same logic

In other words, the future gold layer should not become "everything interesting." It should become the small set of stable, analysis-ready products that make cross-stock research easy and repeatable.
