# Stock Scorecard Specification

## Purpose

This document defines a default stock scorecard for the Investment Research Tool. The goal is to evaluate a stock using a balanced mix of investment performance, business quality, balance sheet strength, and valuation.

The scorecard is designed to be:

- interpretable
- modular
- easy to implement in Python or R
- suitable for peer based comparison
- extendable into style specific variants later

The default model uses a 100 point scale.

---

## Implementation Status

The original scorecard design mixes metrics that can be calculated from the current normalized foundation with metrics that require additional history, peer sets, or benchmark-mapping logic.

To keep Sprint 3 focused and shippable, this spec now separates:

- `V1 Now`:
  metrics that can be calculated from the current staged price, fundamentals, and security master outputs
- `V1 Later`:
  metrics that remain part of the intended scorecard design but require more data engineering or modeling work before they can be scored reliably

The current repo already supports:

- normalized daily price history for the current sample universe
- normalized latest fundamentals snapshots
- a security master with sector and industry labels for companies
- benchmark definitions and benchmark price history for `SPY`, `QQQ`, and `IWM`

The current repo does not yet support:

- multi-period historical fundamentals suitable for trend scoring
- peer group definitions and peer-relative percentiles
- sector-to-benchmark ETF mapping rules
- custom benchmark-set generation by company sector or industry

The current repo now also includes:

- a buildable `V1 Now` scorecard implementation for `AAPL`
- metric-level score outputs, category scores, and coverage or confidence tracking
- a first temporary benchmark assignment rule using `SPY` plus sector or style proxy logic
- a notebook-first review workflow and shared report helpers

---

## Scorecard Structure

### Default V1 Weighting

| Category | Weight |
|---|---:|
| Market Performance | 25 |
| Growth and Profitability | 35 |
| Balance Sheet Strength | 25 |
| Valuation | 15 |

### Why this structure

This weighting intentionally gives more emphasis to business quality than short term price action, while still rewarding actual investment performance and financial strength.

It aims to answer four questions:

1. Has the stock performed well as an investment
2. Is the underlying business strong and compounding
3. Is the company financially resilient
4. Is the valuation reasonable relative to what you get

---

## V1 Scorecard Metrics

### V1 Now: buildable with current data

This is the scorecard we can implement immediately for `AAPL` using the existing staged datasets and shared analytics modules.

| Category | Metric | Weight | Status | Notes |
|---|---|---:|---|---|
| Market Performance | 12 month return vs `SPY` | 10 | ready now | Broad market benchmark comparison |
| Market Performance | 12 month return vs sector or style benchmark | 7 | ready now with temporary mapping | Use `QQQ` as current proxy for `AAPL`; move to true sector mapping later |
| Market Performance | Max drawdown over trailing 1 year | 4 | ready now | Based on normalized price history |
| Market Performance | Price vs 200 day moving average | 4 | ready now | Based on normalized price history |
| Growth and Profitability | Operating margin | 8 | ready now | Latest normalized snapshot |
| Growth and Profitability | Free cash flow margin | 8 | ready now | Derived from `free_cash_flow / revenue_ttm` |
| Growth and Profitability | ROE | 7 | ready now | Use ROE directly; ROIC remains future enhancement |
| Growth and Profitability | ROA | 6 | ready now | Latest normalized snapshot |
| Growth and Profitability | Gross margin | 6 | ready now | Latest normalized snapshot |
| Balance Sheet Strength | Net debt to EBITDA | 8 | ready now | Derived from `(total_debt - total_cash) / ebitda` when available |
| Balance Sheet Strength | Current ratio | 5 | ready now | Latest normalized snapshot |
| Balance Sheet Strength | Debt to equity | 5 | ready now | Latest normalized snapshot |
| Balance Sheet Strength | Free cash flow to debt | 4 | ready now | Derived from `free_cash_flow / total_debt` |
| Balance Sheet Strength | Cash to debt | 3 | ready now | Derived from `total_cash / total_debt` |
| Valuation | Forward P/E | 5 | ready now | Absolute threshold scoring in V1 |
| Valuation | EV/EBITDA | 4 | ready now | Absolute threshold scoring in V1 |
| Valuation | Price to sales | 3 | ready now | Absolute threshold scoring in V1 |
| Valuation | Dividend yield or free cash flow yield | 3 | partial now | Dividend yield is available now; free cash flow yield requires market-cap-based derivation |

**Total = 100**

Implementation note:

- this scorecard now exists as a shared code path and should be treated as the baseline for narrative reporting
- the next round of work should improve benchmark assignment, visual explanation, and narrative interpretation before broadening the metric set

### V1 Later: intended but not yet buildable from the current foundation

These metrics remain valuable, but they should not block the first scorecard implementation.

| Category | Metric | Why deferred |
|---|---|---|
| Growth and Profitability | Revenue growth | Needs historical fundamentals snapshots across multiple periods |
| Growth and Profitability | EPS growth | Needs historical fundamentals snapshots across multiple periods |
| Growth and Profitability | Earnings or margin stability | Needs multi-period history and consistency logic |
| Growth and Profitability | ROIC | Needs invested-capital modeling not currently in the canonical snapshot |
| Balance Sheet Strength | Interest coverage | Needs interest expense or a consistent proxy not currently modeled |
| Valuation | Forward P/E vs peers | Needs peer-set definition and percentile scoring context |
| Valuation | EV/EBITDA vs peers | Needs peer-set definition and percentile scoring context |
| Valuation | Growth-adjusted valuation | Needs growth history or forecast-growth modeling for a stable proxy |
| Market Performance | Return vs true sector ETF | Needs sector-to-benchmark mapping layer or benchmark assignment rules |
| Any category | Custom benchmark scoring | Needs reusable company-to-benchmark set generation logic |

### Current limitations observed during first implementation

- `QQQ` is currently being used as a practical sector or style proxy for `AAPL`, but the model should evolve toward explicit sector ETF mapping
- the scorecard currently relies on absolute thresholds rather than peer-relative percentile logic
- interpretation labels, strengths, and weaknesses are not yet standardized output fields
- some metrics, especially balance-sheet and valuation context, will become more informative once peer medians exist

## 1. Market Performance: 25 points

This section captures how the stock has behaved as an investment.

| Metric | Weight | Purpose |
|---|---:|---|
| 12 month return vs S&P 500 | 10 | Measures broad market outperformance |
| 12 month return vs sector | 7 | Measures relative strength within its sector or industry context |
| Max drawdown over trailing 1 year | 4 | Penalizes severe declines |
| Price vs 200 day moving average | 4 | Captures trend health |

### Notes
- This category should reward both outperformance and stability
- Relative comparisons should ideally be made against a peer set or benchmark set
- In `V1 Now`, the primary market benchmark is `SPY`
- In `V1 Now`, the secondary benchmark can be a temporary sector or style proxy such as `QQQ` for `AAPL`
- In `V1 Later`, sector benchmark selection should come from an explicit mapping layer such as `Technology -> XLK`
- Max drawdown should be scored inversely, where smaller drawdowns receive better scores

---

## 2. Growth and Profitability: 35 points

This section evaluates the quality of the underlying business.

| Metric | Weight | Purpose |
|---|---:|---|
| Revenue growth | 8 | Measures top line expansion |
| EPS growth | 7 | Measures earnings expansion |
| Operating margin | 6 | Measures operating efficiency |
| Free cash flow margin | 6 | Measures cash generation quality |
| ROIC or ROE | 5 | Measures capital efficiency |
| Earnings or margin stability | 3 | Rewards consistency over time |

### Notes
- This category should carry the most weight in the default model
- For cyclical firms, margin stability and cash generation may be more informative than raw EPS growth
- ROIC is preferred when available, though ROE is a practical fallback
- In `V1 Now`, this category should be implemented from latest-snapshot quality metrics instead of historical growth metrics
- In `V1 Later`, growth and stability metrics should be added once historical fundamentals are modeled

---

## 3. Balance Sheet Strength: 25 points

This section evaluates financial resilience and flexibility.

| Metric | Weight | Purpose |
|---|---:|---|
| Net debt to EBITDA | 8 | Measures leverage burden |
| Interest coverage | 6 | Measures ability to service debt |
| Current ratio | 4 | Measures short term liquidity |
| Debt to equity | 4 | Measures capital structure leverage |
| Free cash flow to debt | 3 | Measures debt paydown capacity |

### Notes
- This category matters more for cyclical, capital intensive, or distressed businesses
- Threshold based scoring often works better here than peer percentile scoring
- Some sectors such as financials may need custom balance sheet logic
- In `V1 Now`, point-in-time balance sheet health is sufficient for the first scorecard
- Over time, this category should add trend-aware metrics such as debt reduction, liquidity change, and deterioration or improvement in net cash position

---

## 4. Valuation: 15 points

This section evaluates whether the stock looks expensive or attractive relative to peers and growth.

| Metric | Weight | Purpose |
|---|---:|---|
| Forward P/E vs peers | 5 | Compares valuation within peer group |
| EV/EBITDA vs peers | 4 | Measures enterprise value versus operating earnings |
| Free cash flow yield | 3 | Rewards stronger cash return on price |
| Growth adjusted valuation | 3 | Balances price versus expected growth |

### Notes
- Valuation should matter, but not dominate the scorecard
- Great companies can screen expensive for long periods, so this category is intentionally lighter than quality and balance sheet
- Growth adjusted valuation can be implemented via PEG or a similar internal proxy
- In `V1 Now`, valuation should rely on available absolute multiples and yields
- In `V1 Later`, peer-relative valuation should replace or complement absolute threshold scoring

---

## Full V1 Scorecard Table

### Full V1 Now Table

| Category | Metric | Weight |
|---|---|---:|
| Market Performance | 12 month return vs `SPY` | 10 |
| Market Performance | 12 month return vs sector or style benchmark | 7 |
| Market Performance | Max drawdown over trailing 1 year | 4 |
| Market Performance | Price vs 200 day moving average | 4 |
| Growth and Profitability | Operating margin | 8 |
| Growth and Profitability | Free cash flow margin | 8 |
| Growth and Profitability | ROE | 7 |
| Growth and Profitability | ROA | 6 |
| Growth and Profitability | Gross margin | 6 |
| Balance Sheet Strength | Net debt to EBITDA | 8 |
| Balance Sheet Strength | Current ratio | 5 |
| Balance Sheet Strength | Debt to equity | 5 |
| Balance Sheet Strength | Free cash flow to debt | 4 |
| Balance Sheet Strength | Cash to debt | 3 |
| Valuation | Forward P/E | 5 |
| Valuation | EV/EBITDA | 4 |
| Valuation | Price to sales | 3 |
| Valuation | Dividend yield or free cash flow yield | 3 |

**Total = 100**

---

## Recommended Scoring Method

The scorecard should not score raw values directly without context. Metrics should be converted into points using one of two methods.

### 1. Percentile Scoring

Best for:
- performance metrics
- valuation versus peers
- peer relative comparisons

Example approach:

| Percentile Rank | Points Awarded |
|---|---:|
| 80 to 100 | 100 percent of metric points |
| 60 to 79 | 75 percent of metric points |
| 40 to 59 | 50 percent of metric points |
| 20 to 39 | 25 percent of metric points |
| 0 to 19 | 0 percent of metric points |

### 2. Threshold Scoring

Best for:
- leverage metrics
- liquidity metrics
- interest coverage
- other balance sheet health metrics

Example for net debt to EBITDA:

| Value | Points Awarded |
|---|---:|
| less than 1.0 | 100 percent |
| 1.0 to 2.0 | 75 percent |
| 2.0 to 3.0 | 50 percent |
| 3.0 to 4.0 | 25 percent |
| greater than 4.0 | 0 percent |

### Recommended approach

Use a hybrid model:

- percentile scoring for market performance and valuation metrics
- threshold scoring for balance sheet metrics
- either percentile or threshold scoring for growth and profitability depending on the sector and data distribution

### V1 Now scoring simplification

For the first implementation:

- use direct comparison for market-performance metrics against `SPY` and a second benchmark
- use threshold scoring for balance sheet metrics
- use threshold scoring for current profitability and valuation metrics
- defer peer-relative percentile scoring until peer sets exist

This keeps the first scorecard transparent and deterministic while the benchmark-mapping and peer-group infrastructure is still being built.

---

## Scoring Rules and Design Notes

### Metric directionality

Each metric should have a clear scoring direction.

#### Higher is better
- 12 month return vs S&P 500
- 12 month return vs sector
- revenue growth
- EPS growth
- operating margin
- free cash flow margin
- ROIC or ROE
- interest coverage
- current ratio
- free cash flow yield
- free cash flow to debt

#### Lower is better
- max drawdown
- net debt to EBITDA
- debt to equity
- valuation multiples such as forward P/E and EV/EBITDA, depending on context

### Missing data rules

The scorecard should not silently fail when data is unavailable.

Recommended fields:
- `metric_available`
- `metric_applicable`
- `metric_score`
- `metric_weight`
- `confidence_flag`
- `metric_id`
- `category`
- `scoring_method`
- `raw_value`
- `normalized_value`
- `notes`

Possible approaches:
1. reweight within category when a metric is unavailable
2. keep the missing metric as zero and reduce confidence
3. mark the category as incomplete

The preferred V1 approach is:
- reweight within category for unavailable but applicable metrics
- exclude non applicable metrics
- store a confidence score for the final stock score

Recommended summary fields:
- `available_weight`
- `total_intended_weight`
- `coverage_ratio`

Recommended additional stock-level outputs for report generation:
- `interpretation_label`
- `top_strengths`
- `top_constraints`
- `benchmark_assignment_method`

### Sector specific exceptions

Some metrics are not equally useful across all sectors.

Examples:
- P/E is less useful for unprofitable growth companies
- debt metrics require different treatment for banks and insurers
- EV/sales may matter more than earnings multiples for high growth software
- EBITDA may be less meaningful for some financial businesses

This means the scorecard should eventually support sector specific logic or metric substitutions.

### Balance sheet over time

Balance sheet trend should be considered important, but it should be treated as `V1 Later` rather than a blocker for the first implementation.

Why it matters:

- point-in-time balance sheet strength can miss deteriorating leverage
- rising debt and falling liquidity often matter more than a single static ratio
- improving net cash and improving cash generation can justify a stronger resilience score

What the first version should do:

- score the latest snapshot cleanly
- preserve the scorecard schema so trend metrics can be added without redesign

What a later version should add:

- net cash or net debt change over time
- debt to equity trend
- current ratio trend
- free cash flow to debt trend
- stability or deterioration flags for capital structure

---

## Data Requirements and Gaps

### Ready from the current foundation

- normalized price history
- trailing return calculations
- drawdown and moving-average metrics
- latest normalized fundamentals snapshot
- company sector and industry from the security master
- benchmark price history for the current benchmark universe

### Missing for later scorecard expansion

#### Historical fundamentals time series

Needed for:

- revenue growth
- EPS growth
- margin stability
- balance-sheet trend scoring
- growth-adjusted valuation based on real historical growth

Work required:

- ingest and persist multiple fundamentals snapshots per ticker over time
- define retention rules for point-in-time fundamentals history
- confirm canonical modeling for historical snapshot series in `docs/data_models.md`
- add transforms and tests for deduping, time alignment, and snapshot lineage

#### Benchmark mapping layer

Needed for:

- true sector benchmark scoring
- custom benchmark sets by company sector, industry, or strategy
- consistent benchmark assignment across companies

Work required:

- define a sector-to-benchmark mapping table, for example `Technology -> XLK`
- decide whether style proxies such as `QQQ` are explicit benchmark types rather than sector ETFs
- create reusable mapping logic in shared modules rather than notebook code
- extend benchmark definitions and staged benchmark sets to support mapped or custom benchmark sets
- add tests for benchmark assignment rules

Recommended near-term staging or model update:

- document a `benchmark_mapping` output in `docs/data_models.md` so assignment rules become part of the shared data contract

#### Peer group and percentile context

Needed for:

- peer-relative valuation
- peer-relative profitability percentiles
- later peer research workflows

Work required:

- define peer-group construction rules
- add peer-set outputs to the shared research or analytics layer
- implement percentile scoring against those peer sets
- add fixtures and tests for peer-relative scoring

Recommended sequencing note:

- this should follow the first polished single-ticker visual narrative workflow rather than block it

#### Additional fundamentals modeling

Needed for:

- ROIC
- interest coverage
- more robust free cash flow yield and valuation logic

Work required:

- extend canonical fundamentals fields where needed
- confirm providers and transformation logic for interest expense or equivalent fields
- document any new fields in `docs/data_models.md`

---

## Suggested Output Structure

The stock scorecard output should include:

### Stock level outputs
- ticker
- company name
- sector
- industry
- primary_benchmark
- secondary_benchmark
- score_date
- total_score
- market_performance_score
- growth_profitability_score
- balance_sheet_score
- valuation_score
- confidence_score
- interpretation_label
- top_strengths
- top_constraints
- benchmark_assignment_method

### Metric level outputs
- metric_id
- category
- metric_name
- raw_value
- normalized_value
- scoring_method
- metric_score
- metric_weight
- metric_available
- metric_applicable
- confidence_flag
- notes

---

## Example Scorecard Interpretation

### High score profile
A high scoring stock should generally show:
- sustained outperformance versus market and sector
- healthy growth and strong margins
- manageable leverage and solid liquidity
- acceptable or attractive valuation relative to peers

### Low score profile
A low scoring stock may show:
- weak relative returns
- poor profitability or deteriorating growth
- elevated leverage or weak debt coverage
- expensive valuation without corresponding business quality

---

## Future Extensions

The V1 scorecard is intended as a general purpose model. Future versions could branch into style specific scorecards.

### Potential variants

#### Quality Compounder model
- heavier weight on profitability, balance sheet, and consistency
- lower weight on short term momentum

#### Value model
- heavier weight on valuation and balance sheet
- moderate weight on business quality
- lower weight on market performance

#### Growth model
- heavier weight on revenue growth, margin trend, and price strength
- lower weight on current valuation multiples

### Foundation work required before these variants

- historical fundamentals ingestion
- benchmark mapping logic
- peer-set modeling
- sector-specific metric substitution rules

---

## Recommended Next Step

The next implementation step is to turn this specification into a metric dictionary with:

- exact formulas
- source fields
- lookback windows
- scoring bands
- peer comparison rules
- missing data rules
- sector specific overrides

That metric dictionary can then feed the research layer and scorecard tables in the Investment Research Tool.

The next practical follow-on after the current implementation is:

- refine the visual narrative and notebook flow around the existing scorecard outputs
- add explicit benchmark assignment outputs
- add interpretation label and strengths or weaknesses extraction
- only then expand into historical fundamentals and peers for a fuller `V1 Later` scorecard
