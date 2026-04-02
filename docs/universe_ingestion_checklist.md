# Universe Ingestion Checklist

## Purpose

This checklist defines the maintained backlog for widening coverage after the validated `DIA` proof. The operating rule is:

1. Start with narrower industry ETFs.
2. Move to sector ETFs.
3. Move to broader benchmarks.
4. Use DuckDB-backed ingestion status to exclude securities that are already fully staged.

DuckDB control table:

- canonical table: `security_ingestion_status`
- use it to track whether a security already has staged prices and historical fundamentals
- treat `is_fully_ingested = true` as the default exclusion rule when moving from a narrow basket to a broader one

## Execution Order

Run the broad-benchmark batch workflow with these commands:

```bash
.venv/bin/python -m dumb_money.cli plan-basket --ticker <ETF> --batch-size 100
.venv/bin/python -m dumb_money.cli basket-status --ticker <ETF>
.venv/bin/python -m dumb_money.cli ingest-basket --ticker <ETF> --batch-index <N>
.venv/bin/python -m dumb_money.cli basket-validate --ticker <ETF>
```

### Phase 1: Industry ETFs

- [x] `XSD` Semiconductor
- [x] `XSW` Software & Services
- [x] `XBI` Biotech
- [x] `XPH` Pharmaceuticals
- [x] `KBE` Bank
- [x] `KRE` Regional Bank
- [x] `KIE` Insurance
- [x] `XRT` Retail
- [x] `XHB` Homebuilders
- [x] `XOP` Oil & Gas Exploration
- [x] `XME` Metals & Mining
- [x] `XTN` Transportation
- [x] `XAR` Aerospace & Defense

### Phase 2: Sector ETFs

- [x] `XLK` Technology
- [x] `XLF` Financial
- [x] `XLV` Health Care
- [x] `XLI` Industrial
- [x] `XLY` Consumer Discretionary
- [x] `XLP` Consumer Staples
- [x] `XLE` Energy
- [x] `XLB` Materials
- [x] `XLC` Communication Services
- [x] `XLRE` Real Estate
- [x] `XLU` Utilities

### Phase 3: Broad Benchmarks

- [x] `SPY` S&P 500
- [ ] `IWM` Russell 2000

### Already Validated

- [x] `DIA` Dow Jones Industrial Average

## QA Gates Per Basket

- [ ] ticker universe resolves from DuckDB SQL, not notebook logic
- [ ] cash/footer artifacts are excluded from the ticker list
- [ ] price ingestion completes and stages into `normalized_prices`
- [ ] historical fundamentals ingestion completes and stages into `normalized_fundamentals`
- [ ] `security_ingestion_status` is rebuilt after staging
- [ ] benchmark membership joins to `security_master`
- [ ] benchmark membership joins to staged prices
- [ ] benchmark membership joins to staged historical fundamentals
- [ ] missing tickers and provider failures are recorded before moving on

## IWM Batch Runbook

Working directory:

```bash
cd /Users/danberle/Documents/projects/dumb_money
```

Plan the run once:

```bash
.venv/bin/python -m dumb_money.cli plan-basket --ticker IWM --batch-size 100
```

Check the run status:

```bash
.venv/bin/python -m dumb_money.cli basket-status --ticker IWM
```

Run batches manually in order:

```bash
.venv/bin/python -m dumb_money.cli ingest-basket --ticker IWM --batch-index 0
.venv/bin/python -m dumb_money.cli ingest-basket --ticker IWM --batch-index 1
.venv/bin/python -m dumb_money.cli ingest-basket --ticker IWM --batch-index 2
```

Keep incrementing `--batch-index` until `basket-status` shows no remaining planned or partial work.

Validate at the end:

```bash
.venv/bin/python -m dumb_money.cli basket-validate --ticker IWM
```

Tracking:

- [x] manifest planned with batch size `100`
- [x] batch `0`
- [x] batch `1`
- [x] batch `2`
- [x] batch `3`
- [x] batch `4`
- [x] batch `5`
- [x] batch `6`
- [x] batch `7`
- [x] batch `8`
- [x] batch `9`
- [x] batch `10`
- [x] batch `11`
- [x] batch `12`
- [x] batch `13`
- [x] batch `14`
- [ ] final `basket-validate`

## Useful SQL

Industry or sector benchmark, excluding fully ingested names:

```sql
select distinct bm.member_ticker as ticker
from benchmark_memberships bm
left join security_ingestion_status sis
  on sis.ticker = bm.member_ticker
where bm.benchmark_ticker = 'XSD'
  and lower(coalesce(bm.asset_class, '')) = 'equity'
  and regexp_matches(bm.member_ticker, '^[A-Z][A-Z0-9.\-]*$')
  and bm.member_ticker not in ('', '-', 'NAN', 'NONE', 'CASH', 'USD')
  and coalesce(sis.is_fully_ingested, false) = false
order by bm.member_ticker;
```

Static basket:

```sql
select * from (
  values ('AAPL'), ('MSFT'), ('NVDA')
) as t(ticker);
```

Coverage summary for a target basket:

```sql
select
  bm.benchmark_ticker,
  count(distinct bm.member_ticker) as target_tickers,
  count(distinct case when sis.has_price_history then bm.member_ticker end) as tickers_with_prices,
  count(distinct case when sis.has_historical_fundamentals then bm.member_ticker end) as tickers_with_fundamentals,
  count(distinct case when sis.is_fully_ingested then bm.member_ticker end) as fully_ingested_tickers
from benchmark_memberships bm
left join security_ingestion_status sis
  on sis.ticker = bm.member_ticker
where bm.benchmark_ticker = 'XSD'
  and lower(coalesce(bm.asset_class, '')) = 'equity'
  and regexp_matches(bm.member_ticker, '^[A-Z][A-Z0-9.\-]*$')
  and bm.member_ticker not in ('', '-', 'NAN', 'NONE', 'CASH', 'USD')
group by 1;
```

## Working Prompt

Use this prompt each time we work the next basket:

```text
Work in the dumb_money repo.

Target basket: <TICKER>
Target query SQL:
<PASTE SQL HERE>

Instructions:
- Resolve the ticker universe from DuckDB using the supplied SQL.
- Ingest 5 years of daily prices for the unresolved tickers only.
- Ingest historical period-aware fundamentals for the unresolved tickers only.
- Stage prices, fundamentals, and security_ingestion_status into DuckDB.
- Validate the joins:
  benchmark_memberships -> security_master
  benchmark_memberships -> normalized_prices
  benchmark_memberships -> normalized_fundamentals
- Report:
  targeted tickers
  newly ingested tickers
  skipped already-ingested tickers
  price rows staged
  fundamentals rows staged
  period types present
  failures or missing tickers

Scope rules:
- keep logic in shared modules
- preserve DuckDB as canonical
- do not widen to the next basket until validation is clean
- update the checklist/doc status for the basket before stopping
```
