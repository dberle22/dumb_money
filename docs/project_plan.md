# Dumb Money Project Plan

## Purpose

This document turns the implementation roadmap into a working project tracker we can use during build-out. It is designed to be updated sprint by sprint as work is completed, deferred, or expanded.

Primary planning source:

- [IMPLEMENTATION_ROADMAP.md]

Supporting references:

- [architecture.md]
- [data_models.md]
- [workflows.md]

## How To Use This Tracker

- Update sprint status at the top of each sprint: `Not Started`, `In Progress`, `Blocked`, `Done`
- Check off tasks as they are completed
- Treat acceptance criteria as the definition of done for the sprint
- Do not close a phase until its exit criteria are met
- Add links to PRs, notebooks, reports, or issues under the sprint notes section as work progresses

## Delivery Principles

- Build the shared data foundation before UI or LLM features
- Move reusable logic into `src/dumb_money/...`, not notebooks
- Keep outputs local-first, file-based, and easy to inspect
- Prefer stable data contracts and repeatable workflows over fast one-off prototypes
- Only advance to the next phase when the prior phase has a working handoff

## Current Assessment

Assessment date: `2026-04-03`

- Sprint 0 is complete:
  the repo has a consolidated top-level structure, one `pyproject.toml`, one `src/dumb_money/` package tree, a working `.gitignore`, legacy material isolated under `legacy/`, and a documented working local setup path on Python 3.12
- Sprint 1 is complete:
  benchmark ingestion now exists, the repo has a callable CLI entry point for prices, fundamentals, and benchmarks, and fixture-backed tests cover both ticker and benchmark ingestion paths
- Sprint 2 is complete:
  normalized staging transforms, benchmark sets, a first security master build, and a staging CLI path now exist with passing transform coverage
- Sprint 4 is complete:
  DuckDB is now the canonical analytical store for the expanded shared datasets, maintained-universe ingestion has been validated through broad benchmark runs including `IWM`, and shared `benchmark_mappings` now move benchmark assignment logic out of notebook and scorecard defaults
- The default dependency path now installs cleanly with `yfinance` as the required provider, while `yahooquery` has been moved to an optional `marketdata` extra
- Local verification now works in `.venv` on Python `3.12.13`:
  `python -m pytest -q` passes and `python -m ruff check .` passes
- The repo has populated local `data/raw/` and `data/staging/` datasets for a small sample universe, including `AAPL`, `STNE`, `ISRG`, `META`, `SNOW`, `CRM`, and benchmarks `SPY`, `QQQ`, `IWM`
- CSV-backed staging has been sufficient for the first small-universe workflows, but the next data-model phase should introduce a project DuckDB warehouse for scaled normalized and derived datasets

## Status Snapshot

| Sprint | Theme | Phase | Status | Target Outcome |
|---|---|---|---|---|
| 0 | Repo consolidation | Phase 0 | Done | Clean repo with one package structure and one dependency flow |
| 1 | Shared ingestion foundation | Phase 1 | Done | Reusable config, schemas, and ingestion entry points |
| 2 | Normalized staging layer | Phase 2 | Done | Canonical datasets for prices, fundamentals, security master, and benchmarks |
| 3 | Company research MVP | Phase 3 | In Progress | First end-to-end single-company research packet |
| 4 | Data foundation expansion | Phase 3 foundation | Done | Scalable shared datasets, benchmark mappings, and DuckDB storage |
| 5 | Sector and peer research MVP | Phase 4 | Done | Sector context and peer-relative research outputs |
| 6 | Reporting standardization | Phase 6 | Not Started | Repeatable report exports, scorecards, and chart helpers |
| 7 | Portfolio fit MVP | Phase 5 | Not Started | Holdings import and candidate fit analysis on shared data |
| 8 | App layer MVP | Phase 7 | Not Started | Local Streamlit app powered by shared modules |
| 9 | LLM summaries | Phase 8 | Not Started | Narrative summaries generated from structured outputs |

## Phase Map

| Phase | Goal | Depends On | Exit Milestone |
|---|---|---|---|
| 0 | Consolidate repo and clean project structure | None | Clean Consolidated Repo |
| 1 | Build shared data foundation | Phase 0 | Repeatable Market Data Pipeline |
| 2 | Normalize raw data into canonical datasets | Phase 1 | Repeatable Market Data Pipeline |
| 3 | Deliver company research MVP | Phase 2 | Company Research Workflow |
| 3 foundation | Expand scalable shared datasets and storage | Phase 3 | Expanded Shared Data Foundation |
| 4 | Add sector and peer context | Phase 3 | Expanded research context ready for reports and UI |
| 6 | Standardize reporting outputs | Phases 3, 3 foundation, 4 | Report-ready research artifacts |
| 5 | Deliver portfolio fit MVP | Phases 3, 3 foundation, 6 | Portfolio Fit Workflow |
| 7 | Expose workflows in app | Phases 4, 5, 6 | Usable Research App |
| 8 | Add LLM-assisted summaries | Phase 7 and stable structured outputs | Narrative insight layer |

## Sprint 0: Repo Consolidation

**Status:** Done

**Goal**

Turn the repo into a clean, single-project codebase with a stable top-level structure and one setup path.

**Planned Outputs**

- top-level package and directory structure aligned to roadmap
- one dependency definition and setup flow
- preserved legacy assets moved into intentional locations
- cleanup notes documenting what was kept, archived, or removed

**Tasks**

- [x] confirm top-level directories match the target structure in the roadmap
- [x] confirm `pyproject.toml` is the single dependency entry point
- [x] confirm `.gitignore` covers local envs, notebook artifacts, raw caches, and generated outputs as intended
- [x] audit `legacy/` contents and document which files remain source references versus migration candidates
- [x] move any remaining reusable legacy code into transitional locations if needed
- [x] isolate imported source material under `legacy/` rather than active package code
- [x] create the foundational repo structure for `app/`, `data/`, `docs/`, `notebooks/`, `reports/`, `src/`, and `tests/`

**Acceptance Criteria**

- repo has one obvious install and run path
- no nested repo artifacts or accidental environment files are tracked
- target top-level folders exist and are intentional
- legacy material is clearly separated from active source code

**Exit Criteria**

- Milestone 1 is satisfied:
  one dependency file, one package structure, and no imported repo junk committed

**Notes**

- Links:
- Recommended first implementation slice:
  build the shared peer foundation first by introducing a canonical `peer_sets` data product plus one reusable peer-relative valuation output that can be attached to the existing company research packet without changing scorecard scoring yet
- Why this slice comes first:
  it uses the strongest completed Sprint 4 prerequisites directly (`security_master`, historical and latest `normalized_fundamentals`, DuckDB-backed loaders, and `benchmark_mappings`), creates the missing shared peer contract needed by later return and scoring work, and avoids coupling the first Sprint 5 change to notebook-only workflows or custom basket modeling
- Suggested implementation order:
  1. add a canonical DuckDB-backed `peer_sets` table and shared loader path
  2. implement deterministic peer-group construction rules from `security_master`
  3. implement one shared peer valuation comparison output using `peer_sets` plus latest fundamentals snapshots
  4. attach that peer valuation context to the company research packet and report helpers
  5. only after the peer contract is stable, expand into peer-relative return outputs, sector snapshots, and any scorecard percentile changes
- Suggested module boundaries for the first slice:
  - `src/dumb_money/storage/warehouse.py`:
    add the canonical `peer_sets` table spec and schema contract so DuckDB remains the system of record
  - `src/dumb_money/transforms/peer_sets.py`:
    own canonical peer-set construction and staging into DuckDB and optional CSV export
  - `src/dumb_money/research/company.py`:
    add a shared loader for `peer_sets` and extend the research packet assembly to include peer context from canonical tables only
  - `src/dumb_money/analytics/company.py`:
    add the first peer comparison builder, scoped narrowly to a peer valuation panel and peer summary statistics rather than a full peer research mart
  - `src/dumb_money/outputs/company_report.py`:
    add a notebook- and report-friendly peer valuation table only after the shared analytics output exists
- Suggested function boundaries for the first slice:
  - in `src/dumb_money/transforms/peer_sets.py`:
    `build_peer_sets_frame(security_master: pd.DataFrame, fundamentals: pd.DataFrame) -> pd.DataFrame`
    `stage_peer_sets(settings: AppSettings | None = None, write_warehouse: bool = True, write_csv: bool = True) -> pd.DataFrame`
  - in `src/dumb_money/research/company.py`:
    `load_peer_sets(*, settings: AppSettings | None = None) -> pd.DataFrame`
  - in `src/dumb_money/analytics/company.py`:
    `build_peer_valuation_comparison(ticker: str, peer_sets: pd.DataFrame, fundamentals: pd.DataFrame) -> pd.DataFrame`
    `build_peer_summary_stats(peer_valuation: pd.DataFrame) -> dict[str, Any]`
  - in `src/dumb_money/outputs/company_report.py`:
    `build_peer_valuation_table(packet: CompanyResearchPacket) -> pd.DataFrame`
- Proposed default peer-group rules for the first slice:
  - start from `security_master` and keep only active, research-eligible, non-benchmark common-stock names
  - exclude the focal ticker from its own peer rows
  - prefer same-industry peers first
  - fall back to same-sector peers when industry coverage is too thin
  - use latest fundamentals only as a lightweight eligibility or ordering aid, not as a second independent peer-definition system
  - record `relationship_type`, `selection_method`, and `peer_order` on every row so downstream research and scorecard logic can remain deterministic
- Scope guardrails for the first slice:
  - do not change scorecard weights or replace absolute valuation scoring yet
  - do not start with notebook-only peer logic
  - do not make custom benchmark baskets a prerequisite for peer research
  - do not build sector snapshots before peer-set rules are stable enough to reuse there
- Focused tests to write first:
  - transform tests for industry-first peer selection, sector fallback behavior, exclusion rules, and deterministic ordering
  - warehouse tests for `peer_sets` DuckDB write and read round-trips and schema enforcement
  - analytics tests for peer valuation joins, peer medians, peer counts, and missing-data handling
  - one research integration test confirming company research can consume canonical peer context without changing current benchmark-mapping behavior
  `docs/universe_ingestion_checklist.md`
- Follow-up cleanup:
  committed `.DS_Store` files still exist under `legacy/`; `.gitignore` now covers them, but they should be removed from git in a later housekeeping pass if we want a fully clean index
- Environment setup:
  `README.md` now includes project setup, install, lint, and test commands, and the repo has a working verified `.venv` on Python 3.12
- Dependency note:
  core installs use `yfinance`; `yahooquery` is now optional via the `marketdata` extra so environment setup is not blocked by that dependency chain

## Sprint 1: Shared Ingestion Foundation

**Status:** Done

**Goal**

Create the shared configuration, schemas, and ingestion modules that all research workflows will depend on.

**Planned Outputs**

- shared config module under `src/dumb_money/config/`
- canonical schema models for securities, benchmarks, prices, fundamentals, and holdings
- price ingestion module
- fundamentals ingestion module
- benchmark ingestion module
- basic CLI or script entry points for ingestion workflows

**Tasks**

- [x] implement or refine canonical Pydantic models based on [data_models.md]
- [x] create shared config/settings module for paths, providers, and defaults
- [x] refactor price ingestion logic into `src/dumb_money/ingestion/prices.py`
- [x] refactor fundamentals ingestion logic into `src/dumb_money/ingestion/fundamentals.py`
- [x] create `src/dumb_money/ingestion/benchmarks.py`
- [x] define raw file naming conventions and output path helpers
- [x] add entry points in `scripts/` or `src/dumb_money/cli/`
- [x] add fixtures for at least one ticker and one benchmark path
- [x] add initial tests covering config resolution and ingestion contracts

**Acceptance Criteria**

- prices can be ingested through one repeatable module entry point
- fundamentals can be ingested through one repeatable module entry point
- benchmark definitions can be created without hard-coding them inside analytics logic
- all raw outputs land in predictable directories with consistent names

**Exit Criteria**

- shared ingestion modules exist and are callable
- schema contracts are documented and reflected in code
- repo can produce consistent raw outputs for a small known ticker set

**Notes**

- Links:
- Current implementation evidence:
  `src/dumb_money/config/settings.py`, `src/dumb_money/models/`, `src/dumb_money/ingestion/prices.py`, `src/dumb_money/ingestion/fundamentals.py`, `src/dumb_money/ingestion/benchmarks.py`, `src/dumb_money/cli/main.py`, and fixture-backed tests under `tests/`
- Verification snapshot:
  `.venv/bin/python -m pytest -q` passes and `.venv/bin/python -m ruff check .` passes on `2026-03-30`
- Environment note:
  the working local setup now uses Python 3.12, the default install path is unblocked, and `yahooquery` is optional rather than required

## Sprint 2: Normalized Staging Layer

**Status:** Done

**Goal**

Transform raw provider outputs into canonical staging datasets that downstream analytics can trust.

**Planned Outputs**

- normalized price tables
- normalized fundamentals tables
- initial security master dataset
- benchmark set definitions
- transform tests for critical normalization paths

**Tasks**

- [x] implement price normalization transforms into `data/staging/normalized_prices/`
- [x] implement fundamentals normalization transforms into `data/staging/normalized_fundamentals/`
- [x] create security master build logic in `src/dumb_money/transforms/`
- [x] create benchmark set generation logic
- [x] enforce canonical field names and type coercion rules
- [x] handle missing `adj_close` fallback logic consistently
- [x] add transform validation tests for representative raw inputs
- [x] document staging output contracts in [data_models.md] if implementation expands them

**Acceptance Criteria**

- normalized outputs use a stable schema across providers
- security metadata is stitchable by ticker and usable in downstream joins
- benchmark sets are reusable and not embedded inside notebook logic
- transform tests cover the highest-risk field mappings and null handling

**Exit Criteria**

- Milestone 2 is satisfied:
  prices and fundamentals ingest from one command path and land in predictable raw and staging folders

**Notes**

- Links:
- Current implementation evidence:
  `src/dumb_money/transforms/prices.py`, `src/dumb_money/transforms/fundamentals.py`, `src/dumb_money/transforms/security_master.py`, `src/dumb_money/transforms/benchmark_sets.py`, and `src/dumb_money/cli/main.py`
- Verification snapshot:
  `.venv/bin/python -m pytest -q` passes and `.venv/bin/python -m ruff check .` passes on `2026-03-30`

## Sprint 3: Company Research MVP

**Status:** Done

**Goal**

Deliver the first complete company research workflow for a single ticker using the normalized foundation.

**Planned Outputs**

- reusable company analytics modules
- benchmark comparison logic
- company scorecard
- company research notebook or report packet
- standard charts and tables for one ticker
- narrative-oriented notebook flow aligned to the scorecard visual spec

**Tasks**

- [x] implement return calculations across standard windows
- [x] implement volatility, drawdown, and relative strength metrics
- [x] implement moving average and simple trend metrics
- [x] build a fundamentals summary layer from normalized snapshots
- [x] implement company versus benchmark comparison outputs
- [x] define company scorecard structure and scoring rules
- [x] create a notebook in `notebooks/02_company_research/` using shared modules only
- [x] add tests for core analytics calculations
- [ ] align the notebook and shared report helpers to the scorecard visual narrative spec using the visuals supported by current data
- [x] document the next enabling data-model work for benchmark mapping, historical fundamentals, peer sets, and DuckDB storage without expanding sprint scope to implement them yet

**Acceptance Criteria**

- one ticker can generate a consistent research packet end to end
- all calculations come from reusable modules, not notebook-only code
- benchmark comparison outputs match the company scorecard inputs
- core analytics functions are covered by tests using stable fixtures
- the first notebook review flow follows a coherent research narrative rather than a disconnected artifact dump
- the documentation clearly distinguishes Sprint 3 visual work from later data-model expansions

**Exit Criteria**

- Milestone 3 is satisfied:
  one ticker can produce a standard research packet and benchmark comparisons work end to end

**Notes**

- Links:
- Current implementation evidence:
  `src/dumb_money/analytics/company.py`, `src/dumb_money/analytics/scorecard.py`, `src/dumb_money/research/company.py`, `src/dumb_money/outputs/company_report.py`, `notebooks/02_company_research/aapl_company_research.ipynb`, `tests/analytics/test_company.py`, `tests/analytics/test_scorecard.py`, `tests/outputs/test_company_report.py`, and `tests/research/test_company_research.py`
- Verification snapshot:
  `.venv/bin/python -m pytest -q` passes and `.venv/bin/python -m ruff check .` passes on `2026-03-31`
- Scope note:
  the current sprint now includes refining the single-ticker notebook and shared visuals to match the scorecard narrative spec where current data supports it; benchmark mapping formalization, historical fundamentals, and peer-set modeling are documented follow-on data tasks rather than required Sprint 3 implementation scope

### Sprint 3 Follow-On: Data Model Expansion Plan

This is intentionally documented inside Sprint 3 scope notes rather than treated as completed Sprint 3 implementation work.

Priority follow-on items identified during the first company research workflow:

- expand `security_master` from a small stitched sample into a broader reusable research universe
- scale `normalized_prices` to match the expanded security universe rather than a manually curated sample list
- redesign `normalized_fundamentals` to support quarterly, annual, and `TTM` period-aware snapshots
- split benchmark modeling into reusable definitions, mappings, sets, and custom basket memberships
- introduce DuckDB as the canonical analytical storage layer for normalized and derived tables, with CSV outputs retained as optional inspection artifacts

## Sprint 4: Data Foundation Expansion

**Status:** In Progress

**Goal**

Expand the shared analytical data foundation so company, sector, and peer workflows can scale beyond the current CSV-based sample universe.

**Planned Outputs**

- expanded `security_master` universe and enrichment workflow
- broader `normalized_prices` coverage aligned to the maintained universe
- period-aware `normalized_fundamentals` with quarterly, annual, and `TTM` support
- benchmark mapping and benchmark membership tables
- DuckDB warehouse for normalized and derived datasets
- shared loader pattern for canonical analytical tables

**Workstreams**

### Workstream 1: Storage And Access Layer

Goal:
make DuckDB the canonical analytical store before the table surface area expands further.

Tasks:

- [x] define the DuckDB warehouse location, naming convention, and config settings
- [x] define canonical table names for normalized and derived datasets
- [x] implement shared read and write helpers that prefer DuckDB and allow optional CSV export
- [x] document which outputs remain raw-file artifacts versus which become warehouse-backed analytical tables
- [x] add tests covering DuckDB write, read, overwrite, and schema checks for a small fixture dataset

### Workstream 2: Security Master Expansion

Goal:
turn `security_master` into the maintained eligible universe table for downstream ingestion and joins.

Tasks:

- [x] define the expanded `security_master` schema with lineage, coverage, and active-status fields
- [x] choose and document the source strategy for broad universe coverage plus metadata enrichment and manual overrides
- [x] implement `security_master` expansion workflow for a larger maintained universe
- [x] define the manual override pattern for aliases, classification cleanup, and benchmark exceptions
- [x] add validation checks for duplicate tickers, missing classifications, and inactive or unsupported listings

### Workstream 3: Historical Fundamentals Model

Goal:
upgrade `normalized_fundamentals` into a period-aware historical table that supports quarterly, annual, and `TTM` analysis.

Tasks:

- [x] redesign `normalized_fundamentals` to include `period_end_date`, `report_date`, `fiscal_year`, `fiscal_quarter`, `fiscal_period`, and `period_type`
- [x] support both quarterly and annual fundamentals rows in staging
- [x] add any balance-sheet fields needed for historical balance sheet and liquidity analysis when providers support them reliably
- [x] define deduplication rules for one row per ticker-period snapshot
- [x] add tests covering mixed quarterly and annual fundamentals inputs and expected normalized outputs

### Workstream 4: Benchmark Data Model Split

Goal:
separate benchmark definitions, default mappings, reusable sets, and custom basket memberships into explicit shared tables.

Tasks:

- [x] split benchmark data into reusable definitions, mappings, sets, and current-snapshot benchmark memberships
- [x] define default assignment logic for `primary_benchmark`, `sector_benchmark`, `industry_benchmark`, `style_benchmark`, and optional `custom_benchmark`
- [x] define the shared current-benchmark membership contract used for benchmark ETFs and indexes, with mixed custom composites deferred to Sprint 5
- [x] add tests covering benchmark mapping resolution and benchmark membership integrity

### Workstream 5: Universe-Aligned Ingestion And Validation

Goal:
make the expanded foundation operational by connecting the maintained universe back to ingestion and staging workflows.

Tasks:

- [x] align recurring price ingestion targets to the maintained security universe
- [x] define how fundamentals ingestion should target the same maintained universe over time
- [x] preserve optional CSV exports for fixtures, inspection, and manual debugging
- [x] add end-to-end validation that a maintained ticker can flow through security master, prices, fundamentals, and benchmark assignment using shared loaders

**Recommended Sequence**

1. Finish Workstream 1 so storage and loader decisions are stable before expanding tables.
2. Finish Workstream 2 so the eligible universe is explicit.
3. Finish Workstream 3 so historical fundamentals have a stable contract.
4. Finish Workstream 4 so benchmark assignment and custom baskets are modeled cleanly.
5. Finish Workstream 5 to operationalize the expanded foundation.

**Checkpoint Milestones**

- `Sprint 4A`: DuckDB warehouse and shared loader path work for the current normalized tables
- `Sprint 4B`: expanded `security_master` and universe-driven price coverage work
- `Sprint 4C`: period-aware `normalized_fundamentals` and benchmark mapping tables work
- `Sprint 4D`: the full expanded foundation passes end-to-end validation for a maintained universe subset

**Acceptance Criteria**

- DuckDB is the canonical analytical store for normalized and derived tables used by shared loaders
- `security_master` can represent a broad research universe beyond the initial sample tickers
- price ingestion targets are driven by the maintained universe rather than notebook-specific lists
- `normalized_fundamentals` supports quarter-aware and annual historical snapshots
- benchmark assignments are represented by explicit shared tables and current benchmark memberships are modeled in shared canonical datasets
- the repo can materialize normalized analytical tables into DuckDB and load them through shared access paths

**Exit Criteria**

- the shared data foundation can support broader company coverage, historical balance-sheet work, and benchmark assignment without relying on CSV-only joins

**Notes**

- Links:
- Suggested implementation order:
  start with storage and access abstractions, then expand the universe table, then redesign historical fundamentals, then split benchmark modeling, and only then broaden recurring coverage
- Sprint 4B implementation notes:
  the repo now stages `listed_security_seed` from listed-security directory inputs, applies `security_master_overrides`, expands `security_master` to include lineage and eligibility fields, and validates duplicate tickers and unsupported classifications before materializing the canonical table
- Sprint 4 benchmark membership notes:
  current-snapshot benchmark definitions now refresh from `data/raw/benchmark_holdings/etf_benchmark_mapping.csv`, benchmark constituent memberships materialize from the mapped holdings files, and a join-ready benchmark membership coverage table shows which constituents already exist in `security_master`
- Sprint 4 historical fundamentals notes:
  `normalized_fundamentals` is now a period-aware historical table in DuckDB with quarterly, annual, and `TTM` rows, provider payload lineage, deduplicated ticker-period staging rules, and fixture-backed transform coverage for mixed-period inputs
- Sprint 4 maintained-universe ingestion notes:
  ticker selection is now a shared concern under `src/dumb_money/universe.py`; ingestion can target either an explicit static ticker list or a DuckDB SQL selector, and benchmark-derived universes such as `DIA` are resolved through SQL against `benchmark_memberships` rather than notebook or ad hoc Python filters
- Sprint 4 maintained-universe validation notes:
  the repo has now proven the expanded foundation on maintained benchmark universes including `DIA`, `SPY`, and a 15-batch `IWM` run. Final `IWM` validation on `2026-04-03` shows `1936` target tickers, `1925` fully ingested tickers, no `security_master` or `normalized_fundamentals` gaps, and 11 residual price misses isolated for targeted cleanup
- Sprint 4 benchmark assignment notes:
  `benchmark_mappings` is now a shared canonical data product built from `security_master`, `benchmark_definitions`, `benchmark_memberships`, and the benchmark holdings reference mapping. The live repo currently materializes `5523` active assignment rows into DuckDB and CSV, and company research can consume those mappings when present
- Sprint 4 closeout:
  mixed custom benchmark composites, peer grouping, sector snapshots, and broader research interpretation now move forward as Sprint 5 work rather than remaining Sprint 4 blockers

## Sprint 5: Sector And Peer Research MVP

**Status:** Done

**Goal**

Add sector and peer context so company research can be interpreted relative to comparable businesses and sector benchmarks.

**Planned Outputs**

- sector mapping logic
- peer group definitions
- peer-relative return and valuation comparison outputs
- sector snapshot tables
- sector and peer research notebook
- reusable benchmark mapping outputs for sectors, industries, and custom baskets

**Tasks**

- [x] expand `security_master` coverage and enrichment so sector and industry fields are available for a broad eligible universe
- [x] define standard sector and industry benchmark mapping logic and benchmark associations
- [x] create benchmark mapping outputs for `primary_benchmark`, `sector_benchmark`, `industry_benchmark`, `style_benchmark`, and optional `custom_benchmark`
- [x] create custom benchmark basket membership outputs that can combine ETFs, indexes, or stocks
- [x] define peer group rules for company comparison
- [x] implement peer-relative return comparison outputs
- [x] implement peer-relative valuation comparison outputs
- [x] build sector snapshot tables for reusable downstream consumption
- [x] create notebook in `notebooks/03_sector_research/`
- [x] add tests for peer grouping and sector mapping logic

**Acceptance Criteria**

- sector context can be attached to a researched company without manual notebook edits
- benchmark assignments are generated from shared data products rather than notebook rules
- peer group definitions are explicit and reusable
- outputs support both company research and future app/report views
- sector and peer comparisons are generated from shared data products

**Exit Criteria**

- sector and peer outputs are stable enough to support reporting and app integration
- company research can be supplemented with sector and peer context in one workflow

**Notes**

- Links:
- Sprint 5 closeout:
  shared `peer_sets`, peer-relative return outputs, peer-relative valuation outputs, sector snapshot tables, a sector research notebook, and mixed custom benchmark basket membership support now exist as shared code paths rather than notebook-only logic

## Sprint 6: Reporting Standardization

**Status:** Not Started

**Goal**

Standardize reporting around an iterative section-by-section workflow so each report section can be built, rendered, debugged, and reviewed visually before moving to the next one.

**Planned Outputs**

- section-level shared report outputs built from DuckDB-backed shared data products
- one report file per section for review and debugging
- standardized table and chart builders for each section
- repeatable section review workflow for notebooks and later exports
- standardized scorecard and narrative formatting that can be reused across reports

**Tasks**

- [ ] define the canonical Sprint 6 report sections and lock the section-by-section implementation order
- [ ] structure report-building so each section lands as a single file that can be reviewed independently
- [ ] keep DuckDB as the canonical source for shared analytical tables used by report sections
- [ ] standardize section interfaces so each section has explicit shared inputs, table outputs, chart outputs, and focused tests
- [ ] implement `Market Performance` first as the anchor section for the Sprint 6 workflow
- [ ] implement `Research Summary` second using the shared scorecard and section formatting patterns
- [ ] implement `Trend and Risk Profile` once the market section contract is stable
- [ ] implement `Balance Sheet Strength` as a point-in-time standardized section before expanding peer context
- [ ] implement `Valuation` with shared current-state outputs first, then shared peer context where supported
- [ ] implement `Peer Positioning` using canonical `peer_sets`, peer return outputs, and peer valuation outputs
- [ ] implement `Score Decomposition` as a standardized transparency section built on shared scorecard outputs
- [ ] implement `Final Research Summary` only after upstream sections expose stable structured fields
- [ ] defer `Growth and Profitability` trend visuals until canonical historical fundamentals support is ready
- [ ] update report loaders and section builders to prefer DuckDB-backed shared datasets and marts over raw-file or notebook-only logic
- [ ] add focused tests for each section before advancing to the next section
- [ ] keep notebooks as thin review layers over shared section modules rather than defining section logic inline

**Recommended Section Order**

1. `Market Performance`
2. `Research Summary`
3. `Trend and Risk Profile`
4. `Balance Sheet Strength`
5. `Valuation`
6. `Peer Positioning`
7. `Score Decomposition`
8. `Final Research Summary`
9. `Growth and Profitability`

**Section Plan**

1. `Market Performance`
   Narrative goal:
   show whether the stock has outperformed the broader market and its sector or style benchmark
   Shared data inputs:
   canonical company prices, benchmark mappings, benchmark prices, trailing return windows
   Visuals/tables:
   indexed price chart, trailing return comparison, optional excess-return chart, shared comparison table
   Shared boundaries:
   analytics in shared company analytics modules, section formatting in report outputs, thin review file for this section only
   Focused tests:
   return-window math, benchmark alignment, indexed rebasing, missing benchmark handling
   Risks/dependencies:
   benchmark assignment must stay shared and benchmark price access should continue moving toward canonical DuckDB-backed paths

2. `Research Summary`
   Narrative goal:
   orient the reader quickly around total score, strongest support, and main watch items
   Shared data inputs:
   company metadata, total score, category scores, interpretation label, strengths, constraints
   Visuals/tables:
   score summary strip, summary table, short memo summary
   Shared boundaries:
   score interpretation and summary text should remain shared output builders, not notebook prose
   Focused tests:
   strongest and weakest pillar selection, interpretation label mapping, empty-metric fallback behavior
   Risks/dependencies:
   current strengths and constraints are heuristic and should remain deterministic until richer narrative fields exist

3. `Trend and Risk Profile`
   Narrative goal:
   show whether returns came with acceptable drawdowns, volatility, and trend structure
   Shared data inputs:
   price history, moving averages, drawdown series, volatility metrics, current drawdown
   Visuals/tables:
   drawdown chart, price plus moving-average chart, compact risk panel
   Shared boundaries:
   reusable derived risk series in analytics and thin presentation helpers in outputs
   Focused tests:
   drawdown-series correctness, moving-average calculations, insufficient-history handling
   Risks/dependencies:
   beta and downside-volatility outputs are still missing and should not block the first standardized section

4. `Balance Sheet Strength`
   Narrative goal:
   evaluate leverage, liquidity, and financial resilience in a standardized point-in-time section
   Shared data inputs:
   latest fundamentals snapshot, derived leverage metrics, later peer medians where available
   Visuals/tables:
   balance-sheet scorecard table first, peer leverage comparison later
   Shared boundaries:
   metric derivation stays in shared scorecard and analytics modules, interpretation and display stay in report outputs
   Focused tests:
   leverage calculations, zero-debt handling, unavailable EBITDA behavior, interpretation flags
   Risks/dependencies:
   interest coverage is still not modeled from canonical inputs

5. `Valuation`
   Narrative goal:
   show whether valuation is supportive, fair, or constraining relative to current fundamentals and peers
   Shared data inputs:
   valuation multiples, free-cash-flow yield, peer medians, scorecard valuation metrics
   Visuals/tables:
   current-state valuation table first, peer comparison visual second
   Shared boundaries:
   peer-relative valuation logic remains shared in analytics and scorecard modules with thin report formatting
   Focused tests:
   peer-median fallback, free-cash-flow-yield derivation, missing peer coverage
   Risks/dependencies:
   historical valuation bands and valuation-versus-growth views remain out of scope until more canonical history exists

6. `Peer Positioning`
   Narrative goal:
   show where the company sits versus canonical peers on returns and valuation
   Shared data inputs:
   canonical `peer_sets`, peer return comparison outputs, peer valuation comparison outputs, sector snapshot context
   Visuals/tables:
   peer return table, peer valuation table, later compact peer-positioning visuals
   Shared boundaries:
   peer definition and peer analytics remain shared data products; section file should only review and render them
   Focused tests:
   peer ordering, focal-company inclusion, missing peer fundamentals and prices, peer summary stats
   Risks/dependencies:
   this section depends on stable canonical peer data and should avoid notebook-only peer logic

7. `Score Decomposition`
   Narrative goal:
   make the score transparent and easy to inspect at category and metric level
   Shared data inputs:
   metric-level scores, category weights, coverage ratios, confidence fields
   Visuals/tables:
   score decomposition chart, metric score table
   Shared boundaries:
   score math remains in the shared scorecard module and this section standardizes the review presentation
   Focused tests:
   score-total consistency, sort order, coverage calculations, missing metric behavior
   Risks/dependencies:
   low technical risk, but should follow the upstream section contracts so labels stay stable

8. `Final Research Summary`
   Narrative goal:
   close the memo with a short, standardized takeaway tied back to structured evidence
   Shared data inputs:
   total score, strongest and weakest sections, major valuation and risk watch items
   Visuals/tables:
   short final memo text and optional compact closing summary panel
   Shared boundaries:
   final summary should be generated from stable shared section outputs rather than handcrafted notebook text
   Focused tests:
   narrative branch selection, missing-data fallback, consistency with upstream score outputs
   Risks/dependencies:
   should be implemented after the upstream sections expose stable structured fields

9. `Growth and Profitability`
   Narrative goal:
   show whether the business is growing, compounding, and improving over time
   Shared data inputs:
   historical revenue, EPS, margin, and return-on-capital series plus peer medians when available
   Visuals/tables:
   growth trend charts, margin trend charts, return-on-capital summary
   Shared boundaries:
   requires a canonical historical fundamentals data product before the report section is standardized
   Focused tests:
   period alignment, growth-rate math, sparse-history handling, TTM versus annual handling
   Risks/dependencies:
   this remains the clearest dependency-blocked section and should come after the ready-now sections

**Single-File Section Workflow**

- each report section should have its own file so we can build, debug, and review it independently
- section files should stay thin and depend on shared analytics and shared data products rather than embedding logic inline
- section files should be landable one at a time with their own focused tests and review pass
- notebooks should remain section reviewers, not the source of truth for section logic

**Acceptance Criteria**

- each report section can be rendered and reviewed independently before the full report is assembled
- scorecards, tables, and charts use shared section builders and shared presentation helpers
- report section logic lives in shared modules or section files rather than notebook-only code
- DuckDB-backed shared tables remain the canonical analytical source for report sections

**Exit Criteria**

- at least the ready-now report sections can be built and reviewed one by one through a repeatable shared workflow
- the report layer is stable enough for internal section-by-section review and later app embedding
- section outputs are standardized enough that a full end-to-end report becomes assembly work rather than bespoke notebook work

**Notes**

- Links:

## Sprint 7: Portfolio Fit MVP

**Status:** Not Started

**Goal**

Use the now-mature shared data foundation and reporting layer to analyze a current portfolio and evaluate a new candidate security.

**Planned Outputs**

- holdings schema and import workflow
- portfolio metrics and exposure outputs
- candidate fit analysis
- portfolio-fit notebook
- watchlist or decision-support table

**Tasks**

- [ ] finalize holdings schema implementation in code
- [ ] create holdings import workflow for file-based inputs
- [ ] build allocation and concentration metrics
- [ ] build exposure metrics by sector, industry, and position weight where data allows
- [ ] implement portfolio versus benchmark comparison
- [ ] implement candidate ticker fit logic against current holdings
- [ ] define watchlist output structure and scoring fields
- [ ] create notebook in `notebooks/04_portfolio_fit/`
- [ ] add tests for holdings parsing and portfolio metric calculations

**Acceptance Criteria**

- a holdings file can be validated and ingested
- current portfolio metrics can be generated from shared staging datasets and standardized outputs
- candidate ticker fit uses explicit, inspectable rules rather than ad hoc notebook logic
- output tables are suitable for later report and UI reuse

**Exit Criteria**

- Milestone 7 is satisfied:
  a holdings file can be ingested and a current portfolio plus candidate ticker can be evaluated together

**Notes**

- Links:

## Sprint 8: App Layer MVP

**Status:** Not Started

**Goal**

Expose the core research flows in a simple local Streamlit app backed by the shared analytics and output layers.

**Planned Outputs**

- Streamlit app shell
- company research page
- sector research page
- portfolio-fit page
- shared UI components

**Tasks**

- [ ] create initial Streamlit app structure under `app/streamlit/`
- [ ] build company research page using precomputed or on-demand shared modules
- [ ] build sector research page
- [ ] build portfolio-fit page
- [ ] create shared page layout and reusable UI helpers
- [ ] connect app pages to standardized report and analytics outputs
- [ ] validate local run workflow and document it in `README.md`

**Acceptance Criteria**

- app runs locally from one documented command path
- company, sector, and portfolio workflows are exposed in the UI
- app logic does not duplicate analytics logic from notebooks or core modules

**Exit Criteria**

- Milestone 8 is satisfied:
  local dashboard exposes company, sector, and portfolio workflows using shared modules

**Notes**

- Links:

## Sprint 9: LLM Summaries And Decision Support

**Status:** Not Started

**Goal**

Generate narrative summaries only after the structured research layer is trustworthy and inspectable.

**Planned Outputs**

- summary input schema
- prompt generation pipeline
- narrative summaries for company, sector, and portfolio views
- summary validation checks against source metrics

**Tasks**

- [ ] define structured summary inputs from scorecards, tables, and report outputs
- [ ] implement prompt assembly from structured analytics outputs
- [ ] create company summary generation flow
- [ ] create sector summary generation flow
- [ ] create portfolio summary generation flow
- [ ] add validation or guardrails to ensure summaries align with source metrics
- [ ] document model usage and failure modes

**Acceptance Criteria**

- summaries are generated from structured data, not raw provider payloads
- summary pipeline is optional and does not block non-LLM workflows
- validation checks reduce risk of metric mismatches or unsupported claims

**Exit Criteria**

- narrative output is useful, inspectable, and tied back to structured scorecards and tables

**Notes**

- Links:

## Cross-Sprint Backlog

Use this section for work that spans multiple sprints or needs to be tracked continuously.

### Engineering Quality

- [ ] add `ruff` and `pytest` commands to documented local workflow
- [ ] define fixture strategy for raw and normalized sample data
- [ ] establish minimum testing expectations per module area
- [ ] keep notebook logic thin and refactor reusable logic into `src/`

### Documentation

- [ ] expand [architecture.md] as module boundaries solidify
- [ ] expand [workflows.md] with actual run commands and data flow steps
- [ ] keep [data_models.md] synchronized with implementation
- [ ] update `README.md` whenever setup or core usage changes

### Risks To Watch

- [ ] useful legacy behavior may still be trapped in notebooks or copied scripts
- [ ] provider schemas may drift or return incomplete data
- [ ] portfolio-fit scope may expand too quickly without explicit scoring boundaries
- [ ] app work may start before analytics contracts are stable
- [ ] LLM summary work may outrun structured scorecard maturity

## Definition Of MVP

The MVP is complete when the repo can:

- ingest prices and fundamentals for a defined ticker set
- normalize and store those outputs consistently
- produce a company research output with benchmark comparisons
- ingest a simple holdings file
- evaluate a candidate security for portfolio fit
- generate repeatable notebook or report outputs from shared modules

## Update Log

| Date | Update | Owner |
|---|---|---|
| 2026-03-29 | Initial project plan created from implementation roadmap | Codex |
| 2026-04-04 | Expanded Sprint 6 into a section-by-section reporting plan with single-file-per-section review workflow | Codex |
