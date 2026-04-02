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

Assessment date: `2026-03-30`

- Sprint 0 is complete:
  the repo has a consolidated top-level structure, one `pyproject.toml`, one `src/dumb_money/` package tree, a working `.gitignore`, legacy material isolated under `legacy/`, and a documented working local setup path on Python 3.12
- Sprint 1 is complete:
  benchmark ingestion now exists, the repo has a callable CLI entry point for prices, fundamentals, and benchmarks, and fixture-backed tests cover both ticker and benchmark ingestion paths
- Sprint 2 is complete:
  normalized staging transforms, benchmark sets, a first security master build, and a staging CLI path now exist with passing transform coverage
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
| 4 | Data foundation expansion | Phase 3 foundation | In Progress | Scalable shared datasets, benchmark mappings, and DuckDB storage |
| 5 | Sector and peer research MVP | Phase 4 | Not Started | Sector context and peer-relative research outputs |
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

**Status:** In Progress

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
- [ ] define default assignment logic for `primary_benchmark`, `sector_benchmark`, `industry_benchmark`, `style_benchmark`, and optional `custom_benchmark`
- [ ] define a benchmark basket membership contract that supports ETFs, indexes, and stocks in one table
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
- benchmark assignments and custom benchmark baskets are represented by explicit shared tables
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
  the repo has now proven the expanded foundation on the 30 real `DIA` constituents: about 5 years of daily prices and historical fundamentals stage successfully into DuckDB, and direct joins across `benchmark_memberships -> security_master -> normalized_prices -> normalized_fundamentals` validate the shared-table path end to end
- Remaining Sprint 4 scope:
  broaden recurring coverage from the validated `DIA` subset to additional maintained universes, continue provider-scale validation, and finish benchmark assignment/custom basket modeling before declaring Sprint 4 fully complete

## Sprint 5: Sector And Peer Research MVP

**Status:** Not Started

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

- [ ] expand `security_master` coverage and enrichment so sector and industry fields are available for a broad eligible universe
- [ ] define standard sector and industry benchmark mapping logic and benchmark associations
- [ ] create benchmark mapping outputs for `primary_benchmark`, `sector_benchmark`, `industry_benchmark`, `style_benchmark`, and optional `custom_benchmark`
- [ ] create custom benchmark basket membership outputs that can combine ETFs, indexes, or stocks
- [ ] define peer group rules for company comparison
- [ ] implement peer-relative return comparison outputs
- [ ] implement peer-relative valuation comparison outputs
- [ ] build sector snapshot tables for reusable downstream consumption
- [ ] create notebook in `notebooks/03_sector_research/`
- [ ] add tests for peer grouping and sector mapping logic

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

## Sprint 6: Reporting Standardization

**Status:** Not Started

**Goal**

Turn research outputs into repeatable deliverables with consistent formatting and export behavior.

**Planned Outputs**

- standard table builders
- chart helper utilities
- report templates
- generated Markdown or HTML reports
- standardized scorecard formatting

**Tasks**

- [ ] define standard report sections for company, sector, and portfolio outputs
- [ ] implement reusable table rendering helpers
- [ ] implement reusable chart rendering helpers
- [ ] create report templates in `reports/templates/`
- [ ] implement export flow for Markdown or HTML reports
- [ ] standardize scorecard presentation and labeling
- [ ] update report loaders to prefer DuckDB-backed shared datasets and marts where available
- [ ] generate at least one sample report for company research and one for portfolio fit
- [ ] add smoke tests for report generation paths where practical

**Acceptance Criteria**

- research outputs can be exported without manual notebook copy/paste
- scorecards, tables, and charts use a shared presentation layer
- generated reports are readable, repeatable, and sourced from shared modules

**Exit Criteria**

- report generation is stable enough for internal use and app embedding
- at least two workflow types can produce standardized outputs

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
