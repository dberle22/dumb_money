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

## Status Snapshot

| Sprint | Theme | Phase | Status | Target Outcome |
|---|---|---|---|---|
| 0 | Repo consolidation | Phase 0 | Not Started | Clean repo with one package structure and one dependency flow |
| 1 | Shared ingestion foundation | Phase 1 | Not Started | Reusable config, schemas, and ingestion entry points |
| 2 | Normalized staging layer | Phase 2 | Not Started | Canonical datasets for prices, fundamentals, security master, and benchmarks |
| 3 | Company research MVP | Phase 3 | Not Started | First end-to-end single-company research packet |
| 4 | Portfolio fit MVP | Phase 5 | Not Started | Holdings import and candidate fit analysis on shared data |
| 5 | Sector and peer research MVP | Phase 4 | Not Started | Sector context and peer-relative research outputs |
| 6 | Reporting standardization | Phase 6 | Not Started | Repeatable report exports, scorecards, and chart helpers |
| 7 | App layer MVP | Phase 7 | Not Started | Local Streamlit app powered by shared modules |
| 8 | LLM summaries | Phase 8 | Not Started | Narrative summaries generated from structured outputs |

## Phase Map

| Phase | Goal | Depends On | Exit Milestone |
|---|---|---|---|
| 0 | Consolidate repo and clean project structure | None | Clean Consolidated Repo |
| 1 | Build shared data foundation | Phase 0 | Repeatable Market Data Pipeline |
| 2 | Normalize raw data into canonical datasets | Phase 1 | Repeatable Market Data Pipeline |
| 3 | Deliver company research MVP | Phase 2 | Company Research Workflow |
| 5 | Deliver portfolio fit MVP | Phase 3 | Portfolio Fit Workflow |
| 4 | Add sector and peer context | Phase 3 | Expanded research context ready for reports and UI |
| 6 | Standardize reporting outputs | Phases 3, 4, 5 | Report-ready research artifacts |
| 7 | Expose workflows in app | Phases 4, 5, 6 | Usable Research App |
| 8 | Add LLM-assisted summaries | Phase 7 and stable structured outputs | Narrative insight layer |

## Sprint 0: Repo Consolidation

**Status:** Not Started

**Goal**

Turn the repo into a clean, single-project codebase with a stable top-level structure and one setup path.

**Planned Outputs**

- top-level package and directory structure aligned to roadmap
- one dependency definition and setup flow
- preserved legacy assets moved into intentional locations
- cleanup notes documenting what was kept, archived, or removed

**Tasks**

- [ ] confirm top-level directories match the target structure in the roadmap
- [ ] confirm `pyproject.toml` is the single dependency entry point
- [ ] confirm `.gitignore` covers local envs, notebook artifacts, raw caches, and generated outputs as intended
- [ ] audit `legacy/` contents and document which files remain source references versus migration candidates
- [ ] move any remaining reusable legacy code into transitional locations if needed
- [ ] document environment setup in `README.md`
- [ ] create a short migration note section in docs or roadmap if repo cleanup decisions need to be preserved

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

## Sprint 1: Shared Ingestion Foundation

**Status:** Not Started

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

- [ ] implement or refine canonical Pydantic models based on [data_models.md]
- [ ] create shared config/settings module for paths, providers, and defaults
- [ ] refactor price ingestion logic into `src/dumb_money/ingestion/prices.py`
- [ ] refactor fundamentals ingestion logic into `src/dumb_money/ingestion/fundamentals.py`
- [ ] create `src/dumb_money/ingestion/benchmarks.py`
- [ ] define raw file naming conventions and output path helpers
- [ ] add entry points in `scripts/` or `src/dumb_money/cli/`
- [ ] add fixtures for at least one ticker and one benchmark path
- [ ] add initial tests covering config resolution and ingestion contracts

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

## Sprint 2: Normalized Staging Layer

**Status:** Not Started

**Goal**

Transform raw provider outputs into canonical staging datasets that downstream analytics can trust.

**Planned Outputs**

- normalized price tables
- normalized fundamentals tables
- initial security master dataset
- benchmark set definitions
- transform tests for critical normalization paths

**Tasks**

- [ ] implement price normalization transforms into `data/staging/normalized_prices/`
- [ ] implement fundamentals normalization transforms into `data/staging/normalized_fundamentals/`
- [ ] create security master build logic in `src/dumb_money/transforms/`
- [ ] create benchmark set generation logic
- [ ] enforce canonical field names and type coercion rules
- [ ] handle missing `adj_close` fallback logic consistently
- [ ] add transform validation tests for representative raw inputs
- [ ] document staging output contracts in [data_models.md] if implementation expands them

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

## Sprint 3: Company Research MVP

**Status:** Not Started

**Goal**

Deliver the first complete company research workflow for a single ticker using the normalized foundation.

**Planned Outputs**

- reusable company analytics modules
- benchmark comparison logic
- company scorecard
- company research notebook or report packet
- standard charts and tables for one ticker

**Tasks**

- [ ] implement return calculations across standard windows
- [ ] implement volatility, drawdown, and relative strength metrics
- [ ] implement moving average and simple trend metrics
- [ ] build a fundamentals summary layer from normalized snapshots
- [ ] implement company versus benchmark comparison outputs
- [ ] define company scorecard structure and scoring rules
- [ ] create a notebook in `notebooks/02_company_research/` using shared modules only
- [ ] add tests for core analytics calculations

**Acceptance Criteria**

- one ticker can generate a consistent research packet end to end
- all calculations come from reusable modules, not notebook-only code
- benchmark comparison outputs match the company scorecard inputs
- core analytics functions are covered by tests using stable fixtures

**Exit Criteria**

- Milestone 3 is satisfied:
  one ticker can produce a standard research packet and benchmark comparisons work end to end

**Notes**

- Links:

## Sprint 4: Portfolio Fit MVP

**Status:** Not Started

**Goal**

Use the same shared data foundation to analyze a current portfolio and evaluate a new candidate security.

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
- current portfolio metrics can be generated from shared staging datasets
- candidate ticker fit uses explicit, inspectable rules rather than ad hoc notebook logic
- output tables are suitable for later report and UI reuse

**Exit Criteria**

- Milestone 4 is satisfied:
  a holdings file can be ingested and a current portfolio plus candidate ticker can be evaluated together

**Notes**

- Links:

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

**Tasks**

- [ ] finalize security master enrichment for sector and industry fields
- [ ] define sector ETF mapping logic and benchmark associations
- [ ] define peer group rules for company comparison
- [ ] implement peer-relative return comparison outputs
- [ ] implement peer-relative valuation comparison outputs
- [ ] build sector snapshot tables for reusable downstream consumption
- [ ] create notebook in `notebooks/03_sector_research/`
- [ ] add tests for peer grouping and sector mapping logic

**Acceptance Criteria**

- sector context can be attached to a researched company without manual notebook edits
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

## Sprint 7: App Layer MVP

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

- Milestone 5 is satisfied:
  local dashboard exposes company, sector, and portfolio workflows using shared modules

**Notes**

- Links:

## Sprint 8: LLM Summaries And Decision Support

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
