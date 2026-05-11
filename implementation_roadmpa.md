# Dumb Money Implementation Roadmap

## Purpose

This roadmap translates the goals in [tool_overview_analysis.md](./tool_overview_analysis.md) into a practical migration and build plan for the consolidated `dumb_money` repo.

The end-state product is a single modular investment research tool with three connected capabilities:

1. Company research
2. Sector and industry research
3. Decision support and portfolio fit

This roadmap assumes the current repo contains:

- `investment_analyzer copy/` as the main source of reusable ingestion and research groundwork
- `portfolio_analyzer copy/` as the main source of portfolio-oriented product goals and planning

---

## Product Goal

Build one research platform with a shared data foundation, shared analytics layer, and multiple user-facing views.

The platform should eventually support:

- repeatable ticker and benchmark ingestion
- normalized market and fundamentals datasets
- company-level scorecards and research outputs
- sector and peer context
- portfolio-fit analysis for current holdings and new candidates
- notebook, report, and dashboard outputs
- later, LLM-assisted summaries built on structured outputs

---

## Current State Summary

### What is already useful

The current repo has real implementation value in `investment_analyzer copy/`:

- working price ingestion logic
- working fundamentals ingestion and flattening patterns
- sample raw data for prices and fundamentals
- notebook-based prototypes for ingestion and benchmarking

The current repo has planning value in `portfolio_analyzer copy/`:

- portfolio analysis goals
- portfolio-fit product direction
- roadmap concepts for dashboard and portfolio workflows

### What is missing

The consolidated repo does not yet have:

- a unified package structure
- a shared config model
- normalized data models
- reusable analytics modules
- portfolio data ingestion
- sector/peer research modules
- tests
- a single clean dependency definition

### What should not be treated as production assets

- nested `.git/` directories from imported repos
- local `venv/` directories
- `.DS_Store`
- `.ipynb_checkpoints`
- incomplete scaffold files such as `investment_analyzer copy/scripts/ingest.py`

---

## Recommended Target Structure

```text
dumb_money/
├── README.md
├── pyproject.toml
├── .gitignore
├── .env.example
├── tool_overview_analysis.md
├── IMPLEMENTATION_ROADMAP.md
├── docs/
│   ├── architecture.md
│   ├── data_models.md
│   ├── workflows.md
│   └── research_templates/
├── data/
│   ├── raw/
│   │   ├── prices/
│   │   ├── fundamentals/
│   │   ├── benchmarks/
│   │   ├── portfolios/
│   │   ├── watchlists/
│   │   └── news/
│   ├── staging/
│   │   ├── normalized_prices/
│   │   ├── normalized_fundamentals/
│   │   ├── security_master/
│   │   └── benchmark_sets/
│   ├── marts/
│   │   ├── company_research/
│   │   ├── sector_research/
│   │   ├── portfolio_analytics/
│   │   └── scorecards/
│   └── external/
├── notebooks/
│   ├── 01_ingestion/
│   ├── 02_company_research/
│   ├── 03_sector_research/
│   ├── 04_portfolio_fit/
│   └── 99_prototypes/
├── src/
│   └── dumb_money/
│       ├── config/
│       ├── ingestion/
│       ├── models/
│       ├── transforms/
│       ├── analytics/
│       ├── research/
│       ├── outputs/
│       └── cli/
├── app/
│   └── streamlit/
├── tests/
│   ├── ingestion/
│   ├── transforms/
│   ├── analytics/
│   ├── research/
│   └── fixtures/
├── reports/
│   ├── generated/
│   └── templates/
└── scripts/
    ├── migrate_legacy_data.py
    ├── run_ingestion.py
    ├── build_research_views.py
    └── run_portfolio_analysis.py
```

---

## Design Principles

- Keep one shared market-data foundation for all downstream workflows.
- Move business logic out of notebooks into reusable modules.
- Separate raw ingestion from normalized transforms and research outputs.
- Prefer explainable metrics and tables before narrative generation.
- Keep the first version file-based and local-first.
- Add UI only after the data and analytics layers are stable.

---

## Recommended Tooling

### Core stack

- Python
- `pandas`
- `yfinance`
- `yahooquery`
- `pydantic`
- `pytest`
- `ruff`
- `jupyter`
- `matplotlib` or `plotly`
- `streamlit` for the first UI

### Optional later additions

- `duckdb` for local analytics and marts
- `sqlite` for lightweight persistence
- OpenAI APIs for narrative summaries after scorecards and tables are stable

### What to avoid early

- multiple runtime stacks unless needed
- premature database complexity
- heavy orchestration before repeatable local workflows exist
- dashboard work before analysis contracts are defined

---

## Migration Strategy

### Guiding rule

Treat the old repos as source material, not long-term subprojects.

### Preserve from `investment_analyzer copy/`

- `scripts/config.py` as a starting point for shared settings
- `scripts/ingest_prices.py` as the main source for ingestion refactoring
- useful logic patterns from the ingestion notebooks
- sample price and fundamentals data

### Preserve from `portfolio_analyzer copy/`

- portfolio-oriented goals and use cases from the README and project plan

### Archive or remove

- nested `.git/`
- `venv/`
- `.DS_Store`
- `.ipynb_checkpoints`
- empty scaffold directories from the imported portfolio repo
- incomplete duplicate scripts with no clear value

---

## Implementation Phases

## Phase 0: Repo Consolidation and Cleanup

### Goal

Turn the current repo into a single clean project with one dependency definition and one source tree.

### Tasks

- create the top-level directory structure
- create a single `.gitignore`
- add `pyproject.toml` or a clean top-level `requirements.txt`
- remove imported repo artifacts that should not live in the consolidated codebase
- move retained legacy files into transitional locations if needed

### Deliverables

- clean repo skeleton
- single environment setup flow
- migration notes for preserved legacy assets

### Effort

- 0.5 to 1.5 days

---

## Phase 1: Shared Data Foundation

### Goal

Create the core data contracts and ingestion modules used by every later workflow.

### Tasks

- define security, benchmark, holdings, and watchlist schemas
- refactor price ingestion into `src/dumb_money/ingestion/prices.py`
- refactor fundamentals ingestion into `src/dumb_money/ingestion/fundamentals.py`
- create benchmark ingestion module
- define file naming conventions and output paths
- add basic CLI or script entry points

### Deliverables

- reusable ingestion modules
- shared config module
- consistent raw data layout

### Effort

- 2 to 4 days

---

## Phase 2: Normalization and Data Modeling

### Goal

Convert raw downloads into clean datasets that downstream research can reliably use.

### Tasks

- normalize price schemas across data sources
- normalize flattened fundamentals snapshots
- create a security master dataset
- create benchmark grouping definitions
- write transforms for staging outputs
- add tests for critical transforms

### Deliverables

- normalized prices
- normalized fundamentals
- security master
- benchmark set definitions

### Effort

- 2 to 4 days

---

## Phase 3: Company Research MVP

### Goal

Deliver the first complete research workflow for a single company.

### Tasks

- compute trailing returns across standard windows
- compute volatility, drawdown, and relative strength
- compute moving averages and trend metrics
- summarize key fundamentals
- compare company performance versus benchmark
- build a first-pass company scorecard
- output tables and a small set of charts

### Deliverables

- company research notebook
- reusable analytics modules
- standard company research outputs

### Effort

- 3 to 5 days

---

## Phase 4: Sector and Peer Research MVP

### Goal

Add industry and sector context so company research is not done in isolation.

### Tasks

- map tickers to sector and industry metadata
- define sector ETF mapping logic
- build peer group definitions
- create peer-relative return and valuation comparison
- create sector snapshot outputs

### Deliverables

- sector context tables
- peer comparison outputs
- sector and peer research notebook

### Effort

- 2 to 4 days

---

## Phase 5: Portfolio Fit MVP

### Goal

Layer portfolio analytics on top of the same market and research foundation.

### Tasks

- define holdings input schema
- ingest or import current portfolio holdings
- calculate allocation, concentration, and exposure metrics
- compare portfolio performance to benchmarks
- evaluate candidate ticker fit versus current portfolio
- build watchlist and decision-support outputs

### Deliverables

- holdings import workflow
- portfolio metrics
- candidate fit analysis
- portfolio-fit notebook

### Effort

- 3 to 6 days

---

## Phase 6: Reporting and Output Standardization

### Goal

Turn notebook outputs into reusable deliverables.

### Tasks

- define standard table outputs
- define chart output helpers
- export Markdown or HTML reports
- create reusable report templates
- standardize scorecard formatting

### Deliverables

- exportable report format
- chart and table helper utilities
- generated research reports

### Effort

- 2 to 4 days

---

## Phase 7: App Layer

### Goal

Expose the research flows in a simple interactive UI.

### Tasks

- create initial Streamlit shell
- add company research page
- add sector research page
- add portfolio-fit page
- connect pages to prebuilt data products and analytics modules

### Deliverables

- usable local dashboard
- shared UI components

### Effort

- 3 to 5 days

---

## Phase 8: LLM Summaries and Decision Support

### Goal

Add narrative generation only after the structured layer is trustworthy.

### Tasks

- define summary inputs from scorecards and research tables
- generate structured prompts from analytics outputs
- produce narrative summaries for company, sector, and portfolio views
- validate summaries against source metrics

### Deliverables

- summary pipeline
- optional natural-language insight generation

### Effort

- 2 to 5 days

---

## Priority Order

Build in this order:

1. Repo cleanup and shared structure
2. Shared ingestion modules
3. Normalized data layer
4. Company research MVP
5. Portfolio-fit MVP
6. Sector and peer research
7. Report generation
8. Streamlit app
9. LLM summaries

This order gets a useful research tool working fastest while avoiding premature UI or AI work.

---

## Level of Effort Summary

### Small effort

- repo cleanup
- dependency consolidation
- file migration

### Medium effort

- shared ingestion refactor
- normalization layer
- company research analytics
- reporting utilities

### Larger effort

- portfolio-fit scoring
- sector and peer modeling
- app integration
- LLM summary layer

### Estimated total

For a solid MVP with company research, portfolio-fit basics, standardized outputs, and a lightweight local UI:

- about 2 to 4 focused weeks

For a more polished tool with strong sector context, richer scoring, and narrative summaries:

- about 4 to 8 weeks

---

## Milestones

### Milestone 1: Clean Consolidated Repo

Success criteria:

- one dependency file
- one package structure
- no nested repos or local env artifacts committed

### Milestone 2: Repeatable Market Data Pipeline

Success criteria:

- prices and fundamentals ingest from one command path
- outputs land in predictable raw and staging folders

### Milestone 3: Company Research Workflow

Success criteria:

- one ticker can produce a standard research packet
- benchmark comparisons and scorecard work end-to-end

### Milestone 4: Portfolio Fit Workflow

Success criteria:

- a holdings file can be ingested
- current portfolio and a candidate ticker can be evaluated together

### Milestone 5: Usable Research App

Success criteria:

- local dashboard exposes company, sector, and portfolio workflows using shared modules

---

## Immediate Next Actions

These should be the first concrete implementation tasks:

1. create the new repo skeleton and top-level package directories
2. add a top-level `.gitignore`
3. add a clean dependency definition
4. move `investment_analyzer copy/scripts/config.py` into the new config area as a starting reference
5. refactor `investment_analyzer copy/scripts/ingest_prices.py` into separate ingestion modules
6. move useful notebooks into `notebooks/99_prototypes/`
7. remove or archive nested `.git/`, `venv/`, `.DS_Store`, and checkpoint folders
8. define the first canonical data schemas for prices, fundamentals, benchmarks, and holdings

---

## Risks and Watchouts

- notebook logic may contain useful behavior that is easy to lose during refactor
- market-data APIs may return inconsistent schemas or incomplete fields
- portfolio-fit logic can sprawl if the data model is not defined early
- UI work can absorb too much time before analytics contracts stabilize
- LLM summaries will be low quality if scorecards and research outputs are not standardized first

---

## Definition of MVP

The MVP is complete when the repo can:

- ingest prices and fundamentals for a defined ticker set
- normalize and store those outputs consistently
- produce a company research output with benchmark comparisons
- ingest a simple holdings file
- evaluate a candidate security for portfolio fit
- generate repeatable notebook or report outputs from shared modules

At that point, the project has moved from exploratory repo consolidation into a real working research platform.
