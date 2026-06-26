# Architecture

## Overview

`dumb_money` is a local-first investment research platform built in Python. It follows a layered pipeline: raw data in, normalized tables out, analytics on top, reports at the end. Every layer is independently callable — notebooks and reports consume the same shared modules.

---

## Module Map

```
src/dumb_money/
├── config/          AppSettings — all paths, defaults, and config constants
├── models/          Pydantic data contracts (Security, PriceBar, FundamentalSnapshot, Holding)
├── ingestion/       Raw data fetchers (prices, fundamentals, benchmarks, baskets, universe)
├── transforms/      Raw → canonical DuckDB tables (normalize, deduplicate, stage)
├── storage/         DuckDB read/write helpers and canonical table definitions
├── analytics/       Pure computation on canonical tables (returns, risk, peers, scorecard)
├── research/        Packet assembly — loads canonical tables, runs analytics, returns structured output
├── outputs/         Report section builders — tables, charts, PDF assembly
└── cli/             CLI entry points for ingestion, staging, and report commands
```

---

## Data Flow

```
Provider APIs (yfinance, yahooquery, FMP, FINRA)
        │
        ▼
data/raw/                         Raw provider payloads — never modified after write
        │
        ▼
ingestion/ + transforms/          Normalize, coerce types, deduplicate, upsert
        │
        ▼
data/warehouse/dumb_money.duckdb  Canonical analytical store
  ├── normalized_prices
  ├── normalized_fundamentals
  ├── security_master
  ├── benchmark_definitions
  ├── benchmark_mappings
  ├── benchmark_memberships
  ├── peer_sets
  ├── sector_snapshots
  ├── gold_ticker_metrics_mart
  └── gold_scorecard_metric_rows
        │
        ▼
analytics/                        Calculations on canonical tables (no raw I/O)
  ├── company.py    return windows, risk metrics, trend metrics, peer comparisons
  └── scorecard.py  metric scoring, category scores, confidence flags
        │
        ▼
research/company.py               CompanyResearchPacket — assembles all analytics for one ticker
        │
        ▼
outputs/                          Report sections — each section is one file, independently buildable
  └── company_report.py           Full report assembly
        │
        ▼
reports/generated/                PDF and artifact output
```

---

## Storage Strategy

- **Raw layer** (`data/raw/`): provider payloads and source files. Never overwritten; new pulls add new files.
- **Warehouse** (`data/warehouse/dumb_money.duckdb`): the canonical store for all normalized and derived tables. Transforms upsert into DuckDB; optional CSV exports exist under `data/staging/` and `data/marts/` for inspection.
- **Gold layer**: denormalized, analysis-ready marts (`gold_ticker_metrics_mart`, `gold_scorecard_metric_rows`) used by report sections to avoid rebuilding joins on every render.

---

## Key Design Rules

- **One source of truth per table.** Canonical tables live in DuckDB. CSVs are exports, not the record of truth.
- **Analytics touch only canonical tables.** `analytics/` modules read from DuckDB through shared loaders. They do not call provider APIs or read raw files.
- **Reports touch only analytics outputs.** `outputs/` modules consume `CompanyResearchPacket` and canonical mart tables. No raw I/O, no transforms.
- **Notebooks are thin.** Notebooks call shared modules — they do not define logic inline.
- **No composite scores across lenses.** The existing scorecard (four-category weighted composite) is a standalone product. The three-lens evaluation framework (Value/Growth/Momentum) produces independent verdicts, not a combined score. See [../framework.md](../framework.md).

---

## Upcoming: Three-Lens Framework (Sprint 10–12)

Three new analytics modules will be added alongside the existing scorecard:

```
analytics/
├── value_lens.py    DCF, EV/EBITDA vs peers, P/FCF, margin of safety, value trap check
├── growth_lens.py   Revenue CAGR, PEG, earnings revisions, margin trends, implied growth back-solve
└── momentum_lens.py Relative strength vs ETFs, MA signals, short interest, volume trend
```

A shared `LensVerdict` dataclass will carry each lens output into a narrative synthesis layer (`analytics/synthesis.py`). The synthesis feeds the decision brief report (`outputs/decision_brief.py`).

New ingestion modules will also be added:
- `ingestion/estimates.py` — FMP forward estimates and earnings revision counts
- `ingestion/short_interest.py` — FINRA twice-monthly short interest files
