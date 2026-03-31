# Dumb Money

`dumb_money` is a consolidated investment research tool built around three connected workflows:

1. Company research
2. Sector and industry research
3. Portfolio fit and decision support

The project is being migrated from two earlier prototypes:

- `investment_analyzer`
- `portfolio_analyzer`

See [tool_overview_analysis.md](./tool_overview_analysis.md) for the product vision and [IMPLEMENTATION_ROADMAP.md](./IMPLEMENTATION_ROADMAP.md) for the implementation plan.

## Current Scope

The repo is currently focused on the foundation layer for the platform:

- shared project configuration
- canonical data models for securities, prices, fundamentals, benchmarks, and holdings
- raw ingestion modules for prices and fundamentals
- staging, marts, notebooks, app, and reports directories ready for build-out

## Project Structure

Key directories:

- `src/dumb_money/` for reusable application code
- `data/raw/` for provider extracts and imported input files
- `data/staging/` for normalized datasets
- `data/marts/` for downstream research-ready outputs
- `notebooks/` for workflow notebooks that should call shared modules
- `tests/` for model, ingestion, transform, and analytics tests
- `legacy/` for imported prototype material kept only as reference

## Environment Setup

This project declares `Python >=3.11` in [pyproject.toml](./pyproject.toml).

Recommended setup:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev,viz]"
```

If your machine only has `python3` and it is older than 3.11, install Python 3.12 or newer first, then recreate the virtual environment with that interpreter.

Optional market-data dependency:

```bash
python -m pip install -e ".[dev,viz,marketdata]"
```

That extra installs `yahooquery`. The default install path keeps `yfinance` as the required provider so the environment can be created cleanly even when `yahooquery` is unavailable or problematic on a given machine.

## Daily Workflow

Activate the environment:

```bash
source .venv/bin/activate
```

Install or refresh dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install -e ".[dev,viz]"
```

Run tests:

```bash
python -m pytest -q
```

Run linting:

```bash
python -m ruff check .
```

## Planning Docs

- [IMPLEMENTATION_ROADMAP.md](./IMPLEMENTATION_ROADMAP.md)
- [docs/project_plan.md](./docs/project_plan.md)
- [docs/data_models.md](./docs/data_models.md)
- [docs/architecture.md](./docs/architecture.md)
- [docs/workflows.md](./docs/workflows.md)

## Current Notes

- The active package code lives under `src/dumb_money/`
- Generated outputs under `reports/generated/`, `data/staging/`, and `data/marts/` are ignored by git
- Imported legacy material is intentionally isolated under `legacy/`
