# Workflows

Repeatable command sequences for the most common tasks. All commands assume the `.venv` is active and you are in the repo root.

```bash
source .venv/bin/activate
```

---

## 1. Ingest a New Ticker

Run these in order for a ticker not yet in the warehouse.

```bash
# Fetch prices
dumb-money prices --tickers AAPL

# Fetch fundamentals (annual + quarterly + TTM)
dumb-money fundamentals --tickers AAPL

# Stage prices into DuckDB
dumb-money stage-prices

# Stage fundamentals into DuckDB
dumb-money stage-fundamentals

# Rebuild security master (picks up new ticker metadata)
dumb-money stage-security-master

# Rebuild benchmark mappings (assigns sector/industry ETFs to the new ticker)
dumb-money stage-benchmark-mappings

# Rebuild peer sets (includes new ticker in peer groups)
dumb-money stage-peer-sets

# Rebuild sector snapshots and gold marts
dumb-money stage-sector-snapshots
dumb-money stage-ticker-metrics-mart
dumb-money stage-scorecard-metric-rows
```

---

## 2. Ingest a Full Benchmark Universe

For a large basket like IWM or SPY, use the batch ingestion workflow.

```bash
# Plan the basket (creates a manifest of tickers to ingest)
dumb-money plan-basket --benchmark IWM

# Run the basket in batches (resumable if interrupted)
dumb-money ingest-basket --benchmark IWM

# Check status
dumb-money basket-status --benchmark IWM

# Validate coverage after ingestion
dumb-money basket-validate --benchmark IWM
```

See [universe_ingestion_checklist.md](universe_ingestion_checklist.md) for the full step-by-step checklist.

---

## 3. Generate a Company Research Report

Requires the ticker to already be ingested (see Workflow 1).

```bash
# From the CLI — generates a PDF and saves artifacts
dumb-money report --ticker AAPL --benchmark-set sample_universe
```

Or from Python:

```python
from dumb_money.research import build_company_research_packet
from dumb_money.outputs import save_full_company_report

packet = build_company_research_packet("AAPL", benchmark_set_id="sample_universe")
save_full_company_report(packet)
```

Output lands in `reports/generated/`.

---

## 4. Run the Notebook Research Flow

Open the company research notebook and run all cells:

```bash
jupyter lab notebooks/02_company_research/aapl_company_research.ipynb
```

Change the `TICKER` variable at the top of the notebook to switch companies. The notebook calls shared modules — no research logic lives in the cells themselves.

---

## 5. Add a Curated Peer Set Override

Edit `data/raw/peer_overrides.csv` (or the equivalent override file) to add a curated peer list, then re-stage:

```bash
dumb-money stage-peer-sets
dumb-money stage-ticker-metrics-mart
```

---

## 6. Refresh All Gold Marts

Run after any ingestion update to propagate changes to downstream report tables.

```bash
dumb-money stage-sector-snapshots
dumb-money stage-ticker-metrics-mart
dumb-money stage-scorecard-metric-rows
```

---

## 7. Run Tests and Linting

```bash
python -m pytest -q
python -m ruff check .
```

---

## Upcoming: Three-Lens Evaluation (Sprint 10–12)

Once forward estimates and short interest ingestion are added, the evaluation workflow will extend to:

```bash
# Ingest forward estimates from FMP
dumb-money estimates --tickers AAPL

# Ingest short interest from FINRA
dumb-money short-interest --tickers AAPL

# Generate a decision brief (three-lens evaluation + DCF + portfolio overlay)
dumb-money brief --ticker AAPL
```
