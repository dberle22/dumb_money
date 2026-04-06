# Gold-Layer Ticker Metrics Mart Implementation Prompt

Work in the `dumb_money` repo.

We want to implement the `Gold-Layer Ticker Metrics Mart` from Sprint 6 in [project_plan.md](/Users/danberle/Documents/projects/dumb_money/docs/project_plan.md) before we move on to `Full Report Assembly`.

## Context

- Sprint 6 now has completed shared section modules for:
  - [market_performance_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/market_performance_section.py)
  - [research_summary_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/research_summary_section.py)
  - [trend_risk_profile_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/trend_risk_profile_section.py)
  - [balance_sheet_strength_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/balance_sheet_strength_section.py)
  - [valuation_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/valuation_section.py)
  - [peer_positioning_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/peer_positioning_section.py)
  - [score_decomposition_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/score_decomposition_section.py)
  - [final_research_summary_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/final_research_summary_section.py)
  - [growth_profitability_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/growth_profitability_section.py)
- Those section modules work, but several still assemble broader research packets or pull more shared inputs than they actually need.
- DuckDB remains canonical for shared analytical tables.
- The goal of this slice is to add one stable ticker-level mart in DuckDB that exposes reusable section-ready inputs so sections can gradually query precomputed fields instead of reconstructing them ad hoc.
- We should treat this as a Sprint 6 closeout refactor:
  improve section input contracts and simplify downstream loaders without reintroducing notebook-only logic.

## Please do the following

1. Review the `Gold-Layer Ticker Metrics Mart` plan in [project_plan.md](/Users/danberle/Documents/projects/dumb_money/docs/project_plan.md).
2. Review the current section modules and identify which fields they repeatedly reconstruct from broader shared inputs, especially:
   - [market_performance_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/market_performance_section.py)
   - [research_summary_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/research_summary_section.py)
   - [trend_risk_profile_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/trend_risk_profile_section.py)
   - [balance_sheet_strength_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/balance_sheet_strength_section.py)
   - [valuation_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/valuation_section.py)
   - [peer_positioning_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/peer_positioning_section.py)
   - [score_decomposition_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/score_decomposition_section.py)
   - [final_research_summary_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/final_research_summary_section.py)
   - [growth_profitability_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/growth_profitability_section.py)
3. Review the shared research and analytics modules that currently provide those section inputs, especially:
   - [research/company.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/research/company.py)
   - [analytics/company.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/analytics/company.py)
   - [analytics/scorecard.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/analytics/scorecard.py)
   - [storage/warehouse.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/storage/warehouse.py)
4. Design and implement one canonical DuckDB-backed gold-layer mart for ticker-level section inputs.
   Prefer a single table first, not a family of marts.
5. Be explicit about the mart contract in code:
   - one row per ticker as of a clearly defined scoring or reporting date
   - stable keys and update semantics
   - which upstream canonical tables feed the mart
   - which fields are intended for which sections
6. Keep DuckDB canonical:
   define the mart schema in shared storage code and implement a shared build / refresh path in `src/dumb_money/...`, not in notebooks.
7. Build the mart from existing canonical shared inputs where possible:
   - `normalized_prices`
   - `normalized_fundamentals`
   - `benchmark_mappings`
   - `benchmark_sets` if needed
   - `peer_sets`
   - `sector_snapshots`
   - shared scorecard outputs
8. Include fields that clearly reduce repeated work for Sprint 6 sections, such as:
   - company metadata and report date fields
   - benchmark identifiers actually used by report sections
   - precomputed trailing return / risk / trend snapshot fields
   - latest fundamentals summary fields used in scorecard and section outputs
   - peer summary stats used repeatedly by valuation and peer-positioning outputs
   - score summary fields and category scores
   - selected historical-fundamentals summary pointers or rollups needed by `Growth and Profitability`
9. Do not try to collapse every chart-ready time series into this first mart.
   Keep row-level time series in canonical source tables when appropriate.
   The mart should expose stable ticker-level section inputs first.
10. Refactor at least one or two section builders or shared loaders to read from the mart where it is clearly beneficial, but avoid a broad risky rewrite of every section in one pass.
11. Add concise inline comments where the mart logic or field choices are not obvious.
12. Add or update focused tests for:
   - mart schema and DuckDB write/read behavior
   - key field derivations
   - refresh or build path behavior
   - at least one regression showing a section or shared loader can consume the mart cleanly
13. If appropriate, save one inspection artifact for AAPL under a new review location such as `reports/templates/gold_layer_ticker_metrics_mart`.
   A CSV export is sufficient if no figure is warranted.
14. Run the most relevant verification commands.
15. Summarize:
   - which repeated section inputs were consolidated into the mart
   - what remained intentionally outside the mart
   - what shared modules changed
   - what DuckDB contract was added
   - what follow-up refactors still remain before full report assembly

## Files likely relevant

- `docs/project_plan.md`
- `src/dumb_money/storage/warehouse.py`
- `src/dumb_money/research/company.py`
- `src/dumb_money/analytics/company.py`
- `src/dumb_money/analytics/scorecard.py`
- `src/dumb_money/outputs/market_performance_section.py`
- `src/dumb_money/outputs/research_summary_section.py`
- `src/dumb_money/outputs/trend_risk_profile_section.py`
- `src/dumb_money/outputs/balance_sheet_strength_section.py`
- `src/dumb_money/outputs/valuation_section.py`
- `src/dumb_money/outputs/peer_positioning_section.py`
- `src/dumb_money/outputs/score_decomposition_section.py`
- `src/dumb_money/outputs/final_research_summary_section.py`
- `src/dumb_money/outputs/growth_profitability_section.py`
- `tests/storage/test_warehouse.py`
- `tests/research/test_company_research.py`
- `tests/outputs/`
- `reports/templates/`

## Important constraints

- Do not use notebook-only logic.
- Do not create ad hoc scripts.
- Keep shared logic in `src/dumb_money/...`.
- Keep DuckDB canonical for the mart and its refresh path.
- Prefer one practical mart with clear section value over a speculative architecture redesign.
- Preserve the current canonical source tables:
  the mart should sit on top of them, not replace them.
- Do not force long historical time series into one wide ticker row if that would make the contract brittle.
- Avoid duplicating score math or complex analytics in multiple places.
  Reuse current shared builders where possible, then persist the stable outputs they produce.
- Do not revert unrelated user changes in the worktree.

## Definition of done

- A canonical DuckDB-backed gold-layer ticker metrics mart exists in shared code.
- The mart has an explicit schema and a shared build / refresh path.
- Focused tests cover the mart contract and at least one downstream consumer path.
- At least one existing section builder or shared loader is simplified to use the mart where appropriate.
- An AAPL mart inspection artifact exists under `reports/templates/gold_layer_ticker_metrics_mart` if useful.
- The implementation clearly documents what the mart includes now and what remains for later refactors before `Full Report Assembly`.
