# Gold-Layer Mart Refactor Execution Prompt

Work in the `dumb_money` repo.

We already have a first canonical `gold_ticker_metrics_mart` in shared code. We now need to execute the remaining Sprint 6 refactor that makes this mart a real pre-req for `Full Report Assembly` in [project_plan.md](/Users/danberle/Documents/projects/dumb_money/docs/project_plan.md).

## Goal

Refactor the Sprint 6 section loaders so they use the gold-layer mart where it clearly improves stability and reduces repeated ticker-level reconstruction, while preserving canonical source tables for time series, peer-row detail, and any score detail that should not yet be promoted into gold.

This is not a request for a speculative redesign. It is a targeted closeout refactor to make the final assembled report rely on simpler, more stable shared contracts.

## Current context

- A first gold-layer ticker mart already exists in shared code.
- The current mart and tests live in:
  - [src/dumb_money/transforms/ticker_metrics_mart.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/transforms/ticker_metrics_mart.py)
  - [src/dumb_money/storage/warehouse.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/storage/warehouse.py)
  - [src/dumb_money/research/company.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/research/company.py)
  - [tests/transforms/test_ticker_metrics_mart.py](/Users/danberle/Documents/projects/dumb_money/tests/transforms/test_ticker_metrics_mart.py)
- Initial mart-aware loader usage already exists in:
  - [src/dumb_money/outputs/market_performance_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/market_performance_section.py)
  - [src/dumb_money/outputs/growth_profitability_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/growth_profitability_section.py)
- Sprint 6 planning now treats the mart refactor as a pre-req for `Full Report Assembly`.

## What to do

1. Review the Sprint 6 `Gold-Layer Ticker Metrics Mart Refactor` and `Full Report Assembly` entries in [project_plan.md](/Users/danberle/Documents/projects/dumb_money/docs/project_plan.md).
2. Review the current mart contract and identify which section inputs are already covered well versus which section loaders still rebuild repeated ticker-level fields.
3. Preserve this boundary:
   - use the gold mart for stable ticker-level snapshot fields
   - keep chart-ready time series in canonical source tables
   - keep detailed peer row sets in canonical peer outputs unless there is a strong reason to change
   - do not force all score-detail rows into the current ticker mart
4. Execute the first migration wave if any meaningful work remains there:
   - `Research Summary`
   - `Market Performance`
   - `Growth and Profitability`
5. Execute the second migration wave where beneficial and low-risk:
   - `Trend and Risk Profile`
   - `Balance Sheet Strength`
   - `Valuation`
6. For each migrated section:
   - remove repeated ticker-level reconstruction where the mart already provides the stable field
   - avoid rewriting the section around mart data if the section still fundamentally needs canonical time series or peer-row detail
   - keep the public section output contract stable unless a change is clearly necessary
7. Review whether `Score Decomposition` and `Final Research Summary` should:
   - stay on the current scorecard path for now
   - read partially from the ticker mart
   - or justify a second gold artifact
8. If a second gold artifact is clearly justified, implement only a narrow practical version:
   one row per `ticker x score_date x metric_id` for reusable scorecard metric rows.
   Do not widen the current ticker mart further without a strong reason.
9. Review whether any broader historical rollups are now clearly worth promoting into gold because more than one section needs them repeatedly.
   Examples:
   - multi-period revenue or EPS growth summaries
   - rolling average or change-in-margin summaries
   - profitability consistency counts
   - simple valuation-band summaries
10. Keep DuckDB canonical and keep the mart refresh path in shared `src/dumb_money/...` code.
11. Add focused tests for:
   - each migrated section loader
   - any newly added mart fields
   - any second gold artifact if created
   - one assembled-flow regression if you reach the `Full Report Assembly` handoff point
12. Save refreshed AAPL inspection artifacts if useful under `reports/templates/...`
13. Run the most relevant verification commands.

## Files likely relevant

- `docs/project_plan.md`
- `src/dumb_money/storage/warehouse.py`
- `src/dumb_money/transforms/ticker_metrics_mart.py`
- `src/dumb_money/research/company.py`
- `src/dumb_money/outputs/market_performance_section.py`
- `src/dumb_money/outputs/research_summary_section.py`
- `src/dumb_money/outputs/trend_risk_profile_section.py`
- `src/dumb_money/outputs/balance_sheet_strength_section.py`
- `src/dumb_money/outputs/valuation_section.py`
- `src/dumb_money/outputs/score_decomposition_section.py`
- `src/dumb_money/outputs/final_research_summary_section.py`
- `src/dumb_money/outputs/growth_profitability_section.py`
- `tests/outputs/`
- `tests/transforms/test_ticker_metrics_mart.py`
- `tests/storage/test_warehouse.py`

## Constraints

- Do not use notebook-only logic.
- Do not create ad hoc scripts.
- Keep shared logic in `src/dumb_money/...`.
- Keep DuckDB canonical for mart storage and refresh.
- Prefer incremental loader refactors over a broad risky rewrite.
- Preserve existing canonical source tables.
- Do not collapse detailed time series into the ticker mart just because a section happens to use them.
- Only add a second gold artifact if the repeated score-detail rebuild is clearly real.
- Avoid duplicating score math or peer analytics in multiple places.
- Do not revert unrelated user changes in the worktree.

## Definition of done

- The Sprint 6 mart refactor is materially advanced or completed as a pre-req to `Full Report Assembly`.
- The targeted section loaders use the gold mart where it clearly reduces repeated ticker-level reconstruction.
- Time series and row-level detailed datasets remain in their canonical tables unless there is a clear justified exception.
- Any decision about a second gold artifact is explicit in code and tests if implemented, or explicit in the summary if deferred.
- Focused tests cover the migrated loader paths.
- Verification commands are run and summarized.

## Final summary format

Please summarize:

- which sections were migrated to the mart in this pass
- which fields now come from the mart versus canonical detailed tables
- whether a second gold artifact was added, deferred, or rejected for now
- whether any broader historical rollups were added to gold
- what still remains before `Full Report Assembly`
