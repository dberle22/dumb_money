# Growth and Profitability Section Implementation Prompt

Work in the `dumb_money` repo.

We want to implement the `Growth and Profitability` section from Sprint 6 in [project_plan.md](dumb_money/docs/project_plan.md) using the same section-by-section approach we used for `Market Performance`, `Research Summary`, `Trend and Risk Profile`, `Balance Sheet Strength`, `Valuation`, `Peer Positioning`, `Score Decomposition`, and `Final Research Summary`.

## Context

- Sprint 6 is structured so each report section is built, debugged, and reviewed independently.
- We want one shared file per section.
- DuckDB remains canonical for shared analytical tables.
- We want thin notebooks/review layers and shared reusable section logic in `src/dumb_money/...`.
- We already took this approach for:
  - [market_performance_section.py](dumb_money/src/dumb_money/outputs/market_performance_section.py)
  - [research_summary_section.py](dumb_money/src/dumb_money/outputs/research_summary_section.py)
  - [trend_risk_profile_section.py](dumb_money/src/dumb_money/outputs/trend_risk_profile_section.py)
  - [balance_sheet_strength_section.py](dumb_money/src/dumb_money/outputs/balance_sheet_strength_section.py)
  - [valuation_section.py](dumb_money/src/dumb_money/outputs/valuation_section.py)
  - [peer_positioning_section.py](dumb_money/src/dumb_money/outputs/peer_positioning_section.py)
  - [score_decomposition_section.py](dumb_money/src/dumb_money/outputs/score_decomposition_section.py)
  - [final_research_summary_section.py](dumb_money/src/dumb_money/outputs/final_research_summary_section.py)
- Use those section modules as the implementation pattern:
  explicit shared inputs, clear transformations, focused tests, saved review artifact, and inline comments that explain query, transform, and output-assembly steps.
- The likely target module path for this section should be:
  - [growth_profitability_section.py](dumb_money/src/dumb_money/outputs/growth_profitability_section.py)
- Unlike the already-completed Sprint 6 sections, this section depends much more directly on historical fundamentals coverage and period-aware time-series contracts.

## Please do the following

1. Review the Sprint 6 `Growth and Profitability` section plan in [project_plan.md](dumb_money/docs/project_plan.md).
2. Review the `Growth and Profitability` requirements in [stock_scorecard_visual_narrative_spec.md](dumb_money/docs/stock_scorecard_visual_narrative_spec.md).
3. Review the current historical fundamentals and shared reporting code, especially:
   - [company_report.py](dumb_money/src/dumb_money/outputs/company_report.py)
   - [research_summary_section.py](dumb_money/src/dumb_money/outputs/research_summary_section.py)
   - [balance_sheet_strength_section.py](dumb_money/src/dumb_money/outputs/balance_sheet_strength_section.py)
   - [score_decomposition_section.py](dumb_money/src/dumb_money/outputs/score_decomposition_section.py)
   - [research/company.py](dumb_money/src/dumb_money/research/company.py)
   - [analytics/company.py](dumb_money/src/dumb_money/analytics/company.py)
   - [analytics/scorecard.py](dumb_money/src/dumb_money/analytics/scorecard.py)
   - [storage/warehouse.py](dumb_money/src/dumb_money/storage/warehouse.py)
4. Confirm what canonical historical fundamentals contract already exists in DuckDB and shared loaders:
   - quarterly rows
   - annual rows
   - `TTM` rows
   - stable period identifiers and ordering fields
   - sufficient fields for revenue, EPS if available, operating margin, gross margin, free cash flow margin, and ROE or ROA / return-on-capital proxies
5. If the current shared contract is sufficient, implement a dedicated shared section module for `Growth and Profitability` at [growth_profitability_section.py](dumb_money/src/dumb_money/outputs/growth_profitability_section.py), similar in spirit to the earlier Sprint 6 section modules.
6. If the current shared contract is not yet sufficient, do the minimum enabling shared-model work needed first:
   - keep DuckDB canonical
   - add or refine shared loaders or transforms rather than notebook logic
   - avoid broad unrelated data-model redesign beyond what this section needs
7. Be explicit in code about:
   - what shared historical inputs are used
   - how periods are aligned and filtered
   - how growth rates, margin trends, and return-on-capital summaries are derived
   - how the section assembles reusable review outputs and interpretation text
8. Add inline comments in the new code where the logic is not obvious.
9. Build the section so it produces:
   - a standardized revenue and EPS growth trend output
   - a standardized margin trend output
   - a standardized return-on-capital summary table, panel, or equivalent output
   - a reusable short interpretation output
   - if appropriate, a single combined review figure for this section
10. Prefer deterministic, evidence-backed shared assembly first:
    derive the section from canonical historical fundamentals and stable shared outputs rather than handcrafted notebook prose.
11. Reuse the current shared scorecard and fundamentals contracts where possible, but do not force this section into point-in-time-only logic if a historical series contract is now available.
12. Make period choices explicit:
    if the section uses quarterly, annual, or `TTM` series, document and standardize that choice in code and tests.
13. Handle sparse-history cases gracefully:
    if a ticker lacks enough historical periods for a full trend view, the section should still produce a partial but standardized output with clear fallback behavior.
14. Save an AAPL review artifact for this section under `reports/templates/growth_profitability`, similar to:
   - `reports/templates/research_summary/...`
   - `reports/templates/trend_risk_profile/...`
   - `reports/templates/balance_sheet_strength/...`
   - `reports/templates/valuation/...`
   - `reports/templates/peer_positioning/...`
   - `reports/templates/score_decomposition/...`
   - `reports/templates/final_research_summary/...`
15. Add or update focused tests for the section.
16. Run the most relevant verification commands.
17. Summarize:
   - what historical inputs and contracts were already reusable
   - what enabling data-model work was needed, if any
   - what changed in the section implementation
   - what artifact was generated
   - any remaining gaps in the section

## Files likely relevant

- `docs/project_plan.md`
- `docs/stock_scorecard_visual_narrative_spec.md`
- `src/dumb_money/outputs/company_report.py`
- `src/dumb_money/research/company.py`
- `src/dumb_money/analytics/company.py`
- `src/dumb_money/analytics/scorecard.py`
- `src/dumb_money/storage/warehouse.py`
- `src/dumb_money/outputs/__init__.py`
- `tests/outputs/test_company_report.py`
- `tests/analytics/test_company.py`
- `tests/research/test_company_research.py`
- `tests/storage/test_warehouse.py`
- `reports/templates/`

## Important constraints

- Do not use notebook-only logic.
- Do not create ad hoc scripts.
- Keep shared logic in `src/dumb_money/...`.
- Follow the same section-by-section, single-file-per-section pattern as the other Sprint 6 sections.
- Keep DuckDB canonical where shared data access is needed.
- Prefer the current shared historical fundamentals contract first:
  canonical `normalized_fundamentals` and shared loaders first, then light section-specific formatting and interpretation.
- Keep score math, raw historical staging rules, and shared metric derivation in their existing shared modules where possible.
- Be careful about period mixing:
  do not silently blend quarterly, annual, and `TTM` rows into one trend view without making the contract explicit.
- If EPS history or true ROIC is not yet canonical, prefer a documented shared fallback such as available EPS proxy fields or ROE / ROA rather than inventing notebook-only substitutes.
- Do not revert unrelated user changes in the worktree.

## Definition of done

- `Growth and Profitability` exists as a dedicated shared section module at `src/dumb_money/outputs/growth_profitability_section.py`.
- The section is independently buildable and reviewable.
- A saved AAPL review artifact exists under `reports/templates/growth_profitability`.
- Focused tests cover the section.
- Any enabling historical-fundamentals contract changes needed for the section are implemented in shared code, not notebooks.
- The implementation is documented with concise inline comments where helpful.
