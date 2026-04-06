# Score Decomposition Section Implementation Prompt

Work in the `dumb_money` repo.

We want to implement the `Score Decomposition` section from Sprint 6 in [project_plan.md](/Users/danberle/Documents/projects/dumb_money/docs/project_plan.md) using the same section-by-section approach we used for `Market Performance`, `Research Summary`, `Trend and Risk Profile`, `Balance Sheet Strength`, `Valuation`, and `Peer Positioning`.

## Context

- Sprint 6 is structured so each report section is built, debugged, and reviewed independently.
- We want one shared file per section.
- DuckDB remains canonical for shared analytical tables.
- We want thin notebooks/review layers and shared reusable section logic in `src/dumb_money/...`.
- We already took this approach for:
  - [market_performance_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/market_performance_section.py)
  - [research_summary_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/research_summary_section.py)
  - [trend_risk_profile_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/trend_risk_profile_section.py)
  - [balance_sheet_strength_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/balance_sheet_strength_section.py)
  - [valuation_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/valuation_section.py)
  - [peer_positioning_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/peer_positioning_section.py)
- Use those section modules as the implementation pattern:
  explicit shared inputs, clear transformations, focused tests, saved review artifact, and inline comments that explain query, transform, and visualization steps.
- The likely target module path for this section should be:
  - [score_decomposition_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/score_decomposition_section.py)

## Please do the following

1. Review the Sprint 6 `Score Decomposition` section plan in [project_plan.md](/Users/danberle/Documents/projects/dumb_money/docs/project_plan.md).
2. Review the `Score Decomposition` requirements in [stock_scorecard_visual_narrative_spec.md](/Users/danberle/Documents/projects/dumb_money/docs/stock_scorecard_visual_narrative_spec.md).
3. Review the current shared reporting / scorecard code, especially:
   - [company_report.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/company_report.py)
   - [research_summary_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/research_summary_section.py)
   - [valuation_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/valuation_section.py)
   - [research/company.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/research/company.py)
   - [analytics/scorecard.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/analytics/scorecard.py)
   - [analytics/company.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/analytics/company.py)
4. Implement a dedicated shared section module for `Score Decomposition` at [score_decomposition_section.py](/Users/danberle/Documents/projects/dumb_money/src/dumb_money/outputs/score_decomposition_section.py), similar in spirit to the earlier Sprint 6 section modules.
5. Make the section DuckDB-aware where relevant, but do not invent unnecessary new queries if the current canonical scorecard outputs, research packet, or staged shared tables are already the correct contract.
6. Be explicit in code about:
   - what shared inputs are used
   - how we transform them
   - how we assemble the score-decomposition outputs
7. Add inline comments in the new code where the logic is not obvious.
8. Build the section so it produces:
   - a standardized category contribution table
   - a standardized metric score table
   - a compact score decomposition strip, contribution chart, heatmap-style table, or equivalent summary visual
   - a reusable short interpretation output
   - if appropriate, a single combined review figure for this section
9. Preserve and surface the canonical scorecard transparency fields where helpful, especially:
   - `category`
   - `metric_id`
   - `metric_weight`
   - `metric_score`
   - `normalized_value`
   - `confidence_flag`
   - `coverage_ratio`
10. Reuse the current shared score contract first:
   category scores and metric-level score rows from the scorecard should remain canonical, with only light section-specific formatting and interpretation layered on top.
11. Save an AAPL review artifact for this section under `reports/templates/score_decomposition`, similar to:
   - `reports/templates/market_performance/...`
   - `reports/templates/research_summary/...`
   - `reports/templates/balance_sheet_strength/...`
   - `reports/templates/valuation/...`
   - `reports/templates/peer_positioning/...`
12. Add or update focused tests for the section.
13. Run the most relevant verification commands.
14. Summarize:
   - what was already reusable
   - what changed
   - what artifact was generated
   - any remaining gaps in the section

## Files likely relevant

- `docs/project_plan.md`
- `docs/stock_scorecard_visual_narrative_spec.md`
- `src/dumb_money/outputs/company_report.py`
- `src/dumb_money/outputs/research_summary_section.py`
- `src/dumb_money/outputs/valuation_section.py`
- `src/dumb_money/research/company.py`
- `src/dumb_money/analytics/scorecard.py`
- `src/dumb_money/analytics/company.py`
- `src/dumb_money/outputs/__init__.py`
- `tests/outputs/test_company_report.py`
- `tests/research/test_company_research.py`
- `reports/templates/`

## Important constraints

- Do not use notebook-only logic.
- Do not create ad hoc scripts.
- Keep shared logic in `src/dumb_money/...`.
- Follow the same section-by-section, single-file-per-section pattern as the other Sprint 6 sections.
- Keep DuckDB canonical where shared data access is needed.
- Reuse existing shared scorecard outputs and score math where possible instead of duplicating calculations.
- Prefer the current shared score contract first:
  canonical category scores and metric-level score rows first, then light section-specific formatting and interpretation.
- Keep score math in the shared scorecard module; this section should standardize presentation and review, not redefine scoring.
- Do not revert unrelated user changes in the worktree.

## Definition of done

- `Score Decomposition` exists as a dedicated shared section module at `src/dumb_money/outputs/score_decomposition_section.py`.
- The section is independently buildable and reviewable.
- A saved AAPL review artifact exists under `reports/templates/score_decomposition`.
- Focused tests cover the section.
- The implementation is documented with concise inline comments where helpful.
