# Peer Positioning Section Implementation Prompt

Work in the `dumb_money` repo.

We want to implement the `Peer Positioning` section from Sprint 6 in [project_plan.md](dumb_money/docs/project_plan.md) using the same section-by-section approach we used for `Market Performance`, `Research Summary`, `Trend and Risk Profile`, `Balance Sheet Strength`, and `Valuation`.

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
- Use those section modules as the implementation pattern:
  explicit shared inputs, clear transformations, focused tests, saved review artifact, and inline comments that explain query, transform, and visualization steps.
- The likely target module path for this section should be:
  - [peer_positioning_section.py](dumb_money/src/dumb_money/outputs/peer_positioning_section.py)

## Please do the following

1. Review the Sprint 6 `Peer Positioning` section plan in [project_plan.md](dumb_money/docs/project_plan.md).
2. Review the `Peer Positioning` requirements in [stock_scorecard_visual_narrative_spec.md](dumb_money/docs/stock_scorecard_visual_narrative_spec.md).
3. Review the current shared reporting / peer / scorecard code, especially:
   - [company_report.py](dumb_money/src/dumb_money/outputs/company_report.py)
   - [valuation_section.py](dumb_money/src/dumb_money/outputs/valuation_section.py)
   - [research_summary_section.py](dumb_money/src/dumb_money/outputs/research_summary_section.py)
   - [research/company.py](dumb_money/src/dumb_money/research/company.py)
   - [analytics/company.py](dumb_money/src/dumb_money/analytics/company.py)
   - [analytics/scorecard.py](dumb_money/src/dumb_money/analytics/scorecard.py)
   - [transforms/peer_sets.py](dumb_money/src/dumb_money/transforms/peer_sets.py)
4. Implement a dedicated shared section module for `Peer Positioning` at [peer_positioning_section.py](dumb_money/src/dumb_money/outputs/peer_positioning_section.py), similar in spirit to the earlier Sprint 6 section modules.
5. Make the section DuckDB-aware where relevant, but do not invent unnecessary new queries if the current canonical `peer_sets`, peer valuation comparison, peer return comparison, sector snapshot, research packet, or staged shared tables are already the correct contract.
6. Be explicit in code about:
   - what shared inputs are used
   - how we transform them
   - how we assemble the peer-positioning outputs
7. Add inline comments in the new code where the logic is not obvious.
8. Build the section so it produces:
   - a standardized peer return context table
   - a standardized peer valuation context table
   - a compact peer-positioning summary strip, ranking panel, or equivalent summary visual
   - a reusable short interpretation output
   - if appropriate, a single combined review figure for this section
9. Preserve and surface the canonical peer-set lineage fields where helpful, especially:
   - `peer_source`
   - `relationship_type`
   - `selection_method`
   - `peer_order`
10. Save an AAPL review artifact for this section under `reports/templates/peer_positioning`, similar to:
   - `reports/templates/market_performance/...`
   - `reports/templates/research_summary/...`
   - `reports/templates/balance_sheet_strength/...`
   - `reports/templates/valuation/...`
11. Add or update focused tests for the section.
12. Run the most relevant verification commands.
13. Summarize:
   - what was already reusable
   - what changed
   - what artifact was generated
   - any remaining gaps in the section

## Files likely relevant

- `docs/project_plan.md`
- `docs/stock_scorecard_visual_narrative_spec.md`
- `src/dumb_money/outputs/company_report.py`
- `src/dumb_money/outputs/valuation_section.py`
- `src/dumb_money/outputs/research_summary_section.py`
- `src/dumb_money/research/company.py`
- `src/dumb_money/analytics/company.py`
- `src/dumb_money/analytics/scorecard.py`
- `src/dumb_money/transforms/peer_sets.py`
- `src/dumb_money/outputs/__init__.py`
- `tests/outputs/test_company_report.py`
- `tests/analytics/test_company.py`
- `tests/research/test_company_research.py`
- `tests/transforms/test_peer_sets.py`
- `reports/templates/`

## Important constraints

- Do not use notebook-only logic.
- Do not create ad hoc scripts.
- Keep shared logic in `src/dumb_money/...`.
- Follow the same section-by-section, single-file-per-section pattern as the other Sprint 6 sections.
- Keep DuckDB canonical where shared data access is needed.
- Reuse existing shared peer-set, peer return, peer valuation, and scorecard logic where possible instead of duplicating calculations.
- Prefer the current shared peer contract first:
  canonical `peer_sets` plus peer return and peer valuation outputs first, then light section-specific formatting and interpretation.
- Preserve one canonical peer-set contract even when peer rows come from both `automatic` and `curated` sources.
- Do not revert unrelated user changes in the worktree.

## Definition of done

- `Peer Positioning` exists as a dedicated shared section module at `src/dumb_money/outputs/peer_positioning_section.py`.
- The section is independently buildable and reviewable.
- A saved AAPL review artifact exists under `reports/templates/peer_positioning`.
- Focused tests cover the section.
- The implementation is documented with concise inline comments where helpful.
