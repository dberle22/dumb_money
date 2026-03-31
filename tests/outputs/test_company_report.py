from __future__ import annotations

import json
from pathlib import Path

from dumb_money.outputs import (
    build_balance_sheet_scorecard_table,
    build_company_overview_table,
    build_final_research_summary_text,
    build_research_summary_table,
    build_research_summary_text,
    build_risk_metric_table,
    build_scorecard_metrics_table,
    build_trailing_return_comparison_table,
    build_valuation_summary_table,
    close_figure,
    render_benchmark_excess_return_chart,
    render_drawdown_chart,
    render_indexed_price_performance_chart,
    render_price_with_moving_averages_chart,
    render_score_decomposition_chart,
    render_score_summary_strip,
    render_scorecard_category_chart,
    render_scorecard_metric_chart,
    render_trailing_return_comparison_chart,
)
from dumb_money.research import build_company_research_packet


def test_company_report_tables_and_charts_build_from_aapl_packet() -> None:
    packet = build_company_research_packet("AAPL", benchmark_set_id="sample_universe")

    overview = build_company_overview_table(packet)
    research_summary = build_research_summary_table(packet)
    trailing_returns = build_trailing_return_comparison_table(packet)
    risk_table = build_risk_metric_table(packet)
    balance_sheet = build_balance_sheet_scorecard_table(packet)
    valuation = build_valuation_summary_table(packet)
    metrics = build_scorecard_metrics_table(packet)
    summary_text = build_research_summary_text(packet)
    final_text = build_final_research_summary_text(packet)

    assert overview["label"].tolist()[:3] == ["Ticker", "Company", "Sector"]
    assert research_summary["label"].tolist()[:2] == ["Company", "Ticker"]
    assert trailing_returns["Window"].tolist() == ["1m", "3m", "6m", "1y"]
    assert "Current Drawdown" in risk_table["label"].tolist()
    assert "Interpretation" in balance_sheet.columns
    assert "Interpretation" in valuation.columns
    assert "Metric" in metrics.columns
    assert "quality-led profile" in summary_text.lower()
    assert packet.ticker in final_text

    for figure in [
        render_score_summary_strip(packet),
        render_scorecard_category_chart(packet),
        render_indexed_price_performance_chart(packet),
        render_trailing_return_comparison_chart(packet),
        render_benchmark_excess_return_chart(packet),
        render_drawdown_chart(packet),
        render_price_with_moving_averages_chart(packet),
        render_scorecard_metric_chart(packet),
        render_score_decomposition_chart(packet),
    ]:
        assert len(figure.axes) >= 1
        close_figure(figure)


def test_company_research_notebook_exists_and_uses_shared_modules() -> None:
    notebook_path = Path("notebooks/02_company_research/aapl_company_research.ipynb")
    notebook = json.loads(notebook_path.read_text())

    assert notebook["nbformat"] == 4
    cell_sources = ["".join(cell.get("source", [])) for cell in notebook["cells"]]
    joined = "\n".join(cell_sources)

    assert "## Research Summary" in joined
    assert "## Market Performance" in joined
    assert "## Trend and Risk Profile" in joined
    assert "## Balance Sheet Strength" in joined
    assert "## Valuation" in joined
    assert "## Score Decomposition" in joined
    assert "build_company_research_packet" in joined
    assert "build_research_summary_table" in joined
    assert "render_indexed_price_performance_chart" in joined
    assert "render_drawdown_chart" in joined
    assert "render_score_decomposition_chart" in joined
