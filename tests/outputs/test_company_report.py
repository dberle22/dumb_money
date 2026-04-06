from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

from dumb_money.outputs import (
    FULL_REPORT_SECTION_ORDER,
    build_balance_sheet_scorecard_table,
    build_company_overview_table,
    build_final_research_summary_text,
    build_full_company_report_bundle,
    build_full_company_report_index,
    build_peer_return_comparison_table,
    build_peer_valuation_table,
    build_research_summary_table,
    build_research_summary_text,
    build_risk_metric_table,
    build_sector_snapshot_table,
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
    save_full_company_report,
)
from dumb_money.research import build_company_research_packet


def test_company_report_tables_and_charts_build_from_aapl_packet() -> None:
    packet = build_company_research_packet("AAPL", benchmark_set_id="sample_universe")

    overview = build_company_overview_table(packet)
    research_summary = build_research_summary_table(packet)
    trailing_returns = build_trailing_return_comparison_table(packet)
    peer_returns = build_peer_return_comparison_table(packet)
    risk_table = build_risk_metric_table(packet)
    sector_snapshot = build_sector_snapshot_table(packet)
    balance_sheet = build_balance_sheet_scorecard_table(packet)
    valuation = build_valuation_summary_table(packet)
    peer_valuation = build_peer_valuation_table(packet)
    metrics = build_scorecard_metrics_table(packet)
    summary_text = build_research_summary_text(packet)
    final_text = build_final_research_summary_text(packet)

    assert overview["label"].tolist()[:3] == ["Ticker", "Company", "Sector"]
    assert research_summary["label"].tolist()[:2] == ["Company", "Ticker"]
    assert trailing_returns["Window"].tolist() == ["1m", "3m", "6m", "1y"]
    assert "Excess vs Company" in peer_returns.columns
    assert "Current Drawdown" in risk_table["label"].tolist()
    assert "Sector Benchmark" in sector_snapshot["label"].tolist()
    assert "Interpretation" in balance_sheet.columns
    assert "Interpretation" in valuation.columns
    assert "Peer Median" in valuation.columns
    assert "Role" in peer_valuation.columns
    assert "Metric" in metrics.columns
    assert "screens as" in summary_text.lower()
    assert packet.ticker in final_text
    assert packet.scorecard.summary["secondary_benchmark"] == "XLK"
    assert (
        packet.scorecard.metrics.set_index("metric_id").loc["return_vs_secondary_1y", "metric_available"]
        is True
    )

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


def test_full_company_report_bundle_and_artifacts_build_from_aapl(tmp_path) -> None:
    bundle = build_full_company_report_bundle("AAPL", benchmark_set_id="sample_universe")
    index = build_full_company_report_index(bundle)
    artifacts = save_full_company_report(
        "AAPL",
        benchmark_set_id="sample_universe",
        output_dir=Path(tmp_path) / "full_report",
    )

    assert bundle.ticker == "AAPL"
    assert index["section_title"].tolist() == [title for _, title in FULL_REPORT_SECTION_ORDER]
    assert artifacts["pdf_path"].exists()
    assert artifacts["index_path"].exists()
    assert artifacts["summary_path"].exists()
    assert (artifacts["sections_dir"] / "market_performance").exists()
    assert (artifacts["sections_dir"] / "final_research_summary").exists()

    summary_text = artifacts["summary_path"].read_text()
    assert "Section order:" in summary_text
    assert "Final memo:" in summary_text


def test_sector_research_notebook_exists_and_uses_shared_modules() -> None:
    notebook_path = Path("notebooks/03_sector_research/aapl_sector_peer_research.ipynb")
    notebook = json.loads(notebook_path.read_text())

    assert notebook["nbformat"] == 4
    cell_sources = ["".join(cell.get("source", [])) for cell in notebook["cells"]]
    joined = "\n".join(cell_sources)

    assert "## Research Summary" in joined
    assert "## Sector Snapshot" in joined
    assert "## Peer Return Context" in joined
    assert "## Peer Valuation Context" in joined
    assert "build_company_research_packet" in joined
    assert "build_sector_snapshot_table" in joined
    assert "build_peer_return_comparison_table" in joined
    assert "build_peer_valuation_table" in joined
