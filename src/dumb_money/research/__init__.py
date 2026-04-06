"""Research output modules."""

from dumb_money.research.company import (
    CompanyResearchPacket,
    build_company_research_packet,
    build_company_scorecard_from_gold_artifacts,
    load_gold_ticker_metrics_mart,
    load_gold_scorecard_metric_rows,
    load_gold_scorecard_metric_rows_for_ticker,
    load_gold_ticker_metrics_row,
    load_peer_sets,
    load_sector_snapshots,
    load_security_master,
)

__all__ = [
    "CompanyResearchPacket",
    "build_company_research_packet",
    "build_company_scorecard_from_gold_artifacts",
    "load_gold_scorecard_metric_rows",
    "load_gold_scorecard_metric_rows_for_ticker",
    "load_gold_ticker_metrics_mart",
    "load_gold_ticker_metrics_row",
    "load_peer_sets",
    "load_sector_snapshots",
    "load_security_master",
]
