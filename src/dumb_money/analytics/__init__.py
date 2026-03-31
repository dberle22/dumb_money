"""Analytics modules."""

from dumb_money.analytics.company import (
    STANDARD_MOVING_AVERAGES,
    STANDARD_RETURN_WINDOWS,
    build_benchmark_comparison,
    build_drawdown_series,
    build_fundamentals_summary,
    build_indexed_price_series,
    build_moving_average_series,
    build_trailing_return_comparison,
    calculate_return_windows,
    calculate_risk_metrics,
    calculate_trend_metrics,
    prepare_price_history,
)
from dumb_money.analytics.scorecard import (
    DEFAULT_PRIMARY_BENCHMARK,
    DEFAULT_SECTOR_BENCHMARK_MAP,
    CompanyScorecard,
    build_company_scorecard,
    resolve_secondary_benchmark,
)

__all__ = [
    "CompanyScorecard",
    "DEFAULT_PRIMARY_BENCHMARK",
    "DEFAULT_SECTOR_BENCHMARK_MAP",
    "STANDARD_MOVING_AVERAGES",
    "STANDARD_RETURN_WINDOWS",
    "build_benchmark_comparison",
    "build_drawdown_series",
    "build_company_scorecard",
    "build_fundamentals_summary",
    "build_indexed_price_series",
    "build_moving_average_series",
    "build_trailing_return_comparison",
    "calculate_return_windows",
    "calculate_risk_metrics",
    "calculate_trend_metrics",
    "prepare_price_history",
    "resolve_secondary_benchmark",
]
