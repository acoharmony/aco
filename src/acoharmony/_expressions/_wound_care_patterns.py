# © 2025 HarmonyCares
# All rights reserved.

"""
Wound care pattern detection expressions.

Implements reusable polars expressions for identifying concerning patterns in wound care claims:
- High frequency applications (15+ per patient per year)
- High cost patients (>$1M in costs)
- Clustered claims (3+ within 1 week)
- Same-day duplicates (multiple claims same date/product)
- Identical billing patterns (same code/amount across patients)

These expressions support fraud detection, utilization review, and quality assurance
in wound care claims analysis pipelines.
"""

import polars as pl

from .._decor8 import expression
from ._registry import register_expression


@register_expression(
    "wound_care_patterns",
    schemas=["gold"],
    dataset_types=["claim", "analysis"],
    callable=False,
    description="Pattern detection expressions for wound care claims analysis",
)
class WoundCarePatternExpression:
    """
    Expressions for detecting patterns in wound care claims.

    Provides polars expressions for:
    - High frequency application detection
    - High cost patient identification
    - Temporal clustering analysis
    - Duplicate claim detection
    - Billing pattern standardization analysis
    """

    @staticmethod
    @expression(
        name="high_frequency_filter",
        tier=["gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def high_frequency_filter(min_applications: int = 15) -> pl.Expr:
        """
        Create expression to filter for high frequency application patterns.

        Args:
            min_applications: Minimum number of applications to flag (default 15)

        Returns:
            Polars expression that evaluates to True for high frequency patterns

        Example:
            >>> df.filter(WoundCarePatternExpression.high_frequency_filter())
        """
        return pl.col("application_count") >= min_applications

    @staticmethod
    @expression(
        name="high_cost_filter",
        tier=["gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def high_cost_filter(min_cost: float = 1_000_000.0) -> pl.Expr:
        """
        Create expression to filter for high cost patients.

        Args:
            min_cost: Minimum total cost to flag (default $1,000,000)

        Returns:
            Polars expression that evaluates to True for high cost patients

        Example:
            >>> df.filter(WoundCarePatternExpression.high_cost_filter())
        """
        return pl.col("total_cost") >= min_cost

    @staticmethod
    @expression(
        name="cluster_filter",
        tier=["gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def cluster_filter(min_claims_in_week: int = 3) -> pl.Expr:
        """
        Create expression to filter for clustered claim patterns.

        Args:
            min_claims_in_week: Minimum claims within 7-day window (default 3)

        Returns:
            Polars expression that evaluates to True for clustered patterns

        Example:
            >>> df.filter(WoundCarePatternExpression.cluster_filter())
        """
        return pl.col("claims_in_week") >= min_claims_in_week

    @staticmethod
    @expression(
        name="duplicate_filter",
        tier=["gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def duplicate_filter() -> pl.Expr:
        """
        Create expression to filter for same-day duplicate claims.

        Returns:
            Polars expression that evaluates to True for duplicate patterns

        Example:
            >>> df.filter(WoundCarePatternExpression.duplicate_filter())
        """
        return pl.col("claim_count") > 1

    @staticmethod
    @expression(
        name="identical_pattern_filter",
        tier=["gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def identical_pattern_filter(
        min_identical_claims: int = 10, min_patients: int = 3
    ) -> pl.Expr:
        """
        Create expression to filter for identical billing patterns.

        Args:
            min_identical_claims: Minimum identical claims (default 10)
            min_patients: Minimum unique patients (default 3)

        Returns:
            Polars expression that evaluates to True for standardized billing

        Example:
            >>> df.filter(WoundCarePatternExpression.identical_pattern_filter())
        """
        return (pl.col("claim_count") >= min_identical_claims) & (
            pl.col("unique_patients") >= min_patients
        )

    @staticmethod
    @expression(
        name="calculate_time_span",
        tier=["gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def calculate_time_span(first_col: str, last_col: str) -> pl.Expr:
        """
        Create expression to calculate time span in days.

        Args:
            first_col: Column name for first date
            last_col: Column name for last date

        Returns:
            Polars expression computing day difference

        Example:
            >>> df.with_columns(
            ...     WoundCarePatternExpression.calculate_time_span(
            ...         "first_claim", "last_claim"
            ...     ).alias("span_days")
            ... )
        """
        return (pl.col(last_col) - pl.col(first_col)).dt.total_days()

    @staticmethod
    @expression(
        name="week_window_start",
        tier=["gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def week_window_start(date_col: str) -> pl.Expr:
        """
        Create expression for 7-day rolling window start date.

        Args:
            date_col: Column name for date to anchor window

        Returns:
            Polars expression for window start date

        Example:
            >>> df.with_columns(
            ...     WoundCarePatternExpression.week_window_start("claim_date")
            ...         .alias("window_start")
            ... )
        """
        return pl.col(date_col)

    @staticmethod
    @expression(
        name="week_window_end",
        tier=["gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def week_window_end(date_col: str) -> pl.Expr:
        """
        Create expression for 7-day rolling window end date.

        Args:
            date_col: Column name for date to anchor window

        Returns:
            Polars expression for window end date (date + 7 days)

        Example:
            >>> df.with_columns(
            ...     WoundCarePatternExpression.week_window_end("claim_date")
            ...         .alias("window_end")
            ... )
        """
        return pl.col(date_col) + pl.duration(days=7)
