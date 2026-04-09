# © 2025 HarmonyCares
# All rights reserved.

"""
Vintage cohort expressions for notebook calculations.

Provides reusable Polars expressions for calculating vintage cohort
percentage metrics and distributions.
"""

import polars as pl


def build_vintage_distribution_derived_metrics(total_enrolled: int) -> list[pl.Expr]:
    """
    Build derived metric expressions for vintage distribution statistics.

    These metrics reference columns created during aggregation, so they must
    be applied in a separate .with_columns() call after .agg().

    Args:
        total_enrolled: Total number of enrolled beneficiaries (for percentage calculations)

    Returns:
        list[pl.Expr]: List of derived metric expressions
    """
    if total_enrolled > 0:
        return [
            (pl.col("count") / total_enrolled * 100).alias("pct_of_enrolled"),
            (pl.col("current_reach") / pl.col("count") * 100).fill_nan(0.0).alias("pct_in_reach"),
            (pl.col("current_mssp") / pl.col("count") * 100).fill_nan(0.0).alias("pct_in_mssp"),
            (pl.col("transitions") / pl.col("count") * 100).fill_nan(0.0).alias("pct_with_transitions"),
        ]
    else:
        return [
            pl.lit(0.0).alias("pct_of_enrolled"),
            pl.lit(0.0).alias("pct_in_reach"),
            pl.lit(0.0).alias("pct_in_mssp"),
            pl.lit(0.0).alias("pct_with_transitions"),
        ]


def build_office_vintage_distribution_derived_metrics() -> list[pl.Expr]:
    """
    Build derived metric expressions for office-level vintage distribution statistics.

    These metrics reference columns created during aggregation and join operations,
    so they must be applied in a separate .with_columns() call.

    Returns:
        list[pl.Expr]: List of derived metric expressions
    """
    return [
        (pl.col("count") / pl.col("office_total_enrolled") * 100).fill_null(0.0).alias("pct_of_office_enrolled"),
        (pl.col("currently_enrolled") / pl.col("count") * 100).fill_null(0.0).alias("pct_currently_enrolled"),
        (pl.col("current_reach") / pl.col("count") * 100).fill_null(0.0).alias("pct_in_reach"),
        (pl.col("current_mssp") / pl.col("count") * 100).fill_null(0.0).alias("pct_in_mssp"),
        (pl.col("transitions") / pl.col("count") * 100).fill_null(0.0).alias("pct_with_transitions"),
    ]
