# © 2025 HarmonyCares
# All rights reserved.

"""
Summary statistics expressions for temporal alignment matrix.

Provides Polars expressions for deriving aggregate metrics from the temporal matrix
including FFS service indicators, demographic availability, and MBI stability.
"""

import polars as pl


def build_has_ffs_service_expr() -> pl.Expr:
    """
    Build expression for FFS service indicator.

    Returns:
        pl.Expr: Expression that aliases ever_ffs to has_ffs_service
    """
    return pl.col("ever_ffs").alias("has_ffs_service")


def build_ffs_claim_count_proxy_expr() -> pl.Expr:
    """
    Build expression for FFS claim count proxy using months in FFS.

    Returns:
        pl.Expr: Expression that aliases months_in_ffs to ffs_claim_count
    """
    return pl.col("months_in_ffs").alias("ffs_claim_count")


def build_has_demographics_expr() -> pl.Expr:
    """
    Build expression for demographics availability indicator.

    Returns:
        pl.Expr: Expression checking if birth_date is not null
    """
    return (
        pl.when(pl.col("birth_date").is_not_null())
        .then(True)
        .otherwise(False)
        .alias("has_demographics")
    )


def build_mbi_stability_expr() -> pl.Expr:
    """
    Build expression for MBI stability classification.

    Returns:
        pl.Expr: Expression classifying MBI changes (Multiple/Changed/Stable)
    """
    return (
        pl.when(pl.col("previous_mbi_count") > 1)
        .then(pl.lit("Multiple"))
        .when(pl.col("previous_mbi_count") == 1)
        .then(pl.lit("Changed"))
        .otherwise(pl.lit("Stable"))
        .alias("mbi_stability")
    )


def build_current_provider_tin_expr() -> pl.Expr:
    """
    Build expression for current provider TIN placeholder.

    Returns:
        pl.Expr: Expression for null provider TIN
    """
    return pl.lit(None).cast(pl.String).alias("current_provider_tin")


def build_ffs_first_date_expr() -> pl.Expr:
    """
    Build expression for FFS first date placeholder.

    Returns:
        pl.Expr: Expression for null FFS first date
    """
    return pl.lit(None).cast(pl.Date).alias("ffs_first_date")


def build_summary_statistics_exprs() -> list[pl.Expr]:
    """
    Build all summary statistics expressions.

    Returns:
        list[pl.Expr]: All expressions for summary statistics
    """
    return [
        build_has_ffs_service_expr(),
        build_ffs_claim_count_proxy_expr(),
        build_has_demographics_expr(),
        build_mbi_stability_expr(),
        build_current_provider_tin_expr(),
        build_ffs_first_date_expr(),
    ]
