# © 2025 HarmonyCares
# All rights reserved.

"""
FFS (Fee-for-Service) first dates data preparation expressions.

Provides Polars expressions for preparing FFS eligibility data. These expressions
handle MBI crosswalk normalization and column selection for FFS first claim dates.
"""

import polars as pl


def build_ffs_mbi_crosswalk_expr(mbi_map: dict) -> pl.Expr:
    """
    Build expression to normalize MBIs using crosswalk map.

    Args:
        mbi_map: Dictionary mapping previous_mbi -> current_mbi

    Returns:
        pl.Expr: Expression that maps bene_mbi to current_mbi
    """
    return (
        pl.col("bene_mbi")
        .map_elements(lambda x: mbi_map.get(x, x), return_dtype=pl.String)
        .alias("current_mbi")
    )


def build_ffs_select_expr() -> list[pl.Expr]:
    """
    Build select expressions for FFS data columns.

    Returns:
        list[pl.Expr]: Select expressions for FFS output columns
    """
    return [
        pl.col("current_mbi"),
        pl.lit(True).alias("has_ffs_service"),
        pl.col("ffs_first_date"),
        pl.col("claim_count").alias("ffs_claim_count"),
    ]
