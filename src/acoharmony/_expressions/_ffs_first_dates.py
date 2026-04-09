# © 2025 HarmonyCares
# All rights reserved.

"""
FFS First Dates expression for intermediate transformations.
"""

from datetime import datetime

import polars as pl

from .._decor8 import expression
from ._registry import register_expression


@register_expression(
    "ffs_first_dates",
    schemas=["silver", "gold"],
    dataset_types=["claims"],
    callable=False,
    description="Calculate first FFS claim dates for beneficiaries",
)
class FfsFirstDatesExpression:
    """
    Expression for computing first FFS service dates per beneficiary.

        This expression joins CCLF5 claims data with provider list to identify
        the first service date for each beneficiary from valid providers.
    """

    @staticmethod
    @expression(name="ffs_first_aggregations", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_ffs_first_aggregations() -> list[pl.Expr]:
        """
        Build aggregation expressions for finding first FFS service per beneficiary.

        Returns:
            List of aggregation expressions for min date and claim count
        """
        return [
            pl.min("clm_line_from_dt").alias("ffs_first_date"),
            pl.len().alias("claim_count"),
        ]

    @staticmethod
    @expression(name="ffs_first_metadata", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_ffs_first_metadata_expr() -> list[pl.Expr]:
        """
        Build metadata expressions for FFS first dates.

        Returns:
            List of expressions adding metadata columns
        """
        return [
            pl.col("bene_mbi_id").alias("bene_mbi"),
            pl.lit(datetime.now()).alias("extracted_at"),
        ]

    @staticmethod
    @expression(name="ffs_first_select", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_ffs_first_select_columns() -> list[pl.Expr]:
        """
        Build column list for final select.

        Returns:
            List of Polars column expressions for final output
        """
        return [
            pl.col("bene_mbi"),
            pl.col("ffs_first_date"),
            pl.col("claim_count"),
            pl.col("extracted_at"),
        ]

