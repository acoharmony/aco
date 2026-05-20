# © 2025 HarmonyCares
# All rights reserved.

"""
Last FFS Service expression for intermediate transformations.

Mirrors ffs_first_dates but captures the MOST RECENT FFS service date
to support provider attribution logic (prioritizing most recent provider visit).
"""

from datetime import datetime

import polars as pl

from .._decor8 import expression
from ._registry import register_expression


@register_expression(
    "last_ffs_service",
    schemas=["silver", "gold"],
    dataset_types=["claims"],
    callable=False,
    description="Calculate last/most recent FFS claim dates and provider for beneficiaries",
)
class LastFfsServiceExpression:
    """
    Expression for computing most recent FFS service dates per beneficiary.

        This expression joins CCLF5 claims data with provider list to identify
        the LAST service date for each beneficiary from valid providers, along with
        the provider TIN/NPI combination from that most recent visit.

        Used for:
        - MSSP provider attribution (most recent provider visited)
        - Recency-based provider assignment
        - Provider transition tracking
    """

    @staticmethod
    @expression(name="last_ffs_aggregations", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_last_ffs_aggregations() -> list[pl.Expr]:
        """
        Build aggregation expressions for finding last FFS service per beneficiary.

        Returns:
            List of aggregation expressions for max date and claim count
        """
        return [
            pl.max("clm_line_from_dt").alias("last_ffs_date"),
            pl.len().alias("claim_count"),
        ]

    @staticmethod
    @expression(name="last_ffs_provider", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_last_ffs_provider_aggregations() -> list[pl.Expr]:
        """
        Build aggregation expressions for extracting provider info from last FFS claim.

        Returns:
            List of aggregation expressions for provider TIN/NPI
        """
        return [
            pl.first("last_ffs_date").alias("last_ffs_date"),
            pl.first("clm_rndrg_prvdr_tax_num").alias("last_ffs_tin"),
            pl.first("rndrg_prvdr_npi_num").alias("last_ffs_npi"),
            pl.first("claim_count").alias("claim_count"),
        ]

    @staticmethod
    @expression(name="last_ffs_metadata", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_last_ffs_metadata_expr() -> list[pl.Expr]:
        """
        Build metadata expressions for last FFS service.

        Returns:
            List of expressions adding metadata columns
        """
        return [
            pl.col("bene_mbi_id").alias("bene_mbi"),
            pl.lit(datetime.now()).alias("extracted_at"),
        ]

    @staticmethod
    @expression(name="last_ffs_select", tier=["silver", "gold"], idempotent=True, sql_enabled=True)
    def build_last_ffs_select_columns() -> list[pl.Expr]:
        """
        Build column list for final select.

        Returns:
            List of Polars column expressions for final output
        """
        return [
            pl.col("bene_mbi"),
            pl.col("last_ffs_date"),
            pl.col("last_ffs_tin"),
            pl.col("last_ffs_npi"),
            pl.col("claim_count"),
            pl.col("extracted_at"),
        ]

