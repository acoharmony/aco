"""
Claim ID matching expressions for cross-source validation.

This module provides expressions for comparing claim IDs across different data sources
(HDAI, CCLF, Excel submissions) to identify data quality issues and reconcile claims.

Key Features:
- Claim ID presence detection across data sources
- Categorical flags for missing claim IDs by source
- Idempotent matching logic

Use Cases:
1. Validate wound care claims submitted via Excel against HDAI and CCLF
2. Identify claims present in one source but missing in others
3. Generate reconciliation reports for claim submission validation
"""

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


@register_expression(
    "claim_id_match",
    schemas=["silver", "gold"],
    dataset_types=["claims", "medical_claim", "validation"],
    callable=False,
    description="Claim ID matching expressions for cross-source validation",
)
class ClaimIdMatchExpression:
    """
    Build expressions for claim ID matching across data sources.

    Creates categorical flags indicating whether a claim ID is:
    - Present in both sources
    - Missing in HDAI
    - Missing in CCLF
    """

    @staticmethod
    @expression(
        name="claim_id_match_flag",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def claim_id_match_flag(
        hdai_claim_id_col: str = "hdai_claim_id",
        cclf_claim_id_col: str = "cclf_claim_id",
    ) -> pl.Expr:
        """
        Create categorical flag for claim ID matching status.

        Args:
            hdai_claim_id_col: Column name containing HDAI claim ID
            cclf_claim_id_col: Column name containing CCLF claim ID

        Returns:
            pl.Expr: Categorical expression with values:
                - "yes": Claim ID exists in both sources
                - "missing_hdai": Claim ID missing in HDAI (only in CCLF)
                - "missing_cclf": Claim ID missing in CCLF (only in HDAI)

        Notes:
            - Both columns null is not handled (would indicate invalid data)
            - Comparison is case-sensitive
            - Null-safe matching
        """
        return (
            pl.when(
                pl.col(hdai_claim_id_col).is_not_null()
                & pl.col(cclf_claim_id_col).is_not_null()
            )
            .then(pl.lit("yes"))
            .when(pl.col(hdai_claim_id_col).is_null())
            .then(pl.lit("missing_hdai"))
            .when(pl.col(cclf_claim_id_col).is_null())
            .then(pl.lit("missing_cclf"))
            .otherwise(pl.lit(None))
            .cast(pl.Categorical)
        )

    @staticmethod
    @expression(
        name="has_claim_id_in_source",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def has_claim_id_in_source(claim_id_col: str) -> pl.Expr:
        """
        Check if claim ID exists in a specific source.

        Args:
            claim_id_col: Column name to check for presence

        Returns:
            pl.Expr: Boolean expression that is True when claim ID is present

        """
        return pl.col(claim_id_col).is_not_null()

    @staticmethod
    @expression(
        name="claim_ids_match",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def claim_ids_match(
        source1_col: str = "hdai_claim_id", source2_col: str = "cclf_claim_id"
    ) -> pl.Expr:
        """
        Check if claim IDs match exactly between two sources.

        Args:
            source1_col: First source claim ID column
            source2_col: Second source claim ID column

        Returns:
            pl.Expr: Boolean expression that is True when both claim IDs exist and match

        Notes:
            - Returns False if either column is null
            - Case-sensitive comparison
        """
        return (
            pl.col(source1_col).is_not_null()
            & pl.col(source2_col).is_not_null()
            & (pl.col(source1_col) == pl.col(source2_col))
        )
