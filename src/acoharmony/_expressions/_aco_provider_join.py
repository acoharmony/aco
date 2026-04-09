# © 2025 HarmonyCares
# All rights reserved.

"""
Provider attribution join expression builders.

Pure expression builders for selecting provider attribution columns
to join with alignment data.
"""

import polars as pl


def build_provider_attribution_select_expr() -> list[pl.Expr]:
    """
    Build expressions to select provider attribution columns.

    Returns column expressions for:
    - current_mbi (join key)
    - MSSP provider info (TIN, NPI, name)
    - REACH provider info (TIN, NPI, name, attribution type)
    - Aligned provider info (TIN, NPI, org, practitioner name)
    - Latest ACO ID

    Returns:
        list[pl.Expr]: List of column expressions for provider attribution
    """
    return [
        pl.col("current_mbi"),
        pl.col("mssp_tin"),
        pl.col("mssp_npi"),
        pl.col("mssp_provider_name"),
        pl.col("reach_tin"),
        pl.col("reach_npi"),
        pl.col("reach_provider_name"),
        pl.col("reach_attribution_type"),
        pl.col("aligned_provider_tin"),
        pl.col("aligned_provider_npi"),
        pl.col("aligned_provider_org"),
        pl.col("aligned_practitioner_name"),
        pl.col("latest_aco_id"),
    ]


def build_null_provider_columns_expr() -> list[pl.Expr]:
    """
    Build expressions to create null provider columns when sources unavailable.

    Used as fallback when provider attribution cannot be calculated.

    Returns:
        list[pl.Expr]: Null column expressions for provider fields
    """
    return [
        pl.lit(None).alias("mssp_tin"),
        pl.lit(None).alias("mssp_npi"),
        pl.lit(None).alias("mssp_provider_name"),
        pl.lit(None).alias("reach_tin"),
        pl.lit(None).alias("reach_npi"),
        pl.lit(None).alias("reach_provider_name"),
        pl.lit(None).alias("reach_attribution_type"),
        pl.lit(None).alias("aligned_provider_tin"),
        pl.lit(None).alias("aligned_provider_npi"),
        pl.lit(None).alias("aligned_provider_org"),
        pl.lit(None).alias("aligned_practitioner_name"),
        pl.lit(None).alias("latest_aco_id"),
    ]
