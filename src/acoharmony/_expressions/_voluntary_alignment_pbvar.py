# © 2025 HarmonyCares
# All rights reserved.

"""
PBVAR (Provider Beneficiary Voluntary Alignment Report) expression builders.

Pure expression builders for extracting PBVAR alignment data per beneficiary.
"""

import polars as pl


def build_pbvar_aggregation_exprs() -> list[pl.Expr]:
    """
    Build expressions to aggregate PBVAR alignment data per beneficiary.

    Aggregates:
    - Alignment status (currently aligned in PBVAR)
    - ACO ID from PBVAR
    - Response codes
    - Most recent file date

    Returns:
        list[pl.Expr]: List of aggregation expressions for PBVAR data
    """
    return [
        pl.lit(True).alias("pbvar_aligned"),  # If in PBVAR file, they're aligned
        pl.col("aco_id").sort_by("file_date", descending=True).first().alias("pbvar_aco_id"),
        pl.col("sva_response_code_list").sort_by("file_date", descending=True).first().alias("pbvar_response_codes"),
        pl.col("file_date").max().cast(pl.Date).alias("pbvar_file_date"),
    ]
