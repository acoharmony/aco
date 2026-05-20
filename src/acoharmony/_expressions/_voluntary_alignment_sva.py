# © 2025 HarmonyCares
# All rights reserved.

"""
SVA (Shared Voluntary Alignment) expression builders for voluntary_alignment consolidation.

Pure expression builders for aggregating SVA signature data per beneficiary.
"""

import polars as pl


def build_sva_aggregation_exprs() -> list[pl.Expr]:
    """
    Build expressions to aggregate SVA signature data per beneficiary.

    Aggregates:
    - Signature counts
    - First and most recent signature dates
    - Most recent provider information (NPI, TIN, name)
    - Days since last signature

    Returns:
        list[pl.Expr]: List of aggregation expressions for SVA data
    """
    return [
        pl.col("sva_signature_date").count().alias("sva_signature_count"),
        pl.col("sva_signature_date").min().alias("first_sva_date"),
        pl.col("sva_signature_date").max().alias("most_recent_sva_date"),
        pl.col("sva_npi").sort_by("sva_signature_date", descending=True).first().alias("sva_provider_npi"),
        pl.col("sva_tin").sort_by("sva_signature_date", descending=True).first().alias("sva_provider_tin"),
        pl.col("sva_provider_name").sort_by("sva_signature_date", descending=True).first().alias("sva_provider_name"),
        pl.col("sva_response_code").sort_by("sva_signature_date", descending=True).first().alias("latest_sva_response_code"),
    ]


def build_sva_derived_exprs() -> list[pl.Expr]:
    """
    Build expressions for SVA-derived metrics calculated after aggregation.

    Calculates:
    - Days since last SVA signature
    - SVA pending CMS status (based on response code)

    Returns:
        list[pl.Expr]: List of derived expressions
    """
    from datetime import date

    return [
        (pl.lit(date.today()) - pl.col("most_recent_sva_date")).dt.total_days().alias("days_since_last_sva"),
        (pl.col("latest_sva_response_code") == "00").fill_null(False).alias("sva_pending_cms"),
    ]
