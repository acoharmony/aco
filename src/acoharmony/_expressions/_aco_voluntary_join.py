# © 2025 HarmonyCares
# All rights reserved.

"""
Voluntary alignment join expression builders.

Pure expression builders for joining voluntary alignment data (SVA/PBVAR)
to the base alignment dataset. These are reusable building blocks that
create Polars expressions without inspecting data.
"""

import polars as pl


def build_voluntary_alignment_select_expr() -> list[pl.Expr]:
    """
    Build expressions to select and derive voluntary alignment columns.

    Returns column expressions for:
    - has_voluntary_alignment (derived from SVA count)
    - voluntary_alignment_type (SVA vs PBVAR)
    - voluntary_alignment_date (most recent SVA date)
    - Provider information (name, NPI, TIN)
    - SVA dates (first, last)
    - PBVAR response codes
    - Opt-out flags
    - FFS date

    Returns:
        list[pl.Expr]: List of column expressions for voluntary alignment
    """
    return [
        pl.col("current_mbi"),
        (pl.col("sva_signature_count") > 0).alias("has_voluntary_alignment"),
        pl.when(pl.col("sva_signature_count") > 0)
        .then(pl.lit("SVA"))
        .when(pl.col("pbvar_aligned"))
        .then(pl.lit("PBVAR"))
        .otherwise(None)
        .alias("voluntary_alignment_type"),
        pl.col("most_recent_sva_date").alias("voluntary_alignment_date"),
        pl.col("sva_provider_name").alias("voluntary_provider_name"),
        pl.col("sva_provider_npi").alias("voluntary_provider_npi"),
        pl.col("sva_provider_tin").alias("voluntary_provider_tin"),
        pl.col("sva_provider_valid").alias("sva_provider_valid"),
        pl.col("first_sva_date").alias("first_valid_signature_date"),
        pl.col("most_recent_sva_date").alias("last_valid_signature_date"),
        pl.col("first_sva_date").alias("first_sva_submission_date"),
        pl.col("most_recent_sva_date").alias("last_sva_submission_date"),
        pl.col("pbvar_response_codes").alias("latest_response_codes"),
        pl.col("signature_status").alias("latest_response_detail"),
        pl.col("pbvar_file_date").alias("pbvar_report_date"),
        pl.col("email_unsubscribed").alias("has_email_opt_out"),
        pl.col("email_complained").alias("has_mail_opt_out"),
        pl.col("ffs_first_date").alias("voluntary_ffs_date"),
    ]


def build_valid_voluntary_alignment_expr(
    provider_valid_col: str = "sva_provider_valid",
    current_program_col: str = "current_program",
) -> pl.Expr:
    """
    Build expression to calculate valid voluntary alignment status.

    SVA signatures are ONLY valid when:
    1. Provider is valid (Participant Provider, not Preferred)
    2. Currently enrolled in REACH (not MSSP)

    Args:
        provider_valid_col: Column name for provider validity flag
        current_program_col: Column name for current program

    Returns:
        pl.Expr: Boolean expression for valid voluntary alignment
    """
    return (
        pl.when(
            pl.col(provider_valid_col).fill_null(False) & (pl.col(current_program_col) == "REACH")
        )
        .then(True)
        .otherwise(False)
        .alias("has_valid_voluntary_alignment")
    )
