# © 2025 HarmonyCares
# All rights reserved.

"""
ACO alignment metadata expression builders.

Pure expression builders for adding operational metadata and action flags
to consolidated alignment data.
"""

from datetime import datetime

import polars as pl


def build_data_completeness_expr() -> pl.Expr:
    """
    Build expression for data completeness status.

    Complete if has_demographics is true, otherwise partial.

    Returns:
        pl.Expr: Data completeness classification (COMPLETE, PARTIAL)
    """
    return (
        pl.when(pl.col("has_demographics"))
        .then(pl.lit("COMPLETE"))
        .otherwise(pl.lit("PARTIAL"))
        .alias("data_completeness")
    )


def build_lineage_transform_expr(version: str = "consolidated_alignment_v3") -> pl.Expr:
    """
    Build expression for lineage transform version.

    Args:
        version: Transform version string

    Returns:
        pl.Expr: Lineage transform version
    """
    return pl.lit(version).alias("lineage_transform")


def build_lineage_processed_at_expr() -> pl.Expr:
    """
    Build expression for lineage processed timestamp.

    Uses current timestamp for idempotency tracking.

    Returns:
        pl.Expr: ISO format timestamp
    """
    return pl.lit(datetime.now().isoformat()).alias("lineage_processed_at")


def build_data_date_exprs() -> list[pl.Expr]:
    """
    Build expressions for data date ranges.

    Aliases observable_start and observable_end to data_start_date and data_end_date.

    Returns:
        list[pl.Expr]: Data date range expressions
    """
    return [
        pl.col("observable_start").alias("data_start_date"),
        pl.col("observable_end").alias("data_end_date"),
    ]


def build_source_tables_expr(
    sources: str = "aco_alignment,voluntary_alignment,beneficiary_demographics",
) -> pl.Expr:
    """
    Build expression for source tables tracking.

    Args:
        sources: Comma-separated list of source tables

    Returns:
        pl.Expr: Source tables string
    """
    return pl.lit(sources).alias("source_tables")


def build_last_updated_expr() -> pl.Expr:
    """
    Build expression for last updated timestamp.

    Returns:
        pl.Expr: Current timestamp
    """
    return pl.lit(datetime.now()).alias("last_updated")


def build_has_opt_out_expr() -> pl.Expr:
    """
    Build expression for combined opt-out status.

    Has opt-out if: has_email_opt_out OR has_mail_opt_out

    Returns:
        pl.Expr: Boolean for any opt-out
    """
    return (
        pl.when(pl.col("has_email_opt_out") | pl.col("has_mail_opt_out"))
        .then(True)
        .otherwise(False)
        .alias("has_opt_out")
    )


def build_sva_action_needed_expr() -> pl.Expr:
    """
    Build expression for SVA action needed status.

    Logic:
    - RENEWAL_NEEDED: Has voluntary alignment but not valid (wrong provider or not REACH)
    - SVA_ELIGIBLE: No voluntary alignment but has MSSP history (eligible for conversion)
    - NO_ACTION: Either has valid SVA or not eligible

    Returns:
        pl.Expr: SVA action classification
    """
    return (
        pl.when(pl.col("has_voluntary_alignment") & ~pl.col("has_valid_voluntary_alignment"))
        .then(pl.lit("RENEWAL_NEEDED"))
        .when(~pl.col("has_voluntary_alignment") & pl.col("ever_mssp"))
        .then(pl.lit("SVA_ELIGIBLE"))
        .otherwise(pl.lit("NO_ACTION"))
        .alias("sva_action_needed")
    )


def build_outreach_priority_expr() -> pl.Expr:
    """
    Build expression for outreach priority level.

    Priority based on sva_action_needed:
    - RENEWAL_NEEDED → HIGH
    - SVA_ELIGIBLE → MEDIUM
    - NO_ACTION → LOW

    Returns:
        pl.Expr: Outreach priority classification
    """
    return (
        pl.when(pl.col("sva_action_needed") == "RENEWAL_NEEDED")
        .then(pl.lit("HIGH"))
        .when(pl.col("sva_action_needed") == "SVA_ELIGIBLE")
        .then(pl.lit("MEDIUM"))
        .otherwise(pl.lit("LOW"))
        .alias("outreach_priority")
    )
