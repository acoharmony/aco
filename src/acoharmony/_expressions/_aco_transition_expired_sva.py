# © 2025 HarmonyCares
# All rights reserved.

"""
Year-over-year transition expression: Expired SVA signatures.

Identifies beneficiaries whose Shared Voluntary Alignment signatures
expired and were not renewed, causing loss of voluntary alignment.
"""

from datetime import date

import polars as pl


def build_expired_sva_expr(current_year: int, previous_year: int, current_year_months: list[int] | None = None) -> pl.Expr:
    """
    Calculate beneficiaries lost due to expired SVA signatures.

    An SVA signature expires after the performance year it covers. Logic:
    - Had voluntary alignment in previous year
    - Last valid signature date was for previous year or earlier
    - No valid signature for current year
    - Lost alignment in current year

    SVA signatures are valid for the performance year they're signed for,
    typically expiring on December 31 of that year.

    Args:
        current_year: Current year to compare (e.g., 2026)
        previous_year: Previous year to compare against (e.g., 2025)
        current_year_months: List of months available in current year (e.g., [1, 2, 3]). If None, assumes all 12 months.

    Returns:
        pl.Expr: Boolean expression indicating if beneficiary lost alignment due to expired SVA
    """
    # Previous year end date
    prev_year_end = date(previous_year, 12, 31)

    # Determine which months to check for current year
    if current_year_months is None:
        current_year_months = list(range(1, 13))

    # Check if had voluntary alignment in previous year
    had_voluntary_prev = pl.col("has_voluntary_alignment").fill_null(False)

    # Check if last valid signature is old (previous year or earlier)
    # last_valid_signature_date should exist from voluntary_alignment data
    signature_expired = (
        pl.col("last_valid_signature_date").is_not_null() &
        (pl.col("last_valid_signature_date") <= prev_year_end)
    )

    # Check alignment loss from previous to current year
    prev_year_prefix = f"ym_{previous_year}"
    curr_year_prefix = f"ym_{current_year}"

    # Was in REACH in December of previous year
    was_reach_dec_prev = pl.col(f"{prev_year_prefix}12_reach").fill_null(False)

    # Not in REACH in current year
    not_reach_curr = ~pl.any_horizontal([
        pl.col(f"{curr_year_prefix}{month:02d}_reach").fill_null(False)
        for month in current_year_months
    ])

    # All conditions must be true
    return (
        had_voluntary_prev &
        signature_expired &
        was_reach_dec_prev &
        not_reach_curr
    ).alias(f"expired_sva_{previous_year}")


def get_sva_expirations(
    sva_df: pl.LazyFrame,
    voluntary_df: pl.LazyFrame,
    previous_year: int,
    current_year: int
) -> pl.LazyFrame:
    """
    Get beneficiaries with expired SVA signatures for a specific year transition.

    Joins SVA and voluntary_alignment data to find beneficiaries whose
    signatures expired between the years.

    Args:
        sva_df: SVA source data LazyFrame
        voluntary_df: Voluntary alignment LazyFrame
        previous_year: Previous year
        current_year: Current year

    Returns:
        pl.LazyFrame: DataFrame with current_mbi and expiration details
    """
    prev_year_end = date(previous_year, 12, 31)
    curr_year_start = date(current_year, 1, 1)

    # Get all SVAs that were valid in previous year but expired before current year
    expired_svas = (
        sva_df
        .filter(
            # Signature date is in or before previous year
            (pl.col("signature_date") <= prev_year_end) &
            # And it's marked as not valid for current year (no renewal)
            (
                pl.col("most_recent_sva_date").is_null() |
                (pl.col("most_recent_sva_date") < curr_year_start)
            )
        )
        .select([
            pl.col("normalized_mbi").alias("current_mbi"),
            pl.col("signature_date").alias("sva_signature_date"),
            pl.col("most_recent_sva_date").alias("sva_expiration_date"),
            pl.lit("Expired SVA").alias("expiration_reason")
        ])
        .unique(subset=["current_mbi"])
    )

    return expired_svas
