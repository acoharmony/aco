# © 2025 HarmonyCares
# All rights reserved.

"""
Year-over-year transition expression: Termed on previous year BAR.

Identifies beneficiaries who were terminated on the previous year's BAR file
by checking actual termination dates from BAR source data.
"""

from datetime import date

import polars as pl


def build_termed_bar_expr(current_year: int, previous_year: int, current_year_months: list[int] | None = None) -> pl.Expr:
    """
    Calculate beneficiaries termed on previous year BAR.

    Uses the bene_date_of_term column to check if beneficiary was terminated
    during the previous year on REACH BAR files.

    Logic:
    - Was in REACH in previous year (any ym_{prev_year}*_reach = True)
    - Has termination date in previous year range
    - Not in REACH in current year

    Args:
        current_year: Current year to compare (e.g., 2026)
        previous_year: Previous year to compare against (e.g., 2025)
        current_year_months: List of months available in current year (e.g., [1, 2, 3]). If None, assumes all 12 months.

    Returns:
        pl.Expr: Boolean expression indicating if beneficiary was termed on BAR
    """
    date(previous_year, 1, 1)
    date(previous_year, 12, 31)

    prev_year_prefix = f"ym_{previous_year}"
    curr_year_prefix = f"ym_{current_year}"

    # Determine which months to check for current year
    if current_year_months is None:
        current_year_months = list(range(1, 13))

    # Was in REACH in December of previous year (end of year BAR)
    was_reach_dec_prev = pl.col(f"{prev_year_prefix}12_reach").fill_null(False)

    # Not in REACH in current year
    not_reach_curr = ~pl.any_horizontal([
        pl.col(f"{curr_year_prefix}{month:02d}_reach").fill_null(False)
        for month in current_year_months
    ])

    # Check for termination date in previous year
    # Note: This requires that consolidated_alignment has been joined with BAR termination data
    # The aco_alignment_demographics transform should have brought in bene_date_of_death from BAR
    # We'll check if there's a last_reach_date that falls in previous year
    has_term_in_prev_year = (
        pl.col("last_reach_date").is_not_null() &
        (pl.col("last_reach_date").dt.year() == previous_year)
    )

    return (was_reach_dec_prev & not_reach_curr & has_term_in_prev_year).alias(f"termed_bar_{previous_year}")


def get_bar_terminations(
    bar_df: pl.LazyFrame,
    previous_year: int,
    mbi_col: str = "bene_mbi"
) -> pl.LazyFrame:
    """
    Extract termination data from BAR for a specific year.

    This helper function queries BAR source data to get all beneficiaries
    who were terminated in the specified year.

    Args:
        bar_df: BAR source data LazyFrame
        previous_year: Year to check for terminations
        mbi_col: Column name for MBI

    Returns:
        pl.LazyFrame: DataFrame with current_mbi and bar_term_date
    """
    prev_year_start = date(previous_year, 1, 1)
    prev_year_end = date(previous_year, 12, 31)

    return (
        bar_df
        .filter(
            pl.col("bene_date_of_term").is_not_null() &
            (pl.col("bene_date_of_term") >= prev_year_start) &
            (pl.col("bene_date_of_term") <= prev_year_end)
        )
        .select([
            pl.col(mbi_col).alias("current_mbi"),
            pl.col("bene_date_of_term").alias("bar_term_date"),
            pl.lit(True).alias(f"termed_bar_{previous_year}")
        ])
        .unique(subset=["current_mbi"])
    )
