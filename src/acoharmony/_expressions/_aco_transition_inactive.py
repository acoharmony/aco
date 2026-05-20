# © 2025 HarmonyCares
# All rights reserved.

"""
Year-over-year transition expression: Inactive beneficiaries.

Identifies beneficiaries who became inactive (no claims activity, no contact,
possibly moved or deceased but not yet reported).
"""

from datetime import date

import polars as pl


def build_inactive_expr(current_year: int, previous_year: int, reference_date: date | None = None, current_year_months: list[int] | None = None) -> pl.Expr:
    """
    Calculate beneficiaries who became inactive.

    Inactive criteria:
    - Was aligned in previous year
    - No claims activity (no first_claim months) in current year
    - No outreach activity (no emails/mailings) in current year
    - Not deceased (death_date is null or after current year)
    - No alignment in current year

    This indicates beneficiary may have:
    - Moved out of service area
    - Stopped seeking care
    - Changed to cash-pay or other coverage not captured

    Args:
        current_year: Current year to compare (e.g., 2026)
        previous_year: Previous year to compare against (e.g., 2025)
        reference_date: Optional reference date for calculations (defaults to today)
        current_year_months: List of months available in current year (e.g., [1, 2, 3]). If None, assumes all 12 months.

    Returns:
        pl.Expr: Boolean expression indicating if beneficiary is inactive
    """
    if reference_date is None:
        reference_date = date.today()

    prev_year_prefix = f"ym_{previous_year}"
    curr_year_prefix = f"ym_{current_year}"

    # Determine which months to check for current year
    if current_year_months is None:
        current_year_months = list(range(1, 13))

    # Was in REACH in December of previous year
    was_reach_dec_prev = pl.col(f"{prev_year_prefix}12_reach").fill_null(False)

    # Not in REACH in current year
    not_reach_curr = ~pl.any_horizontal([
        pl.col(f"{curr_year_prefix}{month:02d}_reach").fill_null(False)
        for month in current_year_months
    ])

    # No claims activity in current year (check first_claim columns)
    no_claims_curr = ~pl.any_horizontal([
        pl.col(f"{curr_year_prefix}{month:02d}_first_claim").fill_null(False)
        for month in current_year_months
    ])

    # Not deceased (or death date is after reference date)
    not_deceased = (
        pl.col("death_date").is_null() |
        (pl.col("death_date") > reference_date)
    )

    return (
        was_reach_dec_prev &
        not_reach_curr &
        no_claims_curr &
        not_deceased
    ).alias(f"inactive_{previous_year}")


def build_inactivity_duration_expr(reference_date: date | None = None) -> pl.Expr:
    """
    Calculate days since last activity (claims or outreach).

    Args:
        reference_date: Reference date for calculation (defaults to today)

    Returns:
        pl.Expr: Integer expression for days since last activity
    """
    if reference_date is None:
        reference_date = date.today()

    # Get most recent activity date (max of claims and alignment)
    last_activity = pl.max_horizontal([
        pl.col("ffs_first_date").fill_null(date(1900, 1, 1)),
        pl.col("last_reach_date").fill_null(date(1900, 1, 1)),
        pl.col("last_mssp_date").fill_null(date(1900, 1, 1))
    ])

    return (
        pl.when(last_activity.is_not_null())
        .then((pl.lit(reference_date) - last_activity).dt.total_days())
        .otherwise(pl.lit(None).cast(pl.Int64))
    ).alias("days_since_last_activity")


def get_inactive_beneficiaries(
    alignment_df: pl.LazyFrame,
    previous_year: int,
    current_year: int,
    inactivity_threshold_days: int = 365
) -> pl.LazyFrame:
    """
    Get beneficiaries who have been inactive for a specified period.

    Args:
        alignment_df: Main alignment dataframe
        previous_year: Previous year
        current_year: Current year
        inactivity_threshold_days: Minimum days of inactivity to flag (default 365)

    Returns:
        pl.LazyFrame: DataFrame with current_mbi and inactivity details
    """
    reference_date = date.today()

    inactive = (
        alignment_df
        .with_columns([
            build_inactivity_duration_expr(reference_date)
        ])
        .filter(
            pl.col("days_since_last_activity") >= inactivity_threshold_days
        )
        .select([
            pl.col("current_mbi"),
            pl.col("days_since_last_activity"),
            pl.col("last_outreach_date"),
            pl.col("ffs_first_date").alias("last_claims_date"),
            pl.lit("No activity detected").alias("inactive_reason"),
            pl.lit(True).alias(f"inactive_{previous_year}")
        ])
        .unique(subset=["current_mbi"])
    )

    return inactive
