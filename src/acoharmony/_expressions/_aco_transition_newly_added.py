# © 2025 HarmonyCares
# All rights reserved.

"""
Year-over-year transition expression: Newly added beneficiaries.

Identifies beneficiaries who were NOT in REACH in December of previous year
but ARE in REACH in the current year, and categorizes where they came from.
"""

import polars as pl


def build_newly_added_expr(current_year: int, previous_year: int, current_year_months: list[int] | None = None) -> pl.Expr:
    """
    Calculate beneficiaries newly added to REACH.

    Newly added means:
    - Was NOT in REACH in December of previous year
    - IS in REACH in current year (any month)

    Args:
        current_year: Current year to compare (e.g., 2026)
        previous_year: Previous year to compare against (e.g., 2025)
        current_year_months: List of months available in current year (e.g., [1, 2, 3]). If None, assumes all 12 months.

    Returns:
        pl.Expr: Boolean expression indicating if beneficiary is newly added
    """
    prev_year_prefix = f"ym_{previous_year}"
    curr_year_prefix = f"ym_{current_year}"

    # Determine which months to check for current year
    if current_year_months is None:
        current_year_months = list(range(1, 13))

    # Was NOT in REACH in December of previous year
    not_reach_dec_prev = ~pl.col(f"{prev_year_prefix}12_reach").fill_null(False)

    # IS in REACH in current year
    is_reach_curr = pl.any_horizontal([
        pl.col(f"{curr_year_prefix}{month:02d}_reach").fill_null(False)
        for month in current_year_months
    ])

    return (not_reach_dec_prev & is_reach_curr).alias(f"newly_added_{previous_year}_to_{current_year}")


def build_newly_added_source_expr(current_year: int, previous_year: int) -> pl.Expr:
    """
    Categorize the source of newly added beneficiaries.

    Sources (in priority order):
    1. "From MSSP" - was in MSSP December previous year
    2. "SVA Campaign (Current Year)" - has recent SVA submission in current year with acceptance
    3. "SVA Campaign (Previous Year)" - has SVA submission in previous year with acceptance
    4. "PBVAR Accepted" - has acceptance code (A0/A1) from PBVAR
    5. "Returning (Had SVA)" - was in REACH before, has voluntary alignment
    6. "Returning (Claims-based)" - was in REACH before, no voluntary alignment
    7. "Other ACO Transfer" - has P2 response code (already in another ACO)
    8. "From MA" - has E2 response code (was in Medicare Advantage)
    9. "Pending SVA" - has recent SVA submission but not yet accepted
    10. "Unknown" - no clear source

    Args:
        current_year: Current year
        previous_year: Previous year

    Returns:
        pl.Expr: String expression indicating source of new beneficiary
    """
    prev_year_prefix = f"ym_{previous_year}"

    # Was in MSSP December previous year
    was_mssp_dec_prev = pl.col(f"{prev_year_prefix}12_mssp").fill_null(False)

    # Was in REACH at ANY point in previous year
    was_reach_anytime_prev = pl.any_horizontal([
        pl.col(f"{prev_year_prefix}{month:02d}_reach").fill_null(False)
        for month in range(1, 13)
    ])

    # Has voluntary alignment (SVA)
    has_voluntary = pl.col("has_voluntary_alignment").fill_null(False)

    # Has acceptance response code (A0 = Voluntary, A1 = Claims-based)
    has_acceptance = pl.col("has_acceptance").fill_null(False)

    # Has recent SVA submission in current year
    has_sva_current_year = (
        pl.col("last_sva_submission_date").is_not_null() &
        (pl.col("last_sva_submission_date").dt.year() == current_year)
    )

    # Has SVA submission in previous year
    has_sva_previous_year = (
        pl.col("last_sva_submission_date").is_not_null() &
        (pl.col("last_sva_submission_date").dt.year() == previous_year)
    )

    # Has any recent SVA submission (previous or current year)
    has_recent_sva = (
        pl.col("last_sva_submission_date").is_not_null() &
        (pl.col("last_sva_submission_date").dt.year() >= previous_year)
    )

    # Response code indicators
    has_ma_code = (
        pl.col("latest_response_codes").is_not_null() &
        pl.col("latest_response_codes").str.contains("E2")
    )

    has_other_aco = (
        pl.col("latest_response_codes").is_not_null() &
        pl.col("latest_response_codes").str.contains("P2")
    )

    # New enrollment (first appeared in REACH in current year)
    # Check if first_reach_date is in current year
    new_enrollment = (
        pl.col("first_reach_date").is_not_null() &
        (pl.col("first_reach_date").dt.year() == current_year)
    )

    return (
        pl.when(was_mssp_dec_prev)
        .then(pl.lit("From MSSP"))
        .when(has_sva_current_year & has_acceptance)
        .then(pl.lit("SVA Campaign (Current Year)"))
        .when(has_sva_previous_year & has_acceptance)
        .then(pl.lit("SVA Campaign (Previous Year)"))
        .when(has_acceptance & ~was_reach_anytime_prev)
        .then(pl.lit("PBVAR Accepted"))
        .when(was_reach_anytime_prev & has_voluntary)
        .then(pl.lit("Returning (Had SVA)"))
        .when(was_reach_anytime_prev)
        .then(pl.lit("Returning (Claims-based)"))
        .when(has_other_aco)
        .then(pl.lit("Other ACO Transfer"))
        .when(has_ma_code)
        .then(pl.lit("From MA"))
        .when(has_recent_sva)
        .then(pl.lit("Pending SVA"))
        .when(new_enrollment)
        .then(pl.lit("New Enrollment"))
        .otherwise(pl.lit("Unknown"))
    ).alias(f"newly_added_source_{previous_year}_to_{current_year}")


def build_first_reach_month_current_year_expr(current_year: int, current_year_months: list[int] | None = None) -> pl.Expr:
    """
    Find the first month in current year that beneficiary appeared in REACH.

    Args:
        current_year: Current year
        current_year_months: List of months available

    Returns:
        pl.Expr: Integer (1-12) for first month in REACH, or null
    """
    curr_year_prefix = f"ym_{current_year}"

    if current_year_months is None:
        current_year_months = list(range(1, 13))

    # Build expression that checks months in order and returns first match
    expr = pl.lit(None).cast(pl.Int8)
    for month in reversed(current_year_months):  # Reverse so first month wins
        expr = pl.when(pl.col(f"{curr_year_prefix}{month:02d}_reach").fill_null(False)).then(pl.lit(month)).otherwise(expr)

    return expr.alias(f"first_reach_month_{current_year}")


def get_newly_added_beneficiaries(
    alignment_df: pl.LazyFrame,
    previous_year: int,
    current_year: int
) -> pl.LazyFrame:
    """
    Get beneficiaries who were newly added to REACH with source details.

    Args:
        alignment_df: Main alignment dataframe with transition flags
        previous_year: Previous year
        current_year: Current year

    Returns:
        pl.LazyFrame: DataFrame with current_mbi and newly added details
    """
    newly_added = (
        alignment_df
        .filter(pl.col(f"newly_added_{previous_year}_to_{current_year}"))
        .select([
            pl.col("current_mbi"),
            pl.col("bene_first_name"),
            pl.col("bene_last_name"),
            pl.col("office_location"),
            pl.col(f"newly_added_source_{previous_year}_to_{current_year}"),
            pl.col(f"first_reach_month_{current_year}"),
            pl.col("voluntary_alignment_type"),
            pl.col("has_voluntary_alignment"),
            pl.col("latest_response_codes"),
        ])
        .unique(subset=["current_mbi"])
    )

    return newly_added
