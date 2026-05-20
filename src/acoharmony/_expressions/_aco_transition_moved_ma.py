# © 2025 HarmonyCares
# All rights reserved.

"""
Year-over-year transition expression: Moved to Medicare Advantage.

Identifies beneficiaries who enrolled in Medicare Advantage (Part C),
detected through PBVAR response codes indicating MA enrollment.
"""

import polars as pl

# Medicare Advantage response codes from PBVAR
# Per CMS REACH/MSSP documentation in ResponseCodeParserExpression
MA_RESPONSE_CODES = {
    "E2": "Enrolled in Medicare Advantage"
}


def build_moved_ma_expr(current_year: int, previous_year: int, current_year_months: list[int] | None = None) -> pl.Expr:
    """
    Calculate beneficiaries who moved to Medicare Advantage.

    Medicare Advantage enrollment is indicated by specific PBVAR response codes:
    - 05: Enrolled in MA
    - 06: Enrolled in MA-PD (with drug coverage)
    - 08: Part C coverage

    These codes appear in the latest_response_codes field from PBVAR data.

    Logic:
    - Was aligned in previous year (FFS-based ACO)
    - Not aligned in current year
    - Has MA-related response code in current year
    - Response code indicates Part C enrollment

    Args:
        current_year: Current year to compare (e.g., 2026)
        previous_year: Previous year to compare against (e.g., 2025)
        current_year_months: List of months available in current year (e.g., [1, 2, 3]). If None, assumes all 12 months.

    Returns:
        pl.Expr: Boolean expression indicating if beneficiary moved to MA
    """
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

    # Check for MA response codes in latest_response_codes field
    # Response codes are comma-separated strings like "A0, E1, E2"
    # E2 = Enrolled in Medicare Advantage (per CMS documentation)
    has_ma_response_code = (
        pl.col("latest_response_codes").is_not_null() &
        pl.col("latest_response_codes").str.contains("E2")
    )

    # Also check eligibility_issues field which may have parsed E2
    has_ma_eligibility_issue = (
        pl.col("eligibility_issues").is_not_null() &
        pl.col("eligibility_issues").str.contains("E2")
    )

    # Check if response code detail mentions MA
    has_ma_in_detail = (
        pl.col("latest_response_detail").is_not_null() &
        pl.col("latest_response_detail").str.contains("(?i)medicare advantage|(?i)part c")
    )

    # Combine all MA indicators
    has_ma_indicator = has_ma_response_code | has_ma_eligibility_issue | has_ma_in_detail

    return (
        was_reach_dec_prev &
        not_reach_curr &
        has_ma_indicator
    ).alias(f"moved_ma_{previous_year}")


def build_ma_enrollment_date_expr() -> pl.Expr:
    """
    Extract estimated MA enrollment date from response code data.

    Uses the pbvar_report_date to estimate when
    beneficiary enrolled in Medicare Advantage.

    Returns:
        pl.Expr: Date expression for MA enrollment, or null
    """
    return pl.when(
        pl.col("latest_response_codes").is_not_null() &
        pl.col("latest_response_codes").str.contains("E2")
    ).then(
        pl.col("pbvar_report_date")
    ).otherwise(
        pl.lit(None).cast(pl.Date)
    ).alias("ma_enrollment_date")


def get_ma_enrollments(
    pbvar_df: pl.LazyFrame,
    previous_year: int,
    current_year: int
) -> pl.LazyFrame:
    """
    Get beneficiaries who enrolled in Medicare Advantage.

    Queries PBVAR data for MA response codes between the years.

    Args:
        pbvar_df: PBVAR source data LazyFrame
        previous_year: Previous year
        current_year: Current year

    Returns:
        pl.LazyFrame: DataFrame with current_mbi and MA enrollment details
    """
    ma_enrollments = (
        pbvar_df
        .filter(
            # Response codes indicating MA
            (
                pl.col("response_code").is_in(["05", "06", "08"]) |
                pl.col("response_code_description").str.contains("(?i)medicare advantage")
            ) &
            # File date in current year (when we detected the MA enrollment)
            (pl.col("file_date").dt.year() == current_year)
        )
        .select([
            pl.col("normalized_mbi").alias("current_mbi"),
            pl.col("response_code").alias("ma_response_code"),
            pl.col("response_code_description").alias("ma_reason"),
            pl.col("file_date").alias("ma_detected_date"),
            pl.lit(True).alias(f"moved_ma_{previous_year}")
        ])
        .unique(subset=["current_mbi"])
    )

    return ma_enrollments
