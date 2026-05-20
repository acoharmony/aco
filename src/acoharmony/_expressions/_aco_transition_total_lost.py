# © 2025 HarmonyCares
# All rights reserved.

"""
Year-over-year transition expression: Total beneficiaries lost from previous year.

Idempotent expression builder that calculates beneficiaries who were aligned
in the previous year but not in the current year.
"""

import polars as pl


def build_total_lost_expr(current_year: int, previous_year: int, current_year_months: list[int] | None = None) -> pl.Expr:
    """
    Calculate total beneficiaries lost from previous year.

    Compares alignment between two years by checking:
    - Was aligned in any month of previous year (any ym_{previous_year}* = True)
    - NOT aligned in any month of current year (all ym_{current_year}* = False)

    Args:
        current_year: Current year to compare (e.g., 2026)
        previous_year: Previous year to compare against (e.g., 2025)
        current_year_months: List of months available in current year (e.g., [1, 2, 3]). If None, assumes all 12 months.

    Returns:
        pl.Expr: Boolean expression indicating if beneficiary was lost
    """
    # Build expressions to check if aligned in previous year
    # Check if ANY month in previous year had REACH or MSSP enrollment
    prev_year_prefix = f"ym_{previous_year}"
    curr_year_prefix = f"ym_{current_year}"

    # Determine which months to check for current year
    if current_year_months is None:
        current_year_months = list(range(1, 13))

    # Was aligned in REACH in December of previous year (end of year BAR)
    was_reach_dec_prev = pl.col(f"{prev_year_prefix}12_reach").fill_null(False)

    # Is NOT aligned in REACH in current year (no REACH months)
    # Only check months that actually exist in the data
    not_reach_curr = ~pl.any_horizontal([
        pl.col(f"{curr_year_prefix}{month:02d}_reach").fill_null(False)
        for month in current_year_months
    ])

    return (was_reach_dec_prev & not_reach_curr).alias(f"lost_{previous_year}_to_{current_year}")
