# © 2025 HarmonyCares
# All rights reserved.

"""
Current MSSP attribution expressions.

Provides expressions to determine currently MSSP-attributed patients using:
- Observable window filtering (observable_start, observable_end)
- Most recent ALR file data
- Death date exclusions
- REACH exclusion (beneficiaries in REACH cannot be in MSSP)

IMPORTANT: A beneficiary can only be attributed to ONE program at a time.
REACH takes precedence over MSSP. If a beneficiary is currently in REACH,
they CANNOT be currently in MSSP.

These expressions properly handle the temporal aspect of attribution by focusing
on the most recent observable period.
"""

from datetime import date

import polars as pl

from ._enrollment_status import build_living_beneficiary_expr
from ._file_version import FileVersionExpression


def build_current_mssp_expr(reference_date: date | None = None, df_schema: list[str] | None = None) -> pl.Expr:
    """
    Build expression to identify currently MSSP-attributed beneficiaries.

    Determines current MSSP attribution by:
    1. Filtering to living beneficiaries (death_date is null)
    2. Checking if reference_date falls within observable window
    3. Checking last_mssp_date to ensure recent attribution
    4. CRITICAL: Ensuring beneficiary is NOT currently in REACH
       (REACH takes precedence - a beneficiary cannot be in both)

    Args:
        reference_date: Date to check for current attribution (defaults to today)
        df_schema: List of column names in the dataframe (for death date checking)

    Returns:
        pl.Expr: Boolean expression - True if currently MSSP-attributed (and NOT in REACH)
    """
    if reference_date is None:
        from datetime import date as date_module
        reference_date = date_module.today()

    if df_schema is None:
        df_schema = []

    # Start with living beneficiary check
    living_expr = build_living_beneficiary_expr(df_schema)

    # Build expression for current MSSP attribution
    expr = living_expr

    # Check if observable window columns exist
    if "observable_start" in df_schema and "observable_end" in df_schema:
        # Reference date must be within observable window
        expr = expr & (
            (pl.col("observable_start") <= pl.lit(reference_date))
            & (pl.col("observable_end") >= pl.lit(reference_date))
        )

    # Check if last_mssp_date exists and is recent
    if "last_mssp_date" in df_schema:
        # Must have been in MSSP at some point
        expr = expr & pl.col("last_mssp_date").is_not_null()
        # Last MSSP date should be within observable window
        expr = expr & (pl.col("last_mssp_date") <= pl.lit(reference_date))

    # Check ever_mssp flag if available
    if "ever_mssp" in df_schema:
        expr = expr & pl.col("ever_mssp")

    # CRITICAL: Exclude beneficiaries currently in REACH
    # REACH takes precedence - if last_reach_date is more recent than last_mssp_date,
    # then the beneficiary is in REACH, not MSSP
    if "last_reach_date" in df_schema and "last_mssp_date" in df_schema:
        # Beneficiary is in MSSP only if:
        # - last_reach_date is NULL (never in REACH), OR
        # - last_mssp_date > last_reach_date (MSSP is more recent)
        expr = expr & (
            pl.col("last_reach_date").is_null()
            | (pl.col("last_mssp_date") > pl.col("last_reach_date"))
        )
    elif "last_reach_date" in df_schema:
        # If we only have last_reach_date, exclude anyone who was in REACH
        # (conservative approach - if they were ever in REACH and we don't know
        # their MSSP end date, assume REACH is current)
        expr = expr & pl.col("last_reach_date").is_null()

    return expr


def build_current_mssp_with_alr_expr(reference_date: date | None = None, df_schema: list[str] | None = None) -> pl.Expr:
    """
    Build expression to identify currently MSSP-attributed beneficiaries from ALR data.

    Uses most recent ALR file to determine current attribution status.
    This is more accurate than temporal columns as it reflects the actual
    attribution list from CMS.

    CRITICAL: Excludes beneficiaries who are in REACH (REACH takes precedence).

    Args:
        reference_date: Date to check for current attribution (defaults to today)
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if in most recent ALR file, living, and NOT in REACH
    """
    if reference_date is None:
        from datetime import date as date_module
        reference_date = date_module.today()

    if df_schema is None:
        df_schema = []

    # Start with living beneficiary check
    living_expr = build_living_beneficiary_expr(df_schema)

    # Filter to most recent ALR file (using lexicographic sorting which works with ISO dates)
    most_recent_file_expr = FileVersionExpression.filter_most_recent_by_filename()

    # Check program is MSSP if column exists
    expr = living_expr & most_recent_file_expr

    if "program" in df_schema:
        expr = expr & (pl.col("program") == "MSSP")

    # Check file date is recent if available
    if "file_date_parsed" in df_schema:
        # File date should not be too far in the past (within 6 months)
        from datetime import timedelta
        max_age_days = 180
        cutoff_date = reference_date - timedelta(days=max_age_days)
        expr = expr & (pl.col("file_date_parsed") >= pl.lit(cutoff_date))

    # CRITICAL: Exclude beneficiaries currently in REACH
    # Check if beneficiary is also in REACH (from BAR data)
    if "last_reach_date" in df_schema and "last_mssp_date" in df_schema:
        # Only in MSSP if last_mssp_date is more recent than last_reach_date
        expr = expr & (
            pl.col("last_reach_date").is_null()
            | (pl.col("last_mssp_date") > pl.col("last_reach_date"))
        )
    elif "last_reach_date" in df_schema:
        # Conservative: exclude if ever in REACH
        expr = expr & pl.col("last_reach_date").is_null()

    return expr


def build_mssp_attribution_window_expr(start_date: date, end_date: date, df_schema: list[str] | None = None) -> pl.Expr:
    """
    Build expression to check MSSP attribution within a specific date range.

    Useful for analyzing attribution status during a specific period,
    such as a performance year or quarter.

    CRITICAL: For historical windows, this checks if the beneficiary was in MSSP
    during the window AND not in REACH at the end of the window (since REACH
    takes precedence for current attribution).

    Args:
        start_date: Start of the attribution window
        end_date: End of the attribution window
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if attributed during the window and not in REACH
    """
    if df_schema is None:
        df_schema = []

    # Start with living beneficiary check
    living_expr = build_living_beneficiary_expr(df_schema)

    expr = living_expr

    # Check if MSSP dates overlap with window
    if "first_mssp_date" in df_schema and "last_mssp_date" in df_schema:
        # Beneficiary was in MSSP if:
        # - first_mssp_date <= end_date (started before window ended)
        # - last_mssp_date >= start_date (ended after window started)
        expr = expr & (
            pl.col("first_mssp_date").is_not_null()
            & (pl.col("first_mssp_date") <= pl.lit(end_date))
            & (pl.col("last_mssp_date") >= pl.lit(start_date))
        )

    # CRITICAL: Exclude if in REACH at the end of the window
    # If last_reach_date is >= last_mssp_date and overlaps the window,
    # then REACH takes precedence
    if "first_reach_date" in df_schema and "last_reach_date" in df_schema and "last_mssp_date" in df_schema:
        # Not in MSSP if REACH was more recent within the window
        reach_in_window = (
            pl.col("first_reach_date").is_not_null()
            & (pl.col("first_reach_date") <= pl.lit(end_date))
            & (pl.col("last_reach_date") >= pl.lit(start_date))
        )
        # If both programs overlap the window, use the one with the more recent last_date
        expr = expr & (
            ~reach_in_window  # REACH doesn't overlap window at all, OR
            | (pl.col("last_mssp_date") > pl.col("last_reach_date"))  # MSSP is more recent
        )

    return expr
