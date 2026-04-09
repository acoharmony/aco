# © 2025 HarmonyCares
# All rights reserved.

"""
Current REACH attribution expressions.

Provides expressions to determine currently REACH-attributed patients using:
- Observable window filtering (observable_start, observable_end)
- Most recent BAR file data
- Death date exclusions

These expressions properly handle the temporal aspect of attribution by focusing
on the most recent observable period.
"""

from datetime import date

import polars as pl

from ._enrollment_status import build_living_beneficiary_expr


def build_current_reach_expr(reference_date: date | None = None, df_schema: list[str] | None = None) -> pl.Expr:
    """
    Build expression to identify currently REACH-attributed beneficiaries.

    Determines current REACH attribution by:
    1. Filtering to living beneficiaries (death_date is null)
    2. Checking if reference_date falls within observable window
    3. Checking last_reach_date to ensure recent attribution

    Args:
        reference_date: Date to check for current attribution (defaults to today)
        df_schema: List of column names in the dataframe (for death date checking)

    Returns:
        pl.Expr: Boolean expression - True if currently REACH-attributed
    """
    if reference_date is None:
        from datetime import date as date_module
        reference_date = date_module.today()

    if df_schema is None:
        df_schema = []

    # Start with living beneficiary check
    living_expr = build_living_beneficiary_expr(df_schema)

    # Build expression for current REACH attribution
    expr = living_expr

    # Check if observable window columns exist
    if "observable_start" in df_schema and "observable_end" in df_schema:
        # Reference date must be within observable window
        expr = expr & (
            (pl.col("observable_start") <= pl.lit(reference_date))
            & (pl.col("observable_end") >= pl.lit(reference_date))
        )

    # Check if last_reach_date exists and is recent
    if "last_reach_date" in df_schema:
        # Must have been in REACH at some point
        expr = expr & pl.col("last_reach_date").is_not_null()
        # Last REACH date should be within observable window
        expr = expr & (pl.col("last_reach_date") <= pl.lit(reference_date))

    # Check ever_reach flag if available
    if "ever_reach" in df_schema:
        expr = expr & pl.col("ever_reach")

    return expr


def build_current_reach_with_bar_expr(reference_date: date | None = None, df_schema: list[str] | None = None) -> pl.Expr:
    """
    Build expression to identify currently REACH-attributed beneficiaries from BAR data.

    Uses most recent BAR file to determine current attribution status.
    This is more accurate than temporal columns as it reflects the actual
    attribution list from CMS.

    For BAR files, this filters to:
    1. Current alignment files (ALGC) - not reconciliation files (ALGR)
    2. Most recent ALGC file by lexicographic ordering
    3. Living beneficiaries only

    Args:
        reference_date: Date to check for current attribution (defaults to today)
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if in most recent ALGC BAR file and living
    """
    if reference_date is None:
        from datetime import date as date_module
        reference_date = date_module.today()

    if df_schema is None:
        df_schema = []

    # Start with living beneficiary check
    living_expr = build_living_beneficiary_expr(df_schema)

    # Filter to current alignment files (ALGC) and most recent among those
    # ALGC = Current alignment, ALGR = Reconciliation (historical)
    # For BAR files: P.D0259.ALGC25.RP.D251118... is current, ALGR24 is reconciliation
    #
    # IMPORTANT: We can't use simple AND with filter_most_recent_by_filename() because
    # max() would be computed over ALL files, not just ALGC files.
    # Instead, we filter to ALGC files AND to the max filename within ALGC files.
    # We do this by checking if filename contains ALGC AND equals the max of all ALGC filenames.
    current_alignment_expr = pl.col("source_filename").str.contains(r"\.ALGC")

    # Get max filename among ALGC files using WHEN/THEN to conditionally compute max
    # If filename contains ALGC, check if it's the max among ALGC files
    # This works because polars will filter the max to only ALGC files when we combine with &
    max_algc_filename = (
        pl.when(pl.col("source_filename").str.contains(r"\.ALGC"))
        .then(pl.col("source_filename"))
        .otherwise(None)
    ).max()

    most_recent_algc_expr = pl.col("source_filename") == max_algc_filename

    # Check program is REACH if column exists
    expr = living_expr & current_alignment_expr & most_recent_algc_expr

    if "program" in df_schema:
        expr = expr & (pl.col("program") == "REACH")

    # Check file date is recent if available
    if "file_date_parsed" in df_schema:
        # File date should not be too far in the past (within 6 months)
        from datetime import timedelta
        max_age_days = 180
        cutoff_date = reference_date - timedelta(days=max_age_days)
        expr = expr & (pl.col("file_date_parsed") >= pl.lit(cutoff_date))

    return expr


def build_reach_attribution_window_expr(start_date: date, end_date: date, df_schema: list[str] | None = None) -> pl.Expr:
    """
    Build expression to check REACH attribution within a specific date range.

    Useful for analyzing attribution status during a specific period,
    such as a performance year or quarter.

    Args:
        start_date: Start of the attribution window
        end_date: End of the attribution window
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if attributed during the window
    """
    if df_schema is None:
        df_schema = []

    # Start with living beneficiary check
    living_expr = build_living_beneficiary_expr(df_schema)

    expr = living_expr

    # Check if REACH dates overlap with window
    if "first_reach_date" in df_schema and "last_reach_date" in df_schema:
        # Beneficiary was in REACH if:
        # - first_reach_date <= end_date (started before window ended)
        # - last_reach_date >= start_date (ended after window started)
        expr = expr & (
            pl.col("first_reach_date").is_not_null()
            & (pl.col("first_reach_date") <= pl.lit(end_date))
            & (pl.col("last_reach_date") >= pl.lit(start_date))
        )

    return expr
