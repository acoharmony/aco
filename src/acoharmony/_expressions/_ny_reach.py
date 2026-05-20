# © 2025 HarmonyCares
# All rights reserved.

"""
NY REACH patient expressions.

Provides expressions to identify patients who are:
1. Currently active in the REACH program
2. Have NY as their home office location

This is useful for NY-specific reporting, outreach campaigns, and
office-level analytics.
"""

from datetime import date

import polars as pl

from ._current_reach import build_current_reach_expr


def build_ny_reach_expr(
    reference_date: date | None = None,
    df_schema: list[str] | None = None,
    office_column: str = "office_location",
) -> pl.Expr:
    """
    Build expression to identify currently active REACH patients with NY home office.

    Identifies beneficiaries who are:
    1. Currently attributed to REACH (living, within observable window)
    2. Have NY as their home office location

    Args:
        reference_date: Date to check for current attribution (defaults to today)
        df_schema: List of column names in the dataframe
        office_column: Name of the office location column (defaults to "office_location")

    Returns:
        pl.Expr: Boolean expression - True if currently in REACH with NY home office

    Example:
        >>> # Identify NY REACH patients
        >>> df = df.with_columns(
        ...     is_ny_reach=build_ny_reach_expr(df_schema=df.columns)
        ... )
        >>>
        >>> # Count NY REACH patients
        >>> ny_reach_count = df.filter(
        ...     build_ny_reach_expr(df_schema=df.columns)
        ... ).select(pl.count()).item()
    """
    if df_schema is None:
        df_schema = []

    # Start with current REACH attribution
    current_reach_expr = build_current_reach_expr(
        reference_date=reference_date,
        df_schema=df_schema
    )

    # Add NY office filter
    if office_column in df_schema:
        ny_office_expr = pl.col(office_column) == "NY"
        expr = current_reach_expr & ny_office_expr
    else:
        # If office column doesn't exist, just use current REACH
        # (this allows the expression to work even if office data is missing)
        expr = current_reach_expr

    return expr


def build_ny_reach_lazyframe(
    lf: pl.LazyFrame,
    reference_date: date | None = None,
    include_inactive: bool = False,
) -> pl.LazyFrame:
    """
    Build LazyFrame of patients who are currently active in REACH with NY home office.

    This is a convenience function that filters a LazyFrame to NY REACH patients
    and adds a flag column for easier identification.

    Args:
        lf: Input LazyFrame with beneficiary data
        reference_date: Date to check for current attribution (defaults to today)
        include_inactive: If True, include patients who were previously in NY REACH
                         but are no longer active (defaults to False)

    Returns:
        pl.LazyFrame: Filtered LazyFrame with NY REACH patients and is_ny_reach flag

    Example:
        >>> # Get all current NY REACH patients
        >>> ny_reach_patients = build_ny_reach_lazyframe(
        ...     lf=beneficiary_lf,
        ...     reference_date=date(2025, 3, 1)
        ... )
        >>>
        >>> # Include previously active NY REACH patients
        >>> all_ny_reach = build_ny_reach_lazyframe(
        ...     lf=beneficiary_lf,
        ...     include_inactive=True
        ... )
    """
    df_schema = lf.collect_schema().names()

    # Build the NY REACH expression
    ny_reach_expr = build_ny_reach_expr(
        reference_date=reference_date,
        df_schema=df_schema
    )

    # Add flag column
    lf = lf.with_columns(is_ny_reach=ny_reach_expr)

    # Filter to NY REACH patients unless including inactive
    if not include_inactive:
        lf = lf.filter(pl.col("is_ny_reach"))
    else:
        # For inactive, we want anyone who was ever in REACH with NY office
        # This includes current NY REACH plus those who have NY office and were in REACH
        if "office_location" in df_schema:
            ny_office_expr = pl.col("office_location") == "NY"
            # Check if they were ever in REACH
            if "first_reach_date" in df_schema:
                ever_reach_expr = pl.col("first_reach_date").is_not_null()
                inclusive_expr = ny_office_expr & ever_reach_expr
                lf = lf.filter(inclusive_expr)
            else:
                # Just filter by office if no REACH history available
                lf = lf.filter(ny_office_expr)

    return lf


def build_ny_reach_with_bar_expr(
    reference_date: date | None = None,
    df_schema: list[str] | None = None,
    office_column: str = "office_location",
) -> pl.Expr:
    """
    Build expression to identify NY REACH patients using BAR file data.

    Uses most recent BAR (Beneficiary Alignment Report) file to determine
    current REACH attribution status. This is more accurate than temporal
    columns as it reflects the actual attribution list from CMS.

    Args:
        reference_date: Date to check for current attribution (defaults to today)
        df_schema: List of column names in the dataframe
        office_column: Name of the office location column (defaults to "office_location")

    Returns:
        pl.Expr: Boolean expression - True if in most recent BAR file with NY office

    Example:
        >>> # Using BAR file data for NY REACH identification
        >>> bar_df = bar_df.with_columns(
        ...     is_ny_reach_bar=build_ny_reach_with_bar_expr(df_schema=bar_df.columns)
        ... )
    """
    if df_schema is None:
        df_schema = []

    # Import here to avoid circular dependency
    from ._current_reach import build_current_reach_with_bar_expr

    # Start with current REACH attribution from BAR
    current_reach_bar_expr = build_current_reach_with_bar_expr(
        reference_date=reference_date,
        df_schema=df_schema
    )

    # Add NY office filter
    if office_column in df_schema:
        ny_office_expr = pl.col(office_column) == "NY"
        expr = current_reach_bar_expr & ny_office_expr
    else:
        # If office column doesn't exist, just use current REACH from BAR
        expr = current_reach_bar_expr

    return expr


def count_ny_reach_patients(
    lf: pl.LazyFrame,
    reference_date: date | None = None,
) -> int:
    """
    Count the number of currently active NY REACH patients.

    Args:
        lf: Input LazyFrame with beneficiary data
        reference_date: Date to check for current attribution (defaults to today)

    Returns:
        int: Count of NY REACH patients

    Example:
        >>> # Count NY REACH patients as of March 2025
        >>> count = count_ny_reach_patients(
        ...     lf=beneficiary_lf,
        ...     reference_date=date(2025, 3, 1)
        ... )
        >>> print(f"NY REACH patients: {count:,}")
    """
    df_schema = lf.collect_schema().names()

    ny_reach_expr = build_ny_reach_expr(
        reference_date=reference_date,
        df_schema=df_schema
    )

    count = lf.filter(ny_reach_expr).select(pl.count()).collect().item()
    return count
