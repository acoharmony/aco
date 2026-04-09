# © 2025 HarmonyCares
# All rights reserved.

"""
REACH HEDR (Health Equity Data Reporting) eligibility expressions.

Calculates numerator and denominator for REACH ACO HEDR reporting requirements:

Numerator:
    Number of beneficiaries with at least 6 months of alignment to the REACH ACO
    during the PY as of October 1 for which the REACH ACO successfully reports all
    required data elements.

Denominator:
    Number of beneficiaries with at least 6 months of alignment to the REACH ACO
    accumulated BY October 1 of the PY, based on April 1 of PY+1 final
    eligibility runout file. Beneficiaries who accumulated 6+ months by October 1
    are included regardless of when their enrollment ended (as long as the final
    runout file from April 1 of PY+1 confirms they had 6+ months).

Reference:
    CMS REACH Model Health Equity Data Reporting Requirements
"""

from datetime import date

import polars as pl

from ._enrollment_status import build_living_beneficiary_expr


def build_reach_hedr_denominator_expr(
    performance_year: int,
    october_first_date: date | None = None,
    april_first_final_date: date | None = None,
    df_schema: list[str] | None = None,
) -> pl.Expr:
    """
    Build expression for REACH HEDR denominator.

    Identifies beneficiaries eligible for HEDR reporting based on:
    1. Living beneficiary (not deceased)
    2. At least 6 months of alignment to REACH ACO BY October 1 of performance year
    3. Enrollment started on or before October 1 (to accumulate 6+ months)
    4. Final eligibility check uses April 1 of PY+1 runout file data
    5. Beneficiaries included regardless of when enrollment ended after October 1

    Args:
        performance_year: The performance year (e.g., 2024)
        october_first_date: October 1 of the performance year (defaults to PY-10-01)
        april_first_final_date: April 1 of PY+1 for final checks (defaults to (PY+1)-04-01)
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if eligible for HEDR denominator

    Example:
        >>> # For PY 2024
        >>> expr = build_reach_hedr_denominator_expr(
        ...     performance_year=2024,
        ...     df_schema=df.columns
        ... )
        >>> df = df.with_columns(hedr_denominator=expr)
    """
    if october_first_date is None:
        october_first_date = date(performance_year, 10, 1)

    if april_first_final_date is None:
        april_first_final_date = date(performance_year + 1, 4, 1)

    if df_schema is None:
        df_schema = []

    # Start with living beneficiary check
    living_expr = build_living_beneficiary_expr(df_schema)
    expr = living_expr

    # Check for at least 6 months of alignment to REACH during the performance year
    # BY October 1 (not necessarily still enrolled on October 1)
    # This could be tracked in several ways depending on data structure:

    # Option 1: Direct months_in_reach column for the performance year
    months_col = f"months_in_reach_{performance_year}"
    if months_col in df_schema:
        expr = expr & (pl.col(months_col) >= 6)
    elif "months_in_reach" in df_schema:
        # If there's a generic months_in_reach column, use it
        expr = expr & (pl.col("months_in_reach") >= 6)
    else:
        # Option 2: Count REACH enrollment months from temporal matrix
        # Count ym_YYYYMM_reach columns that are True UP TO AND INCLUDING October
        reach_month_cols = [
            col for col in df_schema
            if col.startswith(f"ym_{performance_year}") and col.endswith("_reach")
        ]
        if reach_month_cols:
            # Filter to only months through October (01-10)
            reach_month_cols_thru_oct = [
                col for col in reach_month_cols
                if int(col.split("_")[1][4:6]) <= 10  # Extract month from ym_YYYYMM_reach
            ]
            # Sum up the months where beneficiary was in REACH through October
            month_count_expr = sum(
                pl.col(col).fill_null(False).cast(pl.Int8)
                for col in reach_month_cols_thru_oct
            )
            expr = expr & (month_count_expr >= 6)

    # Check alignment status: beneficiary must have started REACH on or before October 1
    # This ensures they had the opportunity to accumulate 6 months by October 1
    if "first_reach_date" in df_schema:
        expr = expr & (
            pl.col("first_reach_date").is_not_null()
            & (pl.col("first_reach_date") <= pl.lit(october_first_date))
        )

    # For last_reach_date: If they ended, they must have ended ON or AFTER October 1
    # (i.e., they were enrolled during October, which counts toward the 6 months)
    # Someone enrolled 01-01 to 10-31 should be included (10 months, includes Oct 1)
    if "last_reach_date" in df_schema:
        expr = expr & (
            pl.col("last_reach_date").is_null()
            | (pl.col("last_reach_date") >= pl.lit(october_first_date))
        )

    # Final eligibility check: Based on April 1 of PY+1 final runout eligibility
    # The key requirement is 6+ months of enrollment by October 1 (checked above)
    # The April 1 runout date is used to determine the final count from the runout file
    # As long as the beneficiary shows 6+ months in the final runout file, they qualify
    # NOTE: eligibility_end_date should NOT exclude beneficiaries who had 6+ months by Oct 1
    # The runout file itself (which may be as recent as February) determines final eligibility

    # Check observable window includes October 1 of PY
    if "observable_start" in df_schema and "observable_end" in df_schema:
        expr = expr & (
            (pl.col("observable_start") <= pl.lit(october_first_date))
            & (pl.col("observable_end") >= pl.lit(october_first_date))
        )

    return expr


def build_reach_hedr_numerator_expr(
    performance_year: int,
    october_first_date: date | None = None,
    april_first_final_date: date | None = None,
    required_data_columns: list[str] | None = None,
    df_schema: list[str] | None = None,
) -> pl.Expr:
    """
    Build expression for REACH HEDR numerator.

    Identifies beneficiaries in the denominator for which the REACH ACO
    successfully reported all required data elements.

    Args:
        performance_year: The performance year (e.g., 2024)
        october_first_date: October 1 of the performance year (defaults to PY-10-01)
        april_first_final_date: April 1 of PY+1 for final checks (defaults to (PY+1)-04-01)
        required_data_columns: List of column names that must have non-null values
                              (e.g., ["race", "ethnicity", "language_preference"])
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if eligible for HEDR numerator

    Example:
        >>> # For PY 2024 with required HEDR data elements
        >>> expr = build_reach_hedr_numerator_expr(
        ...     performance_year=2024,
        ...     required_data_columns=["race", "ethnicity", "language_preference"],
        ...     df_schema=df.columns
        ... )
        >>> df = df.with_columns(hedr_numerator=expr)
    """
    if df_schema is None:
        df_schema = []

    # Start with denominator criteria
    denominator_expr = build_reach_hedr_denominator_expr(
        performance_year=performance_year,
        october_first_date=october_first_date,
        april_first_final_date=april_first_final_date,
        df_schema=df_schema,
    )

    expr = denominator_expr

    # Check that all required data elements are reported (non-null)
    if required_data_columns:
        for col in required_data_columns:
            if col in df_schema:
                # Data element must be non-null and non-empty
                # For string columns, also check they're not empty strings after stripping whitespace
                expr = expr & (
                    pl.col(col).is_not_null()
                    & (
                        # Either not a string, or a non-empty string
                        pl.col(col).cast(pl.Utf8, strict=False).str.strip_chars() != ""
                    )
                )
    else:
        # If no specific columns provided, check for common HEDR data elements
        # Based on CMS REACH HEDR requirements
        common_hedr_columns = [
            "race",
            "ethnicity",
            "language_preference",
            "preferred_language",
            "gender_identity",
            "sexual_orientation",
            "disability_status",
        ]

        # Check each column if it exists in the schema
        for col in common_hedr_columns:
            if col in df_schema:
                # Data element must be non-null and non-empty
                # For string columns, also check they're not empty strings after stripping whitespace
                expr = expr & (
                    pl.col(col).is_not_null()
                    & (
                        # Either not a string, or a non-empty string
                        pl.col(col).cast(pl.Utf8, strict=False).str.strip_chars() != ""
                    )
                )

    # Alternatively, check for a data_complete flag if available
    if "hedr_data_complete" in df_schema:
        expr = expr & pl.col("hedr_data_complete")

    return expr


def build_reach_hedr_rate_expr(
    performance_year: int,
    october_first_date: date | None = None,
    april_first_final_date: date | None = None,
    required_data_columns: list[str] | None = None,
    df_schema: list[str] | None = None,
) -> dict[str, pl.Expr]:
    """
    Build expressions for REACH HEDR rate calculation.

    Returns a dictionary of expressions for:
    - hedr_denominator: Boolean flag for denominator eligibility
    - hedr_numerator: Boolean flag for numerator eligibility
    - hedr_eligible: Alias for denominator (beneficiaries eligible for HEDR)
    - hedr_complete: Alias for numerator (beneficiaries with complete data)

    Args:
        performance_year: The performance year (e.g., 2024)
        october_first_date: October 1 of the performance year (defaults to PY-10-01)
        april_first_final_date: April 1 of PY+1 for final checks (defaults to (PY+1)-04-01)
        required_data_columns: List of column names that must have non-null values
        df_schema: List of column names in the dataframe

    Returns:
        dict[str, pl.Expr]: Dictionary of expressions for HEDR metrics

    Example:
        >>> # Calculate HEDR rate for PY 2024
        >>> exprs = build_reach_hedr_rate_expr(
        ...     performance_year=2024,
        ...     required_data_columns=["race", "ethnicity"],
        ...     df_schema=df.columns
        ... )
        >>> df = df.with_columns(**exprs)
        >>>
        >>> # Calculate aggregate rate
        >>> rate_df = df.select([
        ...     pl.col("hedr_numerator").sum().alias("numerator_count"),
        ...     pl.col("hedr_denominator").sum().alias("denominator_count"),
        ... ]).with_columns(
        ...     hedr_rate=(pl.col("numerator_count") / pl.col("denominator_count"))
        ... )
    """
    denominator_expr = build_reach_hedr_denominator_expr(
        performance_year=performance_year,
        october_first_date=october_first_date,
        april_first_final_date=april_first_final_date,
        df_schema=df_schema,
    )

    numerator_expr = build_reach_hedr_numerator_expr(
        performance_year=performance_year,
        october_first_date=october_first_date,
        april_first_final_date=april_first_final_date,
        required_data_columns=required_data_columns,
        df_schema=df_schema,
    )

    return {
        "hedr_denominator": denominator_expr,
        "hedr_numerator": numerator_expr,
        "hedr_eligible": denominator_expr,  # Alias for clarity
        "hedr_complete": numerator_expr,     # Alias for clarity
    }


def calculate_reach_hedr_rate(
    denominator_count: int,
    numerator_count: int,
) -> float:
    """
    Calculate REACH HEDR reporting rate.

    Args:
        denominator_count: Number of eligible beneficiaries
        numerator_count: Number of beneficiaries with complete data

    Returns:
        float: HEDR rate as a percentage (0-100), or 0 if denominator is 0

    Example:
        >>> rate = calculate_reach_hedr_rate(denominator_count=1000, numerator_count=850)
        >>> print(f"HEDR Rate: {rate:.1f}%")
        HEDR Rate: 85.0%
    """
    if denominator_count == 0:
        return 0.0

    return (numerator_count / denominator_count) * 100
