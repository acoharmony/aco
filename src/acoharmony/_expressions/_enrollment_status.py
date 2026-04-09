# © 2025 HarmonyCares
# All rights reserved.

"""
Enrollment status expressions for notebook calculations.

Provides reusable Polars expressions for determining beneficiary enrollment status,
accounting for death dates, end dates, and temporal enrollment tracking.
"""


import polars as pl


def build_living_beneficiary_expr(df_schema: list[str]) -> pl.Expr:
    """
    Build expression to identify living beneficiaries.

    Checks death_date, bene_death_date, and bene_date_of_death columns if they exist.
    Returns True if beneficiary is alive (all death columns are null).

    Args:
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if alive, False if deceased
    """
    # Start with True (assume alive if no death columns)
    expr = pl.lit(True)

    # Check death_date if exists
    if "death_date" in df_schema:
        expr = expr & pl.col("death_date").is_null()

    # Check bene_death_date if exists
    if "bene_death_date" in df_schema:
        expr = expr & pl.col("bene_death_date").is_null()

    # Check bene_date_of_death if exists (BAR file uses this column)
    if "bene_date_of_death" in df_schema:
        expr = expr & pl.col("bene_date_of_death").is_null()

    return expr


def build_active_enrollment_expr(yearmo: str, program: str, df_schema: list[str]) -> pl.Expr:
    """
    Build expression for active enrollment status at a specific yearmo.

    An enrollment is considered "active" if:
    1. ym_{yearmo}_{program} == True (was enrolled that month according to temporal matrix)
       NOTE: The temporal matrix already accounts for enrollment start/end dates when
       building the ym_* columns, so we don't need to check end dates here.
    2. death_date IS NULL (not deceased)
    3. bene_death_date IS NULL (backup death check)

    Args:
        yearmo: Year-month string (e.g., "202401")
        program: Program name ("reach", "mssp", or "ffs")
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: Boolean expression - True if actively enrolled
    """
    # Base enrollment check - does the ym_{yearmo}_{program} column exist and equal True?
    enrollment_col = f"ym_{yearmo}_{program}"
    if enrollment_col not in df_schema:
        # If column doesn't exist, can't be enrolled
        return pl.lit(False)

    # Start with enrollment column (which already accounts for enrollment periods from temporal matrix)
    expr = pl.col(enrollment_col)

    # Add living check (exclude deceased beneficiaries)
    living_expr = build_living_beneficiary_expr(df_schema)
    expr = expr & living_expr

    return expr


def build_enrollment_status_expr(yearmo: str, df_schema: list[str]) -> pl.Expr:
    """
    Build expression that returns enrollment status category.

    Returns one of:
    - "REACH": Actively enrolled in ACO REACH
    - "MSSP": Actively enrolled in MSSP
    - "FFS": Has FFS claims, not in ACO
    - "Deceased": Beneficiary is deceased
    - "Unknown": Living but not enrolled anywhere

    Priority order: Deceased > REACH > MSSP > FFS > Unknown

    Args:
        yearmo: Year-month string (e.g., "202401")
        df_schema: List of column names in the dataframe

    Returns:
        pl.Expr: String expression with status category
    """
    # Build active enrollment expressions for each program
    reach_expr = build_active_enrollment_expr(yearmo, "reach", df_schema)
    mssp_expr = build_active_enrollment_expr(yearmo, "mssp", df_schema)
    ffs_expr = build_active_enrollment_expr(yearmo, "ffs", df_schema)
    living_expr = build_living_beneficiary_expr(df_schema)

    # Build status expression with priority order
    status_expr = (
        pl.when(~living_expr)
        .then(pl.lit("Deceased"))
        .when(reach_expr)
        .then(pl.lit("REACH"))
        .when(mssp_expr)
        .then(pl.lit("MSSP"))
        .when(ffs_expr)
        .then(pl.lit("FFS"))
        .otherwise(pl.lit("Unknown"))
    )

    return status_expr


def build_enrollment_counts_exprs(yearmo: str, df_schema: list[str]) -> list[pl.Expr]:
    """
    Build list of expressions for calculating enrollment counts.

    Returns expressions that sum up counts for each enrollment status.
    Useful for aggregating enrollment statistics in a single select() call.

    Args:
        yearmo: Year-month string (e.g., "202401")
        df_schema: List of column names in the dataframe

    Returns:
        list[pl.Expr]: List of sum expressions for reach, mssp, ffs, deceased, living, total
    """
    # Build individual status expressions
    reach_expr = build_active_enrollment_expr(yearmo, "reach", df_schema)
    mssp_expr = build_active_enrollment_expr(yearmo, "mssp", df_schema)
    ffs_expr = build_active_enrollment_expr(yearmo, "ffs", df_schema)
    living_expr = build_living_beneficiary_expr(df_schema)

    # Return list of aggregation expressions
    return [
        reach_expr.sum().alias("reach_count"),
        mssp_expr.sum().alias("mssp_count"),
        ffs_expr.sum().alias("ffs_count"),
        (~living_expr).sum().alias("deceased_count"),
        living_expr.sum().alias("living_count"),
        pl.len().alias("total_count"),
    ]
