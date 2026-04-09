# © 2025 HarmonyCares
# All rights reserved.

"""
Enrollment statistics calculations for notebook.

Provides transforms for calculating current enrollment status statistics,
properly accounting for death dates, end dates, and temporal enrollment tracking.

These transforms are designed to replace inline calculations in the notebook
with tested, reusable, and maintainable functions.
"""


import polars as pl

from .._expressions._enrollment_status import (
    build_active_enrollment_expr,
    build_enrollment_counts_exprs,
    build_living_beneficiary_expr,
)


def calculate_current_enrollment_stats(df: pl.LazyFrame, yearmo: str) -> dict[str, int]:
    """
    Calculate enrollment statistics for a specific year-month.

    Properly accounts for:
    - death_date: Deceased beneficiaries are NOT counted in enrollment
    - bene_death_date: Backup death date check
    - ym_{yearmo}_{program}: Temporal enrollment status from matrix
      (NOTE: The temporal matrix already accounts for enrollment start/end dates)

    Returns dict with:
    - reach: Count actively enrolled in ACO REACH (alive, not ended, enrolled)
    - mssp: Count actively enrolled in MSSP (alive, not ended, enrolled)
    - ffs: Count with FFS claims (alive, not ended, not in ACO)
    - deceased: Count of ALL deceased beneficiaries (regardless of last status)
    - unknown: Count of living beneficiaries not enrolled anywhere
    - total: Total count of all beneficiaries

    Math check: reach + mssp + ffs + deceased + unknown = total

    Args:
        df: LazyFrame with consolidated alignment data
        yearmo: Year-month string (e.g., "202401")

    Returns:
        dict[str, int]: Enrollment statistics

    """
    # Get schema for column existence checks
    schema = df.collect_schema().names()

    # Build count expressions
    count_exprs = build_enrollment_counts_exprs(yearmo, schema)

    # Calculate counts in one query
    result = df.select(count_exprs).collect()

    # Extract counts
    stats = {
        "reach": int(result["reach_count"][0]),
        "mssp": int(result["mssp_count"][0]),
        "ffs": int(result["ffs_count"][0]),
        "deceased": int(result["deceased_count"][0]),
        "living": int(result["living_count"][0]),
        "total": int(result["total_count"][0]),
    }

    # Calculate unknown (living but not enrolled)
    enrolled_count = stats["reach"] + stats["mssp"] + stats["ffs"]
    stats["unknown"] = stats["living"] - enrolled_count

    # Validation: all categories should sum to total
    calculated_total = stats["reach"] + stats["mssp"] + stats["ffs"] + stats["deceased"] + stats["unknown"]
    if calculated_total != stats["total"]:
        raise ValueError(
            f"Enrollment counts don't sum to total: "
            f"{stats['reach']} + {stats['mssp']} + {stats['ffs']} + "
            f"{stats['deceased']} + {stats['unknown']} = {calculated_total} != {stats['total']}"
        )

    return stats


def calculate_enrollment_breakdown(
    df: pl.LazyFrame, yearmo: str, include_status_column: bool = True
) -> pl.DataFrame:
    """
    Calculate per-beneficiary enrollment status breakdown.

    Returns a DataFrame with enrollment status for each beneficiary.
    Useful for detailed analysis and filtering.

    Args:
        df: LazyFrame with consolidated alignment data
        yearmo: Year-month string (e.g., "202401")
        include_status_column: Whether to include enrollment_status text column

    Returns:
        pl.DataFrame with columns:
        - current_mbi: Beneficiary MBI
        - is_living: Boolean - is alive
        - is_reach: Boolean - actively enrolled in REACH
        - is_mssp: Boolean - actively enrolled in MSSP
        - is_ffs: Boolean - has FFS claims
        - enrollment_status: String - "REACH", "MSSP", "FFS", "Deceased", or "Unknown" (if include_status_column)
    """
    schema = df.collect_schema().names()

    # Build expressions
    reach_expr = build_active_enrollment_expr(yearmo, "reach", schema)
    mssp_expr = build_active_enrollment_expr(yearmo, "mssp", schema)
    ffs_expr = build_active_enrollment_expr(yearmo, "ffs", schema)
    living_expr = build_living_beneficiary_expr(schema)

    # Base columns
    select_exprs = [
        pl.col("current_mbi"),
        living_expr.alias("is_living"),
        reach_expr.alias("is_reach"),
        mssp_expr.alias("is_mssp"),
        ffs_expr.alias("is_ffs"),
    ]

    # Add status column if requested
    if include_status_column:
        from .._expressions._enrollment_status import build_enrollment_status_expr

        status_expr = build_enrollment_status_expr(yearmo, schema)
        select_exprs.append(status_expr.alias("enrollment_status"))

    return df.select(select_exprs).collect()


def get_actively_enrolled_df(df: pl.LazyFrame, yearmo: str, program: str | None = None) -> pl.LazyFrame:
    """
    Filter to actively enrolled beneficiaries for a specific program.

    Returns a LazyFrame filtered to beneficiaries who are:
    - Alive (death_date IS NULL AND bene_death_date IS NULL)
    - Enrolled in specified program for that yearmo (ym_{yearmo}_{program} == TRUE)
      NOTE: Enrollment periods are already accounted for in the temporal matrix

    Args:
        df: LazyFrame with consolidated alignment data
        yearmo: Year-month string (e.g., "202401")
        program: Program to filter for ("reach", "mssp", "ffs", or None for any ACO)

    Returns:
        pl.LazyFrame: Filtered to actively enrolled beneficiaries

    """
    schema = df.collect_schema().names()

    if program:
        # Filter to specific program
        filter_expr = build_active_enrollment_expr(yearmo, program, schema)
    else:
        # Filter to any ACO enrollment
        reach_expr = build_active_enrollment_expr(yearmo, "reach", schema)
        mssp_expr = build_active_enrollment_expr(yearmo, "mssp", schema)
        filter_expr = reach_expr | mssp_expr

    return df.filter(filter_expr)


def get_living_beneficiaries_df(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filter to living beneficiaries only.

    Returns a LazyFrame filtered to beneficiaries who are alive
    (death_date IS NULL AND bene_death_date IS NULL).

    Args:
        df: LazyFrame with consolidated alignment data

    Returns:
        pl.LazyFrame: Filtered to living beneficiaries

    """
    schema = df.collect_schema().names()
    living_expr = build_living_beneficiary_expr(schema)
    return df.filter(living_expr)
