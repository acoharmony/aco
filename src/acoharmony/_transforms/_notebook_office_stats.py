# © 2025 HarmonyCares
# All rights reserved.

"""
Office-level statistics calculations for notebook.

Provides transforms for calculating office-level enrollment, alignment,
and program distribution metrics.
"""

import polars as pl

from .._expressions._enrollment_status import build_living_beneficiary_expr
from .._expressions._office_stats import (
    build_office_alignment_type_aggregations,
    build_office_alignment_type_derived_metrics,
    build_office_enrollment_aggregations,
    build_office_enrollment_derived_metrics,
    build_office_program_distribution_aggregations,
    build_office_transition_aggregations,
    build_office_transition_derived_metrics,
)


def calculate_office_enrollment_stats(
    df: pl.LazyFrame, yearmo: str, office_column: str | None = None
) -> pl.DataFrame | None:
    """
    Calculate office enrollment statistics for a specific year-month.

    For each office, calculates:
    - Total beneficiaries
    - REACH/MSSP/FFS enrollment counts
    - Valid SVA count
    - Penetration rates

    Args:
        df: LazyFrame with consolidated alignment data
        yearmo: Year-month string (e.g., "202401")
        office_column: Column name for office grouping (auto-detects if None)

    Returns:
        DataFrame with office stats, or None if yearmo not available or office column missing
    """
    if not yearmo:
        return None

    schema = df.collect_schema().names()

    # Auto-detect office column if not specified
    if office_column is None:
        if "office_name" in schema:
            office_column = "office_name"
        elif "office_location" in schema:
            office_column = "office_location"
        else:
            return None

    # Check if office column exists
    if office_column not in schema:
        return None

    # Filter to living beneficiaries and non-null offices
    living_expr = build_living_beneficiary_expr(schema)
    df_filtered = df.filter(living_expr & pl.col(office_column).is_not_null())

    # Build aggregation expressions
    agg_exprs = build_office_enrollment_aggregations(yearmo, schema)

    # Group by office and calculate stats
    office_stats = (
        df_filtered
        .group_by([office_column, "office_location"] if "office_location" in schema else [office_column])
        .agg(agg_exprs)
        .with_columns(build_office_enrollment_derived_metrics())
        .sort("total_aco", descending=True)
        .collect()
    )

    return office_stats


def calculate_office_alignment_types(
    df: pl.LazyFrame, yearmo: str, office_column: str | None = None
) -> pl.DataFrame | None:
    """
    Calculate alignment type breakdown by office for a specific year-month.

    For each office, breaks down enrolled beneficiaries by:
    - Voluntary alignment (valid SVA)
    - Claims-based alignment only
    - Invalid voluntary (expired SVA)

    Args:
        df: LazyFrame with consolidated alignment data
        yearmo: Year-month string (e.g., "202401")
        office_column: Column name for office grouping (auto-detects if None)

    Returns:
        DataFrame with alignment type breakdown by office, or None if not available
    """
    if not yearmo:
        return None

    schema = df.collect_schema().names()

    # Auto-detect office column if not specified
    if office_column is None:
        if "office_name" in schema:
            office_column = "office_name"
        elif "office_location" in schema:
            office_column = "office_location"
        else:
            return None

    # Check if office column exists
    if office_column not in schema:
        return None

    # Filter to living beneficiaries and non-null offices
    living_expr = build_living_beneficiary_expr(schema)
    df_filtered = df.filter(living_expr & pl.col(office_column).is_not_null())

    # Build aggregation expressions
    agg_exprs = build_office_alignment_type_aggregations(yearmo, schema)

    # Group by office and calculate stats
    alignment_types = (
        df_filtered
        .group_by([office_column, "office_location"] if "office_location" in schema else [office_column])
        .agg(agg_exprs)
        .with_columns(build_office_alignment_type_derived_metrics())
        .sort("total_aligned", descending=True)
        .collect()
    )

    return alignment_types


def calculate_office_program_distribution(
    df: pl.LazyFrame, yearmo: str, office_column: str | None = None
) -> pl.DataFrame | None:
    """
    Calculate program distribution by office.

    For each office, counts beneficiaries in:
    - REACH only (historical)
    - MSSP only (historical)
    - Both REACH and MSSP (transitions)
    - Neither program

    Args:
        df: LazyFrame with consolidated alignment data
        yearmo: Year-month string (e.g., "202401") - used to ensure columns exist
        office_column: Column name for office grouping (auto-detects if None)

    Returns:
        DataFrame with program distribution by office, or None if not available
    """
    if not yearmo:
        return None

    schema = df.collect_schema().names()

    # Auto-detect office column if not specified
    if office_column is None:
        if "office_name" in schema:
            office_column = "office_name"
        elif "office_location" in schema:
            office_column = "office_location"
        else:
            return None

    # Check if office column exists
    if office_column not in schema:
        return None

    # Filter to living beneficiaries and non-null offices
    living_expr = build_living_beneficiary_expr(schema)
    df_filtered = df.filter(living_expr & pl.col(office_column).is_not_null())

    # Build aggregation expressions
    agg_exprs = build_office_program_distribution_aggregations(yearmo, schema)

    # Group by office and calculate stats
    program_dist = (
        df_filtered
        .group_by([office_column, "office_location"] if "office_location" in schema else [office_column])
        .agg(agg_exprs)
        .sort("total_beneficiaries", descending=True)
        .collect()
    )

    return program_dist


def calculate_office_transition_stats(df: pl.LazyFrame, office_column: str | None = None) -> pl.DataFrame | None:
    """
    Calculate program transition statistics by office.

    For each office, calculates:
    - Beneficiaries with program transitions (ever in both REACH and MSSP)
    - Continuous enrollment count
    - Average months in REACH/MSSP
    - Average total aligned months

    Args:
        df: LazyFrame with consolidated alignment data
        office_column: Column name for office grouping (auto-detects if None)

    Returns:
        DataFrame with transition stats by office, or None if office column missing
    """
    schema = df.collect_schema().names()

    # Auto-detect office column if not specified
    if office_column is None:
        if "office_name" in schema:
            office_column = "office_name"
        elif "office_location" in schema:
            office_column = "office_location"
        else:
            return None

    # Check if office column exists
    if office_column not in schema:
        return None

    # Filter to living beneficiaries and non-null offices
    living_expr = build_living_beneficiary_expr(schema)
    df_filtered = df.filter(living_expr & pl.col(office_column).is_not_null())

    # Build aggregation expressions
    agg_exprs = build_office_transition_aggregations(schema)

    # Group by office and calculate stats
    transition_stats = (
        df_filtered
        .group_by([office_column, "office_location"] if "office_location" in schema else [office_column])
        .agg(agg_exprs)
        .with_columns(build_office_transition_derived_metrics())
        .sort("total_beneficiaries", descending=True)
        .collect()
    )

    return transition_stats


def calculate_office_metadata(df: pl.LazyFrame, office_column: str | None = None) -> pl.DataFrame | None:
    """
    Calculate office location metadata.

    Returns office-level summary showing:
    - office_location (market/service area)
    - office_name (physical office)
    - Total beneficiaries assigned
    - Unique ZIP codes served

    Args:
        df: LazyFrame with consolidated alignment data
        office_column: Column name for office grouping (auto-detects if None)

    Returns:
        DataFrame with office metadata, or None if office column missing
    """
    schema = df.collect_schema().names()

    # Auto-detect office column if not specified
    if office_column is None:
        if "office_name" in schema:
            office_column = "office_name"
        elif "office_location" in schema:
            office_column = "office_location"
        else:
            return None

    # Check if office column exists
    if office_column not in schema:
        return None

    # Build aggregations
    agg_exprs = [
        pl.len().alias("total_beneficiaries"),
    ]

    # Add ZIP code count if available
    if "bene_zip_5" in schema:
        agg_exprs.append(pl.col("bene_zip_5").n_unique().alias("unique_zips"))
    elif "patient_zip" in schema:
        agg_exprs.append(pl.col("patient_zip").n_unique().alias("unique_zips"))
    else:
        # Add column even if underlying data doesn't exist, to match expected schema
        agg_exprs.append(pl.lit(0).alias("unique_zips"))

    # Group by office
    office_metadata = (
        df.filter(pl.col(office_column).is_not_null())
        .group_by([office_column, "office_location"] if "office_location" in schema else [office_column])
        .agg(agg_exprs)
        .sort("total_beneficiaries", descending=True)
        .collect()
    )

    return office_metadata
