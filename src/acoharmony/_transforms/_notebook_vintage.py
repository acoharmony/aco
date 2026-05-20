# © 2025 HarmonyCares
# All rights reserved.

"""
Vintage cohort calculations for notebook.

Provides transforms for calculating enrollment vintage cohorts based on
first enrollment date and analyzing cohort characteristics.
"""

import polars as pl

from .._expressions._vintage import (
    build_office_vintage_distribution_derived_metrics,
    build_vintage_distribution_derived_metrics,
)


def calculate_vintage_cohorts(df_enriched: pl.LazyFrame, most_recent_ym: str | None = None) -> pl.LazyFrame:
    """
    Calculate vintage cohorts based on first enrollment date.

    Adds columns to track:
    - first_enrollment_ym: First month beneficiary was enrolled (either REACH or MSSP)
    - months_since_first_enrollment: Months between first enrollment and most_recent_ym
    - vintage_cohort: Categorical cohort ("0-6 months", "6-12 months", "12-24 months", "24+ months", "Never Enrolled")

    Args:
        df_enriched: LazyFrame with enriched alignment data
        most_recent_ym: Most recent year-month for calculating months since enrollment (optional)

    Returns:
        LazyFrame with added vintage columns
    """
    schema = df_enriched.collect_schema().names()

    # Get all temporal columns
    reach_cols = sorted([col for col in schema if col.startswith("ym_") and col.endswith("_reach")])
    mssp_cols = sorted([col for col in schema if col.startswith("ym_") and col.endswith("_mssp")])

    if not reach_cols and not mssp_cols:
        # No temporal data available
        return df_enriched.with_columns(
            [
                pl.lit(None).alias("first_enrollment_ym"),
                pl.lit(None).alias("months_since_first_enrollment"),
                pl.lit("Never Enrolled").alias("vintage_cohort"),
            ]
        )

    # Collect data to find first enrollment
    # Note: This requires collection because we need to scan across dynamic columns
    df_collected = df_enriched.collect()

    # Find first enrollment for each beneficiary
    def find_first_enrollment(row: dict) -> str | None:
        """Find the first ym where beneficiary was enrolled in REACH or MSSP."""
        for col in reach_cols + mssp_cols:
            if col in row and row[col]:
                # Extract YM from column name: ym_YYYYMM_reach/mssp -> YYYYMM
                ym = col.replace("_reach", "").replace("_mssp", "").replace("ym_", "")
                return ym
        return None

    # Calculate first enrollments
    first_enrollments = []
    for row in df_collected.iter_rows(named=True):
        first_ym = find_first_enrollment(row)
        first_enrollments.append(first_ym)

    # Add first enrollment column
    df_with_vintage = df_collected.with_columns([pl.Series("first_enrollment_ym", first_enrollments)])

    # Calculate months since first enrollment
    if most_recent_ym:
        current_year = int(most_recent_ym[:4])
        current_month = int(most_recent_ym[4:])

        df_with_vintage = df_with_vintage.with_columns(
            [
                pl.when(pl.col("first_enrollment_ym").is_not_null())
                .then(

                        (current_year - pl.col("first_enrollment_ym").str.slice(0, 4).cast(pl.Int32)) * 12
                        + (current_month - pl.col("first_enrollment_ym").str.slice(4, 2).cast(pl.Int32))

                )
                .otherwise(None)
                .alias("months_since_first_enrollment")
            ]
        )

        # Create vintage cohorts
        df_with_vintage = df_with_vintage.with_columns(
            [
                pl.when(pl.col("months_since_first_enrollment").is_null())
                .then(pl.lit("Never Enrolled"))
                .when(pl.col("months_since_first_enrollment") <= 6)
                .then(pl.lit("0-6 months"))
                .when(pl.col("months_since_first_enrollment") <= 12)
                .then(pl.lit("6-12 months"))
                .when(pl.col("months_since_first_enrollment") <= 24)
                .then(pl.lit("12-24 months"))
                .otherwise(pl.lit("24+ months"))
                .alias("vintage_cohort")
            ]
        )
    else:
        # No most_recent_ym provided, just mark as enrolled/not enrolled
        df_with_vintage = df_with_vintage.with_columns(
            [
                pl.lit(None).alias("months_since_first_enrollment"),
                pl.when(pl.col("first_enrollment_ym").is_not_null())
                .then(pl.lit("Enrolled"))
                .otherwise(pl.lit("Never Enrolled"))
                .alias("vintage_cohort"),
            ]
        )

    return df_with_vintage.lazy()


def calculate_vintage_distribution(
    vintage_df: pl.LazyFrame, most_recent_ym: str | None = None
) -> pl.DataFrame | None:
    """
    Calculate vintage cohort statistics and distribution.

    Args:
        vintage_df: LazyFrame with vintage cohort data (from calculate_vintage_cohorts)
        most_recent_ym: Most recent year-month string (e.g., "202401")

    Returns:
        DataFrame with vintage distribution metrics:
            - vintage_cohort: Cohort category
            - count: Number of beneficiaries in cohort
            - current_reach: Currently enrolled in REACH
            - current_mssp: Currently enrolled in MSSP
            - avg_months_reach: Average months in REACH
            - avg_months_mssp: Average months in MSSP
            - avg_total_months: Average total aligned months
            - transitions: Count with program transitions
            - pct_of_enrolled: % of enrolled beneficiaries in this cohort
            - pct_in_reach: % of cohort currently in REACH
            - pct_in_mssp: % of cohort currently in MSSP
            - pct_with_transitions: % of cohort with program transitions
        Returns None if most_recent_ym not provided
    """
    if not most_recent_ym:
        return None

    schema = vintage_df.collect_schema().names()

    current_reach_col = f"ym_{most_recent_ym}_reach"
    current_mssp_col = f"ym_{most_recent_ym}_mssp"

    # Check if required columns exist
    if current_reach_col not in schema or current_mssp_col not in schema:
        return None

    # Build aggregations
    agg_exprs = [
        pl.len().alias("count"),
        pl.col(current_reach_col).sum().alias("current_reach"),
        pl.col(current_mssp_col).sum().alias("current_mssp"),
    ]

    # Add optional metrics if available
    if "months_in_reach" in schema:
        agg_exprs.append(pl.col("months_in_reach").mean().alias("avg_months_reach"))
    else:
        agg_exprs.append(pl.lit(0.0).alias("avg_months_reach"))

    if "months_in_mssp" in schema:
        agg_exprs.append(pl.col("months_in_mssp").mean().alias("avg_months_mssp"))
    else:
        agg_exprs.append(pl.lit(0.0).alias("avg_months_mssp"))

    if "months_in_reach" in schema and "months_in_mssp" in schema:
        agg_exprs.append((pl.col("months_in_reach") + pl.col("months_in_mssp")).mean().alias("avg_total_months"))
    else:
        agg_exprs.append(pl.lit(0.0).alias("avg_total_months"))

    if "has_program_transition" in schema:
        agg_exprs.append(pl.col("has_program_transition").sum().alias("transitions"))
    else:
        agg_exprs.append(pl.lit(0).alias("transitions"))

    # Overall vintage distribution
    vintage_distribution = (
        vintage_df.group_by("vintage_cohort").agg(agg_exprs).collect().sort("vintage_cohort")
    )

    # Calculate percentages
    total_enrolled = vintage_distribution.filter(pl.col("vintage_cohort") != "Never Enrolled")["count"].sum()

    vintage_distribution = vintage_distribution.with_columns(
        build_vintage_distribution_derived_metrics(total_enrolled)
    )

    return vintage_distribution


def calculate_office_vintage_distribution(
    vintage_df: pl.LazyFrame, most_recent_ym: str | None = None, office_column: str | None = None
) -> pl.DataFrame | None:
    """
    Calculate vintage cohort statistics by office.

    Args:
        vintage_df: LazyFrame with vintage cohort data (includes office columns)
        most_recent_ym: Most recent year-month string (e.g., "202401")
        office_column: Column name for office grouping (auto-detects if None)

    Returns:
        DataFrame with vintage distribution by office:
            - office_name/office_location: Office identifier
            - office_location: Office location (if both columns exist)
            - vintage_cohort: Cohort category
            - count: Number in cohort at this office
            - currently_enrolled: Currently enrolled in either program
            - current_reach: Currently in REACH
            - current_mssp: Currently in MSSP
            - avg_months_reach: Average months in REACH
            - avg_months_mssp: Average months in MSSP
            - avg_total_months: Average total aligned months
            - transitions: Count with program transitions
            - pct_of_office_enrolled: % of office enrolled in this cohort
            - pct_currently_enrolled: % of cohort currently enrolled
            - pct_in_reach: % of cohort in REACH
            - pct_in_mssp: % of cohort in MSSP
            - pct_with_transitions: % with transitions
        Returns None if most_recent_ym not provided
    """
    if not most_recent_ym:
        return None

    schema = vintage_df.collect_schema().names()

    # Auto-detect office column if not specified
    if office_column is None:
        if "office_name" in schema:
            office_column = "office_name"
        elif "office_location" in schema:
            office_column = "office_location"
        else:
            return None

    # Check if office columns exist
    if office_column not in schema:
        return None

    current_reach_col = f"ym_{most_recent_ym}_reach"
    current_mssp_col = f"ym_{most_recent_ym}_mssp"

    # Check if temporal columns exist
    if current_reach_col not in schema or current_mssp_col not in schema:
        return None

    # Build aggregations
    agg_exprs = [
        pl.len().alias("count"),
        (pl.col(current_reach_col) | pl.col(current_mssp_col)).sum().alias("currently_enrolled"),
        pl.col(current_reach_col).sum().alias("current_reach"),
        pl.col(current_mssp_col).sum().alias("current_mssp"),
    ]

    # Add optional metrics
    if "months_in_reach" in schema:
        agg_exprs.append(pl.col("months_in_reach").mean().alias("avg_months_reach"))
    else:
        agg_exprs.append(pl.lit(0.0).alias("avg_months_reach"))

    if "months_in_mssp" in schema:
        agg_exprs.append(pl.col("months_in_mssp").mean().alias("avg_months_mssp"))
    else:
        agg_exprs.append(pl.lit(0.0).alias("avg_months_mssp"))

    if "months_in_reach" in schema and "months_in_mssp" in schema:
        agg_exprs.append((pl.col("months_in_reach") + pl.col("months_in_mssp")).mean().alias("avg_total_months"))
    else:
        agg_exprs.append(pl.lit(0.0).alias("avg_total_months"))

    if "has_program_transition" in schema:
        agg_exprs.append(pl.col("has_program_transition").sum().alias("transitions"))
    else:
        agg_exprs.append(pl.lit(0).alias("transitions"))

    # Group by columns
    group_cols = [office_column, "vintage_cohort"]
    if "office_location" in schema and office_column != "office_location":
        group_cols.insert(1, "office_location")

    # Office-level vintage distribution
    office_vintage_distribution = (
        vintage_df.filter(pl.col(office_column).is_not_null())
        .group_by(group_cols)
        .agg(agg_exprs)
        .collect()
        .sort([office_column, "vintage_cohort"])
    )

    # Calculate percentages within each office
    office_totals_group = [office_column]
    if "office_location" in schema and office_column != "office_location":
        office_totals_group.append("office_location")

    office_totals = (
        office_vintage_distribution.filter(pl.col("vintage_cohort") != "Never Enrolled")
        .group_by(office_totals_group)
        .agg(pl.col("count").sum().alias("office_total_enrolled"))
    )

    office_vintage_distribution = (
        office_vintage_distribution.join(office_totals, on=office_totals_group, how="left")
        .with_columns(build_office_vintage_distribution_derived_metrics())
    )

    return office_vintage_distribution
