# © 2025 HarmonyCares
# All rights reserved.

"""
Year-over-year transition analysis transform for ACO alignment pipeline.

Applies comprehensive transition expressions to identify beneficiaries lost
between years and categorize the reasons for loss. Idempotent and schema-driven.
"""

from datetime import date, datetime
from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._aco_transition_expired_sva import build_expired_sva_expr
from .._expressions._aco_transition_inactive import (
    build_inactive_expr,
    build_inactivity_duration_expr,
)
from .._expressions._aco_transition_lost_provider import build_lost_provider_expr
from .._expressions._aco_transition_moved_ma import (
    build_ma_enrollment_date_expr,
    build_moved_ma_expr,
)
from .._expressions._aco_transition_newly_added import (
    build_first_reach_month_current_year_expr,
    build_newly_added_expr,
    build_newly_added_source_expr,
)
from .._expressions._aco_transition_termed_bar import build_termed_bar_expr
from .._expressions._aco_transition_total_lost import build_total_lost_expr
from .._expressions._aco_transition_unresolved import (
    build_potential_unresolved_reasons_expr,
    build_transition_category_expr,
    build_unresolved_expr,
)


@transform(name="aco_alignment_transitions", tier=["silver", "gold"], sql_enabled=True)
@transform_method(enable_composition=True, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame,
    schema: dict,
    catalog: Any,
    logger: Any,
    force: bool = False,
    current_year: int | None = None,
    previous_year: int | None = None
) -> pl.LazyFrame:
    """
    Apply year-over-year transition analysis.

    This transform:
    1. Checks idempotency (_transitions_calculated flag)
    2. Determines years to compare (defaults to current vs previous)
    3. Applies all transition expressions:
       - Total lost
       - Termed on BAR
       - Expired SVA
       - Lost provider
       - Moved to MA
       - Inactive
       - Unresolved
    4. Categorizes each beneficiary's primary transition reason
    5. Marks as processed

    Args:
        df: Alignment LazyFrame with temporal columns and all prior transforms applied
        schema: Schema config
        catalog: Catalog instance (unused but required for signature)
        logger: Logger instance
        force: Force reprocessing
        current_year: Current year to analyze (defaults to this year)
        previous_year: Previous year to compare (defaults to current_year - 1)

    Returns:
        pl.LazyFrame: Transformed data with transition analysis columns
    """
    # Idempotency check
    if not force and "_transitions_calculated" in df.collect_schema().names():
        logger.info("Transitions already calculated, skipping")
        return df

    logger.info("Applying year-over-year transition analysis")

    # Determine years to compare
    # Use max observable_end from data to determine current year (evergreen)
    if current_year is None or previous_year is None:
        # Get the maximum year from temporal columns to determine data range
        schema_names = df.collect_schema().names()
        temporal_cols = [col for col in schema_names if col.startswith("ym_")]

        if temporal_cols:
            # Extract years from temporal column names (ym_YYYYMM_*)
            years_in_data = sorted({
                int(col.split("_")[1][:4])
                for col in temporal_cols
                if len(col.split("_")[1]) >= 4 and col.split("_")[1][:4].isdigit()
            })

            if current_year is None:
                current_year = years_in_data[-1]  # Most recent year
            if previous_year is None:
                previous_year = current_year - 1

            logger.info(f"Detected data range: {years_in_data[0]} to {years_in_data[-1]}")
        else:
            # Fallback to calendar year if no temporal columns found
            today = datetime.now()
            if current_year is None:
                current_year = today.year
            if previous_year is None:
                previous_year = current_year - 1

    logger.info(f"Analyzing transitions from {previous_year} to {current_year}")
    logger.info(f"  Comparing: December {previous_year} REACH BAR → {current_year} REACH enrollment")

    # Check if required temporal columns exist
    schema_names = df.collect_schema().names()
    prev_year_cols = [col for col in schema_names if col.startswith(f"ym_{previous_year}")]
    curr_year_cols = [col for col in schema_names if col.startswith(f"ym_{current_year}")]

    if not prev_year_cols:
        logger.warning(f"No temporal columns found for {previous_year}, skipping transition analysis")
        return df.with_columns([pl.lit(True).alias("_transitions_calculated")])

    if not curr_year_cols:
        logger.warning(f"No temporal columns found for {current_year}, skipping transition analysis")
        return df.with_columns([pl.lit(True).alias("_transitions_calculated")])

    logger.info(f"Found {len(prev_year_cols)} columns for {previous_year}, {len(curr_year_cols)} for {current_year}")

    # Detect available months for current year from schema
    curr_year_months = sorted({
        int(col.split("_")[1][4:6])
        for col in curr_year_cols
        if len(col.split("_")[1]) == 6  # Ensure it's YYYYMM format
    })
    logger.info(f"Available months in {current_year}: {curr_year_months}")

    # Apply all transition expressions
    result = df.with_columns([
        # 1. Total lost (foundational)
        build_total_lost_expr(current_year, previous_year, current_year_months=curr_year_months),

        # 2. Newly added (foundational)
        build_newly_added_expr(current_year, previous_year, current_year_months=curr_year_months),
        build_first_reach_month_current_year_expr(current_year, current_year_months=curr_year_months),

        # 3. Specific loss reasons
        build_termed_bar_expr(current_year, previous_year, current_year_months=curr_year_months),
        build_expired_sva_expr(current_year, previous_year, current_year_months=curr_year_months),
        build_lost_provider_expr(current_year, previous_year, current_year_months=curr_year_months),
        build_moved_ma_expr(current_year, previous_year, current_year_months=curr_year_months),
        build_inactive_expr(current_year, previous_year, reference_date=date.today(), current_year_months=curr_year_months),
    ])

    # 4. Unresolved must come after other reasons (depends on them)
    result = result.with_columns([
        build_unresolved_expr(current_year, previous_year, current_year_months=curr_year_months)
    ])

    # 5. Add newly added source (depends on newly_added flag)
    result = result.with_columns([
        build_newly_added_source_expr(current_year, previous_year)
    ])

    # 6. Add supporting metrics
    result = result.with_columns([
        build_inactivity_duration_expr(reference_date=date.today()),
        build_ma_enrollment_date_expr(),
        build_potential_unresolved_reasons_expr(),
    ])

    # 7. Categorize primary transition reason
    result = result.with_columns([
        build_transition_category_expr(previous_year, current_year)
    ])

    # 8. Add summary counts for reporting
    result = result.with_columns([
        pl.lit(current_year).alias("transition_analysis_current_year"),
        pl.lit(previous_year).alias("transition_analysis_previous_year"),
        pl.lit(datetime.now()).alias("transition_analysis_date"),
    ])

    # Mark as processed
    result = result.with_columns([pl.lit(True).alias("_transitions_calculated")])

    logger.info("Year-over-year transition analysis complete")

    return result


def calculate_transition_summary(
    df: pl.LazyFrame,
    previous_year: int
) -> pl.DataFrame:
    """
    Calculate summary statistics for transition analysis.

    Aggregates counts for each transition category to provide
    high-level metrics for reporting.

    Args:
        df: LazyFrame with transition analysis applied
        previous_year: Previous year analyzed

    Returns:
        pl.DataFrame: Summary statistics with counts per category
    """
    summary = (
        df
        .group_by(f"transition_category_{previous_year}")
        .agg([
            pl.len().alias("count"),
            pl.col("current_program").value_counts().alias("by_program")
        ])
        .sort("count", descending=True)
        .collect()
    )

    return summary
