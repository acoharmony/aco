# © 2025 HarmonyCares
# All rights reserved.

"""
Alignment transition calculations for notebook.

Provides transforms for calculating program transitions between months,
month-over-month comparisons, and enrollment pattern analysis.
"""

import polars as pl

from .._expressions._enrollment_status import build_living_beneficiary_expr


def calculate_alignment_transitions(
    df_enriched: pl.LazyFrame, selected_ym: str | None, year_months: list[str]
) -> tuple[pl.DataFrame | None, str | None, str | None]:
    """
    Calculate alignment transitions between consecutive months.

    Analyzes beneficiary movement between programs:
    - FFS → REACH/MSSP (new enrollments)
    - REACH ↔ MSSP (program switches)
    - REACH/MSSP → FFS (disenrollments)
    - Program retention (no change)

    Args:
        df_enriched: LazyFrame with enriched alignment data
        selected_ym: Selected year-month string (e.g., "202401")
        year_months: List of year-month strings

    Returns:
        Tuple of (transition_stats, prev_ym, curr_ym):
            - transition_stats: DataFrame with transition types and counts
            - prev_ym: Previous year-month string
            - curr_ym: Current year-month string
        Returns (None, None, None) if insufficient data
    """
    if not selected_ym or not year_months:
        return None, None, None

    # Find the current month index
    try:
        curr_idx = year_months.index(selected_ym)
    except ValueError:
        return None, None, None

    # Calculate transitions if we have a previous month
    if curr_idx > 0:
        prev_ym = year_months[curr_idx - 1]
        curr_ym = selected_ym

        schema = df_enriched.collect_schema().names()

        # Build column names
        prev_reach = f"ym_{prev_ym}_reach"
        prev_mssp = f"ym_{prev_ym}_mssp"
        prev_ffs = f"ym_{prev_ym}_ffs"
        curr_reach = f"ym_{curr_ym}_reach"
        curr_mssp = f"ym_{curr_ym}_mssp"
        curr_ffs = f"ym_{curr_ym}_ffs"

        # Check if all required columns exist
        required_cols = [prev_reach, prev_mssp, prev_ffs, curr_reach, curr_mssp, curr_ffs]
        if not all(col in schema for col in required_cols):
            return None, None, None

        # Calculate transitions
        transition_stats = (
            df_enriched.select(
                [
                    pl.when((pl.col(prev_ffs)) & (pl.col(curr_reach)))
                    .then(pl.lit("FFS → REACH"))
                    .when((pl.col(prev_ffs)) & (pl.col(curr_mssp)))
                    .then(pl.lit("FFS → MSSP"))
                    .when((pl.col(prev_reach)) & (pl.col(curr_reach)))
                    .then(pl.lit("REACH → REACH"))
                    .when((pl.col(prev_reach)) & (pl.col(curr_mssp)))
                    .then(pl.lit("REACH → MSSP"))
                    .when((pl.col(prev_reach)) & (pl.col(curr_ffs)))
                    .then(pl.lit("REACH → None"))
                    .when((pl.col(prev_mssp)) & (pl.col(curr_mssp)))
                    .then(pl.lit("MSSP → MSSP"))
                    .when((pl.col(prev_mssp)) & (pl.col(curr_reach)))
                    .then(pl.lit("MSSP → REACH"))
                    .when((pl.col(prev_mssp)) & (pl.col(curr_ffs)))
                    .then(pl.lit("MSSP → None"))
                    .otherwise(pl.lit("FFS → FFS"))
                    .alias("transition_type")
                ]
            )
            .group_by("transition_type")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .collect()
        )

        return transition_stats, prev_ym, curr_ym
    else:
        return None, None, None


def calculate_month_over_month_comparison(
    df_enriched: pl.LazyFrame, selected_ym: str | None, year_months: list[str]
) -> dict[str, int | float | str] | None:
    """
    Calculate month-over-month comparison metrics.

    Compares enrollment counts between selected month and previous month,
    including REACH, MSSP, FFS, and voluntary alignment changes.

    Args:
        df_enriched: LazyFrame with enriched alignment data
        selected_ym: Selected year-month string (e.g., "202401")
        year_months: List of year-month strings

    Returns:
        dict with comparison metrics:
            - reach_change: Change in REACH enrollment
            - mssp_change: Change in MSSP enrollment
            - ffs_change: Change in FFS count
            - total_aco_change: Change in total ACO enrollment
            - reach_pct_change: % change in REACH
            - mssp_pct_change: % change in MSSP
            - voluntary_change: Change in voluntary alignment count
            - claims_change: Change in claims-based count
            - prev_month: Previous month formatted as "YYYY-MM"
            - curr_month: Current month formatted as "YYYY-MM"
        Returns None if insufficient data
    """
    if not selected_ym or not year_months:
        return None

    try:
        curr_idx = year_months.index(selected_ym)
    except ValueError:
        return None

    if curr_idx == 0:
        return None

    prev_ym = year_months[curr_idx - 1]
    schema = df_enriched.collect_schema().names()

    # Current month columns
    curr_reach = f"ym_{selected_ym}_reach"
    curr_mssp = f"ym_{selected_ym}_mssp"
    curr_ffs = f"ym_{selected_ym}_ffs"

    # Previous month columns
    prev_reach = f"ym_{prev_ym}_reach"
    prev_mssp = f"ym_{prev_ym}_mssp"
    prev_ffs = f"ym_{prev_ym}_ffs"

    # Check if all required columns exist
    required_cols = [curr_reach, curr_mssp, curr_ffs, prev_reach, prev_mssp, prev_ffs]
    if not all(col in schema for col in required_cols):
        return None

    # Filter for living beneficiaries only
    living_expr = build_living_beneficiary_expr(schema)
    df_active = df_enriched.filter(living_expr)

    # Calculate current month stats
    curr_stats = df_active.select(
        [
            pl.col(curr_reach).sum().alias("reach_current"),
            pl.col(curr_mssp).sum().alias("mssp_current"),
            pl.col(curr_ffs).sum().alias("ffs_current"),
            (pl.col(curr_reach) | pl.col(curr_mssp)).sum().alias("total_aco_current"),
        ]
    ).collect()

    # Calculate previous month stats
    prev_stats = df_active.select(
        [
            pl.col(prev_reach).sum().alias("reach_previous"),
            pl.col(prev_mssp).sum().alias("mssp_previous"),
            pl.col(prev_ffs).sum().alias("ffs_previous"),
            (pl.col(prev_reach) | pl.col(prev_mssp)).sum().alias("total_aco_previous"),
        ]
    ).collect()

    # Calculate changes
    reach_change = int(curr_stats["reach_current"][0] - prev_stats["reach_previous"][0])
    mssp_change = int(curr_stats["mssp_current"][0] - prev_stats["mssp_previous"][0])
    ffs_change = int(curr_stats["ffs_current"][0] - prev_stats["ffs_previous"][0])
    total_aco_change = int(curr_stats["total_aco_current"][0] - prev_stats["total_aco_previous"][0])

    reach_pct_change = (
        (reach_change / prev_stats["reach_previous"][0] * 100) if prev_stats["reach_previous"][0] > 0 else 0.0
    )
    mssp_pct_change = (
        (mssp_change / prev_stats["mssp_previous"][0] * 100) if prev_stats["mssp_previous"][0] > 0 else 0.0
    )

    comparison_data = {
        "reach_change": reach_change,
        "mssp_change": mssp_change,
        "ffs_change": ffs_change,
        "total_aco_change": total_aco_change,
        "reach_pct_change": reach_pct_change,
        "mssp_pct_change": mssp_pct_change,
        "prev_month": f"{prev_ym[:4]}-{prev_ym[4:]}",
        "curr_month": f"{selected_ym[:4]}-{selected_ym[4:]}",
    }

    # Calculate voluntary alignment changes if columns exist
    if "has_valid_voluntary_alignment" in schema and "has_voluntary_alignment" in schema:
        voluntary_comparison = df_enriched.select(
            [
                # Current voluntary alignment
                (pl.col(curr_reach) & pl.col("has_valid_voluntary_alignment")).sum().alias("voluntary_current"),
                (pl.col(curr_reach) & ~pl.col("has_valid_voluntary_alignment")).sum().alias("claims_current"),
                # Previous voluntary alignment (approximation)
                (pl.col(prev_reach) & pl.col("has_voluntary_alignment")).sum().alias("voluntary_previous"),
                (pl.col(prev_reach) & ~pl.col("has_voluntary_alignment")).sum().alias("claims_previous"),
            ]
        ).collect()

        comparison_data.update(
            {
                "voluntary_change": int(
                    voluntary_comparison["voluntary_current"][0] - voluntary_comparison["voluntary_previous"][0]
                ),
                "claims_change": int(
                    voluntary_comparison["claims_current"][0] - voluntary_comparison["claims_previous"][0]
                ),
            }
        )

    return comparison_data


def analyze_enrollment_patterns(
    df: pl.LazyFrame, df_enriched: pl.LazyFrame, selected_ym: str | None
) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    """
    Analyze enrollment patterns - current vs historical.

    Calculates enrollment continuity, program transitions, average months enrolled,
    and enrollment gaps for both all-time (historical) and currently aligned beneficiaries.

    Args:
        df: LazyFrame with consolidated alignment data
        df_enriched: LazyFrame with enriched alignment data
        selected_ym: Selected year-month string (optional)

    Returns:
        Tuple of (historical_enrollment, current_enrollment):
            - historical_enrollment: DataFrame with all-time enrollment pattern metrics
            - current_enrollment: DataFrame with current month enrollment metrics (None if no selected_ym)
    """
    schema = df.collect_schema().names()

    # Build aggregations for historical enrollment
    hist_agg_exprs = []

    if "has_continuous_enrollment" in schema:
        hist_agg_exprs.append(pl.col("has_continuous_enrollment").sum().alias("continuous_count"))
    else:
        hist_agg_exprs.append(pl.lit(0).alias("continuous_count"))

    if "has_program_transition" in schema:
        hist_agg_exprs.append(pl.col("has_program_transition").sum().alias("transition_count"))
    else:
        hist_agg_exprs.append(pl.lit(0).alias("transition_count"))

    if "months_in_reach" in schema:
        hist_agg_exprs.append(pl.col("months_in_reach").mean().alias("avg_reach_months"))
    else:
        hist_agg_exprs.append(pl.lit(0.0).alias("avg_reach_months"))

    if "months_in_mssp" in schema:
        hist_agg_exprs.append(pl.col("months_in_mssp").mean().alias("avg_mssp_months"))
    else:
        hist_agg_exprs.append(pl.lit(0.0).alias("avg_mssp_months"))

    if "total_aligned_months" in schema:
        hist_agg_exprs.append(pl.col("total_aligned_months").mean().alias("avg_total_months"))
    else:
        hist_agg_exprs.append(pl.lit(0.0).alias("avg_total_months"))

    if "enrollment_gaps" in schema:
        hist_agg_exprs.append(pl.col("enrollment_gaps").mean().alias("avg_gaps"))
    else:
        hist_agg_exprs.append(pl.lit(0.0).alias("avg_gaps"))

    # HISTORICAL enrollment patterns (all-time)
    historical_enrollment = df.select(hist_agg_exprs).collect()

    # CURRENT enrollment patterns (only those aligned in selected month)
    current_enrollment = None
    if selected_ym:
        reach_col = f"ym_{selected_ym}_reach"
        mssp_col = f"ym_{selected_ym}_mssp"

        enriched_schema = df_enriched.collect_schema().names()

        if reach_col in enriched_schema and mssp_col in enriched_schema:
            # Filter for actively aligned beneficiaries (alive)
            living_expr = build_living_beneficiary_expr(enriched_schema)
            currently_aligned_enroll = df_enriched.filter((pl.col(reach_col) | pl.col(mssp_col)) & living_expr)

            # Build aggregations for current enrollment
            curr_agg_exprs = list(hist_agg_exprs)  # Use same expressions
            curr_agg_exprs.append(pl.len().alias("current_total"))

            current_enrollment = currently_aligned_enroll.select(curr_agg_exprs).collect()

    return historical_enrollment, current_enrollment
