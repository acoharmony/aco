# © 2025 HarmonyCares
# All rights reserved.

"""
Office-level statistics expressions for notebook calculations.

Provides reusable Polars expressions for calculating office-level enrollment,
alignment, and program distribution metrics.
"""

import polars as pl


def build_office_enrollment_aggregations(yearmo: str, df_schema: list[str]) -> list[pl.Expr]:
    """
    Build aggregation expressions for office enrollment statistics.

    Calculates per-office:
    - Total beneficiaries
    - REACH enrollment count
    - MSSP enrollment count
    - FFS count
    - Valid SVA count
    - Penetration rates

    Args:
        yearmo: Year-month string (e.g., "202401")
        df_schema: List of column names in the dataframe

    Returns:
        list[pl.Expr]: List of aggregation expressions
    """
    reach_col = f"ym_{yearmo}_reach"
    mssp_col = f"ym_{yearmo}_mssp"
    ffs_col = f"ym_{yearmo}_ffs"

    exprs = [
        pl.len().alias("total_beneficiaries"),
    ]

    # Add enrollment counts if columns exist
    if reach_col in df_schema:
        exprs.append(pl.col(reach_col).sum().alias("reach_count"))
    else:
        exprs.append(pl.lit(0).alias("reach_count"))

    if mssp_col in df_schema:
        exprs.append(pl.col(mssp_col).sum().alias("mssp_count"))
    else:
        exprs.append(pl.lit(0).alias("mssp_count"))

    if ffs_col in df_schema:
        exprs.append(pl.col(ffs_col).sum().alias("ffs_count"))
    else:
        exprs.append(pl.lit(0).alias("ffs_count"))

    # Add SVA count if column exists
    if "has_valid_voluntary_alignment" in df_schema:
        exprs.append(pl.col("has_valid_voluntary_alignment").sum().alias("valid_sva_count"))
    else:
        exprs.append(pl.lit(0).alias("valid_sva_count"))

    # Note: Derived metrics will be added in a separate .with_columns() call
    # after aggregation, since they reference columns created in the aggregation

    return exprs


def build_office_enrollment_derived_metrics() -> list[pl.Expr]:
    """
    Build derived metric expressions for office enrollment statistics.

    These metrics reference columns created during aggregation, so they must
    be applied in a separate .with_columns() call after .agg().

    Returns:
        list[pl.Expr]: List of derived metric expressions
    """
    return [
        (pl.col("reach_count") + pl.col("mssp_count")).alias("total_aco"),
        (pl.col("reach_count") / pl.col("total_beneficiaries") * 100).alias("reach_penetration"),
        (pl.col("mssp_count") / pl.col("total_beneficiaries") * 100).alias("mssp_penetration"),
        ((pl.col("reach_count") + pl.col("mssp_count")) / pl.col("total_beneficiaries") * 100).alias("aco_penetration"),
        (pl.col("valid_sva_count") / pl.col("total_beneficiaries") * 100).alias("sva_penetration"),
    ]


def build_office_alignment_type_aggregations(yearmo: str, df_schema: list[str]) -> list[pl.Expr]:
    """
    Build aggregation expressions for office alignment type breakdown.

    Calculates per-office:
    - Voluntary alignment count (valid SVA)
    - Claims-based alignment count
    - Total aligned count

    Args:
        yearmo: Year-month string (e.g., "202401")
        df_schema: List of column names in the dataframe

    Returns:
        list[pl.Expr]: List of aggregation expressions
    """
    reach_col = f"ym_{yearmo}_reach"
    mssp_col = f"ym_{yearmo}_mssp"

    # Check if columns exist
    has_reach = reach_col in df_schema
    has_mssp = mssp_col in df_schema
    has_valid_sva = "has_valid_voluntary_alignment" in df_schema

    # Build alignment filter
    if has_reach and has_mssp:
        aligned_expr = pl.col(reach_col) | pl.col(mssp_col)
    elif has_reach:
        aligned_expr = pl.col(reach_col)
    elif has_mssp:
        aligned_expr = pl.col(mssp_col)
    else:
        aligned_expr = pl.lit(False)

    exprs = [
        pl.len().alias("total_beneficiaries"),
        aligned_expr.sum().alias("total_aligned"),
    ]

    if has_valid_sva:
        exprs.extend([
            (aligned_expr & pl.col("has_valid_voluntary_alignment")).sum().alias("voluntary_count"),
            (aligned_expr & ~pl.col("has_valid_voluntary_alignment")).sum().alias("claims_count"),
        ])
    else:
        exprs.extend([
            pl.lit(0).alias("voluntary_count"),
            aligned_expr.sum().alias("claims_count"),
        ])

    # Note: Percentages will be added in a separate .with_columns() call
    # after aggregation, since they reference columns created in the aggregation

    return exprs


def build_office_alignment_type_derived_metrics() -> list[pl.Expr]:
    """
    Build derived metric expressions for office alignment type statistics.

    These metrics reference columns created during aggregation, so they must
    be applied in a separate .with_columns() call after .agg().

    Returns:
        list[pl.Expr]: List of derived metric expressions
    """
    return [
        (pl.col("voluntary_count") / pl.col("total_aligned") * 100).alias("voluntary_pct"),
        (pl.col("claims_count") / pl.col("total_aligned") * 100).alias("claims_pct"),
    ]


def build_office_program_distribution_aggregations(yearmo: str, df_schema: list[str]) -> list[pl.Expr]:
    """
    Build aggregation expressions for office program distribution.

    Calculates per-office counts for:
    - REACH only
    - MSSP only
    - Both REACH and MSSP (transitions)
    - Neither (not enrolled)

    Args:
        yearmo: Year-month string (e.g., "202401")
        df_schema: List of column names in the dataframe

    Returns:
        list[pl.Expr]: List of aggregation expressions
    """
    reach_col = f"ym_{yearmo}_reach"
    mssp_col = f"ym_{yearmo}_mssp"

    # Check if columns exist
    has_reach = reach_col in df_schema
    has_mssp = mssp_col in df_schema

    exprs = [pl.len().alias("total_beneficiaries")]

    if has_reach and has_mssp:
        # Use ever_reach and ever_mssp to determine historical program participation
        if "ever_reach" in df_schema and "ever_mssp" in df_schema:
            exprs.extend([
                (pl.col("ever_reach") & ~pl.col("ever_mssp")).sum().alias("reach_only_count"),
                (~pl.col("ever_reach") & pl.col("ever_mssp")).sum().alias("mssp_only_count"),
                (pl.col("ever_reach") & pl.col("ever_mssp")).sum().alias("both_programs_count"),
                (~pl.col("ever_reach") & ~pl.col("ever_mssp")).sum().alias("neither_count"),
            ])
        else:
            # Fall back to current month
            exprs.extend([
                (pl.col(reach_col) & ~pl.col(mssp_col)).sum().alias("reach_only_count"),
                (~pl.col(reach_col) & pl.col(mssp_col)).sum().alias("mssp_only_count"),
                (pl.col(reach_col) & pl.col(mssp_col)).sum().alias("both_programs_count"),
                (~pl.col(reach_col) & ~pl.col(mssp_col)).sum().alias("neither_count"),
            ])
    elif has_reach:
        exprs.extend([
            pl.col(reach_col).sum().alias("reach_only_count"),
            pl.lit(0).alias("mssp_only_count"),
            pl.lit(0).alias("both_programs_count"),
            (~pl.col(reach_col)).sum().alias("neither_count"),
        ])
    elif has_mssp:
        exprs.extend([
            pl.lit(0).alias("reach_only_count"),
            pl.col(mssp_col).sum().alias("mssp_only_count"),
            pl.lit(0).alias("both_programs_count"),
            (~pl.col(mssp_col)).sum().alias("neither_count"),
        ])
    else:
        exprs.extend([
            pl.lit(0).alias("reach_only_count"),
            pl.lit(0).alias("mssp_only_count"),
            pl.lit(0).alias("both_programs_count"),
            pl.len().alias("neither_count"),
        ])

    return exprs


def build_office_transition_aggregations(df_schema: list[str]) -> list[pl.Expr]:
    """
    Build aggregation expressions for office transition statistics.

    Calculates per-office:
    - Beneficiaries with program transitions (ever in both REACH and MSSP)
    - Continuous enrollment count
    - Average months in REACH
    - Average months in MSSP
    - Average total aligned months

    Args:
        df_schema: List of column names in the dataframe

    Returns:
        list[pl.Expr]: List of aggregation expressions
    """
    exprs = [pl.len().alias("total_beneficiaries")]

    if "has_program_transition" in df_schema:
        exprs.append(pl.col("has_program_transition").sum().alias("transitioned_count"))
    else:
        exprs.append(pl.lit(0).alias("transitioned_count"))

    if "has_continuous_enrollment" in df_schema:
        exprs.append(pl.col("has_continuous_enrollment").sum().alias("continuous_count"))
    else:
        exprs.append(pl.lit(0).alias("continuous_count"))

    if "months_in_reach" in df_schema:
        exprs.append(pl.col("months_in_reach").mean().alias("avg_months_reach"))
    else:
        exprs.append(pl.lit(0.0).alias("avg_months_reach"))

    if "months_in_mssp" in df_schema:
        exprs.append(pl.col("months_in_mssp").mean().alias("avg_months_mssp"))
    else:
        exprs.append(pl.lit(0.0).alias("avg_months_mssp"))

    if "total_aligned_months" in df_schema:
        exprs.append(pl.col("total_aligned_months").mean().alias("avg_total_months"))
    else:
        exprs.append(pl.lit(0.0).alias("avg_total_months"))

    return exprs


def build_office_transition_derived_metrics() -> list[pl.Expr]:
    """
    Build derived metric expressions for office transition statistics.

    These metrics reference columns created during aggregation, so they must
    be applied in a separate .with_columns() call after .agg().

    Returns:
        list[pl.Expr]: List of derived metric expressions
    """
    return [
        (pl.col("transitioned_count") / pl.col("total_beneficiaries") * 100).alias("transition_pct"),
        (pl.col("continuous_count") / pl.col("total_beneficiaries") * 100).alias("continuous_pct"),
    ]
