# © 2025 HarmonyCares
# All rights reserved.

"""
Alignment trends calculations for notebook.

Provides transforms for calculating enrollment trends over time,
showing how REACH and MSSP enrollment changes across months.
"""

import polars as pl

_BENEFICIARY_ID_COLUMNS = ("current_mbi", "bene_mbi", "person_id", "member_id", "mbi")
_FRESHNESS_COLUMNS = ("processed_at", "lineage_processed_at")


def _beneficiary_id_column(schema: list[str]) -> str | None:
    """Return the best beneficiary identifier column available in the frame."""
    return next((col for col in _BENEFICIARY_ID_COLUMNS if col in schema), None)


def _beneficiary_count_frame(df: pl.LazyFrame, schema: list[str]) -> pl.LazyFrame:
    """Return one beneficiary row for counts when an identifier is available."""
    beneficiary_col = _beneficiary_id_column(schema)
    if beneficiary_col is None:
        return df

    beneficiary = pl.col(beneficiary_col)
    valid_beneficiary = beneficiary.is_not_null() & (
        beneficiary.cast(pl.String).str.strip_chars() != ""
    )
    count_df = df.filter(valid_beneficiary)

    freshness_col = next((col for col in _FRESHNESS_COLUMNS if col in schema), None)
    if freshness_col is not None:
        count_df = count_df.sort([beneficiary_col, freshness_col])

    return count_df.unique(subset=[beneficiary_col], keep="last")


def calculate_alignment_trends_over_time(
    df: pl.LazyFrame, year_months: list[str]
) -> pl.DataFrame | None:
    """
    Calculate alignment trends over time.

    Tracks REACH and MSSP enrollment counts across all observable months
    to show enrollment trends over time.

    Args:
        df: LazyFrame with consolidated alignment data
        year_months: List of year-month strings to analyze (e.g., ["202401", "202402"])

    Returns:
        DataFrame with trends data containing columns:
            - year_month: Formatted as "YYYY-MM"
            - REACH: Count enrolled in REACH for that month
            - MSSP: Count enrolled in MSSP for that month
            - Total Aligned: Count enrolled in either program
        Returns None if no year_months provided
    """
    if not year_months:
        return None

    schema = df.collect_schema().names()
    valid_months = [
        (ym, f"ym_{ym}_reach", f"ym_{ym}_mssp")
        for ym in year_months
        if f"ym_{ym}_reach" in schema and f"ym_{ym}_mssp" in schema
    ]
    if not valid_months:
        return None

    count_df = _beneficiary_count_frame(df, schema).collect()

    # Calculate counts for each month in the observable window
    trend_data = []

    for ym, reach_col, mssp_col in valid_months:
        reach_expr = pl.col(reach_col).fill_null(False)
        mssp_expr = pl.col(mssp_col).fill_null(False)
        aligned_expr = reach_expr | mssp_expr
        month_counts = count_df.select(
            [
                reach_expr.sum().alias("reach"),
                mssp_expr.sum().alias("mssp"),
                aligned_expr.sum().alias("total_aligned"),
            ]
        )

        trend_data.append(
            {
                "year_month": f"{ym[:4]}-{ym[4:6]}",
                "REACH": int(month_counts["reach"][0]),
                "MSSP": int(month_counts["mssp"][0]),
                "Total Aligned": int(month_counts["total_aligned"][0]),
            }
        )

    trends = pl.DataFrame(trend_data)
    program_columns = [
        column
        for column in ("REACH", "MSSP")
        if column in trends.columns and trends.select(pl.col(column).sum()).item() > 0
    ]
    if not program_columns:
        program_columns = ["Total Aligned"] if "Total Aligned" in trends.columns else []

    if program_columns:
        active_rows = trends.with_row_index("_row").filter(
            pl.all_horizontal([pl.col(column) > 0 for column in program_columns])
        )
        if not active_rows.is_empty():
            first_row = int(active_rows["_row"][0])
            last_row = int(active_rows["_row"][-1])
            trends = trends.slice(first_row, last_row - first_row + 1)

    return trends
