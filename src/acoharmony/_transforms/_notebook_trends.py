# © 2025 HarmonyCares
# All rights reserved.

"""
Alignment trends calculations for notebook.

Provides transforms for calculating enrollment trends over time,
showing how REACH and MSSP enrollment changes across months.
"""

import polars as pl


def calculate_alignment_trends_over_time(df: pl.LazyFrame, year_months: list[str]) -> pl.DataFrame | None:
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

    # Calculate counts for each month in the observable window
    trend_data = []

    for ym in year_months:
        reach_col = f"ym_{ym}_reach"
        mssp_col = f"ym_{ym}_mssp"

        # Check if columns exist
        if reach_col in schema and mssp_col in schema:
            month_counts = df.select(
                [
                    pl.col(reach_col).sum().alias("reach"),
                    pl.col(mssp_col).sum().alias("mssp"),
                    (pl.col(reach_col) | pl.col(mssp_col)).sum().alias("total_aligned"),
                ]
            ).collect()

            trend_data.append(
                {
                    "year_month": f"{ym[:4]}-{ym[4:6]}",
                    "REACH": int(month_counts["reach"][0]),
                    "MSSP": int(month_counts["mssp"][0]),
                    "Total Aligned": int(month_counts["total_aligned"][0]),
                }
            )

    if not trend_data:
        return None

    return pl.DataFrame(trend_data)
