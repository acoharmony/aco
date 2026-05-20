# © 2025 HarmonyCares
# All rights reserved.

"""
Cohort analysis calculations for notebook.

Provides transforms for calculating cohort retention patterns over time.
"""

import polars as pl


def calculate_cohort_analysis(df_enriched: pl.LazyFrame, year_months: list[str]) -> list[dict] | None:
    """
    Calculate cohort retention analysis for alignment patterns.

    Analyzes beneficiaries who were enrolled in a specific month (cohort)
    and tracks how many remain enrolled in subsequent months.

    Args:
        df_enriched: LazyFrame with enriched alignment data
        year_months: List of year-month strings (e.g., ["202401", "202402", ...])

    Returns:
        List of cohort dictionaries with retention metrics:
            - cohort: Cohort month (formatted as "YYYY-MM")
            - initial_size: Initial cohort size
            - month_0: Count at initial month (same as initial_size)
            - month_1, month_2, month_3: Counts retained at each subsequent month
            - retention_1, retention_2, retention_3: Retention % at each month
        Returns None if insufficient months (< 4) or no year_months provided
    """
    if not year_months or len(year_months) < 4:
        return None

    cohort_analysis = []
    schema = df_enriched.collect_schema().names()

    # Analyze last 6 months of cohorts (skip the current month for retention calculation)
    for cohort_month in year_months[-6:-1]:
        cohort_col_reach = f"ym_{cohort_month}_reach"
        cohort_col_mssp = f"ym_{cohort_month}_mssp"

        # Check if columns exist
        if cohort_col_reach not in schema or cohort_col_mssp not in schema:
            continue

        # Get cohort members (those who were enrolled in this month)
        cohort_members = df_enriched.filter(pl.col(cohort_col_reach) | pl.col(cohort_col_mssp))

        cohort_size = cohort_members.select(pl.len()).collect().item()

        if cohort_size > 0:
            cohort_data = {
                "cohort": f"{cohort_month[:4]}-{cohort_month[4:]}",
                "initial_size": cohort_size,
                "month_0": cohort_size,
            }

            # Calculate retention for each subsequent month (up to 3 months)
            month_idx = year_months.index(cohort_month)
            for i, future_month in enumerate(year_months[month_idx + 1 : month_idx + 4], 1):
                future_col_reach = f"ym_{future_month}_reach"
                future_col_mssp = f"ym_{future_month}_mssp"

                if future_col_reach not in schema or future_col_mssp not in schema:
                    break

                retained = (
                    cohort_members.filter(pl.col(future_col_reach) | pl.col(future_col_mssp))
                    .select(pl.len())
                    .collect()
                    .item()
                )

                cohort_data[f"month_{i}"] = retained
                cohort_data[f"retention_{i}"] = (
                    (retained / cohort_size * 100) if cohort_size > 0 else 0.0
                )

            cohort_analysis.append(cohort_data)

    if not cohort_analysis:
        return None

    return cohort_analysis
