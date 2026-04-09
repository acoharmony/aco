# © 2025 HarmonyCares
# All rights reserved.

"""
Financial PMPM by Category Transform.

Calculates Per Member Per Month (PMPM) spend by service category and ACO program.
This is a core analytics metric for value-based care performance tracking.

Inputs (gold):
    - service_category.parquet - Claims categorized by service type
    - consolidated_alignment.parquet - ACO program enrollment by member-month

Outputs (gold):
    - financial_pmpm_by_category.parquet - PMPM spend by program and category
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced
from .._expressions._financial_pmpm import FinancialPmpmExpression
from ..medallion import MedallionLayer


@composable
@traced()
@timeit(log_level="info", threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Calculate PMPM by service category and ACO program.

    Logic:
        1. Load service_category (spend by claim)
        2. Load consolidated_alignment (program enrollment by person-month)
        3. Unpivot alignment from wide (ym_YYYYMM_program) to long format
        4. Join spend with alignment to add program labels
        5. Calculate member-months by program and year-month
        6. Aggregate spend by program, year-month, and category
        7. Calculate PMPM = total_spend / member_months
    """
    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load inputs
    service_category = pl.scan_parquet(gold_path / "service_category.parquet")
    consolidated_alignment = pl.scan_parquet(gold_path / "consolidated_alignment.parquet")

    # Convert alignment from wide to long format
    # Input: current_mbi, ym_202301_reach, ym_202301_mssp, ym_202301_ffs, ...
    # Output: current_mbi (renamed to person_id), year_month, program
    alignment_long = FinancialPmpmExpression.unpivot_alignment_to_long(
        consolidated_alignment, person_id_col="current_mbi"
    ).rename({"current_mbi": "person_id"})

    # Extract year_month from claim dates (YYYY-MM-DD -> YYYYMM)
    spend_with_ym = service_category.with_columns(
        [
            (
                pl.col("claim_start_date").dt.year() * 100
                + pl.col("claim_start_date").dt.month()
            ).alias("year_month")
        ]
    )

    # Join spend with alignment to tag each claim with its program
    spend_with_program = spend_with_ym.join(
        alignment_long,
        on=["person_id", "year_month"],
        how="inner",  # Only include claims for members with known program
    )

    # Calculate member-months by program and year-month
    member_months = FinancialPmpmExpression.calculate_member_months(
        alignment_long, year_month_col="year_month", person_id_col="person_id"
    )

    # Calculate PMPM by category
    pmpm_by_category = FinancialPmpmExpression.calculate_pmpm_by_category(
        spend_with_program,
        member_months,
        year_month_col="year_month",
        category_col="service_category_2",
        spend_col="paid",
    )

    # Format year_month as YYYY-MM for readability
    result = pmpm_by_category.with_columns(
        [FinancialPmpmExpression.format_year_month(pl.col("year_month")).alias("month")]
    ).select(
        [
            "month",
            "year_month",
            "program",
            pl.col("service_category_2").alias("category"),
            "total_spend",
            "member_months",
            "pmpm",
        ]
    )

    return result
