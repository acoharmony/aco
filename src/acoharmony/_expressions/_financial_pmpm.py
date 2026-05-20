# © 2025 HarmonyCares
# All rights reserved.

"""
Financial PMPM (Per-Member-Per-Month) analytics expression builder.

 expressions for calculating PMPM healthcare spend by service
category. PMPM is a key metric for value-based care organizations to track costs
and identify high-cost populations.

The logic:
1. Calculate member months from eligibility
2. Categorize medical and pharmacy spend by service type
3. Aggregate spend by patient, time period, and category
4. Divide total spend by member months

References:
- Value-Based Care PMPM Methodologies
- Healthcare Service Category Classification

"""

from typing import Any

import polars as pl

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


@register_expression(
    "financial_pmpm",
    schemas=["silver", "gold"],
    dataset_types=["claims", "eligibility"],
    description="Per-Member-Per-Month financial analytics by service category",
)
class FinancialPmpmExpression:
    """
    Generate expressions for PMPM cost analytics.

        Configuration Structure:
            ```yaml
            financial_pmpm:
              # Time aggregation
              time_period: month  # 'month', 'quarter', 'year'

              # Service categories
              service_categories:
                - inpatient_facility
                - outpatient_facility
                - professional
                - pharmacy
                - dme

              # Amount fields
              paid_amount_column: paid_amount
              allowed_amount_column: allowed_amount

              # Column mappings
              patient_id_column: patient_id
              service_date_column: claim_end_date
            ```
    """

    @traced()
    @explain(
        why="Build failed",
        how="Check configuration and input data are valid",
        causes=["Invalid config", "Missing required fields", "Data processing error"],
    )
    @timeit(log_level="debug")
    @staticmethod
    def build(config: dict[str, Any]) -> dict[str, Any]:
        """Build PMPM calculation expressions."""
        time_period = config.get("time_period", "month")
        paid_col = config.get("paid_amount_column", "paid_amount")
        allowed_col = config.get("allowed_amount_column", "allowed_amount")
        patient_id_col = config.get("patient_id_column", "patient_id")
        service_date_col = config.get("service_date_column", "claim_end_date")

        config_with_defaults = {
            "time_period": time_period,
            "paid_amount_column": paid_col,
            "allowed_amount_column": allowed_col,
            "patient_id_column": patient_id_col,
            "service_date_column": service_date_col,
        }

        expressions = {
            "calculate_member_months": {
                "description": "Calculate member months from eligibility spans",
            },
            "categorize_spend": {
                "description": "Map claims to service categories",
                "lookup_table": "reference_data_service_category",
            },
            "aggregate_spend": {
                "description": "Sum spend by patient, period, category",
            },
            "calculate_pmpm": {
                "description": "Divide total spend by member months",
                "formula": pl.col("total_spend") / pl.col("member_months"),
            },
        }

        return {"expressions": expressions, "config": config_with_defaults}

    @staticmethod
    def transform_patient_spend_by_category(
        medical_claims: pl.LazyFrame,
        pharmacy_claims: pl.LazyFrame,
        member_months: pl.LazyFrame,
        service_categories: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Calculate patient spend by service category.

                Returns:
                    LazyFrame with columns:
                    - patient_id
                    - year_month
                    - service_category
                    - paid_amount
                    - allowed_amount
                    - claim_count
        """
        patient_id_col = config.get("patient_id_column", "patient_id")
        paid_col = config.get("paid_amount_column", "paid_amount")
        service_date_col = config.get("service_date_column", "claim_end_date")

        medical_spend = medical_claims.select(
            [
                pl.col(patient_id_col),
                pl.col(service_date_col).dt.strftime("%Y-%m").alias("year_month"),
                pl.col(paid_col),
                pl.lit("medical").alias("service_category"),
            ]
        )

        pharmacy_spend = pharmacy_claims.select(
            [
                pl.col(patient_id_col),
                pl.col(service_date_col).dt.strftime("%Y-%m").alias("year_month"),
                pl.col(paid_col),
                pl.lit("pharmacy").alias("service_category"),
            ]
        )

        all_spend = pl.concat([medical_spend, pharmacy_spend], how="vertical")

        spend_summary = all_spend.group_by([patient_id_col, "year_month", "service_category"]).agg(
            [
                pl.col(paid_col).sum().alias("paid_amount"),
                pl.count().alias("claim_count"),
            ]
        )

        return spend_summary

    @staticmethod
    def transform_pmpm_by_payer(
        patient_spend: pl.LazyFrame, member_months: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate PMPM rates by aggregating across patients.

                Returns:
                    LazyFrame with columns:
                    - year_month
                    - service_category
                    - total_spend
                    - total_member_months
                    - pmpm
                    - patient_count
        """
        patient_id_col = config.get("patient_id_column", "patient_id")

        with_member_months = patient_spend.join(
            member_months, on=[patient_id_col, "year_month"], how="left"
        )

        pmpm = with_member_months.group_by(["year_month", "service_category"]).agg(
            [
                pl.col("paid_amount").sum().alias("total_spend"),
                pl.col("member_months").sum().alias("total_member_months"),
                pl.col(patient_id_col).n_unique().alias("patient_count"),
            ]
        )

        pmpm = pmpm.with_columns(
            [(pl.col("total_spend") / pl.col("total_member_months")).alias("pmpm")]
        )

        return pmpm

    @staticmethod
    def calculate_member_months(
        alignment_df: pl.LazyFrame,
        year_month_col: str = "year_month",
        person_id_col: str = "person_id",
    ) -> pl.LazyFrame:
        """
        Calculate member-months from alignment data.

        Args:
            alignment_df: Alignment data in long format with year_month and person_id
            year_month_col: Column name for year-month (YYYYMM format)
            person_id_col: Column name for person identifier

        Returns:
            LazyFrame with columns: year_month, program, member_months
        """
        return (
            alignment_df.group_by([year_month_col, "program"])
            .agg([pl.col(person_id_col).n_unique().alias("member_months")])
            .sort([year_month_col, "program"])
        )

    @staticmethod
    def calculate_pmpm_by_category(
        spend_df: pl.LazyFrame,
        member_months_df: pl.LazyFrame,
        year_month_col: str = "year_month",
        category_col: str = "service_category_2",
        spend_col: str = "paid",
    ) -> pl.LazyFrame:
        """
        Calculate PMPM by service category and program.

        Args:
            spend_df: Spend data with year_month, program, category, and spend amount
            member_months_df: Member-months by year_month and program
            year_month_col: Column name for year-month
            category_col: Column name for service category
            spend_col: Column name for spend amount

        Returns:
            LazyFrame with columns: year_month, program, category, total_spend, member_months, pmpm
        """
        # Aggregate spend by year_month, program, and category
        spend_agg = (
            spend_df.group_by([year_month_col, "program", category_col])
            .agg([pl.col(spend_col).sum().alias("total_spend")])
        )

        # Join with member-months and calculate PMPM
        return (
            spend_agg.join(
                member_months_df,
                on=[year_month_col, "program"],
                how="left",
            )
            .with_columns(
                [
                    # PMPM = total_spend / member_months
                    (
                        pl.when(pl.col("member_months") > 0)
                        .then(pl.col("total_spend") / pl.col("member_months"))
                        .otherwise(0.0)
                    ).alias("pmpm")
                ]
            )
            .select([year_month_col, "program", category_col, "total_spend", "member_months", "pmpm"])
        )

    @staticmethod
    def format_year_month(year_month_int: pl.Expr) -> pl.Expr:
        """
        Format integer year-month (YYYYMM) as string (YYYY-MM).

        Args:
            year_month_int: Integer expression in YYYYMM format

        Returns:
            String expression in YYYY-MM format
        """
        return (
            year_month_int.cast(pl.Utf8).str.slice(0, 4)
            + pl.lit("-")
            + year_month_int.cast(pl.Utf8).str.slice(4, 2)
        )

    @staticmethod
    def unpivot_alignment_to_long(
        alignment_df: pl.LazyFrame,
        person_id_col: str = "person_id",
    ) -> pl.LazyFrame:
        """
        Convert wide-format alignment (ym_YYYYMM_program columns) to long format.

        Args:
            alignment_df: Alignment data with person_id and ym_YYYYMM_program boolean columns
            person_id_col: Column name for person identifier

        Returns:
            LazyFrame with columns: person_id, year_month, program (one row per person-month-program)

        Note:
            Only includes ym_*_reach, ym_*_mssp, and ym_*_ffs columns.
            Excludes ym_*_first_claim which is a flag, not a program.
        """
        # Get all ym_* columns but exclude first_claim (it's a flag, not a program)
        all_cols = alignment_df.collect_schema().names()
        ym_cols = [
            c for c in all_cols
            if c.startswith("ym_")
            and (c.endswith("_reach") or c.endswith("_mssp") or c.endswith("_ffs"))
        ]

        # Unpivot program columns only
        unpivoted = alignment_df.unpivot(
            index=[person_id_col],
            on=ym_cols,
            variable_name="ym_program",
            value_name="enrolled",
        )

        # Filter to only enrolled (True) rows
        unpivoted = unpivoted.filter(pl.col("enrolled"))

        # Parse year_month and program from column name (ym_YYYYMM_program)
        return unpivoted.with_columns(
            [
                # Extract YYYYMM from ym_YYYYMM_program
                pl.col("ym_program").str.slice(3, 6).cast(pl.Int32).alias("year_month"),
                # Extract program name (everything after ym_YYYYMM_)
                pl.col("ym_program").str.slice(10).alias("program"),
            ]
        ).select([person_id_col, "year_month", "program"])
