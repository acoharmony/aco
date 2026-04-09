# © 2025 HarmonyCares
# All rights reserved.

"""
Total Cost of Care (TCOC) Analysis.

 comprehensive financial analysis including:
- Total cost of care by category
- Medical vs pharmacy spend
- Risk-adjusted PMPM calculations
- Trend analysis by period
- Cost concentration analysis (high-cost members)
- Service category breakdown with benchmarking
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.financial_total_cost")


@transform(name="financial_total_cost", tier=["gold"])
class FinancialTotalCostTransform:
    """
    Total Cost of Care (TCOC) analysis with risk adjustment.

        Provides:
        - TCOC metrics (medical + pharmacy)
        - Risk-adjusted PMPM
        - Cost concentration analysis
        - Trend analysis
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_member_months(eligibility: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Calculate member months for PMPM calculations.

                Args:
                    eligibility: Member eligibility data
                    config: Configuration dict with measurement_year

                Returns:
                    LazyFrame with person_id and member_months
        """
        logger.info("Calculating member months...")

        measurement_year = config.get("measurement_year", 2024)
        start_date = pl.date(measurement_year, 1, 1)
        end_date = pl.date(measurement_year, 12, 31)

        member_months = eligibility.with_columns(
            [
                pl.when(pl.col("enrollment_start_date") > start_date)
                .then(pl.col("enrollment_start_date"))
                .otherwise(start_date)
                .alias("effective_start"),
                pl.when(pl.col("enrollment_end_date") < end_date)
                .then(pl.col("enrollment_end_date"))
                .otherwise(end_date)
                .alias("effective_end"),
            ]
        )

        # Calculate months (approximate with days / 30.4)
        member_months = member_months.with_columns(
            [
                (
                    (pl.col("effective_end") - pl.col("effective_start"))
                    .dt.total_days()
                    .cast(pl.Float64)
                    / 30.4
                ).alias("member_months")
            ]
        )

        member_months = member_months.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
        ).select(["person_id", "member_months", "age", "gender"])

        logger.info("Member months calculated")

        return member_months

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def aggregate_costs_by_member(
        medical_claims: pl.LazyFrame, service_categories: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Aggregate medical costs by member and service category.

                Args:
                    medical_claims: Medical claims with costs
                    service_categories: Service category classification
                    config: Configuration dict

                Returns:
                    LazyFrame with member-level cost aggregations
        """
        logger.info("Aggregating medical costs by member...")

        measurement_year = config.get("measurement_year", 2024)

        claims_with_category = medical_claims.join(
            service_categories.select(["claim_id", "service_category_1", "service_category_2"]),
            on="claim_id",
            how="left",
        )

        claims_with_category = claims_with_category.filter(
            pl.col("claim_end_date").dt.year() == measurement_year
        )

        member_costs = claims_with_category.group_by(
            ["person_id", "service_category_1", "service_category_2"]
        ).agg(
            [
                pl.sum("paid_amount").alias("total_paid"),
                pl.sum("allowed_amount").alias("total_allowed"),
                pl.count().alias("claim_count"),
            ]
        )

        logger.info("Medical costs aggregated")

        return member_costs

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_pmpm(
        member_costs: pl.LazyFrame, member_months: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate PMPM (Per Member Per Month) metrics.

                Args:
                    member_costs: Member-level costs by category
                    member_months: Member months for denominator
                    config: Configuration dict

                Returns:
                    LazyFrame with PMPM metrics
        """
        logger.info("Calculating PMPM...")

        member_total_costs = member_costs.group_by("person_id").agg(
            [
                pl.sum("total_paid").alias("total_medical_cost"),
                pl.sum("claim_count").alias("total_claims"),
            ]
        )

        member_pmpm = member_months.join(member_total_costs, on="person_id", how="left")

        member_pmpm = member_pmpm.with_columns(
            [pl.col("total_medical_cost").fill_null(0), pl.col("total_claims").fill_null(0)]
        )

        member_pmpm = member_pmpm.with_columns(
            [(pl.col("total_medical_cost") / pl.col("member_months")).alias("pmpm_medical")]
        )

        logger.info("PMPM calculated")

        return member_pmpm

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_risk_adjustment(
        member_pmpm: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate risk-adjusted PMPM using age/gender factors.

                This is a simplified risk adjustment. In production, would use
                HCC risk scores or other risk adjustment methodology.

                Args:
                    member_pmpm: Member PMPM data
                    config: Configuration dict

                Returns:
                    LazyFrame with risk-adjusted PMPM
        """
        logger.info("Calculating risk adjustment...")

        # Simplified age/gender risk factors
        # In production, would join with HCC risk scores
        member_pmpm = member_pmpm.with_columns(
            [
                pl.when(pl.col("age") < 18)
                .then(0.5)
                .when(pl.col("age").is_between(18, 44))
                .then(1.0)
                .when(pl.col("age").is_between(45, 64))
                .then(1.5)
                .when(pl.col("age") >= 65)
                .then(2.5)
                .otherwise(1.0)
                .alias("age_factor"),
                pl.when(pl.col("gender").str.to_lowercase().is_in(["f", "female"]))
                .then(1.1)
                .otherwise(1.0)
                .alias("gender_factor"),
            ]
        )

        # Combined risk score
        member_pmpm = member_pmpm.with_columns(
            [(pl.col("age_factor") * pl.col("gender_factor")).alias("risk_score")]
        )

        # Risk-adjusted PMPM
        member_pmpm = member_pmpm.with_columns(
            [(pl.col("pmpm_medical") / pl.col("risk_score")).alias("risk_adjusted_pmpm")]
        )

        logger.info("Risk adjustment calculated")

        return member_pmpm

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_high_cost_members(
        member_pmpm: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Identify high-cost members (top decile).

                Args:
                    member_pmpm: Member PMPM data
                    config: Configuration dict

                Returns:
                    LazyFrame with high_cost flag
        """
        logger.info("Identifying high-cost members...")

        # Calculate percentiles
        p90_threshold = member_pmpm.select(
            pl.col("total_medical_cost").quantile(0.90).alias("p90_cost")
        ).collect()["p90_cost"][0]

        p95_threshold = member_pmpm.select(
            pl.col("total_medical_cost").quantile(0.95).alias("p95_cost")
        ).collect()["p95_cost"][0]

        p99_threshold = member_pmpm.select(
            pl.col("total_medical_cost").quantile(0.99).alias("p99_cost")
        ).collect()["p99_cost"][0]

        # Flag high-cost members
        member_pmpm = member_pmpm.with_columns(
            [
                (pl.col("total_medical_cost") >= p90_threshold).alias("top_10_pct"),
                (pl.col("total_medical_cost") >= p95_threshold).alias("top_5_pct"),
                (pl.col("total_medical_cost") >= p99_threshold).alias("top_1_pct"),
                pl.when(pl.col("total_medical_cost") >= p99_threshold)
                .then(pl.lit("top_1_pct"))
                .when(pl.col("total_medical_cost") >= p95_threshold)
                .then(pl.lit("top_5_pct"))
                .when(pl.col("total_medical_cost") >= p90_threshold)
                .then(pl.lit("top_10_pct"))
                .otherwise(pl.lit("other"))
                .alias("cost_tier"),
            ]
        )

        logger.info(
            f"High-cost thresholds: P90=${p90_threshold:,.2f}, P95=${p95_threshold:,.2f}, P99=${p99_threshold:,.2f}"
        )

        return member_pmpm

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def calculate_total_cost_of_care(
        medical_claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        service_categories: pl.LazyFrame,
        config: dict[str, Any],
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Calculate comprehensive total cost of care analysis.

                Args:
                    medical_claims: Medical claims
                    eligibility: Member eligibility
                    service_categories: Service category classification
                    config: Configuration dict

                Returns:
                    Tuple of (member_level_tcoc, summary_by_category, summary_by_cost_tier, overall_summary)
        """
        logger.info("Starting total cost of care analysis...")

        member_months = FinancialTotalCostTransform.calculate_member_months(eligibility, config)

        member_costs_by_category = FinancialTotalCostTransform.aggregate_costs_by_member(
            medical_claims, service_categories, config
        )

        member_pmpm = FinancialTotalCostTransform.calculate_pmpm(
            member_costs_by_category, member_months, config
        )

        member_pmpm = FinancialTotalCostTransform.calculate_risk_adjustment(member_pmpm, config)

        member_pmpm = FinancialTotalCostTransform.identify_high_cost_members(member_pmpm, config)

        summary_by_category = member_costs_by_category.group_by(
            ["service_category_1", "service_category_2"]
        ).agg(
            [
                pl.sum("total_paid").alias("total_cost"),
                pl.count().alias("member_count"),
                pl.sum("claim_count").alias("total_claims"),
            ]
        )

        summary_by_cost_tier = member_pmpm.group_by("cost_tier").agg(
            [
                pl.count().alias("member_count"),
                pl.sum("total_medical_cost").alias("total_cost"),
                pl.mean("pmpm_medical").alias("avg_pmpm"),
                pl.mean("risk_adjusted_pmpm").alias("avg_risk_adjusted_pmpm"),
                pl.mean("risk_score").alias("avg_risk_score"),
            ]
        )

        overall_summary = member_pmpm.select(
            [
                pl.count().alias("total_members"),
                pl.sum("member_months").alias("total_member_months"),
                pl.sum("total_medical_cost").alias("total_cost"),
                pl.mean("pmpm_medical").alias("avg_pmpm"),
                pl.mean("risk_adjusted_pmpm").alias("avg_risk_adjusted_pmpm"),
                pl.sum("total_claims").alias("total_claims"),
            ]
        )

        logger.info("Total cost of care analysis complete")

        return member_pmpm, summary_by_category, summary_by_cost_tier, overall_summary


logger.debug("Registered financial total cost expression")
