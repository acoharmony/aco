# © 2025 HarmonyCares
# All rights reserved.

"""
Pharmacy Analysis and Utilization.

 comprehensive pharmacy analytics:
- Drug utilization by therapeutic class
- Medication costs and trends
- Generic vs brand analysis
- High-cost medication identification
- Polypharmacy detection
- Medication adherence metrics (PDC)
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.pharmacy_analysis")


@transform(name="pharmacy_analysis", tier=["gold"])
class PharmacyAnalysisTransform:
    """
    Comprehensive pharmacy utilization and cost analysis.
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_pharmacy_claims(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Identify pharmacy claims.

                Args:
                    claims: All claims (medical + pharmacy)
                    config: Configuration dict

                Returns:
                    LazyFrame with pharmacy claims
        """
        logger.info("Identifying pharmacy claims...")

        measurement_year = config.get("measurement_year", 2025)

        # Filter to pharmacy claims
        pharmacy = claims.filter(
            (pl.col("claim_type") == "pharmacy")
            & (pl.col("claim_end_date").dt.year() == measurement_year)
        ).select(
            [
                "person_id",
                "claim_id",
                "claim_start_date",
                "claim_end_date",
                pl.col("ndc_code"),
                pl.col("paid_amount"),
                pl.col("allowed_amount"),
                pl.col("quantity"),
                pl.col("days_supply"),
                pl.col("drug_name"),
                pl.col("prescribing_provider_npi"),
            ]
        )

        logger.info(f"Identified {pharmacy.collect().height:,} pharmacy claims")

        return pharmacy

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_member_drug_costs(
        pharmacy_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate member-level pharmacy costs.

                Args:
                    pharmacy_claims: Pharmacy claims
                    config: Configuration dict

                Returns:
                    LazyFrame with member pharmacy costs
        """
        logger.info("Calculating member pharmacy costs...")

        member_costs = pharmacy_claims.group_by("person_id").agg(
            [
                pl.count().alias("total_fills"),
                pl.sum("paid_amount").alias("total_pharmacy_cost"),
                pl.sum("days_supply").alias("total_days_supply"),
                pl.col("ndc_code").n_unique().alias("unique_medications"),
            ]
        )

        logger.info("Member pharmacy costs calculated")

        return member_costs

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_high_cost_medications(
        pharmacy_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Identify high-cost medications.

                Args:
                    pharmacy_claims: Pharmacy claims
                    config: Configuration dict

                Returns:
                    LazyFrame with high-cost medications
        """
        logger.info("Identifying high-cost medications...")

        # Aggregate by NDC
        drug_costs = pharmacy_claims.group_by(["ndc_code", "drug_name"]).agg(
            [
                pl.count().alias("fill_count"),
                pl.sum("paid_amount").alias("total_cost"),
                pl.mean("paid_amount").alias("avg_cost_per_fill"),
                pl.col("person_id").n_unique().alias("unique_members"),
            ]
        )

        # Flag high-cost (>$1000 per fill on average)
        high_cost = drug_costs.filter(pl.col("avg_cost_per_fill") >= 1000).sort(
            "total_cost", descending=True
        )

        logger.info(f"Identified {high_cost.collect().height:,} high-cost medications")

        return high_cost

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def detect_polypharmacy(member_costs: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Detect polypharmacy (members on many medications).

                Args:
                    member_costs: Member-level costs
                    config: Configuration dict

                Returns:
                    LazyFrame with polypharmacy flags
        """
        logger.info("Detecting polypharmacy...")

        # Flag polypharmacy (5+ unique medications)
        polypharmacy = member_costs.with_columns(
            [
                (pl.col("unique_medications") >= 10).alias("high_polypharmacy"),
                (pl.col("unique_medications").is_between(5, 9)).alias("moderate_polypharmacy"),
                pl.when(pl.col("unique_medications") >= 10)
                .then(pl.lit("high"))
                .when(pl.col("unique_medications") >= 5)
                .then(pl.lit("moderate"))
                .otherwise(pl.lit("low"))
                .alias("polypharmacy_risk"),
            ]
        )

        logger.info("Polypharmacy detection complete")

        return polypharmacy

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_pharmacy_pmpm(
        member_costs: pl.LazyFrame, member_months: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate pharmacy PMPM.

                Args:
                    member_costs: Member pharmacy costs
                    member_months: Member months
                    config: Configuration dict

                Returns:
                    LazyFrame with pharmacy PMPM
        """
        logger.info("Calculating pharmacy PMPM...")

        pmpm = member_costs.join(
            member_months.select(["person_id", "member_months"]), on="person_id", how="left"
        )

        pmpm = pmpm.with_columns(
            [(pl.col("total_pharmacy_cost") / pl.col("member_months")).alias("pharmacy_pmpm")]
        )

        logger.info("Pharmacy PMPM calculated")

        return pmpm

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def calculate_pharmacy_analytics(
        claims: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Calculate comprehensive pharmacy analytics.

                Args:
                    claims: All claims
                    eligibility: Member eligibility
                    config: Configuration dict

                Returns:
                    Tuple of (pharmacy_claims, member_costs, high_cost_drugs, summary)
        """
        logger.info("Starting pharmacy analytics...")

        pharmacy_claims = PharmacyAnalysisTransform.identify_pharmacy_claims(claims, config)

        member_costs = PharmacyAnalysisTransform.calculate_member_drug_costs(
            pharmacy_claims, config
        )

        member_costs = PharmacyAnalysisTransform.detect_polypharmacy(member_costs, config)

        high_cost_drugs = PharmacyAnalysisTransform.identify_high_cost_medications(
            pharmacy_claims, config
        )

        summary = pharmacy_claims.select(
            [
                pl.count().alias("total_fills"),
                pl.sum("paid_amount").alias("total_pharmacy_spend"),
                pl.mean("paid_amount").alias("avg_cost_per_fill"),
                pl.col("person_id").n_unique().alias("members_with_pharmacy"),
                pl.col("ndc_code").n_unique().alias("unique_medications"),
            ]
        )

        logger.info("Pharmacy analytics complete")

        return pharmacy_claims, member_costs, high_cost_drugs, summary


logger.debug("Registered pharmacy analysis expression")
