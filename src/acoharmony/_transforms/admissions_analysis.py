# © 2025 HarmonyCares
# All rights reserved.

"""
Comprehensive admissions analysis.

 detailed analysis of hospital admissions including:
- Acute inpatient admissions
- Emergency department visits
- Observation stays
- Admission rates per 1000 members
- Length of stay analysis
- Top admission diagnoses and procedures
- Facility-level analysis
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.admissions_analysis")


@transform(name="admissions_analysis", tier=["gold"])
class AdmissionsAnalysisTransform:
    """
    Comprehensive admissions analysis across all encounter types.

        Analyzes:
        - Acute inpatient admissions (IP)
        - Emergency department visits (ED)
        - Observation stays (OBS)
        - Rates, utilization, trends
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_inpatient_admissions(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Identify acute inpatient admissions.

                Bill type codes:
                - 11x: Inpatient hospital

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with inpatient admissions
        """
        logger.info("Identifying inpatient admissions...")

        inpatient = claims.filter(
            (pl.col("claim_type") == "institutional")
            & (pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11"))
            & pl.col("admission_date").is_not_null()
            & pl.col("discharge_date").is_not_null()
        ).select(
            [
                pl.col("person_id"),
                pl.col("claim_id"),
                pl.lit("inpatient").alias("encounter_type"),
                pl.col("admission_date"),
                pl.col("discharge_date"),
                pl.col("diagnosis_code_1").alias("principal_diagnosis"),
                pl.col("procedure_code_1").alias("principal_procedure"),
                pl.col("facility_npi"),
                pl.col("paid_amount"),
                pl.col("allowed_amount"),
            ]
        )

        inpatient = inpatient.with_columns(
            [
                (pl.col("discharge_date") - pl.col("admission_date"))
                .dt.total_days()
                .cast(pl.Int64)
                .alias("length_of_stay")
            ]
        )

        logger.info(f"Identified {inpatient.collect().height:,} inpatient admissions")

        return inpatient

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_ed_visits(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Identify emergency department visits.

                Identifies ED visits using:
                - Revenue codes 045x, 0981
                - Bill type 13x (outpatient hospital)
                - Place of service 23 (emergency room)

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with ED visits
        """
        logger.info("Identifying ED visits...")

        ed_revenue = claims.filter(
            (pl.col("claim_type") == "institutional")
            & (
                pl.col("revenue_code").cast(pl.Utf8).str.starts_with("045")
                | (pl.col("revenue_code").cast(pl.Utf8) == "0981")
            )
        )

        ed_bill_type = claims.filter(
            (pl.col("claim_type") == "institutional")
            & (pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("13"))
            & (pl.col("place_of_service_code") == "23")
        )

        ed_visits = pl.concat([ed_revenue, ed_bill_type]).unique(subset=["claim_id"])

        ed_visits = ed_visits.select(
            [
                pl.col("person_id"),
                pl.col("claim_id"),
                pl.lit("emergency_department").alias("encounter_type"),
                pl.col("claim_start_date").alias("admission_date"),
                pl.col("claim_end_date").alias("discharge_date"),
                pl.col("diagnosis_code_1").alias("principal_diagnosis"),
                pl.col("procedure_code_1").alias("principal_procedure"),
                pl.col("facility_npi"),
                pl.col("paid_amount"),
                pl.col("allowed_amount"),
            ]
        )

        ed_visits = ed_visits.with_columns([pl.lit(0).cast(pl.Int64).alias("length_of_stay")])

        logger.info(f"Identified {ed_visits.collect().height:,} ED visits")

        return ed_visits

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_observation_stays(claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Identify observation stays.

                Observation stays are identified by:
                - Revenue code 0762
                - Bill type 13x (outpatient)
                - Typically short stays (<2 days)

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with observation stays
        """
        logger.info("Identifying observation stays...")

        observation = claims.filter(
            (pl.col("claim_type") == "institutional")
            & (pl.col("revenue_code").cast(pl.Utf8) == "0762")
        ).select(
            [
                pl.col("person_id"),
                pl.col("claim_id"),
                pl.lit("observation").alias("encounter_type"),
                pl.col("claim_start_date").alias("admission_date"),
                pl.col("claim_end_date").alias("discharge_date"),
                pl.col("diagnosis_code_1").alias("principal_diagnosis"),
                pl.col("procedure_code_1").alias("principal_procedure"),
                pl.col("facility_npi"),
                pl.col("paid_amount"),
                pl.col("allowed_amount"),
            ]
        )

        observation = observation.with_columns(
            [
                (pl.col("discharge_date") - pl.col("admission_date"))
                .dt.total_days()
                .cast(pl.Int64)
                .alias("length_of_stay")
            ]
        )

        logger.info(f"Identified {observation.collect().height:,} observation stays")

        return observation

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_admission_rates(
        admissions: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate admission rates per 1000 members.

                Args:
                    admissions: All admissions (IP + ED + OBS)
                    eligibility: Member eligibility data
                    config: Configuration dict

                Returns:
                    LazyFrame with rates by encounter type
        """
        logger.info("Calculating admission rates...")

        measurement_year = config.get("measurement_year", 2024)

        admission_counts = admissions.group_by("encounter_type").agg(
            [pl.count().alias("admission_count")]
        )

        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
        )

        total_members = eligible_members.select(
            pl.col("person_id").n_unique().alias("member_count")
        )

        eligible_members = eligible_members.with_columns(
            [
                (
                    pl.when(pl.col("enrollment_end_date").dt.year() == measurement_year)
                    .then(pl.col("enrollment_end_date").dt.month())
                    .otherwise(12)
                    - pl.when(pl.col("enrollment_start_date").dt.year() == measurement_year)
                    .then(pl.col("enrollment_start_date").dt.month())
                    .otherwise(1)
                    + 1
                ).alias("member_months")
            ]
        )

        total_member_months = eligible_members.select(
            pl.sum("member_months").alias("total_member_months")
        )

        rates = admission_counts.join(total_members, how="cross").join(
            total_member_months, how="cross"
        )

        rates = rates.with_columns(
            [
                ((pl.col("admission_count") / pl.col("member_count")) * 1000).alias(
                    "admissions_per_1000"
                ),
                ((pl.col("admission_count") / pl.col("total_member_months")) * 1000).alias(
                    "admissions_per_1000_mm"
                ),
                ((pl.col("admission_count") / pl.col("total_member_months")) * 12).alias(
                    "admissions_per_member_per_year"
                ),
            ]
        )

        logger.info("Admission rates calculated")

        return rates

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def analyze_top_diagnoses(
        admissions: pl.LazyFrame, config: dict[str, Any], top_n: int = 20
    ) -> pl.LazyFrame:
        """
        Analyze top admission diagnoses.

                Args:
                    admissions: All admissions
                    config: Configuration dict
                    top_n: Number of top diagnoses to return

                Returns:
                    LazyFrame with top diagnoses by encounter type
        """
        logger.info(f"Analyzing top {top_n} admission diagnoses...")

        top_diagnoses = (
            admissions.group_by(["encounter_type", "principal_diagnosis"])
            .agg([pl.count().alias("admission_count"), pl.sum("paid_amount").alias("total_paid")])
            .sort(["encounter_type", "admission_count"], descending=[False, True])
        )

        top_diagnoses = top_diagnoses.with_columns(
            [
                pl.col("admission_count")
                .rank(method="ordinal", descending=True)
                .over("encounter_type")
                .alias("rank")
            ]
        ).filter(pl.col("rank") <= top_n)

        logger.info("Top diagnoses analysis complete")

        return top_diagnoses

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def analyze_by_facility(admissions: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Analyze admissions by facility.

                Args:
                    admissions: All admissions
                    config: Configuration dict

                Returns:
                    LazyFrame with facility-level metrics
        """
        logger.info("Analyzing admissions by facility...")

        facility_analysis = admissions.group_by(["facility_npi", "encounter_type"]).agg(
            [
                pl.count().alias("admission_count"),
                pl.col("length_of_stay").mean().alias("avg_length_of_stay"),
                pl.sum("paid_amount").alias("total_paid"),
                pl.sum("allowed_amount").alias("total_allowed"),
                pl.col("person_id").n_unique().alias("unique_patients"),
            ]
        )

        facility_analysis = facility_analysis.with_columns(
            [(pl.col("total_paid") / pl.col("admission_count")).alias("avg_cost_per_admission")]
        )

        logger.info("Facility analysis complete")

        return facility_analysis

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def calculate_comprehensive_admissions(
        claims: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Calculate comprehensive admissions analysis.

                Args:
                    claims: Medical claims
                    eligibility: Member eligibility
                    config: Configuration dict

                Returns:
                    Tuple of (all_admissions, rates, top_diagnoses, facility_analysis, summary)
        """
        logger.info("Starting comprehensive admissions analysis...")

        inpatient = AdmissionsAnalysisTransform.identify_inpatient_admissions(claims, config)
        ed_visits = AdmissionsAnalysisTransform.identify_ed_visits(claims, config)
        observation = AdmissionsAnalysisTransform.identify_observation_stays(claims, config)

        all_admissions = pl.concat([inpatient, ed_visits, observation])

        rates = AdmissionsAnalysisTransform.calculate_admission_rates(
            all_admissions, eligibility, config
        )

        top_diagnoses = AdmissionsAnalysisTransform.analyze_top_diagnoses(
            all_admissions, config, top_n=20
        )

        facility_analysis = AdmissionsAnalysisTransform.analyze_by_facility(all_admissions, config)

        summary = all_admissions.group_by("encounter_type").agg(
            [
                pl.count().alias("total_admissions"),
                pl.col("person_id").n_unique().alias("unique_patients"),
                pl.col("length_of_stay").mean().alias("avg_length_of_stay"),
                pl.col("length_of_stay").median().alias("median_length_of_stay"),
                pl.sum("paid_amount").alias("total_paid"),
                pl.mean("paid_amount").alias("avg_paid_per_admission"),
            ]
        )

        logger.info("Comprehensive admissions analysis complete")

        return all_admissions, rates, top_diagnoses, facility_analysis, summary


logger.debug("Registered admissions analysis expression")
