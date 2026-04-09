# © 2025 HarmonyCares
# All rights reserved.

"""
Behavioral Health Analysis Transform.

Provides comprehensive behavioral health tracking:
- Mental health diagnosis identification
- Substance use disorder tracking
- Behavioral health service utilization
- Treatment engagement metrics
- Crisis event tracking
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.behavioral_health")


@transform(name="behavioral_health", tier=["gold"])
class BehavioralHealthTransform:
    """
    Comprehensive behavioral health tracking and analysis.
    """

    # ICD-10 code ranges for behavioral health conditions
    BEHAVIORAL_HEALTH_CODES = {
        # Mental health disorders
        "depression": ["F32", "F33"],  # Major depressive disorder, recurrent
        "anxiety": ["F40", "F41"],  # Phobic/anxiety disorders
        "bipolar": ["F31"],  # Bipolar disorder
        "schizophrenia": ["F20"],  # Schizophrenia
        "ptsd": ["F43.1"],  # Post-traumatic stress disorder
        "adhd": ["F90"],  # Attention-deficit hyperactivity disorder
        "eating_disorders": ["F50"],  # Eating disorders
        "personality_disorders": ["F60"],  # Personality disorders
        # Substance use disorders
        "alcohol_use": ["F10"],  # Alcohol related disorders
        "opioid_use": ["F11"],  # Opioid related disorders
        "cannabis_use": ["F12"],  # Cannabis related disorders
        "stimulant_use": ["F14", "F15"],  # Cocaine, stimulant disorders
        "tobacco_use": ["F17"],  # Nicotine dependence
        "other_substance_use": ["F13", "F16", "F18", "F19"],  # Other substance use
        # Crisis/severe conditions
        "suicidal_ideation": ["R45.851"],  # Suicidal ideation
        "self_harm": [
            "X71",
            "X72",
            "X73",
            "X74",
            "X75",
            "X76",
            "X77",
            "X78",
            "X79",
            "X80",
            "X81",
            "X82",
            "X83",
            "X84",
        ],
    }

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_behavioral_health_conditions(
        claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Identify behavioral health conditions from diagnosis codes.

                Args:
                    claims: Medical claims
                    config: Configuration dict

                Returns:
                    LazyFrame with behavioral health condition identifications
        """
        logger.info("Identifying behavioral health conditions...")

        measurement_year = config.get("measurement_year", 2024)

        # Filter to measurement year
        bh_claims = claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)

        # Check all diagnosis positions
        bh_claims = bh_claims.filter(
            pl.col("diagnosis_code_1")
            .cast(pl.Utf8)
            .str.contains("^(F1|F2|F3|F4|F5|F6|R45|X7|X8)")
            | pl.col("diagnosis_code_2")
            .cast(pl.Utf8)
            .str.contains("^(F1|F2|F3|F4|F5|F6|R45|X7|X8)")
            | pl.col("diagnosis_code_3")
            .cast(pl.Utf8)
            .str.contains("^(F1|F2|F3|F4|F5|F6|R45|X7|X8)")
        )

        # Get primary behavioral health diagnosis
        bh_claims = bh_claims.with_columns(
            [
                pl.when(
                    pl.col("diagnosis_code_1")
                    .cast(pl.Utf8)
                    .str.contains("^(F1|F2|F3|F4|F5|F6|R45|X7|X8)")
                )
                .then(pl.col("diagnosis_code_1"))
                .when(
                    pl.col("diagnosis_code_2")
                    .cast(pl.Utf8)
                    .str.contains("^(F1|F2|F3|F4|F5|F6|R45|X7|X8)")
                )
                .then(pl.col("diagnosis_code_2"))
                .when(
                    pl.col("diagnosis_code_3")
                    .cast(pl.Utf8)
                    .str.contains("^(F1|F2|F3|F4|F5|F6|R45|X7|X8)")
                )
                .then(pl.col("diagnosis_code_3"))
                .alias("bh_diagnosis_code")
            ]
        )

        logger.info(f"Identified {bh_claims.collect().height:,} behavioral health claims")

        return bh_claims

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def categorize_behavioral_health_conditions(
        bh_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Categorize behavioral health conditions.

                Args:
                    bh_claims: Claims with behavioral health diagnoses
                    config: Configuration dict

                Returns:
                    LazyFrame with behavioral health categories
        """
        logger.info("Categorizing behavioral health conditions...")

        # Categorize conditions
        categorized = bh_claims.with_columns(
            [
                # Mental health disorders
                pl.when(
                    pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F32")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F33")
                )
                .then(pl.lit("depression"))
                .when(
                    pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F40")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F41")
                )
                .then(pl.lit("anxiety"))
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F31"))
                .then(pl.lit("bipolar"))
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F20"))
                .then(pl.lit("schizophrenia"))
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F43.1"))
                .then(pl.lit("ptsd"))
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F90"))
                .then(pl.lit("adhd"))
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F50"))
                .then(pl.lit("eating_disorder"))
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F60"))
                .then(pl.lit("personality_disorder"))
                # Substance use disorders
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F10"))
                .then(pl.lit("alcohol_use_disorder"))
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F11"))
                .then(pl.lit("opioid_use_disorder"))
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F12"))
                .then(pl.lit("cannabis_use_disorder"))
                .when(
                    pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F14")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F15")
                )
                .then(pl.lit("stimulant_use_disorder"))
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F17"))
                .then(pl.lit("tobacco_use_disorder"))
                .when(
                    pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F13")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F16")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F18")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F19")
                )
                .then(pl.lit("other_substance_use_disorder"))
                # Crisis
                .when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("R45.851"))
                .then(pl.lit("suicidal_ideation"))
                .when(
                    pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("X7")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("X8")
                )
                .then(pl.lit("self_harm"))
                .otherwise(pl.lit("other_behavioral_health"))
                .alias("bh_condition_category"),
                # High-level grouping
                pl.when(pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F1"))
                .then(pl.lit("substance_use_disorder"))
                .when(
                    pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F2")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F3")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("F4")
                )
                .then(pl.lit("mental_health_disorder"))
                .when(
                    pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("R45.851")
                    | pl.col("bh_diagnosis_code").cast(pl.Utf8).str.starts_with("X")
                )
                .then(pl.lit("crisis"))
                .otherwise(pl.lit("other"))
                .alias("bh_high_level_category"),
            ]
        )

        logger.info("Behavioral health conditions categorized")

        return categorized

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_member_bh_profile(
        bh_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate member-level behavioral health profiles.

                Args:
                    bh_claims: Claims with behavioral health categories
                    config: Configuration dict

                Returns:
                    LazyFrame with member behavioral health profiles
        """
        logger.info("Calculating member behavioral health profiles...")

        # Aggregate by member
        member_profile = bh_claims.group_by("person_id").agg(
            [
                pl.col("bh_condition_category").unique().alias("bh_conditions"),
                pl.col("bh_condition_category").n_unique().alias("unique_bh_conditions"),
                pl.count().alias("bh_claim_count"),
                pl.col("claim_end_date").min().alias("first_bh_service_date"),
                pl.col("claim_end_date").max().alias("last_bh_service_date"),
                # Flag specific high-risk conditions
                pl.col("bh_condition_category")
                .is_in(["suicidal_ideation", "self_harm"])
                .any()
                .alias("has_crisis_condition"),
                pl.col("bh_high_level_category")
                .is_in(["substance_use_disorder"])
                .any()
                .alias("has_substance_use_disorder"),
                pl.col("bh_high_level_category")
                .is_in(["mental_health_disorder"])
                .any()
                .alias("has_mental_health_disorder"),
            ]
        )

        # Calculate comorbidity and engagement
        member_profile = member_profile.with_columns(
            [
                (pl.col("has_mental_health_disorder") & pl.col("has_substance_use_disorder")).alias(
                    "has_dual_diagnosis"
                ),
                (pl.col("bh_claim_count") >= 4).alias(
                    "has_engagement"
                ),  # 4+ visits suggests ongoing treatment
            ]
        )

        member_profile = member_profile.with_columns(
            [
                pl.when(pl.col("has_crisis_condition"))
                .then(pl.lit("high"))
                .when(pl.col("unique_bh_conditions") >= 3)
                .then(pl.lit("high"))
                .when(pl.col("has_dual_diagnosis"))
                .then(pl.lit("high"))
                .when(pl.col("unique_bh_conditions") >= 2)
                .then(pl.lit("moderate"))
                .otherwise(pl.lit("low"))
                .alias("bh_complexity"),
            ]
        )

        logger.info("Member behavioral health profiles calculated")

        return member_profile

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_service_utilization(
        bh_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate behavioral health service utilization.

                Args:
                    bh_claims: Claims with behavioral health categories
                    config: Configuration dict

                Returns:
                    LazyFrame with service utilization
        """
        logger.info("Calculating behavioral health service utilization...")

        # Categorize service types by place of service and bill type
        service_utilization = bh_claims.with_columns(
            [
                pl.when(pl.col("claim_type") == "institutional")
                .then(
                    pl.when(pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11"))
                    .then(pl.lit("inpatient_psychiatric"))
                    .when(pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("13"))
                    .then(pl.lit("outpatient_hospital"))
                    .when(pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("81"))
                    .then(pl.lit("inpatient_psychiatric"))
                    .otherwise(pl.lit("other_institutional"))
                )
                .when(
                    (pl.col("claim_type") == "professional")
                    & (pl.col("place_of_service_code") == "11")
                )
                .then(pl.lit("office_based"))
                .when(
                    (pl.col("claim_type") == "professional")
                    & (pl.col("place_of_service_code") == "22")
                )
                .then(pl.lit("outpatient_hospital"))
                .when(
                    (pl.col("claim_type") == "professional")
                    & (pl.col("place_of_service_code") == "23")
                )
                .then(pl.lit("emergency_department"))
                .when(
                    (pl.col("claim_type") == "professional")
                    & (pl.col("place_of_service_code") == "02")
                )
                .then(pl.lit("telehealth"))
                .otherwise(pl.lit("other_professional"))
                .alias("bh_service_type")
            ]
        )

        # Aggregate service utilization
        utilization = service_utilization.group_by(["person_id", "bh_service_type"]).agg(
            [
                pl.count().alias("visit_count"),
                pl.sum("paid_amount").alias("total_cost"),
            ]
        )

        logger.info("Behavioral health service utilization calculated")

        return utilization

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_condition_prevalence(
        bh_claims: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate prevalence of behavioral health conditions.

                Args:
                    bh_claims: Claims with behavioral health categories
                    config: Configuration dict

                Returns:
                    LazyFrame with condition prevalence
        """
        logger.info("Calculating condition prevalence...")

        # Count unique members per condition
        prevalence = bh_claims.group_by("bh_condition_category").agg(
            [
                pl.col("person_id").n_unique().alias("member_count"),
                pl.count().alias("total_claims"),
                pl.sum("paid_amount").alias("total_cost"),
            ]
        )

        # Sort by prevalence
        prevalence = prevalence.sort("member_count", descending=True)

        logger.info("Condition prevalence calculated")

        return prevalence

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def analyze_behavioral_health(
        claims: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Perform comprehensive behavioral health analysis.

                Args:
                    claims: Medical claims
                    eligibility: Member eligibility
                    config: Configuration dict

                Returns:
                    Tuple of (member_profile, service_utilization, condition_prevalence, bh_claims)
        """
        logger.info("Starting behavioral health analysis...")

        # Identify behavioral health conditions
        bh_claims = BehavioralHealthTransform.identify_behavioral_health_conditions(claims, config)

        # Categorize conditions
        bh_categorized = BehavioralHealthTransform.categorize_behavioral_health_conditions(
            bh_claims, config
        )

        # Calculate member profiles
        member_profile = BehavioralHealthTransform.calculate_member_bh_profile(
            bh_categorized, config
        )

        # Calculate service utilization
        service_utilization = BehavioralHealthTransform.calculate_service_utilization(
            bh_categorized, config
        )

        # Calculate condition prevalence
        condition_prevalence = BehavioralHealthTransform.calculate_condition_prevalence(
            bh_categorized, config
        )

        logger.info("Behavioral health analysis complete")

        return member_profile, service_utilization, condition_prevalence, bh_categorized


logger.debug("Registered behavioral health expression")
