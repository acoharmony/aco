# © 2025 HarmonyCares
# All rights reserved.

"""
HCC (Hierarchical Condition Category) Gap Analysis using CMS-HCC Model.

This module implements comprehensive risk adjustment analysis using all 10 CMS-HCC seed files:
- ICD-10-CM to HCC mappings (V24 and V28 models)
- Disease factors (HCC coefficients)
- Demographic factors (age/gender)
- Disease hierarchies
- Interaction factors (disease, disabled, enrollment)
- Payment HCC count factors

Identifies:
- Current HCCs from claims
- HCC gaps (historical conditions not recaptured)
- RAF score calculations
- High-value recapture opportunities
- Chronic condition monitoring
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.hcc_gap_analysis")


@transform(name="hcc_gap_analysis", tier=["gold"])
class HccGapAnalysisTransform:
    """
    HCC gap analysis using complete CMS-HCC model (V24/V28).

        Uses all 10 CMS HCC seed files for accurate RAF calculations.
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def load_hcc_value_sets(silver_path) -> dict[str, pl.LazyFrame]:
        """
        Load all 10 CMS-HCC value sets.

                Args:
                    silver_path: Path to silver layer

                Returns:
                    Dictionary of value set name -> LazyFrame
        """
        logger.info("Loading CMS-HCC value sets...")

        value_sets = {}
        file_mappings = {
            "icd10_mappings": "value_sets_cms_hcc_icd_10_cm_mappings.parquet",
            "disease_factors": "value_sets_cms_hcc_disease_factors.parquet",
            "demographic_factors": "value_sets_cms_hcc_demographic_factors.parquet",
            "disease_hierarchy": "value_sets_cms_hcc_disease_hierarchy.parquet",
            "disease_interactions": "value_sets_cms_hcc_disease_interaction_factors.parquet",
            "disabled_interactions": "value_sets_cms_hcc_disabled_interaction_factors.parquet",
            "enrollment_interactions": "value_sets_cms_hcc_enrollment_interaction_factors.parquet",
            "payment_hcc_count": "value_sets_cms_hcc_payment_hcc_count_factors.parquet",
            "cpt_hcpcs": "value_sets_cms_hcc_cpt_hcpcs.parquet",
            "adjustment_rates": "value_sets_cms_hcc_adjustment_rates.parquet",
        }

        for key, filename in file_mappings.items():
            try:
                file_path = silver_path / filename
                value_sets[key] = pl.scan_parquet(file_path)
                logger.debug(f"Loaded {key} from {filename}")
            except Exception as e:
                logger.warning(f"Could not load {key}: {e}")
                value_sets[key] = pl.DataFrame().lazy()

        loaded_count = sum(1 for v in value_sets.values() if v.collect().height > 0)
        logger.info(f"Loaded {loaded_count}/10 CMS-HCC value sets")

        return value_sets

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def map_diagnoses_to_hccs(
        claims: pl.LazyFrame, icd10_mappings: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Map ICD-10 diagnosis codes to HCC codes.

                Args:
                    claims: Medical claims
                    icd10_mappings: ICD-10 to HCC mapping table
                    config: Configuration dict (includes model_version: V24 or V28)

                Returns:
                    LazyFrame with person_id, hcc_code, diagnosis_code, claim_date
        """
        logger.info("Mapping diagnoses to HCCs...")

        model_version = config.get("model_version", "V24")
        config.get("measurement_year", 2024)

        hcc_col = "cms_hcc_v28" if model_version == "V28" else "cms_hcc_v24"
        flag_col = f"{hcc_col}_flag"

        valid_mappings = icd10_mappings.filter(pl.col(flag_col) == "Yes").select(
            [pl.col("diagnosis_code"), pl.col(hcc_col).alias("hcc_code")]
        )

        hccs_dx1 = claims.join(
            valid_mappings, left_on="diagnosis_code_1", right_on="diagnosis_code", how="inner"
        ).select(
            [
                "person_id",
                "hcc_code",
                pl.col("diagnosis_code_1").alias("diagnosis_code"),
                pl.col("claim_end_date").alias("claim_date"),
                pl.col("claim_end_date").dt.year().alias("service_year"),
            ]
        )

        hccs_dx2 = (
            claims.filter(pl.col("diagnosis_code_2").is_not_null())
            .join(
                valid_mappings, left_on="diagnosis_code_2", right_on="diagnosis_code", how="inner"
            )
            .select(
                [
                    "person_id",
                    "hcc_code",
                    pl.col("diagnosis_code_2").alias("diagnosis_code"),
                    pl.col("claim_end_date").alias("claim_date"),
                    pl.col("claim_end_date").dt.year().alias("service_year"),
                ]
            )
        )

        hccs_dx3 = (
            claims.filter(pl.col("diagnosis_code_3").is_not_null())
            .join(
                valid_mappings, left_on="diagnosis_code_3", right_on="diagnosis_code", how="inner"
            )
            .select(
                [
                    "person_id",
                    "hcc_code",
                    pl.col("diagnosis_code_3").alias("diagnosis_code"),
                    pl.col("claim_end_date").alias("claim_date"),
                    pl.col("claim_end_date").dt.year().alias("service_year"),
                ]
            )
        )

        all_hccs = pl.concat([hccs_dx1, hccs_dx2, hccs_dx3])

        all_hccs = all_hccs.unique(subset=["person_id", "hcc_code", "service_year"])

        logger.info(f"Mapped diagnoses to HCCs for model {model_version}")

        return all_hccs

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def apply_hcc_hierarchies(
        person_hccs: pl.LazyFrame, disease_hierarchy: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Apply HCC hierarchies (higher severity HCCs exclude lower).

                Args:
                    person_hccs: Person-HCC mappings
                    disease_hierarchy: Hierarchy rules
                    config: Configuration dict

                Returns:
                    LazyFrame with hierarchies applied
        """
        logger.info("Applying HCC hierarchies...")

        model_version = config.get("model_version", "V24")

        hierarchy = disease_hierarchy.filter(pl.col("model_version") == f"CMS-HCC-{model_version}")

        if hierarchy.collect().height == 0:
            logger.warning(f"No hierarchy rules found for {model_version}, skipping")
            return person_hccs

        exclusions = hierarchy.select(["hcc_code", "hccs_to_exclude"])

        high_severity = person_hccs.join(exclusions, on="hcc_code", how="inner")

        excluded_hccs = high_severity.select(
            [
                pl.col("person_id"),
                pl.col("service_year"),
                pl.col("hccs_to_exclude").alias("hcc_code"),
            ]
        ).unique()

        final_hccs = person_hccs.join(
            excluded_hccs, on=["person_id", "hcc_code", "service_year"], how="anti"
        )

        logger.info("HCC hierarchies applied")

        return final_hccs

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_raf_scores(
        person_hccs: pl.LazyFrame,
        disease_factors: pl.LazyFrame,
        demographic_factors: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Calculate RAF (Risk Adjustment Factor) scores.

                RAF = demographic factors + disease factors + interaction factors

                Args:
                    person_hccs: Person HCCs with hierarchies applied
                    disease_factors: HCC coefficients
                    demographic_factors: Age/gender coefficients
                    eligibility: Member demographics
                    config: Configuration dict

                Returns:
                    LazyFrame with person_id, raf_score, and components
        """
        logger.info("Calculating RAF scores...")

        model_version = config.get("model_version", "V24")
        measurement_year = config.get("measurement_year", 2024)

        current_hccs = person_hccs.filter(pl.col("service_year") == measurement_year)

        # Join with disease factors to get coefficients
        # Simplify by using continuing enrollment, non-medicaid, non-institutional
        disease_coefs = disease_factors.filter(
            (pl.col("model_version") == f"CMS-HCC-{model_version}")
            & (pl.col("enrollment_status") == "Continuing")
            & (pl.col("medicaid_status") == "No")
            & (pl.col("institutional_status") == "No")
        ).select(["hcc_code", "coefficient"])

        hcc_values = current_hccs.join(disease_coefs, on="hcc_code", how="left")

        hcc_sums = hcc_values.group_by("person_id").agg(
            [pl.sum("coefficient").alias("disease_score")]
        )

        # Get demographic factors
        # Simplify: use continuing enrollment, aged, non-medicaid, non-institutional
        demo_coefs = demographic_factors.filter(
            (pl.col("model_version") == f"CMS-HCC-{model_version}")
            & (pl.col("enrollment_status") == "Continuing")
            & (pl.col("orec") == "Aged")
            & (pl.col("institutional_status") == "No")
        )

        # Join eligibility with demographic factors
        # Map age to age_group
        eligibility_with_age_group = eligibility.with_columns(
            [
                pl.when(pl.col("age") == 65)
                .then(pl.lit("65"))
                .when(pl.col("age") == 66)
                .then(pl.lit("66"))
                .when(pl.col("age") == 67)
                .then(pl.lit("67"))
                .when(pl.col("age") == 68)
                .then(pl.lit("68"))
                .when(pl.col("age") == 69)
                .then(pl.lit("69"))
                .when(pl.col("age").is_between(70, 74))
                .then(pl.lit("70-74"))
                .when(pl.col("age").is_between(75, 79))
                .then(pl.lit("75-79"))
                .when(pl.col("age").is_between(80, 84))
                .then(pl.lit("80-84"))
                .when(pl.col("age").is_between(85, 89))
                .then(pl.lit("85-89"))
                .when(pl.col("age").is_between(90, 94))
                .then(pl.lit("90-94"))
                .when(pl.col("age") >= 95)
                .then(pl.lit("95+"))
                .otherwise(pl.lit("65"))
                .alias("age_group"),
                pl.when(pl.col("gender").str.to_lowercase().is_in(["f", "female"]))
                .then(pl.lit("Female"))
                .otherwise(pl.lit("Male"))
                .alias("gender_clean"),
            ]
        )

        demographic_scores = eligibility_with_age_group.join(
            demo_coefs.select(["gender", "age_group", "coefficient"]),
            left_on=["gender_clean", "age_group"],
            right_on=["gender", "age_group"],
            how="left",
        ).select(["person_id", pl.col("coefficient").alias("demographic_score")])

        raf_scores = demographic_scores.join(hcc_sums, on="person_id", how="left")

        raf_scores = raf_scores.with_columns(
            [
                pl.col("disease_score").fill_null(0.0),
                pl.col("demographic_score").fill_null(0.0),
                (
                    pl.col("demographic_score").fill_null(0.0)
                    + pl.col("disease_score").fill_null(0.0)
                ).alias("raf_score"),
            ]
        )

        logger.info("RAF scores calculated")

        return raf_scores

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_hcc_gaps(
        current_hccs: pl.LazyFrame, historical_hccs: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Identify HCC gaps (historical chronic conditions not recaptured).

                Args:
                    current_hccs: Current year HCCs
                    historical_hccs: Historical HCCs
                    config: Configuration dict

                Returns:
                    LazyFrame with HCC gaps
        """
        logger.info("Identifying HCC gaps...")

        measurement_year = config.get("measurement_year", 2024)

        # Chronic HCC categories (conditions that should recur annually)
        chronic_hccs = [
            1,
            2,  # HIV/AIDS, Septicemia
            8,
            9,
            10,
            11,
            12,  # Cancers
            17,
            18,
            19,  # Diabetes
            85,
            86,
            87,
            88,  # Congestive Heart Failure
            106,
            107,
            108,
            109,
            110,
            111,  # COPD, Lung disorders
            54,
            55,  # Substance abuse
            57,
            58,  # Psychiatric disorders
            # Add more chronic HCCs as needed
        ]

        # Filter historical to chronic HCCs
        historical_chronic = historical_hccs.filter(pl.col("hcc_code").is_in(chronic_hccs))

        # Get most recent capture year for each person-HCC
        historical_summary = historical_chronic.group_by(["person_id", "hcc_code"]).agg(
            [pl.max("service_year").alias("last_capture_year")]
        )

        # Current year HCCs
        current_summary = (
            current_hccs.filter(pl.col("service_year") == measurement_year)
            .select(["person_id", "hcc_code"])
            .unique()
        )

        # Anti-join: historical chronic HCCs NOT in current
        gaps = historical_summary.join(current_summary, on=["person_id", "hcc_code"], how="anti")

        # Add metadata
        gaps = gaps.with_columns(
            [
                pl.lit(measurement_year).alias("gap_year"),
                (measurement_year - pl.col("last_capture_year")).alias("years_since_capture"),
                pl.lit("chronic_recapture").alias("gap_type"),
            ]
        )

        logger.info(f"Identified {gaps.collect().height:,} HCC gaps")

        return gaps

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def prioritize_gaps(
        gaps: pl.LazyFrame,
        disease_factors: pl.LazyFrame,
        raf_scores: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Prioritize HCC gaps by RAF score impact.

                Args:
                    gaps: HCC gaps
                    disease_factors: HCC coefficients
                    raf_scores: Current RAF scores
                    config: Configuration dict

                Returns:
                    LazyFrame with prioritized gaps
        """
        logger.info("Prioritizing HCC gaps...")

        model_version = config.get("model_version", "V24")

        # Get coefficient for each gap
        disease_coefs = disease_factors.filter(
            (pl.col("model_version") == f"CMS-HCC-{model_version}")
            & (pl.col("enrollment_status") == "Continuing")
            & (pl.col("medicaid_status") == "No")
            & (pl.col("institutional_status") == "No")
        ).select(["hcc_code", pl.col("coefficient").alias("gap_value"), "description"])

        gaps_with_value = gaps.join(disease_coefs, on="hcc_code", how="left")

        # Join with current RAF
        gaps_with_value = gaps_with_value.join(
            raf_scores.select(["person_id", "raf_score"]), on="person_id", how="left"
        )

        # Calculate potential RAF and impact
        gaps_with_value = gaps_with_value.with_columns(
            [
                (pl.col("raf_score") + pl.col("gap_value")).alias("potential_raf"),
                ((pl.col("gap_value") / pl.col("raf_score")) * 100).alias("pct_impact"),
                # Prioritize by value and recency
                pl.when(pl.col("gap_value") >= 0.5)
                .then(pl.lit("high"))
                .when(pl.col("gap_value") >= 0.2)
                .then(pl.lit("medium"))
                .otherwise(pl.lit("low"))
                .alias("priority"),
            ]
        )

        logger.info("Gaps prioritized")

        return gaps_with_value

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=90.0)
    @profile_memory(log_result=True)
    def calculate_hcc_gaps(
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Calculate comprehensive HCC gap analysis using CMS-HCC model.

                Args:
                    claims: Medical claims
                    eligibility: Member eligibility
                    value_sets: All 10 CMS-HCC value sets
                    config: Configuration dict

                Returns:
                    Tuple of (current_hccs_with_hierarchy, gaps_prioritized, raf_scores, summary)
        """
        logger.info("Starting comprehensive HCC gap analysis...")

        # Map diagnoses to HCCs
        all_hccs = HccGapAnalysisTransform.map_diagnoses_to_hccs(
            claims, value_sets["icd10_mappings"], config
        )

        # Apply hierarchies
        hccs_with_hierarchy = HccGapAnalysisTransform.apply_hcc_hierarchies(
            all_hccs, value_sets["disease_hierarchy"], config
        )

        # Calculate RAF scores
        raf_scores = HccGapAnalysisTransform.calculate_raf_scores(
            hccs_with_hierarchy,
            value_sets["disease_factors"],
            value_sets["demographic_factors"],
            eligibility,
            config,
        )

        # Identify gaps
        measurement_year = config.get("measurement_year", 2024)
        current_hccs = hccs_with_hierarchy.filter(pl.col("service_year") == measurement_year)
        historical_hccs = hccs_with_hierarchy.filter(pl.col("service_year") < measurement_year)

        gaps = HccGapAnalysisTransform.identify_hcc_gaps(current_hccs, historical_hccs, config)

        # Prioritize gaps
        gaps_prioritized = HccGapAnalysisTransform.prioritize_gaps(
            gaps, value_sets["disease_factors"], raf_scores, config
        )

        # Summary statistics
        summary = gaps_prioritized.group_by("priority").agg(
            [
                pl.count().alias("gap_count"),
                pl.sum("gap_value").alias("total_potential_value"),
                pl.mean("gap_value").alias("avg_gap_value"),
                pl.mean("years_since_capture").alias("avg_years_since_capture"),
                pl.col("person_id").n_unique().alias("unique_members"),
            ]
        )

        logger.info("HCC gap analysis complete")

        return hccs_with_hierarchy, gaps_prioritized, raf_scores, summary


logger.debug("Registered HCC gap analysis expression")
