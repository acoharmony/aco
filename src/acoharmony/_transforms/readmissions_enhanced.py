# © 2025 HarmonyCares
# All rights reserved.

"""
Enhanced readmissions analysis using comprehensive CMS methodology.

This module implements the CMS Hospital-Wide All-Cause Readmission measure
methodology with additional enhancements:

- Planned vs Unplanned readmission classification
- Specialty cohort analysis (cardiorespiratory, cardiovascular, neurology, etc.)
- Surgery/gynecology cohort analysis
- Exclusions for always-planned procedures
- CCS (Clinical Classifications Software) grouping
- 30-day readmission windows
- Risk stratification by diagnosis
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.readmissions_enhanced")


@transform(name="readmissions_enhanced", tier=["gold"])
class ReadmissionsEnhancedTransform:
    """
    Enhanced readmissions analysis using CMS methodology.

        Uses 11 value sets to classify readmissions:
        1. Acute diagnosis mapping (ICD-10-CM)
        2. Acute diagnosis CCS categories
        3. Always planned diagnosis categories
        4. Always planned procedure categories
        5. Potentially planned procedures
        6. Exclusion diagnosis categories
        7. ICD-10-CM to CCS mapping
        8. ICD-10-PCS to CCS mapping
        9. Specialty cohorts
        10. Surgery/gynecology cohort
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def load_readmissions_value_sets(silver_path) -> dict[str, pl.LazyFrame]:
        """
        Load all 11 readmissions value sets.

                Args:
                    silver_path: Path to silver layer with value sets

                Returns:
                    Dictionary of value set name -> LazyFrame
        """
        logger.info("Loading readmissions value sets...")

        value_sets = {}
        file_mappings = {
            "acute_diagnosis_icd10": "value_sets_readmissions_acute_diagnosis_icd_10_cm.parquet",
            "acute_diagnosis_ccs": "value_sets_readmissions_acute_diagnosis_ccs.parquet",
            "always_planned_dx": "value_sets_readmissions_always_planned_ccs_diagnosis_category.parquet",
            "always_planned_px": "value_sets_readmissions_always_planned_ccs_procedure_category.parquet",
            "potentially_planned_px_ccs": "value_sets_readmissions_potentially_planned_ccs_procedure_category.parquet",
            "potentially_planned_px_icd10": "value_sets_readmissions_potentially_planned_icd_10_pcs.parquet",
            "exclusion_dx": "value_sets_readmissions_exclusion_ccs_diagnosis_category.parquet",
            "icd10cm_to_ccs": "value_sets_readmissions_icd_10_cm_to_ccs.parquet",
            "icd10pcs_to_ccs": "value_sets_readmissions_icd_10_pcs_to_ccs.parquet",
            "specialty_cohort": "value_sets_readmissions_specialty_cohort.parquet",
            "surgery_gyn_cohort": "value_sets_readmissions_surgery_gynecology_cohort.parquet",
        }

        for key, filename in file_mappings.items():
            try:
                file_path = silver_path / filename
                value_sets[key] = pl.scan_parquet(file_path)
                logger.debug(f"Loaded {key} from {filename}")
            except Exception as e:
                logger.warning(f"Could not load {key}: {e}")
                # Create empty placeholder
                value_sets[key] = pl.DataFrame().lazy()

        logger.info(
            f"Loaded {len([v for v in value_sets.values() if v.collect().height > 0])} readmissions value sets"
        )

        return value_sets

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_index_admissions(
        claims: pl.LazyFrame, value_sets: dict[str, pl.LazyFrame], config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Identify index admissions (initial acute inpatient stays).

                An index admission is an acute inpatient admission that:
                - Is not an exclusion (e.g., medical treatment of cancer, rehabilitation)
                - Has a valid principal diagnosis
                - Is not an always-planned admission

                Args:
                    claims: Medical claims with admissions
                    value_sets: Readmissions value sets
                    config: Configuration dict

                Returns:
                    LazyFrame with index admissions
        """
        logger.info("Identifying index admissions...")

        # Filter to acute inpatient admissions
        # Bill type codes 11x = Inpatient hospital
        inpatient = claims.filter(
            (pl.col("claim_type") == "institutional")
            & (pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11"))
            & pl.col("admission_date").is_not_null()
            & pl.col("discharge_date").is_not_null()
        )

        # Map diagnoses to CCS categories
        icd10_to_ccs = value_sets.get("icd10cm_to_ccs")
        if icd10_to_ccs is not None and icd10_to_ccs.collect().height > 0:
            inpatient = inpatient.join(
                icd10_to_ccs.select(
                    [
                        pl.col("icd_10_cm").alias("diagnosis_code_1"),
                        pl.col("ccs_category").alias("principal_ccs"),
                    ]
                ),
                on="diagnosis_code_1",
                how="left",
            )
        else:
            inpatient = inpatient.with_columns([pl.lit(None).alias("principal_ccs")])

        # Exclude admissions with exclusion diagnoses
        exclusion_dx = value_sets.get("exclusion_dx")
        if exclusion_dx is not None and exclusion_dx.collect().height > 0:
            excluded_ccs = exclusion_dx.select("ccs_category").unique()
            inpatient = inpatient.join(
                excluded_ccs.with_columns([pl.lit(True).alias("is_excluded")]),
                left_on="principal_ccs",
                right_on="ccs_category",
                how="left",
            ).filter(pl.col("is_excluded").fill_null(False).not_())

        # Exclude always-planned admissions
        always_planned_dx = value_sets.get("always_planned_dx")
        if always_planned_dx is not None and always_planned_dx.collect().height > 0:
            always_planned_ccs = always_planned_dx.select("ccs_category").unique()
            inpatient = inpatient.join(
                always_planned_ccs.with_columns([pl.lit(True).alias("is_always_planned")]),
                left_on="principal_ccs",
                right_on="ccs_category",
                how="left",
            ).filter(pl.col("is_always_planned").fill_null(False).not_())

        # Select index admission fields
        index_admissions = inpatient.select(
            [
                pl.col("person_id"),
                pl.col("claim_id").alias("index_claim_id"),
                pl.col("admission_date").alias("index_admission_date"),
                pl.col("discharge_date").alias("index_discharge_date"),
                pl.col("diagnosis_code_1").alias("index_principal_diagnosis"),
                pl.col("principal_ccs").alias("index_ccs_category"),
                pl.col("facility_npi").alias("index_facility"),
            ]
        )

        logger.info(f"Identified {index_admissions.collect().height} index admissions")

        return index_admissions

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_readmissions(
        index_admissions: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Identify readmissions within 30 days of index discharge.

                A readmission is:
                - An acute inpatient admission
                - Within 30 days of index discharge
                - For the same patient
                - Not the same encounter as the index

                Args:
                    index_admissions: Index admissions from identify_index_admissions()
                    claims: Medical claims
                    value_sets: Readmissions value sets
                    config: Configuration dict

                Returns:
                    LazyFrame with readmission pairs (index + readmission)
        """
        logger.info("Identifying readmissions...")

        lookback_days = config.get("lookback_days", 30)

        # Get all inpatient admissions
        inpatient = claims.filter(
            (pl.col("claim_type") == "institutional")
            & (pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11"))
            & pl.col("admission_date").is_not_null()
        ).select(
            [
                pl.col("person_id"),
                pl.col("claim_id").alias("readmit_claim_id"),
                pl.col("admission_date").alias("readmit_admission_date"),
                pl.col("discharge_date").alias("readmit_discharge_date"),
                pl.col("diagnosis_code_1").alias("readmit_principal_diagnosis"),
                pl.col("procedure_code_1").alias("readmit_principal_procedure"),
            ]
        )

        # Join index admissions with subsequent admissions
        readmission_pairs = index_admissions.join(inpatient, on="person_id", how="inner")

        # Filter to readmissions within window
        readmission_pairs = readmission_pairs.filter(
            # Readmission date is after index discharge
            (pl.col("readmit_admission_date") > pl.col("index_discharge_date"))
            # Readmission is within 30 days
            & (
                (pl.col("readmit_admission_date") - pl.col("index_discharge_date")).dt.total_days()
                <= lookback_days
            )
            # Not the same encounter
            & (pl.col("index_claim_id") != pl.col("readmit_claim_id"))
        )

        # Calculate days to readmission
        readmission_pairs = readmission_pairs.with_columns(
            [
                (pl.col("readmit_admission_date") - pl.col("index_discharge_date"))
                .dt.total_days()
                .alias("days_to_readmission")
            ]
        )

        logger.info(f"Identified {readmission_pairs.collect().height} potential readmissions")

        return readmission_pairs

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def classify_planned_vs_unplanned(
        readmission_pairs: pl.LazyFrame, value_sets: dict[str, pl.LazyFrame], config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Classify readmissions as planned or unplanned.

                Uses CMS methodology:
                - Always planned: Specific diagnosis/procedure combinations (e.g., chemotherapy, transplant)
                - Potentially planned: Procedures that may be planned (requires diagnosis context)
                - Unplanned: All other readmissions

                Args:
                    readmission_pairs: Readmission pairs from identify_readmissions()
                    value_sets: Readmissions value sets
                    config: Configuration dict

                Returns:
                    LazyFrame with planned_readmission flag
        """
        logger.info("Classifying planned vs unplanned readmissions...")

        # Map readmission diagnoses to CCS
        icd10_to_ccs = value_sets.get("icd10cm_to_ccs")
        if icd10_to_ccs is not None and icd10_to_ccs.collect().height > 0:
            pairs = readmission_pairs.join(
                icd10_to_ccs.select(
                    [
                        pl.col("icd_10_cm").alias("readmit_principal_diagnosis"),
                        pl.col("ccs_category").alias("readmit_ccs"),
                    ]
                ),
                on="readmit_principal_diagnosis",
                how="left",
            )
        else:
            pairs = readmission_pairs.with_columns([pl.lit(None).alias("readmit_ccs")])

        # Map readmission procedures to CCS
        icd10pcs_to_ccs = value_sets.get("icd10pcs_to_ccs")
        if icd10pcs_to_ccs is not None and icd10pcs_to_ccs.collect().height > 0:
            pairs = pairs.join(
                icd10pcs_to_ccs.select(
                    [
                        pl.col("icd_10_pcs").alias("readmit_principal_procedure"),
                        pl.col("ccs_category").alias("readmit_procedure_ccs"),
                    ]
                ),
                on="readmit_principal_procedure",
                how="left",
            )
        else:
            pairs = pairs.with_columns([pl.lit(None).alias("readmit_procedure_ccs")])

        # Check if always planned
        always_planned_dx = value_sets.get("always_planned_dx")
        always_planned_px = value_sets.get("always_planned_px")

        is_always_planned = pl.lit(False)

        if always_planned_dx is not None and always_planned_dx.collect().height > 0:
            always_dx_ccs = (
                always_planned_dx.select("ccs_category")
                .unique()
                .collect()["ccs_category"]
                .to_list()
            )
            is_always_planned = is_always_planned | pl.col("readmit_ccs").is_in(always_dx_ccs)

        if always_planned_px is not None and always_planned_px.collect().height > 0:
            always_px_ccs = (
                always_planned_px.select("ccs_category")
                .unique()
                .collect()["ccs_category"]
                .to_list()
            )
            is_always_planned = is_always_planned | pl.col("readmit_procedure_ccs").is_in(
                always_px_ccs
            )

        pairs = pairs.with_columns([is_always_planned.alias("is_always_planned")])

        # Check if potentially planned (requires acute diagnosis context)
        potentially_planned_px = value_sets.get("potentially_planned_px_ccs")
        is_potentially_planned = pl.lit(False)

        if potentially_planned_px is not None and potentially_planned_px.collect().height > 0:
            potentially_px_ccs = (
                potentially_planned_px.select("ccs_category")
                .unique()
                .collect()["ccs_category"]
                .to_list()
            )
            is_potentially_planned = pl.col("readmit_procedure_ccs").is_in(potentially_px_ccs)

        # Potentially planned is only considered planned if NOT an acute diagnosis
        acute_diagnosis_ccs = value_sets.get("acute_diagnosis_ccs")
        if acute_diagnosis_ccs is not None and acute_diagnosis_ccs.collect().height > 0:
            acute_ccs = (
                acute_diagnosis_ccs.select("ccs_category")
                .unique()
                .collect()["ccs_category"]
                .to_list()
            )
            is_acute = pl.col("readmit_ccs").is_in(acute_ccs)
            is_potentially_planned = is_potentially_planned & ~is_acute
        else:
            is_potentially_planned = pl.lit(False)

        pairs = pairs.with_columns([is_potentially_planned.alias("is_potentially_planned")])

        # Final classification
        pairs = pairs.with_columns(
            [
                (pl.col("is_always_planned") | pl.col("is_potentially_planned")).alias(
                    "planned_readmission"
                ),
                pl.when(pl.col("is_always_planned"))
                .then(pl.lit("always_planned"))
                .when(pl.col("is_potentially_planned"))
                .then(pl.lit("potentially_planned"))
                .otherwise(pl.lit("unplanned"))
                .alias("readmission_type"),
            ]
        )

        n_planned = pairs.filter(pl.col("planned_readmission")).collect().height
        n_unplanned = pairs.filter(~pl.col("planned_readmission")).collect().height

        logger.info(f"Classified: {n_planned} planned, {n_unplanned} unplanned readmissions")

        return pairs

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def assign_specialty_cohorts(
        readmission_pairs: pl.LazyFrame, value_sets: dict[str, pl.LazyFrame], config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Assign specialty cohorts to index admissions.

                Cohorts include:
                - Cardiorespiratory
                - Cardiovascular
                - Neurology
                - Orthopedic
                - Peripheral vascular
                - etc.

                Args:
                    readmission_pairs: Readmission pairs
                    value_sets: Readmissions value sets
                    config: Configuration dict

                Returns:
                    LazyFrame with specialty_cohort column
        """
        logger.info("Assigning specialty cohorts...")

        specialty_cohort = value_sets.get("specialty_cohort")
        if specialty_cohort is None or specialty_cohort.collect().height == 0:
            logger.warning(
                "Specialty cohort value set not found, all admissions assigned to 'general'"
            )
            return readmission_pairs.with_columns([pl.lit("general").alias("specialty_cohort")])

        # Join with specialty cohort mapping (by CCS category)
        pairs = readmission_pairs.join(
            specialty_cohort.select(
                [pl.col("ccs_category"), pl.col("cohort_name").alias("specialty_cohort")]
            ),
            left_on="index_ccs_category",
            right_on="ccs_category",
            how="left",
        )

        # Default to 'general' for unmapped
        pairs = pairs.with_columns([pl.col("specialty_cohort").fill_null("general")])

        # Count cohorts
        cohort_counts = pairs.group_by("specialty_cohort").agg(pl.count().alias("count")).collect()
        logger.info(f"Specialty cohort distribution: {cohort_counts}")

        return pairs

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=30.0)
    @profile_memory(log_result=True)
    def calculate_enhanced_readmissions(
        claims: pl.LazyFrame, value_sets: dict[str, pl.LazyFrame], config: dict[str, Any]
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Calculate comprehensive readmissions analysis.

                Args:
                    claims: Medical claims
                    value_sets: All 11 readmissions value sets
                    config: Configuration dict

                Returns:
                    Tuple of (readmission_pairs, summary_by_cohort, summary_overall)
        """
        logger.info("Starting enhanced readmissions analysis...")

        # Step 1: Identify index admissions
        index_admissions = ReadmissionsEnhancedTransform.identify_index_admissions(
            claims, value_sets, config
        )

        # Step 2: Identify readmissions
        readmission_pairs = ReadmissionsEnhancedTransform.identify_readmissions(
            index_admissions, claims, value_sets, config
        )

        # Step 3: Classify planned vs unplanned
        readmission_pairs = ReadmissionsEnhancedTransform.classify_planned_vs_unplanned(
            readmission_pairs, value_sets, config
        )

        # Step 4: Assign specialty cohorts
        readmission_pairs = ReadmissionsEnhancedTransform.assign_specialty_cohorts(
            readmission_pairs, value_sets, config
        )

        # Calculate summary by specialty cohort
        summary_by_cohort = readmission_pairs.group_by(
            ["specialty_cohort", "readmission_type"]
        ).agg(
            [
                pl.count().alias("readmission_count"),
                pl.col("index_claim_id").n_unique().alias("unique_index_admissions"),
                pl.col("days_to_readmission").mean().alias("avg_days_to_readmission"),
                pl.col("days_to_readmission").median().alias("median_days_to_readmission"),
            ]
        )

        # Calculate overall summary
        summary_overall = readmission_pairs.group_by("readmission_type").agg(
            [
                pl.count().alias("readmission_count"),
                pl.col("index_claim_id").n_unique().alias("unique_index_admissions"),
                pl.col("days_to_readmission").mean().alias("avg_days_to_readmission"),
                pl.col("days_to_readmission").median().alias("median_days_to_readmission"),
                pl.col("days_to_readmission").quantile(0.25).alias("p25_days_to_readmission"),
                pl.col("days_to_readmission").quantile(0.75).alias("p75_days_to_readmission"),
            ]
        )

        logger.info("Enhanced readmissions analysis complete")

        return readmission_pairs, summary_by_cohort, summary_overall


logger.debug("Registered enhanced readmissions expression")
