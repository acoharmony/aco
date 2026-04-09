# © 2025 HarmonyCares
# All rights reserved.

"""
ACR (NQF #1789) Risk-Standardized All-Condition Readmission expression builder.

Expressions for calculating the ACO REACH All-Condition Readmission measure.
This implements the CMS methodology for identifying index admissions, assigning
specialty cohorts, classifying planned readmissions, and calculating observed
readmission rates.

The logic:
1. Identify index admissions (acute inpatient, age >= 65, not excluded by CCS)
2. Assign specialty cohorts (Surgery/Gyn, Cardiorespiratory, Cardiovascular, Neurology, Medicine)
3. Classify 30-day readmissions as planned or unplanned (PAA Rule 2)
4. Calculate observed readmission rate

References:
- CMS ACO REACH Quality Measures
- NQF #1789 Risk-Standardized All-Condition Readmission
- https://qualitynet.cms.gov/inpatient/measures/readmission/methodology
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import explain, profile_memory, timeit, traced
from ._registry import register_expression


@register_expression(
    "acr_readmission",
    schemas=["silver", "gold"],
    dataset_types=["claims", "encounters", "eligibility"],
    description="ACR NQF #1789 Risk-Standardized All-Condition Readmission measure",
)
class AcrReadmissionExpression:
    """
    Generate expressions for ACR readmission measure calculation.

        Configuration Structure:
            ```yaml
            acr_readmission:
              performance_year: 2025
              lookback_days: 30
              min_age: 65
              patient_id_column: person_id
              admission_date_column: admission_date
              discharge_date_column: discharge_date
              diagnosis_column: diagnosis_code_1
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
        """Build ACR readmission measure expressions."""
        performance_year = config.get("performance_year", 2025)
        lookback_days = config.get("lookback_days", 30)
        min_age = config.get("min_age", 65)
        patient_id_col = config.get("patient_id_column", "person_id")
        admit_date_col = config.get("admission_date_column", "admission_date")
        discharge_date_col = config.get("discharge_date_column", "discharge_date")
        diagnosis_col = config.get("diagnosis_column", "diagnosis_code_1")

        config_with_defaults = {
            "performance_year": performance_year,
            "lookback_days": lookback_days,
            "min_age": min_age,
            "patient_id_column": patient_id_col,
            "admission_date_column": admit_date_col,
            "discharge_date_column": discharge_date_col,
            "diagnosis_column": diagnosis_col,
        }

        expressions = {
            "identify_index_admissions": {
                "description": "Identify eligible acute inpatient index admissions for age >= 65",
            },
            "assign_specialty_cohorts": {
                "description": "Assign admissions to Surgery/Gyn, Cardiorespiratory, "
                "Cardiovascular, Neurology, or Medicine cohort",
            },
            "classify_planned_readmissions": {
                "description": "Classify 30-day readmissions as planned or unplanned using PAA",
            },
            "calculate_observed_rate": {
                "description": "Calculate observed readmission rate",
                "formula": pl.col("observed_readmissions") / pl.col("denominator_count"),
            },
        }

        return {"expressions": expressions, "config": config_with_defaults}

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def load_acr_value_sets(silver_path: Path) -> dict[str, pl.LazyFrame]:
        """
        Load all ACR readmission value sets from the silver layer.

        Args:
            silver_path: Path to silver layer containing value set parquet files

        Returns:
            Dictionary of value set name -> LazyFrame
        """
        value_sets: dict[str, pl.LazyFrame] = {}
        file_mappings = {
            "ccs_icd10_cm": "value_sets_acr_ccs_icd10_cm.parquet",
            "exclusions": "value_sets_acr_exclusions.parquet",
            "cohort_icd10": "value_sets_acr_cohort_icd10.parquet",
            "cohort_ccs": "value_sets_acr_cohort_ccs.parquet",
            "paa2": "value_sets_acr_paa2.parquet",
        }

        for key, filename in file_mappings.items():
            try:
                file_path = silver_path / filename
                if file_path.exists():
                    value_sets[key] = pl.scan_parquet(file_path)
                else:
                    value_sets[key] = pl.DataFrame().lazy()
            except Exception:
                value_sets[key] = pl.DataFrame().lazy()

        return value_sets

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_index_admissions(
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Identify eligible index admissions (denominator).

        Criteria:
        - Acute inpatient encounter (bill type 11x)
        - Age >= 65 at admission
        - Admission within performance period
        - Not excluded by CCS diagnosis category

        Args:
            claims: Medical claims with admission/discharge dates
            eligibility: Patient demographics with birth_date
            value_sets: ACR value sets dict
            config: Configuration dict

        Returns:
            LazyFrame with index admissions and exclusion flags
        """
        performance_year = config.get("performance_year", 2025)
        min_age = config.get("min_age", 65)

        period_begin = f"{performance_year}-01-01"
        period_end = f"{performance_year}-12-31"

        # Filter to acute inpatient admissions in performance period
        inpatient = claims.filter(
            (pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11"))
            & pl.col("admission_date").is_not_null()
            & pl.col("discharge_date").is_not_null()
            & (pl.col("admission_date") >= pl.lit(period_begin).str.to_date("%Y-%m-%d"))
            & (pl.col("admission_date") <= pl.lit(period_end).str.to_date("%Y-%m-%d"))
        )

        # Join with eligibility for age calculation
        with_age = inpatient.join(
            eligibility.select(
                [pl.col("person_id"), pl.col("birth_date").cast(pl.Date)]
            ),
            on="person_id",
            how="inner",
        ).with_columns(
            [
                (
                    (pl.col("admission_date") - pl.col("birth_date")).dt.total_days() / 365.25
                )
                .cast(pl.Int32)
                .alias("age_at_admission")
            ]
        )

        # Filter by age
        with_age = with_age.filter(pl.col("age_at_admission") >= min_age)

        # Map principal diagnosis to CCS category
        ccs_mapping = value_sets.get("ccs_icd10_cm")
        if ccs_mapping is not None and ccs_mapping.collect().height > 0:
            with_age = with_age.join(
                ccs_mapping.select(
                    [
                        pl.col("icd_10_cm").alias("diagnosis_code_1"),
                        pl.col("ccs_category").alias("ccs_diagnosis_category"),
                    ]
                ),
                on="diagnosis_code_1",
                how="left",
            )
        else:
            with_age = with_age.with_columns(
                [pl.lit(None).cast(pl.Utf8).alias("ccs_diagnosis_category")]
            )

        # Apply CCS exclusions
        exclusions = value_sets.get("exclusions")
        if exclusions is not None and exclusions.collect().height > 0:
            excluded_ccs = exclusions.select(
                pl.col("ccs_diagnosis_category").unique()
            )
            with_age = with_age.join(
                excluded_ccs.with_columns([pl.lit(True).alias("exclusion_flag")]),
                on="ccs_diagnosis_category",
                how="left",
            )
        else:
            with_age = with_age.with_columns(
                [pl.lit(False).alias("exclusion_flag")]
            )

        with_age = with_age.with_columns(
            [pl.col("exclusion_flag").fill_null(False)]
        )

        # Select index admission fields
        index_admissions = with_age.select(
            [
                pl.col("claim_id"),
                pl.col("person_id"),
                pl.col("admission_date"),
                pl.col("discharge_date"),
                pl.col("discharge_status_code"),
                pl.col("facility_npi").alias("facility_id"),
                pl.col("diagnosis_code_1").alias("principal_diagnosis_code"),
                pl.col("ccs_diagnosis_category"),
                pl.col("age_at_admission"),
                pl.col("exclusion_flag"),
            ]
        )

        return index_admissions

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def assign_specialty_cohorts(
        index_admissions: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Assign each non-excluded index admission to a specialty cohort.

        Cohort priority:
        1. SURGERY_GYNECOLOGY - matched by ICD-10-PCS procedure codes
        2. CARDIORESPIRATORY - matched by CCS diagnosis category
        3. CARDIOVASCULAR - matched by CCS diagnosis category
        4. NEUROLOGY - matched by CCS diagnosis category
        5. MEDICINE - default for all others

        Args:
            index_admissions: Index admissions from identify_index_admissions()
            claims: Medical claims (for procedure codes)
            value_sets: ACR value sets dict
            config: Configuration dict

        Returns:
            LazyFrame with claim_id and specialty_cohort
        """
        eligible = index_admissions.filter(~pl.col("exclusion_flag"))

        # Step 1: Surgery/Gynecology cohort via ICD-10-PCS procedures
        surgery_claim_ids: pl.LazyFrame | None = None
        cohort_icd10 = value_sets.get("cohort_icd10")

        if cohort_icd10 is not None and cohort_icd10.collect().height > 0:
            # Get procedure codes from claims (unpivot procedure columns)
            procedure_cols = [
                f"procedure_code_{i}" for i in range(1, 14)
            ]
            # Build union of all procedure code columns that exist
            existing_cols = claims.collect_schema().names()
            available_proc_cols = [c for c in procedure_cols if c in existing_cols]

            if available_proc_cols:
                proc_frames = []
                for col in available_proc_cols:
                    proc_frames.append(
                        claims.select(
                            [pl.col("claim_id"), pl.col(col).alias("procedure_code")]
                        ).filter(pl.col("procedure_code").is_not_null())
                    )
                claim_procedures = pl.concat(proc_frames)

                surgery_icd10_codes = cohort_icd10.filter(
                    pl.col("specialty_cohort") == "SURGERY_GYNECOLOGY"
                ).select(pl.col("icd_10_pcs").alias("procedure_code"))

                surgery_claim_ids = (
                    eligible.select("claim_id")
                    .join(claim_procedures, on="claim_id", how="inner")
                    .join(surgery_icd10_codes, on="procedure_code", how="inner")
                    .select("claim_id")
                    .unique()
                    .with_columns(
                        [
                            pl.lit("SURGERY_GYNECOLOGY").alias("specialty_cohort"),
                            pl.lit("ICD10_PCS").alias("cohort_assignment_rule"),
                        ]
                    )
                )

        # Step 2: CCS-based cohorts (excluding surgery claims)
        cohort_ccs = value_sets.get("cohort_ccs")
        ccs_cohort_claims: pl.LazyFrame | None = None

        if cohort_ccs is not None and cohort_ccs.collect().height > 0:
            base = eligible.select(["claim_id", "ccs_diagnosis_category"])

            if surgery_claim_ids is not None:
                base = base.join(
                    surgery_claim_ids.select("claim_id").with_columns(
                        [pl.lit(True).alias("_is_surgery")]
                    ),
                    on="claim_id",
                    how="left",
                ).filter(
                    pl.col("_is_surgery").fill_null(False).not_()
                ).drop("_is_surgery")

            ccs_cohort_claims = base.join(
                cohort_ccs.select(
                    [
                        pl.col("ccs_category").alias("ccs_diagnosis_category"),
                        pl.col("specialty_cohort"),
                    ]
                ),
                on="ccs_diagnosis_category",
                how="inner",
            ).select(
                [
                    pl.col("claim_id"),
                    pl.col("specialty_cohort"),
                    pl.lit("CCS_DIAGNOSIS").alias("cohort_assignment_rule"),
                ]
            ).unique(subset=["claim_id"], keep="first")

        # Step 3: Combine all cohort assignments with default MEDICINE
        cohort_parts = []
        if surgery_claim_ids is not None:
            cohort_parts.append(surgery_claim_ids)
        if ccs_cohort_claims is not None:
            cohort_parts.append(ccs_cohort_claims)

        if cohort_parts:
            assigned = pl.concat(cohort_parts).unique(subset=["claim_id"], keep="first")
        else:
            assigned = pl.DataFrame(
                schema={
                    "claim_id": pl.Utf8,
                    "specialty_cohort": pl.Utf8,
                    "cohort_assignment_rule": pl.Utf8,
                }
            ).lazy()

        # Left join to get defaults for unmatched
        result = (
            eligible.select("claim_id")
            .join(assigned, on="claim_id", how="left")
            .with_columns(
                [
                    pl.col("specialty_cohort").fill_null("MEDICINE"),
                    pl.col("cohort_assignment_rule").fill_null("DEFAULT_MEDICINE"),
                ]
            )
        )

        return result

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_planned_readmissions(
        index_admissions: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Identify 30-day readmissions and classify as planned or unplanned.

        PAA Rule 2: Readmissions with always-planned diagnoses (PAA2 value set)
        are classified as planned and excluded from the unplanned count.

        Args:
            index_admissions: Index admissions (non-excluded)
            claims: Medical claims
            value_sets: ACR value sets dict
            config: Configuration dict

        Returns:
            LazyFrame with readmission pairs and classification flags
        """
        lookback_days = config.get("lookback_days", 30)

        # Index discharges
        index_discharges = index_admissions.filter(
            ~pl.col("exclusion_flag")
        ).select(
            [
                pl.col("claim_id").alias("index_claim_id"),
                pl.col("person_id"),
                pl.col("discharge_date").alias("index_discharge_date"),
            ]
        )

        # Candidate readmissions (all acute inpatient)
        candidate_admits = claims.filter(
            (pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11"))
            & pl.col("admission_date").is_not_null()
        ).select(
            [
                pl.col("claim_id").alias("readmission_claim_id"),
                pl.col("person_id"),
                pl.col("admission_date").alias("readmission_date"),
                pl.col("diagnosis_code_1").alias("readmit_diagnosis_code"),
                pl.col("facility_npi").alias("readmit_facility_id"),
            ]
        )

        # Pair index discharges with readmissions 1-30 days later
        pairs = index_discharges.join(
            candidate_admits, on="person_id", how="inner"
        ).filter(
            (pl.col("readmission_claim_id") != pl.col("index_claim_id"))
            & (pl.col("readmission_date") > pl.col("index_discharge_date"))
            & (
                (pl.col("readmission_date") - pl.col("index_discharge_date")).dt.total_days()
                <= lookback_days
            )
        ).with_columns(
            [
                (
                    pl.col("readmission_date") - pl.col("index_discharge_date")
                )
                .dt.total_days()
                .alias("days_to_readmission")
            ]
        )

        # Map readmission diagnosis to CCS
        ccs_mapping = value_sets.get("ccs_icd10_cm")
        if ccs_mapping is not None and ccs_mapping.collect().height > 0:
            pairs = pairs.join(
                ccs_mapping.select(
                    [
                        pl.col("icd_10_cm").alias("readmit_diagnosis_code"),
                        pl.col("ccs_category").alias("readmit_dx_ccs"),
                    ]
                ),
                on="readmit_diagnosis_code",
                how="left",
            )
        else:
            pairs = pairs.with_columns(
                [pl.lit(None).cast(pl.Utf8).alias("readmit_dx_ccs")]
            )

        # Apply PAA Rule 2 (always-planned diagnoses)
        paa2 = value_sets.get("paa2")
        if paa2 is not None and paa2.collect().height > 0:
            paa2_ccs = (
                paa2.select("ccs_diagnosis_category")
                .unique()
                .collect()["ccs_diagnosis_category"]
                .to_list()
            )
            pairs = pairs.with_columns(
                [
                    pl.col("readmit_dx_ccs").is_in(paa2_ccs).alias("is_planned"),
                    pl.when(pl.col("readmit_dx_ccs").is_in(paa2_ccs))
                    .then(pl.lit("RULE2"))
                    .otherwise(pl.lit(None))
                    .alias("planned_rule"),
                ]
            )
        else:
            pairs = pairs.with_columns(
                [
                    pl.lit(False).alias("is_planned"),
                    pl.lit(None).cast(pl.Utf8).alias("planned_rule"),
                ]
            )

        # Final classification
        pairs = pairs.with_columns(
            [
                pl.when(~pl.col("is_planned"))
                .then(True)
                .otherwise(False)
                .alias("unplanned_readmission_flag"),
            ]
        )

        return pairs

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=30.0)
    @profile_memory(log_result=True)
    def calculate_acr_summary(
        index_admissions: pl.LazyFrame,
        readmission_pairs: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Calculate ACO-level ACR summary with observed readmission rate.

        Args:
            index_admissions: Index admissions with exclusion flags
            readmission_pairs: Readmission pairs with planned/unplanned flags
            config: Configuration dict

        Returns:
            LazyFrame with ACR summary metrics
        """
        performance_year = config.get("performance_year", 2025)

        denominator_count = (
            index_admissions.filter(~pl.col("exclusion_flag"))
            .select(pl.col("claim_id").n_unique().alias("denominator_count"))
        )

        observed_readmissions = (
            readmission_pairs.filter(pl.col("unplanned_readmission_flag"))
            .select(
                pl.col("readmission_claim_id").n_unique().alias("observed_readmissions")
            )
        )

        summary = (
            denominator_count.with_columns(
                [observed_readmissions.select("observed_readmissions").collect().item()]
            )
            if False
            else None
        )

        # Build summary as a cross join of the two scalar values
        denom_df = denominator_count.collect()
        numer_df = observed_readmissions.collect()

        denom_val = denom_df["denominator_count"][0] if denom_df.height > 0 else 0
        numer_val = numer_df["observed_readmissions"][0] if numer_df.height > 0 else 0

        observed_rate = numer_val / denom_val if denom_val > 0 else None

        summary = pl.DataFrame(
            {
                "program": ["REACH"],
                "measure_id": ["ACR"],
                "measure_name": ["Risk-Standardized All-Condition Readmission"],
                "nqf_id": ["1789"],
                "performance_year": [performance_year],
                "denominator_count": [denom_val],
                "observed_readmissions": [numer_val],
                "observed_rate": [observed_rate],
                "expected_readmissions": [None],
                "rsrr": [None],
            }
        ).lazy()

        return summary

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def calculate_acr_measure(
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Calculate the complete ACR measure end-to-end.

        Args:
            claims: Medical claims
            eligibility: Patient demographics
            value_sets: ACR value sets
            config: Configuration dict

        Returns:
            Tuple of:
            - index_admissions: All index admissions with flags
            - specialty_cohorts: Cohort assignments
            - readmission_pairs: All readmission pairs with classification
            - summary: ACO-level summary
        """
        # Step 1: Identify index admissions
        index_admissions = AcrReadmissionExpression.identify_index_admissions(
            claims, eligibility, value_sets, config
        )

        # Step 2: Assign specialty cohorts
        specialty_cohorts = AcrReadmissionExpression.assign_specialty_cohorts(
            index_admissions, claims, value_sets, config
        )

        # Step 3: Identify and classify readmissions
        readmission_pairs = AcrReadmissionExpression.identify_planned_readmissions(
            index_admissions, claims, value_sets, config
        )

        # Step 4: Calculate summary
        summary = AcrReadmissionExpression.calculate_acr_summary(
            index_admissions, readmission_pairs, config
        )

        return index_admissions, specialty_cohorts, readmission_pairs, summary
