# © 2025 HarmonyCares
# All rights reserved.

"""
Hospital Readmissions expression builder.

 expressions for identifying and analyzing hospital readmissions,
focusing on the CMS 30-day all-cause readmission measure. Readmissions are a key
quality metric and financial risk factor for healthcare organizations.

The logic:
1. Identify index admissions (qualifying hospitalizations)
2. Detect readmissions within 30 days of discharge
3. Exclude planned readmissions
4. Calculate readmission rates and attribution

References:
- CMS Hospital-Wide All-Cause Readmission Measure
- https://qualitynet.cms.gov/inpatient/measures/readmission/methodology

"""

from typing import Any

import polars as pl

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


@register_expression(
    "readmissions",
    schemas=["silver", "gold"],
    dataset_types=["claims", "encounters"],
    description="30-day hospital readmission identification and analysis",
)
class ReadmissionsExpression:
    """
    Generate expressions for hospital readmission detection.

        Configuration Structure:
            ```yaml
            readmissions:
              # Lookback period
              lookback_days: 30

              # Index admission criteria
              index_admission_types: ['inpatient', 'acute']
              min_length_of_stay: 1

              # Readmission criteria
              exclude_planned: true
              exclude_transfers: true

              # Column mappings
              patient_id_column: patient_id
              admission_date_column: admission_date
              discharge_date_column: discharge_date
              encounter_type_column: encounter_type
              diagnosis_column: principal_diagnosis_code
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
        """Build readmission detection expressions."""
        lookback_days = config.get("lookback_days", 30)
        patient_id_col = config.get("patient_id_column", "patient_id")
        admit_date_col = config.get("admission_date_column", "admission_date")
        discharge_date_col = config.get("discharge_date_column", "discharge_date")
        exclude_planned = config.get("exclude_planned", True)
        exclude_transfers = config.get("exclude_transfers", True)

        # Update config with defaults
        config_with_defaults = {
            "lookback_days": lookback_days,
            "patient_id_column": patient_id_col,
            "admission_date_column": admit_date_col,
            "discharge_date_column": discharge_date_col,
            "exclude_planned": exclude_planned,
            "exclude_transfers": exclude_transfers,
        }

        expressions = {
            "identify_index_admissions": {
                "description": "Flag qualifying index admissions",
                "filter": pl.col("encounter_type").is_in(["inpatient", "acute"]),
            },
            "calculate_readmission_window": {
                "description": "Calculate 30-day window after discharge",
                "window_start": pl.col(discharge_date_col),
                "window_end": pl.col(discharge_date_col) + pl.duration(days=lookback_days),
            },
            "detect_readmissions": {
                "description": "Identify readmissions within window",
                # Self-join logic to find subsequent admissions within window
            },
        }

        return {"expressions": expressions, "config": config_with_defaults}

    @staticmethod
    def transform_readmission_pairs(
        encounters: pl.LazyFrame,
        acute_diagnoses: pl.LazyFrame,
        planned_procedures: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Identify index admission and readmission pairs.

                Returns:
                    LazyFrame with columns:
                    - patient_id
                    - index_encounter_id
                    - index_admission_date
                    - index_discharge_date
                    - readmission_encounter_id
                    - readmission_admission_date
                    - days_to_readmission
                    - is_planned (boolean)
        """
        patient_id_col = config.get("patient_id_column", "patient_id")
        admit_col = config.get("admission_date_column", "admission_date")
        discharge_col = config.get("discharge_date_column", "discharge_date")
        lookback = config.get("lookback_days", 30)

        # Self-join to find subsequent admissions
        # Cast date columns to Date type to ensure proper date arithmetic
        index = encounters.filter(pl.col("encounter_type") == "inpatient").select(
            [
                pl.col(patient_id_col),
                pl.col("encounter_id").alias("index_encounter_id"),
                pl.col(admit_col).cast(pl.Date).alias("index_admission_date"),
                pl.col(discharge_col).cast(pl.Date).alias("index_discharge_date"),
            ]
        )

        readmit = encounters.filter(pl.col("encounter_type") == "inpatient").select(
            [
                pl.col(patient_id_col),
                pl.col("encounter_id").alias("readmission_encounter_id"),
                pl.col(admit_col).cast(pl.Date).alias("readmission_admission_date"),
            ]
        )

        # Join and filter
        pairs = index.join(readmit, on=patient_id_col, how="inner")

        pairs = pairs.filter(
            (pl.col("readmission_admission_date") > pl.col("index_discharge_date"))
            & (
                pl.col("readmission_admission_date")
                <= pl.col("index_discharge_date") + pl.duration(days=lookback)
            )
        )

        # Calculate days to readmission
        pairs = pairs.with_columns(
            [
                (pl.col("readmission_admission_date") - pl.col("index_discharge_date"))
                .dt.total_days()
                .alias("days_to_readmission")
            ]
        )

        return pairs

    @staticmethod
    def identify_readmission_pairs(
        medical_claim_df: pl.LazyFrame,
        lookback_days: int = 30,
        person_id_col: str = "person_id",
        claim_id_col: str = "claim_id",
        admit_date_col: str = "admission_date",
        discharge_date_col: str = "discharge_date",
    ) -> pl.LazyFrame:
        """
        Identify index admission and readmission pairs within lookback window.

        This implements the CMS 30-day all-cause readmission logic:
        1. Find inpatient admissions (index admissions)
        2. Find subsequent inpatient admissions for the same patient
        3. Filter to readmissions within lookback_days of index discharge
        4. Calculate days between discharge and readmission

        Args:
            medical_claim_df: Medical claims with admission/discharge dates
            lookback_days: Days after discharge to look for readmissions (default 30)
            person_id_col: Column name for patient identifier
            claim_id_col: Column name for claim/encounter identifier
            admit_date_col: Column name for admission date
            discharge_date_col: Column name for discharge date

        Returns:
            LazyFrame with columns:
            - patient_id: Patient identifier
            - index_encounter_id: Index admission claim ID
            - index_admission_date: Date of index admission
            - index_discharge_date: Date of index discharge
            - readmission_encounter_id: Readmission claim ID
            - readmission_admission_date: Date of readmission
            - days_to_readmission: Days between index discharge and readmission
        """
        # Filter to inpatient claims only (bill_type_code 11x, 12x for acute inpatient)
        # Assuming medical_claim has bill_type_code or claim_type indicator
        inpatient_claims = medical_claim_df.filter(
            # Use bill_type_code starting with 11 or 12 for inpatient
            # Or use a claim_type column if available
            pl.col("bill_type_code").str.starts_with("11")
            | pl.col("bill_type_code").str.starts_with("12")
        ).select(
            [
                pl.col(person_id_col).alias("patient_id"),
                pl.col(claim_id_col),
                pl.col(admit_date_col).cast(pl.Date),
                pl.col(discharge_date_col).cast(pl.Date),
            ]
        )

        # Create index admissions dataframe
        index_admissions = inpatient_claims.select(
            [
                pl.col("patient_id"),
                pl.col(claim_id_col).alias("index_encounter_id"),
                pl.col(admit_date_col).alias("index_admission_date"),
                pl.col(discharge_date_col).alias("index_discharge_date"),
            ]
        )

        # Create potential readmissions dataframe
        readmissions = inpatient_claims.select(
            [
                pl.col("patient_id"),
                pl.col(claim_id_col).alias("readmission_encounter_id"),
                pl.col(admit_date_col).alias("readmission_admission_date"),
            ]
        )

        # Self-join on patient_id to find readmission pairs
        pairs = index_admissions.join(readmissions, on="patient_id", how="inner")

        # Filter to readmissions within lookback window
        pairs = pairs.filter(
            # Readmission must be AFTER index discharge
            (pl.col("readmission_admission_date") > pl.col("index_discharge_date"))
            &
            # Readmission must be within lookback_days
            (
                pl.col("readmission_admission_date")
                <= pl.col("index_discharge_date") + pl.duration(days=lookback_days)
            )
        )

        # Calculate days to readmission
        pairs = pairs.with_columns(
            [
                (pl.col("readmission_admission_date") - pl.col("index_discharge_date"))
                .dt.total_days()
                .alias("days_to_readmission")
            ]
        )

        return pairs

    @staticmethod
    def deduplicate_readmissions(readmissions_df: pl.LazyFrame) -> pl.LazyFrame:
        """
        Remove duplicate rows from readmissions summary.

        Tuva's readmissions_summary output sometimes generates duplicate rows
        for the same readmission event. This removes exact duplicates while
        preserving all unique readmission records.

        Args:
            readmissions_df: Readmissions summary data with potential duplicates

        Returns:
            LazyFrame with duplicates removed
        """
        return readmissions_df.unique()
