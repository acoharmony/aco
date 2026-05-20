# © 2025 HarmonyCares
# All rights reserved.

"""
Lab Results Analysis Transform.

Provides comprehensive lab result tracking and analysis:
- Key lab test tracking (HbA1c, LDL, blood pressure, etc.)
- Diabetic control metrics (HbA1c control)
- Cardiovascular control metrics (LDL control)
- Abnormal result flagging
- Lab testing compliance
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import profile_memory, timeit, traced, transform
from .._log import LogWriter

logger = LogWriter("transforms.lab_results")


@transform(name="lab_results", tier=["gold"])
class LabResultsTransform:
    """
    Comprehensive lab result tracking and analysis.
    """

    # Standard LOINC codes for key lab tests
    LOINC_CODES = {
        # HbA1c
        "hba1c": ["4548-4", "17856-6", "59261-8"],
        # LDL Cholesterol
        "ldl": ["13457-7", "18262-6", "2089-1"],
        # Total Cholesterol
        "total_cholesterol": ["2093-3", "14647-2"],
        # HDL Cholesterol
        "hdl": ["2085-9", "14646-4"],
        # Triglycerides
        "triglycerides": ["2571-8", "14927-6"],
        # Blood Glucose
        "glucose": ["2339-0", "2345-7"],
        # Creatinine
        "creatinine": ["2160-0", "38483-4"],
        # eGFR
        "egfr": ["33914-3", "48642-3", "48643-1"],
        # Blood Pressure (Systolic/Diastolic)
        "bp_systolic": ["8480-6"],
        "bp_diastolic": ["8462-4"],
    }

    # Reference ranges for abnormal flagging
    REFERENCE_RANGES = {
        "hba1c": {
            "optimal": (None, 5.7),
            "controlled": (None, 7.0),
            "poor": (9.0, None),
            "unit": "%",
        },
        "ldl": {
            "optimal": (None, 100),
            "elevated": (130, None),
            "high": (160, None),
            "unit": "mg/dL",
        },
        "total_cholesterol": {
            "normal": (None, 200),
            "borderline": (200, 240),
            "high": (240, None),
            "unit": "mg/dL",
        },
        "hdl": {"low": (None, 40), "normal": (40, 60), "optimal": (60, None), "unit": "mg/dL"},
        "triglycerides": {
            "normal": (None, 150),
            "borderline": (150, 200),
            "high": (200, None),
            "unit": "mg/dL",
        },
        "glucose": {
            "normal": (70, 100),
            "prediabetes": (100, 126),
            "diabetes": (126, None),
            "unit": "mg/dL",
        },
        "creatinine": {"normal_male": (0.7, 1.3), "normal_female": (0.6, 1.1), "unit": "mg/dL"},
        "egfr": {
            "normal": (90, None),
            "mild_decrease": (60, 90),
            "moderate": (30, 60),
            "severe": (15, 30),
            "kidney_failure": (None, 15),
            "unit": "mL/min/1.73m²",
        },
        "bp_systolic": {
            "normal": (None, 120),
            "elevated": (120, 130),
            "stage1": (130, 140),
            "stage2": (140, None),
            "unit": "mmHg",
        },
        "bp_diastolic": {
            "normal": (None, 80),
            "elevated": (80, 80),
            "stage1": (80, 90),
            "stage2": (90, None),
            "unit": "mmHg",
        },
    }

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def identify_lab_tests(lab_results: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Identify and categorize lab tests by type.

                Args:
                    lab_results: Lab result data with LOINC codes
                    config: Configuration dict

                Returns:
                    LazyFrame with lab test categories
        """
        logger.info("Identifying lab tests...")

        # Categorize by LOINC code
        lab_categorized = lab_results.with_columns(
            [
                pl.when(pl.col("loinc_code").is_in(LabResultsTransform.LOINC_CODES["hba1c"]))
                .then(pl.lit("hba1c"))
                .when(pl.col("loinc_code").is_in(LabResultsTransform.LOINC_CODES["ldl"]))
                .then(pl.lit("ldl"))
                .when(
                    pl.col("loinc_code").is_in(
                        LabResultsTransform.LOINC_CODES["total_cholesterol"]
                    )
                )
                .then(pl.lit("total_cholesterol"))
                .when(pl.col("loinc_code").is_in(LabResultsTransform.LOINC_CODES["hdl"]))
                .then(pl.lit("hdl"))
                .when(pl.col("loinc_code").is_in(LabResultsTransform.LOINC_CODES["triglycerides"]))
                .then(pl.lit("triglycerides"))
                .when(pl.col("loinc_code").is_in(LabResultsTransform.LOINC_CODES["glucose"]))
                .then(pl.lit("glucose"))
                .when(pl.col("loinc_code").is_in(LabResultsTransform.LOINC_CODES["creatinine"]))
                .then(pl.lit("creatinine"))
                .when(pl.col("loinc_code").is_in(LabResultsTransform.LOINC_CODES["egfr"]))
                .then(pl.lit("egfr"))
                .when(pl.col("loinc_code").is_in(LabResultsTransform.LOINC_CODES["bp_systolic"]))
                .then(pl.lit("bp_systolic"))
                .when(pl.col("loinc_code").is_in(LabResultsTransform.LOINC_CODES["bp_diastolic"]))
                .then(pl.lit("bp_diastolic"))
                .otherwise(pl.lit("other"))
                .alias("lab_test_type")
            ]
        )

        logger.info("Lab tests identified")

        return lab_categorized

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def flag_abnormal_results(lab_results: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        """
        Flag abnormal lab results based on reference ranges.

                Args:
                    lab_results: Lab results with test types
                    config: Configuration dict

                Returns:
                    LazyFrame with abnormal flags
        """
        logger.info("Flagging abnormal results...")

        # HbA1c abnormal flags
        lab_flagged = lab_results.with_columns(
            [
                # HbA1c control status
                pl.when(pl.col("lab_test_type") == "hba1c")
                .then(
                    pl.when(pl.col("result_value").cast(pl.Float64) < 5.7)
                    .then(pl.lit("optimal"))
                    .when(pl.col("result_value").cast(pl.Float64) < 7.0)
                    .then(pl.lit("controlled"))
                    .when(pl.col("result_value").cast(pl.Float64) < 9.0)
                    .then(pl.lit("uncontrolled"))
                    .otherwise(pl.lit("poor_control"))
                )
                .alias("hba1c_control_status"),
                # LDL control status
                pl.when(pl.col("lab_test_type") == "ldl")
                .then(
                    pl.when(pl.col("result_value").cast(pl.Float64) < 100)
                    .then(pl.lit("optimal"))
                    .when(pl.col("result_value").cast(pl.Float64) < 130)
                    .then(pl.lit("near_optimal"))
                    .when(pl.col("result_value").cast(pl.Float64) < 160)
                    .then(pl.lit("borderline_high"))
                    .when(pl.col("result_value").cast(pl.Float64) < 190)
                    .then(pl.lit("high"))
                    .otherwise(pl.lit("very_high"))
                )
                .alias("ldl_control_status"),
                # Generic abnormal flag
                pl.when(
                    (
                        (pl.col("lab_test_type") == "hba1c")
                        & (pl.col("result_value").cast(pl.Float64) >= 9.0)
                    )
                    | (
                        (pl.col("lab_test_type") == "ldl")
                        & (pl.col("result_value").cast(pl.Float64) >= 160)
                    )
                    | (
                        (pl.col("lab_test_type") == "glucose")
                        & (pl.col("result_value").cast(pl.Float64) >= 126)
                    )
                    | (
                        (pl.col("lab_test_type") == "bp_systolic")
                        & (pl.col("result_value").cast(pl.Float64) >= 140)
                    )
                    | (
                        (pl.col("lab_test_type") == "bp_diastolic")
                        & (pl.col("result_value").cast(pl.Float64) >= 90)
                    )
                    | (
                        (pl.col("lab_test_type") == "egfr")
                        & (pl.col("result_value").cast(pl.Float64) < 60)
                    )
                )
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("is_abnormal"),
            ]
        )

        logger.info("Abnormal results flagged")

        return lab_flagged

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_diabetic_control(
        lab_results: pl.LazyFrame, diabetic_members: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate HbA1c control metrics for diabetic members.

                Args:
                    lab_results: Lab results with HbA1c tests
                    diabetic_members: List of members with diabetes
                    config: Configuration dict

                Returns:
                    LazyFrame with diabetic control metrics
        """
        logger.info("Calculating diabetic control metrics...")

        measurement_year = config.get("measurement_year", 2024)

        # Filter to HbA1c tests for diabetic members in measurement year
        hba1c_diabetic = lab_results.filter(
            (pl.col("lab_test_type") == "hba1c")
            & (pl.col("result_date").dt.year() == measurement_year)
        ).join(diabetic_members.select(["person_id"]), on="person_id", how="inner")

        # Get most recent HbA1c per member
        most_recent_hba1c = (
            hba1c_diabetic.sort("result_date", descending=True)
            .group_by("person_id")
            .agg(
                [
                    pl.col("result_value").first().alias("most_recent_hba1c"),
                    pl.col("result_date").first().alias("most_recent_date"),
                    pl.col("hba1c_control_status").first().alias("control_status"),
                    pl.count().alias("hba1c_test_count"),
                ]
            )
        )

        # Calculate control metrics
        control_metrics = most_recent_hba1c.with_columns(
            [
                (pl.col("most_recent_hba1c").cast(pl.Float64) < 7.0).alias("is_controlled_lt_7"),
                (pl.col("most_recent_hba1c").cast(pl.Float64) < 8.0).alias("is_controlled_lt_8"),
                (pl.col("most_recent_hba1c").cast(pl.Float64) >= 9.0).alias("is_poor_control"),
                (pl.col("hba1c_test_count") >= 2).alias("has_adequate_testing"),
            ]
        )

        logger.info("Diabetic control metrics calculated")

        return control_metrics

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_cvd_control(
        lab_results: pl.LazyFrame, cvd_members: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate LDL control metrics for CVD members.

                Args:
                    lab_results: Lab results with LDL tests
                    cvd_members: List of members with cardiovascular disease
                    config: Configuration dict

                Returns:
                    LazyFrame with CVD control metrics
        """
        logger.info("Calculating CVD control metrics...")

        measurement_year = config.get("measurement_year", 2024)

        # Filter to LDL tests for CVD members in measurement year
        ldl_cvd = lab_results.filter(
            (pl.col("lab_test_type") == "ldl")
            & (pl.col("result_date").dt.year() == measurement_year)
        ).join(cvd_members.select(["person_id"]), on="person_id", how="inner")

        # Get most recent LDL per member
        most_recent_ldl = (
            ldl_cvd.sort("result_date", descending=True)
            .group_by("person_id")
            .agg(
                [
                    pl.col("result_value").first().alias("most_recent_ldl"),
                    pl.col("result_date").first().alias("most_recent_date"),
                    pl.col("ldl_control_status").first().alias("control_status"),
                    pl.count().alias("ldl_test_count"),
                ]
            )
        )

        # Calculate control metrics
        control_metrics = most_recent_ldl.with_columns(
            [
                (pl.col("most_recent_ldl").cast(pl.Float64) < 100).alias("is_controlled_lt_100"),
                (pl.col("most_recent_ldl").cast(pl.Float64) < 70).alias("is_optimal_lt_70"),
                (pl.col("most_recent_ldl").cast(pl.Float64) >= 160).alias("is_high_risk"),
                (pl.col("ldl_test_count") >= 1).alias("has_testing"),
            ]
        )

        logger.info("CVD control metrics calculated")

        return control_metrics

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_testing_compliance(
        lab_results: pl.LazyFrame, eligibility: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Calculate lab testing compliance rates.

                Args:
                    lab_results: Lab results
                    eligibility: Member eligibility
                    config: Configuration dict

                Returns:
                    LazyFrame with testing compliance
        """
        logger.info("Calculating testing compliance...")

        measurement_year = config.get("measurement_year", 2024)

        # Count tests by member and type
        test_counts = (
            lab_results.filter(pl.col("result_date").dt.year() == measurement_year)
            .group_by(["person_id", "lab_test_type"])
            .agg([pl.count().alias("test_count")])
        )

        # Pivot to wide format (pivot is only available on DataFrame)
        test_compliance = test_counts.collect().pivot(
            index="person_id", on="lab_test_type", values="test_count"
        ).lazy()

        # Join with eligibility to get all members
        all_members = eligibility.select(["person_id"]).unique()
        compliance = all_members.join(test_compliance, on="person_id", how="left")

        # Fill nulls with 0
        compliance_cols = compliance.collect_schema().names()
        for test_type in ["hba1c", "ldl", "glucose", "creatinine", "egfr", "bp_systolic"]:
            if test_type in compliance_cols:
                compliance = compliance.with_columns([pl.col(test_type).fill_null(0)])

        logger.info("Testing compliance calculated")

        return compliance

    @staticmethod
    @traced()
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    def analyze_lab_results(
        lab_results: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        diabetic_members: pl.LazyFrame | None,
        cvd_members: pl.LazyFrame | None,
        config: dict[str, Any],
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Perform comprehensive lab result analysis.

                Args:
                    lab_results: Lab result data
                    eligibility: Member eligibility
                    diabetic_members: Optional members with diabetes
                    cvd_members: Optional members with CVD
                    config: Configuration dict

                Returns:
                    Tuple of (lab_categorized, abnormal_results, diabetic_control, cvd_control, testing_compliance)
        """
        logger.info("Starting lab results analysis...")

        lab_categorized = LabResultsTransform.identify_lab_tests(lab_results, config)

        lab_flagged = LabResultsTransform.flag_abnormal_results(lab_categorized, config)

        abnormal_results = lab_flagged.filter(pl.col("is_abnormal"))

        if diabetic_members is not None:
            diabetic_control = LabResultsTransform.calculate_diabetic_control(
                lab_flagged, diabetic_members, config
            )
        else:
            diabetic_control = pl.DataFrame(
                {
                    "person_id": [],
                    "most_recent_hba1c": [],
                    "control_status": [],
                    "is_controlled_lt_7": [],
                }
            ).lazy()

        if cvd_members is not None:
            cvd_control = LabResultsTransform.calculate_cvd_control(
                lab_flagged, cvd_members, config
            )
        else:
            cvd_control = pl.DataFrame(
                {
                    "person_id": [],
                    "most_recent_ldl": [],
                    "control_status": [],
                    "is_controlled_lt_100": [],
                }
            ).lazy()

        testing_compliance = LabResultsTransform.calculate_testing_compliance(
            lab_categorized, eligibility, config
        )

        logger.info("Lab results analysis complete")

        return lab_categorized, abnormal_results, diabetic_control, cvd_control, testing_compliance


logger.debug("Registered lab results expression")
