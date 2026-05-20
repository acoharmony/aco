# © 2025 HarmonyCares
# All rights reserved.

"""
Diabetes quality measures.

This module implements quality measures related to diabetes care:
- HbA1c Poor Control (>9%)
- HbA1c Testing
- Blood Pressure Control
- Eye Exam
- Nephropathy Screening
"""

from __future__ import annotations

import polars as pl

from .._decor8 import timeit, traced
from .._log import LogWriter
from ._quality_measure_base import MeasureFactory, MeasureMetadata, QualityMeasureBase

logger = LogWriter("transforms.quality_diabetes")


class DiabetesHbA1cPoorControl(QualityMeasureBase):
    """
    NQF0059: Diabetes HbA1c Poor Control (>9%).

        Measures the percentage of patients 18-75 years old with diabetes
        who had HbA1c >9.0% (poor control) during the measurement year.

        Lower rates are better (this is an inverse measure).

        Denominator: Patients 18-75 with diabetes
        Numerator: Patients with HbA1c >9.0% or no HbA1c test
        Exclusions: None
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF0059",
            measure_name="Diabetes: HbA1c Poor Control (>9%)",
            measure_steward="NQF",
            measure_version="2024",
            description="Percentage of patients 18-75 years old with diabetes (type 1 or type 2) "
            "who had hemoglobin A1c >9.0% during the measurement year.",
            numerator_description="Patients with HbA1c >9.0% or no HbA1c test during measurement year",
            denominator_description="Patients 18-75 years old with diabetes diagnosis",
            exclusions_description="None",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate denominator: Patients 18-75 with diabetes.

                Args:
                    claims: Medical claims with diagnosis codes
                    eligibility: Member eligibility with age/enrollment
                    value_sets: Value sets including "Diabetes" concept

                Returns:
                    LazyFrame with person_id and denominator_flag=True
        """
        # Get measurement year from config
        measurement_year = self.config.get("measurement_year", 2024)

        # Filter eligibility to measurement year and age 18-75
        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age").is_between(18, 75))
        )

        # Get diabetes diagnosis codes from value sets
        diabetes_codes = value_sets.get("Diabetes")
        if diabetes_codes is None:
            logger.warning("Diabetes value set not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        # Find members with diabetes diagnosis
        diabetes_claims = claims.join(
            diabetes_codes.select("code").unique(),
            left_on="diagnosis_code_1",
            right_on="code",
            how="inner",
        )

        # Members with diabetes in measurement year
        members_with_diabetes = (
            diabetes_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        # Intersect with eligible members
        denominator = (
            eligible_members.join(members_with_diabetes, on="person_id", how="inner")
            .select("person_id")
            .unique()
            .with_columns([pl.lit(True).alias("denominator_flag")])
        )

        return denominator

    @traced()
    @timeit(log_level="debug")
    def calculate_numerator(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate numerator: Patients with HbA1c >9.0%.

                Args:
                    denominator: Eligible patients from calculate_denominator()
                    claims: Medical claims (including lab results if available)
                    value_sets: Value sets including "HbA1c Lab Test" concept

                Returns:
                    LazyFrame with person_id and numerator_flag=True
        """
        # Get measurement year from config
        self.config.get("measurement_year", 2024)

        # Get HbA1c test codes from value sets
        hba1c_codes = value_sets.get(
            "HbA1c Lab Test", value_sets.get("HbA1c", pl.DataFrame(schema={"code": pl.Utf8}).lazy())
        )

        if hba1c_codes.collect().height == 0:
            logger.warning("HbA1c value set not found, marking all as numerator (poor control)")
            # If no lab data, assume all in denominator are in numerator (poor control)
            return denominator.with_columns([pl.lit(True).alias("numerator_flag")])

        # Find HbA1c tests in measurement year
        # Note: In a real implementation, we'd join with lab_result table
        # For now, we'll use a placeholder logic
        # TODO: Integrate with lab_result table when available

        # Placeholder: Assume 20% have poor control (>9%)
        # In production, this would check actual lab values
        numerator = (
            denominator.with_columns([pl.lit(False).alias("numerator_flag")])  # Placeholder
        )

        logger.warning(
            "HbA1c measure using placeholder logic - integrate with lab_result table for production"
        )

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate exclusions: None for this measure.

                Args:
                    denominator: Eligible patients
                    claims: Medical claims
                    value_sets: Value sets

                Returns:
                    LazyFrame with person_id and exclusion_flag=False (no exclusions)
        """
        return denominator.select("person_id").with_columns([pl.lit(False).alias("exclusion_flag")])


class DiabetesHbA1cTesting(QualityMeasureBase):
    """
    Diabetes HbA1c Testing.

        Measures the percentage of patients 18-75 years old with diabetes
        who had HbA1c testing during the measurement year.

        Higher rates are better.

        Denominator: Patients 18-75 with diabetes
        Numerator: Patients with at least one HbA1c test
        Exclusions: None
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF0061",
            measure_name="Diabetes: HbA1c Testing",
            measure_steward="NQF",
            measure_version="2024",
            description="Percentage of patients 18-75 years old with diabetes (type 1 or type 2) "
            "who had hemoglobin A1c testing during the measurement year.",
            numerator_description="Patients with at least one HbA1c test during measurement year",
            denominator_description="Patients 18-75 years old with diabetes diagnosis",
            exclusions_description="None",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate denominator: Patients 18-75 with diabetes.

                Reuses same logic as HbA1c Poor Control measure.
        """
        measurement_year = self.config.get("measurement_year", 2024)

        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age").is_between(18, 75))
        )

        diabetes_codes = value_sets.get("Diabetes")
        if diabetes_codes is None:
            logger.warning("Diabetes value set not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        diabetes_claims = claims.join(
            diabetes_codes.select("code").unique(),
            left_on="diagnosis_code_1",
            right_on="code",
            how="inner",
        )

        members_with_diabetes = (
            diabetes_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        denominator = (
            eligible_members.join(members_with_diabetes, on="person_id", how="inner")
            .select("person_id")
            .unique()
            .with_columns([pl.lit(True).alias("denominator_flag")])
        )

        return denominator

    @traced()
    @timeit(log_level="debug")
    def calculate_numerator(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate numerator: Patients with at least one HbA1c test.
        """
        measurement_year = self.config.get("measurement_year", 2024)

        hba1c_codes = value_sets.get(
            "HbA1c Lab Test", value_sets.get("HbA1c", pl.DataFrame(schema={"code": pl.Utf8}).lazy())
        )

        if hba1c_codes.collect().height == 0:
            logger.warning("HbA1c value set not found, marking none as tested")
            return denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        # Find HbA1c tests in measurement year (using procedure codes from claims)
        hba1c_claims = claims.join(
            hba1c_codes.select("code").unique(),
            left_on="procedure_code",
            right_on="code",
            how="inner",
        )

        members_with_test = (
            hba1c_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        numerator = (
            denominator.join(members_with_test, on="person_id", how="left")
            .with_columns(
                [
                    pl.when(pl.col("person_id").is_not_null())
                    .then(True)
                    .otherwise(False)
                    .alias("numerator_flag")
                ]
            )
            .select(["person_id", "denominator_flag", "numerator_flag"])
        )

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """No exclusions for this measure."""
        return denominator.select("person_id").with_columns([pl.lit(False).alias("exclusion_flag")])


class DiabetesBPControl(QualityMeasureBase):
    """
    Diabetes: Blood Pressure Control (<140/90 mmHg).

        Measures the percentage of patients 18-75 years old with diabetes
        who had blood pressure <140/90 mmHg during the measurement year.

        Higher rates are better.

        Denominator: Patients 18-75 with diabetes
        Numerator: Patients with BP <140/90 mmHg
        Exclusions: Patients with diagnosis of hypertension or end-stage renal disease
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF0061_BP",
            measure_name="Diabetes: Blood Pressure Control (<140/90)",
            measure_steward="NQF",
            measure_version="2024",
            description="Percentage of patients 18-75 years old with diabetes "
            "who had blood pressure <140/90 mmHg during the measurement year.",
            numerator_description="Patients with BP <140/90 mmHg during measurement year",
            denominator_description="Patients 18-75 years old with diabetes diagnosis",
            exclusions_description="Patients with diagnosis indicating pregnancy",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: Patients 18-75 with diabetes."""
        measurement_year = self.config.get("measurement_year", 2024)

        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age").is_between(18, 75))
        )

        diabetes_codes = value_sets.get("Diabetes")
        if diabetes_codes is None:
            logger.warning("Diabetes value set not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        diabetes_claims = claims.join(
            diabetes_codes.select("code").unique(),
            left_on="diagnosis_code_1",
            right_on="code",
            how="inner",
        )

        members_with_diabetes = (
            diabetes_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        denominator = (
            eligible_members.join(members_with_diabetes, on="person_id", how="inner")
            .select("person_id")
            .unique()
            .with_columns([pl.lit(True).alias("denominator_flag")])
        )

        return denominator

    @traced()
    @timeit(log_level="debug")
    def calculate_numerator(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate numerator: Patients with BP <140/90.

                Note: Requires clinical/vital signs data for actual BP values.
                Placeholder implementation uses proxy logic.
        """
        logger.warning(
            "BP Control measure using placeholder logic - integrate with vital_signs table for production"
        )

        # TODO: Integrate with vital_signs table when available
        # In production, would filter vital_signs where:
        # - systolic_bp < 140 AND diastolic_bp < 90
        # - measurement_date in measurement_year

        # Placeholder: Assume 60% have controlled BP
        numerator = denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate exclusions: Patients with pregnancy diagnosis."""
        measurement_year = self.config.get("measurement_year", 2024)

        pregnancy_codes = value_sets.get("Pregnancy")
        if pregnancy_codes is None:
            logger.debug("No pregnancy value set, no exclusions applied")
            return denominator.select("person_id").with_columns(
                [pl.lit(False).alias("exclusion_flag")]
            )

        pregnancy_claims = claims.join(
            pregnancy_codes.select("code").unique(),
            left_on="diagnosis_code_1",
            right_on="code",
            how="inner",
        )

        excluded_members = (
            pregnancy_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        exclusions = (
            denominator.join(excluded_members, on="person_id", how="left")
            .with_columns(
                [
                    pl.when(pl.col("person_id").is_not_null())
                    .then(True)
                    .otherwise(False)
                    .alias("exclusion_flag")
                ]
            )
            .select(["person_id", "exclusion_flag"])
        )

        return exclusions


class DiabetesEyeExam(QualityMeasureBase):
    """
    Diabetes: Eye Exam (Retinal) Performed.

        Measures the percentage of patients 18-75 years old with diabetes
        who had a retinal or dilated eye exam during the measurement year.

        Higher rates are better.

        Denominator: Patients 18-75 with diabetes
        Numerator: Patients with retinal/dilated eye exam
        Exclusions: None
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF0055",
            measure_name="Diabetes: Eye Exam (Retinal) Performed",
            measure_steward="NQF",
            measure_version="2024",
            description="Percentage of patients 18-75 years old with diabetes "
            "who had a retinal or dilated eye exam during the measurement year or negative retinal exam in prior year.",
            numerator_description="Patients with retinal or dilated eye exam during measurement year",
            denominator_description="Patients 18-75 years old with diabetes diagnosis",
            exclusions_description="None",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: Patients 18-75 with diabetes."""
        measurement_year = self.config.get("measurement_year", 2024)

        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age").is_between(18, 75))
        )

        diabetes_codes = value_sets.get("Diabetes")
        if diabetes_codes is None:
            logger.warning("Diabetes value set not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        diabetes_claims = claims.join(
            diabetes_codes.select("code").unique(),
            left_on="diagnosis_code_1",
            right_on="code",
            how="inner",
        )

        members_with_diabetes = (
            diabetes_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        denominator = (
            eligible_members.join(members_with_diabetes, on="person_id", how="inner")
            .select("person_id")
            .unique()
            .with_columns([pl.lit(True).alias("denominator_flag")])
        )

        return denominator

    @traced()
    @timeit(log_level="debug")
    def calculate_numerator(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate numerator: Patients with retinal/dilated eye exam."""
        measurement_year = self.config.get("measurement_year", 2024)

        eye_exam_codes = value_sets.get(
            "Diabetic Retinal Screening",
            value_sets.get("Eye Exam", pl.DataFrame(schema={"code": pl.Utf8}).lazy()),
        )

        if eye_exam_codes.collect().height == 0:
            logger.warning("Eye Exam value set not found, marking none as having exam")
            return denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        # Find eye exam procedures in measurement year
        eye_exam_claims = claims.join(
            eye_exam_codes.select("code").unique(),
            left_on="procedure_code",
            right_on="code",
            how="inner",
        )

        members_with_exam = (
            eye_exam_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        numerator = (
            denominator.join(members_with_exam, on="person_id", how="left")
            .with_columns(
                [
                    pl.when(pl.col("person_id").is_not_null())
                    .then(True)
                    .otherwise(False)
                    .alias("numerator_flag")
                ]
            )
            .select(["person_id", "denominator_flag", "numerator_flag"])
        )

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """No exclusions for this measure."""
        return denominator.select("person_id").with_columns([pl.lit(False).alias("exclusion_flag")])


# Register all diabetes measures
MeasureFactory.register("NQF0059", DiabetesHbA1cPoorControl)
MeasureFactory.register("NQF0061", DiabetesHbA1cTesting)
MeasureFactory.register("NQF0061_BP", DiabetesBPControl)
MeasureFactory.register("NQF0055", DiabetesEyeExam)

logger.debug("Registered 4 diabetes quality measures")
