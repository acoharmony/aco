# © 2025 HarmonyCares
# All rights reserved.

"""
Preventive care quality measures.

This module implements quality measures related to preventive care:
- Breast Cancer Screening
- Colorectal Cancer Screening
- Annual Wellness Visits
"""

from __future__ import annotations

import polars as pl

from .._decor8 import timeit, traced
from .._log import LogWriter
from ._quality_measure_base import MeasureFactory, MeasureMetadata, QualityMeasureBase

logger = LogWriter("transforms.quality_preventive")


class BreastCancerScreening(QualityMeasureBase):
    """
    NQF2372/HEDIS BCS: Breast Cancer Screening.

        Measures the percentage of women 50-74 years of age who had a
        mammogram to screen for breast cancer in the past 27 months.

        Higher rates are better.

        Denominator: Women 50-74 years old
        Numerator: Women with mammogram in past 27 months
        Exclusions: Bilateral mastectomy, history of breast cancer
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF2372",
            measure_name="Breast Cancer Screening",
            measure_steward="NCQA",
            measure_version="2024",
            description="Percentage of women 50-74 years of age who had a mammogram "
            "to screen for breast cancer in the past 27 months.",
            numerator_description="Women with mammogram in past 27 months",
            denominator_description="Women 50-74 years old continuously enrolled",
            exclusions_description="Bilateral mastectomy, history of breast cancer",
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
        Calculate denominator: Women 50-74 years old.

                Args:
                    claims: Medical claims
                    eligibility: Member eligibility with age/enrollment/gender
                    value_sets: Value sets (not used for denominator)

                Returns:
                    LazyFrame with person_id and denominator_flag=True
        """
        measurement_year = self.config.get("measurement_year", 2024)

        # Filter eligibility to measurement year, age 50-74, female
        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age").is_between(50, 74))
            & (pl.col("gender").str.to_lowercase().is_in(["f", "female", "woman"]))
        )

        denominator = (
            eligible_members.select("person_id")
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
        Calculate numerator: Women with mammogram in past 27 months.

                Args:
                    denominator: Eligible women from calculate_denominator()
                    claims: Medical claims
                    value_sets: Value sets including "Mammography" concept

                Returns:
                    LazyFrame with person_id and numerator_flag=True
        """
        measurement_year = self.config.get("measurement_year", 2024)

        # Get mammography procedure codes from value sets
        mammography_codes = value_sets.get("Mammography")

        if mammography_codes is None or mammography_codes.collect().height == 0:
            logger.warning("Mammography value set not found, marking none as screened")
            return denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        # Find mammography procedures in past 27 months
        # Note: Simplified to measurement year only for now
        # In production, would check: measurement_year and (measurement_year - 1)
        mammography_claims = claims.join(
            mammography_codes.select("code").unique(),
            left_on="procedure_code",
            right_on="code",
            how="inner",
        )

        members_with_screening = (
            mammography_claims.filter(
                pl.col("claim_end_date").dt.year().is_in([measurement_year, measurement_year - 1])
            )
            .select("person_id")
            .unique()
        )

        numerator = (
            denominator.join(members_with_screening, on="person_id", how="left")
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
        """Calculate exclusions: Bilateral mastectomy, history of breast cancer."""
        measurement_year = self.config.get("measurement_year", 2024)

        exclusion_concepts = [
            "Bilateral Mastectomy",
            "History of Bilateral Mastectomy",
            "Breast Cancer",
        ]
        excluded_members_list = []

        for concept in exclusion_concepts:
            concept_codes = value_sets.get(concept)
            if concept_codes is not None:
                # Check any time before or during measurement year
                excluded_claims = claims.join(
                    concept_codes.select("code").unique(),
                    left_on="diagnosis_code_1",
                    right_on="code",
                    how="inner",
                )
                excluded = (
                    excluded_claims.filter(pl.col("claim_end_date").dt.year() <= measurement_year)
                    .select("person_id")
                    .unique()
                )
                excluded_members_list.append(excluded)

        if not excluded_members_list:
            return denominator.select("person_id").with_columns(
                [pl.lit(False).alias("exclusion_flag")]
            )

        all_excluded = excluded_members_list[0]
        for excluded_df in excluded_members_list[1:]:
            all_excluded = pl.concat([all_excluded, excluded_df]).unique()

        exclusions = (
            denominator.join(all_excluded, on="person_id", how="left")
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


class ColorectalCancerScreening(QualityMeasureBase):
    """
    NQF0034/HEDIS COL: Colorectal Cancer Screening.

        Measures the percentage of adults 45-75 years of age who had
        appropriate screening for colorectal cancer.

        Appropriate screening includes:
        - Fecal occult blood test (FOBT) during measurement year
        - FIT-DNA test every 3 years
        - Flexible sigmoidoscopy every 5 years
        - Colonoscopy every 10 years
        - CT colonography every 5 years

        Higher rates are better.

        Denominator: Adults 45-75 years old
        Numerator: Adults with appropriate colorectal cancer screening
        Exclusions: Colorectal cancer, total colectomy
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF0034",
            measure_name="Colorectal Cancer Screening",
            measure_steward="NCQA",
            measure_version="2024",
            description="Percentage of adults 45-75 years of age who had appropriate screening "
            "for colorectal cancer (FOBT, colonoscopy, sigmoidoscopy, or CT colonography).",
            numerator_description="Adults with appropriate colorectal cancer screening",
            denominator_description="Adults 45-75 years old continuously enrolled",
            exclusions_description="Colorectal cancer, total colectomy",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: Adults 45-75 years old."""
        measurement_year = self.config.get("measurement_year", 2024)

        # Filter eligibility to measurement year and age 45-75
        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age").is_between(45, 75))
        )

        denominator = (
            eligible_members.select("person_id")
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
        Calculate numerator: Adults with appropriate screening.

                Checks for any of the following:
                - FOBT in measurement year
                - Colonoscopy in past 10 years
                - Flexible sigmoidoscopy in past 5 years
                - CT colonography in past 5 years
        """
        measurement_year = self.config.get("measurement_year", 2024)

        # Different screening methods with different lookback periods
        screening_methods = [
            ("FOBT", 1),  # 1 year
            ("Colonoscopy", 10),  # 10 years
            ("Flexible Sigmoidoscopy", 5),  # 5 years
            ("CT Colonography", 5),  # 5 years
        ]

        members_with_screening_list = []

        for method_name, lookback_years in screening_methods:
            method_codes = value_sets.get(method_name)
            if method_codes is not None:
                screening_claims = claims.join(
                    method_codes.select("code").unique(),
                    left_on="procedure_code",
                    right_on="code",
                    how="inner",
                )

                # Check if screening occurred within lookback period
                # Simplified: Check if year is within range
                valid_years = list(
                    range(measurement_year - lookback_years + 1, measurement_year + 1)
                )
                members = (
                    screening_claims.filter(pl.col("claim_end_date").dt.year().is_in(valid_years))
                    .select("person_id")
                    .unique()
                )

                members_with_screening_list.append(members)

        if not members_with_screening_list:
            logger.warning("Colorectal screening value sets not found, marking none as screened")
            return denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        # Union all members with any screening method
        all_screened_members = members_with_screening_list[0]
        for screened_df in members_with_screening_list[1:]:
            all_screened_members = pl.concat([all_screened_members, screened_df]).unique()

        numerator = (
            denominator.join(all_screened_members, on="person_id", how="left")
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
        """Calculate exclusions: Colorectal cancer, total colectomy."""
        measurement_year = self.config.get("measurement_year", 2024)

        exclusion_concepts = ["Colorectal Cancer", "Total Colectomy"]
        excluded_members_list = []

        for concept in exclusion_concepts:
            concept_codes = value_sets.get(concept)
            if concept_codes is not None:
                # Check any time before or during measurement year
                excluded_claims = claims.join(
                    concept_codes.select("code").unique(),
                    left_on="diagnosis_code_1",
                    right_on="code",
                    how="inner",
                )
                excluded = (
                    excluded_claims.filter(pl.col("claim_end_date").dt.year() <= measurement_year)
                    .select("person_id")
                    .unique()
                )
                excluded_members_list.append(excluded)

        if not excluded_members_list:
            return denominator.select("person_id").with_columns(
                [pl.lit(False).alias("exclusion_flag")]
            )

        all_excluded = excluded_members_list[0]
        for excluded_df in excluded_members_list[1:]:
            all_excluded = pl.concat([all_excluded, excluded_df]).unique()

        exclusions = (
            denominator.join(all_excluded, on="person_id", how="left")
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


class AnnualWellnessVisit(QualityMeasureBase):
    """
    Annual Wellness Visit (AWV).

        Measures the percentage of patients who had an annual wellness visit
        or comprehensive preventive care visit during the measurement year.

        Higher rates are better.

        Denominator: All enrolled members
        Numerator: Members with annual wellness visit
        Exclusions: Hospice
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="AWV",
            measure_name="Annual Wellness Visit",
            measure_steward="CMS",
            measure_version="2024",
            description="Percentage of patients who had an annual wellness visit "
            "or comprehensive preventive care visit during the measurement year.",
            numerator_description="Members with annual wellness visit or preventive care visit",
            denominator_description="All continuously enrolled members",
            exclusions_description="Hospice care",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: All enrolled members."""
        measurement_year = self.config.get("measurement_year", 2024)

        # Filter eligibility to measurement year
        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
        )

        denominator = (
            eligible_members.select("person_id")
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
        """Calculate numerator: Members with annual wellness visit."""
        measurement_year = self.config.get("measurement_year", 2024)

        # Get wellness visit codes from value sets
        wellness_concepts = [
            "Annual Wellness Visit",
            "Preventive Care Services - Established Office Visit, 18 and Up",
        ]
        members_with_visit_list = []

        for concept in wellness_concepts:
            visit_codes = value_sets.get(concept)
            if visit_codes is not None:
                visit_claims = claims.join(
                    visit_codes.select("code").unique(),
                    left_on="procedure_code",
                    right_on="code",
                    how="inner",
                )
                members = (
                    visit_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
                    .select("person_id")
                    .unique()
                )
                members_with_visit_list.append(members)

        if not members_with_visit_list:
            logger.warning("Wellness visit value sets not found, marking none as having visit")
            return denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        # Union all members with any wellness visit
        all_visit_members = members_with_visit_list[0]
        for visit_df in members_with_visit_list[1:]:
            all_visit_members = pl.concat([all_visit_members, visit_df]).unique()

        numerator = (
            denominator.join(all_visit_members, on="person_id", how="left")
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
        """Calculate exclusions: Hospice care."""
        measurement_year = self.config.get("measurement_year", 2024)

        hospice_codes = value_sets.get("Hospice Encounter")
        if hospice_codes is None:
            return denominator.select("person_id").with_columns(
                [pl.lit(False).alias("exclusion_flag")]
            )

        hospice_claims = claims.join(
            hospice_codes.select("code").unique(),
            left_on="procedure_code",
            right_on="code",
            how="inner",
        )

        excluded_members = (
            hospice_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
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


# Register all preventive care measures
MeasureFactory.register("NQF2372", BreastCancerScreening)
MeasureFactory.register("NQF0034", ColorectalCancerScreening)
MeasureFactory.register("AWV", AnnualWellnessVisit)

logger.debug("Registered 3 preventive care quality measures")
