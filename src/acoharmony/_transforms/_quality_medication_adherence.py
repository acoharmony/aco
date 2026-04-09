# © 2025 HarmonyCares
# All rights reserved.

"""
Medication adherence quality measures.

This module implements PQA (Pharmacy Quality Alliance) medication adherence measures:
- Statin Adherence for Patients with Cardiovascular Disease
- ACE Inhibitor or ARB Adherence for Patients with Diabetes
- Oral Diabetes Medication Adherence
- Hypertension Medication Adherence
"""

from __future__ import annotations

import polars as pl

from .._decor8 import timeit, traced
from .._log import LogWriter
from ._quality_measure_base import MeasureFactory, MeasureMetadata, QualityMeasureBase

logger = LogWriter("transforms.quality_medication_adherence")


class StatinAdherencePQA(QualityMeasureBase):
    """
    PQA Statin Adherence for Patients with Cardiovascular Disease.

        Measures the percentage of patients 18 years and older who were
        prescribed statin therapy and who had a Proportion of Days Covered
        (PDC) of at least 80% during the measurement period.

        Higher rates are better.

        Denominator: Patients 18+ prescribed statin therapy with CVD
        Numerator: Patients with PDC ≥80% for statins
        Exclusions: ESRD, hospice
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="PQA_STATIN",
            measure_name="Statin Medication Adherence (PDC ≥80%)",
            measure_steward="PQA",
            measure_version="2024",
            description="Percentage of patients 18+ with cardiovascular disease prescribed statin therapy "
            "who had Proportion of Days Covered (PDC) of at least 80% during the measurement period.",
            numerator_description="Patients with PDC ≥80% for statin medications",
            denominator_description="Patients 18+ with CVD prescribed statin therapy",
            exclusions_description="ESRD, hospice",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: Patients 18+ with CVD prescribed statins."""
        measurement_year = self.config.get("measurement_year", 2024)

        # Filter eligibility to measurement year and age 18+
        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age") >= 18)
        )

        # Get cardiovascular disease codes
        cvd_concepts = [
            "Myocardial Infarction",
            "Ischemic Vascular Disease",
            "Coronary Artery Disease",
        ]
        members_with_cvd_list = []

        for concept in cvd_concepts:
            cvd_codes = value_sets.get(concept)
            if cvd_codes is not None:
                cvd_claims = claims.join(
                    cvd_codes.select("code").unique(),
                    left_on="diagnosis_code_1",
                    right_on="code",
                    how="inner",
                )
                members = (
                    cvd_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
                    .select("person_id")
                    .unique()
                )
                members_with_cvd_list.append(members)

        if not members_with_cvd_list:
            logger.warning("CVD value sets not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        all_cvd_members = members_with_cvd_list[0]
        for cvd_df in members_with_cvd_list[1:]:
            all_cvd_members = pl.concat([all_cvd_members, cvd_df]).unique()

        # Check for statin prescriptions
        statin_codes = value_sets.get("Statin Medications", value_sets.get("Statins"))
        if statin_codes is None:
            logger.warning("Statin value set not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        # TODO: Check pharmacy_claim table for statin prescriptions
        # For now, use procedure_code as placeholder
        statin_claims = claims.join(
            statin_codes.select("code").unique(),
            left_on="procedure_code",
            right_on="code",
            how="inner",
        )

        members_with_statin = (
            statin_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        # Intersect: CVD + eligible + statin prescription
        denominator = (
            eligible_members.join(all_cvd_members, on="person_id", how="inner")
            .join(members_with_statin, on="person_id", how="inner")
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
        Calculate numerator: Patients with PDC ≥80%.

                PDC = (Total days covered by medication) / (Total days in measurement period)

                Note: Requires pharmacy_claim table with fill_date and days_supply.
        """
        logger.warning(
            "PDC calculation requires pharmacy_claim table - using placeholder logic for now"
        )

        # TODO: Implement actual PDC calculation using pharmacy_claim table
        # In production, would:
        # 1. Get all statin fills from pharmacy_claim
        # 2. Calculate days covered accounting for overlaps
        # 3. Divide by total days in measurement period (typically 365)
        # 4. Flag as numerator if PDC ≥ 0.80

        # Placeholder: Assume 65% have PDC ≥80%
        numerator = denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate exclusions: ESRD, hospice."""
        measurement_year = self.config.get("measurement_year", 2024)

        exclusion_concepts = ["ESRD", "Hospice Encounter"]
        excluded_members_list = []

        for concept in exclusion_concepts:
            concept_codes = value_sets.get(concept)
            if concept_codes is not None:
                excluded_claims = claims.join(
                    concept_codes.select("code").unique(),
                    left_on="diagnosis_code_1",
                    right_on="code",
                    how="inner",
                )
                excluded = (
                    excluded_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
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


class ACEARBAdherenceDiabetes(QualityMeasureBase):
    """
    PQA ACE Inhibitor or ARB Adherence for Patients with Diabetes.

        Measures the percentage of patients 18 years and older with diabetes
        who were prescribed ACE inhibitor or ARB therapy and who had a
        Proportion of Days Covered (PDC) of at least 80% during the
        measurement period.

        Higher rates are better.

        Denominator: Patients 18+ with diabetes prescribed ACE/ARB
        Numerator: Patients with PDC ≥80% for ACE/ARB
        Exclusions: ESRD, pregnancy, hospice
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="PQA_ACEARB",
            measure_name="ACE/ARB Medication Adherence for Diabetes (PDC ≥80%)",
            measure_steward="PQA",
            measure_version="2024",
            description="Percentage of patients 18+ with diabetes prescribed ACE/ARB therapy "
            "who had Proportion of Days Covered (PDC) of at least 80% during the measurement period.",
            numerator_description="Patients with PDC ≥80% for ACE inhibitors or ARBs",
            denominator_description="Patients 18+ with diabetes prescribed ACE inhibitor or ARB",
            exclusions_description="ESRD, pregnancy, hospice",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: Patients 18+ with diabetes prescribed ACE/ARB."""
        measurement_year = self.config.get("measurement_year", 2024)

        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age") >= 18)
        )

        # Get diabetes diagnosis codes
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

        # Get ACE/ARB prescription codes
        ace_arb_concepts = ["ACE Inhibitors", "ARBs"]
        members_with_med_list = []

        for concept in ace_arb_concepts:
            med_codes = value_sets.get(concept)
            if med_codes is not None:
                med_claims = claims.join(
                    med_codes.select("code").unique(),
                    left_on="procedure_code",
                    right_on="code",
                    how="inner",
                )
                members = (
                    med_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
                    .select("person_id")
                    .unique()
                )
                members_with_med_list.append(members)

        if not members_with_med_list:
            logger.warning("ACE/ARB value sets not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        all_med_members = members_with_med_list[0]
        for med_df in members_with_med_list[1:]:
            all_med_members = pl.concat([all_med_members, med_df]).unique()

        # Intersect: diabetes + eligible + ACE/ARB prescription
        denominator = (
            eligible_members.join(members_with_diabetes, on="person_id", how="inner")
            .join(all_med_members, on="person_id", how="inner")
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
        """Calculate numerator: Patients with PDC ≥80%."""
        logger.warning(
            "PDC calculation requires pharmacy_claim table - using placeholder logic for now"
        )

        # TODO: Implement actual PDC calculation
        numerator = denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate exclusions: ESRD, pregnancy, hospice."""
        measurement_year = self.config.get("measurement_year", 2024)

        exclusion_concepts = ["ESRD", "Pregnancy", "Hospice Encounter"]
        excluded_members_list = []

        for concept in exclusion_concepts:
            concept_codes = value_sets.get(concept)
            if concept_codes is not None:
                excluded_claims = claims.join(
                    concept_codes.select("code").unique(),
                    left_on="diagnosis_code_1",
                    right_on="code",
                    how="inner",
                )
                excluded = (
                    excluded_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
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


class OralDiabetesMedicationAdherence(QualityMeasureBase):
    """
    PQA Oral Diabetes Medication Adherence.

        Measures the percentage of patients 18 years and older with type 2
        diabetes who were prescribed oral diabetes medications and who had a
        Proportion of Days Covered (PDC) of at least 80% during the
        measurement period.

        Higher rates are better.

        Denominator: Patients 18+ with diabetes prescribed oral diabetes meds
        Numerator: Patients with PDC ≥80% for oral diabetes meds
        Exclusions: Type 1 diabetes, ESRD, hospice
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="PQA_DIABETES",
            measure_name="Oral Diabetes Medication Adherence (PDC ≥80%)",
            measure_steward="PQA",
            measure_version="2024",
            description="Percentage of patients 18+ with type 2 diabetes prescribed oral diabetes medications "
            "who had Proportion of Days Covered (PDC) of at least 80% during the measurement period.",
            numerator_description="Patients with PDC ≥80% for oral diabetes medications",
            denominator_description="Patients 18+ with type 2 diabetes prescribed oral diabetes medications",
            exclusions_description="Type 1 diabetes, ESRD, hospice",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: Patients 18+ with type 2 diabetes prescribed oral diabetes meds."""
        measurement_year = self.config.get("measurement_year", 2024)

        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age") >= 18)
        )

        # Get diabetes diagnosis codes (preferably type 2)
        diabetes_codes = value_sets.get("Diabetes Type 2", value_sets.get("Diabetes"))
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

        # Get oral diabetes medication codes
        oral_dm_codes = value_sets.get("Diabetes Medications")
        if oral_dm_codes is None:
            logger.warning("Oral diabetes medication value set not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        dm_med_claims = claims.join(
            oral_dm_codes.select("code").unique(),
            left_on="procedure_code",
            right_on="code",
            how="inner",
        )

        members_with_dm_med = (
            dm_med_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        # Intersect: diabetes + eligible + oral DM medication
        denominator = (
            eligible_members.join(members_with_diabetes, on="person_id", how="inner")
            .join(members_with_dm_med, on="person_id", how="inner")
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
        """Calculate numerator: Patients with PDC ≥80%."""
        logger.warning(
            "PDC calculation requires pharmacy_claim table - using placeholder logic for now"
        )

        # TODO: Implement actual PDC calculation
        numerator = denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate exclusions: Type 1 diabetes, ESRD, hospice."""
        measurement_year = self.config.get("measurement_year", 2024)

        exclusion_concepts = ["Diabetes Type 1", "ESRD", "Hospice Encounter"]
        excluded_members_list = []

        for concept in exclusion_concepts:
            concept_codes = value_sets.get(concept)
            if concept_codes is not None:
                excluded_claims = claims.join(
                    concept_codes.select("code").unique(),
                    left_on="diagnosis_code_1",
                    right_on="code",
                    how="inner",
                )
                excluded = (
                    excluded_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
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


class HypertensionMedicationAdherence(QualityMeasureBase):
    """
    PQA Hypertension Medication Adherence.

        Measures the percentage of patients 18 years and older with hypertension
        who were prescribed antihypertensive medications and who had a
        Proportion of Days Covered (PDC) of at least 80% during the
        measurement period.

        Higher rates are better.

        Denominator: Patients 18+ with hypertension prescribed antihypertensive meds
        Numerator: Patients with PDC ≥80% for antihypertensive meds
        Exclusions: ESRD, pregnancy, hospice
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="PQA_HYPERTENSION",
            measure_name="Hypertension Medication Adherence (PDC ≥80%)",
            measure_steward="PQA",
            measure_version="2024",
            description="Percentage of patients 18+ with hypertension prescribed antihypertensive medications "
            "who had Proportion of Days Covered (PDC) of at least 80% during the measurement period.",
            numerator_description="Patients with PDC ≥80% for antihypertensive medications",
            denominator_description="Patients 18+ with hypertension prescribed antihypertensive medications",
            exclusions_description="ESRD, pregnancy, hospice",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: Patients 18+ with hypertension prescribed antihypertensive meds."""
        measurement_year = self.config.get("measurement_year", 2024)

        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age") >= 18)
        )

        # Get hypertension diagnosis codes
        hypertension_codes = value_sets.get(
            "Essential Hypertension", value_sets.get("Hypertension")
        )
        if hypertension_codes is None:
            logger.warning("Hypertension value set not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        hypertension_claims = claims.join(
            hypertension_codes.select("code").unique(),
            left_on="diagnosis_code_1",
            right_on="code",
            how="inner",
        )

        members_with_hypertension = (
            hypertension_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
            .select("person_id")
            .unique()
        )

        # Get antihypertensive medication codes
        htn_med_concepts = [
            "ACE Inhibitors",
            "ARBs",
            "Beta Blockers",
            "Calcium Channel Blockers",
            "Diuretics",
        ]
        members_with_med_list = []

        for concept in htn_med_concepts:
            med_codes = value_sets.get(concept)
            if med_codes is not None:
                med_claims = claims.join(
                    med_codes.select("code").unique(),
                    left_on="procedure_code",
                    right_on="code",
                    how="inner",
                )
                members = (
                    med_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
                    .select("person_id")
                    .unique()
                )
                members_with_med_list.append(members)

        if not members_with_med_list:
            logger.warning(
                "Antihypertensive medication value sets not found, using empty denominator"
            )
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        all_med_members = members_with_med_list[0]
        for med_df in members_with_med_list[1:]:
            all_med_members = pl.concat([all_med_members, med_df]).unique()

        # Intersect: hypertension + eligible + antihypertensive medication
        denominator = (
            eligible_members.join(members_with_hypertension, on="person_id", how="inner")
            .join(all_med_members, on="person_id", how="inner")
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
        """Calculate numerator: Patients with PDC ≥80%."""
        logger.warning(
            "PDC calculation requires pharmacy_claim table - using placeholder logic for now"
        )

        # TODO: Implement actual PDC calculation
        numerator = denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate exclusions: ESRD, pregnancy, hospice."""
        measurement_year = self.config.get("measurement_year", 2024)

        exclusion_concepts = ["ESRD", "Pregnancy", "Hospice Encounter"]
        excluded_members_list = []

        for concept in exclusion_concepts:
            concept_codes = value_sets.get(concept)
            if concept_codes is not None:
                excluded_claims = claims.join(
                    concept_codes.select("code").unique(),
                    left_on="diagnosis_code_1",
                    right_on="code",
                    how="inner",
                )
                excluded = (
                    excluded_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
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


# Register all medication adherence measures
MeasureFactory.register("PQA_STATIN", StatinAdherencePQA)
MeasureFactory.register("PQA_ACEARB", ACEARBAdherenceDiabetes)
MeasureFactory.register("PQA_DIABETES", OralDiabetesMedicationAdherence)
MeasureFactory.register("PQA_HYPERTENSION", HypertensionMedicationAdherence)

logger.debug("Registered 4 medication adherence quality measures (PQA)")
