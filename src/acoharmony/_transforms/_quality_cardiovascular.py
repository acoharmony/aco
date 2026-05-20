# © 2025 HarmonyCares
# All rights reserved.

"""
Cardiovascular quality measures.

This module implements quality measures related to cardiovascular care:
- Controlling High Blood Pressure
- Ischemic Vascular Disease (IVD): Use of Aspirin or Another Antiplatelet
- Statin Therapy for Patients with Cardiovascular Disease
"""

from __future__ import annotations

import polars as pl

from .._decor8 import timeit, traced
from .._log import LogWriter
from ._quality_measure_base import MeasureFactory, MeasureMetadata, QualityMeasureBase

logger = LogWriter("transforms.quality_cardiovascular")


class ControllingHighBloodPressure(QualityMeasureBase):
    """
    NQF0018/HEDIS CBP: Controlling High Blood Pressure.

        Measures the percentage of members 18-85 years of age who had a
        diagnosis of hypertension and whose blood pressure was adequately
        controlled (<140/90 mmHg) during the measurement period.

        Higher rates are better.

        Denominator: Patients 18-85 with hypertension diagnosis
        Numerator: Patients with BP <140/90 mmHg
        Exclusions: Patients with ESRD, kidney transplant, pregnancy
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF0018",
            measure_name="Controlling High Blood Pressure",
            measure_steward="NCQA",
            measure_version="2024",
            description="Percentage of members 18-85 years of age with hypertension "
            "whose blood pressure was adequately controlled (<140/90) during measurement period.",
            numerator_description="Patients with BP <140/90 mmHg during measurement year",
            denominator_description="Patients 18-85 years old with hypertension diagnosis",
            exclusions_description="ESRD, kidney transplant, pregnancy, hospice",
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
        Calculate denominator: Patients 18-85 with hypertension.

                Args:
                    claims: Medical claims with diagnosis codes
                    eligibility: Member eligibility with age/enrollment
                    value_sets: Value sets including "Essential Hypertension" concept

                Returns:
                    LazyFrame with person_id and denominator_flag=True
        """
        measurement_year = self.config.get("measurement_year", 2024)

        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age").is_between(18, 85))
        )

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

        denominator = (
            eligible_members.join(members_with_hypertension, on="person_id", how="inner")
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
        """
        logger.warning(
            "BP Control measure using placeholder logic - integrate with vital_signs table for production"
        )

        # TODO: Integrate with vital_signs table
        # In production, would filter most recent BP reading where:
        # - systolic_bp < 140 AND diastolic_bp < 90
        # - measurement_date in measurement_year

        # Placeholder: Assume 55% have controlled BP
        numerator = denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate exclusions: ESRD, kidney transplant, pregnancy, hospice."""
        measurement_year = self.config.get("measurement_year", 2024)

        exclusion_concepts = ["ESRD", "Kidney Transplant", "Pregnancy", "Hospice Encounter"]
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


class IschemicVascularDiseaseAspirin(QualityMeasureBase):
    """
    NQF0068/HEDIS IVD: Use of Aspirin or Another Antiplatelet.

        Measures the percentage of patients 18 years and older who were
        discharged alive for acute myocardial infarction (AMI), coronary artery
        bypass graft (CABG) or percutaneous coronary intervention (PCI) who
        had documentation of use of aspirin or another antiplatelet during
        the measurement period.

        Higher rates are better.

        Denominator: Patients 18+ with IVD (AMI, CABG, PCI)
        Numerator: Patients with aspirin/antiplatelet prescription
        Exclusions: Patients with contraindications (bleeding disorders, peptic ulcer)
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF0068",
            measure_name="IVD: Use of Aspirin or Another Antiplatelet",
            measure_steward="NCQA",
            measure_version="2024",
            description="Percentage of patients 18+ with ischemic vascular disease "
            "who were prescribed aspirin or another antiplatelet during the measurement period.",
            numerator_description="Patients with aspirin or antiplatelet prescription",
            denominator_description="Patients 18+ with IVD (AMI, CABG, PCI, IVD diagnosis)",
            exclusions_description="Bleeding disorders, peptic ulcer, anticoagulant therapy contraindication",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: Patients 18+ with IVD."""
        measurement_year = self.config.get("measurement_year", 2024)

        # Filter eligibility to measurement year and age 18+
        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age") >= 18)
        )

        # Get IVD diagnosis/procedure codes from value sets
        ivd_concepts = [
            "Acute Myocardial Infarction",
            "Coronary Artery Bypass Graft",
            "Percutaneous Coronary Intervention",
            "Ischemic Vascular Disease",
        ]
        members_with_ivd_list = []

        for concept in ivd_concepts:
            ivd_codes = value_sets.get(concept)
            if ivd_codes is not None:
                ivd_claims = claims.join(
                    ivd_codes.select("code").unique(),
                    left_on="diagnosis_code_1",
                    right_on="code",
                    how="inner",
                )
                members = (
                    ivd_claims.filter(pl.col("claim_end_date").dt.year() == measurement_year)
                    .select("person_id")
                    .unique()
                )
                members_with_ivd_list.append(members)

        if not members_with_ivd_list:
            logger.warning("IVD value sets not found, using empty denominator")
            return (
                eligible_members.select("person_id")
                .head(0)
                .with_columns([pl.lit(True).alias("denominator_flag")])
            )

        # Union all members with any IVD diagnosis
        all_ivd_members = members_with_ivd_list[0]
        for ivd_df in members_with_ivd_list[1:]:
            all_ivd_members = pl.concat([all_ivd_members, ivd_df]).unique()

        # Intersect with eligible members
        denominator = (
            eligible_members.join(all_ivd_members, on="person_id", how="inner")
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
        """Calculate numerator: Patients with aspirin/antiplatelet prescription."""
        measurement_year = self.config.get("measurement_year", 2024)

        # Get aspirin/antiplatelet medication codes from value sets
        antiplatelet_concepts = ["Aspirin", "Antiplatelet Medications"]
        members_with_medication_list = []

        for concept in antiplatelet_concepts:
            med_codes = value_sets.get(concept)
            if med_codes is not None:
                # Note: In production, would check pharmacy_claim table
                # For now, check if procedure_code matches (placeholder)
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
                members_with_medication_list.append(members)

        if not members_with_medication_list:
            logger.warning("Antiplatelet value sets not found, marking none as having medication")
            return denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        # Union all members with any antiplatelet medication
        all_med_members = members_with_medication_list[0]
        for med_df in members_with_medication_list[1:]:
            all_med_members = pl.concat([all_med_members, med_df]).unique()

        numerator = (
            denominator.join(all_med_members, on="person_id", how="left")
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
        """Calculate exclusions: Bleeding disorders, peptic ulcer."""
        measurement_year = self.config.get("measurement_year", 2024)

        exclusion_concepts = ["Bleeding Disorders", "Peptic Ulcer"]
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


class StatinTherapyCardiovascular(QualityMeasureBase):
    """
    Statin Therapy for Patients with Cardiovascular Disease.

        Measures the percentage of patients 21 years and older who were
        diagnosed with or had a previous myocardial infarction (MI), coronary
        artery bypass graft (CABG), percutaneous coronary intervention (PCI),
        carotid intervention, peripheral vascular disease (PVD), and who were
        prescribed statin therapy during the measurement period.

        Higher rates are better.

        Denominator: Patients 21+ with cardiovascular disease
        Numerator: Patients prescribed statin therapy
        Exclusions: Patients with contraindications (pregnancy, ESRD, cirrhosis)
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF0439",
            measure_name="Statin Therapy for Patients with Cardiovascular Disease",
            measure_steward="NCQA",
            measure_version="2024",
            description="Percentage of patients 21+ with cardiovascular disease "
            "who were prescribed statin therapy during the measurement period.",
            numerator_description="Patients prescribed statin therapy",
            denominator_description="Patients 21+ with cardiovascular disease (MI, CABG, PCI, CVD)",
            exclusions_description="Pregnancy, ESRD on dialysis, cirrhosis",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: Patients 21+ with cardiovascular disease."""
        measurement_year = self.config.get("measurement_year", 2024)

        # Filter eligibility to measurement year and age 21+
        eligible_members = eligibility.filter(
            (pl.col("enrollment_start_date").dt.year() <= measurement_year)
            & (pl.col("enrollment_end_date").dt.year() >= measurement_year)
            & (pl.col("age") >= 21)
        )

        # Get cardiovascular disease codes from value sets
        cvd_concepts = [
            "Myocardial Infarction",
            "Coronary Artery Bypass Graft",
            "Percutaneous Coronary Intervention",
            "Carotid Intervention",
            "Peripheral Vascular Disease",
            "Ischemic Vascular Disease",
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

        # Union all members with any CVD diagnosis
        all_cvd_members = members_with_cvd_list[0]
        for cvd_df in members_with_cvd_list[1:]:
            all_cvd_members = pl.concat([all_cvd_members, cvd_df]).unique()

        # Intersect with eligible members
        denominator = (
            eligible_members.join(all_cvd_members, on="person_id", how="inner")
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
        """Calculate numerator: Patients prescribed statin therapy."""
        measurement_year = self.config.get("measurement_year", 2024)

        statin_codes = value_sets.get("Statin Medications", value_sets.get("Statins"))

        if statin_codes is None or statin_codes.collect().height == 0:
            logger.warning("Statin value set not found, marking none as having medication")
            return denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        # Note: In production, would check pharmacy_claim table
        # For now, check if procedure_code matches (placeholder)
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

        numerator = (
            denominator.join(members_with_statin, on="person_id", how="left")
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
        """Calculate exclusions: Pregnancy, ESRD on dialysis, cirrhosis."""
        measurement_year = self.config.get("measurement_year", 2024)

        exclusion_concepts = ["Pregnancy", "ESRD", "Cirrhosis"]
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


# Register all cardiovascular measures
MeasureFactory.register("NQF0018", ControllingHighBloodPressure)
MeasureFactory.register("NQF0068", IschemicVascularDiseaseAspirin)
MeasureFactory.register("NQF0439", StatinTherapyCardiovascular)

logger.debug("Registered 3 cardiovascular quality measures")
