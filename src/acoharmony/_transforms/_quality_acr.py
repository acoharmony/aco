# © 2025 HarmonyCares
# All rights reserved.

"""
ACR (NQF #1789) Risk-Standardized All-Condition Readmission Quality Measure.

Implements the ACO REACH All-Condition Readmission measure:
- Identifies eligible index admissions (age >= 65, acute inpatient)
- Assigns specialty cohorts (Surgery/Gyn, Cardiorespiratory, Cardiovascular, Neurology, Medicine)
- Classifies 30-day readmissions as planned or unplanned
- Calculates observed readmission rate at the ACO level
"""

from __future__ import annotations

import polars as pl

from .._decor8 import timeit, traced
from .._expressions._acr_readmission import AcrReadmissionExpression
from .._log import LogWriter
from ._quality_measure_base import MeasureFactory, MeasureMetadata, QualityMeasureBase

logger = LogWriter("transforms.quality_acr")


class AllConditionReadmission(QualityMeasureBase):
    """
    NQF1789: Risk-Standardized All-Condition Readmission.

        ACO REACH quality measure that calculates the rate of unplanned
        30-day readmissions among Medicare beneficiaries aged 65+.

        Lower rates are better.

        Denominator: Eligible acute inpatient index admissions (age >= 65)
        Numerator: Unplanned readmissions within 30 days of discharge
        Exclusions: Admissions with excluded CCS diagnosis categories
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF1789",
            measure_name="Risk-Standardized All-Condition Readmission",
            measure_steward="CMS",
            measure_version="2025",
            description="Rate of unplanned 30-day readmissions among Medicare "
            "beneficiaries aged 65+ with acute inpatient hospitalizations. "
            "Lower rates are better.",
            numerator_description="Unplanned readmissions within 30 days of index discharge",
            denominator_description="Eligible acute inpatient index admissions for "
            "patients aged 65+, excluding specified CCS diagnosis categories",
            exclusions_description="Index admissions with excluded CCS diagnosis categories "
            "(e.g., medical treatment of cancer, rehabilitation)",
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
        Calculate denominator: patients with eligible index admissions.

        Note: ACR operates at the admission level. This method adapts the
        admission-level denominator to the patient-level interface by
        returning unique patients with at least one eligible index admission.
        """
        measurement_year = self.config.get("measurement_year", 2025)

        config = {
            "performance_year": measurement_year,
            "min_age": 65,
            "lookback_days": 30,
        }

        index_admissions = AcrReadmissionExpression.identify_index_admissions(
            claims, eligibility, value_sets, config
        )

        denominator = (
            index_admissions.filter(~pl.col("exclusion_flag"))
            .select("person_id")
            .unique()
            .with_columns([pl.lit(True).alias("denominator_flag")])
        )

        # REACH alignment filter — CMS PY2025 QMMR §3.1.2 p11 ACR
        # denominator inclusion criterion #2: 'Patient is actively aligned
        # to a REACH ACO.' The mx_validate pipeline injects the per-PY
        # REACH-aligned bene list under value_sets['reach_aligned_persons']
        # (one column: person_id), built from
        # gold/consolidated_alignment.parquet using the alignment-eligible-
        # month rule (§3 p11). When absent, log and skip; the denominator
        # then includes the full claims-derived pool, with predictable
        # tieout drift.
        reach = value_sets.get("reach_aligned_persons")
        if reach is not None:
            denominator = denominator.join(
                reach.select("person_id"), on="person_id", how="inner"
            )
        else:
            logger.warning(
                "ACR denominator: value_sets['reach_aligned_persons'] not "
                "provided; REACH alignment criterion (§3.1.2 p11 #2) NOT "
                "enforced."
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
        Calculate numerator: patients with at least one unplanned readmission.

        Note: This adapts the admission-level numerator to the patient-level
        interface required by QualityMeasureBase.
        """
        self.config.get("measurement_year", 2025)

        # We need eligibility for index admission identification but
        # the base class doesn't pass it to calculate_numerator.
        # Use claims-only logic to identify readmission pairs.

        # Reconstruct index admissions from denominator members
        eligible_claims = claims.filter(
            (pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11"))
            & pl.col("admission_date").is_not_null()
        )

        # Build index admissions for denominator members only
        denom_members = denominator.select("person_id")
        index_for_denom = eligible_claims.join(
            denom_members, on="person_id", how="inner"
        ).select(
            [
                pl.col("claim_id").alias("index_claim_id"),
                pl.col("person_id"),
                pl.col("discharge_date").alias("index_discharge_date"),
            ]
        ).filter(pl.col("index_discharge_date").is_not_null())

        # Find readmissions
        candidate_admits = eligible_claims.select(
            [
                pl.col("claim_id").alias("readmission_claim_id"),
                pl.col("person_id"),
                pl.col("admission_date").alias("readmission_date"),
                pl.col("diagnosis_code_1").alias("readmit_diagnosis_code"),
            ]
        )

        pairs = index_for_denom.join(
            candidate_admits, on="person_id", how="inner"
        ).filter(
            (pl.col("readmission_claim_id") != pl.col("index_claim_id"))
            & (pl.col("readmission_date") > pl.col("index_discharge_date"))
            & (
                (pl.col("readmission_date") - pl.col("index_discharge_date")).dt.total_days()
                <= 30
            )
        )

        # Apply PAA Rule 2 exclusions
        paa2 = value_sets.get("paa2")
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

        if paa2 is not None and paa2.collect().height > 0:
            paa2_ccs = (
                paa2.select("ccs_diagnosis_category")
                .unique()
                .collect()["ccs_diagnosis_category"]
                .to_list()
            )
            unplanned_pairs = pairs.filter(
                ~pl.col("readmit_dx_ccs").is_in(paa2_ccs)
            )
        else:
            unplanned_pairs = pairs

        members_with_readmission = (
            unplanned_pairs.select("person_id").unique()
        )

        numerator = (
            denominator.join(
                members_with_readmission.with_columns(
                    [pl.lit(True).alias("_has_readmission")]
                ),
                on="person_id",
                how="left",
            )
            .with_columns(
                [pl.col("_has_readmission").fill_null(False).alias("numerator_flag")]
            )
            .select(["person_id", "numerator_flag"])
        )

        return numerator

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """ACR exclusions are applied during index admission identification."""
        return denominator.select("person_id").with_columns(
            [pl.lit(False).alias("exclusion_flag")]
        )


# Register quality measure for discovery via MeasureFactory
MeasureFactory.register("NQF1789", AllConditionReadmission)

logger.debug("Registered ACR (NQF #1789) quality measure and transform")
