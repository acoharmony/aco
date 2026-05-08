# © 2025 HarmonyCares
# All rights reserved.

"""
UAMCC (NQF #2888) All-Cause Unplanned Admissions for Patients with
Multiple Chronic Conditions Quality Measure.

Implements the ACO REACH UAMCC measure:
- Identifies MCC cohort (9 chronic condition groups)
- Builds denominator (age >= 66, 2+ MCC groups)
- Classifies admissions via Planned Admission Algorithm v4.0
- Applies outcome exclusions
- Calculates person-time at risk
- Computes observed rate per 100 person-years
"""

from __future__ import annotations

import polars as pl

from .._decor8 import timeit, traced
from .._expressions._uamcc import UamccExpression
from .._log import LogWriter
from ._quality_measure_base import MeasureFactory, MeasureMetadata, QualityMeasureBase

logger = LogWriter("transforms.quality_uamcc")


class AllCauseUnplannedAdmissions(QualityMeasureBase):
    """
    NQF2888: All-Cause Unplanned Admissions for Patients with MCCs.

        ACO REACH quality measure that calculates the rate of unplanned
        acute admissions per 100 person-years among Medicare beneficiaries
        aged 66+ with 2 or more chronic condition groups.

        Lower rates are better.

        Denominator: Medicare FFS beneficiaries aged 66+ with 2+ MCC groups
        Numerator: Unplanned acute inpatient admissions
        Exclusions: Planned admissions (PAA v4.0), complications, injuries
    """

    def get_metadata(self) -> MeasureMetadata:
        """Get measure metadata."""
        return MeasureMetadata(
            measure_id="NQF2888",
            measure_name="All-Cause Unplanned Admissions for Patients with MCCs",
            measure_steward="CMS",
            measure_version="2025",
            description="Rate of unplanned acute admissions per 100 person-years "
            "among Medicare FFS beneficiaries aged 66+ with 2 or more "
            "chronic condition groups. Lower rates are better.",
            numerator_description="Unplanned acute inpatient admissions during "
            "the performance year",
            denominator_description="Medicare FFS beneficiaries aged 66+ with "
            "diagnoses in 2 or more of 9 chronic condition groups",
            exclusions_description="Planned admissions (PAA v4.0), procedure "
            "complications (CCS 145/237/238/257), injuries (CCS 2601-2621)",
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate denominator: patients with 2+ MCC groups, aged 66+.

        When ``self.config`` includes ``program``, ``aco_id``, and
        ``silver_path``, also restricts to that program's PY-aligned
        beneficiaries (today only ``program="REACH"``).
        """
        measurement_year = self.config.get("measurement_year", 2025)

        config = {
            "performance_year": measurement_year,
            "min_age": 66,
            "min_mcc_groups": 2,
            "program": self.config.get("program"),
            "aco_id": self.config.get("aco_id"),
            "silver_path": self.config.get("silver_path"),
        }

        mcc_cohort = UamccExpression.identify_mcc_cohort(
            claims, value_sets.get("cohort", pl.DataFrame().lazy()), config
        )

        denominator = UamccExpression.build_denominator(
            mcc_cohort, eligibility, config
        )

        result = denominator.select("person_id").with_columns(
            [pl.lit(True).alias("denominator_flag")]
        )

        # REACH alignment filter — CMS PY2025 QMMR §3.2.2 p13 UAMCC
        # denominator inclusion requirement: 'aligned beneficiaries who
        # are 66 years of age or older at the start of the measurement
        # period.' Alignment is the implicit prefix of every denominator
        # statement; the mx_validate pipeline injects the per-PY
        # REACH-aligned bene list under value_sets['reach_aligned_persons']
        # (one column: person_id), built from
        # gold/consolidated_alignment.parquet using the alignment-eligible-
        # month rule (§3 p11). Without this filter UAMCC over-counts the
        # denominator by ~45× the BLQQR ref.
        reach = value_sets.get("reach_aligned_persons")
        if reach is not None:
            result = result.join(
                reach.select("person_id"), on="person_id", how="inner"
            )
        # No warning when absent — the existing program/aco_id config-driven
        # path can also restrict the cohort; we don't want duplicate alerts.

        return result

    @traced()
    @timeit(log_level="debug")
    def calculate_numerator(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """Calculate numerator: count of unplanned admissions per beneficiary.

        Returns one row per denominator member with:
            count_unplanned_adm: int — number of distinct unplanned admissions
            numerator_flag: bool — count_unplanned_adm > 0 (kept for callers
                that only need the binary)

        CMS BLQQR_UAMCC reports admissions as a count, not a flag. Earlier
        versions of this method collapsed to a binary, which under-reported
        the true admission count for repeat-admitters.
        """
        measurement_year = self.config.get("measurement_year", 2025)

        config = {
            "performance_year": measurement_year,
            "min_age": 66,
            "min_mcc_groups": 2,
        }

        planned = UamccExpression.classify_planned_admissions(
            claims, value_sets, config
        )
        with_exclusions = UamccExpression.apply_outcome_exclusions(
            planned, value_sets, config
        )
        # Collapse contiguous inpatient stays into one spell before
        # counting (transfers + same-day re-admits + admit-from-acute).
        # Counting per claim_id over-reports admissions vs CMS by ~50%.
        spells = UamccExpression.link_admission_spells(with_exclusions)

        per_person_counts = (
            spells.filter(~pl.col("is_excluded"))
            .join(
                denominator.select("person_id"),
                on="person_id",
                how="inner",
            )
            .group_by("person_id")
            .agg(pl.col("spell_id").n_unique().alias("count_unplanned_adm"))
        )

        return (
            denominator.select("person_id")
            .join(per_person_counts, on="person_id", how="left")
            .with_columns(
                [
                    pl.col("count_unplanned_adm")
                    .fill_null(0)
                    .cast(pl.Int64)
                    .alias("count_unplanned_adm"),
                ]
            )
            .with_columns(
                [(pl.col("count_unplanned_adm") > 0).alias("numerator_flag")]
            )
            .select(["person_id", "count_unplanned_adm", "numerator_flag"])
        )

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """UAMCC exclusions are applied at the admission level via PAA."""
        return denominator.select("person_id").with_columns(
            [pl.lit(False).alias("exclusion_flag")]
        )


# Register quality measure for discovery via MeasureFactory
MeasureFactory.register("NQF2888", AllCauseUnplannedAdmissions)

logger.debug("Registered UAMCC (NQF #2888) quality measure")
