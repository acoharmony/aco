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

        result = self._apply_spec_denominator_exclusions(
            result, claims, eligibility, int(measurement_year)
        )

        return result

    @staticmethod
    def _apply_spec_denominator_exclusions(
        denom: pl.LazyFrame,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        py: int,
    ) -> pl.LazyFrame:
        """Apply CMS PY2025 QMMR §3.2.2 p13 denominator exclusions to UAMCC.

        Source: ACO REACH Model PY2025 Quality Measurement Methodology Report
        (https://www.cms.gov/files/document/py25-reach-qual-meas-meth-report.pdf),
        §3.2.2 p13 UAMCC Denominator Exclusions.

        Implements the three exclusions we have data for:

          (1) <12 months continuous Medicare A+B in the year before the
              measurement year. Spec rationale: 'to ensure adequate claims
              data to identify beneficiaries.'
          (2) <12 months continuous Medicare A+B during the measurement
              year. Spec note: beneficiaries who die or enter hospice
              during the measurement period are NOT excluded if they are
              continuously enrolled until death/hospice (the 12-month
              requirement is relaxed for these).
          (3) Patients enrolled in hospice during the year before the
              measurement year OR at the start of the measurement year.

        Three other spec exclusions are NOT applied here, with reason:
          (4) 'No PCQ E&M visit with attributed REACH provider' — needs
              provider attribution data not currently in this layer.
          (5) 'Not at risk for hospitalization at any time during the
              measurement year' — not derivable from claims alone.
          (6) 'Non-claims-based-aligned voluntarily aligned after Jan 1
              2025' — needs voluntary-alignment metadata.

        Each unimplemented exclusion is a separate ticket; their absence
        will surface as continued denominator over-broadness in the
        mx_validate tieout, which is the spec's intent for this matrix.
        """
        from datetime import date as _date

        py_start = _date(py, 1, 1)
        py_end = _date(py, 12, 31)
        prior_year_start = _date(py - 1, 1, 1)

        # Per-bene enrollment window collapsed to (min_start, max_end, dod).
        bene_enroll = (
            eligibility.select(
                [
                    "person_id",
                    pl.col("enrollment_start_date").cast(pl.Date, strict=False),
                    pl.col("enrollment_end_date").cast(pl.Date, strict=False),
                    pl.col("death_date").cast(pl.Date, strict=False).alias("dod"),
                ]
            )
            .group_by("person_id")
            .agg(
                [
                    pl.col("enrollment_start_date").min().alias("enroll_start"),
                    pl.col("enrollment_end_date").max().alias("enroll_end"),
                    pl.col("dod").min().alias("dod"),
                ]
            )
        )

        # Hospice enrollment timing: any hospice TOB 81x claim with
        # claim_start_date <= py_start counts as 'enrolled in hospice during
        # the year before the measurement year OR at the start of the
        # measurement year' per §3.2.2 p13 #3.
        # TOB encoding note: codebase strips the leading 0 (so '811',
        # '813' instead of '0811'/'0813'). Match first 2 chars of the
        # leading-zero-stripped TOB.
        cols_present = claims.collect_schema().names()
        if "claim_start_date" in cols_present and "bill_type_code" in cols_present:
            hospice_pre_py = (
                claims.filter(
                    pl.col("bill_type_code")
                    .cast(pl.Utf8)
                    .str.strip_prefix("0")
                    .str.slice(0, 2)
                    == "81"
                )
                .filter(pl.col("claim_start_date").cast(pl.Date, strict=False) <= pl.lit(py_start))
                .select("person_id")
                .unique()
            )
        else:
            hospice_pre_py = pl.LazyFrame(
                {"person_id": []}, schema={"person_id": pl.Utf8}
            )

        # Apply the three exclusions via inner-join keep-set.
        keep = bene_enroll.filter(
            # Exclusion (1): full prior-year continuous A+B.
            (pl.col("enroll_start") <= pl.lit(prior_year_start))
            # Exclusion (2): full PY continuous A+B, OR continuous up to
            # death/hospice during the PY.
            & (
                pl.col("enroll_end").is_null()
                | (pl.col("enroll_end") >= pl.lit(py_end))
                | (
                    pl.col("dod").is_not_null()
                    & (pl.col("enroll_end") >= pl.col("dod"))
                )
            )
        ).select("person_id")

        result = denom.join(keep, on="person_id", how="inner")

        # Exclusion (3): drop hospice-pre-PY benes.
        result = result.join(hospice_pre_py, on="person_id", how="anti")

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
