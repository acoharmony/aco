# © 2025 HarmonyCares
# All rights reserved.

"""
DAH (Days at Home) ACO REACH quality measure — per-beneficiary observed values.

Implements the per-bene observation that BLQQR DAH publishes (the
``observed_dah`` column we tie out against). Risk adjustment, mortality
adjustment, and nursing-home-transition adjustment all happen *downstream*
at the ACO level (CMS spec §3.3.1) and are not in scope here — what we
compute is the raw observed value that feeds those adjustments.

Spec source
-----------
- Title:   ACO REACH Model — PY 2025 Quality Measurement Methodology Report
- Issuer:  CMS Innovation Center, prepared by RTI International (May 2025)
- URL:     https://www.cms.gov/files/document/py25-reach-qual-meas-meth-report.pdf
- Sections used here: §3.3 (DAH overall), §3.3.1 (DAH Summary p14),
  §3.3.2 (DAH Denominator and Numerator Information p15)

All spec citations in this module reference that PDF unless noted.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from .._decor8 import timeit, traced
from .._log import LogWriter
from ._quality_measure_base import MeasureFactory, MeasureMetadata, QualityMeasureBase

logger = LogWriter("transforms.quality_dah")

# UB-04 Type-of-Bill (TOB) facility-type prefixes that count as "day in care"
# per §3.3.2 p15. Spec list: short-term acute care hospitals, critical access
# hospitals, IRFs, IPFs, LTCHs, SNFs, EDs (handled separately via
# revenue/HCPCS), observation stays (handled separately via revenue/HCPCS).
#
# TOB encoding note: the canonical UB-04 form is 4 chars with a leading
# zero (e.g. "0111"), but Medicare claim files often store the 3-char form
# without the leading zero (e.g. "111"). This codebase's gold/medical_claim
# uses the 3-char form, so we match the FIRST TWO digits of the
# leading-zero-stripped TOB to identify facility type:
#   "11"   hospital inpatient (acute IP, also subsumes IRF + LTCH which
#          are distinguished by CCN at the facility level — we cannot
#          disambiguate without a CCN crosswalk, so we count all 11x)
#   "18"   hospital swing-bed (counted as SNF-equivalent per CMS)
#   "21"   SNF inpatient
#   "28"   SNF swing-bed
#   "41"   religious non-medical health care institution inpatient (rare)
#   "85"   CAH (critical access hospital)
#   "86"   IPF (inpatient psychiatric facility)
_DIC_TOB_PREFIXES: tuple[str, ...] = ("11", "18", "21", "28", "41", "85", "86")

# TOB prefix for hospice — per §3.3.2 p15, hospice ALWAYS counts as "at home"
# even if the bene receives care in an otherwise-DIC setting.
_HOSPICE_TOB_PREFIX = "81"

# ED visit identification per CMS — outpatient claim with revenue center in
# the ED range or a CPT/HCPCS in the ED E&M family.
# Source: CMS NUBC revenue codes 0450–0459 = "Emergency Room"; CPT 99281–99285
# = ED E&M; HCPCS G0380–G0384 = Type B ED visits.
_ED_REVENUE_PREFIX = "045"  # 0450–0459
_ED_HCPCS_CODES: frozenset[str] = frozenset(
    {"99281", "99282", "99283", "99284", "99285", "G0380", "G0381", "G0382", "G0383", "G0384"}
)

# Observation stay identification per CMS — revenue center 0762, or HCPCS
# G0378 (observation per hour) / G0379 (direct admission to observation).
_OBS_REVENUE_CODE = "0762"
_OBS_HCPCS_CODES: frozenset[str] = frozenset({"G0378", "G0379"})

# ICD-10-CM categories whose hospital admissions DO NOT count as DIC per
# §3.3.2 p15 numerator carve-out #2: childbirth, miscarriage, termination.
# Codes:
#   O00–O08  pregnancy with abortive outcome (incl. miscarriage, termination)
#   O80–O82  encounter for delivery
#   Z37.x    outcome of delivery
# We match by ICD-10-CM prefix on diagnosis_code_1 of the admission.
_OBSTETRIC_DX_PREFIXES: tuple[str, ...] = (
    "O00", "O01", "O02", "O03", "O04", "O05", "O06", "O07", "O08",
    "O80", "O81", "O82",
    "Z37",
)


class DaysAtHome(QualityMeasureBase):
    """
    ACO REACH Days at Home (DAH) — per-beneficiary observed value.

    Per §3.3.1 (p14) and §3.3.2 (p15) of the PY2025 QMMR. The observed
    per-bene value is what BLQQR DAH publishes and what we tie out against;
    the ACO-level risk/mortality/NH-transition adjustments are downstream.
    """

    def get_metadata(self) -> MeasureMetadata:
        """
        DAH measure identity per §3.3.1 (PY2025 QMMR p14).

        URL: https://www.cms.gov/files/document/py25-reach-qual-meas-meth-report.pdf
        """
        return MeasureMetadata(
            measure_id="REACH_DAH",
            measure_name="Days at Home for Patients with Complex, Chronic Conditions",
            measure_steward="CMS",
            measure_version="PY2025",
            description=(
                "Days spent at home or in community settings — not in acute, "
                "post-acute, or institutional care — by adult Medicare FFS "
                "beneficiaries with complex chronic conditions aligned to a "
                "REACH ACO. Higher is better. Applies to High Needs Population "
                "ACOs only (§2.1 Table 2.1)."
            ),
            numerator_description=(
                "Eligible days in the measurement year that are NOT 'days in care', "
                "where DIC = days receiving care in acute IP, CAH, IRF, IPF, LTCH, "
                "SNF, ED, or observation. Hospice days always count as at-home; "
                "obstetric admissions are excluded from DIC. (§3.3.2 p15)"
            ),
            denominator_description=(
                "Adult (≥18) Medicare FFS bene aligned to a REACH ACO, alive on "
                "day 1 of PY, continuously enrolled in Parts A & B for the full "
                "PY (up to dod) and the full year prior, with avg HCC composite "
                "risk score ≥ 2.0 in the year before the PY. (§3.3.2 p15)"
            ),
            exclusions_description=(
                "Non-claims-based-aligned beneficiaries who were voluntarily "
                "aligned after Jan 1 of the PY are excluded. (§3.3.2 p15)"
            ),
        )

    def _period_bounds(self) -> tuple[date, date]:
        """
        Measurement period = full calendar year of the PY.

        Per §3 p11: 'For PY 2025, the final Quality Measure scores will be
        based on a performance period that covers January 1, 2025, through
        December 31, 2025.' We use the same full-calendar-year window for
        any PY this transform is run on.
        """
        py: int = int(self.config.get("performance_year", 2025))
        return date(py, 1, 1), date(py, 12, 31)

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Eligible beneficiaries per §3.3.2 (PY2025 QMMR p15):

            1. Adult (≥18 years old).
            2. Alive on the first day of the PY.
            3. Continuously enrolled in Medicare FFS Parts A AND B during
               the full PY (up to dod for those who died) AND the full year
               prior to PY start.
            4. Avg HCC composite risk score ≥ 2.0 in the year before the PY.
            5. Aligned to a participating REACH ACO.

        Denominator exclusion (§3.3.2 p15): non-claims-based-aligned
        beneficiaries voluntarily aligned after Jan 1 of the PY. We don't
        have voluntary-alignment metadata at this layer, so the exclusion
        is enforced upstream by the ACO-alignment join; this method does
        not re-apply it.

        Inputs:
            eligibility: gold/eligibility.parquet — needs person_id,
                birth_date, death_date, enrollment_start_date,
                enrollment_end_date.
            value_sets['hcc_scores']: gold/hcc_risk_scores.parquet
                pre-filtered to performance_year == py - 1. Required for
                the HCC ≥ 2.0 lookback (criterion 4). If missing, we skip
                the HCC filter and log a warning rather than silently
                dropping all benes.
            value_sets['mbi_to_person']: optional crosswalk LazyFrame with
                columns mbi, person_id. hcc_risk_scores is keyed by mbi;
                eligibility by person_id.
        """
        period_start, period_end = self._period_bounds()
        py = period_start.year
        prior_year_start = date(py - 1, 1, 1)

        # Criterion 2 + 3: alive on day 1 of PY, continuous A+B for full PY
        # (we approximate "Parts A and B" with the existing A-or-B enrollment
        # span columns; eligibility doesn't currently split A vs B), AND
        # 12-month prior lookback. The lookback requires enrollment_start
        # to be on/before prior_year_start.
        base = (
            eligibility.select(
                [
                    "person_id",
                    pl.col("birth_date").cast(pl.Date, strict=False),
                    pl.col("death_date").cast(pl.Date, strict=False),
                    pl.col("enrollment_start_date").cast(pl.Date, strict=False),
                    pl.col("enrollment_end_date").cast(pl.Date, strict=False),
                ]
            )
            .group_by("person_id")
            .agg(
                [
                    pl.col("birth_date").min().alias("dob"),
                    pl.col("death_date").min().alias("dod"),
                    pl.col("enrollment_start_date").min().alias("enroll_start"),
                    pl.col("enrollment_end_date").max().alias("enroll_end"),
                ]
            )
            # Criterion 1: adult on day 1 of PY (age >= 18 at PY start).
            .filter(
                pl.col("dob").is_not_null()
                & ((pl.lit(period_start) - pl.col("dob")).dt.total_days() // 365 >= 18)
            )
            # Criterion 2: alive on day 1 of PY.
            .filter(
                pl.col("dod").is_null() | (pl.col("dod") >= pl.lit(period_start))
            )
            # Criterion 3: 12-month prior lookback satisfied.
            .filter(pl.col("enroll_start") <= pl.lit(prior_year_start))
            # Criterion 3 cont'd: continuous enrollment through PY end (or dod).
            .filter(
                pl.col("enroll_end").is_null()
                | (pl.col("enroll_end") >= pl.lit(period_end))
                | (
                    pl.col("dod").is_not_null()
                    & (pl.col("enroll_end") >= pl.col("dod"))
                )
            )
        )

        # Criterion 4: avg HCC composite risk score ≥ 2.0 in year before PY.
        # Source: §3.3.2 p15. We expect the caller to pre-filter
        # hcc_risk_scores to performance_year == py-1; we then average
        # across model_versions per person and apply the threshold.
        hcc = value_sets.get("hcc_scores")
        if hcc is not None:
            mbi_xwalk = value_sets.get("mbi_to_person")
            if mbi_xwalk is not None:
                hcc = hcc.join(
                    mbi_xwalk.select(["mbi", "person_id"]), on="mbi", how="inner"
                ).drop("mbi")
            else:
                # In CCLF-sourced data person_id == mbi (same identifier
                # space). When no explicit crosswalk is provided, treat
                # the HCC mbi column as person_id directly.
                hcc = hcc.rename({"mbi": "person_id"})
            hcc_filter = (
                hcc.group_by("person_id")
                .agg(pl.col("total_risk_score").mean().alias("avg_hcc_score"))
                .filter(pl.col("avg_hcc_score") >= 2.0)
                .select("person_id")
            )
            base = base.join(hcc_filter, on="person_id", how="inner")
        else:
            logger.warning(
                "DAH denominator: value_sets['hcc_scores'] not provided; "
                "HCC ≥ 2.0 criterion (§3.3.2 p15 #4) NOT enforced. Tieout "
                "drift expected."
            )

        # Criterion 5: aligned to a participating REACH ACO.
        # Source: §3.3.2 p15 ('The measure includes eligible beneficiaries
        # who are aligned to a participating REACH ACO, as determined by
        # the model.') The mx_validate pipeline injects the per-PY
        # REACH-aligned bene list under value_sets['reach_aligned_persons']
        # (one column: person_id), derived from
        # gold/consolidated_alignment.parquet using the alignment-eligible-
        # month rule from §3 p11.
        reach = value_sets.get("reach_aligned_persons")
        if reach is not None:
            base = base.join(reach.select("person_id"), on="person_id", how="inner")
        else:
            logger.warning(
                "DAH denominator: value_sets['reach_aligned_persons'] not "
                "provided; REACH alignment criterion (§3.3.2 p15 #5) NOT "
                "enforced. Tieout drift expected — denominator will include "
                "the full claims-derived bene pool."
            )

        return base.select("person_id").unique().with_columns(
            pl.lit(True).alias("denominator_flag")
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_numerator(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Per-bene observed Days at Home and Days in Care.

        Per §3.3.2 (PY2025 QMMR p15):

            survival_days  = eligible days in PY (alive)
                           = days from PY start to min(PY end, dod) inclusive.
            observed_dic   = sum of distinct in-care days from claims (see
                             _build_dic_claims for the claim universe).
                             Hospice days are subtracted: a bene enrolled in
                             hospice is ALWAYS at home for that day, even if
                             they receive care in a normally-DIC setting.
                             Obstetric admissions are excluded from DIC.
                             Long-term/residential nursing-home days
                             (non-SNF) are excluded from DIC.
            observed_dah   = max(survival_days - observed_dic, 0)

        Eligibility carries hospice indication implicitly via hospice
        bill-type 081x in claims; we don't get a hospice flag in
        ``eligibility``. We compute hospice days from claims and subtract
        them from any overlapping DIC stay before summing.
        """
        period_start, period_end = self._period_bounds()

        eligibility = value_sets.get("eligibility")
        if eligibility is None:
            raise ValueError(
                "DaysAtHome.calculate_numerator requires value_sets['eligibility']; "
                "the mx_validate pipeline injects it."
            )

        survival = self._compute_survival_days(denominator, eligibility, period_start, period_end)
        dic_per_bene = self._compute_dic_days(denominator, claims, period_start, period_end)

        # Observed DAH = survival_days - observed_dic, floored at 0.
        # (§3.3.2 p15: "A 'day at home' is defined as any eligible day
        # that is not considered a 'day in care' based on the above
        # definition. 'Eligible days' are all days in the measurement
        # year that the beneficiary is alive.")
        result = (
            denominator.select("person_id")
            .join(survival, on="person_id", how="left")
            .join(dic_per_bene, on="person_id", how="left")
            .with_columns(
                [
                    pl.col("survival_days").fill_null(0).cast(pl.Int64),
                    pl.col("observed_dic").fill_null(0).cast(pl.Int64),
                ]
            )
            .with_columns(
                pl.max_horizontal(
                    [pl.col("survival_days") - pl.col("observed_dic"), pl.lit(0)]
                )
                .cast(pl.Int64)
                .alias("observed_dah")
            )
            .select(
                [
                    "person_id",
                    "survival_days",
                    "observed_dic",
                    "observed_dah",
                    pl.lit(True).alias("numerator_flag"),
                ]
            )
        )
        return result

    @staticmethod
    def _compute_survival_days(
        denominator: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        period_start: date,
        period_end: date,
    ) -> pl.LazyFrame:
        """
        survival_days per §3.3.2 p15 ("Eligible days are all days in the
        measurement year that the beneficiary is alive").

        survival_days(bene) = days from period_start to min(period_end, dod)
        inclusive, or 0 if the bene was already dead on period_start (which
        shouldn't happen for denominator members, but we floor defensively).
        No artificial 365-day cap — leap years contribute 366.
        """
        bene_dod = (
            denominator.select("person_id")
            .join(
                eligibility.select(
                    [
                        "person_id",
                        pl.col("death_date").cast(pl.Date, strict=False).alias("dod"),
                    ]
                ),
                on="person_id",
                how="left",
            )
            .group_by("person_id")
            .agg(pl.col("dod").min().alias("dod"))
        )
        full_period_days = (period_end - period_start).days + 1
        return bene_dod.with_columns(
            pl.when(pl.col("dod").is_not_null() & (pl.col("dod") <= pl.lit(period_end)))
            .then((pl.col("dod") - pl.lit(period_start)).dt.total_days() + 1)
            .otherwise(full_period_days)
            .clip(0, full_period_days)
            .cast(pl.Int64)
            .alias("survival_days")
        ).select(["person_id", "survival_days"])

    @classmethod
    def _compute_dic_days(
        cls,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        period_start: date,
        period_end: date,
    ) -> pl.LazyFrame:
        """
        Per-bene observed_dic = distinct in-care days inside the PY window.

        'In care' day rules per §3.3.2 (PY2025 QMMR p15):

          - Counted: acute IP, CAH, IRF, IPF, LTCH, SNF (TOB prefixes in
            _DIC_TOB_PREFIXES), ED visits (revenue 045x or HCPCS 9928x /
            G0380-4), observation (rev 0762 or HCPCS G0378/G0379).
          - NOT counted (carve-outs):
              * Hospice days (TOB 081x): a bene in hospice is ALWAYS at
                home, even if otherwise DIC-coded. We subtract hospice
                days from overlapping DIC days.
              * Obstetric admissions (childbirth/miscarriage/termination
                ICD-10 prefixes O00–O08, O80–O82, Z37): excluded from DIC.
              * Long-term/residential nursing home days (non-SNF): the
                spec excludes these via the carve-out for 'residential
                psychiatric and substance abuse facilities, assisted
                living facilities and group homes' — long-term custodial
                NH is not counted unless billed as SNF (021x/028x).
                Outpatient (TOB 013x), home health (TOB 032x/033x/034x),
                and telehealth are NOT in our DIC TOB set so they're
                naturally excluded.

        We use distinct calendar days (set union across overlapping stays)
        rather than summing stay-day counts, so a SNF stay overlapping an
        IRF transfer day doesn't double-count.
        """
        # 1) Inpatient/SNF/IRF/IPF/LTCH/CAH stays: explode admit→discharge.
        inst_intervals = cls._inst_dic_intervals(claims, period_start, period_end)

        # 2) ED + observation: outpatient claims, treated as same-day care.
        same_day_dic = cls._ed_obs_dic_days(claims, period_start, period_end)

        # 3) Hospice intervals (will be subtracted from DIC).
        hospice_intervals = cls._hospice_intervals(claims, period_start, period_end)

        # Explode each interval into one row per (person_id, day).
        inst_days = cls._explode_intervals_to_days(inst_intervals)
        hospice_days = cls._explode_intervals_to_days(hospice_intervals)

        all_dic = pl.concat([inst_days, same_day_dic], how="diagonal").unique()
        # Subtract hospice days. Anti-join.
        dic_after_hospice = all_dic.join(
            hospice_days.select(["person_id", "dic_day"]),
            on=["person_id", "dic_day"],
            how="anti",
        )

        per_bene = (
            denominator.select("person_id")
            .join(
                dic_after_hospice.group_by("person_id")
                .agg(pl.col("dic_day").n_unique().alias("observed_dic")),
                on="person_id",
                how="left",
            )
            .with_columns(pl.col("observed_dic").fill_null(0).cast(pl.Int64))
        )
        return per_bene.select(["person_id", "observed_dic"])

    @staticmethod
    def _inst_dic_intervals(
        claims: pl.LazyFrame, period_start: date, period_end: date
    ) -> pl.LazyFrame:
        """
        Institutional DIC intervals (admit, discharge) clipped to the PY.

        Spec basis: §3.3.2 p15 'in care' settings — acute IP, CAH, IRF,
        IPF, LTCH, SNF. We also apply the obstetric-admission exclusion
        from §3.3.2 p15 numerator carve-out #2 by filtering out claims
        whose primary diagnosis (diagnosis_code_1) starts with an
        ICD-10 prefix in _OBSTETRIC_DX_PREFIXES.
        """
        # Some claim sources carry diagnosis_code_1 on the same row as TOB;
        # if it isn't there, the obstetric filter is a no-op (we don't lose
        # safety — we'd just over-count obstetric days, which we'll see in
        # tieout drift and address with a join to int_diagnosis_pivot later).
        cols = claims.collect_schema().names()
        dx_col_present = "diagnosis_code_1" in cols

        base = claims.with_columns(
            pl.col("bill_type_code")
            .cast(pl.Utf8)
            .str.strip_prefix("0")
            .str.slice(0, 2)
            .alias("_tob2")
        ).filter(
            pl.col("_tob2").is_in(list(_DIC_TOB_PREFIXES))
            & pl.col("admission_date").is_not_null()
            & pl.col("discharge_date").is_not_null()
        )
        if dx_col_present:
            obstetric_pat = "^(" + "|".join(_OBSTETRIC_DX_PREFIXES) + ")"
            # NULL dx → treat as non-obstetric (don't drop the claim).
            base = base.filter(
                pl.col("diagnosis_code_1").is_null()
                | ~pl.col("diagnosis_code_1").cast(pl.Utf8).str.contains(obstetric_pat)
            )

        return (
            base.select(
                [
                    "person_id",
                    pl.col("admission_date").cast(pl.Date, strict=False).alias("interval_start"),
                    pl.col("discharge_date").cast(pl.Date, strict=False).alias("interval_end"),
                ]
            )
            .filter(
                (pl.col("interval_start") <= pl.lit(period_end))
                & (pl.col("interval_end") >= pl.lit(period_start))
            )
            .with_columns(
                [
                    pl.max_horizontal([pl.col("interval_start"), pl.lit(period_start)]).alias(
                        "interval_start"
                    ),
                    pl.min_horizontal([pl.col("interval_end"), pl.lit(period_end)]).alias(
                        "interval_end"
                    ),
                ]
            )
        )

    @staticmethod
    def _hospice_intervals(
        claims: pl.LazyFrame, period_start: date, period_end: date
    ) -> pl.LazyFrame:
        """
        Hospice claim intervals (TOB 081x) clipped to PY.

        Per §3.3.2 p15 carve-out #1: a bene enrolled in hospice is ALWAYS
        at home. We subtract hospice days from the DIC day-set so that any
        DIC claim overlapping a hospice day does not contribute.
        """
        return (
            claims.with_columns(
                pl.col("bill_type_code")
            .cast(pl.Utf8)
            .str.strip_prefix("0")
            .str.slice(0, 2)
            .alias("_tob2")
            )
            .filter(
                (pl.col("_tob2") == _HOSPICE_TOB_PREFIX)
                & pl.col("claim_start_date").is_not_null()
                & pl.col("claim_end_date").is_not_null()
            )
            .select(
                [
                    "person_id",
                    pl.col("claim_start_date").cast(pl.Date, strict=False).alias("interval_start"),
                    pl.col("claim_end_date").cast(pl.Date, strict=False).alias("interval_end"),
                ]
            )
            .filter(
                (pl.col("interval_start") <= pl.lit(period_end))
                & (pl.col("interval_end") >= pl.lit(period_start))
            )
            .with_columns(
                [
                    pl.max_horizontal([pl.col("interval_start"), pl.lit(period_start)]).alias(
                        "interval_start"
                    ),
                    pl.min_horizontal([pl.col("interval_end"), pl.lit(period_end)]).alias(
                        "interval_end"
                    ),
                ]
            )
        )

    @staticmethod
    def _ed_obs_dic_days(
        claims: pl.LazyFrame, period_start: date, period_end: date
    ) -> pl.LazyFrame:
        """
        ED-visit and observation-stay DIC days per §3.3.2 p15.

        Identification per CMS NUBC + HCPCS conventions:
          - ED:    revenue_center_code starts with '045' (0450–0459) OR
                   hcpcs_code in ED E&M / Type B ED ranges.
          - Obs:   revenue_center_code == '0762' OR hcpcs_code in
                   {G0378, G0379}.

        We treat these as same-day care (one DIC day per claim service date)
        rather than spans, since ED/observation are billed line-item.
        """
        cols = claims.collect_schema().names()
        rev_present = "revenue_center_code" in cols
        hcpcs_present = "hcpcs_code" in cols
        if not (rev_present or hcpcs_present):
            # Claim source doesn't expose ED/obs identifiers; skip silently
            # (will surface as tieout drift, recorded here for traceability).
            return pl.LazyFrame(
                {"person_id": [], "dic_day": []},
                schema={"person_id": pl.Utf8, "dic_day": pl.Date},
            )

        conds: list[pl.Expr] = []
        if rev_present:
            conds.append(
                pl.col("revenue_center_code").cast(pl.Utf8).str.starts_with(_ED_REVENUE_PREFIX)
            )
            conds.append(pl.col("revenue_center_code").cast(pl.Utf8) == _OBS_REVENUE_CODE)
        if hcpcs_present:
            conds.append(
                pl.col("hcpcs_code").cast(pl.Utf8).is_in(list(_ED_HCPCS_CODES | _OBS_HCPCS_CODES))
            )
        match_any: pl.Expr = conds[0]
        for c in conds[1:]:
            match_any = match_any | c

        # Day = claim_line_start_date if present else claim_start_date.
        date_col = (
            "claim_line_start_date" if "claim_line_start_date" in cols else "claim_start_date"
        )
        return (
            claims.filter(match_any & pl.col(date_col).is_not_null())
            .select(
                [
                    "person_id",
                    pl.col(date_col).cast(pl.Date, strict=False).alias("dic_day"),
                ]
            )
            .filter(
                (pl.col("dic_day") >= pl.lit(period_start))
                & (pl.col("dic_day") <= pl.lit(period_end))
            )
        )

    @staticmethod
    def _explode_intervals_to_days(intervals: pl.LazyFrame) -> pl.LazyFrame:
        """
        Explode (interval_start, interval_end) into one row per calendar day.

        Helper for converting stay-spans into the per-day set used by DIC
        aggregation (so overlapping stays don't double-count days).
        """
        return (
            intervals.with_columns(
                pl.date_ranges(
                    pl.col("interval_start"), pl.col("interval_end"), interval="1d"
                ).alias("dic_day")
            )
            .explode("dic_day")
            .select(["person_id", "dic_day"])
        )

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        DAH has no per-bene numerator exclusions inside the calculation.

        Per §3.3.2 (PY2025 QMMR p15), the only DAH 'numerator exclusions'
        are *settings* removed from the DIC counted set (outpatient,
        hospice, residential psych/SUD, assisted living, home health,
        telehealth) — those are handled inside _compute_dic_days by
        narrowly defining what counts as DIC, not by flagging benes.
        Denominator exclusion (post-Jan-1 voluntary alignment) is enforced
        upstream.
        """
        return denominator.select("person_id").with_columns(
            pl.lit(False).alias("exclusion_flag")
        )


MeasureFactory.register("REACH_DAH", DaysAtHome)
logger.debug("Registered DAH (REACH) quality measure and transform")
