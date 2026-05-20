# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility criterion (c): moderate risk score + 2+ unplanned
admissions.

The binding text, Appendix A Section IV.B.1(c) of the Participation
Agreement ($BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt, line
3779):

    "Have a risk score between 2.0 and 3.0 for A&D Beneficiaries, or
    a risk score between 0.24 and 0.35 for ESRD Beneficiaries, and two
    or more unplanned hospital admissions in the previous 12 months as
    determined by CMS based on criteria specified by CMS in advance of
    the relevant Performance Year."

The range endpoints are half-open: the upper bound is the **criterion
(b) threshold**. A beneficiary whose score is ≥ 3.0 (A&D) or ≥ 0.35
(ESRD) satisfies criterion (b), and criterion (c)'s score range
intentionally does not extend that high — otherwise (b) and (c) would
overlap.

Interpretation of the range boundaries:

    A&D  : 2.0 ≤ score < 3.0
    ESRD : 0.24 ≤ score < 0.35

This follows the "between X and Y" phrasing as it is consistently used
in the PA and FOG for risk-score buckets. The FOG confirms the
operationalisation (``$BRONZE/REACHPY2024FinancialOperatGde.txt`` line
1179):

    "Have a CMS-HCC risk score between 2.0 and 3.0 for A&D
    beneficiaries (or a risk score between 0.24 and 0.35 for ESRD
    beneficiaries) and two or more unplanned hospital admissions in
    the previous 12 months."

Operational rules
-----------------

Model selection matches criterion (b) — use the max across all
applicable models per PA Section IV.B.2 / FOG line 1406.

"Unplanned admission" definition, FOG footnote 5 (line 1193):

    "An unplanned hospital admission is defined as the claim for the
    inpatient stay being coded as non-elective, specifically based on
    the 'reason for admission' code (CLM_IP_ADMSN_TYPE_CD is not 3)."

Counting lives in ``_hcc_unplanned_admissions``; this module joins the
per-MBI count to the per-MBI max score and applies the AND.

Threshold:
    criterion_c_met iff
        (AD and 2.0 <= score < 3.0 and admits >= 2)
     or (ESRD and 0.24 <= score < 0.35 and admits >= 2)

Input shape
-----------

- ``scores_lf`` : same as criterion (b) — one row per
  (mbi, model_version, score).
- ``admissions_lf`` : ``mbi``, ``unplanned_admission_count`` — produced
  by ``_hcc_unplanned_admissions.count_unplanned_admissions_in_window``
  using the same Table C window.
"""

from __future__ import annotations

import polars as pl

from acoharmony._expressions._high_needs_criterion_b import (
    AD_RISK_SCORE_THRESHOLD_B,
    ESRD_RISK_SCORE_THRESHOLD_B,
)


# Lower bounds on the moderate risk-score band.
AD_RISK_SCORE_LOWER_C = 2.0
ESRD_RISK_SCORE_LOWER_C = 0.24

# Upper bounds on the moderate risk-score band (exclusive) == criterion
# (b) threshold.
AD_RISK_SCORE_UPPER_C_EXCLUSIVE = AD_RISK_SCORE_THRESHOLD_B  # 3.0
ESRD_RISK_SCORE_UPPER_C_EXCLUSIVE = ESRD_RISK_SCORE_THRESHOLD_B  # 0.35

CRITERION_C_MIN_UNPLANNED_ADMITS = 2


def build_criterion_c_met_expr(
    scores_lf: pl.LazyFrame,
    admissions_lf: pl.LazyFrame,
    *,
    mbi_col: str = "mbi",
) -> pl.LazyFrame:
    """
    Per beneficiary, compute whether criterion (c) is met.

    Uses the same "max across applicable risk models" convention as
    criterion (b). Left-joins admission counts (beneficiaries with no
    unplanned admissions are kept with count = 0).

    Output columns:
        mbi                             str
        cohort                          str
        max_risk_score                  f64
        unplanned_admission_count       i64
        criterion_c_score_band_met      bool
        criterion_c_admission_count_met bool
        criterion_c_met                 bool
    """
    # Max score per bene (reuse the pattern from criterion-b)
    per_bene_max = (
        scores_lf.sort(["mbi", "total_risk_score"], descending=[False, True])
        .group_by("mbi")
        .agg(
            pl.col("cohort").first().alias("cohort"),
            pl.col("total_risk_score").first().alias("max_risk_score"),
        )
    )

    # Normalise admissions frame
    admissions = admissions_lf.rename(
        {mbi_col: "mbi"} if mbi_col != "mbi" else {}
    ).select("mbi", "unplanned_admission_count")

    joined = per_bene_max.join(admissions, on="mbi", how="left").with_columns(
        pl.col("unplanned_admission_count").fill_null(0).cast(pl.Int64),
    )

    score = pl.col("max_risk_score")
    is_ad = pl.col("cohort") != "ESRD"

    score_band_met = (
        (is_ad & score.is_between(AD_RISK_SCORE_LOWER_C, AD_RISK_SCORE_UPPER_C_EXCLUSIVE, closed="left"))
        | (~is_ad & score.is_between(ESRD_RISK_SCORE_LOWER_C, ESRD_RISK_SCORE_UPPER_C_EXCLUSIVE, closed="left"))
    )

    admission_count_met = pl.col("unplanned_admission_count") >= CRITERION_C_MIN_UNPLANNED_ADMITS

    return joined.with_columns(
        score_band_met.alias("criterion_c_score_band_met"),
        admission_count_met.alias("criterion_c_admission_count_met"),
    ).with_columns(
        (
            pl.col("criterion_c_score_band_met")
            & pl.col("criterion_c_admission_count_met")
        ).alias("criterion_c_met"),
    )
