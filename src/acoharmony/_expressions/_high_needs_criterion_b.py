# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility criterion (b): high risk score.

The binding text, Appendix A Section IV.B.1(b) of the Participation
Agreement ($BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt, line
3765):

    "Have at least one significant chronic or other serious illness
    (defined as having a risk score of 3.0 or greater for Aged &
    Disabled (A&D) Beneficiaries or a risk score of 0.35 or greater
    for ESRD Beneficiaries)."

PA Section IV.B.2 (line 3791) specifies which risk-adjustment model's
score to use:

    "... CMS determines Beneficiary risk scores for the purposes of
    Section IV.B of this Appendix for A&D Beneficiaries by using the
    risk score calculated under the 2020 CMS-HCC Risk Adjustment
    Model (Version 24) or the 2024 CMS-HCC Risk Adjustment Model
    (Version 28) or the CMMI-HCC Concurrent Risk Adjustment Model (as
    defined in Appendix B of the Agreement), whichever risk score is
    higher. CMS calculates Beneficiary risk scores for the purposes
    of Section IV.B of this Appendix for ESRD Beneficiaries by using
    the CMS-HCC Risk Adjustment Model."

The Financial Operating Guide reiterates the "whichever is higher"
construction for A&D (``REACHPY2024FinancialOperatGde.txt`` line 1406):

    "To generate risk scores for the eligibility criteria listed
    above, diagnoses from the most recent 12-month period are run
    through both the prospective CMS-HCC risk adjustment model and
    the concurrent CMMI-HCC risk adjustment model, and a beneficiary
    will be considered eligible if they meet the requirements with
    either risk score."

Operational rules
-----------------

For each beneficiary at each check date:

    1. Cohort classification: A&D or ESRD (via OREC per
       ``_hcc_cohort``).
    2. Applicable risk scores:
        A&D for PY≥2024: CMS-HCC V24, CMS-HCC V28, CMMI-HCC Concurrent.
        A&D pre-PY2024: CMS-HCC V24, CMMI-HCC Concurrent.
        ESRD: CMS-HCC ESRD (V24).
    3. Threshold:
        A&D  → max score ≥ 3.0
        ESRD → max score ≥ 0.35
    4. Diagnoses-in-lookback: the Table C 12-month window feeds all
       three models; the scoring drivers don't re-window, they take
       the pre-filtered dx list. Windowing is applied in the transform.

Input shape
-----------

This module assumes scores are already computed upstream and loaded as
a long-form LazyFrame with columns:

    mbi              str
    cohort           str      "AD" or "ESRD"
    model_version    str      e.g. "cms_hcc_v24", "cmmi_concurrent"
    total_risk_score f64

The transform layer materialises this frame from the CMS-HCC and
CMMI-HCC drivers; this module only applies the threshold/max logic.
"""

from __future__ import annotations

import polars as pl


AD_RISK_SCORE_THRESHOLD_B = 3.0
ESRD_RISK_SCORE_THRESHOLD_B = 0.35


def build_criterion_b_met_expr(scores_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Per beneficiary, compute whether criterion (b) is met.

    Input columns: ``mbi``, ``cohort``, ``model_version``, ``total_risk_score``.

    Output: one row per MBI with:
        mbi                        str
        cohort                     str
        max_risk_score             f64
        max_risk_score_model       str   — model_version of the max
        criterion_b_threshold      f64   — 3.0 for AD, 0.35 for ESRD
        criterion_b_met            bool
    """
    # Pick the max score per beneficiary AND tag which model produced it
    # (useful for auditing reconciliation disagreements). We do this by
    # sorting and taking the first row per mbi.
    ranked = scores_lf.sort(
        ["mbi", "total_risk_score"], descending=[False, True]
    )
    per_bene_max = ranked.group_by("mbi").agg(
        pl.col("cohort").first().alias("cohort"),
        pl.col("total_risk_score").first().alias("max_risk_score"),
        pl.col("model_version").first().alias("max_risk_score_model"),
    )
    return per_bene_max.with_columns(
        pl.when(pl.col("cohort") == "ESRD")
        .then(pl.lit(ESRD_RISK_SCORE_THRESHOLD_B))
        .otherwise(pl.lit(AD_RISK_SCORE_THRESHOLD_B))
        .alias("criterion_b_threshold"),
    ).with_columns(
        (pl.col("max_risk_score") >= pl.col("criterion_b_threshold")).alias(
            "criterion_b_met"
        ),
    )
