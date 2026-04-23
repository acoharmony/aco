# © 2025 HarmonyCares
# All rights reserved.

"""
SNF and Home Health day counter for High-Needs criterion IV.B.1(e).

Counts, per beneficiary, the total days of Skilled Nursing Facility
(SNF) and Home Health (HH) care received in a lookback window. Used
solely by criterion IV.B.1(e) of the High-Needs eligibility rule —
applies only for Performance Year 2024 and beyond.

The binding text, Appendix A Section IV.B.1(e) of the Participation
Agreement ($BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt, line
3786):

    "For Performance Year 2024 and each subsequent Performance Year,
    have qualified for and received skilled nursing and/or
    rehabilitation services in a SNF for a minimum of 45 Days or
    qualified for and received home health services for a minimum of
    90 Days in the previous 12 months as determined by CMS."

The PY2024 Financial Operating Guide operationalizes the rule as two
alternatives (either clause qualifies the beneficiary):

    "Have qualified for and received skilled nursing and/or
    rehabilitation services in a SNF for a minimum of 45 days in the
    previous 12 months as determined by CMS; or

    Have qualified for and received home health services for a
    minimum of 90 days in the previous 12 months as determined by
    CMS."
        — $BRONZE/REACHPY2024FinancialOperatGde.txt, lines 1185 and 1188

Operational rules
-----------------

1. Source: CCLF1 institutional claim header. We read ``silver/cclf1``.

2. Claim-type filter. CMS CCLF claim-type codes split institutional
   bills by setting:
       10 = Home Health Agency (HHA)
       20 = Skilled Nursing Facility (SNF)
   Other institutional claim types (hospice, outpatient, inpatient,
   etc.) don't count toward criterion (e).

3. Days computation. For each qualifying claim, the days of service are
   inclusive of both endpoints — ``clm_thru_dt - clm_from_dt + 1``.
   The caller must clip each claim's service span to the intersection
   with the lookback window before summing, otherwise a claim that
   straddles the window boundary would contribute days outside the
   measurement period.

4. Per-day deduplication. A beneficiary occasionally has overlapping
   SNF claims (adjustments, re-billings). Sum distinct service days
   per beneficiary per setting so a single day of care counts once,
   regardless of how many claims represent it.

5. Threshold check. Criterion (e) is met when
   ``snf_days >= 45 OR home_health_days >= 90``. That OR is evaluated
   by the criterion-(e) expression module; this module emits the raw
   counts only.

6. PY gating. The criterion applies only for PY ≥ 2024; the rollup
   expression at ``_high_needs_eligibility`` short-circuits for PY
   2022 / 2023.

This module builds Polars expressions only — it does not load data or
resolve lookback windows.
"""

from __future__ import annotations

from datetime import date

import polars as pl


# CCLF claim-type codes.
SNF_CLAIM_TYPE_CODE = "20"
HOME_HEALTH_CLAIM_TYPE_CODE = "10"

# Criterion (e) thresholds (PA Section IV.B.1(e), line 3786).
SNF_DAYS_THRESHOLD = 45
HOME_HEALTH_DAYS_THRESHOLD = 90


def _count_days_in_window_expr(
    window_begin: date,
    window_end: date,
    from_col: str,
    thru_col: str,
) -> pl.Expr:
    """
    Build an expression that computes the per-claim number of service
    days that fall inside [window_begin, window_end] inclusive.

    Clips each claim's [clm_from_dt, clm_thru_dt] span to the window
    boundaries, then returns ``(clipped_end - clipped_begin).days + 1``
    for claims that still have a non-empty span after clipping; zero
    otherwise.
    """
    from_d = pl.col(from_col).cast(pl.Date, strict=False)
    thru_d = pl.col(thru_col).cast(pl.Date, strict=False)

    clipped_begin = pl.max_horizontal(from_d, pl.lit(window_begin))
    clipped_end = pl.min_horizontal(thru_d, pl.lit(window_end))

    # Inclusive day count: (end - begin) + 1 days when begin <= end.
    delta_days = (clipped_end - clipped_begin).dt.total_days() + 1
    return pl.when(clipped_begin <= clipped_end).then(delta_days).otherwise(0)


def build_snf_hh_days_in_window(
    claims_lf: pl.LazyFrame,
    *,
    window_begin: date,
    window_end: date,
    mbi_col: str = "bene_mbi_id",
    from_col: str = "clm_from_dt",
    thru_col: str = "clm_thru_dt",
    claim_type_col: str = "clm_type_cd",
) -> pl.LazyFrame:
    """
    Per-beneficiary counts of SNF and Home Health service days inside
    the lookback window.

    Takes a CCLF1 LazyFrame, filters to SNF (claim type 20) and HH
    (claim type 10) claims, clips each claim's service span to the
    window, sums the days per beneficiary per claim-type, and returns a
    wide LazyFrame with one row per beneficiary:

        bene_mbi_id | snf_days | home_health_days

    Beneficiaries with zero qualifying claims are absent from the
    output — callers that need dense output (one row per beneficiary
    including zeros) should left-join against the beneficiary roster.
    """
    days_in_window = _count_days_in_window_expr(window_begin, window_end, from_col, thru_col)
    claim_type_str = pl.col(claim_type_col).cast(pl.String, strict=False)

    # Keep only the two qualifying claim types; drop others early to
    # minimize work.
    filtered = claims_lf.filter(
        claim_type_str.is_in([SNF_CLAIM_TYPE_CODE, HOME_HEALTH_CLAIM_TYPE_CODE])
    ).with_columns(
        claim_type_str.alias("_clm_type_str"),
        days_in_window.alias("_days_in_window"),
    )

    # Deduplicate per (mbi, from, thru, claim_type) so identical
    # adjusted/interim bills don't double-count.
    filtered = filtered.unique(subset=[mbi_col, from_col, thru_col, "_clm_type_str"])

    return (
        filtered.group_by(mbi_col)
        .agg(
            pl.when(pl.col("_clm_type_str") == SNF_CLAIM_TYPE_CODE)
            .then(pl.col("_days_in_window"))
            .otherwise(0)
            .sum()
            .alias("snf_days"),
            pl.when(pl.col("_clm_type_str") == HOME_HEALTH_CLAIM_TYPE_CODE)
            .then(pl.col("_days_in_window"))
            .otherwise(0)
            .sum()
            .alias("home_health_days"),
        )
    )


def build_criterion_e_met_expr() -> pl.Expr:
    """
    Boolean expression: criterion (e) is met iff
    ``snf_days >= 45 OR home_health_days >= 90``.

    Assumes the input frame carries ``snf_days`` and ``home_health_days``
    columns — as produced by ``build_snf_hh_days_in_window``.
    """
    return (
        (pl.col("snf_days") >= SNF_DAYS_THRESHOLD)
        | (pl.col("home_health_days") >= HOME_HEALTH_DAYS_THRESHOLD)
    )
