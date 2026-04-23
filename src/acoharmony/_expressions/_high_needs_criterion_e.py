# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility criterion (e): SNF ≥ 45 days OR home health ≥ 90
days (PY2024 and later only).

The binding text, Appendix A Section IV.B.1(e) of the Participation
Agreement ($BRONZE/ACO_REACH_PY2026_AR_PA_2023_Starters_508.txt, line
3786):

    "For Performance Year 2024 and each subsequent Performance Year,
    have qualified for and received skilled nursing and/or
    rehabilitation services in a SNF for a minimum of 45 Days or
    qualified for and received home health services for a minimum of
    90 Days in the previous 12 months as determined by CMS."

The Financial Operating Guide splits this into two explicit clauses
(``REACHPY2024FinancialOperatGde.txt`` lines 1185 and 1188). The
day-counter lives in ``_hcc_snf_home_health_days``; this module just
applies the PY gating and threshold test.

PY gating
---------

Criterion (e) applies **only** for PY 2024 and later. For PY 2022 and
2023, the criterion is not evaluable; this module returns
``criterion_e_met = False`` and ``criterion_e_applicable = False`` for
earlier PYs so the rollup expression knows to exclude this row from its
OR without inadvertently treating the False as a substantive negative.
"""

from __future__ import annotations

import polars as pl

from acoharmony._expressions._hcc_snf_home_health_days import (
    HOME_HEALTH_DAYS_THRESHOLD,
    SNF_DAYS_THRESHOLD,
    build_criterion_e_met_expr as build_e_threshold_expr,
)


CRITERION_E_FIRST_APPLICABLE_PY = 2024


def build_criterion_e_applicable(performance_year: int) -> bool:
    """
    Returns True iff criterion (e) applies for the given performance
    year. Pure function; callers use it to short-circuit the entire
    criterion-e branch for PY 2022/2023.
    """
    return performance_year >= CRITERION_E_FIRST_APPLICABLE_PY


def build_criterion_e_met_expr(
    days_lf: pl.LazyFrame,
    performance_year: int,
    *,
    mbi_col: str = "bene_mbi_id",
) -> pl.LazyFrame:
    """
    Apply the criterion-e threshold to a per-beneficiary SNF/HH day
    frame. Returns one row per MBI with:

        mbi              str
        snf_days         i64
        home_health_days i64
        criterion_e_met  bool     — always False for PY < 2024

    For PY < 2024, ``criterion_e_met`` is forced to ``False`` even if
    the day counts would otherwise satisfy the thresholds. The
    eligibility rollup should key on the module-level
    ``build_criterion_e_applicable(performance_year)`` predicate to
    decide whether to include this row in its OR.
    """
    normalised = days_lf.rename(
        {mbi_col: "mbi"} if mbi_col != "mbi" else {}
    ).select(
        "mbi",
        pl.col("snf_days").cast(pl.Int64),
        pl.col("home_health_days").cast(pl.Int64),
    )

    applicable = build_criterion_e_applicable(performance_year)

    if applicable:
        met = build_e_threshold_expr()
    else:
        met = pl.lit(False)

    return normalised.with_columns(met.alias("criterion_e_met"))


__all__ = [
    "CRITERION_E_FIRST_APPLICABLE_PY",
    "HOME_HEALTH_DAYS_THRESHOLD",
    "SNF_DAYS_THRESHOLD",
    "build_criterion_e_applicable",
    "build_criterion_e_met_expr",
]
