# © 2025 HarmonyCares
# All rights reserved.

"""
BNMR risk-score reconciliation view (milestone M2b).

Ties out the ``raw_risk_score`` column on the BNMR ``reach_bnmr_risk``
sheet against a population-weighted average of per-beneficiary HCC
scores, aggregated to the BNMR risk monthly grain
``(perf_yr, clndr_yr, clndr_mnth, bnmrk, align_type, va_cat, bnmrk_type,
aco_id)``.

Scoring responsibility
----------------------
The caller supplies ``scored_benes`` — a LazyFrame where each row is a
beneficiary-month (or beneficiary-period) with a pre-computed
``risk_score`` and the full BNMR grain stamped on. Running the HCC
engine is a Python-level per-row operation that happens outside this
transform so the tie-out logic itself stays pure LazyFrame.

The reconciliation computes the simple arithmetic mean of the per-bene
scores at each grain bucket (matching the CMS "raw_risk_score" = average
of contributing-bene RAFs, before normalization). ``norm_risk_score``
and ``risk_denom`` tie-outs are deferred to M9 (normalization factors
live on ``report_parameters``).

Tolerance: 4 decimal places.
"""

from __future__ import annotations

import polars as pl

from .._expressions._bnmr_risk_reconciliation import RISK_MONTHLY_GRAIN

RISK_SCORE_TOLERANCE = 1e-4


def aggregate_scored_benes(
    scored_benes: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Reduce scored_benes to the BNMR risk monthly grain with:

    - ``expected_raw_risk_score`` — mean of ``risk_score`` per bucket
    - ``expected_bene_dcnt`` — distinct ``member_id`` per bucket
      (kept for cross-check against M2a's counts)

    Args:
        scored_benes: LazyFrame with ``member_id``, ``risk_score``, and
            all BNMR risk monthly grain columns.

    Returns:
        LazyFrame with one row per BNMR risk monthly bucket.
    """
    return scored_benes.group_by(*RISK_MONTHLY_GRAIN).agg(
        pl.col("risk_score").mean().alias("expected_raw_risk_score"),
        pl.col("member_id").n_unique().alias("expected_bene_dcnt"),
    )


def build_bnmr_risk_score_reconciliation_view(
    bnmr_risk: pl.LazyFrame,
    scored_benes: pl.LazyFrame,
    as_of_delivery_date: str | None = None,
) -> pl.LazyFrame:
    """
    Build the BNMR risk **score** tie-out view (M2b).

    Full-outer-joins aggregated scored_benes against BNMR risk so
    either-side-only rows surface as non-zero diffs rather than being
    dropped.

    Args:
        bnmr_risk: ``silver.reach_bnmr_risk`` LazyFrame. Must carry
            ``raw_risk_score`` and the full monthly grain + ``file_date``.
        scored_benes: Per-bene-month frame with ``member_id``,
            ``risk_score``, and all BNMR monthly grain columns.
        as_of_delivery_date: Optional ISO date. Filters BNMR rows by
            ``file_date <= cutoff``.

    Returns:
        LazyFrame keyed on BNMR monthly grain with:
            - ``raw_risk_score`` (from BNMR)
            - ``expected_raw_risk_score`` (reconstructed)
            - ``raw_risk_score_diff`` — absolute diff
            - ``expected_bene_dcnt`` — carried through for cross-check
    """
    if as_of_delivery_date is not None:
        bnmr_risk = bnmr_risk.filter(pl.col("file_date") <= as_of_delivery_date)

    bnmr_side = bnmr_risk.select(*RISK_MONTHLY_GRAIN, "raw_risk_score")
    recon_side = aggregate_scored_benes(scored_benes)

    joined = bnmr_side.join(
        recon_side, on=list(RISK_MONTHLY_GRAIN), how="full", coalesce=True
    )

    return joined.with_columns(
        (pl.col("raw_risk_score") - pl.col("expected_raw_risk_score"))
        .abs()
        .alias("raw_risk_score_diff")
    )
