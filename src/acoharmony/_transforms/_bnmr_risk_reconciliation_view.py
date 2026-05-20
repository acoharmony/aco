# © 2025 HarmonyCares
# All rights reserved.

"""
BNMR risk-sheet reconciliation view (milestone M2a).

Joins the BNMR ``reach_bnmr_risk`` sheet against reconstructed
beneficiary counts from a stamped eligibility frame at two grains:

1. **Monthly** — full risk grain
   ``(perf_yr, clndr_yr, clndr_mnth, bnmrk, align_type, va_cat,
   bnmrk_type, aco_id)`` — ties out ``bene_dcnt``.
2. **Annual** — rollup
   ``(perf_yr, bnmrk, align_type, va_cat, bnmrk_type, aco_id)`` — ties
   out ``bene_dcnt_annual``.

Tolerance is zero: counts must match exactly. A single-bene drift
means the eligibility stamping is broken.
"""

from __future__ import annotations

import polars as pl

from .._expressions._bnmr_risk_reconciliation import (
    RISK_ANNUAL_GRAIN,
    RISK_MONTHLY_GRAIN,
    BnmrRiskReconciliationExpression,
)


def aggregate_eligibility_monthly(
    eligibility: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Produce distinct-beneficiary counts at the full BNMR risk monthly grain.

    Args:
        eligibility: LazyFrame with the monthly grain columns plus
            ``member_id``.

    Returns:
        LazyFrame with monthly grain columns plus ``expected_bene_dcnt``.
    """
    return eligibility.group_by(*RISK_MONTHLY_GRAIN).agg(
        BnmrRiskReconciliationExpression.bene_dcnt_expr()
    )


def aggregate_eligibility_annual(
    eligibility: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Produce distinct-beneficiary counts at the annual rollup grain.

    Any beneficiary with eligibility in any month of the perf_yr counts
    once in the annual total.
    """
    return eligibility.group_by(*RISK_ANNUAL_GRAIN).agg(
        BnmrRiskReconciliationExpression.bene_dcnt_annual_expr()
    )


def build_bnmr_risk_count_reconciliation_view(
    bnmr_risk: pl.LazyFrame,
    eligibility: pl.LazyFrame,
    as_of_delivery_date: str | None = None,
) -> pl.LazyFrame:
    """
    Build the BNMR risk **monthly count** tie-out view.

    Full outer join on the monthly grain so either-side-only rows surface
    as count diffs rather than silent drops.

    Args:
        bnmr_risk: ``silver.reach_bnmr_risk`` LazyFrame. Must carry
            ``bene_dcnt`` and the monthly grain columns plus ``file_date``.
        eligibility: Per-beneficiary-month eligibility frame stamped with
            the BNMR dims (see module docstring for schema).
        as_of_delivery_date: Optional ISO date string. Filters BNMR rows
            by ``file_date <= cutoff`` for PIT reconstructions.

    Returns:
        LazyFrame keyed on the monthly grain with ``bene_dcnt``,
        ``expected_bene_dcnt``, ``bene_dcnt_diff``.
    """
    if as_of_delivery_date is not None:
        bnmr_risk = bnmr_risk.filter(pl.col("file_date") <= as_of_delivery_date)

    bnmr_side = bnmr_risk.select(*RISK_MONTHLY_GRAIN, "bene_dcnt")
    recon_side = aggregate_eligibility_monthly(eligibility)

    joined = bnmr_side.join(
        recon_side, on=list(RISK_MONTHLY_GRAIN), how="full", coalesce=True
    ).with_columns(
        pl.col("bene_dcnt").fill_null(0).cast(pl.Int64),
        pl.col("expected_bene_dcnt").fill_null(0).cast(pl.Int64),
    )

    return joined.with_columns(
        (pl.col("bene_dcnt") - pl.col("expected_bene_dcnt"))
        .abs()
        .alias("bene_dcnt_diff")
    )


def build_bnmr_risk_annual_count_reconciliation_view(
    bnmr_risk: pl.LazyFrame,
    eligibility: pl.LazyFrame,
    as_of_delivery_date: str | None = None,
) -> pl.LazyFrame:
    """
    Build the BNMR risk **annual count** tie-out view.

    BNMR ships ``bene_dcnt_annual`` repeated across every monthly row of
    the same ``(perf_yr, bnmrk, align_type, va_cat, bnmrk_type, aco_id)``
    key; we dedupe to a single value per key before joining.

    Args:
        bnmr_risk, eligibility, as_of_delivery_date: as above.

    Returns:
        LazyFrame keyed on the annual grain with ``bene_dcnt_annual``,
        ``expected_bene_dcnt_annual``, ``bene_dcnt_annual_diff``.
    """
    if as_of_delivery_date is not None:
        bnmr_risk = bnmr_risk.filter(pl.col("file_date") <= as_of_delivery_date)

    bnmr_annual = (
        bnmr_risk.select(*RISK_ANNUAL_GRAIN, "bene_dcnt_annual")
        .unique(subset=list(RISK_ANNUAL_GRAIN))
    )
    recon_annual = aggregate_eligibility_annual(eligibility)

    joined = bnmr_annual.join(
        recon_annual, on=list(RISK_ANNUAL_GRAIN), how="full", coalesce=True
    ).with_columns(
        pl.col("bene_dcnt_annual").fill_null(0).cast(pl.Int64),
        pl.col("expected_bene_dcnt_annual").fill_null(0).cast(pl.Int64),
    )

    return joined.with_columns(
        (pl.col("bene_dcnt_annual") - pl.col("expected_bene_dcnt_annual"))
        .abs()
        .alias("bene_dcnt_annual_diff")
    )
