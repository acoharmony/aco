"""
BNMR risk-sheet reconciliation expressions (milestone M2a).

The BNMR ``reach_bnmr_risk`` sheet aggregates beneficiary counts and
risk scores at grain:

    (perf_yr, clndr_yr, clndr_mnth, bnmrk, align_type, va_cat, bnmrk_type, aco_id)

M2a scopes tie-out to the **count** columns only:

- ``bene_dcnt``         — distinct beneficiaries with eligibility in the
                          given clndr_yr/clndr_mnth
- ``bene_dcnt_annual``  — distinct beneficiaries with any eligibility in
                          the performance year

``raw_risk_score`` / ``norm_risk_score`` / ``risk_denom`` are deferred
to M2b, which requires per-beneficiary HCC scoring.

Upstream input schema
---------------------
The reconstruction takes a per-beneficiary-month eligibility frame
already stamped with the full BNMR dim set (the pipeline's job to
prepare upstream; we tie out the counting, not the stamping).

Expected columns on ``eligibility``:

    perf_yr, clndr_yr, clndr_mnth, bnmrk, align_type, va_cat,
    bnmrk_type, aco_id, member_id, file_date

``member_id`` is the MBI that gets distinct-counted.
"""

from __future__ import annotations

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression

# Full BNMR risk-sheet grain. Used as the group-by key for monthly counts.
RISK_MONTHLY_GRAIN: tuple[str, ...] = (
    "perf_yr",
    "clndr_yr",
    "clndr_mnth",
    "bnmrk",
    "align_type",
    "va_cat",
    "bnmrk_type",
    "aco_id",
)

# Annual counts roll up the clndr_yr/clndr_mnth dims — one row per
# (perf_yr, bnmrk, align_type, va_cat, bnmrk_type, aco_id).
RISK_ANNUAL_GRAIN: tuple[str, ...] = (
    "perf_yr",
    "bnmrk",
    "align_type",
    "va_cat",
    "bnmrk_type",
    "aco_id",
)


@register_expression(
    "bnmr_risk_reconciliation",
    schemas=["silver", "gold"],
    dataset_types=["reconciliation", "bnmr", "reach_bnmr", "enrollment"],
    callable=False,
    description="BNMR risk-sheet count reconciliation helpers",
)
class BnmrRiskReconciliationExpression:
    """Expression builders for BNMR risk → enrollment reconciliation views."""

    @staticmethod
    @expression(
        name="bene_dcnt_from_eligibility",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def bene_dcnt_expr() -> pl.Expr:
        """
        Count distinct beneficiaries per monthly BNMR-risk-grain bucket.

        Returns a ``pl.Expr.alias('expected_bene_dcnt')`` suitable for use
        inside a group-by agg — so the caller supplies the grain.
        """
        return pl.col("member_id").n_unique().alias("expected_bene_dcnt")

    @staticmethod
    @expression(
        name="bene_dcnt_annual_from_eligibility",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def bene_dcnt_annual_expr() -> pl.Expr:
        """Same pattern but for the annual rollup grain."""
        return pl.col("member_id").n_unique().alias("expected_bene_dcnt_annual")
