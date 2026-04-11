"""
BNMR (Benchmark Report) reconciliation expressions.

The BNMR's ``reach_bnmr_claims`` sheet shares MER's dim schema (perf_yr,
clndr_yr, clndr_mnth, bnmrk, align_type, bnmrk_type, aco_id, clm_type_cd)
AND the same claim-type taxonomy (10/20/30/40/50/60/71/72/81/82), so the
MER-side helpers — label/part/srvc_month/program/point-in-time filter —
are re-used directly via ``MerReconciliationExpression``.

The one thing that differs is the net expenditure computation. MER ships
a CMS pre-netted ``total_exp_amt_agg`` column; BNMR does not. Instead
BNMR carries component reduction columns (sqstr, apa_rdctn, ucc,
op_dsh, cp_dsh, op_ime, cp_ime, nonpbp_rdct) and one additive adjustment
(``aco_amt_agg_apa``) that must be combined arithmetically:

    net = clm_pmt_amt_agg
          - sqstr_amt_agg
          - apa_rdctn_amt_agg
          - ucc_amt_agg
          - op_dsh_amt_agg
          - cp_dsh_amt_agg
          - op_ime_amt_agg
          - cp_ime_amt_agg
          - nonpbp_rdct_amt_agg
          + aco_amt_agg_apa

``aco_amt_agg_apa`` is ADDED because it represents an ACO-specific payment
adjustment that flows back into the ACO's claim total. All the others are
reductions (sequestration, APA reductions, uncompensated care, DSH, IME,
non-PBP reductions) that subtract from the gross.

BNMR does not ship ``pcc_rdctn_amt_agg``, ``tcc_rdctn_amt_agg``, or
``apo_rdctn_amt_agg`` (those are MER-only) so they are intentionally
absent from the BNMR formula.
"""

from __future__ import annotations

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


@register_expression(
    "bnmr_reconciliation",
    schemas=["silver", "gold"],
    dataset_types=["reconciliation", "bnmr", "reach_bnmr"],
    callable=False,
    description="BNMR reconciliation helpers: component-based net expenditure",
)
class BnmrReconciliationExpression:
    """Expression builders for BNMR → CCLF reconciliation views."""

    @staticmethod
    @expression(
        name="bnmr_net_expenditure",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def net_expenditure_expr() -> pl.Expr:
        """
        Compute net expenditure from BNMR component reduction columns.

        BNMR does not ship a pre-netted total the way MER does, so the net
        must be computed arithmetically from the gross ``clm_pmt_amt_agg``
        minus reductions plus the ACO-specific adjustment. Nulls in any
        column propagate to the result (no silent-zero substitution) so
        an incomplete BNMR row surfaces as a missing reconciliation bucket
        rather than a falsely-zero one.
        """
        return (
            pl.col("clm_pmt_amt_agg")
            - pl.col("sqstr_amt_agg")
            - pl.col("apa_rdctn_amt_agg")
            - pl.col("ucc_amt_agg")
            - pl.col("op_dsh_amt_agg")
            - pl.col("cp_dsh_amt_agg")
            - pl.col("op_ime_amt_agg")
            - pl.col("cp_ime_amt_agg")
            - pl.col("nonpbp_rdct_amt_agg")
            + pl.col("aco_amt_agg_apa")
        ).alias("net_expenditure")
