# © 2025 HarmonyCares
# All rights reserved.

"""
BNMR reconciliation view transform (PR D of BNMR reconciliation track).

Analogous to the MER view (``_mer_reconciliation_view``) but operates on
the ``reach_bnmr_claims`` silver table. The BNMR shares MER's dim schema
and claim-type taxonomy, so the label / part / service-month / program
expressions are reused directly from ``MerReconciliationExpression``.

The one BNMR-specific piece is ``BnmrReconciliationExpression.net_expenditure_expr``,
which computes net from component reduction columns since BNMR does not
ship the ``total_exp_amt_agg`` column that MER provides.

BNMR has no enrollment sheet analogous to MER's ``mexpr_data_enroll``, so
the view does not compute PBPM — BNMR reconciliation is a pure dollar
tie-out. If future work needs BNMR-side PBPM, it can join to
``reach_bnmr_cap`` which carries per-capita costs at an aggregated grain.
"""

from __future__ import annotations

import polars as pl

from .._expressions._bnmr_reconciliation import BnmrReconciliationExpression
from .._expressions._mer_reconciliation import MerReconciliationExpression


def build_bnmr_reconciliation_view(
    bnmr_claims: pl.LazyFrame,
    as_of_delivery_date: str | None = None,
) -> pl.LazyFrame:
    """
    Build the BNMR reconciliation view.

    Args:
        bnmr_claims: LazyFrame sourced from ``silver.reach_bnmr_claims``.
            Must contain the MER-shared dim columns (perf_yr, clndr_yr,
            clndr_mnth, bnmrk, align_type, bnmrk_type, aco_id, clm_type_cd),
            the component reduction columns used by
            ``BnmrReconciliationExpression.net_expenditure_expr``, and
            ``file_date``.
        as_of_delivery_date: Optional ISO date string (``'YYYY-MM-DD'``).
            Filters rows to ``file_date <= cutoff`` for historical
            point-in-time reconciliations.

    Returns:
        LazyFrame with the original BNMR columns plus these derived
        columns (same names as the MER view so the two are join-compatible):
            - ``net_expenditure`` (computed from BNMR components)
            - ``claim_type_label``, ``claim_type_part``
            - ``srvc_month_date``
            - ``program``
    """
    if as_of_delivery_date is not None:
        bnmr_claims = bnmr_claims.filter(
            MerReconciliationExpression.as_of_delivery_filter(as_of_delivery_date)
        )

    return bnmr_claims.with_columns(
        BnmrReconciliationExpression.net_expenditure_expr(),
        MerReconciliationExpression.claim_type_label_expr(),
        MerReconciliationExpression.claim_type_part_expr(),
        MerReconciliationExpression.srvc_month_date_expr(),
        MerReconciliationExpression.program_expr(),
    )
