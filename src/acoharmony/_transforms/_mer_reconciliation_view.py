# © 2025 HarmonyCares
# All rights reserved.

"""
MER reconciliation view transform.

Joins the two silver-tier MER data sheets — ``mexpr_data_claims`` (incurred
expenditures bucketed by claim type) and ``mexpr_data_enroll`` (eligible
member months) — into a single long-format frame suitable for 1:1
reconciliation against our CCLF-side expenditure calc.

The transform is a pure function: it takes two ``pl.LazyFrame`` inputs and
returns a ``pl.LazyFrame``. No catalog lookups, no filesystem reads, no
global state. This makes it trivially testable with inline fixtures while
still composing cleanly inside a pipe that supplies the frames from the
catalog.

Point-in-time correctness
-------------------------
The optional ``as_of_delivery_date`` argument filters BOTH inputs to rows
whose ``file_date`` is on or before the given cutoff. This is how we enforce
no-data-leak on historical reconciliation tests: when reconciling a MER
delivery dated 2025-11-30, our calc side must only see MER rows from
deliveries that existed on or before 2025-11-30.
"""

from __future__ import annotations

import polars as pl

from .._expressions._mer_reconciliation import MerReconciliationExpression

# The full dim tuple the MER uses to bucket its measures. Both claims and
# enroll carry all seven columns; the join is an exact match on all of them.
_DIM_COLS: list[str] = [
    "perf_yr",
    "clndr_yr",
    "clndr_mnth",
    "bnmrk",
    "align_type",
    "bnmrk_type",
    "aco_id",
]


def build_mer_reconciliation_view(
    claims: pl.LazyFrame,
    enroll: pl.LazyFrame,
    as_of_delivery_date: str | None = None,
) -> pl.LazyFrame:
    """
    Build the long-format MER reconciliation view.

    Args:
        claims: LazyFrame sourced from ``silver.mexpr_data_claims``. Must
            contain the MER dim columns, ``clm_type_cd``, ``total_exp_amt_agg``,
            and ``file_date``.
        enroll: LazyFrame sourced from ``silver.mexpr_data_enroll``. Must
            contain the MER dim columns, ``elig_mnths``, and ``file_date``.
        as_of_delivery_date: Optional ISO date string (``'YYYY-MM-DD'``).
            When provided, both frames are filtered to ``file_date <= cutoff``
            so historical reconciliations do not leak future data.

    Returns:
        LazyFrame with one row per input claims row, carrying the dim columns
        plus these derived columns:
            - ``net_expenditure`` (alias of ``total_exp_amt_agg``)
            - ``eligible_member_months`` (alias of ``elig_mnths`` from enroll)
            - ``pbpm`` (net_expenditure / eligible_member_months, null on 0)
            - ``claim_type_label`` (human-readable CMS claim type)
            - ``claim_type_part`` (Part A / Part B / Unknown)
            - ``srvc_month_date`` (first-of-month Date built from clndr_yr/mnth)
            - ``program`` (REACH / MSSP / Unknown from aco_id prefix)
    """
    if as_of_delivery_date is not None:
        cutoff_filter = MerReconciliationExpression.as_of_delivery_filter(
            as_of_delivery_date
        )
        claims = claims.filter(cutoff_filter)
        enroll = enroll.filter(cutoff_filter)

    # Only keep the enroll columns the view needs, and rename elig_mnths so
    # the output uses a speakable name. We deduplicate by the dim tuple in
    # case the enroll frame has multiple snapshots of the same dim.
    enroll_slim = (
        enroll.select([*_DIM_COLS, "elig_mnths"])
        .unique(subset=_DIM_COLS, keep="first")
        .rename({"elig_mnths": "eligible_member_months"})
    )

    joined = claims.join(enroll_slim, on=_DIM_COLS, how="left")

    # Derived columns via the registered expressions. Order matters:
    # net_expenditure must exist before pbpm_expr can reference it.
    return joined.with_columns(
        MerReconciliationExpression.net_expenditure_expr(),
        MerReconciliationExpression.claim_type_label_expr(),
        MerReconciliationExpression.claim_type_part_expr(),
        MerReconciliationExpression.srvc_month_date_expr(),
        MerReconciliationExpression.program_expr(),
    ).with_columns(
        # pbpm references net_expenditure which was added in the previous
        # with_columns block.
        MerReconciliationExpression.pbpm_expr(),
    )
