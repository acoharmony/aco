# © 2025 HarmonyCares
# All rights reserved.

"""
BNMR cap ↔ PLARU payment-history reconciliation view (milestone M1).

Joins the BNMR ``reach_bnmr_cap`` sheet against an aggregated PLARU
``payment_history`` roll-up to produce a tie-out diff at grain
``(aco_id, pmt_mnth)``.

BNMR cap's native grain is ``(perf_yr, aco_id, bnmrk, pmt_mnth,
align_type)``. PLARU's ``payment_history`` has no perf_yr / bnmrk /
align_type dimensions — it is a straight payment ledger keyed on
``payment_date``. We therefore collapse the BNMR side to
``(aco_id, pmt_mnth)`` before joining; any within-month bnmrk/align_type
split checks belong in a later milestone.

Three component tie-outs are computed — BPCC, EPCC, APO — plus a TCC
internal-consistency check (``bpcc + epcc + apo == tcc``).
"""

from __future__ import annotations

import polars as pl

from .._expressions._bnmr_cap_reconciliation import BnmrCapReconciliationExpression


def aggregate_plaru_payment_history(
    plaru_payment_history: pl.LazyFrame,
    aco_id: str,
) -> pl.LazyFrame:
    """
    Collapse PLARU ``payment_history`` to ``(aco_id, pmt_mnth)`` grain.

    PLARU is scoped to a single ACO (one workbook per ACO per delivery),
    so the aco_id is supplied as a scalar and broadcast across all rows.
    Monetary columns are parsed from strings to floats.

    Args:
        plaru_payment_history: LazyFrame with columns ``payment_date``,
            ``base_pcc_total``, ``enhanced_pcc_total``, ``apo_total``.
            Monetary columns are string-typed in the raw parse.
        aco_id: ACO identifier to stamp on every aggregated row.

    Returns:
        LazyFrame with columns ``aco_id``, ``pmt_mnth``, ``plaru_bpcc``,
        ``plaru_epcc``, ``plaru_apo``, ``plaru_tcc``.
    """
    return (
        plaru_payment_history.with_columns(
            BnmrCapReconciliationExpression.pmt_mnth_from_payment_date_expr(),
            pl.col("base_pcc_total").cast(pl.Float64, strict=False).alias("_bpcc"),
            pl.col("enhanced_pcc_total").cast(pl.Float64, strict=False).alias("_epcc"),
            pl.col("apo_total").cast(pl.Float64, strict=False).alias("_apo"),
        )
        .group_by("pmt_mnth")
        .agg(
            pl.col("_bpcc").sum().alias("plaru_bpcc"),
            pl.col("_epcc").sum().alias("plaru_epcc"),
            pl.col("_apo").sum().alias("plaru_apo"),
        )
        .with_columns(
            pl.lit(aco_id).alias("aco_id"),
            (pl.col("plaru_bpcc") + pl.col("plaru_epcc") + pl.col("plaru_apo")).alias(
                "plaru_tcc"
            ),
        )
        .select("aco_id", "pmt_mnth", "plaru_bpcc", "plaru_epcc", "plaru_apo", "plaru_tcc")
    )


def aggregate_bnmr_cap(
    bnmr_cap: pl.LazyFrame,
    as_of_delivery_date: str | None = None,
) -> pl.LazyFrame:
    """
    Collapse BNMR cap to ``(aco_id, pmt_mnth)`` grain with reconcilable
    amounts resolved across old-vs-new format.

    Args:
        bnmr_cap: LazyFrame sourced from ``silver.reach_bnmr_cap``. Must
            carry the full set of old- and new-format cap columns plus
            ``aco_id``, ``pmt_mnth``, ``file_date``.
        as_of_delivery_date: Optional ISO date string (``'YYYY-MM-DD'``).
            Filters rows to ``file_date <= cutoff``.

    Returns:
        LazyFrame with columns ``aco_id``, ``pmt_mnth``, ``bnmr_bpcc``,
        ``bnmr_epcc``, ``bnmr_apo``, ``bnmr_tcc``.
    """
    if as_of_delivery_date is not None:
        bnmr_cap = bnmr_cap.filter(pl.col("file_date") <= as_of_delivery_date)

    return (
        bnmr_cap.with_columns(
            BnmrCapReconciliationExpression.bpcc_reconcilable_expr(),
            BnmrCapReconciliationExpression.epcc_reconcilable_expr(),
            BnmrCapReconciliationExpression.apo_reconcilable_expr(),
            BnmrCapReconciliationExpression.tcc_reconcilable_expr(),
        )
        .group_by("aco_id", "pmt_mnth")
        .agg(
            pl.col("bpcc_reconcilable").sum().alias("bnmr_bpcc"),
            pl.col("epcc_reconcilable").sum().alias("bnmr_epcc"),
            pl.col("apo_reconcilable").sum().alias("bnmr_apo"),
            pl.col("tcc_reconcilable").sum().alias("bnmr_tcc"),
        )
    )


def build_bnmr_cap_reconciliation_view(
    bnmr_cap: pl.LazyFrame,
    plaru_payment_history: pl.LazyFrame,
    aco_id: str,
    as_of_delivery_date: str | None = None,
) -> pl.LazyFrame:
    """
    Build the BNMR cap reconciliation view.

    Joins aggregated BNMR cap to aggregated PLARU payment history on
    ``(aco_id, pmt_mnth)`` via a full outer join so either-side-only
    buckets surface as non-zero diffs rather than being dropped.

    Args:
        bnmr_cap: ``silver.reach_bnmr_cap`` LazyFrame.
        plaru_payment_history: PLARU payment_history sheet LazyFrame
            (sheet_type ``payment_history``).
        aco_id: ACO identifier scoping the PLARU rollup.
        as_of_delivery_date: Optional ISO date; filters BNMR cap by
            ``file_date``.

    Returns:
        LazyFrame keyed on ``(aco_id, pmt_mnth)`` with:
            - ``bnmr_bpcc`` / ``plaru_bpcc`` / ``bpcc_diff``
            - ``bnmr_epcc`` / ``plaru_epcc`` / ``epcc_diff``
            - ``bnmr_apo`` / ``plaru_apo`` / ``apo_diff``
            - ``bnmr_tcc`` / ``plaru_tcc`` / ``tcc_diff``
            - ``tcc_internal_diff`` — ``bnmr_tcc - (bpcc+epcc+apo)``;
              should be zero regardless of upstream.
    """
    bnmr_side = aggregate_bnmr_cap(bnmr_cap, as_of_delivery_date=as_of_delivery_date)
    plaru_side = aggregate_plaru_payment_history(plaru_payment_history, aco_id=aco_id)

    joined = bnmr_side.join(
        plaru_side, on=["aco_id", "pmt_mnth"], how="full", coalesce=True
    ).with_columns(
        pl.col("bnmr_bpcc").fill_null(0.0),
        pl.col("bnmr_epcc").fill_null(0.0),
        pl.col("bnmr_apo").fill_null(0.0),
        pl.col("bnmr_tcc").fill_null(0.0),
        pl.col("plaru_bpcc").fill_null(0.0),
        pl.col("plaru_epcc").fill_null(0.0),
        pl.col("plaru_apo").fill_null(0.0),
        pl.col("plaru_tcc").fill_null(0.0),
    )

    return joined.with_columns(
        (pl.col("bnmr_bpcc") - pl.col("plaru_bpcc")).abs().alias("bpcc_diff"),
        (pl.col("bnmr_epcc") - pl.col("plaru_epcc")).abs().alias("epcc_diff"),
        (pl.col("bnmr_apo") - pl.col("plaru_apo")).abs().alias("apo_diff"),
        (pl.col("bnmr_tcc") - pl.col("plaru_tcc")).abs().alias("tcc_diff"),
        (
            pl.col("bnmr_tcc")
            - (pl.col("bnmr_bpcc") + pl.col("bnmr_epcc") + pl.col("bnmr_apo"))
        )
        .abs()
        .alias("tcc_internal_diff"),
    )
