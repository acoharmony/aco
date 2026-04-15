"""
BNMR capitation reconciliation expressions.

The BNMR ``reach_bnmr_cap`` sheet reports per-capita-cost totals at a grain
of ``(perf_yr, aco_id, bnmrk, pmt_mnth, align_type)``. CMS changed the
column layout around April 2025:

- **Old format** (pre-April 2025): four columns each carrying a total
  amount — ``aco_tcc_amt_total``, ``aco_bpcc_amt_total``,
  ``aco_epcc_amt_total_seq``, ``aco_apo_amt_total_seq``.
- **New format** (April 2025 onward): three variants per metric
  (``_pre_seq_actual``, ``_post_seq_actual``, ``_post_seq_paid``) for
  TCC, BPCC, and APO, plus a single ``aco_epcc_amt_post_seq_paid``.

Every cap row is in exactly one format: old fields populated XOR new
fields populated. Coalescing the two formats together is **wrong** — the
methodology differs and the numbers are not directly comparable.

For reconciliation we pick one reconcilable-amount per component:

- **Old format** → the ``_total`` / ``_total_seq`` column as-is
- **New format** → ``_post_seq_paid`` (the final paid amount after
  sequestration; the closest analog to old ``_total_seq``)

The reconcilable amounts then tie out against PLARU's ``payment_history``
rollups at the ``(aco_id, pmt_mnth)`` grain.
"""

from __future__ import annotations

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


@register_expression(
    "bnmr_cap_reconciliation",
    schemas=["silver", "gold"],
    dataset_types=["reconciliation", "bnmr", "reach_bnmr", "plaru"],
    callable=False,
    description="BNMR cap ↔ PLARU payment_history reconciliation helpers",
)
class BnmrCapReconciliationExpression:
    """Expression builders for BNMR cap → PLARU reconciliation views."""

    @staticmethod
    @expression(
        name="is_new_cap_format",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def is_new_cap_format_expr() -> pl.Expr:
        """
        True when the row uses the post-April-2025 three-variant layout.

        Decided by presence of any of the new-format-only columns. A row is
        "new format" if *any* ``_pre_seq_actual``, ``_post_seq_actual``, or
        ``_post_seq_paid`` column is non-null.
        """
        return (
            pl.col("aco_tcc_amt_pre_seq_actual").is_not_null()
            | pl.col("aco_tcc_amt_post_seq_actual").is_not_null()
            | pl.col("aco_tcc_amt_post_seq_paid").is_not_null()
            | pl.col("aco_bpcc_amt_pre_seq_actual").is_not_null()
            | pl.col("aco_bpcc_amt_post_seq_actual").is_not_null()
            | pl.col("aco_bpcc_amt_post_seq_paid").is_not_null()
            | pl.col("aco_apo_amt_pre_seq_actual").is_not_null()
            | pl.col("aco_apo_amt_post_seq_actual").is_not_null()
            | pl.col("aco_apo_amt_post_seq_paid").is_not_null()
            | pl.col("aco_epcc_amt_post_seq_paid").is_not_null()
        ).alias("is_new_cap_format")

    @staticmethod
    @expression(
        name="is_old_cap_format",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def is_old_cap_format_expr() -> pl.Expr:
        """True when *any* old-format column is non-null."""
        return (
            pl.col("aco_tcc_amt_total").is_not_null()
            | pl.col("aco_bpcc_amt_total").is_not_null()
            | pl.col("aco_epcc_amt_total_seq").is_not_null()
            | pl.col("aco_apo_amt_total_seq").is_not_null()
        ).alias("is_old_cap_format")

    @staticmethod
    @expression(
        name="bpcc_reconcilable",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def bpcc_reconcilable_expr() -> pl.Expr:
        """BPCC amount to reconcile — old ``_total`` or new ``_post_seq_paid``."""
        return (
            pl.coalesce(
                pl.col("aco_bpcc_amt_total"),
                pl.col("aco_bpcc_amt_post_seq_paid"),
            )
        ).alias("bpcc_reconcilable")

    @staticmethod
    @expression(
        name="epcc_reconcilable",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def epcc_reconcilable_expr() -> pl.Expr:
        """EPCC amount — old ``_total_seq`` or new ``_post_seq_paid``."""
        return (
            pl.coalesce(
                pl.col("aco_epcc_amt_total_seq"),
                pl.col("aco_epcc_amt_post_seq_paid"),
            )
        ).alias("epcc_reconcilable")

    @staticmethod
    @expression(
        name="apo_reconcilable",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def apo_reconcilable_expr() -> pl.Expr:
        """APO amount — old ``_total_seq`` or new ``_post_seq_paid``."""
        return (
            pl.coalesce(
                pl.col("aco_apo_amt_total_seq"),
                pl.col("aco_apo_amt_post_seq_paid"),
            )
        ).alias("apo_reconcilable")

    @staticmethod
    @expression(
        name="tcc_reconcilable",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def tcc_reconcilable_expr() -> pl.Expr:
        """TCC amount — old ``_total`` or new ``_post_seq_paid``."""
        return (
            pl.coalesce(
                pl.col("aco_tcc_amt_total"),
                pl.col("aco_tcc_amt_post_seq_paid"),
            )
        ).alias("tcc_reconcilable")

    @staticmethod
    @expression(
        name="pmt_mnth_from_payment_date",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def pmt_mnth_from_payment_date_expr() -> pl.Expr:
        """
        Derive BNMR's ``pmt_mnth`` key (e.g. ``'2026-03'``) from PLARU's
        ``payment_date`` string. PLARU payment dates arrive as MM/DD/YYYY
        strings in the raw parse; extract YYYY-MM.
        """
        return (
            pl.col("payment_date")
            .str.strptime(pl.Date, format="%m/%d/%Y", strict=False)
            .dt.strftime("%Y-%m")
            .alias("pmt_mnth")
        )
