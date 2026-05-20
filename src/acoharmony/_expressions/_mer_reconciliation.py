"""
MER (Monthly Expenditure Report) reconciliation expressions.

Smallest-unit polars expressions used to tie CMS Monthly Expenditure Reports
to our calculated expenditure data. Each expression is a pure lambda-shaped
transformation that can be composed inside a ``with_columns`` or ``filter``
call; none of them read data or manage state.

Background
----------
The CMS MER ``data_claims`` sheet reports incurred expenditures bucketed by
``clm_type_cd`` — a two-digit code CMS uses to split medical claims across
Part A institutional (Inpatient, SNF, SNF-Swing Beds, HHA, Hospice) and
Part B (Outpatient hospital, Physician, DME/Physician, DMERC). Our
reconciliation view needs:

- Human-readable labels for the codes
- Part A/B classification (MER-specific: Outpatient=40 is Part B here)
- Derived PBPM = net expenditure / eligible member-months
- Point-in-time filtering so "as of delivery X" views drop rows whose
  source file was not yet available at X
- Service month first-of-month Date built from ``(clndr_yr, clndr_mnth)``
- Program (REACH/MSSP) derived from the ACO ID prefix

These expressions are consumed by the ``_mer_reconciliation_view`` transform
(next layer up), which joins ``mexpr_data_claims`` to ``mexpr_data_enroll``
and produces the tidy long-format reconciliation frame.

References
----------
- CMS REACH Monthly Expenditure Report Implementation Guide
- CCLF Implementation Guide v40.0 §5.3.1 (for the CMS claim-type taxonomy)
"""

from __future__ import annotations

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression

# ---------------------------------------------------------------------------
# Canonical MER claim-type taxonomy
# ---------------------------------------------------------------------------
# These codes are stable CMS identifiers; changing them breaks reconciliation.
# Keep the label strings exactly as they appear in the MER Excel sheets so
# downstream diffing is straightforward.
_CLAIM_TYPE_LABELS: dict[str, str] = {
    "10": "HHA",
    "20": "SNF",
    "30": "SNF - Swing Beds",
    "40": "Outpatient",
    "50": "Hospice",
    "60": "Inpatient",
    "71": "Physician",
    "72": "DME/Physician",
    "81": "DMERC/non-DMEPOS",
    "82": "DMERC/DMEPOS",
}

# Part A = institutional inpatient-like services (MER convention).
# Part B = professional + outpatient hospital + DME (MER convention).
# NOTE: Outpatient hospital (40) is classified as Part B by the MER, which
# differs from the common "CCLF2=outpatient institutional" mental model.
_PART_A_CODES: frozenset[str] = frozenset({"10", "20", "30", "50", "60"})
_PART_B_CODES: frozenset[str] = frozenset({"40", "71", "72", "81", "82"})


@register_expression(
    "mer_reconciliation",
    schemas=["silver", "gold"],
    dataset_types=["reconciliation", "mexpr"],
    callable=False,
    description="MER reconciliation helpers: labels, PBPM, PIT filter",
)
class MerReconciliationExpression:
    """Expression builders for MER → CCLF reconciliation views."""

    @staticmethod
    @expression(
        name="mer_claim_type_label",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def claim_type_label_expr() -> pl.Expr:
        """
        Map ``clm_type_cd`` → human-readable label.

        Unknown codes (including nulls) are labeled ``Unknown (<code>)`` so
        divergence between MER and our calcs is immediately visible instead
        of silently nulled.
        """
        # Build the nested when/then chain explicitly so we never lose an
        # "unknown" row. ``pl.lit`` + string concat gives us the fallback.
        expr = pl.when(pl.col("clm_type_cd").is_null()).then(pl.lit("Unknown (null)"))
        for code, label in _CLAIM_TYPE_LABELS.items():
            expr = expr.when(pl.col("clm_type_cd") == code).then(pl.lit(label))
        expr = expr.otherwise(
            pl.lit("Unknown (") + pl.col("clm_type_cd").cast(pl.Utf8) + pl.lit(")")
        )
        return expr.alias("claim_type_label")

    @staticmethod
    @expression(
        name="mer_claim_type_part",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def claim_type_part_expr() -> pl.Expr:
        """
        Map ``clm_type_cd`` → 'Part A' / 'Part B' / 'Unknown'.

        MER convention: Outpatient (40) counts as Part B even though CCLF2
        (outpatient institutional) is an institutional file. Do not "fix" this.
        """
        return (
            pl.when(pl.col("clm_type_cd").is_in(list(_PART_A_CODES)))
            .then(pl.lit("Part A"))
            .when(pl.col("clm_type_cd").is_in(list(_PART_B_CODES)))
            .then(pl.lit("Part B"))
            .otherwise(pl.lit("Unknown"))
            .alias("claim_type_part")
        )

    @staticmethod
    @expression(
        name="mer_net_expenditure",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def net_expenditure_expr() -> pl.Expr:
        """
        Expose the CMS-provided ``total_exp_amt_agg`` as ``net_expenditure``.

        The MER ships ~10 component reduction columns (sqstr, apa_rdctn,
        pcc_rdctn, tcc_rdctn, apo_rdctn, ucc, nonpbp_rdct, op_dsh, cp_dsh,
        op_ime, cp_ime, dc_amt_agg_apa) alongside ``clm_pmt_amt_agg`` and a
        pre-netted ``total_exp_amt_agg``. Reconciliation compares against
        ``total_exp_amt_agg`` directly rather than recomputing the net
        locally — any arithmetic drift we might introduce here would
        manifest as a spurious diff against CMS ground truth.

        Nulls in ``total_exp_amt_agg`` propagate as nulls; there is no
        silent zero-fill.
        """
        return pl.col("total_exp_amt_agg").alias("net_expenditure")

    @staticmethod
    @expression(
        name="mer_pbpm",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def pbpm_expr() -> pl.Expr:
        """
        Compute PBPM = net_expenditure / eligible_member_months.

        Inputs:
            - ``net_expenditure`` (Decimal or Float)
            - ``eligible_member_months`` (integer)

        Zero member-months produces null (not inf or an error). The result
        is cast to Float64 for downstream numeric comparison; use the
        Decimal form of ``net_expenditure`` upstream if absolute precision
        matters.
        """
        denom = pl.col("eligible_member_months").cast(pl.Float64)
        num = pl.col("net_expenditure").cast(pl.Float64)
        return (
            pl.when(denom == 0).then(None).otherwise(num / denom).alias("pbpm")
        )

    @staticmethod
    @expression(
        name="mer_as_of_delivery_filter",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def as_of_delivery_filter(cutoff: str) -> pl.Expr:
        """
        Point-in-time filter: keep rows whose ``file_date`` ≤ ``cutoff``.

        Used to ensure reconciliation as-of a given MER delivery only sees
        data that existed on or before that delivery, preventing future-data
        leaks into historical tie-outs.

        Args:
            cutoff: ISO date string (e.g. ``'2025-11-30'``).

        Returns:
            A filter expression suitable for ``LazyFrame.filter(...)``.
        """
        return pl.col("file_date") <= pl.lit(cutoff)

    @staticmethod
    @expression(
        name="mer_srvc_month_date",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def srvc_month_date_expr() -> pl.Expr:
        """
        Build a first-of-month ``pl.Date`` from ``(clndr_yr, clndr_mnth)``.

        MER stores year and month as separate string columns without
        zero-padding the month, so ``clndr_mnth='1'`` must become
        ``2024-01-01``, not ``2024-1-01`` which would fail strict parsing.
        """
        padded_month = pl.col("clndr_mnth").cast(pl.Utf8).str.zfill(2)
        iso = pl.col("clndr_yr").cast(pl.Utf8) + pl.lit("-") + padded_month + pl.lit("-01")
        return iso.str.strptime(pl.Date, "%Y-%m-%d").alias("srvc_month_date")

    @staticmethod
    @expression(
        name="mer_program",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def program_expr() -> pl.Expr:
        """
        Derive ACO program (REACH / MSSP / Unknown) from the aco_id prefix.

        REACH participants get ACO IDs prefixed with ``D`` (e.g. ``D0259``),
        MSSP participants get ``A`` (e.g. ``A5678``). Any other prefix —
        including null — yields 'Unknown' so bad data is visible.
        """
        return (
            pl.when(pl.col("aco_id").cast(pl.Utf8).str.starts_with("D"))
            .then(pl.lit("REACH"))
            .when(pl.col("aco_id").cast(pl.Utf8).str.starts_with("A"))
            .then(pl.lit("MSSP"))
            .otherwise(pl.lit("Unknown"))
            .alias("program")
        )
