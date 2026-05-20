"""
CCLF → MER-taxonomy expressions.

Smallest-unit polars expressions for classifying, filtering, and dating CCLF
bronze-to-silver claim rows into the CMS MER (Monthly Expenditure Report)
bucketing scheme. These expressions are consumed by the companion transform
``_cclf_mer_spend`` (PR B1) and the member-months transform (PR B2) to build
the CCLF-side input for 1:1 MER reconciliation.

Each CCLF file ships with a ``clm_type_cd`` column whose values map directly
onto the MER taxonomy:

  - CCLF1 (Part A institutional claim header):  10, 20, 30, 40, 50, 60
  - CCLF5 (Part B professional line):           71, 72
  - CCLF6 (Part B DME line):                    81, 82

Stray CMS codes outside the MER taxonomy (e.g. ``61`` = "inpatient denied")
are filtered out by ``mer_claim_type_filter`` so they cannot leak into a
reconciliation bucket.

Sign correctness is handled by ``net_header_payment_expr`` (CCLF1) and
``net_line_payment_expr`` (CCLF5/6) which apply the CCLF Implementation Guide
§5.3.1 cancellation-negation rule: ``clm_adjsmt_type_cd='1'`` flips the sign
of the payment amount. These expressions are intentionally simpler than
``CclfAdrExpression.negate_cancellations_*`` — they emit a single
``net_payment`` column and avoid touching the charge columns (which aren't
present on every CCLF variant we reconcile).
"""

from __future__ import annotations

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression

# The canonical MER claim-type taxonomy. Kept as a frozenset here (not a list)
# so ``is_in`` membership is a constant-time lookup. These values are stable
# CMS identifiers — changing them breaks reconciliation.
_MER_CLAIM_TYPE_CODES: frozenset[str] = frozenset(
    {"10", "20", "30", "40", "50", "60", "71", "72", "81", "82"}
)


@register_expression(
    "cclf_mer_taxonomy",
    schemas=["silver", "gold"],
    dataset_types=["claims", "reconciliation"],
    callable=False,
    description="CCLF claim filters + net-payment exprs for MER reconciliation",
)
class CclfMerTaxonomyExpression:
    """Expression builders for CCLF → MER-taxonomy bucketing."""

    @staticmethod
    @expression(
        name="cclf_mer_claim_type_filter",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def mer_claim_type_filter() -> pl.Expr:
        """
        Keep only rows whose ``clm_type_cd`` is in the canonical MER taxonomy.

        Drops stray CMS codes (e.g. ``61``) and nulls. Used as a filter expr
        on any CCLF frame before aggregation into MER buckets.
        """
        return pl.col("clm_type_cd").is_in(list(_MER_CLAIM_TYPE_CODES))

    @staticmethod
    @expression(
        name="cclf_service_year_month",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def service_year_month_expr() -> pl.Expr:
        """
        Derive YYYYMM integer service month from ``clm_from_dt``.

        Matches the YYYYMM-int grain used by the alignment long-format
        frame (see ``FinancialPmpmExpression.calculate_member_months``),
        so the PR B3 join on ``(bene_mbi_id, year_month, program)`` is
        a direct key match.

        Null dates propagate as null — no silent-zero substitution.
        """
        return (
            pl.col("clm_from_dt").dt.year() * 100 + pl.col("clm_from_dt").dt.month()
        ).alias("year_month")

    @staticmethod
    @expression(
        name="cclf_net_header_payment",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def net_header_payment_expr() -> pl.Expr:
        """
        CCLF1 header-level payment with CCLF IG §5.3.1 cancellation netting.

        Rule: ``clm_adjsmt_type_cd == '1'`` (cancellation) flips the sign
        of ``clm_pmt_amt``. Originals ('0') and adjustments ('2') keep
        their sign.

        Returns a single expression aliased ``net_payment`` so the transform
        layer can do ``sum("net_payment")`` directly. Unlike
        ``CclfAdrExpression.negate_cancellations_header`` this does not
        touch ``clm_mdcr_instnl_tot_chrg_amt`` — we don't need it for
        reconciliation and requiring it would bloat every caller.
        """
        return (
            pl.when(pl.col("clm_adjsmt_type_cd") == "1")
            .then(-pl.col("clm_pmt_amt").cast(pl.Decimal(scale=2)))
            .otherwise(pl.col("clm_pmt_amt").cast(pl.Decimal(scale=2)))
            .alias("net_payment")
        )

    @staticmethod
    @expression(
        name="cclf_net_line_payment",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def net_line_payment_expr() -> pl.Expr:
        """
        CCLF5/6 line-level payment with CCLF IG §5.3.1 cancellation netting.

        Same rule as the header variant but operates on the line-level
        ``clm_line_cvrd_pd_amt`` column. Again, returns a single aliased
        ``net_payment`` expression and leaves charge columns alone.
        """
        return (
            pl.when(pl.col("clm_adjsmt_type_cd") == "1")
            .then(-pl.col("clm_line_cvrd_pd_amt").cast(pl.Decimal(scale=2)))
            .otherwise(pl.col("clm_line_cvrd_pd_amt").cast(pl.Decimal(scale=2)))
            .alias("net_payment")
        )

    @staticmethod
    @expression(
        name="cclf_mer_as_of_delivery_filter",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def as_of_delivery_filter(cutoff: str) -> pl.Expr:
        """
        Point-in-time filter: keep CCLF rows whose ``file_date`` ≤ ``cutoff``.

        Mirrors the MER-side ``MerReconciliationExpression.as_of_delivery_filter``
        so both sides of the reconciliation use identical semantics. Lives
        here (in addition to the MER module) so CCLF-side transforms do not
        have to reach across modules.
        """
        return pl.col("file_date") <= pl.lit(cutoff)
