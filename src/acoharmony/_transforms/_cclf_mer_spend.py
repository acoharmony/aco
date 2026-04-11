# © 2025 HarmonyCares
# All rights reserved.

"""
CCLF → MER-taxonomy spend transform (PR B1 of 3).

Pure-function transform that takes three CCLF LazyFrames (CCLF1 header-level,
CCLF5 line-level physician, CCLF6 line-level DMERC) and produces a tidy
spend frame keyed by ``(bene_mbi_id, year_month, clm_type_cd)``. Each row
carries a ``total_spend`` column representing net incurred expenditure for
that beneficiary-month-bucket, with CCLF IG §5.3.1 cancellation netting
applied.

This transform is the numerator side of the eventual
``financial_expenditure_by_cms_claim_type`` gold table. The denominator
(``member_months``) is computed separately in PR B2 from the ALR/BAR
historical alignment deliveries, and the two are joined in PR B3.

Point-in-time correctness
-------------------------
The optional ``as_of_cutoff`` argument filters every CCLF input to rows
whose ``file_date <= cutoff``, so historical reconciliation against a MER
delivery never sees CCLF deliveries that did not yet exist at that cutoff.

The transform intentionally does NOT join alignment. It has no notion of
``aco_id`` or ``program``; those attributions are applied downstream in
PR B3 via the ALR/BAR-derived member-months frame. Keeping spend pure
means it can be reused for any attribution scheme (ACO, cohort, geography)
without rewriting the dollars-by-claim-type logic.
"""

from __future__ import annotations

import polars as pl

from .._expressions._cclf_mer_taxonomy import CclfMerTaxonomyExpression


def _bucket_cclf1(cclf1: pl.LazyFrame) -> pl.LazyFrame:
    """Prepare CCLF1 (Part A institutional headers) for aggregation.

    CCLF1 is header-grained (one row per claim), so aggregation is a
    straight sum of net payments. No line-level rollup required.
    """
    return (
        cclf1.filter(CclfMerTaxonomyExpression.mer_claim_type_filter())
        .with_columns(
            CclfMerTaxonomyExpression.service_year_month_expr(),
            CclfMerTaxonomyExpression.net_header_payment_expr(),
        )
        .select("bene_mbi_id", "year_month", "clm_type_cd", "net_payment")
    )


def _bucket_cclf_line(cclf: pl.LazyFrame) -> pl.LazyFrame:
    """Prepare CCLF5 or CCLF6 (line-grained Part B) for aggregation.

    Both Part B files are line-level, so we apply the line-level net
    expression. The downstream group_by naturally sums lines within the
    same beneficiary-month-bucket.
    """
    return (
        cclf.filter(CclfMerTaxonomyExpression.mer_claim_type_filter())
        .with_columns(
            CclfMerTaxonomyExpression.service_year_month_expr(),
            CclfMerTaxonomyExpression.net_line_payment_expr(),
        )
        .select("bene_mbi_id", "year_month", "clm_type_cd", "net_payment")
    )


def build_cclf_mer_spend(
    cclf1: pl.LazyFrame,
    cclf5: pl.LazyFrame,
    cclf6: pl.LazyFrame,
    as_of_cutoff: str | None = None,
) -> pl.LazyFrame:
    """
    Build CCLF-side spend frame in MER taxonomy.

    Args:
        cclf1: CCLF1 header-level LazyFrame. Must contain ``bene_mbi_id``,
            ``clm_type_cd``, ``clm_from_dt``, ``clm_pmt_amt``,
            ``clm_adjsmt_type_cd``, ``file_date``.
        cclf5: CCLF5 line-level LazyFrame (Part B professional). Must
            contain ``bene_mbi_id``, ``clm_type_cd``, ``clm_from_dt``,
            ``clm_line_cvrd_pd_amt``, ``clm_adjsmt_type_cd``, ``file_date``.
        cclf6: CCLF6 line-level LazyFrame (Part B DMERC). Same column set
            as cclf5.
        as_of_cutoff: Optional ISO date string (``'YYYY-MM-DD'``). When
            provided, every input is filtered to ``file_date <= cutoff``
            before aggregation, enforcing no-future-data-leak on historical
            reconciliations.

    Returns:
        LazyFrame with one row per ``(bene_mbi_id, year_month, clm_type_cd)``
        tuple, carrying a Decimal ``total_spend`` column (sum of net payments
        across all lines/headers in that bucket). Only claim types in the
        canonical MER taxonomy (10, 20, 30, 40, 50, 60, 71, 72, 81, 82)
        appear in the output; stray codes like 61 are dropped.
    """
    if as_of_cutoff is not None:
        cutoff_filter = CclfMerTaxonomyExpression.as_of_delivery_filter(as_of_cutoff)
        cclf1 = cclf1.filter(cutoff_filter)
        cclf5 = cclf5.filter(cutoff_filter)
        cclf6 = cclf6.filter(cutoff_filter)

    cclf1_prep = _bucket_cclf1(cclf1)
    cclf5_prep = _bucket_cclf_line(cclf5)
    cclf6_prep = _bucket_cclf_line(cclf6)

    unioned = pl.concat([cclf1_prep, cclf5_prep, cclf6_prep], how="vertical_relaxed")

    return (
        unioned.group_by("bene_mbi_id", "year_month", "clm_type_cd")
        .agg(pl.col("net_payment").sum().alias("total_spend"))
        .sort("bene_mbi_id", "year_month", "clm_type_cd")
    )
