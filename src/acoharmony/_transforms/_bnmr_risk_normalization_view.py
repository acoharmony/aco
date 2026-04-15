# © 2025 HarmonyCares
# All rights reserved.

"""
BNMR risk-score normalization + capping reconciliation view (milestone M9).

Builds on M2b: given a per-beneficiary scored_benes frame, this view
computes the full normalization + capping + benchmark chain and ties
out each stage against what the BNMR ``riskscore_ad`` / ``riskscore_esrd``
sheets report.

Tie-out layers
--------------
Two independent layers of correctness, either of which catching a bug:

1. **Internal sheet arithmetic.** For each BNMR delivery, the
   ``expected_*`` values the transform computes from Raw + NF + RY
   baselines must equal the ``*_py`` named fields CMS already put on
   the sheet. This is a check on the CMS workbook's own formulas being
   interpreted correctly — if BNMR ships inconsistent numbers, this
   layer flags it.

2. **Raw score reconstruction.** The Raw Risk Score on the BNMR sheet
   should equal the population average of per-bene HCC scores M2b
   reconstructs from CCLF claims + CCLF8 demographics. If M2b matches
   and the sheet's internal arithmetic checks, the entire normalization
   chain is reproduced from upstream data.

The Raw comparison comes for free via the M2b reconciliation view; M9
focuses on the normalization-and-capping layer.

Tolerance: 1e-4 (4 decimal places). Matrix-stamped CMS values arrive
as strings from the parse, so all numeric cols get cast up front.
"""

from __future__ import annotations

import polars as pl

from .._expressions._bnmr_risk_normalization import (
    BnmrRiskNormalizationExpression,
)

RISK_NORMALIZATION_TOLERANCE = 1e-4

# Grain of the riskscore_ad / riskscore_esrd sheets is per-delivery.
# ``source_filename`` is the authoritative delivery key (carries
# perf_yr, aco_id, delivery timestamp implicitly).
DELIVERY_GRAIN: tuple[str, ...] = ("source_filename", "aco_id", "performance_year")


def _as_float(col: str) -> pl.Expr:
    """Cast a string-or-null column to Float64, non-strict (nulls pass through)."""
    return pl.col(col).cast(pl.Float64, strict=False).alias(col)


def prepare_riskscore_inputs(
    riskscore_sheet: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Collapse a ``riskscore_ad`` or ``riskscore_esrd`` silver frame to one
    row per delivery with numeric inputs / outputs typed as Float64.

    Each riskscore sheet ships ~36 rows per delivery (one per calculation
    line). The matrix-extracted named fields (``*_py``) appear on every
    row via broadcast, so a ``unique()`` on the delivery grain recovers
    one row per file. Same pattern M2a's aggregator uses for
    ``bene_dcnt_annual``.

    Expected input columns:
        raw_risk_score, normalization_factor, ry_normalized_risk_score,
        cif, normalized_risk_score_claims_py, capped_risk_score_claims_py,
        benchmark_risk_score_claims_py,
        + DELIVERY_GRAIN.

    Returns a lazy frame keyed on DELIVERY_GRAIN with all numeric cols
    cast to Float64.
    """
    numeric_cols = [
        "raw_risk_score",
        "normalization_factor",
        "ry_normalized_risk_score",
        "cif",
        "normalized_risk_score_claims_py",
        "capped_risk_score_claims_py",
        "benchmark_risk_score_claims_py",
    ]
    return (
        riskscore_sheet.unique(subset=list(DELIVERY_GRAIN))
        .with_columns(*[_as_float(c) for c in numeric_cols])
    )


def build_bnmr_risk_normalization_reconciliation_view(
    riskscore_sheet: pl.LazyFrame,
    as_of_delivery_date: str | None = None,
) -> pl.LazyFrame:
    """
    Build the BNMR risk-normalization tie-out view.

    Takes a single riskscore sheet (AD or ESRD) and verifies the
    normalization chain: each delivery's CMS-reported
    ``normalized_risk_score_claims_py`` equals ``raw_risk_score ×
    normalization_factor``, its ``capped_risk_score_claims_py`` equals
    the clamped value (or bypasses when RY normalized is null), and its
    ``benchmark_risk_score_claims_py`` equals ``capped × CIF``.

    Args:
        riskscore_sheet: ``silver.reach_bnmr_riskscore_ad`` or
            ``silver.reach_bnmr_riskscore_esrd`` LazyFrame.
        as_of_delivery_date: Optional ISO date. Filters BNMR rows by
            ``file_date <= cutoff`` for historical PIT reconstructions.

    Returns:
        LazyFrame keyed on DELIVERY_GRAIN with per-stage diffs:
            - ``normalized_risk_score_diff``
            - ``capped_risk_score_diff``
            - ``benchmark_risk_score_diff``
    """
    if as_of_delivery_date is not None:
        riskscore_sheet = riskscore_sheet.filter(
            pl.col("file_date") <= as_of_delivery_date
        )

    prepped = prepare_riskscore_inputs(riskscore_sheet)

    # Stage 1: Normalized = Raw × NF
    staged = prepped.with_columns(
        BnmrRiskNormalizationExpression.normalized_expr(),
        BnmrRiskNormalizationExpression.cap_floor_expr(),
        BnmrRiskNormalizationExpression.cap_ceiling_expr(),
    )
    # Stage 2: Capped from floor/ceiling (or bypass)
    staged = staged.with_columns(
        BnmrRiskNormalizationExpression.capped_expr()
    )
    # Stage 3: Benchmark = Capped × CIF
    staged = staged.with_columns(
        BnmrRiskNormalizationExpression.benchmark_expr()
    )

    # Per-stage absolute diffs vs. CMS-reported values.
    return staged.with_columns(
        (
            pl.col("expected_normalized_risk_score")
            - pl.col("normalized_risk_score_claims_py")
        )
        .abs()
        .alias("normalized_risk_score_diff"),
        (
            pl.col("expected_capped_risk_score")
            - pl.col("capped_risk_score_claims_py")
        )
        .abs()
        .alias("capped_risk_score_diff"),
        (
            pl.col("expected_benchmark_risk_score")
            - pl.col("benchmark_risk_score_claims_py")
        )
        .abs()
        .alias("benchmark_risk_score_diff"),
    )
