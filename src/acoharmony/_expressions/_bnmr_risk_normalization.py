"""
BNMR risk-score normalization + capping expressions (milestone M9).

The BNMR ``riskscore_ad`` and ``riskscore_esrd`` sheets express a fixed
calculation chain that turns a population-average HCC score (M2b's
``raw_risk_score``) into the ACO's final benchmark risk score. The
chain, documented in the sheets' own line descriptions, is:

    1. Raw Risk Score                       (M2b output)
    2. Normalization Factor                 (CMS-supplied, per delivery)
    3. Normalized Risk Score = (1) × (2)
    4. PY Risk Score Floor   = 0.97 × RY Normalized
    5. PY Risk Score Ceiling = 1.03 × RY Normalized
    6. Capped Risk Score     = clamp((3), (4), (5))
    7. CIF (Completion-Incurred Factor)     (CMS-supplied, per delivery)
    8. PY Benchmark Risk Score = (6) × (7)

The ±3% corridor is ACO-level — it bounds how far the ACO's normalized
score may drift year-over-year from its own reference-year baseline.
When the reference-year normalized score is missing (e.g. first PY for
a new ACO), the cap bypasses and capped = normalized.

Both sheets (AD = Aged/Disabled, ESRD) use identical formulas; the
difference is population cohort.
"""

from __future__ import annotations

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


# The ±3% corridor is embedded in CMS's BNMR worksheet line descriptions:
#   "PY Risk Score Floor (0.97 X RY normalized risk score)"
#   "PY Risk Score Ceiling (1.03 X RY normalized risk score)"
CAP_FLOOR_MULTIPLIER = 0.97
CAP_CEILING_MULTIPLIER = 1.03


@register_expression(
    "bnmr_risk_normalization",
    schemas=["silver", "gold"],
    dataset_types=["reconciliation", "bnmr", "reach_bnmr"],
    callable=False,
    description="BNMR risk-score normalization / capping / benchmark chain",
)
class BnmrRiskNormalizationExpression:
    """Expression builders for the BNMR riskscore_ad / riskscore_esrd chain."""

    @staticmethod
    @expression(
        name="normalized_risk_score",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def normalized_expr(
        raw_col: str = "raw_risk_score",
        factor_col: str = "normalization_factor",
    ) -> pl.Expr:
        """Normalized = Raw × Normalization Factor."""
        return (pl.col(raw_col) * pl.col(factor_col)).alias(
            "expected_normalized_risk_score"
        )

    @staticmethod
    @expression(
        name="cap_floor",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def cap_floor_expr(ry_normalized_col: str = "ry_normalized_risk_score") -> pl.Expr:
        """Floor = 0.97 × RY normalized."""
        return (pl.col(ry_normalized_col) * CAP_FLOOR_MULTIPLIER).alias(
            "expected_cap_floor"
        )

    @staticmethod
    @expression(
        name="cap_ceiling",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def cap_ceiling_expr(ry_normalized_col: str = "ry_normalized_risk_score") -> pl.Expr:
        """Ceiling = 1.03 × RY normalized."""
        return (pl.col(ry_normalized_col) * CAP_CEILING_MULTIPLIER).alias(
            "expected_cap_ceiling"
        )

    @staticmethod
    @expression(
        name="capped_risk_score",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def capped_expr(
        normalized_col: str = "expected_normalized_risk_score",
        floor_col: str = "expected_cap_floor",
        ceiling_col: str = "expected_cap_ceiling",
    ) -> pl.Expr:
        """
        Capped = clamp(normalized, floor, ceiling), with bypass.

        When the RY-normalized reference is missing (null → floor / ceiling
        null), the cap has no baseline to clamp against and the normalized
        value passes through unchanged. CMS documents this as the "new
        ACO" path: first PY for an organization has no prior-year anchor
        so no corridor applies yet.
        """
        return (
            pl.when(
                pl.col(floor_col).is_null() | pl.col(ceiling_col).is_null()
            )
            .then(pl.col(normalized_col))
            .otherwise(
                pl.min_horizontal(
                    pl.max_horizontal(pl.col(normalized_col), pl.col(floor_col)),
                    pl.col(ceiling_col),
                )
            )
            .alias("expected_capped_risk_score")
        )

    @staticmethod
    @expression(
        name="benchmark_risk_score",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=False,
    )
    def benchmark_expr(
        capped_col: str = "expected_capped_risk_score",
        cif_col: str = "cif",
    ) -> pl.Expr:
        """Benchmark = Capped × CIF (Completion-Incurred Factor)."""
        return (pl.col(capped_col) * pl.col(cif_col)).alias(
            "expected_benchmark_risk_score"
        )
