# © 2025 HarmonyCares
# All rights reserved.

"""
Clinical indicator expressions.

Provides Polars expressions for identifying clinical events and care transitions:
- Hospice admissions
- SNF admissions
- IRF admissions
- LTAC admissions
- Home health episodes

These expressions are composable units used by transforms to calculate clinical metrics.
"""


import polars as pl

from .._decor8 import expression_method
from ._registry import register_expression


@register_expression(
    "clinical_indicators",
    schemas=["gold"],
    dataset_types=["claims"],
    description="Clinical events and care transitions",
)
class ClinicalIndicatorExpression:
    """
    Generate Polars expressions for clinical indicators.

    Expressions identify care transitions and clinical events by facility type.
    """

    @staticmethod
    @expression_method(
        expression_name="is_hospice_admission",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_hospice_admission(bill_type_col: str = "bill_type_code") -> pl.Expr:
        """
        Expression to identify hospice admissions.

        Hospice: bill_type 81x, 82x

        Returns:
            Expression that evaluates to 1 if hospice admission, else 0
        """
        return pl.when(
            (pl.col(bill_type_col).str.starts_with("81"))
            | (pl.col(bill_type_col).str.starts_with("82"))
        ).then(1).otherwise(0)

    @staticmethod
    @expression_method(
        expression_name="is_snf_admission",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_snf_admission(bill_type_col: str = "bill_type_code") -> pl.Expr:
        """
        Expression to identify skilled nursing facility admissions.

        SNF: bill_type 21x, 22x

        Returns:
            Expression that evaluates to 1 if SNF admission, else 0
        """
        return pl.when(
            (pl.col(bill_type_col).str.starts_with("21"))
            | (pl.col(bill_type_col).str.starts_with("22"))
        ).then(1).otherwise(0)

    @staticmethod
    @expression_method(
        expression_name="is_irf_admission",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_irf_admission(bill_type_col: str = "bill_type_code") -> pl.Expr:
        """
        Expression to identify inpatient rehabilitation facility admissions.

        IRF: bill_type 82x (subset - need to verify with revenue codes)

        Returns:
            Expression that evaluates to 1 if IRF admission, else 0
        """
        return pl.when(pl.col(bill_type_col).str.starts_with("82")).then(1).otherwise(0)

    @staticmethod
    @expression_method(
        expression_name="is_ltac_admission",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_ltac_admission(
        bill_type_col: str = "bill_type_code",
        drg_code_col: str = "drg_code",
    ) -> pl.Expr:
        """
        Expression to identify long-term acute care hospital admissions.

        LTAC: bill_type 11x with specific DRG codes or length of stay > 25 days

        Returns:
            Expression that evaluates to 1 if LTAC admission, else 0
        """
        # LTAC typically identified by bill_type 11x with long length of stay
        # For now, use bill_type 11x as proxy (need LOS calculation for precision)
        return pl.when(pl.col(bill_type_col).str.starts_with("11")).then(0).otherwise(
            0
        )  # Placeholder - need LOS logic

    @staticmethod
    @expression_method(
        expression_name="is_home_health_episode",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_home_health_episode(bill_type_col: str = "bill_type_code") -> pl.Expr:
        """
        Expression to identify home health episodes.

        Home Health: bill_type 32x, 33x, 34x

        Returns:
            Expression that evaluates to 1 if home health episode, else 0
        """
        return pl.when(
            (pl.col(bill_type_col).str.starts_with("32"))
            | (pl.col(bill_type_col).str.starts_with("33"))
            | (pl.col(bill_type_col).str.starts_with("34"))
        ).then(1).otherwise(0)
