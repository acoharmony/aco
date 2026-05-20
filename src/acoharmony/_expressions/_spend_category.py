# © 2025 HarmonyCares
# All rights reserved.

"""
Spend category classification expressions.

Provides Polars expressions for identifying and categorizing healthcare spend
by type (inpatient, outpatient, SNF, hospice, home health, DME, Part B carrier).

These expressions are composable units used by transforms to calculate spend metrics.
"""


import polars as pl

from .._decor8 import expression_method
from ._registry import register_expression


@register_expression(
    "spend_category",
    schemas=["gold"],
    dataset_types=["claims"],
    description="Spend categorization by service type",
)
class SpendCategoryExpression:
    """
    Generate Polars expressions for spend categorization.

    Expressions identify spend by facility type using bill_type_code.
    """

    @staticmethod
    @expression_method(
        expression_name="inpatient_spend",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_inpatient_spend(
        bill_type_col: str = "bill_type_code",
        paid_col: str = "paid_amount",
    ) -> pl.Expr:
        """
        Expression to identify inpatient facility spend.

        Inpatient: bill_type 11x, 12x

        Returns:
            Expression that evaluates to paid amount if inpatient, else 0
        """
        return pl.when(
            (pl.col(bill_type_col).str.starts_with("11"))
            | (pl.col(bill_type_col).str.starts_with("12"))
        ).then(pl.col(paid_col)).otherwise(0.0)

    @staticmethod
    @expression_method(
        expression_name="outpatient_spend",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_outpatient_spend(
        bill_type_col: str = "bill_type_code",
        paid_col: str = "paid_amount",
    ) -> pl.Expr:
        """
        Expression to identify outpatient facility spend.

        Outpatient: bill_type 13x

        Returns:
            Expression that evaluates to paid amount if outpatient, else 0
        """
        return pl.when(pl.col(bill_type_col).str.starts_with("13")).then(
            pl.col(paid_col)
        ).otherwise(0.0)

    @staticmethod
    @expression_method(
        expression_name="snf_spend",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_snf_spend(
        bill_type_col: str = "bill_type_code",
        paid_col: str = "paid_amount",
    ) -> pl.Expr:
        """
        Expression to identify skilled nursing facility spend.

        SNF: bill_type 21x, 22x

        Returns:
            Expression that evaluates to paid amount if SNF, else 0
        """
        return pl.when(
            (pl.col(bill_type_col).str.starts_with("21"))
            | (pl.col(bill_type_col).str.starts_with("22"))
        ).then(pl.col(paid_col)).otherwise(0.0)

    @staticmethod
    @expression_method(
        expression_name="hospice_spend",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_hospice_spend(
        bill_type_col: str = "bill_type_code",
        paid_col: str = "paid_amount",
    ) -> pl.Expr:
        """
        Expression to identify hospice spend.

        Hospice: bill_type 81x, 82x

        Returns:
            Expression that evaluates to paid amount if hospice, else 0
        """
        return pl.when(
            (pl.col(bill_type_col).str.starts_with("81"))
            | (pl.col(bill_type_col).str.starts_with("82"))
        ).then(pl.col(paid_col)).otherwise(0.0)

    @staticmethod
    @expression_method(
        expression_name="home_health_spend",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_home_health_spend(
        bill_type_col: str = "bill_type_code",
        paid_col: str = "paid_amount",
    ) -> pl.Expr:
        """
        Expression to identify home health spend.

        Home Health: bill_type 32x, 33x, 34x

        Returns:
            Expression that evaluates to paid amount if home health, else 0
        """
        return pl.when(
            (pl.col(bill_type_col).str.starts_with("32"))
            | (pl.col(bill_type_col).str.starts_with("33"))
            | (pl.col(bill_type_col).str.starts_with("34"))
        ).then(pl.col(paid_col)).otherwise(0.0)

    @staticmethod
    @expression_method(
        expression_name="part_b_carrier_spend",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_part_b_carrier_spend(
        bill_type_col: str = "bill_type_code",
        paid_col: str = "paid_amount",
    ) -> pl.Expr:
        """
        Expression to identify Part B carrier (professional) spend.

        Part B carrier claims have null or empty bill_type_code.

        Returns:
            Expression that evaluates to paid amount if Part B, else 0
        """
        return pl.when(
            (pl.col(bill_type_col).is_null()) | (pl.col(bill_type_col) == "")
        ).then(pl.col(paid_col)).otherwise(0.0)
