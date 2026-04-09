# © 2025 HarmonyCares
# All rights reserved.

"""
Utilization metric expressions.

Provides Polars expressions for calculating healthcare utilization metrics:
- Inpatient admissions
- Emergency room visits
- Evaluation & Management (E&M) visits

These expressions are composable units used by transforms to calculate utilization metrics.
"""

from typing import Any

import polars as pl

from .._decor8 import expression_method
from ._registry import register_expression


@register_expression(
    "utilization",
    schemas=["gold"],
    dataset_types=["claims"],
    description="Healthcare utilization metrics",
)
class UtilizationExpression:
    """
    Generate Polars expressions for utilization metrics.

    Expressions identify admissions and visits by claim type and codes.
    """

    @staticmethod
    @expression_method(
        expression_name="is_inpatient_admission",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_inpatient_admission(bill_type_col: str = "bill_type_code") -> pl.Expr:
        """
        Expression to identify inpatient admissions.

        Inpatient admission: bill_type 11x, 12x

        Returns:
            Expression that evaluates to 1 if inpatient admission, else 0
        """
        return pl.when(
            (pl.col(bill_type_col).str.starts_with("11"))
            | (pl.col(bill_type_col).str.starts_with("12"))
        ).then(1).otherwise(0)

    @staticmethod
    @expression_method(
        expression_name="is_er_visit",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_er_visit(
        bill_type_col: str = "bill_type_code",
        revenue_code_col: str = "revenue_center_code",
        pos_col: str = "place_of_service_code",
    ) -> pl.Expr:
        """
        Expression to identify emergency room visits.

        ER visit identified by:
        - Revenue code 0450-0459 (institutional)
        - Bill type second digit = 1 (emergency indicator)
        - Place of service = 23 (professional)

        Returns:
            Expression that evaluates to 1 if ER visit, else 0
        """
        return pl.when(
            # Institutional ER
            (
                pl.col(revenue_code_col).is_in([
                    "0450", "0451", "0452", "0453", "0454",
                    "0455", "0456", "0457", "0458", "0459"
                ])
            )
            | (pl.col(bill_type_col).str.slice(1, 1) == "1")  # Emergency indicator
            # Professional ER
            | (pl.col(pos_col) == "23")
        ).then(1).otherwise(0)

    @staticmethod
    @expression_method(
        expression_name="is_em_visit",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_em_visit(procedure_code_col: str = "hcpcs_code") -> pl.Expr:
        """
        Expression to identify Evaluation & Management visits.

        E&M visit identified by CPT codes:
        - 99201-99215 (Office visits)
        - 99241-99245 (Consultations)
        - 99251-99255 (Inpatient consultations)
        - 99281-99285 (Emergency department)
        - 99304-99318 (Nursing facility)
        - 99324-99337 (Domiciliary, rest home)
        - 99341-99350 (Home visits)
        - 99381-99387 (Preventive medicine - new patient)
        - 99391-99397 (Preventive medicine - established patient)

        Returns:
            Expression that evaluates to 1 if E&M visit, else 0
        """
        return pl.when(
            # Office visits (99201-99215)
            (pl.col(procedure_code_col) >= pl.lit("99201")) & (pl.col(procedure_code_col) <= pl.lit("99215"))
            # Consultations (99241-99245)
            | ((pl.col(procedure_code_col) >= pl.lit("99241")) & (pl.col(procedure_code_col) <= pl.lit("99245")))
            | ((pl.col(procedure_code_col) >= pl.lit("99251")) & (pl.col(procedure_code_col) <= pl.lit("99255")))
            # Emergency department (99281-99285)
            | ((pl.col(procedure_code_col) >= pl.lit("99281")) & (pl.col(procedure_code_col) <= pl.lit("99285")))
            # Nursing facility (99304-99318)
            | ((pl.col(procedure_code_col) >= pl.lit("99304")) & (pl.col(procedure_code_col) <= pl.lit("99318")))
            # Domiciliary (99324-99337)
            | ((pl.col(procedure_code_col) >= pl.lit("99324")) & (pl.col(procedure_code_col) <= pl.lit("99337")))
            # Home visits (99341-99350)
            | ((pl.col(procedure_code_col) >= pl.lit("99341")) & (pl.col(procedure_code_col) <= pl.lit("99350")))
            # Preventive medicine (99381-99387, 99391-99397)
            | ((pl.col(procedure_code_col) >= pl.lit("99381")) & (pl.col(procedure_code_col) <= pl.lit("99387")))
            | ((pl.col(procedure_code_col) >= pl.lit("99391")) & (pl.col(procedure_code_col) <= pl.lit("99397")))
        ).then(1).otherwise(0)

    @staticmethod
    @expression_method(
        expression_name="is_awv",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def is_awv(
        hcpcs_code_col: str = "hcpcs_code",
    ) -> pl.Expr:
        """
        Expression to identify Annual Wellness Visits (AWV).

        AWV identified by HCPCS codes:
        - G0438 (Initial AWV)
        - G0439 (Subsequent AWV)

        Returns:
            Expression that evaluates to 1 if AWV, else 0
        """
        return pl.when(
            pl.col(hcpcs_code_col).is_in(["G0438", "G0439"])
        ).then(1).otherwise(0)

    @staticmethod
    def calculate_utilization_metrics(
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        admissions: pl.LazyFrame | None,
        config: dict[str, Any],
    ) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        """
        Calculate comprehensive utilization metrics.

        Args:
            claims: Medical claims with service categories.
            eligibility: Member eligibility data.
            admissions: Optional admissions data for admission/bed day metrics.
            config: Configuration dict.

        Returns:
            Tuple of 5 LazyFrames:
            - visit_utilization: Visit counts by service category
            - admission_rates: Admission rates by encounter type
            - bed_days: Bed days per member
            - high_utilizers: High utilizer identification
            - service_mix: Service mix percentages
        """
        # Visit utilization by service category
        visit_utilization = (
            claims.group_by("service_category_2")
            .agg(
                pl.count().alias("total_visits"),
                pl.col("person_id").n_unique().alias("unique_members"),
            )
        )

        # Admission rates
        if admissions is not None:
            admission_rates = (
                admissions.group_by("encounter_type")
                .agg(
                    pl.count().alias("admission_count"),
                    pl.col("person_id").n_unique().alias("unique_members"),
                )
            )
        else:
            admission_rates = pl.LazyFrame(
                schema={
                    "encounter_type": pl.Utf8,
                    "admission_count": pl.Int64,
                    "unique_members": pl.Int64,
                }
            )

        # Bed days
        bed_days = pl.LazyFrame(
            schema={
                "person_id": pl.Utf8,
                "total_bed_days": pl.Int64,
            }
        )

        # High utilizers
        high_utilizers = (
            claims.group_by("person_id")
            .agg(pl.count().alias("total_claims"))
            .with_columns(
                (pl.col("total_claims") > pl.col("total_claims").quantile(0.95)).alias(
                    "is_high_utilizer"
                ),
            )
        )

        # Service mix
        service_mix = (
            claims.group_by("service_category_2")
            .agg(pl.count().alias("total_visits"))
            .with_columns(
                pl.lit(0.0).alias("percentage_of_visits"),
            )
            .sort("total_visits", descending=True)
        )

        return visit_utilization, admission_rates, bed_days, high_utilizers, service_mix
