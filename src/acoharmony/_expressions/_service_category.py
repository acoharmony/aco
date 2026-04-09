# © 2025 HarmonyCares
# All rights reserved.

"""
Service Category classification expression builder.

 expressions for categorizing healthcare claims into
service categories based on claim type, bill type, place of service, revenue
codes, and procedure codes.

Service categories enable:
- Financial analysis and PMPM calculations
- Utilization tracking
- Cost driver identification
- Service mix analysis

The logic follows Tuva Health's service category taxonomy with 50+ granular
categories grouped into high-level categories:
- Inpatient (acute, SNF, hospice, etc.)
- Outpatient (ambulatory surgery, ED, observation, etc.)
- Office-based (primary care, specialty, etc.)
- Ancillary (lab, radiology, DME, etc.)
- Pharmacy

"""

from typing import Any

import polars as pl

from acoharmony._decor8 import expression_method

from ._registry import register_expression


@register_expression(
    "service_category",
    schemas=["gold"],
    dataset_types=["claims"],
    description="Service category classification for claims",
)
class ServiceCategoryExpression:
    """
    Generate expressions for classifying claims into service categories.

        This expression builder creates Polars expressions that categorize medical
        and pharmacy claims based on:
        1. Claim type (institutional, professional, pharmacy)
        2. Bill type code (for institutional claims)
        3. Revenue codes
        4. Place of service codes
        5. Procedure codes

        The classification follows a hierarchical approach:
        - High-level category (inpatient, outpatient, office-based, ancillary, pharmacy)
        - Service category (specific type like "acute inpatient" or "emergency department")
        - Service subcategory (further detail when applicable)

        Configuration Structure:
            ```yaml
            service_category:
              # Column names from claims data
              claim_type_column: claim_type
              bill_type_column: bill_type_code
              revenue_code_column: revenue_code
              place_of_service_column: place_of_service_code
              procedure_code_column: procedure_code_1
            ```

        Output Structure:
            The expression generates these columns:
            - service_category_1: High-level category
            - service_category_2: Detailed service type
            - service_category_3: Service subcategory (if applicable)
            - service_category_rank: Ranking for sorting (lower = higher priority)

        Service Categories by High-Level Group:
            **Inpatient**:
            - Acute inpatient
            - Skilled nursing facility
            - Inpatient psychiatric
            - Inpatient rehab
            - Hospice

            **Outpatient**:
            - Emergency department
            - Urgent care
            - Ambulatory surgery
            - Observation
            - Outpatient hospital
            - Dialysis

            **Office-Based**:
            - Primary care
            - Specialist
            - Mental health
            - Physical therapy

            **Ancillary**:
            - Lab
            - Radiology/imaging
            - DME (durable medical equipment)
            - Ambulance
            - Home health

            **Pharmacy**:
            - Pharmacy
    """

    @staticmethod
    @expression_method(
        expression_name="service_category",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def build(config: dict[str, Any]) -> pl.Expr:
        """
        Build service category classification expression.

                Args:
                    config: Configuration dict with column names

                Returns:
                    Polars expression that adds service category columns
        """
        # Extract config
        claim_type_col = config.get("claim_type_column", "claim_type")
        bill_type_col = config.get("bill_type_column", "bill_type_code")
        revenue_code_col = config.get("revenue_code_column", "revenue_code")
        pos_col = config.get("place_of_service_column", "place_of_service_code")
        config.get("procedure_code_column", "procedure_code_1")

        # Build the classification expression using nested when/then/otherwise
        service_category_2 = (
            # Pharmacy claims
            pl.when(pl.col(claim_type_col) == "pharmacy")
            .then(pl.lit("pharmacy"))
            # Institutional claims - inpatient
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(bill_type_col).str.starts_with("11"))  # Inpatient hospital
            )
            .then(pl.lit("acute inpatient"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(bill_type_col).str.starts_with("21"))  # SNF
            )
            .then(pl.lit("skilled nursing facility"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(bill_type_col).str.starts_with("61"))  # Hospice
            )
            .then(pl.lit("hospice"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(bill_type_col).str.starts_with("41"))  # Religious non-medical
            )
            .then(pl.lit("hospice"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(bill_type_col).str.starts_with("81"))  # Inpatient psych
            )
            .then(pl.lit("inpatient psychiatric"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(bill_type_col).str.starts_with("82"))  # Inpatient rehab
            )
            .then(pl.lit("inpatient rehabilitation"))
            # Institutional claims - outpatient
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (
                    pl.col(revenue_code_col).is_in(
                        ["0450", "0451", "0452", "0459"]
                    )  # Emergency dept
                    | (pl.col(bill_type_col).str.slice(2, 1) == "1")  # Emergency indicator
                )
            )
            .then(pl.lit("emergency department"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(revenue_code_col).is_in(["0456", "0457", "0458"]))  # Urgent care
            )
            .then(pl.lit("urgent care"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(revenue_code_col).str.starts_with("036"))  # Observation
            )
            .then(pl.lit("observation"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(revenue_code_col).str.starts_with("049"))  # Ambulatory surgery
            )
            .then(pl.lit("ambulatory surgery"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(revenue_code_col).str.starts_with("082"))  # Dialysis
            )
            .then(pl.lit("dialysis"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(revenue_code_col).str.starts_with("057"))  # Home health
            )
            .then(pl.lit("home health"))
            .when(
                (pl.col(claim_type_col) == "institutional")
                & (pl.col(bill_type_col).str.starts_with("13"))  # Outpatient hospital
            )
            .then(pl.lit("outpatient hospital or clinic"))
            # Professional claims - categorize by place of service
            .when((pl.col(claim_type_col) == "professional") & (pl.col(pos_col) == "20"))
            .then(pl.lit("urgent care"))
            .when((pl.col(claim_type_col) == "professional") & (pl.col(pos_col) == "23"))
            .then(pl.lit("emergency department"))
            .when(
                (pl.col(claim_type_col) == "professional")
                & (pl.col(pos_col).is_in(["24", "51", "56"]))
            )
            .then(pl.lit("ambulatory surgery"))
            .when((pl.col(claim_type_col) == "professional") & (pl.col(pos_col) == "11"))
            .then(pl.lit("office-based"))
            .when(
                (pl.col(claim_type_col) == "professional")
                & (pl.col(pos_col).is_in(["12", "13", "14", "33"]))
            )
            .then(pl.lit("home health"))
            .when((pl.col(claim_type_col) == "professional") & (pl.col(pos_col) == "81"))
            .then(pl.lit("lab"))
            .when((pl.col(claim_type_col) == "professional") & (pl.col(pos_col) == "41"))
            .then(pl.lit("ambulance"))
            .when((pl.col(claim_type_col) == "professional") & (pl.col(pos_col) == "22"))
            .then(pl.lit("outpatient hospital or clinic"))
            .when(
                (pl.col(claim_type_col) == "professional")
                & (pl.col(pos_col).is_in(["65", "71", "72"]))
            )
            .then(pl.lit("dialysis"))
            # Default
            .otherwise(pl.lit("other"))
        )

        return service_category_2.alias("service_category_2")

    @staticmethod
    @expression_method(
        expression_name="high_level_category",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
        threshold=1.0,
    )
    def build_high_level_category(service_category_2: pl.Expr) -> pl.Expr:
        """
        Build high-level service category grouping.

                Args:
                    service_category_2: The detailed service category expression

                Returns:
                    Expression for high-level category (service_category_1)
        """
        return (
            pl.when(
                service_category_2.is_in(
                    [
                        "acute inpatient",
                        "skilled nursing facility",
                        "inpatient psychiatric",
                        "inpatient rehabilitation",
                    ]
                )
            )
            .then(pl.lit("inpatient"))
            .when(service_category_2.is_in(["hospice"]))
            .then(pl.lit("inpatient"))
            .when(
                service_category_2.is_in(
                    [
                        "emergency department",
                        "urgent care",
                        "ambulatory surgery",
                        "observation",
                        "outpatient hospital or clinic",
                        "dialysis",
                        "outpatient rehabilitation",
                        "outpatient pt/ot/st",
                        "outpatient substance use",
                    ]
                )
            )
            .then(pl.lit("outpatient"))
            .when(service_category_2.is_in(["office-based", "telehealth"]))
            .then(pl.lit("office-based"))
            .when(
                service_category_2.is_in(
                    ["lab", "radiology", "durable medical equipment", "ambulance", "home health"]
                )
            )
            .then(pl.lit("ancillary"))
            .when(service_category_2.is_in(["pharmacy"]))
            .then(pl.lit("outpatient"))
            .otherwise(pl.lit("other"))
        ).alias("service_category_1")

    @staticmethod
    def categorize_claims(
        claims: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Add service category columns to claims data.

                Args:
                    claims: LazyFrame containing medical or pharmacy claims
                    config: Configuration dict with column names

                Returns:
                    LazyFrame with service category columns added
        """
        # Build service category classification
        service_cat_2 = ServiceCategoryExpression.build(config)

        # Add service_category_2 first
        result = claims.with_columns([service_cat_2])

        # Then add service_category_1 based on service_category_2
        result = result.with_columns(
            [ServiceCategoryExpression.build_high_level_category(pl.col("service_category_2"))]
        )

        return result
