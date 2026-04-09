# © 2025 HarmonyCares
# All rights reserved.

"""
Service Category Transform for healthcare analytics.

 a transform that classifies medical and pharmacy claims into
standardized service categories for financial and utilization analysis.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import polars as pl

from .._decor8 import explain, profile_memory, timeit, traced, validate_args
from .._expressions._service_category import ServiceCategoryExpression
from .._log import LogWriter
from ._base import HealthcareTransformBase

logger = LogWriter("transforms.service_category")


class ServiceCategoryTransform(HealthcareTransformBase):
    """
    Transform for service category classification.

        Categorizes medical and pharmacy claims into service categories based on:
        - Claim type (institutional, professional, pharmacy)
        - Bill type codes
        - Revenue codes
        - Place of service codes
        - Procedure codes

        Inputs (gold):
        - medical_claim.parquet
        - pharmacy_claim.parquet (optional)

        Outputs (gold):
        - service_category.parquet

        Service categories include:
        - Acute inpatient
        - Skilled nursing facility
        - Emergency department
        - Ambulatory surgery
        - Office-based services
        - Pharmacy
        - Dialysis
        - Home health
        - And 40+ more granular categories
    """

    transform_name: ClassVar[str] = "service_category"
    required_inputs: ClassVar[list[str]] = ["medical_claim.parquet"]
    required_seeds: ClassVar[list[str]] = [
        "value_sets_service_categories_service_categories.parquet"
    ]
    output_names: ClassVar[list[str]] = ["service_category"]

    @traced()
    @explain(
        why="Service category transform failed",
        how="Check medical_claim data exists and service category logic is valid",
        causes=["Missing input data", "Service category calculation error", "Invalid config"],
    )
    @timeit(log_level="info", threshold=15.0)
    @profile_memory(log_result=True)
    @validate_args(config=(dict, type(None)))
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute service category classification.

                Args:
                    config: Optional configuration overrides

                Returns:
                    Dictionary mapping output names to file paths
        """
        if config is None:
            config = {}

        logger.info("Starting service category transform", transform=self.name)

        # Load inputs using helper methods
        logger.info("Loading medical claims...")
        medical_claims = self.load_parquet("medical_claim.parquet")

        # Load pharmacy claims if available (optional)
        logger.info("Loading pharmacy claims...")
        pharmacy_claims = self.load_optional_parquet(
            "pharmacy_claim.parquet",
            default_schema={
                "claim_id": pl.Utf8,
                "person_id": pl.Utf8,
                "claim_type": pl.Utf8,
                "dispensing_date": pl.Date,
                "paid_amount": pl.Float64,
            },
        )

        # Add claim_type for pharmacy if exists
        if pharmacy_claims.collect().height > 0:
            pharmacy_claims = pharmacy_claims.with_columns([pl.lit("pharmacy").alias("claim_type")])

        # Configure expression
        expr_config = {
            "claim_type_column": "claim_type",
            "bill_type_column": "bill_type_code",
            "revenue_code_column": "revenue_center_code",  # medical_claim uses revenue_center_code, not revenue_code
            "place_of_service_column": "place_of_service_code",
            "procedure_code_column": "hcpcs_code",  # medical_claim uses hcpcs_code, not procedure_code_1
            **config,  # Allow overrides
        }

        logger.info("Categorizing medical claims...")
        medical_categorized = ServiceCategoryExpression.categorize_claims(
            medical_claims, expr_config
        )

        medical_output = medical_categorized.select(
            [
                pl.col("claim_id"),
                pl.col("person_id"),
                pl.col("claim_type"),
                pl.col("claim_start_date"),
                pl.col("claim_end_date"),
                pl.col("paid_amount").alias("paid"),
                pl.col("service_category_1"),
                pl.col("service_category_2"),
            ]
        )

        if pharmacy_claims.collect().height > 0:
            logger.info("Categorizing pharmacy claims...")
            pharmacy_output = pharmacy_claims.select(
                [
                    pl.col("claim_id"),
                    pl.col("person_id"),
                    pl.col("claim_type"),
                    pl.col("dispensing_date").alias("claim_start_date"),
                    pl.col("dispensing_date").alias("claim_end_date"),
                    pl.col("paid_amount").alias("paid"),
                    pl.lit("outpatient").alias("service_category_1"),
                    pl.lit("pharmacy").alias("service_category_2"),
                ]
            )

            logger.info("Combining medical and pharmacy claims...")
            service_category_all = pl.concat([medical_output, pharmacy_output])
        else:
            service_category_all = medical_output

        logger.info("Writing service category output...")
        results = self.write_outputs({"service_category": service_category_all})

        logger.info("Service category transform complete", transform=self.name)
        logger.info(
            f"Total claims categorized: {service_category_all.select(pl.len()).collect().item()}"
        )

        return results
