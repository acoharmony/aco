# © 2025 HarmonyCares
# All rights reserved.

"""
CCSR Transform for healthcare analytics.

 a transform that maps diagnosis and procedure codes to
CCSR (Clinical Classifications Software Refined) categories.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import polars as pl

from .._decor8 import explain, profile_memory, timeit, traced, validate_args
from .._expressions._ccsr import CcsrExpression
from .._log import LogWriter
from ..medallion import MedallionLayer
from ._base import HealthcareTransformBase

logger = LogWriter("transforms.ccsr")


class CcsrTransform(HealthcareTransformBase):
    """
    Transform for CCSR clinical classification.

        Maps ICD-10-CM diagnosis codes and ICD-10-PCS procedure codes to CCSR
        categories developed by AHRQ. CCSR provides clinically meaningful
        groupings of diagnoses and procedures.

        Diagnosis CCSR Features:
        - 530+ categories across 21 body systems
        - Multi-category mapping support
        - Separate inpatient/outpatient defaults
        - Body system grouping

        Procedure CCSR Features:
        - 240+ categories
        - Clinical domain grouping
        - Detailed procedure descriptions

        Inputs (gold):
        - medical_claim.parquet

        Seeds (silver):
        - value_sets_ccsr_dxccsr_v2023_1_cleaned_map.parquet (diagnosis)
        - value_sets_ccsr_dxccsr_v2023_1_body_systems.parquet (body systems)
        - value_sets_ccsr_prccsr_v2023_1_cleaned_map.parquet (procedure)

        Outputs (gold):
        - diagnosis_ccsr.parquet
        - procedure_ccsr.parquet
    """

    # Metadata
    transform_name: ClassVar[str] = "ccsr"
    required_inputs: ClassVar[list[str]] = ["medical_claim.parquet"]
    required_seeds: ClassVar[list[str]] = [
        "value_sets_ccsr_dxccsr_v2023_1_cleaned_map.parquet",
        "value_sets_ccsr_dxccsr_v2023_1_body_systems.parquet",
        "value_sets_ccsr_prccsr_v2023_1_cleaned_map.parquet",
    ]
    output_names: ClassVar[list[str]] = ["diagnosis_ccsr", "procedure_ccsr"]

    @traced()
    @explain(
        why="CCSR transform failed",
        how="Check medical_claim data exists and CCSR mappings are available",
        causes=["Missing input data", "Missing seed data", "CCSR mapping error", "Invalid config"],
    )
    @timeit(log_level="info", threshold=20.0)
    @profile_memory(log_result=True)
    @validate_args(config=(dict, type(None)))
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute CCSR classification.

                Args:
                    config: Optional configuration overrides

                Returns:
                    Dictionary mapping output names to file paths
        """
        if config is None:
            config = {}

        logger.info("Starting CCSR transform", transform=self.name)

        # Load inputs using helper methods
        logger.info("Loading medical claims...")
        medical_claims = self.load_parquet("medical_claim.parquet")

        # Load CCSR mappings
        logger.info("Loading CCSR mappings...")
        dx_ccsr_mapping = self.load_parquet(
            "value_sets_ccsr_dxccsr_v2023_1_cleaned_map.parquet", MedallionLayer.SILVER
        )
        self.load_parquet(
            "value_sets_ccsr_dxccsr_v2023_1_body_systems.parquet", MedallionLayer.SILVER
        )
        pr_ccsr_mapping = self.load_parquet(
            "value_sets_ccsr_prccsr_v2023_1_cleaned_map.parquet", MedallionLayer.SILVER
        )

        # Configure expression
        expr_config = {
            "diagnosis_column": "diagnosis_code_1",  # Primary diagnosis
            "procedure_column": "procedure_code_1",  # Primary procedure
            "use_inpatient_default": True,  # Use inpatient defaults
            **config,  # Allow overrides
        }

        # Map diagnoses to CCSR
        logger.info("Mapping diagnoses to CCSR categories...")
        diagnosis_ccsr = CcsrExpression.map_diagnoses_to_ccsr(
            claims=medical_claims, dx_ccsr_mapping=dx_ccsr_mapping, config=expr_config
        )

        # Select diagnosis output columns
        diagnosis_output = diagnosis_ccsr.select(
            [
                pl.col("claim_id"),
                pl.col("person_id"),
                pl.col("claim_start_date"),
                pl.col("claim_end_date"),
                pl.col("diagnosis_code_1"),
                pl.col("ccsr_default_category"),
                pl.col("ccsr_default_description"),
                pl.col("ccsr_body_system"),
                pl.col("ccsr_category_1"),
                pl.col("ccsr_category_1_description"),
                pl.col("ccsr_category_2"),
                pl.col("ccsr_category_2_description"),
                pl.col("ccsr_category_3"),
                pl.col("ccsr_category_3_description"),
            ]
        )

        # Map procedures to CCSR
        logger.info("Mapping procedures to CCSR categories...")
        procedure_ccsr = CcsrExpression.map_procedures_to_ccsr(
            claims=medical_claims, pr_ccsr_mapping=pr_ccsr_mapping, config=expr_config
        )

        # Select procedure output columns and filter to claims with procedures
        procedure_output = procedure_ccsr.filter(pl.col("procedure_code_1").is_not_null()).select(
            [
                pl.col("claim_id"),
                pl.col("person_id"),
                pl.col("claim_start_date"),
                pl.col("claim_end_date"),
                pl.col("procedure_code_1"),
                pl.col("prccsr"),
                pl.col("prccsr_description"),
                pl.col("clinical_domain"),
            ]
        )

        # Write outputs using helper method
        logger.info("Writing CCSR outputs...")
        results = self.write_outputs(
            {"diagnosis_ccsr": diagnosis_output, "procedure_ccsr": procedure_output}
        )

        # Log summary statistics
        dx_count = diagnosis_output.select(pl.len()).collect().item()
        pr_count = procedure_output.select(pl.len()).collect().item()
        logger.info(
            f"CCSR transform complete - {dx_count} diagnoses, {pr_count} procedures classified",
            transform=self.name,
        )

        return results
