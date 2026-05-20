# © 2025 HarmonyCares
# All rights reserved.

"""
ED Classification Transform for healthcare analytics.

 a transform that classifies emergency department visits
using the NYU ED Algorithm (Johnston et al.).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

import polars as pl

from .._decor8 import explain, profile_memory, timeit, traced, validate_args
from .._expressions._ed_classification import EdClassificationExpression
from .._log import LogWriter
from ..medallion import MedallionLayer
from ._base import HealthcareTransformBase

logger = LogWriter("transforms.ed_classification")


class EdClassificationTransform(HealthcareTransformBase):
    """
    Transform for ED visit classification using NYU ED Algorithm.

        Classifies emergency department visits into categories:
        - Non-Emergent
        - Emergent, Primary Care Treatable
        - Emergent, ED Care Needed, Preventable
        - Emergent, ED Care Needed, Not Preventable
        - Injury
        - Mental Health Related
        - Alcohol Related
        - Drug Related

        Uses the Johnston et al. algorithm which assigns probability scores
        based on primary diagnosis codes.

        Inputs (gold):
        - medical_claim.parquet (filtered to ED visits)

        Seeds (silver):
        - value_sets_ed_classification_johnston_icd10.parquet
        - value_sets_ed_classification_categories.parquet
        - value_sets_ed_classification_icd_10_cm_to_ccs.parquet

        Outputs (gold):
        - ed_classification.parquet

    """

    transform_name: ClassVar[str] = "ed_classification"
    required_inputs: ClassVar[list[str]] = ["medical_claim.parquet"]
    required_seeds: ClassVar[list[str]] = [
        "value_sets_ed_classification_johnston_icd10.parquet",
        "value_sets_ed_classification_categories.parquet",
    ]
    output_names: ClassVar[list[str]] = ["ed_classification"]

    @traced()
    @explain(
        why="ED classification transform failed",
        how="Check medical_claim data exists and ED classification logic is valid",
        causes=[
            "Missing input data",
            "Missing seed data",
            "ED classification calculation error",
            "Invalid config",
        ],
    )
    @timeit(log_level="info", threshold=15.0)
    @profile_memory(log_result=True)
    @validate_args(config=(dict, type(None)))
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute ED visit classification.

                Args:
                    config: Optional configuration overrides

                Returns:
                    Dictionary mapping output names to file paths
        """
        if config is None:
            config = {}

        logger.info("Starting ED classification transform", transform=self.name)

        # Load inputs using helper methods
        logger.info("Loading medical claims...")
        medical_claims = self.load_parquet("medical_claim.parquet")

        logger.info("Filtering to ED visits...")
        ed_visits = medical_claims.filter(
            (
                pl.col("revenue_code").is_in(
                    [
                        "0450",
                        "0451",
                        "0452",
                        "0453",
                        "0456",
                        "0457",
                        "0458",
                        "0459",
                    ]
                )
            )
            | (pl.col("place_of_service_code") == "23")
        )

        logger.info("Loading Johnston algorithm mapping...")
        johnston_mapping = self.load_parquet(
            "value_sets_ed_classification_johnston_icd10.parquet", MedallionLayer.SILVER
        )

        logger.info("Loading ED classification categories...")
        self.load_parquet("value_sets_ed_classification_categories.parquet", MedallionLayer.SILVER)

        expr_config = {
            "diagnosis_column": "diagnosis_code_1",  # Primary diagnosis
            "claim_id_column": "claim_id",
            **config,  # Allow overrides
        }

        logger.info("Classifying ED visits...")
        ed_classified = EdClassificationExpression.classify_ed_visits(
            ed_visits=ed_visits, johnston_mapping=johnston_mapping, config=expr_config
        )

        logger.info("Calculating preventable ED flags...")
        ed_classified = EdClassificationExpression.calculate_preventable_ed_flag(ed_classified)

        ed_output = ed_classified.select(
            [
                pl.col("claim_id"),
                pl.col("person_id"),
                pl.col("claim_start_date"),
                pl.col("claim_end_date"),
                pl.col("diagnosis_code_1"),
                pl.col("ed_classification_primary"),
                pl.col("preventable_ed_flag"),
                pl.col("non_emergent"),
                pl.col("emergent_primary_care"),
                pl.col("emergent_ed_preventable"),
                pl.col("emergent_ed_not_preventable"),
                pl.col("injury"),
                pl.col("mental_health"),
                pl.col("alcohol"),
                pl.col("drug"),
                pl.col("unclassified"),
            ]
        )

        logger.info("Writing ED classification output...")
        results = self.write_outputs({"ed_classification": ed_output})

        ed_count = ed_output.select(pl.len()).collect().item()
        logger.info(
            f"ED classification transform complete - {ed_count} ED visits classified",
            transform=self.name,
        )

        return results
