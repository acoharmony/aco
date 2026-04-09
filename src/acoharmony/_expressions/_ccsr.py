# © 2025 HarmonyCares
# All rights reserved.

"""
CCSR (Clinical Classifications Software Refined) expression builder.

 expressions for mapping ICD-10-CM diagnosis codes and
ICD-10-PCS procedure codes to CCSR categories. CCSR is an AHRQ tool that
groups diagnoses and procedures into clinically meaningful categories.

CCSR enables:
- Clinical grouping and analysis
- Service line identification
- Quality measurement
- Risk stratification
- Utilization analysis

The CCSR v2023.1 taxonomy includes:
- 530+ diagnosis categories across 21 body systems
- 240+ procedure categories across clinical domains
- Support for multi-category mapping (one code → multiple categories)

References:
- AHRQ Clinical Classifications Software Refined (CCSR)
- https://www.hcup-us.ahrq.gov/toolssoftware/ccsr/ccs_refined.jsp

"""

from typing import Any

import polars as pl

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


@register_expression(
    "ccsr",
    schemas=["gold"],
    dataset_types=["claims"],
    description="CCSR clinical classification for diagnoses and procedures",
)
class CcsrExpression:
    """
    Generate expressions for mapping ICD-10 codes to CCSR categories.

        This expression builder creates Polars expressions that map:
        1. ICD-10-CM diagnosis codes to CCSR diagnosis categories
        2. ICD-10-PCS procedure codes to CCSR procedure categories

        The CCSR provides clinically meaningful groupings that enable:
        - Analysis by body system (circulatory, respiratory, etc.)
        - Service line categorization
        - Clinical domain grouping
        - Multi-category mapping (one code can map to multiple categories)

        Output Structure - Diagnosis:
            The expression generates these columns:
            - ccsr_default_category: Primary CCSR category (IP or OP)
            - ccsr_default_description: Description of primary category
            - ccsr_category_1: First CCSR category assignment
            - ccsr_category_1_description: Description
            - ccsr_category_2 through ccsr_category_6: Additional categories
            - ccsr_body_system: Body system (CIR, RSP, DIG, etc.)

        Output Structure - Procedure:
            The expression generates these columns:
            - prccsr: Procedure CCSR category code
            - prccsr_description: Category description
            - clinical_domain: Clinical domain (e.g., "Central Nervous System")
    """

    @staticmethod
    @traced()
    @explain(
        why="CCSR diagnosis mapping failed",
        how="Check diagnosis column exists and CCSR mapping is available",
        causes=["Invalid config", "Missing diagnosis column", "Missing CCSR mapping"],
    )
    @timeit(log_level="debug")
    def map_diagnoses_to_ccsr(
        claims: pl.LazyFrame,
        dx_ccsr_mapping: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Map diagnosis codes to CCSR categories.

                Args:
                    claims: LazyFrame containing claims with diagnosis codes
                    dx_ccsr_mapping: LazyFrame with ICD-10-CM to CCSR mapping
                    config: Configuration dict with column names

                Returns:
                    LazyFrame with CCSR diagnosis columns added
        """
        diag_col = config.get("diagnosis_column", "diagnosis_code_1")
        use_inpatient = config.get("use_inpatient_default", True)

        default_col = "default_ccsr_category_ip" if use_inpatient else "default_ccsr_category_op"
        default_desc_col = (
            "default_ccsr_category_description_ip"
            if use_inpatient
            else "default_ccsr_category_description_op"
        )

        mapped = claims.join(
            dx_ccsr_mapping.select(
                [
                    pl.col("icd_10_cm_code"),
                    pl.col(default_col).alias("ccsr_default_category"),
                    pl.col(default_desc_col).alias("ccsr_default_description"),
                    pl.col("ccsr_category_1"),
                    pl.col("ccsr_category_1_description"),
                    pl.col("ccsr_category_2"),
                    pl.col("ccsr_category_2_description"),
                    pl.col("ccsr_category_3"),
                    pl.col("ccsr_category_3_description"),
                    pl.col("ccsr_category_4"),
                    pl.col("ccsr_category_4_description"),
                    pl.col("ccsr_category_5"),
                    pl.col("ccsr_category_5_description"),
                    pl.col("ccsr_category_6"),
                    pl.col("ccsr_category_6_description"),
                ]
            ),
            left_on=diag_col,
            right_on="icd_10_cm_code",
            how="left",
        )

        mapped = mapped.with_columns(
            [
                pl.when(pl.col("ccsr_default_category").is_not_null())
                .then(pl.col("ccsr_default_category").str.slice(0, 3))
                .otherwise(None)
                .alias("ccsr_body_system")
            ]
        )

        return mapped

    @staticmethod
    @traced()
    @explain(
        why="CCSR procedure mapping failed",
        how="Check procedure column exists and CCSR mapping is available",
        causes=["Invalid config", "Missing procedure column", "Missing CCSR mapping"],
    )
    @timeit(log_level="debug")
    def map_procedures_to_ccsr(
        claims: pl.LazyFrame,
        pr_ccsr_mapping: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Map procedure codes to CCSR categories.

                Args:
                    claims: LazyFrame containing claims with procedure codes
                    pr_ccsr_mapping: LazyFrame with ICD-10-PCS to CCSR mapping
                    config: Configuration dict with column names

                Returns:
                    LazyFrame with CCSR procedure columns added
        """
        proc_col = config.get("procedure_column", "procedure_code_1")

        mapped = claims.join(
            pr_ccsr_mapping.select(
                [
                    pl.col("icd_10_pcs"),
                    pl.col("prccsr"),
                    pl.col("prccsr_description"),
                    pl.col("clinical_domain"),
                ]
            ),
            left_on=proc_col,
            right_on="icd_10_pcs",
            how="left",
        )

        return mapped
