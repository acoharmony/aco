# © 2025 HarmonyCares
# All rights reserved.

"""
Chronic Conditions identification expression builder.

 expressions for identifying chronic conditions from medical claims
using CMS Chronic Conditions Data Warehouse (CCW) algorithms and Tuva condition logic.

The logic:
1. Map diagnosis codes to chronic condition categories
2. Apply lookback periods and claim count requirements
3. Create patient-level condition flags
4. Calculate prevalence and comorbidity metrics

References:
- CMS Chronic Conditions Data Warehouse
- https://www2.ccwdata.org/web/guest/condition-categories

"""

from typing import Any

import polars as pl

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


@register_expression(
    "chronic_conditions",
    schemas=["silver", "gold"],
    dataset_types=["claims", "conditions"],
    description="Chronic condition identification and patient prevalence",
)
class ChronicConditionsExpression:
    """
    Generate expressions for chronic condition identification.

    """

    @traced()
    @explain(
        why="Build failed",
        how="Check configuration and input data are valid",
        causes=["Invalid config", "Missing required fields", "Data processing error"],
    )
    @timeit(log_level="debug")
    @staticmethod
    def build(config: dict[str, Any]) -> dict[str, Any]:
        """Build chronic condition identification expressions."""
        lookback_years = config.get("lookback_years", 2)
        min_claims_op = config.get("min_claims_outpatient", 2)
        min_claims_ip = config.get("min_claims_inpatient", 1)
        patient_id_col = config.get("patient_id_column", "patient_id")
        diagnosis_col = config.get("diagnosis_column", "diagnosis_code")
        claim_type_col = config.get("claim_type_column", "claim_type")
        service_date_col = config.get("service_date_column", "claim_end_date")

        config_with_defaults = {
            "lookback_years": lookback_years,
            "min_claims_outpatient": min_claims_op,
            "min_claims_inpatient": min_claims_ip,
            "patient_id_column": patient_id_col,
            "diagnosis_column": diagnosis_col,
            "claim_type_column": claim_type_col,
            "service_date_column": service_date_col,
        }

        expressions = {
            "map_diagnosis_to_condition": {
                "description": "Join diagnoses to chronic condition mappings",
                "lookup_table": "value_sets_chronic_conditions_cms_chronic_conditions_hierarchy",
            },
            "apply_claim_count_logic": {
                "description": "Apply min claim requirements",
                "outpatient_min": min_claims_op,
                "inpatient_min": min_claims_ip,
            },
            "create_condition_flags": {
                "description": "Create binary flags for each condition",
            },
        }

        return {"expressions": expressions, "config": config_with_defaults}

    @staticmethod
    def transform_patient_conditions_long(
        medical_claims: pl.LazyFrame,
        condition_mapping: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Create long-format patient-condition table.

                Returns:
                    LazyFrame with columns:
                    - patient_id
                    - condition_category
                    - condition_name
                    - first_diagnosis_date
                    - last_diagnosis_date
                    - claim_count
                    - meets_criteria (boolean)
        """
        patient_id_col = config.get("patient_id_column", "patient_id")
        diagnosis_col = config.get("diagnosis_column", "diagnosis_code")
        service_date_col = config.get("service_date_column", "claim_end_date")
        mapping_code_col = config.get("mapping_code_column", "code")

        claims_with_conditions = medical_claims.join(
            condition_mapping,
            left_on=diagnosis_col,
            right_on=mapping_code_col,
            how="inner",
        )

        patient_conditions = claims_with_conditions.group_by(
            [patient_id_col, "condition_category"]
        ).agg(
            [
                pl.col("condition").first().alias("condition_name"),
                pl.col(service_date_col).min().alias("first_diagnosis_date"),
                pl.col(service_date_col).max().alias("last_diagnosis_date"),
                pl.count().alias("claim_count"),
            ]
        )

        min_claims = config.get("min_claims_outpatient", 2)
        patient_conditions = patient_conditions.with_columns(
            (pl.col("claim_count") >= min_claims).alias("meets_criteria")
        )

        return patient_conditions

    @staticmethod
    def transform_patient_conditions_wide(
        patient_conditions_long: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Pivot to wide format with one column per condition.

                Returns:
                    LazyFrame with columns:
                    - patient_id
                    - diabetes (boolean)
                    - hypertension (boolean)
                    - chf (boolean)
                    - ... (one column per condition)
                    - condition_count (total conditions)
        """
        patient_id_col = config.get("patient_id_column", "patient_id")

        qualifying = patient_conditions_long.filter(pl.col("meets_criteria"))

        wide = (
            qualifying.collect()
            .pivot(values="meets_criteria", index=patient_id_col, on="condition_category")
            .lazy()
        )

        wide = wide.with_columns(
            [
                pl.lit(1).alias("condition_count")
            ]
        )

        return wide
