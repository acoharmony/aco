# © 2025 HarmonyCares
# All rights reserved.

"""
Quality Measures expression builder.

 expressions for calculating healthcare quality measures similar
to HEDIS (Healthcare Effectiveness Data and Information Set). Quality measures assess
preventive care, chronic disease management, and adherence to evidence-based guidelines.

The logic:
1. Define measure numerator and denominator criteria
2. Identify eligible patient population (denominator)
3. Identify patients who met quality criteria (numerator)
4. Calculate rates and performance scores

References:
- NCQA HEDIS Measures
- CMS Star Ratings Quality Measures
- https://www.ncqa.org/hedis/

"""

from typing import Any

import polars as pl

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


@register_expression(
    "quality_measures",
    schemas=["silver", "gold"],
    dataset_types=["claims", "eligibility", "pharmacy"],
    description="Healthcare quality measure calculation (HEDIS-like)",
)
class QualityMeasuresExpression:
    """
    Generate expressions for quality measure calculation.

        Configuration Structure:
            ```yaml
            quality_measures:
              # Measurement period
              measurement_year: 2024

              # Measures to calculate
              measures:
                - diabetes_hba1c_control
                - breast_cancer_screening
                - colorectal_cancer_screening
                - controlling_high_blood_pressure
                - statin_therapy_diabetes

              # Column mappings
              patient_id_column: patient_id
              diagnosis_column: diagnosis_code
              procedure_column: procedure_code
              service_date_column: claim_end_date
            ```
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
        """Build quality measure expressions."""
        measurement_year = config.get("measurement_year", 2024)
        measures = config.get("measures", [])
        patient_id_col = config.get("patient_id_column", "patient_id")
        diagnosis_col = config.get("diagnosis_column", "diagnosis_code")
        procedure_col = config.get("procedure_column", "procedure_code")
        service_date_col = config.get("service_date_column", "claim_end_date")

        # Update config with defaults
        config_with_defaults = {
            "measurement_year": measurement_year,
            "measures": measures,
            "patient_id_column": patient_id_col,
            "diagnosis_column": diagnosis_col,
            "procedure_column": procedure_col,
            "service_date_column": service_date_col,
        }

        expressions = {
            "identify_denominator": {
                "description": "Identify eligible patients for each measure",
                # E.g., diabetes patients aged 18-75 for HbA1c measure
            },
            "identify_numerator": {
                "description": "Identify patients who met quality criteria",
                # E.g., had HbA1c test in measurement year
            },
            "calculate_rates": {
                "description": "Calculate numerator/denominator rates",
                "formula": pl.col("numerator_count") / pl.col("denominator_count"),
            },
        }

        return {"expressions": expressions, "config": config_with_defaults}

    @staticmethod
    def transform_measure_summary(
        medical_claims: pl.LazyFrame,
        pharmacy_claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Calculate quality measure summary across all measures.

                Returns:
                    LazyFrame with columns:
                    - measure_name
                    - measure_description
                    - denominator_count (eligible patients)
                    - numerator_count (patients meeting criteria)
                    - rate (%)
                    - measurement_year
        """
        measurement_year = config.get("measurement_year", 2024)
        measures = config.get("measures", [])

        # Placeholder: Build measure logic for each measure
        # Actual implementation would have specific logic per measure

        measure_results = []

        # Example: Diabetes HbA1c Control measure
        if "diabetes_hba1c_control" in measures:
            # Denominator: Patients with diabetes aged 18-75
            # Numerator: Patients with HbA1c < 8% or < 9% (depending on spec)
            # This requires value sets for:
            # - Diabetes diagnosis codes
            # - HbA1c procedure codes
            # - Lab result data (if available)

            # Simplified placeholder
            diabetes_measure = pl.DataFrame(
                {
                    "measure_name": ["diabetes_hba1c_control"],
                    "measure_description": ["HbA1c Control for Patients with Diabetes"],
                    "denominator_count": [0],
                    "numerator_count": [0],
                    "rate": [0.0],
                    "measurement_year": [measurement_year],
                }
            ).lazy()

            measure_results.append(diabetes_measure)

        # Combine all measure results
        if measure_results:
            summary = pl.concat(measure_results, how="vertical")
        else:
            # Empty result if no measures configured
            summary = pl.DataFrame(
                {
                    "measure_name": [],
                    "measure_description": [],
                    "denominator_count": [],
                    "numerator_count": [],
                    "rate": [],
                    "measurement_year": [],
                }
            ).lazy()

        return summary
