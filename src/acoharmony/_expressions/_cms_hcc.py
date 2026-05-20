# © 2025 HarmonyCares
# All rights reserved.

"""
CMS HCC (Hierarchical Condition Category) risk adjustment expression builder.

 expressions for calculating CMS-HCC risk scores based on
diagnosis codes from medical claims. HCC risk adjustment is used by CMS to
adjust capitated payments based on patient health status.

The logic:
1. Map ICD-10 diagnosis codes to HCC categories using CMS value sets
2. Apply hierarchies (higher HCCs supersede lower ones)
3. Calculate demographic coefficients (age, gender, Medicaid, etc.)
4. Sum HCC coefficients + demographic factors = risk score

References:
- CMS-HCC Model V24/V28
- https://www.cms.gov/medicare/health-plans/medicareadvtgspecratestats/risk-adjustors

"""

from typing import Any

import polars as pl

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


@register_expression(
    "cms_hcc",
    schemas=["silver", "gold"],
    dataset_types=["claims", "eligibility"],
    description="CMS Hierarchical Condition Category (HCC) risk adjustment",
)
class CmsHccExpression:
    """
    Generate expressions for CMS-HCC risk score calculation.

        This expression builder creates Polars expressions that:
        1. Map ICD-10 diagnosis codes to HCC categories
        2. Deduplicate HCCs at patient level
        3. Apply hierarchical logic (suppress subordinate HCCs)
        4. Calculate demographic risk factors
        5. Sum coefficients to produce final risk scores

        The HCC model adjusts payments based on expected healthcare costs,
        accounting for patient age, gender, Medicaid status, and chronic conditions.

        Configuration Structure:
            ```yaml
            cms_hcc:
              # Input configuration
              patient_id_column: patient_id
              diagnosis_column: diagnosis_code
              claim_through_date_column: claim_end_date

              # Reference data
              hcc_mapping_table: value_sets_cms_hcc_icd_10_cm_mappings
              hierarchy_table: value_sets_cms_hcc_hierarchy

              # Demographic columns (from eligibility)
              age_column: age
              gender_column: gender
              medicaid_column: dual_eligible
              disabled_column: disabled

              # Model version
              hcc_version: v28  # or v24

              # Payment year for coefficient lookups
              payment_year: 2024
            ```

        Output Structure:
            The expression generates two output types:

            1. Patient Risk Factors (long format):
               - patient_id
               - hcc_code (e.g., 'HCC85')
               - hcc_description
               - coefficient
               - diagnosis_codes (array of contributing ICD-10 codes)

            2. Patient Risk Scores (summary):
               - patient_id
               - demographic_score (age/gender/Medicaid factors)
               - disease_score (sum of HCC coefficients)
               - total_risk_score (demographic + disease)
               - hcc_count (number of distinct HCCs)
               - model_version
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
        """
        Build CMS-HCC risk adjustment expressions.

                Args:
                    config: Configuration dict with columns, tables, and model settings

                Returns:
                    Dictionary containing:
                    - 'map_diagnosis_to_hcc': Expression to join diagnoses to HCC codes
                    - 'apply_hierarchies': Expression to suppress subordinate HCCs
                    - 'calculate_demographic_score': Expression for age/gender/status factors
                    - 'calculate_disease_score': Expression to sum HCC coefficients
                    - 'calculate_total_score': Expression for final risk score
                    - 'patient_risk_factors': LazyFrame transformer for detailed HCC list
                    - 'patient_risk_scores': LazyFrame transformer for summary scores
        """
        # Extract configuration with defaults
        patient_id_col = config.get("patient_id_column", "patient_id")
        diagnosis_col = config.get("diagnosis_column", "diagnosis_code")
        claim_date_col = config.get("claim_through_date_column", "claim_end_date")
        hcc_mapping_table = config.get("hcc_mapping_table", "value_sets_cms_hcc_icd_10_cm_mappings")
        hierarchy_table = config.get("hierarchy_table", "value_sets_cms_hcc_hierarchy")
        age_col = config.get("age_column", "age")
        gender_col = config.get("gender_column", "gender")
        medicaid_col = config.get("medicaid_column", "dual_eligible")
        disabled_col = config.get("disabled_column", "disabled")
        hcc_version = config.get("hcc_version", "v28")
        payment_year = config.get("payment_year", 2024)
        # Update config with defaults for downstream use
        config_with_defaults = {
            "patient_id_column": patient_id_col,
            "diagnosis_column": diagnosis_col,
            "claim_through_date_column": claim_date_col,
            "hcc_mapping_table": hcc_mapping_table,
            "hierarchy_table": hierarchy_table,
            "age_column": age_col,
            "gender_column": gender_col,
            "medicaid_column": medicaid_col,
            "disabled_column": disabled_col,
            "hcc_version": hcc_version,
            "payment_year": payment_year,
        }

        # Build expression components
        expressions = {}

        # 1. Map diagnosis codes to HCC categories
        # This will be used as a join expression
        expressions["map_diagnosis_to_hcc"] = {
            "description": "Join ICD-10 codes to HCC mappings",
            "join_key": diagnosis_col,
            "lookup_table": hcc_mapping_table,
            "select_columns": ["hcc_code", "hcc_label", "coefficient"],
        }

        # 2. Deduplicate patient-HCC pairs
        # After joining, we need to dedupe at patient-HCC level
        expressions["dedupe_patient_hcc"] = {
            "description": "Deduplicate patient-HCC combinations",
            "group_by": [patient_id_col, "hcc_code"],
            "aggregations": {
                "diagnosis_codes": pl.col(diagnosis_col).unique(),
                "first_diagnosis_date": pl.col(claim_date_col).min(),
                "last_diagnosis_date": pl.col(claim_date_col).max(),
                "diagnosis_count": pl.col(diagnosis_col).n_unique(),
            },
        }

        # 3. Apply hierarchical suppressions
        # Higher HCCs suppress lower ones (e.g., HCC85 suppresses HCC86)
        # This requires checking which HCCs are present and removing subordinates
        expressions["apply_hierarchies"] = {
            "description": "Apply HCC hierarchical suppressions",
            "details": """
            For each patient:
                        1. Get list of all HCCs
                        2. Check hierarchy table for suppressions
                        3. Remove subordinate HCCs
                        4. Keep only dominant HCCs
            """,
            # This is complex logic that requires the hierarchy seed table
            # and custom suppression rules
        }

        # 4. Calculate demographic score
        # Age/gender cells map to coefficient ranges
        expressions["calculate_demographic_score"] = {
            "description": "Calculate demographic risk factors",
            "age_gender_cells": {
                # Example cells (actual coefficients from CMS rate announcement)
                "F0_34": 0.26,
                "F35_44": 0.31,
                "F45_54": 0.39,
                "F55_59": 0.48,
                "F60_64": 0.60,
                "F65_69": 0.72,
                "F70_74": 0.88,
                "F75_79": 1.05,
                "F80_84": 1.27,
                "F85_89": 1.52,
                "F90_94": 1.77,
                "F95_GT": 2.02,
                "M0_34": 0.20,
                "M35_44": 0.24,
                "M45_54": 0.30,
                "M55_59": 0.38,
                "M60_64": 0.49,
                "M65_69": 0.59,
                "M70_74": 0.72,
                "M75_79": 0.86,
                "M80_84": 1.02,
                "M85_89": 1.20,
                "M90_94": 1.38,
                "M95_GT": 1.56,
            },
            "medicaid_multiplier": 1.13,  # Medicaid dual-eligible adjustment
            "disabled_multiplier": 1.07,  # Originally disabled adjustment
        }
        # 5. Calculate disease score (sum of HCC coefficients)
        expressions["calculate_disease_score"] = pl.col("coefficient").sum().alias("disease_score")
        # 6. Calculate total risk score
        expressions["calculate_total_score"] = (
            pl.col("demographic_score") + pl.col("disease_score")
        ).alias("total_risk_score")
        # Return transformer functions
        return {
            "expressions": expressions,
            "config": config_with_defaults,
            "version": hcc_version,
            "year": payment_year,
        }

    @staticmethod
    def transform_patient_risk_factors(
        medical_claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        hcc_mapping: pl.LazyFrame,
        disease_factors: pl.LazyFrame | None = None,
        disease_hierarchy: pl.LazyFrame | None = None,
        config: dict[str, Any] | None = None,
    ) -> pl.LazyFrame:
        """
        Transform medical claims into patient-level HCC risk factors.

                Args:
                    medical_claims: Medical claims with diagnosis codes
                    eligibility: Patient eligibility/demographic data
                    hcc_mapping: ICD-10 to HCC mapping table (cms_hcc_icd_10_cm_mappings)
                    disease_factors: HCC coefficients and descriptions (cms_hcc_disease_factors)
                    disease_hierarchy: Hierarchy suppression rules (cms_hcc_disease_hierarchy)
                    config: Configuration dict

                Returns:
                    LazyFrame with columns:
                    - patient_id
                    - hcc_code
                    - hcc_description
                    - coefficient
                    - diagnosis_codes (array)
                    - first_diagnosis_date
                    - last_diagnosis_date
        """
        if config is None:
            config = {}

        patient_id_col = config.get("patient_id_column", "patient_id")
        diagnosis_col = config.get("diagnosis_column", "diagnosis_code")
        claim_date_col = config.get("claim_through_date_column", "claim_end_date")
        hcc_version = config.get("hcc_version", "v28")  # Default to V28

        # Determine which HCC column to use based on version
        hcc_col = "cms_hcc_v28" if hcc_version == "v28" else "cms_hcc_v24"

        # Step 1: Join claims to HCC mappings (ICD-10 → HCC codes)
        claims_with_hcc = medical_claims.join(
            hcc_mapping,
            left_on=diagnosis_col,
            right_on="diagnosis_code",  # Column name from Tuva seed schema
            how="inner",
        )

        # Step 2: Extract HCC code from version-specific column
        # Filter out null HCC codes and convert to int
        claims_with_hcc = claims_with_hcc.filter(pl.col(hcc_col).is_not_null()).with_columns(
            pl.col(hcc_col).cast(pl.Int64).alias("hcc_code")
        )

        # Step 3: Aggregate to patient-HCC level
        patient_hccs = claims_with_hcc.group_by([patient_id_col, "hcc_code"]).agg(
            [
                pl.col(diagnosis_col).unique().alias("diagnosis_codes"),
                pl.col(claim_date_col).min().alias("first_diagnosis_date"),
                pl.col(claim_date_col).max().alias("last_diagnosis_date"),
            ]
        )

        # Step 4: Join to disease_factors to get descriptions and coefficients
        if disease_factors is not None:
            # Filter disease_factors to matching model version
            model_version = f"CMS-HCC-V{hcc_version.upper().replace('V', '')}"
            disease_factors_filtered = disease_factors.filter(
                pl.col("model_version") == model_version
            )

            # For simplicity, take first coefficient per HCC (can be refined with eligibility status)
            hcc_coefficients = disease_factors_filtered.group_by("hcc_code").agg(
                [
                    pl.col("description").first().alias("hcc_description"),
                    pl.col("coefficient").first(),
                ]
            )

            patient_hccs = patient_hccs.join(
                hcc_coefficients,
                on="hcc_code",
                how="left",
            )
        else:
            # No disease_factors provided, add placeholder columns
            patient_hccs = patient_hccs.with_columns(
                [
                    pl.lit(None).alias("hcc_description"),
                    pl.lit(0.0).alias("coefficient"),
                ]
            )

        # Step 5: Apply hierarchies (if provided)
        # TODO: Implement hierarchy suppression logic using disease_hierarchy
        # For now, just return all HCCs

        return patient_hccs

    @staticmethod
    def transform_patient_risk_scores(
        patient_risk_factors: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """
        Aggregate patient-level HCCs into risk scores.

                Args:
                    patient_risk_factors: Output from transform_patient_risk_factors
                    eligibility: Patient demographic data
                    config: Configuration dict

                Returns:
                    LazyFrame with columns:
                    - patient_id
                    - age
                    - gender
                    - demographic_score
                    - disease_score
                    - total_risk_score
                    - hcc_count
                    - model_version
        """
        patient_id_col = config.get("patient_id_column", "patient_id")
        config.get("age_column", "age")
        birth_date_col = config.get("birth_date_column", "birth_date")
        gender_col = config.get("gender_column", "gender")
        config.get("medicaid_column", "dual_eligible")
        config.get("disabled_column", "disabled")
        hcc_version = config.get("hcc_version", "v28")

        # Calculate disease score (sum of HCC coefficients)
        # Also capture the max diagnosis date as the observable window reference
        disease_scores = patient_risk_factors.group_by(patient_id_col).agg(
            [
                pl.col("coefficient").sum().alias("disease_score"),
                pl.count().alias("hcc_count"),
                pl.col("last_diagnosis_date").max().alias("max_diagnosis_date"),
            ]
        )

        # Join with eligibility for demographics
        risk_scores = eligibility.join(disease_scores, on=patient_id_col, how="left")

        # Calculate age using the max diagnosis date as the reference (observable window)
        # This ensures idempotent processing - age is based on the data's temporal extent
        age_expr = (
            (
                (
                    pl.col("max_diagnosis_date").cast(pl.Date)
                    - pl.col(birth_date_col).cast(pl.Date)
                ).dt.total_days()
                / 365.25
            )
            .cast(pl.Int32)
            .alias("age")
        )

        # Add age column
        risk_scores = risk_scores.with_columns([age_expr])

        # Calculate demographic score
        # This is simplified - actual implementation would use age/gender cells
        # For now, use a placeholder formula
        risk_scores = risk_scores.with_columns(
            [
                # Base demographic score (age normalized to 65-year-old)
                (pl.col("age") / 65.0).alias("demographic_score"),
                # Fill nulls for patients with no HCCs
                pl.col("disease_score").fill_null(0.0),
                pl.col("hcc_count").fill_null(0),
            ]
        )

        # Calculate total risk score
        risk_scores = risk_scores.with_columns(
            [
                (pl.col("demographic_score") + pl.col("disease_score")).alias("total_risk_score"),
                pl.lit(hcc_version).alias("model_version"),
            ]
        )

        return risk_scores.select(
            [
                patient_id_col,
                "age",  # Always use "age" since we create it above
                gender_col,
                "demographic_score",
                "disease_score",
                "total_risk_score",
                "hcc_count",
                "model_version",
            ]
        )
