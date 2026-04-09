# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for annual_quality_report schema.

Generated from: _schemas/annual_quality_report.yml

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_sheets,
    with_storage,
    with_transform,
)


@register_schema(
    name="annual_quality_report",
    version=2,
    tier="bronze",
    description="Annual Quality Report",
    file_patterns={"reach": ["*ANLQR*"]},
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*ANLQR*"]},
    silver={
        "output_name": "annual_quality_report.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_sheets(
    sheets=[
        {
            "sheet_index": 0,
            "sheet_type": "documentation",
            "description": "Cover page - documentation sheet",
        },
        {
            "sheet_index": 1,
            "sheet_type": "documentation",
            "description": "Table of Contents - documentation sheet",
        },
        {
            "sheet_index": 2,
            "sheet_type": "documentation",
            "description": "Glossary - documentation sheet",
        },
        {
            "sheet_index": 3,
            "sheet_type": "documentation",
            "description": "About This Report - documentation sheet",
        },
        {
            "sheet_index": 4,
            "sheet_type": "parameters",
            "description": "Parameters - quality measure definitions",
            "header_row": 7,
            "data_start_row": 8,
            "columns": [
                {
                    "name": "measure",
                    "position": 0,
                    "data_type": "string",
                    "description": "Quality measure code (ACR, UAMCC, DAH, CAHPS)",
                },
                {
                    "name": "scale",
                    "position": 1,
                    "data_type": "string",
                    "description": "Performance direction indicator",
                },
                {
                    "name": "p4p_or_p4r",
                    "position": 2,
                    "data_type": "string",
                    "description": "Pay for Performance or Pay for Reporting designation",
                },
                {
                    "name": "possible_points",
                    "position": 3,
                    "data_type": "integer",
                    "description": "Maximum points available for the measure",
                },
            ],
        },
        {
            "sheet_index": 5,
            "sheet_type": "summary_information",
            "description": "Table 1. Summary Information - overall quality performance",
            "header_row": 8,
            "data_start_row": 9,
            "columns": [
                {
                    "name": "measure",
                    "position": 0,
                    "data_type": "string",
                    "description": "Quality measure code",
                },
                {
                    "name": "measure_name",
                    "position": 1,
                    "data_type": "string",
                    "description": "Full measure name with performance direction",
                },
                {
                    "name": "measure_score",
                    "position": 2,
                    "data_type": "float",
                    "description": "ACO's measure performance score",
                },
                {
                    "name": "points_earned",
                    "position": 3,
                    "data_type": "float",
                    "description": "Quality points earned for this measure",
                },
                {
                    "name": "points_possible",
                    "position": 4,
                    "data_type": "float",
                    "description": "Maximum points available",
                },
                {
                    "name": "total_points",
                    "position": 5,
                    "data_type": "float",
                    "description": "Cumulative points across all measures",
                },
                {
                    "name": "initial_quality_score",
                    "position": 6,
                    "data_type": "float",
                    "description": "Quality score before adjustments (0-100%)",
                },
                {
                    "name": "ci_sep_gateway_multiplier",
                    "position": 7,
                    "data_type": "float",
                    "description": "Continuous Improvement/Sustained Exceptional Performance multiplier",
                },
                {
                    "name": "hedr_adjustment",
                    "position": 8,
                    "data_type": "float",
                    "description": "Health Equity Data Reporting adjustment in percentage points",
                },
                {
                    "name": "total_quality_score",
                    "position": 9,
                    "data_type": "float",
                    "description": "Final quality score after all adjustments (0-100%)",
                },
                {
                    "name": "quality_withhold_earned_back",
                    "position": 10,
                    "data_type": "float",
                    "description": "Percentage of financial benchmark earned back (0-2%)",
                },
                {
                    "name": "hpp_bonus",
                    "position": 11,
                    "data_type": "float",
                    "description": "High Performing Population bonus",
                },
            ],
        },
        {
            "sheet_index": 6,
            "sheet_type": "cisep_hedr_hpp",
            "description": "Tables 2a-2c. CISEP-HEDR-HPP - continuous improvement, sustained exceptional performance, and health equity",
            "header_row": [8, 9],
            "data_start_row": 10,
            "columns": [
                {
                    "name": "measure",
                    "position": 0,
                    "data_type": "string",
                    "description": "Quality measure code",
                },
                {
                    "name": "measure_name",
                    "position": 1,
                    "data_type": "string",
                    "description": "Full measure name with performance direction",
                },
                {
                    "name": "py_current_measure_score",
                    "position": 2,
                    "data_type": "float",
                    "description": "Current performance year measure score",
                },
                {
                    "name": "py_current_standardized_score",
                    "position": 3,
                    "data_type": "float",
                    "description": "Current year standardized score for comparison",
                },
                {
                    "name": "py_current_percentile_rank",
                    "position": 4,
                    "data_type": "float",
                    "description": "Current year percentile rank among peers",
                },
                {
                    "name": "py_prior_measure_score",
                    "position": 5,
                    "data_type": "float",
                    "description": "Prior performance year measure score",
                },
                {
                    "name": "py_prior_standardized_score",
                    "position": 6,
                    "data_type": "float",
                    "description": "Prior year standardized score",
                },
                {
                    "name": "py_prior_percentile_rank",
                    "position": 7,
                    "data_type": "float",
                    "description": "Prior year percentile rank",
                },
                {
                    "name": "continuous_improvement",
                    "position": 8,
                    "data_type": "string",
                    "description": "Continuous improvement achievement indicator",
                },
                {
                    "name": "sustained_exceptional_performance",
                    "position": 9,
                    "data_type": "string",
                    "description": "Sustained exceptional performance indicator",
                },
                {
                    "name": "ci_sep_points",
                    "position": 10,
                    "data_type": "float",
                    "description": "Points earned for CI or SEP",
                },
                {
                    "name": "total_ci_sep_points",
                    "position": 11,
                    "data_type": "float",
                    "description": "Cumulative CI/SEP points",
                },
                {
                    "name": "met_overall_ci_sep_criteria",
                    "position": 12,
                    "data_type": "string",
                    "description": "Overall CI/SEP gateway criteria met",
                },
            ],
        },
        {
            "sheet_index": 7,
            "sheet_type": "hedr_sdoh",
            "description": "Table 2d. HEDR SDOH Data - Social Determinants of Health health equity reporting",
            "header_row": 9,
            "data_start_row": 10,
            "columns": [
                {
                    "name": "calculation_component",
                    "position": 0,
                    "data_type": "string",
                    "description": "Numerator or denominator component",
                },
                {
                    "name": "description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Component definition",
                },
                {
                    "name": "initial_beneficiary_counts",
                    "position": 2,
                    "data_type": "integer",
                    "description": "Initial count of beneficiaries for this component",
                },
                {
                    "name": "final_beneficiary_counts",
                    "position": 3,
                    "data_type": "integer",
                    "description": "Final validated count of beneficiaries",
                },
                {
                    "name": "sdoh_hedr_reporting_rate",
                    "position": 4,
                    "data_type": "string",
                    "description": "SDOH health equity data reporting rate",
                },
            ],
        },
        {
            "sheet_index": 8,
            "sheet_type": "claims_results",
            "description": "Tables 3a-3b. Claims Results - performance on claims-based quality measures",
            "header_row": 8,
            "data_start_row": 9,
            "columns": [
                {
                    "name": "measure",
                    "position": 0,
                    "data_type": "string",
                    "description": "Quality measure code",
                },
                {
                    "name": "measure_name",
                    "position": 1,
                    "data_type": "string",
                    "description": "Full measure name with performance direction",
                },
                {
                    "name": "measure_score",
                    "position": 4,
                    "data_type": "float",
                    "description": "ACO's measure score",
                },
                {
                    "name": "mean_measure_score",
                    "position": 5,
                    "data_type": "float",
                    "description": "Average score across all ACOs of the same type",
                },
                {
                    "name": "measure_percentile_rank",
                    "position": 6,
                    "data_type": "float",
                    "description": "Percentile ranking among peer ACOs",
                },
                {
                    "name": "highest_quality_benchmark_met",
                    "position": 7,
                    "data_type": "string",
                    "description": "Highest benchmark threshold achieved (e.g., 90th)",
                },
                {
                    "name": "points_earned",
                    "position": 8,
                    "data_type": "float",
                    "description": "Quality points earned for this measure",
                },
            ],
        },
        {
            "sheet_index": 9,
            "sheet_type": "documentation",
            "description": "About CAHPS - documentation sheet",
        },
        {
            "sheet_index": 10,
            "sheet_type": "cahps_results",
            "description": "Table 4. CAHPS Results - Consumer Assessment of Healthcare Providers and Systems survey results",
            "header_row": 8,
            "data_start_row": 9,
            "columns": [
                {
                    "name": "ssm",
                    "position": 0,
                    "data_type": "string",
                    "description": "Summary Survey Measure name",
                },
                {
                    "name": "ssm_score",
                    "position": 2,
                    "data_type": "float",
                    "description": "ACO's Summary Survey Measure score",
                },
                {
                    "name": "mean_ssm_score",
                    "position": 3,
                    "data_type": "float",
                    "description": "Average SSM score across all ACOs of the same type",
                },
                {
                    "name": "prior_year_ssm_linearized",
                    "position": 4,
                    "data_type": "string",
                    "description": "Prior year linearized mean SSM score",
                },
                {
                    "name": "prior_year_ssm_top_box",
                    "position": 5,
                    "data_type": "string",
                    "description": "Prior year top-box SSM score",
                },
            ],
        },
        {
            "sheet_index": 11,
            "sheet_type": "cahps_questions",
            "description": "Table 5. CAHPS Questions - detailed CAHPS survey question responses",
            "header_row": 8,
            "data_start_row": 9,
            "columns": [
                {
                    "name": "question_category",
                    "position": 0,
                    "data_type": "string",
                    "description": "CAHPS question category or measure type",
                },
                {
                    "name": "metric_type",
                    "position": 1,
                    "data_type": "string",
                    "description": "Type of metric (e.g., Linearized Mean, Number of Respondents)",
                },
                {
                    "name": "your_aco",
                    "position": 2,
                    "data_type": "string",
                    "description": "Your ACO's score or count",
                },
                {
                    "name": "all_acos",
                    "position": 3,
                    "data_type": "string",
                    "description": "All ACOs' average score or count",
                },
            ],
        },
        {
            "sheet_index": 12,
            "sheet_type": "documentation",
            "description": "About Stratified Reporting - documentation sheet",
        },
        {
            "sheet_index": 13,
            "sheet_type": "stratified_reporting",
            "description": "Tables 6-8. Stratified Reporting - performance stratified by beneficiary demographics and social factors",
            "header_row": [9, 10, 11],
            "data_start_row": 12,
            "columns": [
                {
                    "name": "measure",
                    "position": 0,
                    "data_type": "string",
                    "description": "Quality measure code",
                },
                {
                    "name": "measure_name",
                    "position": 1,
                    "data_type": "string",
                    "description": "Full measure name with performance direction",
                },
                {
                    "name": "stratified_measure_volume",
                    "position": 2,
                    "data_type": "float",
                    "description": "Measure volume for stratified population (Your ACO)",
                },
                {
                    "name": "stratified_measure_score",
                    "position": 3,
                    "data_type": "float",
                    "description": "Measure score for stratified population (Your ACO)",
                },
                {
                    "name": "stratified_mean_score",
                    "position": 4,
                    "data_type": "float",
                    "description": "Mean measure score for stratified population (All ACOs)",
                },
                {
                    "name": "all_beneficiaries_mean_score",
                    "position": 6,
                    "data_type": "float",
                    "description": "Mean measure score for all beneficiaries (All ACOs)",
                },
            ],
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=217,
    file_pattern="P.D????.ANLQR.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class AnnualQualityReport:
    """
    Annual Quality Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - AnnualQualityReport.schema_name() -> str
        - AnnualQualityReport.schema_metadata() -> dict
        - AnnualQualityReport.parser_config() -> dict
        - AnnualQualityReport.transform_config() -> dict
        - AnnualQualityReport.lineage_config() -> dict
    """

    sheet_type: str | None = Field(default=None, description="Sheet type identifier")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AnnualQualityReport":
        """Create instance from dictionary."""
        return cls(**data)
