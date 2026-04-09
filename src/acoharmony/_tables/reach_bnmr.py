# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for reach_bnmr schema.

Generated from: _schemas/reach_bnmr.yml

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
    name="reach_bnmr",
    version=2,
    tier="bronze",
    description="REACH Benchmark Report - Detailed ACO REACH financial and risk data",
    file_patterns={"reach": ["REACH.D*.BNMR.*.xlsx"]},
)
@with_parser(
    type="excel_multi_sheet",
    parser="excel_multi_sheet",
    multi_output=True,
    encoding="utf-8",
    has_header=False,
    embedded_transforms=False,
    sheet_config={
        "header_row": 0,
        "data_start_row": 1,
        "column_mapping_strategy": "header_match",
        "end_marker_column": 0,
        "end_marker_value": "__NO_MARKER__",
    },
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["REACH.D*.BNMR.*.xlsx"]},
    silver={
        "output_name": "reach_bnmr_metadata.parquet",
        "refresh_frequency": "quarterly",
    },
)
@with_sheets(
    sheets=[
        {
            "sheet_name": "REPORT_PARAMETERS",
            "sheet_index": 0,
            "sheet_type": "report_parameters",
            "description": "ACO parameters, model-wide parameters, risk score parameters, HEBA parameters, risk corridors - 59 rows total",
            "dynamic_columns": {
                "year_header_row": 27,
                "year_columns": [2, 3, 4, 5, 6, 7, 8],
                "year_column_prefix": "year_",
                "description": "Year column headers extracted dynamically from row 27",
            },
            "columns": [
                {
                    "name": "parameter_name",
                    "position": 0,
                    "data_type": "string",
                    "description": "Parameter name/label",
                },
                {
                    "name": "value_primary",
                    "position": 1,
                    "data_type": "string",
                    "description": "Primary value (or Q1 value for first column)",
                },
                {
                    "name": "year_col_2",
                    "position": 2,
                    "data_type": "string",
                    "description": "Year column 2 (extract actual year from row 27, col 2)",
                },
                {
                    "name": "year_col_3",
                    "position": 3,
                    "data_type": "string",
                    "description": "Year column 3 (extract actual year from row 27, col 3)",
                },
                {
                    "name": "year_col_4",
                    "position": 4,
                    "data_type": "string",
                    "description": "Year column 4 (extract actual year from row 27, col 4)",
                },
                {
                    "name": "year_col_5",
                    "position": 5,
                    "data_type": "string",
                    "description": "Year column 5 (extract actual year from row 27, col 5)",
                },
                {
                    "name": "year_col_6",
                    "position": 6,
                    "data_type": "string",
                    "description": "Year column 6 (extract actual year from row 27, col 6)",
                },
                {
                    "name": "year_col_7",
                    "position": 7,
                    "data_type": "string",
                    "description": "Year column 7 (extract actual year from row 27, col 7)",
                },
                {
                    "name": "year_col_8",
                    "position": 8,
                    "data_type": "string",
                    "description": "Year column 8 (extract actual year from row 27, col 8) - typically performance year",
                },
            ],
            "sections": [
                {
                    "name": "aco_parameters",
                    "start_row": 0,
                    "end_row": 16,
                    "description": "ACO-specific configuration",
                },
                {
                    "name": "model_wide_parameters",
                    "start_row": 17,
                    "end_row": 25,
                    "description": "Model-wide calculation parameters",
                },
                {
                    "name": "incurred_paid_parameters",
                    "start_row": 26,
                    "end_row": 35,
                    "description": "Performance and reporting period date ranges by year",
                },
                {
                    "name": "risk_score_parameters",
                    "start_row": 36,
                    "end_row": 46,
                    "description": "Normalization factors and CIF by year and population",
                },
                {
                    "name": "heba_parameters",
                    "start_row": 47,
                    "end_row": 51,
                    "description": "Health Equity Benchmark Adjustment percentile scores",
                },
                {
                    "name": "risk_arrangement_parameters",
                    "start_row": 52,
                    "end_row": 58,
                    "description": "Risk corridor thresholds and savings/losses rates",
                },
            ],
        },
        {
            "sheet_name": "FINANCIAL_SETTLEMENT",
            "sheet_index": 1,
            "sheet_type": "financial_settlement",
            "description": "Financial settlement calculation flow - 72 rows, 5 cols (AD, ESRD, TOTAL)",
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number in calculation flow",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of calculation line",
                },
                {
                    "name": "ad_value",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Aged & Disabled value",
                },
                {
                    "name": "esrd_value",
                    "position": 3,
                    "data_type": "decimal",
                    "description": "ESRD value",
                },
                {
                    "name": "total_value",
                    "position": 4,
                    "data_type": "decimal",
                    "description": "Total combined value",
                },
            ],
            "named_fields": [
                {
                    "row": 12,
                    "field_name": "benchmark_before_discount_ad",
                    "column": 2,
                    "data_type": "decimal",
                    "description": "Line 6: Benchmark before Discount (AD)",
                },
                {
                    "row": 12,
                    "field_name": "benchmark_before_discount_esrd",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 6: Benchmark before Discount (ESRD)",
                },
                {
                    "row": 20,
                    "field_name": "benchmark_all_aligned_ad",
                    "column": 2,
                    "data_type": "decimal",
                    "description": "Line 14: Benchmark for All Aligned Beneficiaries (AD)",
                },
                {
                    "row": 20,
                    "field_name": "benchmark_all_aligned_esrd",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 14: Benchmark for All Aligned Beneficiaries (ESRD)",
                },
                {
                    "row": 20,
                    "field_name": "benchmark_all_aligned_total",
                    "column": 4,
                    "data_type": "decimal",
                    "description": "Line 14: Benchmark for All Aligned Beneficiaries (TOTAL)",
                },
                {
                    "row": 27,
                    "field_name": "benchmark_after_heba_total",
                    "column": 4,
                    "data_type": "decimal",
                    "description": "Line 21: Benchmark after Health Equity Adjustment (TOTAL)",
                },
                {
                    "row": 31,
                    "field_name": "total_cost_before_stoploss_total",
                    "column": 4,
                    "data_type": "decimal",
                    "description": "Line 25: Total Cost of Care before Stop-Loss (TOTAL)",
                },
                {
                    "row": 34,
                    "field_name": "total_cost_after_stoploss_total",
                    "column": 4,
                    "data_type": "decimal",
                    "description": "Line 28: Total Cost of Care after Stop-Loss (TOTAL)",
                },
                {
                    "row": 37,
                    "field_name": "total_cost_with_ibnr_total",
                    "column": 4,
                    "data_type": "decimal",
                    "description": "Line 31: Total Cost with Estimated IBNR (TOTAL)",
                },
                {
                    "row": 39,
                    "field_name": "gross_savings_losses_total",
                    "column": 4,
                    "data_type": "decimal",
                    "description": "Line 33: Gross Savings (Losses) (TOTAL)",
                },
                {
                    "row": 51,
                    "field_name": "total_monies_owed",
                    "column": 4,
                    "data_type": "decimal",
                    "description": "Line 45: Total Monies Owed (TOTAL)",
                },
            ],
            "sections": [
                {
                    "name": "benchmark_expenditure",
                    "start_row": 6,
                    "end_row": 27,
                    "description": "Lines 1-21: Benchmark calculation for claims and voluntary aligned",
                },
                {
                    "name": "performance_expenditure",
                    "start_row": 28,
                    "end_row": 37,
                    "description": "Lines 22-31: ACO performance period expenditure",
                },
                {
                    "name": "savings_losses",
                    "start_row": 38,
                    "end_row": 44,
                    "description": "Lines 32-38: Gross savings/losses by corridor",
                },
                {
                    "name": "total_monies_owed",
                    "start_row": 45,
                    "end_row": 55,
                    "description": "Lines 39-49: Calculation of total monies owed",
                },
                {
                    "name": "risk_corridors",
                    "start_row": 56,
                    "end_row": 71,
                    "description": "Lines 50-65: Application of risk corridors",
                },
            ],
        },
        {
            "sheet_name": "BENCHMARK_HISTORICAL_AD",
            "sheet_index": 2,
            "sheet_type": "benchmark_historical_ad",
            "description": "AD historical blended benchmark - 36 rows, 10 cols (Claims/Vol historical years)",
            "dynamic_columns": {
                "year_header_row": 6,
                "year_columns": [2, 3, 4, 6, 7, 8],
                "description": "Year column headers extracted from row 6 - claims years in cols 2-4, voluntary years in cols 6-8",
            },
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number or label",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of calculation line",
                },
                {
                    "name": "claims_year_1",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Claims aligned year 1 value (extract year from row 6, col 2)",
                },
                {
                    "name": "claims_year_2",
                    "position": 3,
                    "data_type": "decimal",
                    "description": "Claims aligned year 2 value (extract year from row 6, col 3)",
                },
                {
                    "name": "claims_year_3",
                    "position": 4,
                    "data_type": "decimal",
                    "description": "Claims aligned year 3 value (extract year from row 6, col 4)",
                },
                {
                    "name": "claims_benchmark",
                    "position": 5,
                    "data_type": "decimal",
                    "description": "Claims aligned benchmark value",
                },
                {
                    "name": "vol_year_1",
                    "position": 6,
                    "data_type": "decimal",
                    "description": "Voluntary aligned year 1 value (extract year from row 6, col 6)",
                },
                {
                    "name": "vol_year_2",
                    "position": 7,
                    "data_type": "decimal",
                    "description": "Voluntary aligned year 2 value (extract year from row 6, col 7)",
                },
                {
                    "name": "vol_year_3",
                    "position": 8,
                    "data_type": "decimal",
                    "description": "Voluntary aligned year 3 value (extract year from row 6, col 8)",
                },
                {
                    "name": "vol_benchmark",
                    "position": 9,
                    "data_type": "decimal",
                    "description": "Voluntary aligned benchmark value",
                },
            ],
            "named_fields": [
                {
                    "row": 13,
                    "field_name": "pbpm_historical_rate_claims_benchmark",
                    "column": 5,
                    "data_type": "decimal",
                    "description": "Line 7: PBPM Historical Rate - Claims Benchmark",
                },
                {
                    "row": 13,
                    "field_name": "pbpm_historical_rate_vol_benchmark",
                    "column": 9,
                    "data_type": "decimal",
                    "description": "Line 7: PBPM Historical Rate - Voluntary Benchmark",
                },
                {
                    "row": 14,
                    "field_name": "aco_regional_rate_claims",
                    "column": 5,
                    "data_type": "decimal",
                    "description": "Line 8: ACO Regional Rate - Claims",
                },
                {
                    "row": 14,
                    "field_name": "aco_regional_rate_vol",
                    "column": 9,
                    "data_type": "decimal",
                    "description": "Line 8: ACO Regional Rate - Voluntary",
                },
                {
                    "row": 20,
                    "field_name": "blended_benchmark_claims",
                    "column": 5,
                    "data_type": "decimal",
                    "description": "Line 14: Blended Benchmark - Claims",
                },
                {
                    "row": 20,
                    "field_name": "blended_benchmark_vol",
                    "column": 9,
                    "data_type": "decimal",
                    "description": "Line 14: Blended Benchmark - Voluntary",
                },
                {
                    "row": 26,
                    "field_name": "aco_normalized_risk_score_claims",
                    "column": 5,
                    "data_type": "decimal",
                    "description": "Line 19: ACO Normalized Risk Score - Claims",
                },
                {
                    "row": 26,
                    "field_name": "aco_normalized_risk_score_vol",
                    "column": 9,
                    "data_type": "decimal",
                    "description": "Line 19: ACO Normalized Risk Score - Voluntary",
                },
            ],
            "sections": [
                {
                    "name": "benchmark_calculation",
                    "start_row": 7,
                    "end_row": 21,
                    "description": "Lines 1-15: Blended benchmark calculation flow",
                },
                {
                    "name": "risk_score",
                    "start_row": 23,
                    "end_row": 26,
                    "description": "Lines 16-19: Risk score calculation",
                },
                {
                    "name": "prospective_trend",
                    "start_row": 28,
                    "end_row": 33,
                    "description": "Lines 20-25: Prospective trend calculations",
                },
                {
                    "name": "parameters",
                    "start_row": 35,
                    "end_row": 35,
                    "description": "Line A: Baseline weights",
                },
            ],
        },
        {
            "sheet_name": "BENCHMARK_HISTORICAL_ESRD",
            "sheet_index": 3,
            "sheet_type": "benchmark_historical_esrd",
            "description": "ESRD historical blended benchmark - 36 rows, 10 cols (Claims/Vol historical years)",
            "dynamic_columns": {
                "year_header_row": 6,
                "year_columns": [2, 3, 4, 6, 7, 8],
                "description": "Year column headers extracted from row 6 - claims years in cols 2-4, voluntary years in cols 6-8",
            },
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number or label",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of calculation line",
                },
                {
                    "name": "claims_year_1",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Claims aligned year 1 value (extract year from row 6, col 2)",
                },
                {
                    "name": "claims_year_2",
                    "position": 3,
                    "data_type": "decimal",
                    "description": "Claims aligned year 2 value (extract year from row 6, col 3)",
                },
                {
                    "name": "claims_year_3",
                    "position": 4,
                    "data_type": "decimal",
                    "description": "Claims aligned year 3 value (extract year from row 6, col 4)",
                },
                {
                    "name": "claims_benchmark",
                    "position": 5,
                    "data_type": "decimal",
                    "description": "Claims aligned benchmark value",
                },
                {
                    "name": "vol_year_1",
                    "position": 6,
                    "data_type": "decimal",
                    "description": "Voluntary aligned year 1 value (extract year from row 6, col 6)",
                },
                {
                    "name": "vol_year_2",
                    "position": 7,
                    "data_type": "decimal",
                    "description": "Voluntary aligned year 2 value (extract year from row 6, col 7)",
                },
                {
                    "name": "vol_year_3",
                    "position": 8,
                    "data_type": "decimal",
                    "description": "Voluntary aligned year 3 value (extract year from row 6, col 8)",
                },
                {
                    "name": "vol_benchmark",
                    "position": 9,
                    "data_type": "decimal",
                    "description": "Voluntary aligned benchmark value",
                },
            ],
            "named_fields": [
                {
                    "row": 13,
                    "field_name": "pbpm_historical_rate_claims_benchmark",
                    "column": 5,
                    "data_type": "decimal",
                    "description": "Line 7: PBPM Historical Rate - Claims Benchmark",
                },
                {
                    "row": 13,
                    "field_name": "pbpm_historical_rate_vol_benchmark",
                    "column": 9,
                    "data_type": "decimal",
                    "description": "Line 7: PBPM Historical Rate - Voluntary Benchmark",
                },
                {
                    "row": 14,
                    "field_name": "aco_regional_rate_claims",
                    "column": 5,
                    "data_type": "decimal",
                    "description": "Line 8: ACO Regional Rate - Claims",
                },
                {
                    "row": 14,
                    "field_name": "aco_regional_rate_vol",
                    "column": 9,
                    "data_type": "decimal",
                    "description": "Line 8: ACO Regional Rate - Voluntary",
                },
                {
                    "row": 20,
                    "field_name": "blended_benchmark_claims",
                    "column": 5,
                    "data_type": "decimal",
                    "description": "Line 14: Blended Benchmark - Claims",
                },
                {
                    "row": 20,
                    "field_name": "blended_benchmark_vol",
                    "column": 9,
                    "data_type": "decimal",
                    "description": "Line 14: Blended Benchmark - Voluntary",
                },
                {
                    "row": 26,
                    "field_name": "aco_normalized_risk_score_claims",
                    "column": 5,
                    "data_type": "decimal",
                    "description": "Line 19: ACO Normalized Risk Score - Claims",
                },
                {
                    "row": 26,
                    "field_name": "aco_normalized_risk_score_vol",
                    "column": 9,
                    "data_type": "decimal",
                    "description": "Line 19: ACO Normalized Risk Score - Voluntary",
                },
            ],
            "sections": [
                {
                    "name": "benchmark_calculation",
                    "start_row": 7,
                    "end_row": 21,
                    "description": "Lines 1-15: Blended benchmark calculation flow",
                },
                {
                    "name": "risk_score",
                    "start_row": 23,
                    "end_row": 26,
                    "description": "Lines 16-19: Risk score calculation",
                },
                {
                    "name": "prospective_trend",
                    "start_row": 28,
                    "end_row": 33,
                    "description": "Lines 20-25: Prospective trend calculations",
                },
                {
                    "name": "parameters",
                    "start_row": 35,
                    "end_row": 35,
                    "description": "Line A: Baseline weights",
                },
            ],
        },
        {
            "sheet_name": "RISKSCORE_AD",
            "sheet_index": 4,
            "sheet_type": "riskscore_ad",
            "description": "A&D Risk Score calculations - 36 rows, 4 cols (Claims Aligned, Voluntary Aligned)",
            "dynamic_columns": {
                "reference_year_row": 4,
                "reference_year_col": 1,
                "description": "Reference year (e.g., RY2022) extracted from row 4, col 1",
            },
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number or section label",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of risk score component",
                },
                {
                    "name": "reference_year_value",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Reference Year value (extract year from row 4, col 1)",
                },
                {
                    "name": "py_value",
                    "position": 3,
                    "data_type": "decimal",
                    "description": "Performance Year value",
                },
            ],
            "named_fields": [
                {
                    "row": 8,
                    "field_name": "normalized_risk_score_claims_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 3: PY Normalized Risk Score - Claims Aligned",
                },
                {
                    "row": 12,
                    "field_name": "capped_risk_score_claims_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 7: PY Capped Risk Score - Claims Aligned",
                },
                {
                    "row": 14,
                    "field_name": "benchmark_risk_score_claims_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 9: PY Benchmark Risk Score - Claims Aligned",
                },
                {
                    "row": 19,
                    "field_name": "normalized_risk_score_vol_new_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 12: PY Normalized Risk Score - Newly Voluntary",
                },
                {
                    "row": 23,
                    "field_name": "normalized_risk_score_vol_cont_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 15: PY Normalized Risk Score - Continuously Voluntary",
                },
                {
                    "row": 29,
                    "field_name": "benchmark_risk_score_vol_cont_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 21: PY Benchmark Risk Score - Continuously Voluntary",
                },
                {
                    "row": 35,
                    "field_name": "weighted_avg_vol_risk_score",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 26: Weighted Average Voluntary Aligned Risk Score",
                },
            ],
            "sections": [
                {
                    "name": "claims_aligned",
                    "start_row": 6,
                    "end_row": 14,
                    "description": "Lines 1-9: Claims aligned risk score calculation",
                },
                {
                    "name": "voluntary_aligned_new",
                    "start_row": 17,
                    "end_row": 19,
                    "description": "Lines 10-12: Newly voluntary-aligned risk score",
                },
                {
                    "name": "voluntary_aligned_continuous",
                    "start_row": 21,
                    "end_row": 29,
                    "description": "Lines 13-21: Continuously voluntary-aligned risk score",
                },
                {
                    "name": "weighted_average",
                    "start_row": 31,
                    "end_row": 35,
                    "description": "Lines 22-26: Weighted average risk scores",
                },
            ],
        },
        {
            "sheet_name": "RISKSCORE_ESRD",
            "sheet_index": 5,
            "sheet_type": "riskscore_esrd",
            "description": "ESRD Risk Score calculations - 34 rows, 4 cols (same structure as AD)",
            "dynamic_columns": {
                "reference_year_row": 4,
                "reference_year_col": 1,
                "description": "Reference year (e.g., RY2022) extracted from row 4, col 1",
            },
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number or section label",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of risk score component",
                },
                {
                    "name": "reference_year_value",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Reference Year value (extract year from row 4, col 1)",
                },
                {
                    "name": "py_value",
                    "position": 3,
                    "data_type": "decimal",
                    "description": "Performance Year value",
                },
            ],
            "named_fields": [
                {
                    "row": 8,
                    "field_name": "normalized_risk_score_claims_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 3: PY Normalized Risk Score - Claims Aligned",
                },
                {
                    "row": 11,
                    "field_name": "capped_risk_score_claims_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 6: PY Capped Risk Score - Claims Aligned",
                },
                {
                    "row": 13,
                    "field_name": "benchmark_risk_score_claims_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 8: PY Benchmark Risk Score - Claims Aligned",
                },
                {
                    "row": 18,
                    "field_name": "normalized_risk_score_vol_new_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 11: PY Normalized Risk Score - Newly Voluntary",
                },
                {
                    "row": 22,
                    "field_name": "normalized_risk_score_vol_cont_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 14: PY Normalized Risk Score - Continuously Voluntary",
                },
                {
                    "row": 27,
                    "field_name": "benchmark_risk_score_vol_cont_py",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 19: PY Benchmark Risk Score - Continuously Voluntary",
                },
                {
                    "row": 33,
                    "field_name": "weighted_avg_vol_risk_score",
                    "column": 3,
                    "data_type": "decimal",
                    "description": "Line 24: Weighted Average Voluntary Aligned Risk Score",
                },
            ],
            "sections": [
                {
                    "name": "claims_aligned",
                    "start_row": 6,
                    "end_row": 13,
                    "description": "Lines 1-8: Claims aligned risk score calculation",
                },
                {
                    "name": "voluntary_aligned_new",
                    "start_row": 16,
                    "end_row": 18,
                    "description": "Lines 9-11: Newly voluntary-aligned risk score",
                },
                {
                    "name": "voluntary_aligned_continuous",
                    "start_row": 20,
                    "end_row": 27,
                    "description": "Lines 12-19: Continuously voluntary-aligned risk score",
                },
                {
                    "name": "weighted_average",
                    "start_row": 29,
                    "end_row": 33,
                    "description": "Lines 20-24: Weighted average risk scores",
                },
            ],
        },
        {
            "sheet_name": "STOP_LOSS_CHARGE",
            "sheet_index": 6,
            "sheet_type": "stop_loss_charge",
            "description": "Stop loss charge calculations (WARNING: may contain #DIV/0! errors)",
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of charge component",
                },
                {
                    "name": "value",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Charge value (may be null due to formula errors)",
                },
            ],
            "sections": [
                {
                    "name": "full_sheet",
                    "start_row": 0,
                    "end_row": None,
                    "description": "Entire sheet - handle parsing errors gracefully",
                },
            ],
        },
        {
            "sheet_name": "STOP_LOSS_PAYOUT",
            "sheet_index": 7,
            "sheet_type": "stop_loss_payout",
            "description": "Stop loss payout calculations - 30 rows, 3 cols",
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of payout component",
                },
                {
                    "name": "value",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Payout value",
                },
            ],
            "named_fields": [
                {
                    "row": 10,
                    "field_name": "total_beneficiaries",
                    "column": 2,
                    "data_type": "decimal",
                    "description": "Line 4: Total Beneficiaries",
                },
                {
                    "row": 15,
                    "field_name": "total_expenditures",
                    "column": 2,
                    "data_type": "decimal",
                    "description": "Line 8: Total Expenditures",
                },
                {
                    "row": 20,
                    "field_name": "total_stop_loss_payouts",
                    "column": 2,
                    "data_type": "decimal",
                    "description": "Line 12: Total Stop-Loss Payouts",
                },
                {
                    "row": 25,
                    "field_name": "total_payout_rate",
                    "column": 2,
                    "data_type": "decimal",
                    "description": "Line 16: Total Payout Rate as % of Expenditure",
                },
                {
                    "row": 28,
                    "field_name": "stop_loss_neutrality_factor",
                    "column": 2,
                    "data_type": "decimal",
                    "description": "Line 18: Stop-Loss Payout Neutrality Factor",
                },
                {
                    "row": 29,
                    "field_name": "adjusted_aggregate_stoploss_payout",
                    "column": 2,
                    "data_type": "decimal",
                    "description": "Line 19: Adjusted Aggregate Stop-Loss Payout",
                },
            ],
            "sections": [
                {
                    "name": "beneficiary_counts",
                    "start_row": 7,
                    "end_row": 10,
                    "description": "Lines 1-4: Beneficiaries by stop-loss band",
                },
                {
                    "name": "expenditures",
                    "start_row": 12,
                    "end_row": 15,
                    "description": "Lines 5-8: Expenditures by stop-loss band",
                },
                {
                    "name": "payouts",
                    "start_row": 17,
                    "end_row": 20,
                    "description": "Lines 9-12: Stop-loss payouts by band",
                },
                {
                    "name": "payout_rates",
                    "start_row": 22,
                    "end_row": 25,
                    "description": "Lines 13-16: Payout rates as percentage",
                },
                {
                    "name": "neutrality_factor",
                    "start_row": 27,
                    "end_row": 29,
                    "description": "Lines 17-19: Neutrality factor calculation",
                },
            ],
        },
        {
            "sheet_name": "DATA_CLAIMS",
            "sheet_index": 8,
            "sheet_type": "claims",
            "description": "Claims data by calendar year/month, benchmark type, and claim type",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "clndr_mnth", "header_text": "CLNDR_MNTH", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "align_type", "header_text": "ALIGN_TYPE", "data_type": "string"},
                {"name": "bnmrk_type", "header_text": "BNMRK_TYPE", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "clm_type_cd", "header_text": "CLM_TYPE_CD", "data_type": "string"},
                {
                    "name": "clm_pmt_amt_agg",
                    "header_text": "CLM_PMT_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "sqstr_amt_agg",
                    "header_text": "SQSTR_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "apa_rdctn_amt_agg",
                    "header_text": "APA_RDCTN_AMT_AGG",
                    "data_type": "decimal",
                },
                {"name": "ucc_amt_agg", "header_text": "UCC_AMT_AGG", "data_type": "decimal"},
                {
                    "name": "op_dsh_amt_agg",
                    "header_text": "OP_DSH_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "cp_dsh_amt_agg",
                    "header_text": "CP_DSH_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "op_ime_amt_agg",
                    "header_text": "OP_IME_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "cp_ime_amt_agg",
                    "header_text": "CP_IME_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "nonpbp_rdct_amt_agg",
                    "header_text": "NONPBP_RDCT_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_amt_agg_apa",
                    "header_text": "ACO_AMT_AGG_APA",
                    "data_type": "decimal",
                },
                {"name": "srvc_month", "header_text": "SRVC_MONTH", "data_type": "string"},
                {"name": "efctv_month", "header_text": "EFCTV_MONTH", "data_type": "string"},
                {"name": "apa_cd", "header_text": "APA_CD", "data_type": "string"},
            ],
        },
        {
            "sheet_name": "DATA_RISK",
            "sheet_index": 9,
            "sheet_type": "risk",
            "description": "Risk score and beneficiary count data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "clndr_mnth", "header_text": "CLNDR_MNTH", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "align_type", "header_text": "ALIGN_TYPE", "data_type": "string"},
                {"name": "va_cat", "header_text": "VA_CAT", "data_type": "string"},
                {"name": "bnmrk_type", "header_text": "BNMRK_TYPE", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "bene_dcnt", "header_text": "BENE_DCNT", "data_type": "integer"},
                {"name": "elig_mnths", "header_text": "ELIG_MNTHS", "data_type": "integer"},
                {
                    "name": "raw_risk_score",
                    "header_text": "RAW_RISK_SCORE",
                    "data_type": "decimal",
                },
                {
                    "name": "norm_risk_score",
                    "header_text": "NORM_RISK_SCORE",
                    "data_type": "decimal",
                },
                {"name": "risk_denom", "header_text": "RISK_DENOM", "data_type": "decimal"},
                {"name": "score_type", "header_text": "SCORE_TYPE", "data_type": "string"},
                {
                    "name": "bene_dcnt_annual",
                    "header_text": "BENE_DCNT_ANNUAL",
                    "data_type": "integer",
                },
            ],
        },
        {
            "sheet_name": "DATA_COUNTY",
            "sheet_index": 10,
            "sheet_type": "county",
            "description": "County-level beneficiary and payment data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "align_type", "header_text": "ALIGN_TYPE", "data_type": "string"},
                {"name": "bnmrk_type", "header_text": "BNMRK_TYPE", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "cty_accrl_cd", "header_text": "CTY_ACCRL_CD", "data_type": "string"},
                {"name": "bene_dcnt", "header_text": "BENE_DCNT", "data_type": "integer"},
                {"name": "elig_mnths", "header_text": "ELIG_MNTHS", "data_type": "integer"},
                {"name": "cty_rate", "header_text": "CTY_RATE", "data_type": "decimal"},
                {"name": "adj_cty_pmt", "header_text": "ADJ_CTY_PMT", "data_type": "decimal"},
                {"name": "gaf_trend", "header_text": "GAF_TREND", "data_type": "decimal"},
                {
                    "name": "adj_gaf_trend",
                    "header_text": "ADJ_GAF_TREND",
                    "data_type": "decimal",
                },
            ],
        },
        {
            "sheet_name": "DATA_USPCC",
            "sheet_index": 11,
            "sheet_type": "uspcc",
            "description": "US Per Capita Cost data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "uspcc", "header_text": "USPCC", "data_type": "decimal"},
                {"name": "ucc_hosp_adj", "header_text": "UCC_HOSP_ADJ", "data_type": "decimal"},
                {
                    "name": "adj_ffs_uspcc",
                    "header_text": "ADJ_FFS_USPCC",
                    "data_type": "decimal",
                },
            ],
        },
        {
            "sheet_name": "DATA_HEBA",
            "sheet_index": 12,
            "sheet_type": "heba",
            "description": "Health Equity Benchmark Adjustment data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {
                    "name": "heba_up_mnths",
                    "header_text": "HEBA_UP_MNTHS",
                    "data_type": "integer",
                },
                {
                    "name": "heba_down_mnths",
                    "header_text": "HEBA_DOWN_MNTHS",
                    "data_type": "integer",
                },
                {"name": "heba_up_amt", "header_text": "HEBA_UP_AMT", "data_type": "decimal"},
                {
                    "name": "heba_down_amt",
                    "header_text": "HEBA_DOWN_AMT",
                    "data_type": "decimal",
                },
            ],
        },
        {
            "sheet_name": "DATA_STOP_LOSS_COUNTY",
            "sheet_index": 13,
            "sheet_type": "stop_loss_county",
            "description": "Stop Loss County-level data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "cty_accrl_cd", "header_text": "CTY_ACCRL_CD", "data_type": "string"},
                {"name": "bene_dcnt", "header_text": "BENE_DCNT", "data_type": "integer"},
                {"name": "elig_mnths", "header_text": "ELIG_MNTHS", "data_type": "integer"},
                {"name": "gaf_trend", "header_text": "GAF_TREND", "data_type": "decimal"},
                {
                    "name": "adj_gaf_trend",
                    "header_text": "ADJ_GAF_TREND",
                    "data_type": "decimal",
                },
                {
                    "name": "avg_payout_pct",
                    "header_text": "AVG_PAYOUT_PCT",
                    "data_type": "decimal",
                },
                {
                    "name": "ad_ry_avg_pbpm",
                    "header_text": "AD_RY_AVG_PBPM",
                    "data_type": "decimal",
                },
                {
                    "name": "esrd_ry_avg_pbpm",
                    "header_text": "ESRD_RY_AVG_PBPM",
                    "data_type": "decimal",
                },
                {
                    "name": "adj_avg_payout_pct",
                    "header_text": "ADJ_AVG_PAYOUT_PCT",
                    "data_type": "decimal",
                },
                {
                    "name": "adj_ad_ry_avg_pbpm",
                    "header_text": "ADJ_AD_RY_AVG_PBPM",
                    "data_type": "decimal",
                },
                {
                    "name": "adj_esrd_ry_avg_pbpm",
                    "header_text": "ADJ_ESRD_RY_AVG_PBPM",
                    "data_type": "decimal",
                },
            ],
        },
        {
            "sheet_name": "DATA_STOP_LOSS_PAYOUT",
            "sheet_index": 14,
            "sheet_type": "data_stop_loss_payout",
            "description": "Stop Loss Payout data",
            "columns": [
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {
                    "name": "algn_aco_amt_agg",
                    "header_text": "ALGN_ACO_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_exp",
                    "header_text": "ACO_STOPLOSS_EXP",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_exp_b0",
                    "header_text": "ACO_STOPLOSS_EXP_B0",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_exp_b1",
                    "header_text": "ACO_STOPLOSS_EXP_B1",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_exp_b2",
                    "header_text": "ACO_STOPLOSS_EXP_B2",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_payout_b0",
                    "header_text": "ACO_STOPLOSS_PAYOUT_B0",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_payout_b1",
                    "header_text": "ACO_STOPLOSS_PAYOUT_B1",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_payout_b2",
                    "header_text": "ACO_STOPLOSS_PAYOUT_B2",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_payout_total",
                    "header_text": "ACO_STOPLOSS_PAYOUT_TOTAL",
                    "data_type": "decimal",
                },
                {"name": "bene_cnt_b0", "header_text": "BENE_CNT_B0", "data_type": "integer"},
                {"name": "bene_cnt_b1", "header_text": "BENE_CNT_B1", "data_type": "integer"},
                {"name": "bene_cnt_b2", "header_text": "BENE_CNT_B2", "data_type": "integer"},
            ],
        },
        {
            "sheet_name": "DATA_STOP_LOSS_CLAIMS",
            "sheet_index": 15,
            "sheet_type": "stop_loss_claims",
            "description": "Stop Loss Claims data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "aco_amt_agg", "header_text": "ACO_AMT_AGG", "data_type": "decimal"},
            ],
        },
        {
            "sheet_name": "DATA_CAP",
            "sheet_index": 16,
            "sheet_type": "cap",
            "description": "Capitation payment data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "pmt_mnth", "header_text": "PMT_MNTH", "data_type": "string"},
                {"name": "align_type", "header_text": "ALIGN_TYPE", "data_type": "string"},
                {
                    "name": "aco_tcc_amt_total",
                    "header_text": "ACO_TCC_AMT_TOTAL",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_bpcc_amt_total",
                    "header_text": "ACO_BPCC_AMT_TOTAL",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_epcc_amt_total_seq",
                    "header_text": "ACO_EPCC_AMT_TOTAL_SEQ",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_apo_amt_total_seq",
                    "header_text": "ACO_APO_AMT_TOTAL_SEQ",
                    "data_type": "decimal",
                },
            ],
        },
    ],
    matrix_fields=[
        {"matrix": [0, 0, 1], "field_name": "performance_year", "data_type": "string", "search_label": "Performance Year"},
        {"matrix": [0, 0, 1], "field_name": "aco_id", "data_type": "string", "search_label": "Organization ID"},
        {"matrix": [0, 0, 1], "field_name": "aco_type", "data_type": "string", "search_label": "Organization Type"},
        {"matrix": [0, 0, 1], "field_name": "risk_arrangement", "data_type": "string", "search_label": "Risk Arrangement"},
        {"matrix": [0, 0, 1], "field_name": "payment_mechanism", "data_type": "string", "search_label": "Payment Mechanism"},
        {"matrix": [0, 0, 1], "field_name": "discount", "data_type": "decimal", "search_label": "Discount"},
        {"matrix": [0, 0, 1], "field_name": "shared_savings_rate", "data_type": "decimal", "search_label": "Shared Savings Rate"},
        {"matrix": [0, 0, 1], "field_name": "advanced_payment_option", "data_type": "string", "search_label": "Advanced Payment Option"},
        {"matrix": [0, 0, 1], "field_name": "stop_loss_elected", "data_type": "string", "search_label": "Stop-Loss Elected"},
        {"matrix": [0, 0, 1], "field_name": "stop_loss_type", "data_type": "string", "search_label": "Stop-Loss Type"},
        {"matrix": [0, 0, 1], "field_name": "quality_withhold", "data_type": "decimal", "search_label": "Quality Withhold"},
        {"matrix": [0, 0, 1], "field_name": "quality_score", "data_type": "decimal", "search_label": "Quality Score"},
        {"matrix": [0, 0, 1], "field_name": "voluntary_aligned_benchmark", "data_type": "string", "search_label": "Voluntary Aligned Benchmark"},
        {"matrix": [0, 0, 1], "field_name": "blend_percentage", "data_type": "decimal", "search_label": "Blend Percentage"},
        {"matrix": [0, 0, 1], "field_name": "blend_ceiling", "data_type": "decimal", "search_label": "Ceiling"},
        {"matrix": [0, 0, 1], "field_name": "blend_floor", "data_type": "decimal", "search_label": "Floor"},
        {"matrix": [0, 0, 1], "field_name": "ad_retrospective_trend", "data_type": "decimal", "search_label": "A&D Retrospective Trend"},
        {"matrix": [0, 0, 1], "field_name": "esrd_retrospective_trend", "data_type": "decimal", "search_label": "ESRD Retrospective Trend"},
        {"matrix": [0, 0, 1], "field_name": "ad_completion_factor", "data_type": "decimal", "search_label": "A&D Completion Factor"},
        {"matrix": [0, 0, 1], "field_name": "esrd_completion_factor", "data_type": "decimal", "search_label": "ESRD Completion Factor"},
        {"matrix": [0, 0, 1], "field_name": "stop_loss_payout_neutrality_factor", "data_type": "decimal", "search_label": "Stop-Loss Payout Neutrality"},
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=215,
    file_pattern="REACH.D*.BNMR.*.xlsx",
    extract_zip=False,
    refresh_frequency="quarterly",
)
@dataclass
class ReachBnmr:
    """
    REACH Benchmark Report - Detailed ACO REACH financial and risk data

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - ReachBnmr.schema_name() -> str
        - ReachBnmr.schema_metadata() -> dict
        - ReachBnmr.parser_config() -> dict
        - ReachBnmr.transform_config() -> dict
        - ReachBnmr.lineage_config() -> dict
    """

    sheet_type: str | None = Field(default=None, description="Sheet type identifier")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ReachBnmr":
        """Create instance from dictionary."""
        return cls(**data)
