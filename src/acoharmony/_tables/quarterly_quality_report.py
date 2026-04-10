# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for quarterly_quality_report schema.

Generated from: _schemas/quarterly_quality_report.yml
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_sheets,
    with_storage,
)


@register_schema(
    name="quarterly_quality_report",
    version=2,
    tier="bronze",
    description="Quarterly Quality Report - REACH ACO quarterly quality performance feedback",
    file_patterns={"reach": ["*QTLQR*"]},
)
@with_parser(
    type="excel_multi_sheet",
    parser="excel_multi_sheet",
    multi_output=True,
    encoding="utf-8",
    has_header=False,
    embedded_transforms=False,
    sheet_config={
        "header_row": 8,
        "data_start_row": 9,
        "column_mapping_strategy": "position",
        "end_marker_column": 0,
        "end_marker_value": "",
    },
)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*QTLQR*"]},
    silver={
        "output_name": "quarterly_quality_report.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_sheets(
    sheets=[
        {
            "sheet_name": "Parameters",
            "sheet_type": "parameters",
            "description": "Reporting parameters and metadata (key-value pairs)",
            "header_row": 2,
            "data_start_row": 3,
            "columns": [
                {"name": "parameter_name", "position": 0, "data_type": "string",
                 "description": "Parameter name"},
                {"name": "parameter_value", "position": 1, "data_type": "string",
                 "description": "Parameter value"},
            ],
        },
        {
            "sheet_name": "Tables 1a-1b",
            "sheet_type": "claims_results",
            "description": "Claims-Based Quality Measure Results and Concurrent Benchmarks",
            "header_row": 8,
            "data_start_row": 9,
            "columns": [
                {"name": "measure", "position": 0, "data_type": "string",
                 "description": "Quality measure code (ACR, UAMCC, DAH)"},
                {"name": "measure_name", "position": 1, "data_type": "string",
                 "description": "Full measure name with performance direction"},
                {"name": "measure_score", "position": 4, "data_type": "float",
                 "description": "ACO's quality measure performance score"},
                {"name": "measure_volume", "position": 5, "data_type": "float",
                 "description": "Measure volume (beneficiary count)"},
                {"name": "mean_measure_score", "position": 6, "data_type": "float",
                 "description": "Mean score across all ACOs"},
                {"name": "provisional_percentile", "position": 7, "data_type": "float",
                 "description": "Provisional measure percentile rank"},
                {"name": "highest_benchmark_met", "position": 8, "data_type": "string",
                 "description": "Highest provisional benchmark met (e.g., 70th, 90th)"},
            ],
        },
        {
            "sheet_name": "Tables 2-4",
            "sheet_type": "stratified_reporting",
            "description": "Stratified Reporting by Dual Eligibility, SES, and Race",
            "header_row": 11,
            "data_start_row": 11,
            "columns": [
                {"name": "measure", "position": 0, "data_type": "string",
                 "description": "Quality measure code"},
                {"name": "measure_name", "position": 1, "data_type": "string",
                 "description": "Full measure name"},
                {"name": "stratified_volume", "position": 2, "data_type": "float",
                 "description": "Measure volume for stratified population"},
                {"name": "stratified_score", "position": 3, "data_type": "float",
                 "description": "Measure score for stratified population"},
                {"name": "stratified_mean", "position": 4, "data_type": "float",
                 "description": "Mean score for stratified population (All ACOs)"},
                {"name": "all_bene_mean", "position": 6, "data_type": "float",
                 "description": "Mean score for all beneficiaries (All ACOs)"},
            ],
        },
        # Also try the Tables 2-3 variant name (used in later files)
        {
            "sheet_name": "Tables 2-3",
            "sheet_type": "stratified_reporting",
            "description": "Stratified Reporting (Tables 2-3 variant)",
            "header_row": 11,
            "data_start_row": 11,
            "columns": [
                {"name": "measure", "position": 0, "data_type": "string",
                 "description": "Quality measure code"},
                {"name": "measure_name", "position": 1, "data_type": "string",
                 "description": "Full measure name"},
                {"name": "stratified_volume", "position": 2, "data_type": "float",
                 "description": "Measure volume for stratified population"},
                {"name": "stratified_score", "position": 3, "data_type": "float",
                 "description": "Measure score for stratified population"},
                {"name": "stratified_mean", "position": 4, "data_type": "float",
                 "description": "Mean score for stratified population (All ACOs)"},
                {"name": "all_bene_mean", "position": 6, "data_type": "float",
                 "description": "Mean score for all beneficiaries (All ACOs)"},
            ],
        },
    ],
    matrix_fields=[
        # Extract from Parameters sheet (index 4) using label search
        {"matrix": [4, 0, 1], "field_name": "reporting_period", "data_type": "string",
         "search_label": "Reporting Period"},
        {"matrix": [4, 0, 1], "field_name": "aco_type", "data_type": "string",
         "search_label": "ACO Type"},
        {"matrix": [4, 0, 1], "field_name": "hcc_version", "data_type": "string",
         "search_label": "Hierarchical Condition"},
        {"matrix": [4, 0, 1], "field_name": "measure_spec_version", "data_type": "string",
         "search_label": "Measure Specification"},
        {"matrix": [4, 0, 1], "field_name": "benchmark_year", "data_type": "string",
         "search_label": "Benchmark Year"},
        {"matrix": [4, 0, 1], "field_name": "report_production_date", "data_type": "string",
         "search_label": "Report Production Date"},
        # Extract ACO ID from data sheet preamble (row 4 of Tables 1a-1b = sheet 5)
        {"matrix": [5, 0, 1], "field_name": "aco_id", "data_type": "string",
         "search_label": "ACO ID"},
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=176,
    file_pattern="P.D????.QTLQR.Q?.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class QuarterlyQualityReport:
    """Quarterly Quality Report — REACH ACO quarterly quality performance feedback."""

    pass  # Multi-sheet Excel — columns defined per-sheet above
