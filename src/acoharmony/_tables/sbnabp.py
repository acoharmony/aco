# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for sbnabp schema.

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
)


@register_schema(
    name="sbnabp",
    version=2,
    tier="bronze",
    description="REACH Shadow Bundles National Adjusted Benchmarks",
    file_patterns={"reach": ["D????.PY????.SBNABP.D??????.T*.xlsx"]},
)
@with_parser(
    type="excel_multi_sheet", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["D????.PY????.SBNABP.D??????.T*.xlsx"]},
    silver={"output_name": "sbnabp.parquet", "refresh_frequency": "annually"},
)
@with_sheets(
    sheets=[
        {
            "sheet_index": 0,
            "sheet_type": "overview",
            "description": "Overview metadata",
        },
        {
            "sheet_index": 1,
            "sheet_type": "definitions",
            "description": "Definitions metadata",
        },
        {
            "sheet_index": 2,
            "sheet_type": "nat_ach_bp_summary",
            "description": "National ACH Benchmark Price Summary",
            "columns": [
                {
                    "name": "initial_sort_order",
                    "position": 0,
                    "header_text": "Initial Sort Order",
                    "data_type": "integer",
                    "description": "Sort order for display",
                },
                {
                    "name": "ccn",
                    "position": 1,
                    "header_text": "CCN",
                    "data_type": "string",
                    "description": "CMS Certification Number",
                },
                {
                    "name": "medical_surgical_classification",
                    "position": 2,
                    "header_text": "Medical/Surgical Classification",
                    "data_type": "string",
                    "description": "Medical or Surgical classification",
                },
                {
                    "name": "clinical_episode_category",
                    "position": 3,
                    "header_text": "Clinical Episode Category",
                    "data_type": "string",
                    "description": "Clinical episode category",
                },
                {
                    "name": "clinical_episode_service_line_group",
                    "position": 4,
                    "header_text": "Clinical Episode Service Line Group",
                    "data_type": "string",
                    "description": "Service line group for clinical episode",
                },
                {
                    "name": "ach_baseline_clinical_episode_count",
                    "position": 5,
                    "header_text": "ACH Baseline Clinical Episode Count",
                    "data_type": "integer",
                    "description": "Baseline count of clinical episodes for ACH",
                },
                {
                    "name": "meets_minimum_threshold",
                    "position": 6,
                    "header_text": "Meets Minimum Threshold",
                    "data_type": "string",
                    "description": "Whether ACH meets ≥41 clinical episodes threshold (Y/N)",
                },
                {
                    "name": "benchmark_price_cy25_fy25",
                    "position": 7,
                    "header_text": "Benchmark Price CY25/FY25",
                    "data_type": "decimal",
                    "description": "Benchmark price in CY25/FY25 real dollars",
                },
            ],
        },
        {
            "sheet_index": 3,
            "sheet_type": "nat_ach_bp_components",
            "description": "National ACH Benchmark Price Components",
            "columns": [
                {
                    "name": "initial_sort_order",
                    "position": 0,
                    "header_text": "Initial Sort Order",
                    "data_type": "integer",
                    "description": "Sort order for display",
                },
                {
                    "name": "ccn",
                    "position": 1,
                    "header_text": "CCN",
                    "data_type": "string",
                    "description": "CMS Certification Number",
                },
                {
                    "name": "clinical_episode_category",
                    "position": 2,
                    "header_text": "Clinical Episode Category",
                    "data_type": "string",
                    "description": "Clinical episode category",
                },
            ],
        },
        {
            "sheet_index": 4,
            "sheet_type": "risk_adjustment_parameter",
            "description": "Risk Adjustment Parameters",
            "columns": [
                {
                    "name": "parameter_name",
                    "position": 0,
                    "header_text": "Parameter Name",
                    "data_type": "string",
                    "description": "Risk adjustment parameter name",
                },
                {
                    "name": "parameter_value",
                    "position": 1,
                    "header_text": "Parameter Value",
                    "data_type": "decimal",
                    "description": "Risk adjustment parameter value",
                },
            ],
        },
        {
            "sheet_index": 5,
            "sheet_type": "peer_group_characteristics",
            "description": "Peer Group Characteristics",
            "columns": [
                {
                    "name": "peer_group",
                    "position": 0,
                    "header_text": "Peer Group",
                    "data_type": "string",
                    "description": "Peer group identifier",
                },
                {
                    "name": "major_teaching_hospital",
                    "position": 1,
                    "header_text": "Major Teaching Hospital",
                    "data_type": "string",
                    "description": "Major teaching hospital flag (Y/N)",
                },
                {
                    "name": "urban_rural",
                    "position": 2,
                    "header_text": "Urban/Rural",
                    "data_type": "string",
                    "description": "Urban or Rural classification",
                },
                {
                    "name": "safety_net",
                    "position": 3,
                    "header_text": "Safety Net",
                    "data_type": "string",
                    "description": "Safety net hospital flag (Y/N)",
                },
            ],
        },
        {
            "sheet_index": 6,
            "sheet_type": "winsor_values_baseline",
            "description": "Winsor Values for Baseline",
            "columns": [
                {
                    "name": "clinical_episode_category",
                    "position": 0,
                    "header_text": "Clinical Episode Category",
                    "data_type": "string",
                    "description": "Clinical episode category",
                },
                {
                    "name": "winsor_threshold",
                    "position": 1,
                    "header_text": "Winsor Threshold",
                    "data_type": "decimal",
                    "description": "Winsorization threshold value",
                },
            ],
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=243,
    file_pattern="D????.PY????.SBNABP.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="annually",
)
@dataclass
class Sbnabp:
    """
    REACH Shadow Bundles National Adjusted Benchmarks

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Sbnabp.schema_name() -> str
        - Sbnabp.schema_metadata() -> dict
        - Sbnabp.parser_config() -> dict
        - Sbnabp.transform_config() -> dict
        - Sbnabp.lineage_config() -> dict
    """

    sheet_type: str | None = Field(default=None, description="Sheet type identifier")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Sbnabp":
        """Create instance from dictionary."""
        return cls(**data)
