# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for sbqr schema.

Generated from: _schemas/sbqr.yml

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
    name="sbqr",
    version=2,
    tier="bronze",
    description="REACH Shadow Bundles Quarterly Report",
    file_patterns={"reach": ["D????.PY????.Q?.SBQR.D??????.T*.xlsx"]},
)
@with_parser(
    type="excel_multi_sheet", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["D????.PY????.Q?.SBQR.D??????.T*.xlsx"]},
    silver={"output_name": "sbqr.parquet", "refresh_frequency": "quarterly"},
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
            "sheet_type": "aco_graph_test_data_2024",
            "description": "ACO graph test data for 2024",
            "columns": [
                {
                    "name": "data_point",
                    "position": 0,
                    "data_type": "string",
                    "description": "Data point identifier",
                },
            ],
        },
        {
            "sheet_index": 3,
            "sheet_type": "aco_graph_test_data_2025",
            "description": "ACO graph test data for 2025",
            "columns": [
                {
                    "name": "data_point",
                    "position": 0,
                    "data_type": "string",
                    "description": "Data point identifier",
                },
            ],
        },
        {
            "sheet_index": 4,
            "sheet_type": "aco_vs_national_spending",
            "description": "ACO vs National Spending metadata",
        },
        {
            "sheet_index": 5,
            "sheet_type": "aco_summary",
            "description": "ACO Summary data",
            "columns": [
                {
                    "name": "initial_sort_order",
                    "position": 0,
                    "data_type": "integer",
                    "description": "Sort order for display",
                },
                {
                    "name": "year_quarter",
                    "position": 1,
                    "data_type": "string",
                    "description": "Year and quarter (e.g., 2024Q1, 2024-All)",
                },
                {
                    "name": "clinical_episode_category",
                    "position": 2,
                    "data_type": "string",
                    "description": "Clinical episode category",
                },
                {
                    "name": "medical_surgical",
                    "position": 3,
                    "data_type": "string",
                    "description": "Medical or Surgical classification",
                },
                {
                    "name": "clinical_episode_count",
                    "position": 4,
                    "data_type": "integer",
                    "description": "Number of clinical episodes",
                },
                {
                    "name": "beneficiary_count",
                    "position": 5,
                    "data_type": "integer",
                    "description": "Number of beneficiaries",
                },
            ],
        },
        {
            "sheet_index": 6,
            "sheet_type": "provider_details",
            "description": "Provider Details data",
            "columns": [
                {
                    "name": "initial_sort_order",
                    "position": 0,
                    "data_type": "integer",
                    "description": "Sort order for display",
                },
                {
                    "name": "year_quarter",
                    "position": 1,
                    "data_type": "string",
                    "description": "Year and quarter",
                },
                {
                    "name": "ccn",
                    "position": 2,
                    "data_type": "string",
                    "description": "CMS Certification Number",
                },
            ],
        },
        {
            "sheet_index": 7,
            "sheet_type": "outcomes_and_utilization",
            "description": "Outcomes and Utilization data",
            "columns": [
                {
                    "name": "initial_sort_order",
                    "position": 0,
                    "data_type": "integer",
                    "description": "Sort order for display",
                },
                {
                    "name": "year_quarter",
                    "position": 1,
                    "data_type": "string",
                    "description": "Year and quarter",
                },
                {
                    "name": "clinical_episode_category",
                    "position": 2,
                    "data_type": "string",
                    "description": "Clinical episode category",
                },
            ],
        },
        {
            "sheet_index": 8,
            "sheet_type": "patient_severity_details",
            "description": "Patient Severity Details data",
            "columns": [
                {
                    "name": "initial_sort_order",
                    "position": 0,
                    "data_type": "integer",
                    "description": "Sort order for display",
                },
                {
                    "name": "year_quarter",
                    "position": 1,
                    "data_type": "string",
                    "description": "Year and quarter",
                },
            ],
        },
        {
            "sheet_index": 9,
            "sheet_type": "spending_comparisons",
            "description": "Spending Comparisons data",
            "columns": [
                {
                    "name": "initial_sort_order",
                    "position": 0,
                    "data_type": "integer",
                    "description": "Sort order for display",
                },
                {
                    "name": "year_quarter",
                    "position": 1,
                    "data_type": "string",
                    "description": "Year and quarter",
                },
            ],
        },
        {
            "sheet_index": 10,
            "sheet_type": "trigger_code_spending_summary",
            "description": "Trigger Code Spending Summary data",
            "columns": [
                {
                    "name": "initial_sort_order",
                    "position": 0,
                    "data_type": "integer",
                    "description": "Sort order for display",
                },
                {
                    "name": "year_quarter",
                    "position": 1,
                    "data_type": "string",
                    "description": "Year and quarter",
                },
            ],
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=243,
    file_pattern="D????.PY????.Q?.SBQR.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="quarterly",
)
@dataclass
class Sbqr:
    """
    REACH Shadow Bundles Quarterly Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Sbqr.schema_name() -> str
        - Sbqr.schema_metadata() -> dict
        - Sbqr.parser_config() -> dict
        - Sbqr.transform_config() -> dict
        - Sbqr.lineage_config() -> dict
    """

    sheet_type: str | None = Field(default=None, description="Sheet type identifier")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Sbqr":
        """Create instance from dictionary."""
        return cls(**data)
