# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for estimated_cisep_change_threshold_report schema.

Generated from: _schemas/estimated_cisep_change_threshold_report.yml

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

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
    name="estimated_cisep_change_threshold_report",
    version=2,
    tier="bronze",
    description="Estimated CI/SEP Change Threshold Report",
    file_patterns={"reach": ["*ECCTR*"]},
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*ECCTR*"]},
    silver={
        "output_name": "estimated_cisep_change_threshold_report.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_sheets(
    sheets=[
        {
            "sheet_index": 0,
            "sheet_type": "cover",
            "description": "Report cover page with ACO ID, report title, and agreement start date",
        },
        {
            "sheet_index": 1,
            "sheet_type": "table_of_contents",
            "description": "Navigation for the report sections",
        },
        {
            "sheet_index": 2,
            "sheet_type": "glossary",
            "description": "Abbreviations and terms used in the report",
        },
        {
            "sheet_index": 3,
            "sheet_type": "about_this_report",
            "description": "Report purpose and overview",
        },
        {
            "sheet_index": 4,
            "sheet_type": "change_thresholds",
            "description": "ACO's estimated CI/SEP change thresholds for claims-based measures",
            "header_row": 7,
            "data_start_row": 8,
        },
        {
            "sheet_index": 5,
            "sheet_type": "how_to_use_report",
            "description": "Instructions and examples for interpreting the report",
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=265,
    file_pattern="D????.ECCTR.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class EstimatedCisepChangeThresholdReport:
    """
    Estimated CI/SEP Change Threshold Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - EstimatedCisepChangeThresholdReport.schema_name() -> str
        - EstimatedCisepChangeThresholdReport.schema_metadata() -> dict
        - EstimatedCisepChangeThresholdReport.parser_config() -> dict
        - EstimatedCisepChangeThresholdReport.transform_config() -> dict
        - EstimatedCisepChangeThresholdReport.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "EstimatedCisepChangeThresholdReport":
        """Create instance from dictionary."""
        return cls(**data)
