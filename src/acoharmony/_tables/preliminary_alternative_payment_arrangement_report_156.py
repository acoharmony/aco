# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for preliminary_alternative_payment_arrangement_report_156 schema.

Generated from: _schemas/preliminary_alternative_payment_arrangement_report_156.yml

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
    name="preliminary_alternative_payment_arrangement_report_156",
    version=2,
    tier="bronze",
    description="Preliminary Alternative Payment Arrangement Report",
    file_patterns={"reach": ["*ALPAR*"]},
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*ALPAR*"]},
    silver={
        "output_name": "preliminary_alternative_payment_arrangement_report_156.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_sheets(
    sheets=[
        {
            "sheet_index": 0,
            "sheet_type": "report_parameters",
            "description": "ACO parameters, report configuration, and performance period details",
        },
        {
            "sheet_index": 1,
            "sheet_type": "payment_history",
            "description": "Historical monthly payments for Base PCC, Enhanced PCC, and APO",
        },
        {
            "sheet_index": 2,
            "sheet_type": "base_pcc_detailed",
            "description": "Monthly base PCC payment calculations with projected alignment and experience",
        },
        {
            "sheet_index": 3,
            "sheet_type": "enhanced_pcc_detailed",
            "description": "Monthly enhanced PCC payment calculations with projected alignment and experience",
        },
        {
            "sheet_index": 4,
            "sheet_type": "base_pcc_pct",
            "description": "Base PCC percentage calculations by claim type and provider category",
        },
        {
            "sheet_index": 5,
            "sheet_type": "enhanced_pcc_pct_ceil",
            "description": "Enhanced PCC percentage ceiling calculations by claim type and provider category",
        },
        {
            "sheet_index": 6,
            "sheet_type": "apo_detailed",
            "description": "Monthly Alternative Payment Option payment calculations",
        },
        {
            "sheet_index": 7,
            "sheet_type": "apo_pbpm",
            "description": "APO PBPM calculations by claim type and provider category",
        },
        {
            "sheet_index": 8,
            "sheet_type": "data_claims_prvdr",
            "description": "Provider-level claims data with payment adjustments and reductions",
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=156,
    file_pattern="P.D????.ALPAR.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class PreliminaryAlternativePaymentArrangementReport156:
    """
    Preliminary Alternative Payment Arrangement Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - PreliminaryAlternativePaymentArrangementReport156.schema_name() -> str
        - PreliminaryAlternativePaymentArrangementReport156.schema_metadata() -> dict
        - PreliminaryAlternativePaymentArrangementReport156.parser_config() -> dict
        - PreliminaryAlternativePaymentArrangementReport156.transform_config() -> dict
        - PreliminaryAlternativePaymentArrangementReport156.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PreliminaryAlternativePaymentArrangementReport156":
        """Create instance from dictionary."""
        return cls(**data)
