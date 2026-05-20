# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for alternative_payment_arrangement_report schema.

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
)


@register_schema(
    name="alternative_payment_arrangement_report",
    version=2,
    tier="bronze",
    description="""Alternative Payment Arrangement Report""",
    file_patterns={"reach": ["*ALTPR*"]},
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*ALTPR*"]},
    silver={
        "output_name": "alternative_payment_arrangement_report.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_sheets(
    sheets=[
        {
            "sheet_index": 0,
            "sheet_type": "report_parameters",
            "description": "Report parameters and configuration",
        },
        {
            "sheet_index": 1,
            "sheet_type": "payment_history",
            "description": "Historical payment data",
            "header_row": 5,
            "data_start_row": 6,
        },
        {
            "sheet_index": 2,
            "sheet_type": "base_pcc_detailed",
            "description": "Base PCC payment details",
            "header_row": 7,
            "data_start_row": 8,
        },
        {
            "sheet_index": 3,
            "sheet_type": "enhanced_pcc_detailed",
            "description": "Enhanced PCC payment details",
            "header_row": 7,
            "data_start_row": 8,
        },
        {
            "sheet_index": 4,
            "sheet_type": "apo_detailed",
            "description": "APO payment details",
            "header_row": 7,
            "data_start_row": 8,
        },
        {
            "sheet_index": 5,
            "sheet_type": "data_claims_prvdr",
            "description": "Provider claims data",
            "skip_rows": 0,
        },
        {
            "sheet_index": 6,
            "sheet_type": "benchmark_rebasing",
            "description": "Benchmark rebasing data",
            "skip_rows": 0,
        },
        {
            "sheet_index": 7,
            "sheet_type": "risk_scores",
            "description": "Risk score data",
            "skip_rows": 0,
        },
        {
            "sheet_index": 8,
            "sheet_type": "claims_truncation",
            "description": "Claims truncation data",
            "skip_rows": 0,
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=216,
    file_pattern="REACH.D????.ALTPR.PY????.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class AlternativePaymentArrangementReport:
    """
    Alternative Payment Arrangement Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - AlternativePaymentArrangementReport.schema_name() -> str
        - AlternativePaymentArrangementReport.schema_metadata() -> dict
        - AlternativePaymentArrangementReport.parser_config() -> dict
        - AlternativePaymentArrangementReport.transform_config() -> dict
        - AlternativePaymentArrangementReport.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AlternativePaymentArrangementReport":
        """Create instance from dictionary."""
        return cls(**data)
