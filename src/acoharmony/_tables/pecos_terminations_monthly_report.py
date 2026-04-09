# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for pecos_terminations_monthly_report schema.

Generated from: _schemas/pecos_terminations_monthly_report.yml

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
    with_storage,
    with_transform,
)


@register_schema(
    name="pecos_terminations_monthly_report",
    version=2,
    tier="bronze",
    description="PECOS Terminations Monthly Report",
    file_patterns={"reach": ["P.D*.PECOSTRMN.RP.D*.T*.xlsx"]},
)
@with_parser(type="excel", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["P.D*.PECOSTRMN.RP.D*.T*.xlsx"]},
    silver={
        "output_name": "pecos_terminations_monthly_report.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_four_icli(
    category="Reports",
    file_type_code=298,
    file_pattern="P.D????.PECOSTRMN.RP.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class PecosTerminationsMonthlyReport:
    """
    PECOS Terminations Monthly Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - PecosTerminationsMonthlyReport.schema_name() -> str
        - PecosTerminationsMonthlyReport.schema_metadata() -> dict
        - PecosTerminationsMonthlyReport.parser_config() -> dict
        - PecosTerminationsMonthlyReport.transform_config() -> dict
        - PecosTerminationsMonthlyReport.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PecosTerminationsMonthlyReport":
        """Create instance from dictionary."""
        return cls(**data)
