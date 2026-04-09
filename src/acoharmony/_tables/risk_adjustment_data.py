# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for risk_adjustment_data schema.

Generated from: _schemas/risk_adjustment_data.yml

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
    name="risk_adjustment_data",
    version=2,
    tier="bronze",
    description="Risk Adjustment Data",
    file_patterns={"reach": ["P.D*.RAP5V*.D*.T*.csv", "P.D*.RAP3V*.D*.T*.csv"]},
)
@with_parser(
    type="csv", delimiter="|", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["P.D*.RAP5V*.D*.T*.csv", "P.D*.RAP3V*.D*.T*.csv"]},
    silver={"output_name": "risk_adjustment_data.parquet", "refresh_frequency": "monthly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=140,
    file_pattern="P.D????.RAP?V?.D??????.T*.csv",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class RiskAdjustmentData:
    """
    Risk Adjustment Data

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - RiskAdjustmentData.schema_name() -> str
        - RiskAdjustmentData.schema_metadata() -> dict
        - RiskAdjustmentData.parser_config() -> dict
        - RiskAdjustmentData.transform_config() -> dict
        - RiskAdjustmentData.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "RiskAdjustmentData":
        """Create instance from dictionary."""
        return cls(**data)
