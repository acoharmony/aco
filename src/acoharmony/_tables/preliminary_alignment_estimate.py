# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for preliminary_alignment_estimate schema.

Generated from: _schemas/preliminary_alignment_estimate.yml

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
    with_storage,
    with_transform,
)


@register_schema(
    name="preliminary_alignment_estimate",
    version=2,
    tier="bronze",
    description="Preliminary Alignment Estimate Report",
    file_patterns={"reach": ["*PAER*"]},
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*PAER*"]},
    silver={
        "output_name": "preliminary_alignment_estimate.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_four_icli(
    category="Reports",
    file_type_code=221,
    file_pattern="REACH.D????.PAER.PY????.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class PreliminaryAlignmentEstimate:
    """
    Preliminary Alignment Estimate Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - PreliminaryAlignmentEstimate.schema_name() -> str
        - PreliminaryAlignmentEstimate.schema_metadata() -> dict
        - PreliminaryAlignmentEstimate.parser_config() -> dict
        - PreliminaryAlignmentEstimate.transform_config() -> dict
        - PreliminaryAlignmentEstimate.lineage_config() -> dict
    """

    alignment_category: str | None = Field(
        default=None,
        description="Alignment category (Total, Claims-aligned only, Voluntarily-aligned only, Claims- and voluntarily aligned)",
    )
    beneficiary_count: str | None = Field(
        default=None,
        description="Number of beneficiaries in this alignment category (may be 'SUPPRESSED' if 10 or fewer)",
    )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PreliminaryAlignmentEstimate":
        """Create instance from dictionary."""
        return cls(**data)
