# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for vwyearmo_engagement schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from datetime import date

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
)


@register_schema(name="vwyearmo_engagement", version=2, tier="bronze", description="""\2""")
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@dataclass
class VwyearmoEngagement:
    """
    Year-month view of patient engagement data

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - VwyearmoEngagement.schema_name() -> str
        - VwyearmoEngagement.schema_metadata() -> dict
        - VwyearmoEngagement.parser_config() -> dict
        - VwyearmoEngagement.transform_config() -> dict
        - VwyearmoEngagement.lineage_config() -> dict
    """

    mrn: str = Field(alias="MRN", description="Medical Record Number")
    monthyear: str | None = Field(default=None, description="Month and year")
    em_touch_points: int | None = Field(
        alias="EM_TouchPoints", default=None, description="E&M Touch Points count"
    )
    engagement_type: str | None = Field(default=None, description="Type of engagement")
    engagement_channel: str | None = Field(default=None, description="Channel of engagement")
    yearmo: str = Field(description="Year-month in YYYYMM format")
    patient_id: str | None = Field(default=None, description="Patient identifier")
    program_id: str | None = Field(default=None, description="Program identifier")
    engagement_count: int | None = Field(default=None, description="Total engagement count", ge=0)
    first_engagement_date: date | None = Field(
        default=None, description="First engagement date in period"
    )
    last_engagement_date: date | None = Field(
        default=None, description="Last engagement date in period"
    )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "VwyearmoEngagement":
        """Create instance from dictionary."""
        return cls(**data)
