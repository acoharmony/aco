# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for engagement schema.

Generated from: _schemas/engagement.yml

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
    with_keys,
    with_parser,
    with_polars,
    with_standardization,
    with_storage,
    with_transform,
)


@register_schema(name="engagement", version=2, tier="silver", description="""\2""")
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="silver",
    medallion_layer="silver",
    silver={"output_name": "engagement.parquet", "refresh_frequency": "weekly"},
    gold={"output_name": None, "refresh_frequency": None, "last_updated_by": None},
)
@with_standardization(
    add_columns=[
        {"name": "source_file", "value": "engagement"},
    ],
)
@with_keys(
    primary_key=["mrn", "monthyear", "engagement_type", "engagement_channel"],
    natural_key=["mrn", "monthyear"],
    deduplication_key=["mrn", "monthyear", "engagement_type", "engagement_channel"],
    foreign_keys=[
        {"column": "mrn", "references": "hcmpi_master.identifier"},
    ],
)
@with_polars(
    lazy_evaluation=True,
    string_trim=True,
    categorical_columns=["engagement_type", "engagement_channel"],
)
@dataclass
class Engagement:
    """
    Patient engagement tracking data from EDW

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Engagement.schema_name() -> str
        - Engagement.schema_metadata() -> dict
        - Engagement.parser_config() -> dict
        - Engagement.transform_config() -> dict
        - Engagement.lineage_config() -> dict
    """

    mrn: str = Field(alias="MRN", description="Medical Record Number")
    monthyear: str = Field(description="Month and year of engagement")
    em_touchpoints: int | None = Field(
        alias="EM_TouchPoints", default=None, description="E&M Touch Points count"
    )
    engagement_type: str | None = Field(default=None, description="Type of engagement")
    engagement_channel: str | None = Field(
        default=None, description="Channel of engagement (phone, portal, etc.)"
    )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Engagement":
        """Create instance from dictionary."""
        return cls(**data)
