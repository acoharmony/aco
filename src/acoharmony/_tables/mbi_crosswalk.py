# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for mbi_crosswalk schema.

Generated from: _schemas/mbi_crosswalk.yml

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
    with_standardization,
    with_storage,
    with_transform,
)


@register_schema(name="mbi_crosswalk", version=2, tier="silver", description="""\2""")
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(tier="silver", medallion_layer="silver")
@with_standardization(
    add_columns=[
        {"name": "source_file", "value": "mbi_crosswalk"},
    ],
)
@dataclass
class MbiCrosswalk:
    """
    Schema for mbi_crosswalk dataset

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - MbiCrosswalk.schema_name() -> str
        - MbiCrosswalk.schema_metadata() -> dict
        - MbiCrosswalk.parser_config() -> dict
        - MbiCrosswalk.transform_config() -> dict
        - MbiCrosswalk.lineage_config() -> dict
    """

    crnt_num: str = Field(description="Current MBI Number")
    prvs_num: str = Field(description="Previous MBI/HIC Number")
    prvs_id_efctv_dt: date | None = Field(default=None, description="Previous ID Effective Date")
    prvs_id_obsolete_dt: date | None = Field(default=None, description="Previous ID Obsolete Date")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "MbiCrosswalk":
        """Create instance from dictionary."""
        return cls(**data)
