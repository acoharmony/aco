# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for zip_to_county schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from decimal import Decimal

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)
from acoharmony._validators.field_validators import (
    ZIP5,
    zip5_validator,
)


@register_schema(name="zip_to_county", version=2, tier="bronze", description="""\2""")
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    medallion_layer="bronze",
    bronze={"output_name": "zip_to_county.parquet"},
    silver={"output_name": "zip_to_county.parquet"},
    gold={"output_name": "zip_to_county.parquet"},
)
@dataclass
class ZipToCounty:
    """
    ZIP code to county mapping with geographic coordinates

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - ZipToCounty.schema_name() -> str
        - ZipToCounty.schema_metadata() -> dict
        - ZipToCounty.parser_config() -> dict
        - ZipToCounty.transform_config() -> dict
        - ZipToCounty.lineage_config() -> dict
    """

    zip_code: str = ZIP5(description="5-digit ZIP code")
    county_name: str = Field(description="County name")
    county_fips: str = Field(description="County FIPS code")
    state_code: str = Field(description="State abbreviation")
    state_name: str | None = Field(default=None, description="State full name")
    latitude: Decimal | None = Field(default=None, description="Latitude coordinate")
    longitude: Decimal | None = Field(default=None, description="Longitude coordinate")

    # Field Validators (from centralized _validators module)
    _validate_zip_code = zip5_validator("zip_code")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ZipToCounty":
        """Create instance from dictionary."""
        return cls(**data)
