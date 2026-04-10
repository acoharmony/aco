# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for office_zip schema.

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
    with_parser,
    with_storage,
)
from acoharmony._validators.field_validators import (
    ZIP5,
    zip5_validator,
)


@register_schema(
    name="office_zip",
    version=2,
    tier="bronze",
    description="Office ZIP code to market mapping for geographic service area assignment from vwzipoffice_br.csv",
    file_patterns={"default": "vwzipoffice_br.csv"},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"default": "vwzipoffice_br.csv"},
    medallion_layer="bronze",
    bronze={"output_name": "office_zip.parquet"},
    silver={
        "output_name": "office_zip.parquet",
        "refresh_frequency": "quarterly",
        "last_updated_by": "office_zip transform",
    },
)
@dataclass
class OfficeZip:
    """
    Office ZIP code to market mapping for geographic service area assignment from vwzipoffice_br.csv

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - OfficeZip.schema_name() -> str
        - OfficeZip.schema_metadata() -> dict
        - OfficeZip.parser_config() -> dict
        - OfficeZip.transform_config() -> dict
        - OfficeZip.lineage_config() -> dict
    """

    zip_code: str = ZIP5(alias="Zip", description="5-digit ZIP code")
    state: str = Field(alias="State", description="Two-letter state code")
    latitude: float = Field(alias="lat", description="Latitude coordinate")
    longitude: float = Field(alias="lng", description="Longitude coordinate")
    provider_code: str | None = Field(
        alias="Provcode", default=None, description="Provider code (e.g., OON for out-of-network)"
    )
    borderline_flag: str | None = Field(
        alias="Borderlineflag", default=None, description="Flag indicating borderline ZIP"
    )
    override_flag: str | None = Field(
        alias="OverrideFlag", default=None, description="Override flag (0 or 1)"
    )
    office_distance: float | None = Field(
        alias="OfficeDistance",
        default=None,
        description="Distance to assigned office (null if direct assignment)",
    )
    office_name: str | None = Field(
        alias="OfficeName", default=None, description="Name of assigned office location"
    )
    market: str | None = Field(
        alias="Market", default=None, description="Market or service area name"
    )
    region_id: str | None = Field(alias="RegionID", default=None, description="Region identifier")
    region_name: str | None = Field(alias="RegionName", default=None, description="Region name")
    subdivision_name: str | None = Field(
        alias="SubDivisionName", default=None, description="Subdivision name"
    )
    division_name: str | None = Field(
        alias="DivisionName", default=None, description="Division name"
    )
    rcmo_id: str | None = Field(alias="RCMOID", default=None, description="RCMO identifier")
    rcmo_name: str | None = Field(alias="RCMOName", default=None, description="RCMO name")
    meta_created_on: str | None = Field(
        alias="META_CreatedOn", default=None, description="Metadata creation timestamp"
    )

    # Field Validators (from centralized _validators module)
    _validate_zip_code = zip5_validator("zip_code")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "OfficeZip":
        """Create instance from dictionary."""
        return cls(**data)
