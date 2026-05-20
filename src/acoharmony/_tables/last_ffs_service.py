# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for last_ffs_service schema.

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
    with_storage,
)
from acoharmony._validators.field_validators import (
    MBI,
    NPI,
    TIN,
    mbi_validator,
    npi_validator,
    tin_validator,
)


@register_schema(name="last_ffs_service", version=2, tier="silver", description="""\2""")
@with_storage(
    tier="silver",
    medallion_layer="silver",
    silver={
        "output_name": "last_ffs_service.parquet",
        "refresh_frequency": "continuous",
        "last_updated_by": "aco transform last_ffs_service",
    },
)
@dataclass
class LastFfsService:
    """
    Pre-computed most recent FFS service dates for beneficiaries with valid provider matches

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - LastFfsService.schema_name() -> str
        - LastFfsService.schema_metadata() -> dict
        - LastFfsService.parser_config() -> dict
        - LastFfsService.transform_config() -> dict
        - LastFfsService.lineage_config() -> dict
    """

    bene_mbi: str = MBI(
        description="Beneficiary Medicare Beneficiary Identifier",
    )
    last_ffs_date: date = Field(description="Most recent date of FFS service for this beneficiary")
    last_ffs_tin: str = TIN(description="Provider TIN from most recent FFS service")
    last_ffs_npi: str = Field(description="Provider NPI from most recent FFS service")
    claim_count: str = Field(
        description="Total number of claims for this beneficiary from valid providers"
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi = mbi_validator("bene_mbi")
    _validate_last_ffs_tin = tin_validator("last_ffs_tin")
    _validate_last_ffs_npi = npi_validator("last_ffs_npi")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "LastFfsService":
        """Create instance from dictionary."""
        return cls(**data)
