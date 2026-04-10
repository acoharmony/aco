# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for ffs_first_dates schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from datetime import date, datetime

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_storage,
)
from acoharmony._validators.field_validators import (
    MBI,
    mbi_validator,
)


@register_schema(name="ffs_first_dates", version=2, tier="silver", description="""\2""")
@with_storage(
    tier="silver",
    medallion_layer="silver",
    gold={"output_name": "ffs_first_dates.parquet"},
)
@dataclass
class FfsFirstDates:
    """
    Pre-computed first FFS service dates for beneficiaries with valid provider matches

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - FfsFirstDates.schema_name() -> str
        - FfsFirstDates.schema_metadata() -> dict
        - FfsFirstDates.parser_config() -> dict
        - FfsFirstDates.transform_config() -> dict
        - FfsFirstDates.lineage_config() -> dict
    """

    bene_mbi: str = MBI(
        description="Beneficiary Medicare Beneficiary Identifier",
    )
    ffs_first_date: date = Field(description="First date of FFS service for this beneficiary")
    claim_count: str = Field(
        description="Total number of claims for this beneficiary from valid providers"
    )
    extracted_at: datetime = Field(description="Timestamp when this data was extracted")

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi = mbi_validator("bene_mbi")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "FfsFirstDates":
        """Create instance from dictionary."""
        return cls(**data)
