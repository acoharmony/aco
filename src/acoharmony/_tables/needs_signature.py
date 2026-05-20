# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for needs_signature schema.

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
    with_storage,
)
from acoharmony._validators.field_validators import (
    MBI,
    mbi_validator,
)


@register_schema(name="needs_signature", version=2, tier="silver", description="""\2""")
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class NeedsSignature:
    """
    Schema for needs_signature dataset

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - NeedsSignature.schema_name() -> str
        - NeedsSignature.schema_metadata() -> dict
        - NeedsSignature.parser_config() -> dict
        - NeedsSignature.transform_config() -> dict
        - NeedsSignature.lineage_config() -> dict
    """

    bene_mbi: str | None = MBI(
        default=None,
        description="bene_mbi field",
    )
    bene_first_name: str | None = Field(default=None, description="bene_first_name field")
    bene_last_name: str | None = Field(default=None, description="bene_last_name field")
    start_date: date | None = Field(default=None, description="start_date field")
    end_date: date | None = Field(default=None, description="end_date field")
    signature_date: date | None = Field(default=None, description="signature_date field")
    bene_sex_cd: str | None = Field(default=None, description="bene_sex_cd field")
    bene_birth_dt: date | None = Field(default=None, description="bene_birth_dt field")
    death_date: date | None = Field(default=None, description="death_date field")
    master_id: int | None = Field(default=None, description="master_id field")

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi = mbi_validator("bene_mbi")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "NeedsSignature":
        """Create instance from dictionary."""
        return cls(**data)
