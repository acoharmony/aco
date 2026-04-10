# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for enrollment schema.

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
    with_staging,
    with_storage,
)
from acoharmony._validators.field_validators import (
    ZIP5,
    zip5_validator,
)


@register_schema(name="enrollment", version=2, tier="silver", description="""\2""")
@with_storage(tier="silver", medallion_layer="silver", gold={"output_name": "enrollment.parquet"})
@with_staging(source="beneficiary_demographics")
@dataclass
class Enrollment:
    """
    Custom enrollment input combining beneficiary demographics with assignment periods

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Enrollment.schema_name() -> str
        - Enrollment.schema_metadata() -> dict
        - Enrollment.parser_config() -> dict
        - Enrollment.transform_config() -> dict
        - Enrollment.lineage_config() -> dict
    """

    member_id: str = Field(description="Medicare Beneficiary Identifier")
    person_id: str = Field(description="Person identifier (same as member_id)")
    enrollment_start_date: date = Field(description="Start date of enrollment period")
    enrollment_end_date: date = Field(description="End date of enrollment period")
    payer: str = Field(default="Medicare", description="Payer name")
    payer_type: str | None = Field(default="Medicare", description="Type of payer")
    plan: str | None = Field(default="ACO", description="Insurance plan")
    gender: str | None = Field(default=None, description="Member gender")
    birth_date: date | None = Field(default=None, description="Member birth date")
    death_date: date | None = Field(default=None, description="Member death date (if applicable)")
    race: str | None = Field(default=None, description="Member race")
    ethnicity: str | None = Field(default=None, description="Member ethnicity")
    state: str | None = Field(default=None, description="State code")
    zip_code: str | None = ZIP5(default=None, description="ZIP code")
    county: str | None = Field(default=None, description="County code")
    dual_status_code: str | None = Field(
        default=None, description="Medicare/Medicaid dual eligibility status"
    )
    medicare_status_code: str | None = Field(default=None, description="Medicare enrollment status")
    original_reason_entitlement_code: str | None = Field(
        default=None, description="Original reason for Medicare entitlement"
    )
    program_type: str | None = Field(
        default=None, description="ACO program type (MSSP, REACH, etc.)"
    )
    assignment_method: str | None = Field(
        default=None, description="How member was assigned (claims-based, voluntary, etc.)"
    )
    data_source: str | None = Field(default="CCLF", description="Source of the data")
    processed_date: date | None = Field(default=None, description="Date record was processed")

    # Field Validators (from centralized _validators module)
    _validate_zip_code = zip5_validator("zip_code")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Enrollment":
        """Create instance from dictionary."""
        return cls(**data)
