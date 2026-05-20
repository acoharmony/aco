# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for eligibility schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from datetime import date
from decimal import Decimal

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_storage,
)
from acoharmony._validators.field_validators import (
    ZIP5,
    zip5_validator,
)


@register_schema(name="eligibility", version=2, tier="gold", description="""\2""")
@with_storage(tier="gold", medallion_layer="gold")
@dataclass
class Eligibility:
    """
    Member eligibility and enrollment spans

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Eligibility.schema_name() -> str
        - Eligibility.schema_metadata() -> dict
        - Eligibility.parser_config() -> dict
        - Eligibility.transform_config() -> dict
        - Eligibility.lineage_config() -> dict
    """

    person_id: str = Field(description="Person identifier")
    member_id: str = Field(description="Member identifier (Medicare Beneficiary Identifier)")
    enrollment_start_date: date = Field(description="Start date of enrollment period")
    enrollment_end_date: date = Field(description="End date of enrollment period")
    payer: str = Field(default="Medicare", description="Payer name")
    payer_type: str | None = Field(default="Medicare", description="Type of payer")
    plan: str | None = Field(default="ACO", description="Insurance plan")
    gender: str | None = Field(default=None, description="Member gender")
    gender_name: str | None = Field(default=None, description="Gender description")
    birth_date: date | None = Field(default=None, description="Member birth date")
    death_date: date | None = Field(default=None, description="Member death date (if applicable)")
    death_flag: int | None = Field(default=None, description="Indicates if member is deceased")
    race: str | None = Field(default=None, description="Member race")
    race_name: str | None = Field(default=None, description="Race description")
    ethnicity: str | None = Field(default=None, description="Member ethnicity")
    ethnicity_name: str | None = Field(default=None, description="Ethnicity description")
    state: str | None = Field(default=None, description="State code")
    state_name: str | None = Field(default=None, description="State name")
    zip_code: str | None = ZIP5(default=None, description="ZIP code")
    county: str | None = Field(default=None, description="County code")
    county_name: str | None = Field(default=None, description="County name")
    latitude: Decimal | None = Field(default=None, description="Geographic latitude")
    longitude: Decimal | None = Field(default=None, description="Geographic longitude")
    dual_status_code: str | None = Field(
        default=None, description="Medicare/Medicaid dual eligibility status"
    )
    dual_status_description: str | None = Field(default=None, description="Dual status description")
    medicare_status_code: str | None = Field(default=None, description="Medicare enrollment status")
    medicare_status_description: str | None = Field(
        default=None, description="Medicare status description"
    )
    original_reason_entitlement_code: str | None = Field(
        default=None, description="Original reason for Medicare entitlement"
    )
    original_reason_entitlement_description: str | None = Field(
        default=None, description="Original reason description"
    )
    mssp_enrolled: bool | None = Field(default=None, description="Currently enrolled in MSSP")
    reach_enrolled: bool | None = Field(default=None, description="Currently enrolled in REACH")
    current_program: str | None = Field(default=None, description="Current ACO program")
    data_source: str | None = Field(default="CCLF", description="Source of the data")
    processed_date: date | None = Field(default=None, description="Date record was processed")

    # Field Validators (from centralized _validators module)
    _validate_zip_code = zip5_validator("zip_code")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Eligibility":
        """Create instance from dictionary."""
        return cls(**data)
