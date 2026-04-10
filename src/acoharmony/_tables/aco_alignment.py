# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for aco_alignment schema.

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
    NPI,
    TIN,
    ZIP5,
    mbi_validator,
    npi_validator,
    tin_validator,
    zip5_validator,
)


@register_schema(name="aco_alignment", version=2, tier="silver", description="""\2""")
@with_storage(
    tier="silver",
    medallion_layer="silver",
    gold={"output_name": "aco_alignment.parquet"},
)
@dataclass
class AcoAlignment:
    """
    Comprehensive ACO alignment tracking across all programs (REACH, MSSP) with temporal coverage

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - AcoAlignment.schema_name() -> str
        - AcoAlignment.schema_metadata() -> dict
        - AcoAlignment.parser_config() -> dict
        - AcoAlignment.transform_config() -> dict
        - AcoAlignment.lineage_config() -> dict
    """

    bene_mbi: str = MBI(
        description="Original Medicare Beneficiary Identifier",
    )
    current_mbi: str = MBI(
        description="Current MBI from crosswalk for consistent tracking",
    )
    hcmpi: str | None = MBI(
        default=None, description="Master patient identifier from enterprise crosswalk"
    )
    previous_mbi_count: str = NPI(
        description="Number of previous MBIs for this beneficiary",
    )
    birth_date: date | None = Field(default=None, description="Beneficiary date of birth")
    death_date: date | None = Field(
        default=None, description="Beneficiary date of death if applicable"
    )
    sex: str | None = Field(default=None, description="Beneficiary sex")
    race: str | None = Field(default=None, description="Beneficiary race")
    ethnicity: str | None = Field(default=None, description="Beneficiary ethnicity")
    state: str | None = Field(default=None, description="Beneficiary state of residence")
    county: str | None = Field(default=None, description="Beneficiary county")
    zip_code: str | None = Field(default=None, description="Beneficiary ZIP code")
    has_ffs_service: bool = Field(description="Whether beneficiary has any FFS service record")
    ffs_first_date: date | None = Field(
        default=None, description="First date of FFS service for this beneficiary"
    )
    ffs_claim_count: str | None = Field(default=None, description="Total number of FFS claims")
    ever_reach: bool = Field(description="Whether beneficiary was ever in REACH program")
    ever_mssp: bool = Field(description="Whether beneficiary was ever in MSSP program")
    months_in_reach: str = Field(description="Total months enrolled in REACH")
    months_in_mssp: str = Field(description="Total months enrolled in MSSP")
    first_reach_date: date | None = Field(
        default=None, description="First month beneficiary was in REACH"
    )
    last_reach_date: date | None = Field(
        default=None, description="Last month beneficiary was in REACH"
    )
    first_mssp_date: date | None = Field(
        default=None, description="First month beneficiary was in MSSP"
    )
    last_mssp_date: date | None = Field(
        default=None, description="Last month beneficiary was in MSSP"
    )
    current_program: str = Field(description="Current program enrollment (REACH, MSSP, or None)")
    current_aco_id: str | None = Field(default=None, description="Current ACO ID if enrolled")
    current_provider_tin: str | None = Field(default=None, description="Current provider TIN")
    continuous_enrollment: bool = Field(
        description="Whether beneficiary has continuous enrollment without gaps"
    )
    program_switches: str = Field(description="Number of times switched between programs")
    enrollment_gaps: str = Field(description="Number of months with gaps in enrollment")
    has_demographics: bool = Field(description="Whether demographics data is available")
    mbi_stability: str = Field(
        description="MBI stability indicator (Stable, Changed, Multiple)",
    )
    observable_start: date = Field(description="Start date of observable period")
    observable_end: date = Field(description="End date of observable period")
    processed_at: datetime = Field(description="Timestamp when this record was processed")

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi = mbi_validator("bene_mbi")
    _validate_current_mbi = mbi_validator("current_mbi")
    _validate_previous_mbi_count = mbi_validator("previous_mbi_count")
    _validate_zip_code = zip5_validator("zip_code")
    _validate_current_provider_tin = npi_validator("current_provider_tin")
    _validate_continuous_enrollment = tin_validator("continuous_enrollment")
    _validate_mbi_stability = mbi_validator("mbi_stability")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AcoAlignment":
        """Create instance from dictionary."""
        return cls(**data)
