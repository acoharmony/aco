# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for beneficiary_demographics schema.

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
    ZIP5,
    mbi_validator,
    zip5_validator,
)


@register_schema(name="beneficiary_demographics", version=2, tier="silver", description="""\2""")
@with_storage(
    tier="silver",
    medallion_layer="silver",
    gold={"output_name": "beneficiary_demographics.parquet"},
)
@dataclass
class BeneficiaryDemographics:
    """
    Beneficiary demographics from CCLF8

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - BeneficiaryDemographics.schema_name() -> str
        - BeneficiaryDemographics.schema_metadata() -> dict
        - BeneficiaryDemographics.parser_config() -> dict
        - BeneficiaryDemographics.transform_config() -> dict
        - BeneficiaryDemographics.lineage_config() -> dict
    """

    bene_mbi_id: str = MBI(
        description="Medicare Beneficiary Identifier",
    )
    bene_fips_state_cd: str | None = Field(default=None, description="FIPS state code")
    bene_fips_cnty_cd: str | None = Field(default=None, description="FIPS county code")
    bene_zip_cd: str | None = ZIP5(default=None, description="ZIP code")
    bene_dob: date | None = Field(default=None, description="Date of birth")
    bene_sex_cd: str | None = Field(default=None, description="Sex code")
    bene_race_cd: str | None = Field(default=None, description="Race code")
    bene_age: str | None = Field(default=None, description="Age")
    bene_mdcr_stus_cd: str | None = Field(default=None, description="Medicare status code")
    bene_dual_stus_cd: str | None = Field(default=None, description="Dual status code")
    bene_death_dt: date | None = Field(default=None, description="Date of death")
    bene_rng_bgn_dt: date | None = Field(default=None, description="Range begin date")
    bene_rng_end_dt: date | None = Field(default=None, description="Range end date")
    bene_fst_name: str | None = Field(default=None, description="First name")
    bene_mdl_name: str | None = Field(default=None, description="Middle name")
    bene_lst_name: str | None = Field(default=None, description="Last name")
    bene_orgnl_entlmt_rsn_cd: str | None = Field(
        default=None, description="Original entitlement reason code"
    )
    bene_entlmt_buyin_ind: str | None = Field(
        default=None, description="Entitlement buy-in indicator"
    )
    bene_part_a_enrlmt_bgn_dt: date | None = Field(
        default=None, description="Part A enrollment begin date"
    )
    bene_part_b_enrlmt_bgn_dt: date | None = Field(
        default=None, description="Part B enrollment begin date"
    )
    bene_line_1_adr: str | None = Field(default=None, description="Address line 1")
    bene_line_2_adr: str | None = Field(default=None, description="Address line 2")
    bene_line_3_adr: str | None = Field(default=None, description="Address line 3")
    bene_line_4_adr: str | None = Field(default=None, description="Address line 4")
    bene_line_5_adr: str | None = Field(default=None, description="Address line 5")
    bene_line_6_adr: str | None = Field(default=None, description="Address line 6")
    bene_city: str | None = Field(default=None, description="City")
    bene_state: str | None = Field(default=None, description="State")
    bene_zip: str | None = ZIP5(default=None, description="ZIP code")
    current_bene_mbi_id: str | None = Field(
        default=None,
        description="Current MBI after crosswalk",
    )
    file_date: date | None = Field(description="File date from source")

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")
    _validate_bene_zip_cd = zip5_validator("bene_zip_cd")
    _validate_bene_zip = zip5_validator("bene_zip")
    _validate_current_bene_mbi_id = mbi_validator("current_bene_mbi_id")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "BeneficiaryDemographics":
        """Create instance from dictionary."""
        return cls(**data)
