# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for sva schema.

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
    with_four_icli,
    with_parser,
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


@register_schema(
    name="sva",
    version=2,
    tier="bronze",
    description="Shared Voluntary Alignment (SVA) submission file for beneficiary voluntary alignment",
    file_patterns={"reach": ["*SVA*.xlsx"]},
)
@with_parser(type="excel", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*SVA*.xlsx"]},
    medallion_layer="bronze",
    silver={
        "output_name": "sva.parquet",
        "refresh_frequency": "quarterly",
        "last_updated_by": "aco transform sva",
    },
)
@with_four_icli(
    category="Beneficiary List",
    file_type_code=None,
    file_pattern="*SVA*.xlsx",
    extract_zip=False,
    refresh_frequency="quarterly",
    default_date_filter={"createdWithinLastQuarter": True},
)
@dataclass
class Sva:
    """
    Shared Voluntary Alignment (SVA) submission file for beneficiary voluntary alignment

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Sva.schema_name() -> str
        - Sva.schema_metadata() -> dict
        - Sva.parser_config() -> dict
        - Sva.transform_config() -> dict
        - Sva.lineage_config() -> dict
    """

    aco_id: str = Field(description="ACO Identifier", alias="ACO ID")
    beneficiary_s_mbi: str = MBI(
        alias="Beneficiary_s_MBI",
        description="Medicare Beneficiary Identifier",
    )
    beneficiary_s_first_name: str | None = Field(
        alias="Beneficiary_s_First_Name",
    )
    beneficiary_s_last_name: str | None = Field(
        alias="Beneficiary_s_Last_Name",
    )
    beneficiary_s_street_address: str | None = Field(
        alias="Beneficiary_s_Street_Address",
    )
    city: str | None = Field(alias="City", default=None, description="City")
    state: str | None = Field(alias="State", default=None, description="State code")
    zip: str | None = ZIP5(alias="Zip", default=None, description="ZIP code")
    provider_name_primary_place_the_beneficiary_receives_care_as_it_appears_on_the_signed_sva_letter: (
        str | None
    ) = Field(
        default=None,
        description="Provider name from SVA letter",
        alias="Provider Name/Primary place the Beneficiary receives care (as it appears on the signed SVA letter)",
    )
    name_of_individual_participant_provider_associated_w_attestation: str | None = Field(
        alias="Name_of_Individual__Participant_Provider_associated_w__attestation",
        default=None,
        description="DC Participant Provider name",
    )
    i_npi_for_individual_participant_provider_column_j: str = NPI(
        alias="iNPI_for_Individual__Participant_Provider__column_J_",
        description="DC Participant Provider NPI",
    )
    tin_for_individual_participant_provider_column_j: str | None = TIN(
        alias="TIN_for_Individual_Participant_Provider__column_J_",
        default=None,
        description="DC Participant Provider TIN",
    )
    signature_date_on_sva_letter: date | None = Field(
        alias="Signature_Date_on_SVA_letter",
        default=None,
        description="Date of signature on SVA letter",
    )
    response_code_cms_to_fill_out: str | None = Field(
        alias="Response_Code__CMS_to_fill_out_",
    )

    # Field Validators (from centralized _validators module)
    _validate_beneficiary_s_mbi = mbi_validator("beneficiary_s_mbi")
    _validate_zip = zip5_validator("zip")
    _validate_i_npi_for_individual_participant_provider_column_j = npi_validator(
        "i_npi_for_individual_participant_provider_column_j"
    )
    _validate_tin_for_individual_participant_provider_column_j = tin_validator(
        "tin_for_individual_participant_provider_column_j"
    )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Sva":
        """Create instance from dictionary."""
        return cls(**data)
