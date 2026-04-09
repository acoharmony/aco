# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for pbvar schema.

Generated from: _schemas/pbvar.yml

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
    with_transform,
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
    name="pbvar",
    version=2,
    tier="bronze",
    description="Part B Volume and Risk Report for physician services",
    file_patterns={"reach": ["*PBVAR*.xlsx"]},
)
@with_parser(type="excel", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*PBVAR*.xlsx"]},
    medallion_layer="bronze",
    silver={
        "output_name": "pbvar.parquet",
        "refresh_frequency": "quarterly",
        "last_updated_by": "aco transform pbvar",
    },
)
@with_four_icli(
    category="Beneficiary List",
    file_type_code=175,
    file_pattern="P.D????.PBVAR.D??????.T0112000.xlsx",
    extract_zip=False,
    refresh_frequency="quarterly",
    default_date_filter={"createdWithinLastQuarter": True},
)
@dataclass
class Pbvar:
    """
    Part B Volume and Risk Report for physician services

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Pbvar.schema_name() -> str
        - Pbvar.schema_metadata() -> dict
        - Pbvar.parser_config() -> dict
        - Pbvar.transform_config() -> dict
        - Pbvar.lineage_config() -> dict
    """

    aco_id: str = MBI(alias="ACO_ID", description="ACO Identifier")
    sva_response_code_list: str | None = NPI(
        alias="RESPONSE_CODE_LIST", default=None, description="Response code list"
    )
    id_received: str | None = TIN(alias="ID_RECEIVED", default=None, description="ID received")
    bene_mbi: str = ZIP5(
        alias="BENE_MBI",
        description="Medicare Beneficiary Identifier",
    )
    bene_first_name: str | None = Field(
        alias="BENE_FIRST_NAME", default=None, description="Beneficiary First Name"
    )
    bene_last_name: str | None = Field(
        alias="BENE_LAST_NAME", default=None, description="Beneficiary Last Name"
    )
    bene_line_1_address: str | None = Field(
        alias="BENE_LINE_1_ADDRESS", default=None, description="Beneficiary Address Line 1"
    )
    bene_line_2_address: str | None = Field(
        alias="BENE_LINE_2_ADDRESS", default=None, description="Beneficiary Address Line 2"
    )
    bene_city: str | None = Field(alias="BENE_CITY", default=None, description="Beneficiary City")
    bene_state: str | None = Field(
        alias="BENE_STATE", default=None, description="Beneficiary State"
    )
    bene_zipcode: str | None = Field(
        alias="BENE_ZIPCODE", default=None, description="Beneficiary ZIP Code"
    )
    provider_name: str | None = Field(
        alias="PROVIDER_NAME", default=None, description="Provider Organization Name"
    )
    practitioner_name: str | None = Field(
        alias="PRACTITIONER_NAME", default=None, description="Practitioner Name"
    )
    sva_npi: str = NPI(alias="IND_NPI", description="Individual Provider NPI")
    sva_tin: str | None = Field(
        alias="IND_TIN", default=None, description="Individual Provider TIN"
    )
    sva_signature_date: date | None = Field(
        alias="SIGNATURE_DATE", default=None, description="Signature Date"
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi = mbi_validator("bene_mbi")
    _validate_bene_zipcode = zip5_validator("bene_zipcode")
    _validate_provider_name = npi_validator("provider_name")
    _validate_sva_npi = npi_validator("sva_npi")
    _validate_sva_tin = tin_validator("sva_tin")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Pbvar":
        """Create instance from dictionary."""
        return cls(**data)
