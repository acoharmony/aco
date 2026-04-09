# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for palmr schema.

Generated from: _schemas/palmr.yml

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
    with_four_icli,
    with_parser,
    with_storage,
    with_transform,
)
from acoharmony._validators.field_validators import (
    MBI,
    NPI,
    TIN,
    mbi_validator,
    npi_validator,
    tin_validator,
)


@register_schema(
    name="palmr",
    version=2,
    tier="bronze",
    description="Part A Line Monthly Report with claim line details",
    file_patterns={"reach": ["*PALMR*.csv"]},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*PALMR*.csv"]},
    medallion_layer="bronze",
    silver={
        "output_name": "palmr.parquet",
        "refresh_frequency": "quarterly",
        "last_updated_by": "aco transform palmr",
    },
)
@with_four_icli(
    category="Beneficiary List",
    file_type_code=165,
    file_pattern="P.D????.PALMR.D??????.T*.csv",
    extract_zip=False,
    refresh_frequency="quarterly",
    default_date_filter={"createdWithinLastQuarter": True},
)
@dataclass
class Palmr:
    """
    Part A Line Monthly Report with claim line details

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Palmr.schema_name() -> str
        - Palmr.schema_metadata() -> dict
        - Palmr.parser_config() -> dict
        - Palmr.transform_config() -> dict
        - Palmr.lineage_config() -> dict
    """

    aco_id: str = MBI(alias="ACO_ID", description="ACO Identifier")
    bene_mbi: str = NPI(
        alias="MBI_ID",
        description="Medicare Beneficiary Identifier",
    )
    algn_type_clm: str | None = TIN(
        alias="ALGN_TYPE_CLM", default=None, description="Alignment Type - Claims Based"
    )
    algn_type_va: str | None = Field(
        alias="ALGN_TYPE_VA", default=None, description="Alignment Type - Voluntary Alignment"
    )
    prvdr_tin__clm_or_va_: str | None = Field(
        alias="PRVDR_TIN__CLM_OR_VA_",
        default=None,
        description="Provider TIN (Claims or Voluntary)",
    )
    prvdr_npi__clm_or_va_: str | None = Field(
        alias="PRVDR_NPI__CLM_OR_VA_",
        default=None,
        description="Provider NPI (Claims or Voluntary)",
    )
    fac_prvdr_oscar_num: str | None = Field(
        alias="FAC_PRVDR_OSCAR_NUM", default=None, description="Facility Provider OSCAR Number"
    )
    qem_allowed_primary_ay1: str | None = Field(
        alias="QEM_ALLOWED_PRIMARY_AY1",
        default=None,
        description="QEM Allowed Primary Care Alignment Year 1",
    )
    qem_allowed_nonprimary_ay1: str | None = Field(
        alias="QEM_ALLOWED_NONPRIMARY_AY1",
        default=None,
        description="QEM Allowed Non-Primary Care Alignment Year 1",
    )
    qem_allowed_other_ay1: str | None = Field(
        alias="QEM_ALLOWED_OTHER_AY1",
        default=None,
        description="QEM Allowed Other Alignment Year 1",
    )
    qem_allowed_primary_ay2: str | None = Field(
        alias="QEM_ALLOWED_PRIMARY_AY2",
        default=None,
        description="QEM Allowed Primary Care Alignment Year 2",
    )
    qem_allowed_nonprimary_ay2: str | None = Field(
        alias="QEM_ALLOWED_NONPRIMARY_AY2",
        default=None,
        description="QEM Allowed Non-Primary Care Alignment Year 2",
    )
    qem_allowed_other_ay2: str | None = Field(
        alias="QEM_ALLOWED_OTHER_AY2",
        default=None,
        description="QEM Allowed Other Alignment Year 2",
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi = mbi_validator("bene_mbi")
    _validate_prvdr_tin__clm_or_va_ = tin_validator("prvdr_tin__clm_or_va_")
    _validate_prvdr_npi__clm_or_va_ = npi_validator("prvdr_npi__clm_or_va_")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Palmr":
        """Create instance from dictionary."""
        return cls(**data)
