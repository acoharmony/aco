# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for bnex schema.

Generated from: _schemas/bnex.yml
"""

from datetime import date

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
    with_transform,
)


@register_schema(
    name="bnex",
    version=2,
    tier="bronze",
    description="Beneficiary Data Sharing Opt-Out Files (BNEX) - XML file containing beneficiaries who have opted out of data sharing under MSSP",
    file_patterns={"mssp": ["P.A*.BNEX.Y*.D*.T*.xml"]},
)
@with_parser(type="xml", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"mssp": ["P.A*.BNEX.Y*.D*.T*.xml"]},
    silver={
        "output_name": "bnex.parquet",
        "refresh_frequency": "monthly",
        "last_updated_by": "aco transform bnex",
    },
    gold={"output_name": None, "refresh_frequency": None, "last_updated_by": None},
)
@dataclass
class Bnex:
    """
    Beneficiary Data Sharing Opt-Out Files (BNEX) - XML file containing beneficiaries who have opted out of data sharing under MSSP
    """

    mbi: str = Field(description="Medicare Beneficiary Identifier")
    firstname: str = Field(description="Beneficiary first name")
    lastname: str = Field(description="Beneficiary last name")
    dob: date | None = Field(description="Date of birth in YYYYMMDD format")
    gender: str = Field(description="Beneficiary gender (M or F)")
    beneexcreason: str = Field(
        description="Beneficiary exclusion reason code (PC=Patient Choice, BD=Beneficiary Death, etc.)"
    )
    hicn: str | None = Field(
        default=None, description="Health Insurance Claim Number (legacy identifier, often empty)"
    )
    middlename: str | None = Field(
        default=None, description="Beneficiary middle name or initial (can be empty)"
    )
