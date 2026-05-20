# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for gcm schema.

Generated from: _schemas/gcm.yml
"""

from datetime import date

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)


@register_schema(
    name="gcm",
    version=2,
    tier="bronze",
    description="Gift Card Management tracking for ACO patient incentive program based on AWV completion",
    file_patterns={"main": "gift_card_management*.csv"},
)
@with_parser(type="csv", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"main": "gift_card_management*.csv"},
    medallion_layer="bronze",
    silver={
        "output_name": "gcm.parquet",
        "refresh_frequency": "monthly",
        "last_updated_by": "aco transform gcm",
    },
    gold={"output_name": None, "refresh_frequency": None, "last_updated_by": None},
)
@dataclass
class Gcm:
    """
    Gift Card Management tracking for ACO patient incentive program based on AWV completion
    """

    total_count: int = Field(description="Total count of eligible patients")
    hcmpi: str = Field(description="Harmony Care Master Patient Index identifier")
    payer_current: str = Field(description="Current payer designation")
    payer: str = Field(description="Primary payer for the patient")
    roll12_awv_enc: int = Field(
        description="Rolling 12-month Annual Wellness Visit encounter count"
    )
    awv_status: str = Field(description="Status of Annual Wellness Visit completion")
    roll12_em: int = Field(description="Rolling 12-month Evaluation and Management visit count")
    lc_status_current: str = Field(description="Current lifecycle status of the patient")
    awv_date: date = Field(description="Date of the Annual Wellness Visit")
    mbi: str = Field(description="Medicare Beneficiary Identifier")
    gift_card_status: str = Field(description="Gift card eligibility and processing status")
    patientaddress: str | None = Field(default=None, description="Patient street address line 1")
    patientaddress2: str | None = Field(default=None, description="Patient street address line 2")
    patientcity: str | None = Field(default=None, description="Patient city")
    patientstate: str | None = Field(default=None, description="Patient state abbreviation")
    patientzip: str | None = Field(default=None, description="Patient ZIP code")
