# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for sbmhs schema.

Generated from: _schemas/sbmhs.yml
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


@register_schema(
    name="sbmhs",
    version=2,
    tier="bronze",
    description="Shadow Bundles Monthly Hospice",
    file_patterns={"reach": ["D????.PY????.??.SBMHS.D??????.T*.csv"]},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=True, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["D????.PY????.??.SBMHS.D??????.T*.csv"]},
    silver={"output_name": "sbmhs.parquet", "refresh_frequency": "monthly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=243,
    file_pattern="D????.PY????.??.SBMHS.D??????.T*.csv",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class Sbmhs:
    """
    Shadow Bundles Monthly Hospice
    """

    episode_id: str | None = Field(default=None, description="Episode identifier")
    curhic_uneq: str | None = Field(default=None, description="Current HIC unique identifier")
    mbi_id: str | None = Field(default=None, description="Medicare Beneficiary Identifier")
    claimno: str | None = Field(default=None, description="Claim number")
    sgmt_num: int | None = Field(default=None, description="Segment number")
    fac_type: str | None = Field(default=None, description="Facility type code")
    from_dt: date | None = Field(default=None, description="Service from date")
    thru_dt: date | None = Field(default=None, description="Service through date")
    provider: str | None = Field(default=None, description="Provider number")
    pmt_amt: str | None = Field(default=None, description="Payment amount")
    prpayamt: str | None = Field(default=None, description="Primary payer amount")
    clm_allowed: str | None = Field(default=None, description="Claim allowed amount")
    std_allowed: str | None = Field(default=None, description="Standardized allowed amount")
    std_cost_epi_total: str | None = Field(
        default=None, description="Standardized cost episode total"
    )
    typesrvc: str | None = Field(default=None, description="Type of service code")
    dgnscd01: str | None = Field(default=None, description="Diagnosis code 1")
    dgnscd02: str | None = Field(default=None, description="Diagnosis code 2")
    dgnscd03: str | None = Field(default=None, description="Diagnosis code 3")
    dgnscd04: str | None = Field(default=None, description="Diagnosis code 4")
    dgnscd05: str | None = Field(default=None, description="Diagnosis code 5")
    dgnscd06: str | None = Field(default=None, description="Diagnosis code 6")
    dgnscd07: str | None = Field(default=None, description="Diagnosis code 7")
    dgnscd08: str | None = Field(default=None, description="Diagnosis code 8")
    dgnscd09: str | None = Field(default=None, description="Diagnosis code 9")
    dgnscd10: str | None = Field(default=None, description="Diagnosis code 10")
    dgnscd11: str | None = Field(default=None, description="Diagnosis code 11")
    dgnscd12: str | None = Field(default=None, description="Diagnosis code 12")
    dgnscd13: str | None = Field(default=None, description="Diagnosis code 13")
    dgnscd14: str | None = Field(default=None, description="Diagnosis code 14")
    dgnscd15: str | None = Field(default=None, description="Diagnosis code 15")
    dgnscd16: str | None = Field(default=None, description="Diagnosis code 16")
    dgnscd17: str | None = Field(default=None, description="Diagnosis code 17")
    dgnscd18: str | None = Field(default=None, description="Diagnosis code 18")
    dgnscd19: str | None = Field(default=None, description="Diagnosis code 19")
    dgnscd20: str | None = Field(default=None, description="Diagnosis code 20")
    dgnscd21: str | None = Field(default=None, description="Diagnosis code 21")
    dgnscd22: str | None = Field(default=None, description="Diagnosis code 22")
    dgnscd23: str | None = Field(default=None, description="Diagnosis code 23")
    dgnscd24: str | None = Field(default=None, description="Diagnosis code 24")
    dgnscd25: str | None = Field(default=None, description="Diagnosis code 25")
