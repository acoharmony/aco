# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for sbmopl schema.

Generated from: _schemas/sbmopl.yml
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
    name="sbmopl",
    version=2,
    tier="bronze",
    description="Shadow Bundles Monthly Outpatient",
    file_patterns={"reach": ["D????.PY????.??.SBMOPL.D??????.T*.csv"]},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=True, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["D????.PY????.??.SBMOPL.D??????.T*.csv"]},
    silver={"output_name": "sbmopl.parquet", "refresh_frequency": "monthly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=243,
    file_pattern="D????.PY????.??.SBMOPL.D??????.T*.csv",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class Sbmopl:
    """
    Shadow Bundles Monthly Outpatient
    """

    episode_id: str | None = Field(default=None, description="Episode identifier")
    curhic_uneq: str | None = Field(default=None, description="Current HIC unique identifier")
    mbi_id: str | None = Field(default=None, description="Medicare Beneficiary Identifier")
    claimno: str | None = Field(default=None, description="Claim number")
    lineitem: int | None = Field(default=None, description="Line item number")
    from_dt: date | None = Field(default=None, description="Service from date")
    thru_dt: date | None = Field(default=None, description="Service through date")
    rev_cntr: str | None = Field(default=None, description="Revenue center code")
    rev_dt: date | None = Field(default=None, description="Revenue date")
    rstusind: str | None = Field(default=None, description="Revenue status indicator")
    at_npi: str | None = Field(default=None, description="Attending NPI")
    op_npi: str | None = Field(default=None, description="Operating NPI")
    provider: str | None = Field(default=None, description="Provider number")
    hcpcs_cd: str | None = Field(default=None, description="HCPCS procedure code")
    rev_msp1: str | None = Field(default=None, description="Revenue MSP 1 amount")
    rev_msp2: str | None = Field(default=None, description="Revenue MSP 2 amount")
    linepmt: str | None = Field(default=None, description="Line payment amount")
    line_allowed: str | None = Field(default=None, description="Line allowed amount")
    line_std_allowed: str | None = Field(
        default=None, description="Line standardized allowed amount"
    )
    std_cost_epi_total: str | None = Field(
        default=None, description="Standardized cost episode total"
    )
    rbenepmt: str | None = Field(default=None, description="Revenue beneficiary payment amount")
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
