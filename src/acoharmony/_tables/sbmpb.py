# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for sbmpb schema.

Generated from: _schemas/sbmpb.yml
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
    name="sbmpb",
    version=2,
    tier="bronze",
    description="Shadow Bundles Monthly Part B (Professional/Physician)",
    file_patterns={"reach": ["D????.PY????.??.SBMPB.D??????.T*.csv"]},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=True, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["D????.PY????.??.SBMPB.D??????.T*.csv"]},
    silver={"output_name": "sbmpb.parquet", "refresh_frequency": "monthly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=243,
    file_pattern="D????.PY????.??.SBMPB.D??????.T*.csv",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class Sbmpb:
    """
    Shadow Bundles Monthly Part B (Professional/Physician)
    """

    episode_id: str | None = Field(default=None, description="Episode identifier")
    curhic_uneq: str | None = Field(default=None, description="Current HIC unique identifier")
    mbi_id: str | None = Field(default=None, description="Medicare Beneficiary Identifier")
    claimno: str | None = Field(default=None, description="Claim number")
    lineitem: int | None = Field(default=None, description="Line item number")
    from_dt: date | None = Field(default=None, description="Service from date")
    thru_dt: date | None = Field(default=None, description="Service through date")
    expnsdt1: date | None = Field(default=None, description="Expense date 1")
    expnsdt2: date | None = Field(default=None, description="Expense date 2")
    hcpcs_cd: str | None = Field(default=None, description="HCPCS procedure code")
    prfnpi: str | None = Field(default=None, description="Performing provider NPI")
    tax_num: str | None = Field(default=None, description="Tax identification number")
    mdfr_cd1: str | None = Field(default=None, description="Modifier code 1")
    mdfr_cd2: str | None = Field(default=None, description="Modifier code 2")
    mdfr_cd3: str | None = Field(default=None, description="Modifier code 3")
    mdfr_cd4: str | None = Field(default=None, description="Modifier code 4")
    plcsrvc: str | None = Field(default=None, description="Place of service code")
    linepmt: str | None = Field(default=None, description="Line payment amount")
    line_allowed: str | None = Field(default=None, description="Line allowed amount")
    lprpdamt: str | None = Field(default=None, description="Line primary payer paid amount")
    line_std_allowed: str | None = Field(
        default=None, description="Line standardized allowed amount"
    )
    std_cost_epi_total: str | None = Field(
        default=None, description="Standardized cost episode total"
    )
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
    linedgns: str | None = Field(default=None, description="Line diagnosis pointer")
