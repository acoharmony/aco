# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for sbmip schema.

Generated from: _schemas/sbmip.yml
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
    name="sbmip",
    version=2,
    tier="bronze",
    description="Shadow Bundles Monthly Inpatient",
    file_patterns={"reach": ["D????.PY????.??.SBMIP.D??????.T*.csv"]},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=True, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["D????.PY????.??.SBMIP.D??????.T*.csv"]},
    silver={"output_name": "sbmip.parquet", "refresh_frequency": "monthly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=243,
    file_pattern="D????.PY????.??.SBMIP.D??????.T*.csv",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class Sbmip:
    """
    Shadow Bundles Monthly Inpatient
    """

    episode_id: str | None = Field(default=None, description="Episode identifier")
    curhic_uneq: str | None = Field(default=None, description="Current HIC unique identifier")
    mbi_id: str | None = Field(default=None, description="Medicare Beneficiary Identifier")
    stay_admsn_dt: date | None = Field(default=None, description="Stay admission date")
    stay_dschrgdt: date | None = Field(default=None, description="Stay discharge date")
    stay_from_dt: date | None = Field(default=None, description="Stay from date")
    stay_thru_dt: date | None = Field(default=None, description="Stay through date")
    provider: str | None = Field(default=None, description="Provider number")
    at_npi: str | None = Field(default=None, description="Attending physician NPI")
    op_npi: str | None = Field(default=None, description="Operating physician NPI")
    stay_drg_cd: str | None = Field(default=None, description="Stay DRG code")
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
    prcdrcd01: str | None = Field(default=None, description="Procedure code 1")
    prcdrcd02: str | None = Field(default=None, description="Procedure code 2")
    prcdrcd03: str | None = Field(default=None, description="Procedure code 3")
    prcdrcd04: str | None = Field(default=None, description="Procedure code 4")
    prcdrcd05: str | None = Field(default=None, description="Procedure code 5")
    prcdrcd06: str | None = Field(default=None, description="Procedure code 6")
    prcdrcd07: str | None = Field(default=None, description="Procedure code 7")
    prcdrcd08: str | None = Field(default=None, description="Procedure code 8")
    prcdrcd09: str | None = Field(default=None, description="Procedure code 9")
    prcdrcd10: str | None = Field(default=None, description="Procedure code 10")
    prcdrcd11: str | None = Field(default=None, description="Procedure code 11")
    prcdrcd12: str | None = Field(default=None, description="Procedure code 12")
    prcdrcd13: str | None = Field(default=None, description="Procedure code 13")
    prcdrcd14: str | None = Field(default=None, description="Procedure code 14")
    prcdrcd15: str | None = Field(default=None, description="Procedure code 15")
    prcdrcd16: str | None = Field(default=None, description="Procedure code 16")
    prcdrcd17: str | None = Field(default=None, description="Procedure code 17")
    prcdrcd18: str | None = Field(default=None, description="Procedure code 18")
    prcdrcd19: str | None = Field(default=None, description="Procedure code 19")
    prcdrcd20: str | None = Field(default=None, description="Procedure code 20")
    prcdrcd21: str | None = Field(default=None, description="Procedure code 21")
    prcdrcd22: str | None = Field(default=None, description="Procedure code 22")
    prcdrcd23: str | None = Field(default=None, description="Procedure code 23")
    prcdrcd24: str | None = Field(default=None, description="Procedure code 24")
    prcdrcd25: str | None = Field(default=None, description="Procedure code 25")
    stus_cd: str | None = Field(default=None, description="Status code")
    stay_pmt_amt: str | None = Field(default=None, description="Stay payment amount")
    stay_prpayamt: str | None = Field(default=None, description="Stay primary payer amount")
    stay_allowed: str | None = Field(default=None, description="Stay allowed amount")
    stay_std_allowed: str | None = Field(
        default=None, description="Stay standardized allowed amount"
    )
    std_cost_epi_total: str | None = Field(
        default=None, description="Standardized cost episode total"
    )
    ip_stay_id: str | None = Field(default=None, description="Inpatient stay identifier")
    src_adms: str | None = Field(default=None, description="Source of admission")
    ad_dgns: str | None = Field(default=None, description="Admitting diagnosis code")
    ptntcntl: str | None = Field(default=None, description="Patient control number")
    type_adm: str | None = Field(default=None, description="Type of admission")
