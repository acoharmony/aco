# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for pe_summary schema.

Generated from: _schemas/pe_summary.yml
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
    with_transform,
)


@register_schema(
    name="pe_summary",
    version=2,
    tier="bronze",
    description="Practice Expense Summary - Summary of practice expense components by HCPCS code",
    file_patterns={"annual": "*PE_SUMMARY*.csv", "annual_xlsx": "*PE_SUMMARY*.xlsx"},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"annual": "*PE_SUMMARY*.csv", "annual_xlsx": "*PE_SUMMARY*.xlsx"},
    medallion_layer="bronze",
    silver={
        "output_name": "pe_summary.parquet",
        "refresh_frequency": "annual",
        "last_updated_by": "aco transform pe_summary",
    },
)
@dataclass
class PeSummary:
    """
    Practice Expense Summary - Summary of practice expense components by HCPCS code
    """

    hcpcs: str = Field(description="HCPCS Code")
    modifier: str | None = Field(default=None, description="Modifier Code")
    hcpcs_modifier: str | None = Field(default=None, description="Combined HCPCS and Modifier")
    need_nf_pe_flag: str | None = Field(
        default=None, description="Need Non-Facility Practice Expense Flag"
    )
    need_f_pe_flag: str | None = Field(
        default=None, description="Need Facility Practice Expense Flag"
    )
    nf_pre: float | None = Field(default=None, description="Non-Facility Pre-Service")
    nf_pre_svc_cost: float | None = Field(default=None, description="Non-Facility Pre-Service Cost")
    nf_svc_cost: float | None = Field(default=None, description="Non-Facility Service Cost")
    nf_post_cost: float | None = Field(default=None, description="Non-Facility Post-Service Cost")
    f_pre_svc_cost: float | None = Field(default=None, description="Facility Pre-Service Cost")
    f_svc_cost: float | None = Field(default=None, description="Facility Service Cost")
    f_post_svc_cost: float | None = Field(default=None, description="Facility Post-Service Cost")
    nf_supply_cost: float | None = Field(default=None, description="Non-Facility Supply Cost")
    f_supply_cost: float | None = Field(default=None, description="Facility Supply Cost")
    nf_equipment_cost: float | None = Field(default=None, description="Non-Facility Equipment Cost")
    f_equipment_cost: float | None = Field(default=None, description="Facility Equipment Cost")
