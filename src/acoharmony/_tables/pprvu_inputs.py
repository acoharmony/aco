# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for pprvu_inputs schema.

Generated from: _schemas/pprvu_inputs.yml
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
    name="pprvu_inputs",
    version=2,
    tier="bronze",
    description="Medicare Physician Fee Schedule National Physician Fee Schedule Relative Value Datatable",
    file_patterns={"annual": "*PPRVU*.csv", "annual_xlsx": "*PPRVU*.xlsx"},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"annual": "*PPRVU*.csv", "annual_xlsx": "*PPRVU*.xlsx"},
    medallion_layer="bronze",
    silver={
        "output_name": "pprvu_inputs.parquet",
        "refresh_frequency": "annual",
        "last_updated_by": "aco transform pprvu_inputs",
    },
)
@dataclass
class PprvuInputs:
    """
    Medicare Physician Fee Schedule National Physician Fee Schedule Relative Value Datatable
    """

    hcpcs: str = Field(description="HCPCS Code")
    mod: str | None = Field(default=None, description="Modifier")
    description: str | None = Field(default=None, description="Service Description")
    status_code: str | None = Field(default=None, description="Status Code")
    not_used_for_medicare_flag: str | None = Field(
        default=None, description="Not Used for Medicare Payment Flag"
    )
    work_rvu: float | None = Field(default=None, description="Work Relative Value Unit")
    nf_pe_rvu: float | None = Field(default=None, description="Non-Facility Practice Expense RVU")
    nf_na_indicator_flag: str | None = Field(
        default=None, description="Non-Facility Not Applicable Indicator Flag"
    )
    mp_rvu: float | None = Field(default=None, description="Malpractice RVU")
    nf_tot: float | None = Field(default=None, description="Non-Facility Total RVU")
    f_tot: float | None = Field(default=None, description="Facility Total RVU")
    pc_tc_indicator: str | None = Field(
        default=None, description="Professional Component/Technical Component Indicator"
    )
    global_days: str | None = Field(default=None, description="Global Days")
    pre_op: float | None = Field(default=None, description="Pre-Operative Percentage")
    post_op: float | None = Field(default=None, description="Post-Operative Percentage")
    multiple_procedure_ind: str | None = Field(
        default=None, description="Multiple Procedure Indicator"
    )
    bilateral_surgery_ind: str | None = Field(
        default=None, description="Bilateral Surgery Indicator"
    )
    assisted_surg_ind: str | None = Field(default=None, description="Assistant Surgeon Indicator")
    co_surgery_ind: str | None = Field(default=None, description="Co-Surgery Indicator")
    team_surgery_ind: str | None = Field(default=None, description="Team Surgery Indicator")
    end_base: str | None = Field(default=None, description="Endoscopy Base Code")
    conversion_factor: float | None = Field(default=None, description="Conversion Factor")
    phys_sup_diag_proc: str | None = Field(
        default=None, description="Physician Supervision of Diagnostic Procedures"
    )
    calc_flag: str | None = Field(default=None, description="Calculation Flag")
    diag_img_fam_ind: str | None = Field(
        default=None, description="Diagnostic Imaging Family Indicator"
    )
    nf_pe_opps: float | None = Field(
        default=None, description="Non-Facility Practice Expense Used for OPPS Payment Amount"
    )
    f_pe_opps: float | None = Field(
        default=None, description="Facility Practice Expense Used for OPPS Payment Amount"
    )
    mp_opps: float | None = Field(
        default=None, description="Malpractice Used for OPPS Payment Amount"
    )
    tot_rvu: float | None = Field(default=None, description="Total RVU")
