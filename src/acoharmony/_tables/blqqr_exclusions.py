# © 2025 HarmonyCares
# All rights reserved.

"""Beneficiary-Level Quarterly Quality Report — Exclusions (aggregate opt-out/eligibility counts)."""

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
    name="blqqr_exclusions",
    version=2,
    tier="bronze",
    description="BLQQR Exclusions — Aggregate opt-out and prior eligibility counts per measure",
    file_patterns={"reach": ["*BLQQR*.Exclusions.csv"]},
)
@with_parser(type="csv", delimiter=",", encoding="utf-8", has_header=True)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*BLQQR*.Exclusions.csv"]},
    silver={"output_name": "blqqr_exclusions.parquet", "refresh_frequency": "quarterly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=271,
    file_pattern="REACH.D????.BLQQR.Q?.PY????.Exclusions.csv",
    extract_zip=True,
    refresh_frequency="quarterly",
)
@dataclass
class BlqqrExclusions:
    """Aggregate exclusion/opt-out counts for quality measures."""

    aco_id: str = Field(description="ACO identifier")
    ct_benes_acr: str = Field(description="Count of beneficiaries eligible for ACR")
    ct_benes_uamcc: str = Field(description="Count of beneficiaries eligible for UAMCC")
    ct_benes_dah: str = Field(description="Count of beneficiaries eligible for DAH")
    ct_benes_total: str = Field(description="Total eligible beneficiaries")
    ct_opting_out_acr: str = Field(description="Count opting out of ACR")
    ct_opting_out_uamcc: str = Field(description="Count opting out of UAMCC")
    ct_opting_out_dah: str = Field(description="Count opting out of DAH")
    pc_opting_out_acr: str = Field(description="Percent opting out of ACR")
    pc_opting_out_uamcc: str = Field(description="Percent opting out of UAMCC")
    pc_opting_out_dah: str = Field(description="Percent opting out of DAH")
    ct_opting_out_total: str = Field(description="Total count opting out")
    pc_opting_out_total: str = Field(description="Total percent opting out")
    ct_elig_prior_acr: str = Field(description="Count eligible in prior period for ACR")
    ct_elig_prior_uamcc: str = Field(description="Count eligible in prior period for UAMCC")
    ct_elig_prior_dah: str = Field(description="Count eligible in prior period for DAH")
    pc_elig_prior_acr: str = Field(description="Percent eligible in prior period for ACR")
    pc_elig_prior_uamcc: str = Field(description="Percent eligible in prior period for UAMCC")
    pc_elig_prior_dah: str = Field(description="Percent eligible in prior period for DAH")
    ct_elig_prior_total: str = Field(description="Total count eligible in prior period")
    pc_elig_prior_total: str = Field(description="Total percent eligible in prior period")
