# © 2025 HarmonyCares
# All rights reserved.

"""Beneficiary-Level Quarterly Quality Report — DAH (Days at Home) measure."""

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
from acoharmony._validators.field_validators import MBI


@register_schema(
    name="blqqr_dah",
    version=2,
    tier="bronze",
    description="BLQQR DAH — Beneficiary-level Days at Home quality data",
    file_patterns={"reach": ["*BLQQR*.DAH.csv"]},
)
@with_parser(type="csv", delimiter=",", encoding="utf-8", has_header=True)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*BLQQR*.DAH.csv"]},
    silver={"output_name": "blqqr_dah.parquet", "refresh_frequency": "quarterly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=271,
    file_pattern="REACH.D????.BLQQR.Q?.PY????.DAH.csv",
    extract_zip=True,
    refresh_frequency="quarterly",
)
@dataclass
class BlqqrDah:
    """Beneficiary-level Days at Home (DAH) quality measure data."""

    aco_id: str = Field(description="ACO identifier")
    bene_id: str = Field(description="Beneficiary internal ID")
    mbi: str = MBI(description="Medicare Beneficiary Identifier")
    survival_days: str = Field(description="Number of survival days in measurement period")
    observed_dah: str = Field(description="Observed days at home")
    observed_dic: str = Field(description="Observed days in care (institutional)")
    nh_trans_dt: date | None = Field(default=None, description="Nursing home transition date")
    dob: date | None = Field(default=None, description="Date of birth")
    dod: str | None = Field(default=None, description="Date of death")
