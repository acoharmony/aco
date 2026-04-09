# © 2025 HarmonyCares
# All rights reserved.

"""Beneficiary-Level Quarterly Quality Report — ACR (All-Cause Readmission) measure."""

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
    name="blqqr_acr",
    version=2,
    tier="bronze",
    description="BLQQR ACR — Beneficiary-level All-Cause Readmission quality data",
    file_patterns={"reach": ["*BLQQR*.ACR.csv"]},
)
@with_parser(type="csv", delimiter=",", encoding="utf-8", has_header=True)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*BLQQR*.ACR.csv"]},
    silver={"output_name": "blqqr_acr.parquet", "refresh_frequency": "quarterly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=271,
    file_pattern="REACH.D????.BLQQR.Q?.PY????.ACR.csv",
    extract_zip=True,
    refresh_frequency="quarterly",
)
@dataclass
class BlqqrAcr:
    """Beneficiary-level All-Cause Readmission (ACR) quality measure data."""

    # Field order matches CSV column order: ACO_ID, BENE_ID, INDEX_ADMIT_DATE, ...MBI
    aco_id: str = Field(description="ACO identifier")
    bene_id: str = Field(description="Beneficiary internal ID")
    index_admit_date: date | None = Field(description="Index admission date")
    index_disch_date: date | None = Field(description="Index discharge date")
    radm30_flag: str = Field(description="30-day readmission flag (0/1)")
    radm30_admit_date: date | None = Field(default=None, description="Readmission admit date (if readmitted)")
    radm30_disch_date: date | None = Field(default=None, description="Readmission discharge date")
    index_cohort: str | None = Field(default=None, description="Index admission cohort (CV, MEDICINE, CARDIORESPIRATORY, etc.)")
    dob: date | None = Field(default=None, description="Date of birth")
    dod: str | None = Field(default=None, description="Date of death")
    mbi: str = MBI(description="Medicare Beneficiary Identifier")
