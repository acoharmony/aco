# © 2025 HarmonyCares
# All rights reserved.

"""Beneficiary-Level Quarterly Quality Report — UAMCC (Unplanned Admissions for MCC) measure."""

from datetime import date

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_storage,
)
from acoharmony._validators.field_validators import MBI


@register_schema(
    name="blqqr_uamcc",
    version=2,
    tier="bronze",
    description="BLQQR UAMCC — Beneficiary-level Unplanned Admissions for Multiple Chronic Conditions",
    file_patterns={"reach": ["*BLQQR*.UAMCC.csv"]},
)
@with_parser(
    type="csv",
    delimiter=",",
    encoding="utf-8",
    has_header=True,
    # PY2024 files lack the unplanned_adm_date / unplanned_visit_number
    # columns added in PY2025. Map by header name (case-insensitive) so a
    # single schema handles both formats without column-shift bugs.
    map_by="name",
)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*BLQQR*.UAMCC.csv"]},
    silver={"output_name": "blqqr_uamcc.parquet", "refresh_frequency": "quarterly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=271,
    file_pattern="REACH.D????.BLQQR.Q?.PY????.UAMCC.csv",
    extract_zip=True,
    refresh_frequency="quarterly",
)
@dataclass
class BlqqrUamcc:
    """Beneficiary-level Unplanned Admissions for Multiple Chronic Conditions (UAMCC)."""

    # CMS BLQQR CSVs ship dates as %m/%d/%Y. The registry's default
    # %Y-%m-%d would null every date silently — declare both formats so
    # the parser tries the file's actual format first.
    _CMS_DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d"]

    aco_id: str = Field(description="ACO identifier")
    bene_id: str = Field(description="Beneficiary internal ID")
    mbi: str = MBI(description="Medicare Beneficiary Identifier")
    first_visit_date: date | None = Field(
        description="First qualifying visit date",
        json_schema_extra={"date_format": _CMS_DATE_FORMATS},
    )
    hospice_date: date | None = Field(
        default=None,
        description="Hospice enrollment date",
        json_schema_extra={"date_format": _CMS_DATE_FORMATS},
    )
    condition_ami: str | None = Field(default=None, description="Acute myocardial infarction flag")
    condition_alz: str | None = Field(default=None, description="Alzheimer's disease flag")
    condition_afib: str | None = Field(default=None, description="Atrial fibrillation flag")
    condition_ckd: str | None = Field(default=None, description="Chronic kidney disease flag")
    condition_copd: str | None = Field(default=None, description="COPD flag")
    condition_depress: str | None = Field(default=None, description="Depression flag")
    condition_hf: str | None = Field(default=None, description="Heart failure flag")
    condition_stroke_tia: str | None = Field(default=None, description="Stroke/TIA flag")
    condition_diab: str | None = Field(default=None, description="Diabetes flag")
    count_unplanned_adm: str | None = Field(default=None, description="Count of unplanned admissions")
    unplanned_adm_date: date | None = Field(
        default=None,
        description="Unplanned admission date (PY2025+)",
        json_schema_extra={"date_format": _CMS_DATE_FORMATS},
    )
    unplanned_visit_number: str | None = Field(default=None, description="Unplanned visit number (PY2025+)")
    dob: date | None = Field(
        default=None,
        description="Date of birth",
        json_schema_extra={"date_format": _CMS_DATE_FORMATS},
    )
    dod: str | None = Field(default=None, description="Date of death")
