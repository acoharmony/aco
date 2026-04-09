# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for annual_beneficiary_level_quality_report schema.

Generated from: _schemas/annual_beneficiary_level_quality_report.yml

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
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
    name="annual_beneficiary_level_quality_report",
    version=2,
    tier="bronze",
    description="Annual Beneficiary Level Quality Report",
    file_patterns={"reach": ["*BLAQR*"]},
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*BLAQR*"]},
    silver={
        "output_name": "annual_beneficiary_level_quality_report.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_four_icli(
    category="Reports",
    file_type_code=269,
    file_pattern="D????.BLAQR.PY????.D??????.T*.zip",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class AnnualBeneficiaryLevelQualityReport:
    """
    Annual Beneficiary Level Quality Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - AnnualBeneficiaryLevelQualityReport.schema_name() -> str
        - AnnualBeneficiaryLevelQualityReport.schema_metadata() -> dict
        - AnnualBeneficiaryLevelQualityReport.parser_config() -> dict
        - AnnualBeneficiaryLevelQualityReport.transform_config() -> dict
        - AnnualBeneficiaryLevelQualityReport.lineage_config() -> dict
    """

    aco_id: str | None = Field(default=None, description="ACO Identifier")
    bene_id: str | None = Field(default=None, description="Beneficiary ID (ACR, DAH, UAMCC)")
    index_admit_date: date | None = Field(
        default=None, description="Index admission date (ACR only)"
    )
    index_disch_date: date | None = Field(
        default=None, description="Index discharge date (ACR only)"
    )
    radm30_flag: str | None = Field(default=None, description="30-day readmission flag (ACR only)")
    radm30_admit_date: date | None = Field(
        default=None, description="Readmission admit date (ACR only)"
    )
    radm30_disch_date: date | None = Field(
        default=None, description="Readmission discharge date (ACR only)"
    )
    index_cohort: str | None = Field(default=None, description="Index cohort type (ACR only)")
    survival_days: str | None = Field(
        default=None, description="Number of survival days (DAH only)"
    )
    observed_dah: str | None = Field(default=None, description="Observed days at home (DAH only)")
    observed_dic: str | None = Field(default=None, description="Observed days in care (DAH only)")
    nh_trans_dt: date | None = Field(
        default=None, description="Nursing home transfer date (DAH only)"
    )
    first_visit_date: date | None = Field(default=None, description="First visit date (UAMCC only)")
    hospice_date: date | None = Field(default=None, description="Hospice date (UAMCC only)")
    condition_ami: str | None = Field(
        default=None, description="Acute myocardial infarction condition flag (UAMCC only)"
    )
    condition_alz: str | None = Field(
        default=None, description="Alzheimer's condition flag (UAMCC only)"
    )
    condition_afib: str | None = Field(
        default=None, description="Atrial fibrillation condition flag (UAMCC only)"
    )
    condition_ckd: str | None = Field(
        default=None, description="Chronic kidney disease condition flag (UAMCC only)"
    )
    condition_copd: str | None = Field(default=None, description="COPD condition flag (UAMCC only)")
    condition_depress: str | None = Field(
        default=None, description="Depression condition flag (UAMCC only)"
    )
    condition_hf: str | None = Field(
        default=None, description="Heart failure condition flag (UAMCC only)"
    )
    condition_stroke_tia: str | None = Field(
        default=None, description="Stroke/TIA condition flag (UAMCC only)"
    )
    condition_diab: str | None = Field(
        default=None, description="Diabetes condition flag (UAMCC only)"
    )
    count_unplanned_adm: str | None = Field(
        default=None, description="Count of unplanned admissions (UAMCC only)"
    )
    ct_benes_acr: str | None = Field(
        default=None, description="Count of beneficiaries for ACR (Exclusions only)"
    )
    ct_benes_uamcc: str | None = Field(
        default=None, description="Count of beneficiaries for UAMCC (Exclusions only)"
    )
    ct_benes_dah: str | None = Field(
        default=None, description="Count of beneficiaries for DAH (Exclusions only)"
    )
    ct_benes_total: str | None = Field(
        default=None, description="Total count of beneficiaries (Exclusions only)"
    )
    ct_opting_out_acr: str | None = Field(
        default=None, description="Count opting out for ACR (Exclusions only)"
    )
    ct_opting_out_uamcc: str | None = Field(
        default=None, description="Count opting out for UAMCC (Exclusions only)"
    )
    ct_opting_out_dah: str | None = Field(
        default=None, description="Count opting out for DAH (Exclusions only)"
    )
    pc_opting_out_acr: str | None = Field(
        default=None, description="Percent opting out for ACR (Exclusions only)"
    )
    pc_opting_out_uamcc: str | None = Field(
        default=None, description="Percent opting out for UAMCC (Exclusions only)"
    )
    pc_opting_out_dah: str | None = Field(
        default=None, description="Percent opting out for DAH (Exclusions only)"
    )
    ct_opting_out_total: str | None = Field(
        default=None, description="Total count opting out (Exclusions only)"
    )
    pc_opting_out_total: str | None = Field(
        default=None, description="Total percent opting out (Exclusions only)"
    )
    ct_elig_prior_acr: str | None = Field(
        default=None, description="Count eligible prior for ACR (Exclusions only)"
    )
    ct_elig_prior_uamcc: str | None = Field(
        default=None, description="Count eligible prior for UAMCC (Exclusions only)"
    )
    ct_elig_prior_dah: str | None = Field(
        default=None, description="Count eligible prior for DAH (Exclusions only)"
    )
    pc_elig_prior_acr: str | None = Field(
        default=None, description="Percent eligible prior for ACR (Exclusions only)"
    )
    pc_elig_prior_uamcc: str | None = Field(
        default=None, description="Percent eligible prior for UAMCC (Exclusions only)"
    )
    pc_elig_prior_dah: str | None = Field(
        default=None, description="Percent eligible prior for DAH (Exclusions only)"
    )
    ct_elig_prior_total: str | None = Field(
        default=None, description="Total count eligible prior (Exclusions only)"
    )
    pc_elig_prior_total: str | None = Field(
        default=None, description="Total percent eligible prior (Exclusions only)"
    )
    dob: date | None = Field(default=None, description="Date of birth (ACR, DAH, UAMCC)")
    dod: str | None = Field(default=None, description="Date of death (ACR, DAH, UAMCC)")
    mbi: str | None = Field(
        default=None, description="Medicare Beneficiary Identifier (ACR, DAH, UAMCC)"
    )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AnnualBeneficiaryLevelQualityReport":
        """Create instance from dictionary."""
        return cls(**data)
