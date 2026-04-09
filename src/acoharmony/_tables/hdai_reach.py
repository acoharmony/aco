# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for hdai_reach schema.

Generated from: _schemas/hdai_reach.yml

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from datetime import date
from decimal import Decimal

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
    with_transform,
)
from acoharmony._validators.field_validators import (
    MBI as MBI_Field,
)
from acoharmony._validators.field_validators import (
    NPI as NPI_Field,
)
from acoharmony._validators.field_validators import (
    ZIP5 as ZIP5_Field,
)
from acoharmony._validators.field_validators import (
    mbi_validator,
    npi_validator,
    zip5_validator,
)


@register_schema(
    name="hdai_reach",
    version=3,
    tier="bronze",
    description="HDAI REACH data - Excel file containing beneficiary information and spending metrics",
    file_patterns={
        "hdai": [
            "HC REACH Report*.xlsx",
            "HC Reach Report*.xlsx",
            "HC REACH Report ????-??-??.xlsx",
            "HC Reach Report ????-??-??.xlsx",
            "HC Reach Report ????-??-??_v2.xlsx",
            "HC Reach Report ????-??-?? ????.xlsx",
        ]
    },
)
@with_parser(type="excel", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="silver",
    file_patterns={
        "hdai": [
            "HC REACH Report*.xlsx",
            "HC Reach Report*.xlsx",
            "HC REACH Report ????-??-??.xlsx",
            "HC Reach Report ????-??-??.xlsx",
            "HC Reach Report ????-??-??_v2.xlsx",
            "HC Reach Report ????-??-?? ????.xlsx",
        ],
    },
    medallion_layer="silver",
    bronze={"output_name": "hdai_reach.parquet"},
    silver={
        "staged_from": "hdai_reach",
        "output_name": "hdai_reach.parquet",
        "refresh_frequency": "monthly",
    },
)
@dataclass
class HdaiReach:
    """
    HDAI REACH data - Excel file containing beneficiary information and spending metrics

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - HdaiReach.schema_name() -> str
        - HdaiReach.schema_metadata() -> dict
        - HdaiReach.parser_config() -> dict
        - HdaiReach.transform_config() -> dict
        - HdaiReach.lineage_config() -> dict
    """

    mbi: str = MBI_Field(
        alias="MBI",
        description="Medicare Beneficiary Identifier",
    )
    patient_first_name: str | None = Field(
        alias="Patient_First_Name",
    )
    patient_last_name: str | None = Field(
        alias="Patient_Last_Name",
    )
    patient_dob: date | None = Field(
        alias="Patient_DOB",
    )
    patient_dod: date | None = Field(
        alias="Patient_DOD",
    )
    patient_address: str | None = Field(
        alias="Patient_Address",
    )
    patient_city: str | None = Field(default=None, description="Patient city", alias="Patient City")
    patient_state: str | None = Field(
        alias="Patient_State",
    )
    patient_zip: str | None = ZIP5_Field(
        alias="Patient_Zip",
    )
    enrollment_status: str | None = Field(
        alias="Enrollment_Status",
    )
    plurality_assigned_provider_npi: str | None = NPI_Field(
        alias="NPI", default=None, description="NPI of the plurality assigned provider"
    )
    npi_name: str | None = NPI_Field(
        alias="NPI_Name",
        default=None,
        description="Name of the plurality assigned provider",
    )
    b_carrier_cost: Decimal | None = Field(
        alias="B_Carrier_Cost",
    )
    dme_spend_ytd: Decimal | None = Field(
        alias="DME_Spend_YTD",
        default=None,
        description="Durable Medical Equipment spending year-to-date",
    )
    hospice_spend_ytd: Decimal | None = Field(
        alias="Hospice_Spend_YTD",
    )
    outpatient_spend_ytd: Decimal | None = Field(
        alias="Outpatient_Spend_YTD",
    )
    snf_cost_ytd: Decimal | None = Field(
        alias="SNF_Cost_YTD",
        default=None,
        description="Skilled Nursing Facility costs year-to-date",
    )
    inpatient_spend_ytd: Decimal | None = Field(
        alias="Inpatient_Spend_YTD",
    )
    home_health_spend_ytd: Decimal | None = Field(
        alias="Home_Health_Spend_YTD",
    )
    total_spend_ytd: Decimal | None = Field(
        alias="Total_Spend_YTD",
    )
    wound_spend_ytd: Decimal | None = Field(
        alias="Wound_Spend_YTD",
    )
    apcm_spend_ytd: Decimal | None = Field(
        alias="APCM_spend_YTD",
        default=None,
        description="Advanced Primary Care Management spending year-to-date",
    )
    e_m_cost_ytd: Decimal | None = Field(
        alias="E_M_Cost_YTD",
        default=None,
        description="Evaluation and Management costs year-to-date",
    )
    any_inpatient_hospital_admits_ytd: int | None = Field(
        alias="Any_Inpatient_Hospital_Admits_YTD",
        default=None,
        description="Count of inpatient hospital admissions year-to-date",
    )
    any_inpatient_hospital_admits_90_day_prior: int | None = Field(
        alias="Any_Inpatient_Hospital_Admits_90_day_prior",
        default=None,
        description="Count of inpatient hospital admissions in prior 90 days",
    )
    er_admits_ytd: int | None = Field(
        alias="ER_Admits_YTD",
    )
    er_admits_90_day_prior: int | None = Field(
        alias="ER_Admits_90_day_prior",
        default=None,
        description="Emergency Room admissions in prior 90 days",
    )
    e_m_visits_ytd: int | None = Field(
        alias="E_M_Visits_YTD",
        default=None,
        description="Evaluation and Management visits year-to-date",
    )
    hospice_admission_t_f: bool | None = Field(
        alias="Hospice_Admission__T_F_",
        default=None,
        description="Hospice admission flag (True/False)",
    )
    snf: str | None = Field(
        alias="SNF", default=None, description="Skilled Nursing Facility indicator"
    )
    irf: str | None = Field(
        alias="IRF", default=None, description="Inpatient Rehabilitation Facility indicator"
    )
    ltac: str | None = Field(
        alias="LTAC", default=None, description="Long-Term Acute Care indicator"
    )
    home_health: str | None = Field(
        alias="Home_Health",
    )
    most_recent_awv_date: date | None = Field(
        alias="Most_Recent_AWV_Date",
        default=None,
        description="Most recent Annual Wellness Visit date",
    )
    claim_id_of_awv: str | None = Field(
        alias="Claim_ID_of_AWV",
        default=None,
        description="Unique claim id for the most recent AWV",
    )
    last_date_of_e_m_visit: date | None = Field(
        alias="Last_Date_of_E_M_Visit",
    )
    a2671_em_provider_npi: str | None = NPI_Field(
        alias="A2671_EM_Provider_NPI",
        default=None,
        description="npi for most recent E+M",
    )
    em_provider_name: str | None = NPI_Field(
        alias="EM_Provider_Name",
        default=None,
        description="HDAI generated name of individual provider",
    )
    e_m_flag_with_hc_provider: str | None = Field(
        alias="E_M_Flag_with_HC_Provider",
        default=None,
        description="boolean to tell whether the EMs come from an NPI/TIN combo associated with HCMG",
    )

    # Field Validators (from centralized _validators module)
    _validate_mbi = mbi_validator("mbi")
    _validate_patient_zip = zip5_validator("patient_zip")
    _validate_plurality_assigned_provider_npi = npi_validator("plurality_assigned_provider_npi")
    _validate_plurality_assigned_provider_npi_Name = npi_validator("npi_name")
    _validate_a2671_em_provider_npi = npi_validator("a2671_em_provider_npi")
    _validate_em_provider_name = npi_validator("em_provider_name")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "HdaiReach":
        """Create instance from dictionary."""
        return cls(**data)
