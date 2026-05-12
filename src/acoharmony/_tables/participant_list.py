# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for participant_list schema.

ACO REACH Participant List - Excel file containing detailed provider roster
with entity information, provider details, BEI flags, and attestations.

Two source layouts feed this schema and are mapped header-driven (not
positional) via pydantic ``validation_alias`` choices on each field:

1. ACO REACH Participant List (51 columns) — the CMS-issued PY participant
   list. Headers use spaces (``"Entity ID"``, ``"Base Provider TIN"``, …).
2. HarmonyCares Provider List (27 columns) — internal export with a narrower
   schema (no entity columns, no most BEI columns). Missing fields are
   stamped by the downstream transform from the project's ACO identity
   config.

The standard ``excel`` parser opts into header-driven mapping via
``with_parser(header_driven=True)``; alias matching is whitespace- and
underscore-insensitive, so ``"Entity ID"`` and ``"Entity_ID"`` are equivalent.
"""

from datetime import date

from pydantic import AliasChoices, Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)
from acoharmony._validators.field_validators import npi_validator, tin_validator


@register_schema(
    name="participant_list",
    version=1,
    tier="bronze",
    description="ACO REACH Participant List - Provider roster with entity and BEI information",
    file_patterns={
        "participant_list": [
            "ACO REACH Participant List PY[0-9][0-9][0-9][0-9]*.xlsx",
            "ACO REACH Provider List PY[0-9][0-9][0-9][0-9]*.xlsx",
            "*Provider List - *.xlsx",
        ],
    },
)
@with_parser(
    type="excel",
    encoding="utf-8",
    has_header=True,
    header_driven=True,
    embedded_transforms=False,
)
@with_storage(
    tier="bronze",
    file_patterns={
        "participant_list": [
            "ACO REACH Participant List PY[0-9][0-9][0-9][0-9]*.xlsx",
            "ACO REACH Provider List PY[0-9][0-9][0-9][0-9]*.xlsx",
            "*Provider List - *.xlsx",
        ],
    },
    silver={
        "output_name": "participant_list.parquet",
        "refresh_frequency": "monthly",
        "last_updated_by": "aco transform participant_list",
    },
)
@dataclass
class ParticipantList:
    """
    ACO REACH Participant List - Provider roster with entity and BEI information

    This dataclass represents a single row of data from the Participant List.
    Each row represents one provider with their entity affiliation and benefit
    enhancement indicator (BEI) participation.

    Metadata access:
    - ParticipantList.schema_name() -> str
    - ParticipantList.schema_metadata() -> dict
    - ParticipantList.parser_config() -> dict
    - ParticipantList.transform_config() -> dict
    """

    entity_id: str | None = Field(
        alias="Entity_ID",
        default=None,
    )
    entity_tin: str | None = Field(
        alias="Entity_TIN",
        default=None,
        description="Tax Identification Number for Entity",
    )
    entity_legal_business_name: str | None = Field(
        alias="Entity_Legal_Business_Name",
        default=None,
        description="Legal Business Name of Entity",
    )
    performance_year: str | None = Field(
        alias="Performance_Year",
        default=None,
    )
    provider_type: str | None = Field(
        alias="Provider_Type",
        default=None,
        description="Type of provider (e.g., Individual Practitioner)",
    )
    provider_class: str | None = Field(
        alias="Provider_Class",
        default=None,
    )
    provider_legal_business_name: str | None = Field(
        alias="Provider_Legal_Business_Name",
        validation_alias=AliasChoices(
            "Provider_Legal_Business_Name",
            "TIN Legal Bus Name",
        ),
        default=None,
        description="Legal Business Name of Provider",
    )
    individual_npi: str | None = Field(
        alias="Individual_NPI",
        validation_alias=AliasChoices(
            "Individual_NPI",
            "Individual NPI(s)",
        ),
        default=None,
    )
    individual_first_name: str | None = Field(
        alias="Individual_First_Name",
        validation_alias=AliasChoices(
            "Individual_First_Name",
            "First Name",
        ),
        default=None,
    )
    individual_last_name: str | None = Field(
        alias="Individual_Last_Name",
        validation_alias=AliasChoices(
            "Individual_Last_Name",
            "Last Name",
        ),
        default=None,
    )
    base_provider_tin: str | None = Field(
        alias="Base_Provider_TIN",
        validation_alias=AliasChoices(
            "Base_Provider_TIN",
            "Billing TIN",
        ),
        default=None,
        description="Base Provider Tax Identification Number",
    )
    organization_npi: str | None = Field(
        alias="Organization_NPI",
        default=None,
    )
    ccn: str | None = Field(alias="CCN", default=None, description="CMS Certification Number")
    sole_proprietor: str | None = Field(
        alias="Sole_Proprietor",
        default=None,
    )
    sole_proprietor_tin: str | None = Field(
        alias="Sole_Proprietor_TIN",
        default=None,
    )
    primary_care_services: str | None = Field(
        alias="Primary_Care_Services",
        default=None,
    )
    specialty: str | None = Field(alias="Specialty", default=None, description="Provider Specialty")
    base_provider_tin_status: str | None = Field(
        alias="Base_Provider_TIN_Status",
        default=None,
    )
    base_provider_tin_dropped_terminated_reason: str | None = Field(
        alias="Base_Provider_TIN_Dropped_Terminated_Reason",
        validation_alias=AliasChoices(
            "Base_Provider_TIN_Dropped_Terminated_Reason",
            "Base Provider TIN Dropped/Terminated Reason",
        ),
        default=None,
        description="Reason for TIN Drop/Termination",
    )
    effective_start_date: date | None = Field(
        alias="Effective_Start_Date",
        default=None,
        json_schema_extra={"date_format": ["%B %d, %Y %I:%M %p", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]},
    )
    effective_end_date: date | None = Field(
        alias="Effective_End_Date",
        default=None,
        json_schema_extra={"date_format": ["%B %d, %Y %I:%M %p", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]},
    )
    last_updated_date: date | None = Field(
        alias="Last_Updated_Date",
        default=None,
        json_schema_extra={"date_format": ["%B %d, %Y %I:%M %p", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]},
    )
    ad_hoc_provider_addition_reason: str | None = Field(
        alias="Ad_hoc_Provider_Addition_Reason",
        validation_alias=AliasChoices(
            "Ad_hoc_Provider_Addition_Reason",
            "Ad-hoc Provider Addition Reason",
        ),
        default=None,
        description="Reason for Ad-hoc Provider Addition",
    )
    pecos_check_results: str | None = Field(
        alias="PECOS_Check_Results",
        default=None,
    )
    uses_cehrt: str | None = Field(
        alias="Uses_CEHRT",
        validation_alias=AliasChoices(
            "Uses_CEHRT",
            "Uses CEHRT?",
        ),
        default=None,
    )
    cehrt_attestation: str | None = Field(
        alias="CEHRT_Attestation",
        validation_alias=AliasChoices(
            "CEHRT_Attestation",
            (
                "I attest that this provider will use CEHRT in a manner sufficient to meet "
                "the applicable requirements of the Advanced Alternative Payment Model "
                "criterion under 42 CFR § 414.1415(a)(1)(iii), including any amendments "
                "thereto"
            ),
        ),
        default=None,
        description="CEHRT Attestation",
    )
    cehrt_id: str | None = Field(default=None, description="CEHRT Identifier", alias="CEHRT ID")
    low_volume_exception: str | None = Field(
        alias="Low_Volume_Exception",
        validation_alias=AliasChoices(
            "Low_Volume_Exception",
            (
                "I attest that this provider has an exception for Low-volume "
                "threshold, as defined in 42 CFR 414.1305"
            ),
        ),
        default=None,
        description="Low Volume Exception Attestation",
    )
    mips_exception: str | None = Field(
        alias="MIPS_Exception",
        validation_alias=AliasChoices(
            "MIPS_Exception",
            (
                "I attest that this provider has an exception for not being an MIPS "
                "eligible clinician, as set forth in § 414.1310(b)(2)"
            ),
        ),
        default=None,
        description="MIPS Exception Attestation",
    )
    mips_reweighting_exception: str | None = Field(
        alias="MIPS_Reweighting_Exception",
        validation_alias=AliasChoices(
            "MIPS_Reweighting_Exception",
            (
                "I attest that this provider has an exception for Reweighting of the MIPS "
                "Promoting Interoperability, as set forth at 42 CFR 414.1380(c)(2)(i)"
            ),
        ),
        default=None,
        description="MIPS Reweighting Exception Attestation",
    )
    other: str | None = Field(alias="Other", default=None, description="Other Information")
    overlaps_deficiencies: str | None = Field(
        alias="Overlaps_Deficiencies",
        validation_alias=AliasChoices(
            "Overlaps_Deficiencies",
            "Overlaps/Deficiencies",
        ),
        default=None,
    )
    attestation_y_n: str | None = Field(
        alias="Attestation_Y_N",
        validation_alias=AliasChoices(
            "Attestation_Y_N",
            "Attestation (Y/N)",
        ),
        default=None,
    )
    total_care_capitation_pct_reduction: str | None = Field(
        alias="Total_Care_Capitation_Pct_Reduction",
        validation_alias=AliasChoices(
            "Total_Care_Capitation_Pct_Reduction",
            "Total Care Capitation % Reduction",
        ),
        default=None,
        description="Total Care Capitation % Reduction",
    )
    primary_care_capitation_pct_reduction: str | None = Field(
        alias="Primary_Care_Capitation_Pct_Reduction",
        validation_alias=AliasChoices(
            "Primary_Care_Capitation_Pct_Reduction",
            "Primary Care Capitation % Reduction",
        ),
        default=None,
        description="Primary Care Capitation % Reduction",
    )
    advanced_payment_pct_reduction: str | None = Field(
        alias="Advanced_Payment_Pct_Reduction",
        validation_alias=AliasChoices(
            "Advanced_Payment_Pct_Reduction",
            "Advanced Payment % Reduction",
        ),
        default=None,
        description="Advanced Payment % Reduction",
    )
    cardiac_pulmonary_rehabilitation: str | None = Field(
        alias="Cardiac_Pulmonary_Rehabilitation",
        validation_alias=AliasChoices(
            "Cardiac_Pulmonary_Rehabilitation",
            "Cardiac and Pulmonary Rehabilitation",
        ),
        default=None,
        description="Cardiac and Pulmonary Rehabilitation BEI",
    )
    care_management_home_visit: str | None = Field(
        alias="Care_Management_Home_Visit",
        default=None,
        description="Care Management Home Visit BEI",
    )
    concurrent_care_for_hospice: str | None = Field(
        alias="Concurrent_Care_for_Hospice",
        validation_alias=AliasChoices(
            "Concurrent_Care_for_Hospice",
            "Concurrent Care for Hospice Beneficiaries",
        ),
        default=None,
        description="Concurrent Care for Hospice Beneficiaries BEI",
    )
    chronic_disease_management_reward: str | None = Field(
        alias="Chronic_Disease_Management_Reward",
        validation_alias=AliasChoices(
            "Chronic_Disease_Management_Reward",
            "Chronic Disease Management Reward (BEI)",
        ),
        default=None,
        description="Chronic Disease Management Reward (BEI)",
    )
    cost_sharing_for_part_b: str | None = Field(
        alias="Cost_Sharing_for_Part_B",
        validation_alias=AliasChoices(
            "Cost_Sharing_for_Part_B",
            "Cost Sharing for Part B Services (BEI)",
        ),
        default=None,
        description="Cost Sharing for Part B Services (BEI)",
    )
    diabetic_shoes: str | None = Field(
        alias="Diabetic_Shoes",
        default=None,
    )
    home_health_homebound_waiver: str | None = Field(
        alias="Home_Health_Homebound_Waiver",
        default=None,
        description="Home Health Homebound Waiver BEI",
    )
    home_infusion_therapy: str | None = Field(
        alias="Home_Infusion_Therapy",
        default=None,
    )
    hospice_care_certification: str | None = Field(
        alias="Hospice_Care_Certification",
        default=None,
        description="Hospice Care Certification BEI",
    )
    medical_nutrition_therapy: str | None = Field(
        alias="Medical_Nutrition_Therapy",
        default=None,
    )
    nurse_practitioner_services: str | None = Field(
        alias="Nurse_Practitioner_Services",
        default=None,
        description="Nurse Practitioner Services BEI",
    )
    post_discharge_home_visit: str | None = Field(
        alias="Post_Discharge_Home_Visit",
        default=None,
    )
    snf_3_day_stay_waiver: str | None = Field(
        alias="SNF_3_Day_Stay_Waiver",
        validation_alias=AliasChoices(
            "SNF_3_Day_Stay_Waiver",
            "Skilled Nursing Facility (SNF) 3-Day Stay Waiver",
        ),
        default=None,
        description="Skilled Nursing Facility (SNF) 3-Day Stay Waiver BEI",
    )
    telehealth: str | None = Field(default=None, description="Telehealth BEI", alias="Telehealth")
    email: str | None = Field(alias="Email", default=None, description="Provider Email Address")

    # Field Validators
    _validate_entity_tin = tin_validator("entity_tin")
    _validate_individual_npi = npi_validator("individual_npi")
    _validate_base_provider_tin = tin_validator("base_provider_tin")
    _validate_organization_npi = npi_validator("organization_npi")
    _validate_sole_proprietor_TIN = tin_validator("sole_proprietor_tin")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ParticipantList":
        """Create instance from dictionary."""
        return cls(**data)
