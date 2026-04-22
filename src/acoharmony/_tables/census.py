# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for census schema.

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
    with_parser,
    with_storage,
)
from acoharmony._validators.field_validators import (
    NPI,
    TIN,
    npi_validator,
    tin_validator,
)


@register_schema(
    name="census",
    version=2,
    tier="bronze",
    description="Monthly census tracking patient lifecycle, engagement, and attribution",
    file_patterns={"main": "census*.csv"},
)
@with_parser(
    type="csv", delimiter="|", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"main": "census*.csv"},
    medallion_layer="silver",
    silver={
        "output_name": "census.parquet",
        "refresh_frequency": "monthly",
        "last_updated_by": "aco transform census",
    },
    gold={"output_name": None, "refresh_frequency": None, "last_updated_by": None},
)
@dataclass
class Census:
    """
    Monthly census tracking patient lifecycle, engagement, and attribution

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Census.schema_name() -> str
        - Census.schema_metadata() -> dict
        - Census.parser_config() -> dict
        - Census.transform_config() -> dict
        - Census.lineage_config() -> dict
    """

    hcmpi: str = Field(description="HarmonyCares Master Patient Index identifier")
    monthyear: date = Field(description="Census month and year")
    first_status: date | None = Field(default=None, description="Date of first status")
    inactivation_dt: date | None = Field(default=None, description="Date of inactivation")
    lc_status_current: str | None = Field(default=None, description="Current lifecycle status")
    lifecycle_status: str | None = Field(default=None, description="Lifecycle status")
    lc_status: str | None = Field(default=None, description="Lifecycle status code")
    lc_substatus: str | None = Field(default=None, description="Lifecycle substatus")
    lc_substatus_detail: str | None = Field(default=None, description="Lifecycle substatus detail")
    lc_prev_status: str | None = Field(default=None, description="Previous lifecycle status")
    lc_created_at: str | None = Field(
        default=None, description="Lifecycle record creation timestamp"
    )
    lc_inactive_at: str | None = Field(default=None, description="Lifecycle inactivation timestamp")
    lc_changed_by_source: str | None = Field(
        default=None, description="Source of lifecycle status change"
    )
    status_monthdays: str | None = Field(
        default=None, description="Number of days in status during month"
    )
    status_monthstart: str | None = Field(default=None, description="Status at start of month")
    status_monthend: str | None = Field(default=None, description="Status at end of month")
    active_weight: str | None = Field(default=None, description="Active status weighting factor")
    prospect_weight: str | None = Field(
        default=None, description="Prospect status weighting factor"
    )
    lc_last_updated_dttm: date | None = Field(
        default=None, description="Lifecycle last updated timestamp"
    )
    primary_provider_npi_current: str | None = NPI(
        default=None, description="Current primary provider NPI"
    )
    primary_provider_id: str | None = Field(default=None, description="Primary provider identifier")
    primary_provider_npi: str | None = NPI(default=None, description="Primary provider NPI")
    pri_prov_eff_dt: date | None = Field(default=None, description="Primary provider effective date")
    pri_prov_term_dt: date | None = Field(
        default=None, description="Primary provider termination date"
    )
    pri_prov_days_in_month: str | None = Field(
        default=None, description="Primary provider days active in month"
    )
    department_id: str | None = Field(default=None, description="Department identifier")
    office_name: str | None = Field(default=None, description="Office name")
    payer_type: str | None = Field(default=None, description="Payer type classification")
    payer: str | None = Field(default=None, description="Payer name")
    pri_insurance_group: str | None = Field(default=None, description="Primary insurance group")
    assigned_insurance_nm: str | None = Field(default=None, description="Assigned insurance name")
    assigned_ins_reporting_group: str | None = Field(
        default=None, description="Assigned insurance reporting group"
    )
    assigned_insurance_package_id: str | None = Field(
        default=None, description="Assigned insurance package identifier"
    )
    payer_current: str | None = Field(default=None, description="Current payer")
    payer_type_current: str | None = Field(default=None, description="Current payer type")
    first_engagement_dt: date | None = Field(default=None, description="First engagement date")
    latest_engagement_dt: date | None = Field(
        default=None, description="Most recent engagement date"
    )
    first_em_dt: date | None = Field(
        alias="first_EM_dt",
        default=None,
        description="First evaluation and management service date",
    )
    latest_em_dt: date | None = Field(
        alias="latest_EM_dt",
        default=None,
        description="Most recent evaluation and management service date",
    )
    last_priprocedure_cd: str | None = Field(
        default=None, description="Last primary procedure code"
    )
    last_pricharge_id: str | None = Field(
        default=None, description="Last primary charge identifier"
    )
    last_pricharge_dt: date | None = Field(default=None, description="Last primary charge date")
    engaged_contract: int | None = Field(
        default=None, description="Engaged in contract (0=No, 1=Yes)"
    )
    engaged_clinical: int | None = Field(
        default=None, description="Engaged clinically (0=No, 1=Yes)"
    )
    homebased_touches: str | None = Field(
        default=None, description="Home-based touch points in month"
    )
    inperson_touches: str | None = Field(
        default=None, description="In-person touch points in month"
    )
    em_touches: str | None = Field(
        alias="EM_touches", default=None, description="E&M touch points in month"
    )
    homebased_touches_enc: str | None = Field(
        default=None, description="Home-based encounters in month"
    )
    inperson_touches_enc: str | None = Field(
        default=None, description="In-person encounters in month"
    )
    homebased_touches_chg: str | None = Field(
        default=None, description="Home-based charges in month"
    )
    inperson_touches_chg: str | None = Field(default=None, description="In-person charges in month")
    em_touches_enc: str | None = Field(
        alias="EM_touches_enc", default=None, description="E&M encounters in month"
    )
    roll3_homebased: str | None = Field(
        default=None, description="Rolling 3-month home-based touches"
    )
    roll6_homebased: str | None = Field(
        default=None, description="Rolling 6-month home-based touches"
    )
    roll12_homebased: str | None = Field(
        default=None, description="Rolling 12-month home-based touches"
    )
    ytd_homebased: str | None = Field(
        alias="YTD_homebased", default=None, description="Year-to-date home-based touches"
    )
    roll3_inperson: str | None = Field(
        default=None, description="Rolling 3-month in-person touches"
    )
    roll6_inperson: str | None = Field(
        default=None, description="Rolling 6-month in-person touches"
    )
    roll12_inperson: str | None = Field(
        default=None, description="Rolling 12-month in-person touches"
    )
    ytd_inperson: str | None = Field(
        alias="YTD_inperson", default=None, description="Year-to-date in-person touches"
    )
    roll3_homebased_enc: str | None = Field(
        default=None, description="Rolling 3-month home-based encounters"
    )
    roll6_homebased_enc: str | None = Field(
        default=None, description="Rolling 6-month home-based encounters"
    )
    roll12_homebased_enc: str | None = Field(
        default=None, description="Rolling 12-month home-based encounters"
    )
    ytd_homebased_enc: str | None = Field(
        alias="YTD_homebased_enc", default=None, description="Year-to-date home-based encounters"
    )
    roll3_inperson_enc: str | None = Field(
        default=None, description="Rolling 3-month in-person encounters"
    )
    roll6_inperson_enc: str | None = Field(
        default=None, description="Rolling 6-month in-person encounters"
    )
    roll12_inperson_enc: str | None = Field(
        default=None, description="Rolling 12-month in-person encounters"
    )
    ytd_inperson_enc: str | None = Field(
        alias="YTD_inperson_enc", default=None, description="Year-to-date in-person encounters"
    )
    roll3_em: str | None = Field(
        alias="roll3_EM", default=None, description="Rolling 3-month E&M touches"
    )
    roll6_em: str | None = Field(
        alias="roll6_EM", default=None, description="Rolling 6-month E&M touches"
    )
    roll12_em: str | None = Field(
        alias="roll12_EM", default=None, description="Rolling 12-month E&M touches"
    )
    ytd_em: str | None = Field(alias="YTD_EM", default=None, description="Year-to-date E&M touches")
    roll3_mssp_qual_chg: str | None = Field(
        alias="roll3_MSSP_qual_chg",
        default=None,
        description="Rolling 3-month MSSP qualifying charges",
    )
    roll6_mssp_qual_chg: str | None = Field(
        alias="roll6_MSSP_qual_chg",
        default=None,
        description="Rolling 6-month MSSP qualifying charges",
    )
    roll12_mssp_qual_chg: str | None = Field(
        alias="roll12_MSSP_qual_chg",
        default=None,
        description="Rolling 12-month MSSP qualifying charges",
    )
    ytd_mssp_qual_chg: str | None = Field(
        alias="YTD_MSSP_qual_chg", default=None, description="Year-to-date MSSP qualifying charges"
    )
    roll3_awv_enc: str | None = Field(
        alias="roll3_AWV_enc",
        default=None,
        description="Rolling 3-month Annual Wellness Visit encounters",
    )
    roll6_awv_enc: str | None = Field(
        alias="roll6_AWV_enc",
        default=None,
        description="Rolling 6-month Annual Wellness Visit encounters",
    )
    roll12_awv_enc: str | None = Field(
        alias="roll12_AWV_enc",
        default=None,
        description="Rolling 12-month Annual Wellness Visit encounters",
    )
    ytd_awv_enc: str | None = Field(
        alias="YTD_AWV_enc",
        default=None,
        description="Year-to-date Annual Wellness Visit encounters",
    )
    roll3_awv_chg: str | None = Field(
        alias="roll3_AWV_chg",
        default=None,
        description="Rolling 3-month Annual Wellness Visit charges",
    )
    roll6_awv_chg: str | None = Field(
        alias="roll6_AWV_chg",
        default=None,
        description="Rolling 6-month Annual Wellness Visit charges",
    )
    roll12_awv_chg: str | None = Field(
        alias="roll12_AWV_chg",
        default=None,
        description="Rolling 12-month Annual Wellness Visit charges",
    )
    ytd_awv_chg: str | None = Field(
        alias="YTD_AWV_chg", default=None, description="Year-to-date Annual Wellness Visit charges"
    )
    awv_status: str | None = Field(
        alias="AWV_status", default=None, description="Annual Wellness Visit status"
    )
    last_awv_dt: date | None = Field(
        alias="last_AWV_dt", default=None, description="Last Annual Wellness Visit date"
    )
    roll3_pcv_enc: str | None = Field(
        alias="roll3_PCV_enc",
        default=None,
        description="Rolling 3-month preventive care visit encounters",
    )
    roll6_pcv_enc: str | None = Field(
        alias="roll6_PCV_enc",
        default=None,
        description="Rolling 6-month preventive care visit encounters",
    )
    roll12_pcv_enc: str | None = Field(
        alias="roll12_PCV_enc",
        default=None,
        description="Rolling 12-month preventive care visit encounters",
    )
    ytd_pcv_enc: str | None = Field(
        alias="YTD_PCV_enc",
        default=None,
        description="Year-to-date preventive care visit encounters",
    )
    roll3_pcv_chg: str | None = Field(
        alias="roll3_PCV_chg",
        default=None,
        description="Rolling 3-month preventive care visit charges",
    )
    roll6_pcv_chg: str | None = Field(
        alias="roll6_PCV_chg",
        default=None,
        description="Rolling 6-month preventive care visit charges",
    )
    roll12_pcv_chg: str | None = Field(
        alias="roll12_PCV_chg",
        default=None,
        description="Rolling 12-month preventive care visit charges",
    )
    ytd_pcv_chg: str | None = Field(
        alias="YTD_PCV_chg", default=None, description="Year-to-date preventive care visit charges"
    )
    pcv_status: str | None = Field(
        alias="PCV_status", default=None, description="Preventive care visit status"
    )
    roll3_uc_enc: str | None = Field(
        alias="roll3_UC_enc", default=None, description="Rolling 3-month urgent care encounters"
    )
    roll6_uc_enc: str | None = Field(
        alias="roll6_UC_enc", default=None, description="Rolling 6-month urgent care encounters"
    )
    roll12_uc_enc: str | None = Field(
        alias="roll12_UC_enc", default=None, description="Rolling 12-month urgent care encounters"
    )
    ytd_uc_enc: str | None = Field(
        alias="YTD_UC_enc", default=None, description="Year-to-date urgent care encounters"
    )
    roll3_pharm_enc: str | None = Field(
        alias="roll3_Pharm_enc", default=None, description="Rolling 3-month pharmacy encounters"
    )
    roll6_pharm_enc: str | None = Field(
        alias="roll6_Pharm_enc", default=None, description="Rolling 6-month pharmacy encounters"
    )
    roll12_pharm_enc: str | None = Field(
        alias="roll12_Pharm_enc", default=None, description="Rolling 12-month pharmacy encounters"
    )
    ytd_pharm_enc: str | None = Field(
        alias="YTD_Pharm_enc", default=None, description="Year-to-date pharmacy encounters"
    )
    roll3_csw_enc: str | None = Field(
        alias="roll3_CSW_enc",
        default=None,
        description="Rolling 3-month clinical social worker encounters",
    )
    roll6_csw_enc: str | None = Field(
        alias="roll6_CSW_enc",
        default=None,
        description="Rolling 6-month clinical social worker encounters",
    )
    roll12_csw_enc: str | None = Field(
        alias="roll12_CSW_enc",
        default=None,
        description="Rolling 12-month clinical social worker encounters",
    )
    ytd_csw_enc: str | None = Field(
        alias="YTD_CSW_enc",
        default=None,
        description="Year-to-date clinical social worker encounters",
    )
    roll3_csw_chg: str | None = Field(
        alias="roll3_CSW_chg",
        default=None,
        description="Rolling 3-month clinical social worker charges",
    )
    roll6_csw_chg: str | None = Field(
        alias="roll6_CSW_chg",
        default=None,
        description="Rolling 6-month clinical social worker charges",
    )
    roll12_csw_chg: str | None = Field(
        alias="roll12_CSW_chg",
        default=None,
        description="Rolling 12-month clinical social worker charges",
    )
    ytd_csw_chg: str | None = Field(
        alias="YTD_CSW_chg", default=None, description="Year-to-date clinical social worker charges"
    )
    roll3_csw_cme: str | None = Field(
        alias="roll3_CSW_cme",
        default=None,
        description="Rolling 3-month clinical social worker care management episodes",
    )
    roll6_csw_cme: str | None = Field(
        alias="roll6_CSW_cme",
        default=None,
        description="Rolling 6-month clinical social worker care management episodes",
    )
    roll12_csw_cme: str | None = Field(
        alias="roll12_CSW_cme",
        default=None,
        description="Rolling 12-month clinical social worker care management episodes",
    )
    ytd_csw_cme: str | None = Field(
        alias="YTD_CSW_cme",
        default=None,
        description="Year-to-date clinical social worker care management episodes",
    )
    roll3_wc_nurse_enc: str | None = Field(
        alias="roll3_WC_nurse_enc",
        default=None,
        description="Rolling 3-month wound care nurse encounters",
    )
    roll6_wc_nurse_enc: str | None = Field(
        alias="roll6_WC_nurse_enc",
        default=None,
        description="Rolling 6-month wound care nurse encounters",
    )
    roll12_wc_nurse_enc: str | None = Field(
        alias="roll12_WC_nurse_enc",
        default=None,
        description="Rolling 12-month wound care nurse encounters",
    )
    ytd_wc_nurse_enc: str | None = Field(
        alias="YTD_WC_nurse_enc",
        default=None,
        description="Year-to-date wound care nurse encounters",
    )
    roll3_ncm_chg: str | None = Field(
        alias="roll3_NCM_chg",
        default=None,
        description="Rolling 3-month nurse care management charges",
    )
    roll6_ncm_chg: str | None = Field(
        alias="roll6_NCM_chg",
        default=None,
        description="Rolling 6-month nurse care management charges",
    )
    roll12_ncm_chg: str | None = Field(
        alias="roll12_NCM_chg",
        default=None,
        description="Rolling 12-month nurse care management charges",
    )
    ytd_ncm_chg: str | None = Field(
        alias="YTD_NCM_chg", default=None, description="Year-to-date nurse care management charges"
    )
    roll3_ncm_cme: str | None = Field(
        alias="roll3_NCM_cme",
        default=None,
        description="Rolling 3-month nurse care management episodes",
    )
    roll6_ncm_cme: str | None = Field(
        alias="roll6_NCM_cme",
        default=None,
        description="Rolling 6-month nurse care management episodes",
    )
    roll12_ncm_cme: str | None = Field(
        alias="roll12_NCM_cme",
        default=None,
        description="Rolling 12-month nurse care management episodes",
    )
    ytd_ncm_cme: str | None = Field(
        alias="YTD_NCM_cme", default=None, description="Year-to-date nurse care management episodes"
    )
    roll3_phc_chg: str | None = Field(
        alias="roll3_PHC_chg",
        default=None,
        description="Rolling 3-month patient health coaching charges",
    )
    roll6_phc_chg: str | None = Field(
        alias="roll6_PHC_chg",
        default=None,
        description="Rolling 6-month patient health coaching charges",
    )
    roll12_phc_chg: str | None = Field(
        alias="roll12_PHC_chg",
        default=None,
        description="Rolling 12-month patient health coaching charges",
    )
    ytd_phc_chg: str | None = Field(
        alias="YTD_PHC_chg",
        default=None,
        description="Year-to-date patient health coaching charges",
    )
    roll3_phc_cme: str | None = Field(
        alias="roll3_PHC_cme",
        default=None,
        description="Rolling 3-month patient health coaching episodes",
    )
    roll6_phc_cme: str | None = Field(
        alias="roll6_PHC_cme",
        default=None,
        description="Rolling 6-month patient health coaching episodes",
    )
    roll12_phc_cme: str | None = Field(
        alias="roll12_PHC_cme",
        default=None,
        description="Rolling 12-month patient health coaching episodes",
    )
    ytd_phc_cme: str | None = Field(
        alias="YTD_PHC_cme",
        default=None,
        description="Year-to-date patient health coaching episodes",
    )
    roll3_other_cme: str | None = Field(
        alias="roll3_Other_cme",
        default=None,
        description="Rolling 3-month other care management episodes",
    )
    roll6_other_cme: str | None = Field(
        alias="roll6_Other_cme",
        default=None,
        description="Rolling 6-month other care management episodes",
    )
    roll12_other_cme: str | None = Field(
        alias="roll12_Other_cme",
        default=None,
        description="Rolling 12-month other care management episodes",
    )
    ytd_other_cme: str | None = Field(
        alias="YTD_Other_cme",
        default=None,
        description="Year-to-date other care management episodes",
    )
    roll3_pes_touch: str | None = Field(
        alias="roll3_PES_touch",
        default=None,
        description="Rolling 3-month patient engagement specialist touches",
    )
    roll6_pes_touch: str | None = Field(
        alias="roll6_PES_touch",
        default=None,
        description="Rolling 6-month patient engagement specialist touches",
    )
    roll12_pes_touch: str | None = Field(
        alias="roll12_PES_touch",
        default=None,
        description="Rolling 12-month patient engagement specialist touches",
    )
    ytd_pes_touch: str | None = Field(
        alias="YTD_PES_touch",
        default=None,
        description="Year-to-date patient engagement specialist touches",
    )
    attr_start_dt: date | None = Field(default=None, description="Attribution start date")
    attr_end_dt: date | None = Field(default=None, description="Attribution end timestamp")
    disenroll_rcvd_dt: date | None = Field(default=None, description="Disenrollment received date")
    disenroll_reason: str | None = Field(default=None, description="Disenrollment reason")
    payer_attribution_type: str | None = Field(
        default=None, description="Payer attribution type (prospective, retrospective, etc.)"
    )
    payer_attribution: int | None = Field(
        default=None, description="Payer attribution flag (0=No, 1=Yes)"
    )
    riskpayer_datasource: str | None = Field(default=None, description="Risk payer data source")
    product: str | None = Field(default=None, description="Insurance product")
    cohort_name: str | None = Field(default=None, description="Patient cohort name")
    program_nm: str | None = Field(default=None, description="Program name")
    hospice: int | None = Field(default=None, description="Hospice flag (0=No, 1=Yes)")
    intake: int | None = Field(
        alias="Intake", default=None, description="Intake flag (0=No, 1=Yes)"
    )
    reactivated: int | None = Field(default=None, description="Reactivated flag (0=No, 1=Yes)")
    admit: int | None = Field(default=None, description="Admit flag (0=No, 1=Yes)")
    cancelled: int | None = Field(default=None, description="Cancelled flag (0=No, 1=Yes)")
    inactivated: int | None = Field(default=None, description="Inactivated flag (0=No, 1=Yes)")
    censusflag_prov: int | None = Field(
        default=None, description="Census flag for provider (0=No, 1=Yes)"
    )
    censusflag_clinicalt1: int | None = Field(
        alias="censusflag_clinicalT1",
        default=None,
        description="Census flag for clinical tier 1 (0=No, 1=Yes)",
    )
    censusflag_clinicalt2: int | None = Field(
        alias="censusflag_clinicalT2",
        default=None,
        description="Census flag for clinical tier 2 (0=No, 1=Yes)",
    )
    censusflag_operational: int | None = Field(
        default=None, description="Census flag for operational (0=No, 1=Yes)"
    )
    censusflag_contract: int | None = Field(
        default=None, description="Census flag for contract (0=No, 1=Yes)"
    )
    census_payereligible: int | None = Field(
        alias="census_PayerEligible",
        default=None,
        description="Census payer eligible flag (0=No, 1=Yes)",
    )
    census_prov_grace_flg: int | None = Field(
        default=None, description="Census provider grace period flag (0=No, 1=Yes)"
    )
    updatedatetime: str | None = Field(
        alias="UpdateDateTime", default=None, description="Record update timestamp"
    )

    # Field Validators (from centralized _validators module)
    _validate_primary_provider_npi_current = npi_validator("primary_provider_npi_current")
    _validate_primary_provider_id = npi_validator("primary_provider_id")
    _validate_primary_provider_npi = npi_validator("primary_provider_npi")
    _validate_assigned_ins_reporting_group = tin_validator("assigned_ins_reporting_group")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Census":
        """Create instance from dictionary."""
        return cls(**data)
