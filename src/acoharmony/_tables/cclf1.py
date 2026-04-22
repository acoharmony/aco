# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclf1 schema.

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
    with_four_icli,
    with_parser,
    with_storage,
)
from acoharmony._validators.field_validators import (
    MBI,
    NPI,
    drg_validator,
    mbi_validator,
    npi_validator,
)


@register_schema(
    name="cclf1",
    version=2,
    tier="bronze",
    description="CCLF1 Part A Claims Header File - Fixed-width file containing Part A institutional claims data",
    file_patterns={
        "mssp": "P.A*.ACO.ZC1Y*.D*.T*",
        "reach": "P.D*.ACO.ZC1Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC1Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC1R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC1Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC1R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC1WY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZC1Y*.D*.T*",
        "reach": "P.D*.ACO.ZC1Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC1Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC1R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC1Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC1R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC1WY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclf1.parquet",
        "refresh_frequency": "weekly",
        "last_updated_by": "scripts/process_raw_to_parquet.py",
    },
    gold={"output_name": None, "refresh_frequency": None, "last_updated_by": None},
)
@with_four_icli(
    category="Claim and Claim Line Feed (CCLF) Files",
    file_type_code=113,
    file_pattern="P.?????.ACO.ZC*??.D??????.T*.zip, P.?????.ACO.ZCWY??.S??????.E??????.D??????.T*.zip, P.?????.ACO.ZC*Y??.D??????.T*, P.?????.ACO.ZC*WY??.D??????.T*, P.?????.ACO.ZC*R??.D??????.T*",
    extract_zip=True,
    refresh_frequency="weekly",
    default_date_filter={"createdWithinLastWeek": True},
)
@dataclass
class Cclf1:
    """
    CCLF1 Part A Claims Header File - Fixed-width file containing Part A institutional claims data

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclf1.schema_name() -> str
        - Cclf1.schema_metadata() -> dict
        - Cclf1.parser_config() -> dict
        - Cclf1.transform_config() -> dict
        - Cclf1.lineage_config() -> dict
    """

    cur_clm_uniq_id: str = Field(
        description="Current Claim Unique Identifier - A unique identification number assigned to the claim",
        json_schema_extra={"start_pos": 1, "end_pos": 13, "length": 13},
    )
    prvdr_oscar_num: str | None = Field(
        default=None,
        description="Provider OSCAR Number - Medicare/Medicaid identification number (CCN)",
        json_schema_extra={"start_pos": 14, "end_pos": 19, "length": 6},
    )
    bene_mbi_id: str = MBI(
        description="Medicare Beneficiary Identifier - A Medicare Beneficiary Identifier assigned to a beneficiary",
        json_schema_extra={"start_pos": 20, "end_pos": 30, "length": 11},
    )
    bene_hic_num: str | None = Field(
        default=None,
        description="Beneficiary HIC Number - Legacy Beneficiary HICN field (deprecated Jan 2020)",
        json_schema_extra={"start_pos": 31, "end_pos": 41, "length": 11},
    )
    clm_type_cd: str | None = Field(
        default=None,
        description="Claim Type Code - Type of claim (10=HHA, 20=SNF, 30=Swing bed SNF, 40=Outpatient, 50=Hospice, 60=Inpatient, 61=Inpatient Full-Encounter)",
        json_schema_extra={"start_pos": 42, "end_pos": 43, "length": 2},
    )
    clm_from_dt: date | None = Field(
        default=None,
        description="Claim From Date - First day on billing statement",
        json_schema_extra={"start_pos": 44, "end_pos": 53, "length": 10},
    )
    clm_thru_dt: date | None = Field(
        default=None,
        description="Claim Thru Date - Last day on billing statement",
        json_schema_extra={"start_pos": 54, "end_pos": 63, "length": 10},
    )
    clm_bill_fac_type_cd: str | None = Field(
        default=None,
        description="Claim Bill Facility Type Code - Type of facility (1=Hospital, 2=SNF, 3=HHA, etc.)",
        json_schema_extra={"start_pos": 64, "end_pos": 64, "length": 1},
    )
    clm_bill_clsfctn_cd: str | None = Field(
        default=None,
        description="Claim Bill Classification Code - Where service was provided",
        json_schema_extra={"start_pos": 65, "end_pos": 65, "length": 1},
    )
    prncpl_dgns_cd: str | None = Field(
        default=None,
        description="Principal Diagnosis Code - ICD-9/10 diagnosis code",
        json_schema_extra={"start_pos": 66, "end_pos": 72, "length": 7},
    )
    admtg_dgns_cd: str | None = Field(
        default=None,
        description="Admitting Diagnosis Code - ICD-9/10 diagnosis for admission",
        json_schema_extra={"start_pos": 73, "end_pos": 79, "length": 7},
    )
    clm_mdcr_npmt_rsn_cd: str | None = Field(
        default=None,
        description="Claim Medicare Non-Payment Reason Code",
        json_schema_extra={"start_pos": 80, "end_pos": 81, "length": 2},
    )
    clm_pmt_amt: Decimal | None = Field(
        default=None,
        description="Claim Payment Amount - Amount Medicare paid",
        json_schema_extra={"start_pos": 82, "end_pos": 98, "length": 17},
    )
    clm_nch_prmry_pyr_cd: str | None = Field(
        default=None,
        description="Claim NCH Primary Payer Code - Primary payer if not Medicare",
        json_schema_extra={"start_pos": 99, "end_pos": 99, "length": 1},
    )
    prvdr_fac_fips_st_cd: str | None = Field(
        default=None,
        description="Federal Information Processing Standards State Code",
        json_schema_extra={"start_pos": 100, "end_pos": 101, "length": 2},
    )
    bene_ptnt_stus_cd: str | None = Field(
        default=None,
        description="Beneficiary Patient Status Code - Discharge status",
        json_schema_extra={"start_pos": 102, "end_pos": 103, "length": 2},
    )
    dgns_drg_cd: str | None = Field(
        default=None,
        description="Diagnosis Related Group Code",
        json_schema_extra={"start_pos": 104, "end_pos": 107, "length": 4},
    )
    clm_op_srvc_type_cd: str | None = Field(
        default=None,
        description="Claim Outpatient Service Type Code",
        json_schema_extra={"start_pos": 108, "end_pos": 108, "length": 1},
    )
    fac_prvdr_npi_num: str | None = Field(
        default=None,
        description="Facility Provider NPI Number",
        json_schema_extra={"start_pos": 109, "end_pos": 118, "length": 10},
    )
    oprtg_prvdr_npi_num: str | None = NPI(
        default=None,
        description="Operating Provider NPI Number",
        json_schema_extra={"start_pos": 119, "end_pos": 128, "length": 10},
    )
    atndg_prvdr_npi_num: str | None = NPI(
        default=None,
        description="Attending Provider NPI Number",
        json_schema_extra={"start_pos": 129, "end_pos": 138, "length": 10},
    )
    othr_prvdr_npi_num: str | None = NPI(
        default=None,
        description="Other Provider NPI Number",
        json_schema_extra={"start_pos": 139, "end_pos": 148, "length": 10},
    )
    clm_adjsmt_type_cd: str | None = Field(
        default=None,
        description="Claim Adjustment Type Code - 0=Original, 1=Cancellation, 2=Adjustment",
        json_schema_extra={"start_pos": 149, "end_pos": 150, "length": 2},
    )
    clm_efctv_dt: date | None = Field(
        default=None,
        description="Claim Effective Date - Date claim was processed",
        json_schema_extra={"start_pos": 151, "end_pos": 160, "length": 10},
    )
    clm_idr_ld_dt: date | None = Field(
        default=None,
        description="Claim IDR Load Date - When claim was loaded into IDR",
        json_schema_extra={"start_pos": 161, "end_pos": 170, "length": 10},
    )
    bene_eqtbl_bic_hicn_num: str | None = Field(
        default=None,
        description="Beneficiary Equitable BIC HICN Number - Legacy field (blank after Jan 2020)",
        json_schema_extra={"start_pos": 171, "end_pos": 181, "length": 11},
    )
    clm_admsn_type_cd: str | None = Field(
        default=None,
        description="Claim Admission Type Code - Type and priority of inpatient services",
        json_schema_extra={"start_pos": 182, "end_pos": 183, "length": 2},
    )
    clm_admsn_src_cd: str | None = Field(
        default=None,
        description="Claim Admission Source Code - Source of referral for admission",
        json_schema_extra={"start_pos": 184, "end_pos": 185, "length": 2},
    )
    clm_bill_freq_cd: str | None = Field(
        default=None,
        description="Claim Bill Frequency Code - Sequence of claim in episode",
        json_schema_extra={"start_pos": 186, "end_pos": 186, "length": 1},
    )
    clm_query_cd: str | None = Field(
        default=None,
        description="Claim Query Code - Type of claim record being processed",
        json_schema_extra={"start_pos": 187, "end_pos": 187, "length": 1},
    )
    dgns_prcdr_icd_ind: str | None = Field(
        default=None,
        description="ICD Version Indicator - 9=ICD-9, 0=ICD-10",
        json_schema_extra={"start_pos": 188, "end_pos": 188, "length": 1},
    )
    clm_mdcr_instnl_tot_chrg_amt: Decimal | None = Field(
        default=None,
        description="Total Claim Charge Amount",
        json_schema_extra={"start_pos": 189, "end_pos": 203, "length": 15},
    )
    clm_mdcr_ip_pps_cptl_ime_amt: Decimal | None = Field(
        default=None,
        description="Claim Capital Indirect Medical Education Amount",
        json_schema_extra={"start_pos": 204, "end_pos": 218, "length": 15},
    )
    clm_oprtnl_ime_amt: Decimal | None = Field(
        default=None,
        description="Claim Operational Indirect Medical Education Amount",
        json_schema_extra={"start_pos": 219, "end_pos": 240, "length": 22},
    )
    clm_mdcr_ip_pps_dsprprtnt_amt: Decimal | None = Field(
        default=None,
        description="Claim Capital Disproportionate Amount",
        json_schema_extra={"start_pos": 241, "end_pos": 255, "length": 15},
    )
    clm_hipps_uncompd_care_amt: Decimal | None = Field(
        default=None,
        description="Claim HIPPS Uncompensated Care Amount",
        json_schema_extra={"start_pos": 256, "end_pos": 270, "length": 15},
    )
    clm_oprtnl_dsprprtnt_amt: Decimal | None = Field(
        default=None,
        description="Claim Operational Disproportionate Amount",
        json_schema_extra={"start_pos": 271, "end_pos": 292, "length": 22},
    )
    clm_blg_prvdr_oscar_num: str | None = Field(
        default=None,
        description="Claim Provider OSCAR Number - From claims processing system",
        json_schema_extra={"start_pos": 293, "end_pos": 312, "length": 20},
    )
    clm_blg_prvdr_npi_num: str | None = Field(
        default=None,
        description="Claim Facility Provider NPI Number - From claims processing system",
        json_schema_extra={"start_pos": 313, "end_pos": 322, "length": 10},
    )
    clm_oprtg_prvdr_npi_num: str | None = NPI(
        default=None,
        description="Claim Operating Provider NPI Number - From claims processing system",
        json_schema_extra={"start_pos": 323, "end_pos": 332, "length": 10},
    )
    clm_atndg_prvdr_npi_num: str | None = NPI(
        default=None,
        description="Claim Attending Provider NPI Number - From claims processing system",
        json_schema_extra={"start_pos": 333, "end_pos": 342, "length": 10},
    )
    clm_othr_prvdr_npi_num: str | None = NPI(
        default=None,
        description="Claim Other Provider NPI Number - From claims processing system",
        json_schema_extra={"start_pos": 343, "end_pos": 352, "length": 10},
    )
    clm_cntl_num: str | None = Field(
        default=None,
        description="Claim Control Number - Identifier assigned by claim processor",
        json_schema_extra={"start_pos": 353, "end_pos": 392, "length": 40},
    )
    clm_org_cntl_num: str | None = Field(
        default=None,
        description="Claim Original Control Number - Unique number for original claim",
        json_schema_extra={"start_pos": 393, "end_pos": 432, "length": 40},
    )
    clm_cntrctr_num: str | None = Field(
        default=None,
        description="Claim Contractor Number - MAC identifier",
        json_schema_extra={"start_pos": 433, "end_pos": 437, "length": 5},
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")
    _validate_dgns_drg_cd = drg_validator("dgns_drg_cd")
    _validate_fac_prvdr_npi_num = npi_validator("fac_prvdr_npi_num")
    _validate_oprtg_prvdr_npi_num = npi_validator("oprtg_prvdr_npi_num")
    _validate_atndg_prvdr_npi_num = npi_validator("atndg_prvdr_npi_num")
    _validate_othr_prvdr_npi_num = npi_validator("othr_prvdr_npi_num")
    _validate_clm_blg_prvdr_npi_num = npi_validator("clm_blg_prvdr_npi_num")
    _validate_clm_oprtg_prvdr_npi_num = npi_validator("clm_oprtg_prvdr_npi_num")
    _validate_clm_atndg_prvdr_npi_num = npi_validator("clm_atndg_prvdr_npi_num")
    _validate_clm_othr_prvdr_npi_num = npi_validator("clm_othr_prvdr_npi_num")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclf1":
        """Create instance from dictionary."""
        return cls(**data)
