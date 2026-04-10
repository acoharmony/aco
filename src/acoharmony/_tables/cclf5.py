# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclf5 schema.

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
    DRG,
    HICN,
    MBI,
    NPI,
    drg_validator,
    hicn_validator,
    mbi_validator,
    npi_validator,
)


@register_schema(
    name="cclf5",
    version=2,
    tier="bronze",
    description="CCLF5 Part B Physicians File - Fixed-width file containing Part B physician and supplier claims",
    file_patterns={
        "mssp": "P.A*.ACO.ZC5Y*.D*.T*",
        "reach": "P.D*.ACO.ZC5Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC5Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC5R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC5Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC5R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC5WY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZC5Y*.D*.T*",
        "reach": "P.D*.ACO.ZC5Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC5Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC5R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC5Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC5R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC5WY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclf5.parquet",
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
class Cclf5:
    """
    CCLF5 Part B Physicians File - Fixed-width file containing Part B physician and supplier claims

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclf5.schema_name() -> str
        - Cclf5.schema_metadata() -> dict
        - Cclf5.parser_config() -> dict
        - Cclf5.transform_config() -> dict
        - Cclf5.lineage_config() -> dict
    """

    cur_clm_uniq_id: str = MBI(
        description="Current Claim Unique Identifier - A unique identification number assigned to the claim",
        json_schema_extra={"start_pos": 1, "end_pos": 13, "length": 13},
    )
    clm_line_num: str = NPI(
        description="Claim Line Number - A sequential number that identifies a specific claim line within a given claim",
        json_schema_extra={"start_pos": 14, "end_pos": 23, "length": 10},
    )
    bene_mbi_id: str = HICN(
        description="Medicare Beneficiary Identifier - A Medicare Beneficiary Identifier assigned to a beneficiary",
        json_schema_extra={"start_pos": 24, "end_pos": 34, "length": 11},
    )
    bene_hic_num: str | None = DRG(
        default=None,
        description="Beneficiary HIC Number - Legacy Beneficiary HICN field",
        json_schema_extra={"start_pos": 35, "end_pos": 45, "length": 11},
    )
    clm_type_cd: str | None = Field(
        default=None,
        description="Claim Type Code - Type of claim (71=RIC O local carrier non-DMEPOS, 72=RIC O local carrier DMEPOS)",
        json_schema_extra={"start_pos": 46, "end_pos": 47, "length": 2},
    )
    clm_from_dt: date | None = Field(
        default=None,
        description="Claim From Date - First day on billing statement (Statement Covers From Date)",
        json_schema_extra={"start_pos": 48, "end_pos": 57, "length": 10},
    )
    clm_thru_dt: date | None = Field(
        default=None,
        description="Claim Thru Date - Last day on billing statement (Statement Covers Through Date)",
        json_schema_extra={"start_pos": 58, "end_pos": 67, "length": 10},
    )
    rndrg_prvdr_type_cd: str | None = Field(
        default=None,
        description="Rendering Provider Type Code - Type of provider (0=Clinics/groups, 1=Solo practitioners, 2=Suppliers, etc.)",
        json_schema_extra={"start_pos": 68, "end_pos": 70, "length": 3},
    )
    rndrg_prvdr_fips_st_cd: str | None = DRG(
        default=None,
        description="Rendering Provider FIPS State Code - State where provider is located",
        json_schema_extra={"start_pos": 71, "end_pos": 72, "length": 2},
    )
    clm_prvdr_spclty_cd: str | None = DRG(
        default=None,
        description="Claim-Line Provider Specialty Code - CMS specialty code for pricing",
        json_schema_extra={"start_pos": 73, "end_pos": 74, "length": 2},
    )
    clm_fed_type_srvc_cd: str | None = Field(
        default=None,
        description="Claim Federal Type Service Code - Type of service provided (consultation, surgery, etc.)",
        json_schema_extra={"start_pos": 75, "end_pos": 75, "length": 1},
    )
    clm_pos_cd: str | None = Field(
        default=None,
        description="Claim Place of Service Code - Where service was provided (ambulance, school, etc.)",
        json_schema_extra={"start_pos": 76, "end_pos": 77, "length": 2},
    )
    clm_line_from_dt: date | None = Field(
        default=None,
        description="Claim Line From Date - Date service began for this line item",
        json_schema_extra={"start_pos": 78, "end_pos": 87, "length": 10},
    )
    clm_line_thru_dt: date | None = Field(
        default=None,
        description="Claim Line Thru Date - Date service ended for this line item",
        json_schema_extra={"start_pos": 88, "end_pos": 97, "length": 10},
    )
    clm_line_hcpcs_cd: str | None = Field(
        default=None,
        description="HCPCS Code - Procedure/supply/product/service code",
        json_schema_extra={"start_pos": 98, "end_pos": 102, "length": 5},
    )
    clm_line_cvrd_pd_amt: Decimal | None = Field(
        default=None,
        description="Claim Line NCH Payment Amount - Medicare payment after deductible and coinsurance",
        json_schema_extra={"start_pos": 103, "end_pos": 117, "length": 15},
    )
    clm_line_prmry_pyr_cd: str | None = Field(
        default=None,
        description="Claim Primary Payer Code - Primary payer if not Medicare (blank=Medicare is primary)",
        json_schema_extra={"start_pos": 118, "end_pos": 118, "length": 1},
    )
    clm_line_dgns_cd: str | None = Field(
        default=None,
        description="Diagnosis Code - ICD-9/10 diagnosis for principal illness or disability",
        json_schema_extra={"start_pos": 119, "end_pos": 125, "length": 7},
    )
    clm_rndrg_prvdr_tax_num: str | None = Field(
        default=None,
        description="Claim Provider Tax Number - SSN or EIN of provider receiving payment",
        json_schema_extra={"start_pos": 126, "end_pos": 135, "length": 10},
    )
    rndrg_prvdr_npi_num: str | None = Field(
        default=None,
        description="Rendering Provider NPI Number - NPI of provider rendering service from PECOS",
        json_schema_extra={"start_pos": 136, "end_pos": 145, "length": 10},
    )
    clm_carr_pmt_dnl_cd: str | None = NPI(
        default=None,
        description="Claim Carrier Payment Denial Code - To whom payment made or if denied",
        json_schema_extra={"start_pos": 146, "end_pos": 147, "length": 2},
    )
    clm_prcsg_ind_cd: str | None = Field(
        default=None,
        description="Claim-Line Processing Indicator Code - If service allowed or reason denied",
        json_schema_extra={"start_pos": 148, "end_pos": 149, "length": 2},
    )
    clm_adjsmt_type_cd: str | None = Field(
        default=None,
        description="Claim Adjustment Type Code - 0=Original, 1=Cancellation, 2=Adjustment",
        json_schema_extra={"start_pos": 150, "end_pos": 151, "length": 2},
    )
    clm_efctv_dt: date | None = Field(
        default=None,
        description="Claim Effective Date - Date claim processed/added to NCH (Weekly Processing Date)",
        json_schema_extra={"start_pos": 152, "end_pos": 161, "length": 10},
    )
    clm_idr_ld_dt: date | None = Field(
        default=None,
        description="Claim IDR Load Date - When claim was loaded into IDR",
        json_schema_extra={"start_pos": 162, "end_pos": 171, "length": 10},
    )
    clm_cntl_num: str | None = Field(
        default=None,
        description="Claim Control Number - Unique number assigned by Medicare carrier",
        json_schema_extra={"start_pos": 172, "end_pos": 211, "length": 40},
    )
    bene_eqtbl_bic_hicn_num: str | None = Field(
        default=None,
        description="Beneficiary Equitable BIC HICN Number - Legacy field, blank as of January 1, 2020",
        json_schema_extra={"start_pos": 212, "end_pos": 222, "length": 11},
    )
    clm_line_alowd_chrg_amt: Decimal | None = Field(
        default=None,
        description="Claim Line Allowed Charges Amount - Amount Medicare approved for payment",
        json_schema_extra={"start_pos": 223, "end_pos": 239, "length": 17},
    )
    clm_line_srvc_unit_qty: Decimal | None = Field(
        default=None,
        description="Claim Line Service Unit Quantity - Count of units for services needing unit reporting",
        json_schema_extra={"start_pos": 240, "end_pos": 263, "length": 24},
    )
    hcpcs_1_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS First Modifier Code - First modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 264, "end_pos": 265, "length": 2},
    )
    hcpcs_2_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS Second Modifier Code - Second modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 266, "end_pos": 267, "length": 2},
    )
    hcpcs_3_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS Third Modifier Code - Third modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 268, "end_pos": 269, "length": 2},
    )
    hcpcs_4_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS Fourth Modifier Code - Fourth modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 270, "end_pos": 271, "length": 2},
    )
    hcpcs_5_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS Fifth Modifier Code - Fifth modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 272, "end_pos": 273, "length": 2},
    )
    clm_disp_cd: str | None = Field(
        default=None,
        description="Claim Disposition Code - Payment actions (01=Debit accepted, 02=Auto adjustment, 03=Cancel accepted)",
        json_schema_extra={"start_pos": 274, "end_pos": 275, "length": 2},
    )
    clm_dgns_1_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis First Code - First of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 276, "end_pos": 282, "length": 7},
    )
    clm_dgns_2_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Second Code - Second of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 283, "end_pos": 289, "length": 7},
    )
    clm_dgns_3_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Third Code - Third of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 290, "end_pos": 296, "length": 7},
    )
    clm_dgns_4_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Fourth Code - Fourth of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 297, "end_pos": 303, "length": 7},
    )
    clm_dgns_5_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Fifth Code - Fifth of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 304, "end_pos": 310, "length": 7},
    )
    clm_dgns_6_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Sixth Code - Sixth of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 311, "end_pos": 317, "length": 7},
    )
    clm_dgns_7_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Seventh Code - Seventh of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 318, "end_pos": 324, "length": 7},
    )
    clm_dgns_8_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Eighth Code - Eighth of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 325, "end_pos": 331, "length": 7},
    )
    dgns_prcdr_icd_ind: str | None = Field(
        default=None,
        description="ICD Version Indicator - 9=ICD-9, 0=ICD-10, U=any other value",
        json_schema_extra={"start_pos": 332, "end_pos": 332, "length": 1},
    )
    clm_dgns_9_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Ninth Code - Ninth of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 333, "end_pos": 339, "length": 7},
    )
    clm_dgns_10_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Tenth Code - Tenth of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 340, "end_pos": 346, "length": 7},
    )
    clm_dgns_11_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Eleventh Code - Eleventh of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 347, "end_pos": 353, "length": 7},
    )
    clm_dgns_12_cd: str | None = Field(
        default=None,
        description="Claim Diagnosis Twelfth Code - Twelfth of 12 ICD-9/10 diagnosis codes",
        json_schema_extra={"start_pos": 354, "end_pos": 360, "length": 7},
    )
    hcpcs_betos_cd: str | None = Field(
        default=None,
        description="HCPCS BETOS Code - Berenson-Eggers Type of Service clinical category code",
        json_schema_extra={"start_pos": 361, "end_pos": 363, "length": 3},
    )
    clm_rndrg_prvdr_npi_num: str | None = Field(
        default=None,
        description="Claim Rendering Provider NPI Number - NPI from claims processing system (as of Jan 2022)",
        json_schema_extra={"start_pos": 364, "end_pos": 373, "length": 10},
    )
    clm_rfrg_prvdr_npi_num: str | None = NPI(
        default=None,
        description="Claim Referring Provider NPI Number - NPI of provider who referred the service",
        json_schema_extra={"start_pos": 374, "end_pos": 383, "length": 10},
    )
    clm_cntrctor_num: str | None = Field(
        default=None,
        description="Claim contractor number - CMS assigned MAC number authorized to process claims",
        json_schema_extra={"start_pos": 384, "end_pos": 388, "length": 5},
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")
    _validate_rndrg_prvdr_type_cd = drg_validator("rndrg_prvdr_type_cd")
    _validate_rndrg_prvdr_fips_st_cd = drg_validator("rndrg_prvdr_fips_st_cd")
    _validate_clm_rndrg_prvdr_tax_num = drg_validator("clm_rndrg_prvdr_tax_num")
    _validate_rndrg_prvdr_npi_num = npi_validator("rndrg_prvdr_npi_num")
    _validate_bene_eqtbl_bic_hicn_num = hicn_validator("bene_eqtbl_bic_hicn_num")
    _validate_clm_rndrg_prvdr_npi_num = npi_validator("clm_rndrg_prvdr_npi_num")
    _validate_clm_rfrg_prvdr_npi_num = npi_validator("clm_rfrg_prvdr_npi_num")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclf5":
        """Create instance from dictionary."""
        return cls(**data)
