# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclfa schema.

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
    REV,
    mbi_validator,
    revenue_code_validator,
)


@register_schema(
    name="cclfa",
    version=2,
    tier="bronze",
    description="CCLFA Part A Claims Benefit Enhancement and Demonstration Code File - Fixed-width file containing Part A benefit enhancement indicators and demonstration codes",
    file_patterns={
        "mssp": "P.A*.ACO.ZCAY*.D*.T*",
        "reach": "P.D*.ACO.ZCAY*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZCAY*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZCAR*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZCAY*.D*.T*",
        "reach_runout": "P.D*.ACO.ZCAR*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZCAWY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZCAY*.D*.T*",
        "reach": "P.D*.ACO.ZCAY*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZCAY*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZCAR*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZCAY*.D*.T*",
        "reach_runout": "P.D*.ACO.ZCAR*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZCAWY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclfa.parquet",
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
class Cclfa:
    """
    CCLFA Part A Claims Benefit Enhancement and Demonstration Code File - Fixed-width file containing Part A benefit enhancement indicators and demonstration codes

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclfa.schema_name() -> str
        - Cclfa.schema_metadata() -> dict
        - Cclfa.parser_config() -> dict
        - Cclfa.transform_config() -> dict
        - Cclfa.lineage_config() -> dict
    """

    cur_clm_uniq_id: str = MBI(
        description="Current Claim Unique Identifier - A unique identification number assigned to the claim",
        json_schema_extra={"start_pos": 1, "end_pos": 13, "length": 13},
    )
    bene_mbi_id: str = MBI(
        description="Medicare Beneficiary Identifier - A Medicare Beneficiary Identifier assigned to a beneficiary",
        json_schema_extra={"start_pos": 14, "end_pos": 24, "length": 11},
    )
    bene_hic_num: str | None = Field(
        default=None,
        description="Beneficiary HIC Number - Legacy Beneficiary HICN field",
        json_schema_extra={"start_pos": 25, "end_pos": 35, "length": 11},
    )
    clm_type_cd: str | None = Field(
        default=None,
        description="Claim Type Code - Type of claim (10=HHA, 20=Non swing bed SNF, 30=Swing bed SNF, 40=Outpatient, 50=Hospice, 60=Inpatient, 61=Inpatient Full-Encounter)",
        json_schema_extra={"start_pos": 36, "end_pos": 37, "length": 2},
    )
    clm_actv_care_from_dt: date | None = Field(
        default=None,
        description="Claim Admission Date - Date beneficiary admitted to facility",
        json_schema_extra={"start_pos": 38, "end_pos": 47, "length": 10},
    )
    clm_ngaco_pbpmt_sw: str | None = Field(
        default=None,
        description="PBP Benefit Enhancement Indicator - Y/N indicates PBP benefit enhancement",
        json_schema_extra={"start_pos": 48, "end_pos": 48, "length": 1},
    )
    clm_ngaco_pdschrg_hcbs_sw: str | None = Field(
        default=None,
        description="Post Discharge Home Visit Benefit Enhancement Indicator - Y/N indicates Post Discharge Home Visit enhancement",
        json_schema_extra={"start_pos": 49, "end_pos": 49, "length": 1},
    )
    clm_ngaco_snf_wvr_sw: str | None = Field(
        default=None,
        description="SNF 3-Day Waiver Benefit Enhancement Indicator - Y/N indicates SNF 3-Day Waiver enhancement",
        json_schema_extra={"start_pos": 50, "end_pos": 50, "length": 1},
    )
    clm_ngaco_tlhlth_sw: str | None = Field(
        default=None,
        description="Telehealth Benefit Enhancement Indicator - Y/N indicates Telehealth enhancement",
        json_schema_extra={"start_pos": 51, "end_pos": 51, "length": 1},
    )
    clm_ngaco_cptatn_sw: str | None = Field(
        default=None,
        description="AIPBP Benefit Enhancement Indicator - Y/N indicates AIPBP enhancement",
        json_schema_extra={"start_pos": 52, "end_pos": 52, "length": 1},
    )
    clm_demo_1st_num: str | None = Field(
        default=None,
        description="First Program Demonstration Number - Medicare Demonstration Special Processing Number",
        json_schema_extra={"start_pos": 53, "end_pos": 54, "length": 2},
    )
    clm_demo_2nd_num: str | None = Field(
        default=None,
        description="Second Program Demonstration Number - Medicare Demonstration Special Processing Number",
        json_schema_extra={"start_pos": 55, "end_pos": 56, "length": 2},
    )
    clm_demo_3rd_num: str | None = Field(
        default=None,
        description="Third Program Demonstration Number - Medicare Demonstration Special Processing Number",
        json_schema_extra={"start_pos": 57, "end_pos": 58, "length": 2},
    )
    clm_demo_4th_num: str | None = Field(
        default=None,
        description="Fourth Program Demonstration Number - Medicare Demonstration Special Processing Number",
        json_schema_extra={"start_pos": 59, "end_pos": 60, "length": 2},
    )
    clm_demo_5th_num: str | None = Field(
        default=None,
        description="Fifth Program Demonstration Number - Medicare Demonstration Special Processing Number",
        json_schema_extra={"start_pos": 61, "end_pos": 62, "length": 2},
    )
    clm_pbp_inclsn_amt: Decimal | None = Field(
        default=None,
        description="PBP/AIPBP Inclusion Amount - Amount that would have been paid without PBP/AIPBP reduction",
        json_schema_extra={"start_pos": 63, "end_pos": 81, "length": 19},
    )
    clm_pbp_rdctn_amt: Decimal | None = Field(
        default=None,
        description="PBP/AIPBP Reduction Amount - Amount withheld from payment to provider",
        json_schema_extra={"start_pos": 82, "end_pos": 100, "length": 19},
    )
    clm_ngaco_cmg_wvr_sw: str | None = Field(
        default=None,
        description="Care Management Home Visits - Y/N indicates Care Management Home Visits enhancement",
        json_schema_extra={"start_pos": 101, "end_pos": 101, "length": 1},
    )
    clm_instnl_per_diem_amt: Decimal | None = Field(
        default=None,
        description="Claim Institutional Per Diem Amount - Maximum provider payment per day of care",
        json_schema_extra={"start_pos": 102, "end_pos": 120, "length": 19},
    )
    clm_mdcr_ip_bene_ddctbl_amt: Decimal | None = Field(
        default=None,
        description="Claim Medicare Inpatient Beneficiary Deductible Amount - Deductible beneficiary paid for inpatient services",
        json_schema_extra={"start_pos": 121, "end_pos": 135, "length": 15},
    )
    clm_mdcr_coinsrnc_amt: Decimal | None = Field(
        default=None,
        description="Claim Medicare Coinsurance Amount - Portion applied toward beneficiary coinsurance",
        json_schema_extra={"start_pos": 136, "end_pos": 154, "length": 19},
    )
    clm_blood_lblty_amt: Decimal | None = Field(
        default=None,
        description="Claim Blood Liability Amount - Portion of blood deductible beneficiary is liable for",
        json_schema_extra={"start_pos": 155, "end_pos": 169, "length": 15},
    )
    clm_instnl_prfnl_amt: Decimal | None = Field(
        default=None,
        description="Claim Institutional Professional Amount - Physician/professional charges under Part B",
        json_schema_extra={"start_pos": 170, "end_pos": 184, "length": 15},
    )
    clm_ncvrd_chrg_amt: Decimal | None = Field(
        default=None,
        description="Claim Noncovered Charge Amount - Institutional long-term care charges not reimbursable",
        json_schema_extra={"start_pos": 185, "end_pos": 203, "length": 19},
    )
    clm_mdcr_ddctbl_amt: Decimal | None = Field(
        default=None,
        description="Claim Medicare Deductible Amount - Portion applied toward beneficiary deductible",
        json_schema_extra={"start_pos": 204, "end_pos": 222, "length": 19},
    )
    clm_rlt_cond_cd: str | None = Field(
        default=None,
        description="Claim Related Condition Code - Code for PIP (62) or other conditions affecting processing",
        json_schema_extra={"start_pos": 223, "end_pos": 224, "length": 2},
    )
    clm_oprtnl_outlr_amt: Decimal | None = Field(
        default=None,
        description="Operating Outlier Amount (DSH) - Operating outlier/DSH payment amount",
        json_schema_extra={"start_pos": 225, "end_pos": 243, "length": 19},
    )
    clm_mdcr_new_tech_amt: Decimal | None = Field(
        default=None,
        description="Medicare New Technology Add-on Payment - New technology add-on payment amount",
        json_schema_extra={"start_pos": 244, "end_pos": 262, "length": 19},
    )
    clm_islet_isoln_amt: Decimal | None = Field(
        default=None,
        description="Islet Isolation Add-on Payment Amount - Islet isolation add-on payment",
        json_schema_extra={"start_pos": 263, "end_pos": 281, "length": 19},
    )
    clm_sqstrtn_rdctn_amt: Decimal | None = Field(
        default=None,
        description="Part A Sequestration Reduction Amount - Payment reduction amount specified by CMS",
        json_schema_extra={"start_pos": 282, "end_pos": 300, "length": 19},
    )
    clm_1_rev_cntr_ansi_rsn_cd: str | None = Field(
        default=None,
        description="Claim Adjustment Reason Code (CARC) - FISS code for type of adjustment",
        json_schema_extra={"start_pos": 301, "end_pos": 303, "length": 3},
    )
    clm_1_rev_cntr_ansi_grp_cd: str | None = REV(
        default=None,
        description="Claim Adjustment Segment Group Code - CAS codes categorizing payment adjustment",
        json_schema_extra={"start_pos": 304, "end_pos": 305, "length": 2},
    )
    clm_mips_pmt_amt: Decimal | None = Field(
        default=None,
        description="Capital MIPS Payment Costs - MIPS payment costs for capital",
        json_schema_extra={"start_pos": 306, "end_pos": 324, "length": 19},
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")
    _validate_clm_1_rev_cntr_ansi_rsn_cd = revenue_code_validator("clm_1_rev_cntr_ansi_rsn_cd")
    _validate_clm_1_rev_cntr_ansi_grp_cd = revenue_code_validator("clm_1_rev_cntr_ansi_grp_cd")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclfa":
        """Create instance from dictionary."""
        return cls(**data)
