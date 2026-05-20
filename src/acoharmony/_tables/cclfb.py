# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclfb schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

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
    mbi_validator,
)


@register_schema(
    name="cclfb",
    version=2,
    tier="bronze",
    description="CCLFB Part B Claims Benefit Enhancement and Demonstration Code File - Fixed-width file containing Part B benefit enhancement indicators and demonstration codes",
    file_patterns={
        "mssp": "P.A*.ACO.ZCBY*.D*.T*",
        "reach": "P.D*.ACO.ZCBY*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZCBY*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZCBR*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZCBY*.D*.T*",
        "reach_runout": "P.D*.ACO.ZCBR*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZCBWY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZCBY*.D*.T*",
        "reach": "P.D*.ACO.ZCBY*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZCBY*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZCBR*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZCBY*.D*.T*",
        "reach_runout": "P.D*.ACO.ZCBR*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZCBWY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclfb.parquet",
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
class Cclfb:
    """
    CCLFB Part B Claims Benefit Enhancement and Demonstration Code File - Fixed-width file containing Part B benefit enhancement indicators and demonstration codes

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclfb.schema_name() -> str
        - Cclfb.schema_metadata() -> dict
        - Cclfb.parser_config() -> dict
        - Cclfb.transform_config() -> dict
        - Cclfb.lineage_config() -> dict
    """

    cur_clm_uniq_id: str = Field(
        description="Current Claim Unique Identifier - A unique identification number assigned to the claim",
        json_schema_extra={"start_pos": 1, "end_pos": 13, "length": 13},
    )
    clm_line_num: str = Field(
        description="Claim Line Number - A sequential number that identifies a specific claim line within a given claim",
        json_schema_extra={"start_pos": 14, "end_pos": 23, "length": 10},
    )
    bene_mbi_id: str = MBI(
        description="Medicare Beneficiary Identifier - A Medicare Beneficiary Identifier assigned to a beneficiary",
        json_schema_extra={"start_pos": 24, "end_pos": 34, "length": 11},
    )
    bene_hic_num: str | None = Field(
        default=None,
        description="Beneficiary HIC Number - Legacy Beneficiary HICN field",
        json_schema_extra={"start_pos": 35, "end_pos": 45, "length": 11},
    )
    clm_type_cd: str | None = Field(
        default=None,
        description="Claim Type Code - Type of claim (71=RIC O local carrier non-DMEPOS, 72=RIC O local carrier DMEPOS)",
        json_schema_extra={"start_pos": 46, "end_pos": 47, "length": 2},
    )
    clm_line_ngaco_pbpmt_sw: str | None = Field(
        default=None,
        description="PBP Benefit Enhancement Indicator - Y/N indicates PBP benefit enhancement (used for NGACO and VTAPM)",
        json_schema_extra={"start_pos": 48, "end_pos": 48, "length": 1},
    )
    clm_line_ngaco_pdschrg_hcbs_sw: str | None = Field(
        default=None,
        description="Post Discharge Home Visit Benefit Enhancement Indicator - Y/N indicates Post Discharge Home Visit enhancement",
        json_schema_extra={"start_pos": 49, "end_pos": 49, "length": 1},
    )
    clm_line_ngaco_snf_wvr_sw: str | None = Field(
        default=None,
        description="SNF 3-Day Waiver Benefit Enhancement Indicator - Y/N indicates SNF 3-Day Waiver enhancement",
        json_schema_extra={"start_pos": 50, "end_pos": 50, "length": 1},
    )
    clm_line_ngaco_tlhlth_sw: str | None = Field(
        default=None,
        description="Telehealth Benefit Enhancement Indicator - Y/N indicates Telehealth enhancement",
        json_schema_extra={"start_pos": 51, "end_pos": 51, "length": 1},
    )
    clm_line_ngaco_cptatn_sw: str | None = Field(
        default=None,
        description="AIPBP Benefit Enhancement Indicator - Y/N indicates AIPBP enhancement",
        json_schema_extra={"start_pos": 52, "end_pos": 52, "length": 1},
    )
    clm_demo_1st_num: str | None = Field(
        default=None,
        description="First Program Demonstration Number - Medicare Demonstration Special Processing Number for BPCI",
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
        json_schema_extra={"start_pos": 63, "end_pos": 77, "length": 15},
    )
    clm_pbp_rdctn_amt: Decimal | None = Field(
        default=None,
        description="PBP/AIPBP Reduction Amount - Amount withheld from payment to provider",
        json_schema_extra={"start_pos": 78, "end_pos": 92, "length": 15},
    )
    clm_ngaco_cmg_wvr_sw: str | None = Field(
        default=None,
        description="Care Management Home Visits - Y/N indicates Care Management Home Visits enhancement",
        json_schema_extra={"start_pos": 93, "end_pos": 93, "length": 1},
    )
    clm_mdcr_ddctbl_amt: Decimal | None = Field(
        default=None,
        description="Claim Medicare Deductible Amount - Medicare deductible amount billed to Medicaid",
        json_schema_extra={"start_pos": 94, "end_pos": 112, "length": 19},
    )
    clm_sqstrtn_rdctn_amt: Decimal | None = Field(
        default=None,
        description="Part B Sequestration Reduction Amount - Payment reduction amount (2% since 04/01/2013)",
        json_schema_extra={"start_pos": 113, "end_pos": 127, "length": 15},
    )
    clm_line_carr_hpsa_scrcty_cd: str | None = Field(
        default=None,
        description="Claim Line Carrier HPSA Scarcity Code - Tracks Health Professional Shortage Area bonus payments",
        json_schema_extra={"start_pos": 128, "end_pos": 128, "length": 1},
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclfb":
        """Create instance from dictionary."""
        return cls(**data)
