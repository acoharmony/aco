# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclf2 schema.

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
    mbi_validator,
)


@register_schema(
    name="cclf2",
    version=2,
    tier="bronze",
    description="CCLF2 Part A Claims Revenue Center Detail File - Fixed-width file containing Part A revenue center line item details",
    file_patterns={
        "mssp": "P.A*.ACO.ZC2Y*.D*.T*",
        "reach": "P.D*.ACO.ZC2Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC2Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC2R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC2Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC2R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC2WY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZC2Y*.D*.T*",
        "reach": "P.D*.ACO.ZC2Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC2Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC2R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC2Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC2R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC2WY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclf2.parquet",
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
class Cclf2:
    """
    CCLF2 Part A Claims Revenue Center Detail File - Fixed-width file containing Part A revenue center line item details

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclf2.schema_name() -> str
        - Cclf2.schema_metadata() -> dict
        - Cclf2.parser_config() -> dict
        - Cclf2.transform_config() -> dict
        - Cclf2.lineage_config() -> dict
    """

    cur_clm_uniq_id: str = Field(
        description="Current Claim Unique Identifier - A unique identification number assigned to the claim",
        json_schema_extra={"start_pos": 1, "end_pos": 13, "length": 13},
    )
    clm_line_num: str = Field(
        description="Claim Line Number - A sequential number that identifies a specific claim line",
        json_schema_extra={"start_pos": 14, "end_pos": 23, "length": 10},
    )
    bene_mbi_id: str = MBI(
        description="Medicare Beneficiary Identifier - A Medicare Beneficiary Identifier assigned to a beneficiary",
        json_schema_extra={"start_pos": 24, "end_pos": 34, "length": 11},
    )
    bene_hic_num: str | None = Field(
        default=None,
        description="Beneficiary HIC Number - Legacy field, blank as of January 1, 2020",
        json_schema_extra={"start_pos": 35, "end_pos": 45, "length": 11},
    )
    clm_type_cd: str | None = Field(
        default=None,
        description="Claim Type Code - Type of claim (10=HHA, 20=SNF, 30=Swing bed SNF, 40=Outpatient, 50=Hospice, 60=Inpatient, 61=Inpatient Full-Encounter)",
        json_schema_extra={"start_pos": 46, "end_pos": 47, "length": 2},
    )
    clm_line_from_dt: date | None = Field(
        default=None,
        description="Claim Line From Date - The date the service associated with the line item began",
        json_schema_extra={"start_pos": 48, "end_pos": 57, "length": 10},
    )
    clm_line_thru_dt: date | None = Field(
        default=None,
        description="Claim Line Thru Date - The date the service associated with the line item ended",
        json_schema_extra={"start_pos": 58, "end_pos": 67, "length": 10},
    )
    clm_line_prod_rev_ctr_cd: str | None = Field(
        default=None,
        description="Product Revenue Center Code - Cost center code for billing (0001 = total of all revenue centers)",
        json_schema_extra={"start_pos": 68, "end_pos": 71, "length": 4},
    )
    clm_line_instnl_rev_ctr_dt: date | None = Field(
        default=None,
        description="Claim Line Institutional Revenue Center Date - Date that applies to the Revenue Center code service",
        json_schema_extra={"start_pos": 72, "end_pos": 81, "length": 10},
    )
    clm_line_hcpcs_cd: str | None = Field(
        default=None,
        description="HCPCS Code - Procedure/supply/product/service code (HIPPS code when Revenue Center is 0022)",
        json_schema_extra={"start_pos": 82, "end_pos": 86, "length": 5},
    )
    bene_eqtbl_bic_hicn_num: str | None = Field(
        default=None,
        description="Beneficiary Equitable BIC HICN Number - Legacy field, blank as of January 1, 2020",
        json_schema_extra={"start_pos": 87, "end_pos": 97, "length": 11},
    )
    prvdr_oscar_num: str | None = Field(
        default=None,
        description="Provider OSCAR Number - Medicare/Medicaid Provider Number or CCN from PECOS",
        json_schema_extra={"start_pos": 98, "end_pos": 103, "length": 6},
    )
    clm_from_dt: date | None = Field(
        default=None,
        description="Claim From Date - First day on billing statement (Statement Covers From Date)",
        json_schema_extra={"start_pos": 104, "end_pos": 113, "length": 10},
    )
    clm_thru_dt: date | None = Field(
        default=None,
        description="Claim Thru Date - Last day on billing statement (Statement Covers Through Date)",
        json_schema_extra={"start_pos": 114, "end_pos": 123, "length": 10},
    )
    clm_line_srvc_unit_qty: Decimal | None = Field(
        default=None,
        description="Claim Line Service Unit Quantity - Count of units for services needing unit reporting",
        json_schema_extra={"start_pos": 124, "end_pos": 147, "length": 24},
    )
    clm_line_cvrd_pd_amt: Decimal | None = Field(
        default=None,
        description="Claim Line Covered Paid Amount - Amount Medicare reimbursed for covered services",
        json_schema_extra={"start_pos": 148, "end_pos": 164, "length": 17},
    )
    hcpcs_1_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS First Modifier Code - First modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 165, "end_pos": 166, "length": 2},
    )
    hcpcs_2_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS Second Modifier Code - Second modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 167, "end_pos": 168, "length": 2},
    )
    hcpcs_3_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS Third Modifier Code - Third modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 169, "end_pos": 170, "length": 2},
    )
    hcpcs_4_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS Fourth Modifier Code - Fourth modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 171, "end_pos": 172, "length": 2},
    )
    hcpcs_5_mdfr_cd: str | None = Field(
        default=None,
        description="HCPCS Fifth Modifier Code - Fifth modifier for HCPCS procedure code",
        json_schema_extra={"start_pos": 173, "end_pos": 174, "length": 2},
    )
    clm_rev_apc_hipps_cd: str | None = Field(
        default=None,
        description="Claim Revenue APC HIPPS Code - APC group for outpatient claim type",
        json_schema_extra={"start_pos": 175, "end_pos": 179, "length": 5},
    )
    clm_fac_prvdr_oscar_num: str | None = Field(
        default=None,
        description="Claim Facility Provider OSCAR Number - Facility Medicare/Medicaid ID from claims processing system (as of Jan 2022)",
        json_schema_extra={"start_pos": 180, "end_pos": 199, "length": 20},
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclf2":
        """Create instance from dictionary."""
        return cls(**data)
