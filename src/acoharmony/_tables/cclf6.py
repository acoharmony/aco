# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclf6 schema.

Generated from: _schemas/cclf6.yml

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
    with_transform,
)
from acoharmony._validators.field_validators import (
    HICN,
    MBI,
    NPI,
    hicn_validator,
    mbi_validator,
    npi_validator,
)


@register_schema(
    name="cclf6",
    version=2,
    tier="bronze",
    description="CCLF6 Part B DME File - Fixed-width file containing Part B durable medical equipment claims",
    file_patterns={
        "mssp": "P.A*.ACO.ZC6Y*.D*.T*",
        "reach": "P.D*.ACO.ZC6Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC6Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC6R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC6Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC6R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC6WY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZC6Y*.D*.T*",
        "reach": "P.D*.ACO.ZC6Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC6Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC6R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC6Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC6R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC6WY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclf6.parquet",
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
class Cclf6:
    """
    CCLF6 Part B DME File - Fixed-width file containing Part B durable medical equipment claims

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclf6.schema_name() -> str
        - Cclf6.schema_metadata() -> dict
        - Cclf6.parser_config() -> dict
        - Cclf6.transform_config() -> dict
        - Cclf6.lineage_config() -> dict
    """

    cur_clm_uniq_id: str = MBI(
        description="Current Claim Unique Identifier - A unique identification number assigned to the claim",
        json_schema_extra={"start_pos": 1, "end_pos": 13, "length": 13},
    )
    clm_line_num: str = NPI(
        description="Claim Line Number - A sequential number that identifies a specific claim line",
        json_schema_extra={"start_pos": 14, "end_pos": 23, "length": 10},
    )
    bene_mbi_id: str = HICN(
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
        description="Claim Type Code - Type of claim (81=RIC M DMERC non-DMEPOS, 82=RIC M DMERC DMEPOS)",
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
    clm_fed_type_srvc_cd: str | None = Field(
        default=None,
        description="Claim Federal Type Service Code - Type of service provided (consultation, surgery, etc.)",
        json_schema_extra={"start_pos": 68, "end_pos": 68, "length": 1},
    )
    clm_pos_cd: str | None = Field(
        default=None,
        description="Claim Place of Service Code - Where service was provided (ambulance, school, etc.)",
        json_schema_extra={"start_pos": 69, "end_pos": 70, "length": 2},
    )
    clm_line_from_dt: date | None = Field(
        default=None,
        description="Claim Line From Date - Date the service associated with the line item began",
        json_schema_extra={"start_pos": 71, "end_pos": 80, "length": 10},
    )
    clm_line_thru_dt: date | None = Field(
        default=None,
        description="Claim Line Thru Date - Date the service associated with the line item ended",
        json_schema_extra={"start_pos": 81, "end_pos": 90, "length": 10},
    )
    clm_line_hcpcs_cd: str | None = Field(
        default=None,
        description="HCPCS Code - Procedure/supply/product/service code",
        json_schema_extra={"start_pos": 91, "end_pos": 95, "length": 5},
    )
    clm_line_cvrd_pd_amt: Decimal | None = Field(
        default=None,
        description="Claim Line NCH Payment Amount - Medicare payment after deductible and coinsurance",
        json_schema_extra={"start_pos": 96, "end_pos": 110, "length": 15},
    )
    clm_prmry_pyr_cd: str | None = Field(
        default=None,
        description="Claim Primary Payer Code - Primary payer if not Medicare (blank=Medicare is primary)",
        json_schema_extra={"start_pos": 111, "end_pos": 111, "length": 1},
    )
    payto_prvdr_npi_num: str | None = Field(
        default=None,
        description="Pay-to Provider NPI Number - NPI of provider billing for the service",
        json_schema_extra={"start_pos": 112, "end_pos": 121, "length": 10},
    )
    ordrg_prvdr_npi_num: str | None = NPI(
        default=None,
        description="Ordering Provider NPI Number - NPI of provider ordering the service",
        json_schema_extra={"start_pos": 122, "end_pos": 131, "length": 10},
    )
    clm_carr_pmt_dnl_cd: str | None = NPI(
        default=None,
        description="Claim Carrier Payment Denial Code - To whom payment made or if denied",
        json_schema_extra={"start_pos": 132, "end_pos": 133, "length": 2},
    )
    clm_prcsg_ind_cd: str | None = Field(
        default=None,
        description="Claim Processing Indicator Code - If service allowed or reason denied",
        json_schema_extra={"start_pos": 134, "end_pos": 135, "length": 2},
    )
    clm_adjsmt_type_cd: str | None = Field(
        default=None,
        description="Claim Adjustment Type Code - 0=Original, 1=Cancellation, 2=Adjustment",
        json_schema_extra={"start_pos": 136, "end_pos": 137, "length": 2},
    )
    clm_efctv_dt: date | None = Field(
        default=None,
        description="Claim Effective Date - Date claim processed/added to NCH (Weekly Processing Date)",
        json_schema_extra={"start_pos": 138, "end_pos": 147, "length": 10},
    )
    clm_idr_ld_dt: date | None = Field(
        default=None,
        description="Claim IDR Load Date - When claim was loaded into IDR",
        json_schema_extra={"start_pos": 148, "end_pos": 157, "length": 10},
    )
    clm_cntl_num: str | None = Field(
        default=None,
        description="Claim Control Number - Unique number assigned by Medicare carrier",
        json_schema_extra={"start_pos": 158, "end_pos": 197, "length": 40},
    )
    bene_eqtbl_bic_hicn_num: str | None = Field(
        default=None,
        description="Beneficiary Equitable BIC HICN Number - Legacy Beneficiary Equitable BIC HICN Number",
        json_schema_extra={"start_pos": 198, "end_pos": 208, "length": 11},
    )
    clm_line_alowd_chrg_amt: Decimal | None = Field(
        default=None,
        description="Claim Line Allowed Charges Amount - Amount Medicare approved for payment",
        json_schema_extra={"start_pos": 209, "end_pos": 225, "length": 17},
    )
    clm_disp_cd: str | None = Field(
        default=None,
        description="Claim Disposition Code - Payment actions (01=Debit accepted, 02=Auto adjustment, 03=Cancel accepted)",
        json_schema_extra={"start_pos": 226, "end_pos": 227, "length": 2},
    )
    clm_blg_prvdr_npi_num: str | None = Field(
        default=None,
        description="Claim Pay-to Provider NPI Number - NPI of provider billing for the service",
        json_schema_extra={"start_pos": 228, "end_pos": 237, "length": 10},
    )
    clm_rfrg_prvdr_npi_num: str | None = NPI(
        default=None,
        description="Claim Ordering Provider NPI Number - NPI of provider ordering the service",
        json_schema_extra={"start_pos": 238, "end_pos": 247, "length": 10},
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")
    _validate_payto_prvdr_npi_num = npi_validator("payto_prvdr_npi_num")
    _validate_ordrg_prvdr_npi_num = npi_validator("ordrg_prvdr_npi_num")
    _validate_bene_eqtbl_bic_hicn_num = hicn_validator("bene_eqtbl_bic_hicn_num")
    _validate_clm_blg_prvdr_npi_num = npi_validator("clm_blg_prvdr_npi_num")
    _validate_clm_rfrg_prvdr_npi_num = npi_validator("clm_rfrg_prvdr_npi_num")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclf6":
        """Create instance from dictionary."""
        return cls(**data)
