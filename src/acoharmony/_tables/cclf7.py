# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclf7 schema.

Generated from: _schemas/cclf7.yml

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
    MBI,
    NDC,
    mbi_validator,
    ndc_validator,
)


@register_schema(
    name="cclf7",
    version=2,
    tier="bronze",
    description="CCLF7 Part D File - Fixed-width file containing Part D prescription drug event data",
    file_patterns={
        "mssp": "P.A*.ACO.ZC7Y*.D*.T*",
        "reach": "P.D*.ACO.ZC7Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC7Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC7R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC7Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC7R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC7WY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZC7Y*.D*.T*",
        "reach": "P.D*.ACO.ZC7Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC7Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC7R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC7Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC7R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC7WY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclf7.parquet",
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
class Cclf7:
    """
    CCLF7 Part D File - Fixed-width file containing Part D prescription drug event data

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclf7.schema_name() -> str
        - Cclf7.schema_metadata() -> dict
        - Cclf7.parser_config() -> dict
        - Cclf7.transform_config() -> dict
        - Cclf7.lineage_config() -> dict
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
    clm_line_ndc_cd: str | None = Field(
        default=None,
        description="NDC Code - A universal unique product identifier for human drugs",
        json_schema_extra={"start_pos": 36, "end_pos": 46, "length": 11},
    )
    clm_type_cd: str | None = Field(
        default=None,
        description="Claim Type Code - Type of claim (01=Original, 02=Adjusted PDE, 03=Deleted, 04=Resubmitted PDE)",
        json_schema_extra={"start_pos": 47, "end_pos": 48, "length": 2},
    )
    clm_line_from_dt: date | None = Field(
        default=None,
        description="Claim Line From Date - Date prescription was filled",
        json_schema_extra={"start_pos": 49, "end_pos": 58, "length": 10},
    )
    prvdr_srvc_id_qlfyr_cd: str | None = Field(
        default=None,
        description="Provider Service Identifier Qualifier Code - Type of pharmacy ID (01=NPI, 06=UPIN, 07=NCPDP, 08=State License, 11=TIN, 99=Other)",
        json_schema_extra={"start_pos": 59, "end_pos": 60, "length": 2},
    )
    clm_srvc_prvdr_gnrc_id_num: str | None = Field(
        default=None,
        description="Claim Service Provider Generic ID Number - Pharmacy ID number",
        json_schema_extra={"start_pos": 61, "end_pos": 80, "length": 20},
    )
    clm_dspnsng_stus_cd: str | None = Field(
        default=None,
        description="Claim Dispensing Status Code - Prescription fulfillment status (P=Partially filled, C=Completely filled)",
        json_schema_extra={"start_pos": 81, "end_pos": 81, "length": 1},
    )
    clm_daw_prod_slctn_cd: str | None = Field(
        default=None,
        description="Claim Dispense as Written Product Selection Code - Generic substitution instructions (0-9)",
        json_schema_extra={"start_pos": 82, "end_pos": 82, "length": 1},
    )
    clm_line_srvc_unit_qty: Decimal | None = Field(
        default=None,
        description="Claim Line Service Unit Quantity - Count of units dispensed",
        json_schema_extra={"start_pos": 83, "end_pos": 106, "length": 24},
    )
    clm_line_days_suply_qty: str | None = Field(
        default=None,
        description="Claim Line Days Supply Quantity - Number of days medication will cover",
        json_schema_extra={"start_pos": 107, "end_pos": 115, "length": 9},
    )
    prvdr_prsbng_id_qlfyr_cd: str | None = Field(
        default=None,
        description="Provider Prescribing ID Qualifier Code - Type of prescriber ID (01=NPI, 06=UPIN, 07=NCPDP, 08=State License, 11=TIN, 12=DEA, 99=Other)",
        json_schema_extra={"start_pos": 116, "end_pos": 117, "length": 2},
    )
    blank_placeholder: str | None = Field(
        default=None,
        description="BLANK - Placeholder for retired field CLM_PRSBNG_PRVDR_GNRC_ID_NUM",
        json_schema_extra={"start_pos": 118, "end_pos": 137, "length": 20},
    )
    clm_line_bene_pmt_amt: Decimal | None = Field(
        default=None,
        description="Claim Line Beneficiary Payment Amount - Amount paid by beneficiary (copay, coinsurance, deductible)",
        json_schema_extra={"start_pos": 138, "end_pos": 150, "length": 13},
    )
    clm_adjsmt_type_cd: str | None = Field(
        default=None,
        description="Claim Adjustment Type Code - 0=Original, 1=Cancellation, 2=Adjustment",
        json_schema_extra={"start_pos": 151, "end_pos": 152, "length": 2},
    )
    clm_efctv_dt: date | None = Field(
        default=None,
        description="Claim Effective Date - Date claim processed/added to NCH (Weekly Processing Date)",
        json_schema_extra={"start_pos": 153, "end_pos": 162, "length": 10},
    )
    clm_idr_ld_dt: date | None = Field(
        default=None,
        description="Claim IDR Load Date - When claim was loaded into IDR",
        json_schema_extra={"start_pos": 163, "end_pos": 172, "length": 10},
    )
    clm_line_rx_srvc_rfrnc_num: str | None = Field(
        default=None,
        description="Claim Line Prescription Service Reference Number - Identifies prescription by provider and date",
        json_schema_extra={"start_pos": 173, "end_pos": 184, "length": 12},
    )
    clm_line_rx_fill_num: str | None = Field(
        default=None,
        description="Claim Line Prescription Fill Number - Sequential order of fills/refills",
        json_schema_extra={"start_pos": 185, "end_pos": 193, "length": 9},
    )
    clm_phrmcy_srvc_type_cd: str | None = Field(
        default=None,
        description="Claim Pharmacy Service Type Code - Type of pharmacy (1=Community, 2=Compounding, 3=Home Infusion, 4=Institutional, 5=LTC, 6=Mail Order, 7=MCO, 8=Specialty, 99=Other)",
        json_schema_extra={"start_pos": 194, "end_pos": 195, "length": 2},
    )
    clm_prsbng_prvdr_gnrc_id_num: str | None = Field(
        default=None,
        description="Claim Prescribing Provider Generic ID Number - Prescriber ID number",
        json_schema_extra={"start_pos": 196, "end_pos": 230, "length": 35},
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")
    _validate_clm_line_ndc_cd = ndc_validator("clm_line_ndc_cd")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclf7":
        """Create instance from dictionary."""
        return cls(**data)
