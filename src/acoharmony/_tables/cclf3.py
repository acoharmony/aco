# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclf3 schema.

Generated from: _schemas/cclf3.yml

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
    with_four_icli,
    with_parser,
    with_storage,
    with_transform,
)
from acoharmony._validators.field_validators import (
    HICN,
    MBI,
    hicn_validator,
    mbi_validator,
)


@register_schema(
    name="cclf3",
    version=2,
    tier="bronze",
    description="CCLF3 Part A Procedure Code File - Fixed-width file containing Part A procedure codes performed during claims period",
    file_patterns={
        "mssp": "P.A*.ACO.ZC3Y*.D*.T*",
        "reach": "P.D*.ACO.ZC3Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC3Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC3R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC3Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC3R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC3WY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZC3Y*.D*.T*",
        "reach": "P.D*.ACO.ZC3Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC3Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC3R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC3Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC3R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC3WY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclf3.parquet",
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
class Cclf3:
    """
    CCLF3 Part A Procedure Code File - Fixed-width file containing Part A procedure codes performed during claims period

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclf3.schema_name() -> str
        - Cclf3.schema_metadata() -> dict
        - Cclf3.parser_config() -> dict
        - Cclf3.transform_config() -> dict
        - Cclf3.lineage_config() -> dict
    """

    cur_clm_uniq_id: str = MBI(
        description="Current Claim Unique Identifier - A unique identification number assigned to the claim",
        json_schema_extra={"start_pos": 1, "end_pos": 13, "length": 13},
    )
    bene_mbi_id: str = HICN(
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
    clm_val_sqnc_num: str | None = Field(
        default=None,
        description="Claim Value Sequence Number - An arbitrary sequential number that uniquely identifies a procedure code record within the claim",
        json_schema_extra={"start_pos": 38, "end_pos": 39, "length": 2},
    )
    clm_prcdr_cd: str | None = Field(
        default=None,
        description="Procedure Code - The ICD-9/10 code that indicates the procedure performed during the period covered by the claim",
        json_schema_extra={"start_pos": 40, "end_pos": 46, "length": 7},
    )
    clm_prcdr_prfrm_dt: date | None = Field(
        default=None,
        description="Procedure Performed Date - The date the indicated procedure was performed",
        json_schema_extra={"start_pos": 47, "end_pos": 56, "length": 10},
    )
    bene_eqtbl_bic_hicn_num: str | None = Field(
        default=None,
        description="Beneficiary Equitable BIC HICN Number - Legacy Beneficiary Equitable BIC HICN Number",
        json_schema_extra={"start_pos": 57, "end_pos": 67, "length": 11},
    )
    prvdr_oscar_num: str | None = Field(
        default=None,
        description="Provider OSCAR Number - Medicare/Medicaid Provider Number or CCN from PECOS",
        json_schema_extra={"start_pos": 68, "end_pos": 73, "length": 6},
    )
    clm_from_dt: date | None = Field(
        default=None,
        description="Claim From Date - First day on billing statement (Statement Covers From Date)",
        json_schema_extra={"start_pos": 74, "end_pos": 83, "length": 10},
    )
    clm_thru_dt: date | None = Field(
        default=None,
        description="Claim Thru Date - Last day on billing statement (Statement Covers Through Date)",
        json_schema_extra={"start_pos": 84, "end_pos": 93, "length": 10},
    )
    dgns_prcdr_icd_ind: str | None = Field(
        default=None,
        description="ICD Version Indicator - 9=ICD-9, 0=ICD-10, U=any value other than 9 or 0 in source data",
        json_schema_extra={"start_pos": 94, "end_pos": 94, "length": 1},
    )
    clm_blg_prvdr_oscar_num: str | None = Field(
        default=None,
        description="Claim Provider OSCAR Number - Facility Medicare/Medicaid ID from claims processing system (as of Jan 2022)",
        json_schema_extra={"start_pos": 95, "end_pos": 114, "length": 20},
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")
    _validate_bene_eqtbl_bic_hicn_num = hicn_validator("bene_eqtbl_bic_hicn_num")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclf3":
        """Create instance from dictionary."""
        return cls(**data)
