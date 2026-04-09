# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclf8 schema.

Generated from: _schemas/cclf8.yml

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
    MBI,
    ZIP5,
    mbi_validator,
    zip5_validator,
)


@register_schema(
    name="cclf8",
    version=2,
    tier="bronze",
    description="CCLF8 Beneficiary Demographics - Fixed-width file containing beneficiary demographic and enrollment information",
    file_patterns={
        "mssp": "P.A*.ACO.ZC8Y*.D*.T*",
        "reach": "P.D*.ACO.ZC8Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC8Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC8R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC8Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC8R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC8WY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZC8Y*.D*.T*",
        "reach": "P.D*.ACO.ZC8Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC8Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC8R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC8Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC8R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC8WY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclf8.parquet",
        "refresh_frequency": "weekly",
        "last_updated_by": "aco transform cclf8",
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
class Cclf8:
    """
    CCLF8 Beneficiary Demographics - Fixed-width file containing beneficiary demographic and enrollment information

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclf8.schema_name() -> str
        - Cclf8.schema_metadata() -> dict
        - Cclf8.parser_config() -> dict
        - Cclf8.transform_config() -> dict
        - Cclf8.lineage_config() -> dict
    """

    bene_mbi_id: str = MBI(
        description="Medicare Beneficiary Identifier",
        json_schema_extra={"start_pos": 1, "end_pos": 11, "length": 11},
    )
    bene_hic_num: str | None = ZIP5(default=None, description="Beneficiary HIC Number (deprecated)", json_schema_extra={"start_pos": 12, "end_pos": 22, "length": 11})
    bene_fips_state_cd: str | None = Field(default=None, description="Beneficiary FIPS State Code", json_schema_extra={"start_pos": 23, "end_pos": 24, "length": 2})
    bene_fips_cnty_cd: str | None = Field(default=None, description="Beneficiary FIPS County Code", json_schema_extra={"start_pos": 25, "end_pos": 27, "length": 3})
    bene_zip_cd: str | None = Field(default=None, description="Beneficiary ZIP Code", json_schema_extra={"start_pos": 28, "end_pos": 32, "length": 5})
    bene_dob: date | None = Field(default=None, description="Beneficiary Date of Birth", json_schema_extra={"start_pos": 33, "end_pos": 42, "length": 10})
    bene_sex_cd: str | None = Field(
        default=None, description="Beneficiary Sex Code (1=Male, 2=Female, 0=Unknown)",
        json_schema_extra={"start_pos": 43, "end_pos": 43, "length": 1},
    )
    bene_race_cd: str | None = Field(default=None, description="Beneficiary Race Code", json_schema_extra={"start_pos": 44, "end_pos": 44, "length": 1})
    bene_age: str | None = Field(default=None, description="Beneficiary Age", json_schema_extra={"start_pos": 45, "end_pos": 47, "length": 3})
    bene_mdcr_stus_cd: str | None = Field(default=None, description="Medicare Status Code", json_schema_extra={"start_pos": 48, "end_pos": 49, "length": 2})
    bene_dual_stus_cd: str | None = Field(
        default=None, description="Dual Status Code (Medicare/Medicaid)",
        json_schema_extra={"start_pos": 50, "end_pos": 51, "length": 2},
    )
    bene_death_dt: date | None = Field(default=None, description="Beneficiary Date of Death", json_schema_extra={"start_pos": 52, "end_pos": 61, "length": 10})
    bene_rng_bgn_dt: date | None = Field(
        default=None, description="Date beneficiary enrolled in Hospice",
        json_schema_extra={"start_pos": 62, "end_pos": 71, "length": 10},
    )
    bene_rng_end_dt: date | None = Field(default=None, description="Date beneficiary ended Hospice", json_schema_extra={"start_pos": 72, "end_pos": 81, "length": 10})
    bene_fst_name: str | None = Field(default=None, description="Beneficiary First Name", json_schema_extra={"start_pos": 82, "end_pos": 111, "length": 30})
    bene_mdl_name: str | None = Field(default=None, description="Beneficiary Middle Name", json_schema_extra={"start_pos": 112, "end_pos": 126, "length": 15})
    bene_lst_name: str | None = Field(default=None, description="Beneficiary Last Name", json_schema_extra={"start_pos": 127, "end_pos": 166, "length": 40})
    bene_orgnl_entlmt_rsn_cd: str | None = Field(
        default=None, description="Beneficiary Original Entitlement Reason Code",
        json_schema_extra={"start_pos": 167, "end_pos": 167, "length": 1},
    )
    bene_entlmt_buyin_ind: str | None = Field(
        default=None, description="Beneficiary Entitlement Buy-in Indicator",
        json_schema_extra={"start_pos": 168, "end_pos": 168, "length": 1},
    )
    bene_part_a_enrlmt_bgn_dt: date | None = Field(
        default=None, description="Part A Enrollment Begin Date",
        json_schema_extra={"start_pos": 169, "end_pos": 178, "length": 10},
    )
    bene_part_b_enrlmt_bgn_dt: date | None = Field(
        default=None, description="Part B Enrollment Begin Date",
        json_schema_extra={"start_pos": 179, "end_pos": 188, "length": 10},
    )
    bene_line_1_adr: str | None = Field(
        default=None, description="Beneficiary Derived Mailing Line One Address",
        json_schema_extra={"start_pos": 189, "end_pos": 233, "length": 45},
    )
    bene_line_2_adr: str | None = Field(
        default=None, description="Beneficiary Derived Mailing Line Two Address",
        json_schema_extra={"start_pos": 234, "end_pos": 278, "length": 45},
    )
    bene_line_3_adr: str | None = Field(
        default=None, description="Beneficiary Derived Mailing Line Three Address",
        json_schema_extra={"start_pos": 279, "end_pos": 318, "length": 40},
    )
    bene_line_4_adr: str | None = Field(
        default=None, description="Beneficiary Derived Mailing Line Four Address",
        json_schema_extra={"start_pos": 319, "end_pos": 358, "length": 40},
    )
    bene_line_5_adr: str | None = Field(
        default=None, description="Beneficiary Derived Mailing Line Five Address",
        json_schema_extra={"start_pos": 359, "end_pos": 398, "length": 40},
    )
    bene_line_6_adr: str | None = Field(
        default=None, description="Beneficiary Derived Mailing Line Six Address",
        json_schema_extra={"start_pos": 399, "end_pos": 438, "length": 40},
    )
    bene_city: str | None = Field(
        alias="geo_zip_plc_name", default=None, description="Beneficiary City",
        json_schema_extra={"start_pos": 439, "end_pos": 538, "length": 100},
    )
    bene_state: str | None = ZIP5(
        alias="geo_usps_state_cd", default=None, description="Beneficiary State",
        json_schema_extra={"start_pos": 539, "end_pos": 540, "length": 2},
    )
    bene_zip: str | None = Field(
        alias="geo_zip5_cd", default=None, description="Beneficiary Zip Code",
        json_schema_extra={"start_pos": 541, "end_pos": 545, "length": 5},
    )
    bene_zip_ext: str | None = ZIP5(
        alias="geo_zip4_cd", default=None, description="Beneficiary Zip Code Extension",
        json_schema_extra={"start_pos": 546, "end_pos": 549, "length": 4},
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi_id = mbi_validator("bene_mbi_id")
    _validate_bene_zip_cd = zip5_validator("bene_zip_cd")
    _validate_bene_city = zip5_validator("bene_city")
    _validate_bene_zip = zip5_validator("bene_zip")
    _validate_bene_zip_ext = zip5_validator("bene_zip_ext")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclf8":
        """Create instance from dictionary."""
        return cls(**data)
