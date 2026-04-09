# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cclf9 schema.

Generated from: _schemas/cclf9.yml

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
    mbi_validator,
)


@register_schema(
    name="cclf9",
    version=2,
    tier="bronze",
    description="CCLF9 Beneficiary XREF - Fixed-width file containing historical to current MBI crosswalk",
    file_patterns={
        "mssp": "P.A*.ACO.ZC9Y*.D*.T*",
        "reach": "P.D*.ACO.ZC9Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC9Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC9R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC9Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC9R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC9WY*.D*.T*",
    },
)
@with_parser(type="fixed_width", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={
        "mssp": "P.A*.ACO.ZC9Y*.D*.T*",
        "reach": "P.D*.ACO.ZC9Y*.D*.T*",
        "mssp_regular": "P.A*.ACO.ZC9Y*.D*.T*",
        "mssp_runout": "P.A*.ACO.ZC9R*.D*.T*",
        "reach_monthly": "P.D*.ACO.ZC9Y*.D*.T*",
        "reach_runout": "P.D*.ACO.ZC9R*.D*.T*",
        "mssp_weekly": "P.A*.ACO.ZC9WY*.D*.T*",
    },
    medallion_layer="bronze",
    silver={
        "output_name": "cclf9.parquet",
        "refresh_frequency": "weekly",
        "last_updated_by": "aco transform cclf9",
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
class Cclf9:
    """
    CCLF9 Beneficiary XREF - Fixed-width file containing historical to current MBI crosswalk

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Cclf9.schema_name() -> str
        - Cclf9.schema_metadata() -> dict
        - Cclf9.parser_config() -> dict
        - Cclf9.transform_config() -> dict
        - Cclf9.lineage_config() -> dict
    """

    hicn_mbi_xref_ind: str = MBI(
        description="HICN/MBI XREF Indicator (M = MBI)",
        json_schema_extra={"start_pos": 1, "end_pos": 1, "length": 1},
    )
    crnt_num: str = Field(
        description="Current Beneficiary Identifier (Current Beneficiary MBI)",
        json_schema_extra={"start_pos": 2, "end_pos": 12, "length": 11},
    )
    prvs_num: str = Field(
        description="Previous Beneficiary Identifier (Previous Beneficiary MBI)",
        json_schema_extra={"start_pos": 13, "end_pos": 23, "length": 11},
    )
    prvs_id_efctv_dt: date | None = Field(
        default=None,
        description="Previous Identifier Effective Date - The date the previous identifier became active",
        json_schema_extra={"start_pos": 24, "end_pos": 33, "length": 10},
    )
    prvs_id_obslt_dt: date | None = Field(
        default=None,
        description="Previous Identifier Obsolete Date - The date the previous identifier ceased to be active",
        json_schema_extra={"start_pos": 34, "end_pos": 43, "length": 10},
    )
    bene_rrb_num: str | None = Field(
        default=None,
        description="Beneficiary Railroad Board Number (Legacy RRB number)",
        json_schema_extra={"start_pos": 44, "end_pos": 55, "length": 12},
    )

    # Field Validators (from centralized _validators module)
    _validate_hicn_mbi_xref_ind = mbi_validator("hicn_mbi_xref_ind")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Cclf9":
        """Create instance from dictionary."""
        return cls(**data)
