# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for alr schema.

Generated from: _schemas/alr.yml

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
    with_foreign_keys,
    with_keys,
    with_parser,
    with_polars,
    with_standardization,
    with_storage,
    with_transform,
    with_xref,
)
from acoharmony._validators.field_validators import (
    MBI,
    mbi_validator,
)


@register_schema(
    name="alr",
    version=2,
    tier="bronze",
    description="Assignment List Report (ALR) - CSV file with beneficiary assignment for MSSP ACOs",
    file_patterns={
        "annual": "*AALR*.csv",
        "quarterly": "*QALR*.csv",
        "report_year_extraction": {"annual": "Y(\\d{4})", "quarterly": "(\\d{4})Q(\\d)"},
    },
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={
        "annual": "*AALR*_1-2.csv",
        "quarterly": "*QALR*_1-2.csv",
        "report_year_extraction": {"annual": "Y(\\d{4})", "quarterly": "(\\d{4})Q(\\d)"},
    },
    medallion_layer="bronze",
    silver={
        "output_name": "alr.parquet",
        "refresh_frequency": "quarterly",
        "last_updated_by": "aco transform alr",
    },
)
@with_standardization(
    rename_columns={
        "bene_mbi": "member_id",
        "bene_first_name": "first_name",
        "bene_last_name": "last_name",
        "bene_birth_dt": "date_of_birth",
        "bene_death_dt": "date_of_death",
        "bene_sex_cd": "gender_code",
    },
    add_columns=[
        {"name": "assignment_type", "value": "MSSP"},
        {"name": "processed_date", "value": "current_timestamp()"},
        {"name": "source_file", "value": "alr"},
    ],
)
@with_xref(
    table="beneficiary_xref",
    join_key="bene_mbi",
    xref_key="prvs_num",
    current_column="crnt_num",
    output_column="current_bene_mbi",
    description="Apply MBI crosswalk to get current MBI",
)
@with_keys(
    primary_key=["bene_mbi"],
    natural_key=["bene_mbi"],
    deduplication_key=["bene_mbi", "master_id"],
    foreign_keys=[
        {"column": "bene_mbi", "references": "cclf8.bene_mbi"},
    ],
)
@with_foreign_keys(
    description="Relationships to other tables",
    keys=[
        {
            "column": "bene_mbi",
            "references": "beneficiary_demographics.bene_mbi_id",
            "type": "many-to-one",
        },
        {
            "column": "current_bene_mbi",
            "references": "beneficiary_xref.crnt_num",
            "type": "many-to-one",
        },
    ],
)
@with_polars(
    lazy_evaluation=True,
    drop_columns=["bene_hic_num"],
    string_trim=True,
    categorical_columns=["bene_sex_cd"],
)
@dataclass
class Alr:
    """
    Assignment List Report (ALR) - CSV file with beneficiary assignment for MSSP ACOs

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Alr.schema_name() -> str
        - Alr.schema_metadata() -> dict
        - Alr.parser_config() -> dict
        - Alr.transform_config() -> dict
        - Alr.lineage_config() -> dict
    """

    bene_mbi: str = MBI(
        alias="BENE_MBI_ID",
        description="Medicare Beneficiary Identifier",
    )
    bene_hic_num: str | None = Field(
        alias="BENE_HIC_NUM", default=None, description="Beneficiary HIC Number (deprecated)"
    )
    bene_first_name: str | None = Field(
        alias="BENE_1ST_NAME", default=None, description="Beneficiary First Name"
    )
    bene_last_name: str | None = Field(
        alias="BENE_LAST_NAME", default=None, description="Beneficiary Last Name"
    )
    bene_sex_cd: str | None = Field(
        alias="BENE_SEX_CD", default=None, description="Beneficiary Sex Code"
    )
    bene_birth_dt: date | None = Field(
        alias="BENE_BRTH_DT", default=None, description="Beneficiary Birth Date"
    )
    death_date: date | None = Field(
        alias="BENE_DEATH_DT", default=None, description="Beneficiary Death Date"
    )
    master_id: str | None = Field(alias="MASTER_ID", default=None, description="Master Identifier")
    b_em_line_cnt_t: str | None = Field(
        alias="B_EM_LINE_CNT_T", default=None, description="E&M Line Count Total"
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi = mbi_validator("bene_mbi")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Alr":
        """Create instance from dictionary."""
        return cls(**data)
