# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for enrollment schema.

Generated from: _schemas/enrollment.yml

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
    with_deduplication,
    with_staging,
    with_standardization,
    with_storage,
    with_transform,
    with_tuva,
    with_xref,
)
from acoharmony._validators.field_validators import (
    ZIP5,
    zip5_validator,
)


@register_schema(name="enrollment", version=2, tier="silver", description="""\2""")
@with_transform()
@with_storage(tier="silver", medallion_layer="silver", gold={"output_name": "enrollment.parquet"})
@with_staging(source="beneficiary_demographics")
@with_deduplication(
    key=["bene_mbi_id"],
    sort_by=["bene_part_a_enrlmt_bgn_dt", "bene_part_b_enrlmt_bgn_dt"],
    keep="last",
)
@with_standardization(
    rename_columns={
        "bene_part_b_enrlmt_bgn_dt": "enrollment_start_date",
        "source_file": "file_name",
    },
    add_columns=[
        {"name": "person_id", "value": "current_bene_mbi_id"},
        {"name": "payer", "value": "Medicare"},
        {"name": "payer_type", "value": "Medicare"},
        {"name": "plan", "value": "ACO"},
    ],
    add_computed={
        "bene_member_month": "format_year_month_from_enrollment_start",
        "enrollment_end_date": "enrollment_end_date_with_death_truncation",
    },
)
@with_tuva(
    models={"intermediate": ["int_enrollment"], "final": ["eligibility"]},
    inject=["int_enrollment"],
)
@with_xref(
    table="beneficiary_xref",
    join_key="bene_mbi_id",
    xref_key="prvs_num",
    current_column="crnt_num",
    output_column="current_bene_mbi_id",
    description="Apply MBI crosswalk to get current MBI",
)
@dataclass
class Enrollment:
    """
    Custom enrollment input combining beneficiary demographics with assignment periods

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Enrollment.schema_name() -> str
        - Enrollment.schema_metadata() -> dict
        - Enrollment.parser_config() -> dict
        - Enrollment.transform_config() -> dict
        - Enrollment.lineage_config() -> dict
    """

    member_id: str = Field(description="Medicare Beneficiary Identifier")
    person_id: str = Field(description="Person identifier (same as member_id)")
    enrollment_start_date: date = Field(description="Start date of enrollment period")
    enrollment_end_date: date = Field(description="End date of enrollment period")
    payer: str = Field(default="Medicare", description="Payer name")
    payer_type: str | None = Field(default="Medicare", description="Type of payer")
    plan: str | None = Field(default="ACO", description="Insurance plan")
    gender: str | None = Field(default=None, description="Member gender")
    birth_date: date | None = Field(default=None, description="Member birth date")
    death_date: date | None = Field(default=None, description="Member death date (if applicable)")
    race: str | None = Field(default=None, description="Member race")
    ethnicity: str | None = Field(default=None, description="Member ethnicity")
    state: str | None = Field(default=None, description="State code")
    zip_code: str | None = ZIP5(default=None, description="ZIP code")
    county: str | None = Field(default=None, description="County code")
    dual_status_code: str | None = Field(
        default=None, description="Medicare/Medicaid dual eligibility status"
    )
    medicare_status_code: str | None = Field(default=None, description="Medicare enrollment status")
    original_reason_entitlement_code: str | None = Field(
        default=None, description="Original reason for Medicare entitlement"
    )
    program_type: str | None = Field(
        default=None, description="ACO program type (MSSP, REACH, etc.)"
    )
    assignment_method: str | None = Field(
        default=None, description="How member was assigned (claims-based, voluntary, etc.)"
    )
    data_source: str | None = Field(default="CCLF", description="Source of the data")
    processed_date: date | None = Field(default=None, description="Date record was processed")

    # Field Validators (from centralized _validators module)
    _validate_zip_code = zip5_validator("zip_code")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Enrollment":
        """Create instance from dictionary."""
        return cls(**data)
