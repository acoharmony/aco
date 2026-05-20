# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for tparc schema.

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
    with_record_types,
    with_storage,
)
from acoharmony._validators.field_validators import (
    TIN,
    tin_validator,
)


@register_schema(
    name="tparc",
    version=2,
    tier="bronze",
    description="Schema for Weekly Claims Reduction File",
    file_patterns={"reach": ["*TPARC*.txt"]},
)
@with_parser(type="tparc", encoding="utf-8", has_header=False, embedded_transforms=False, delimiter=";")
@with_record_types(
    record_types={
        "CLMH": {
            "columns": [
                {"name": "record_type", "data_type": "string"},
                {"name": "line_number", "data_type": "string"},
                {"name": "patient_id", "data_type": "string"},
                {"name": "plan_code", "data_type": "string"},
                {"name": "claim_number", "data_type": "string"},
                {"name": "member_id", "data_type": "string"},
                {"name": "reserved1", "data_type": "string"},
                {"name": "from_date", "data_type": "string"},
                {"name": "thru_date", "data_type": "string"},
            ]
        },
        "CLML": {
            "columns": [
                {"name": "record_type", "data_type": "string"},
                {"name": "line_number", "data_type": "string"},
                {"name": "reserved1", "data_type": "string"},
                {"name": "rendering_provider_npi", "data_type": "string"},
                {"name": "rendering_provider_tin", "data_type": "string"},
                {"name": "diagnosis_code", "data_type": "string"},
                {"name": "reserved2", "data_type": "string"},
                {"name": "reserved3", "data_type": "string"},
                {"name": "reserved4", "data_type": "string"},
                {"name": "from_date", "data_type": "string"},
                {"name": "thru_date", "data_type": "string"},
                {"name": "service_units", "data_type": "string"},
                {"name": "total_charge_amt", "data_type": "decimal"},
                {"name": "allowed_charge_amt", "data_type": "decimal"},
                {"name": "covered_paid_amt", "data_type": "decimal"},
                {"name": "coinsurance_amt", "data_type": "decimal"},
                {"name": "deductible_amt", "data_type": "decimal"},
                {"name": "reserved5", "data_type": "string"},
                {"name": "reserved6", "data_type": "string"},
                {"name": "sequestration_amt", "data_type": "decimal"},
                {"name": "reserved7", "data_type": "string"},
                {"name": "reserved8", "data_type": "string"},
                {"name": "pcc_reduction_amt", "data_type": "decimal"},
                {"name": "rev_code", "data_type": "string"},
                {"name": "hcpcs_code", "data_type": "string"},
                {"name": "hcpcs_modifier1", "data_type": "string"},
                {"name": "hcpcs_modifier2", "data_type": "string"},
                {"name": "hcpcs_modifier3", "data_type": "string"},
                {"name": "hcpcs_modifier4", "data_type": "string"},
                {"name": "reserved9", "data_type": "string"},
                {"name": "patient_control_num", "data_type": "string"},
                {"name": "place_of_service", "data_type": "integer"},
                {"name": "carc_code", "data_type": "integer"},
                {"name": "rarc_code", "data_type": "string"},
                {"name": "group_code", "data_type": "string"},
                {"name": "reserved10", "data_type": "string"},
            ]
        },
    }
)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*TPARC*.txt"]},
    medallion_layer="bronze",
    silver={"output_name": "tparc.parquet"},
)
@with_four_icli(
    category="Reports",
    file_type_code=157,
    file_pattern="P.D????.TPARC.RP.D??????.T*.txt",
    extract_zip=False,
    refresh_frequency="weekly",
    default_date_filter={"createdWithinLastWeek": True},
)
@dataclass
class Tparc:
    """
    Schema for Weekly Claims Reduction File

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Tparc.schema_name() -> str
        - Tparc.schema_metadata() -> dict
        - Tparc.parser_config() -> dict
        - Tparc.transform_config() -> dict
        - Tparc.lineage_config() -> dict
    """

    record_type: str | None = Field(default=None, description="record_type field")
    line_number: int | None = Field(default=None, description="line_number field", ge=0)
    rev_code: str | None = Field(default=None, description="rev_code field")
    rendering_provider_tin: str | None = TIN(
        default=None, description="rendering_provider_tin field"
    )
    from_date: int | None = Field(default=None, description="from_date field")
    thru_date: int | None = Field(default=None, description="thru_date field")
    service_units: int | None = Field(default=None, description="service_units field")
    total_charge_amt: Decimal | None = Field(default=None, description="total_charge_amt field")
    allowed_charge_amt: Decimal | None = Field(default=None, description="allowed_charge_amt field")
    covered_paid_amt: Decimal | None = Field(default=None, description="covered_paid_amt field")
    coinsurance_amt: Decimal | None = Field(default=None, description="coinsurance_amt field")
    deductible_amt: Decimal | None = Field(default=None, description="deductible_amt field")
    sequestration_amt: Decimal | None = Field(default=None, description="sequestration_amt field")
    pcc_reduction_amt: Decimal | None = Field(default=None, description="pcc_reduction_amt field")
    hcpcs_code: str | None = Field(default=None, description="hcpcs_code field")
    hcpcs_modifier1: str | None = Field(default=None, description="hcpcs_modifier1 field")
    patient_control_num: str | None = Field(default=None, description="patient_control_num field")
    place_of_service: int | None = Field(default=None, description="place_of_service field")
    carc_code: int | None = Field(default=None, description="carc_code field")
    rarc_code: str | None = Field(default=None, description="rarc_code field")
    group_code: str | None = Field(default=None, description="group_code field")
    source_file: str | None = Field(default=None, description="source_file field")
    source_filename: str | None = Field(default=None, description="source_filename field")
    processed_at: date | None = Field(default=None, description="processed_at field")

    # Field Validators (from centralized _validators module)
    _validate_rendering_provider_tin = tin_validator("rendering_provider_tin")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Tparc":
        """Create instance from dictionary."""
        return cls(**data)
