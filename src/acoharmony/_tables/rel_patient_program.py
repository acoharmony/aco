# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for rel_patient_program schema.

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
    with_parser,
)


@register_schema(name="rel_patient_program", version=2, tier="bronze", description="""\2""")
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@dataclass
class RelPatientProgram:
    """
    Patient program enrollment relationships

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - RelPatientProgram.schema_name() -> str
        - RelPatientProgram.schema_metadata() -> dict
        - RelPatientProgram.parser_config() -> dict
        - RelPatientProgram.transform_config() -> dict
        - RelPatientProgram.lineage_config() -> dict
    """

    patient_id: str = Field(description="Patient identifier")
    program_id: str = Field(description="Program identifier")
    program_name: str | None = Field(default=None, description="Program name")
    enrollment_date: date | None = Field(default=None, description="Date of enrollment")
    updated_date: date | None = Field(default=None, description="Last updated date")
    disenrollment_date: date | None = Field(default=None, description="Date of disenrollment")
    status: str | None = Field(default=None, description="Enrollment status")
    created_date: date | None = Field(default=None, description="Record creation date")
    mrn: str | None = Field(default=None, description="Medical Record Number")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "RelPatientProgram":
        """Create instance from dictionary."""
        return cls(**data)
