# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for hcmpi_master schema.

Generated from: _schemas/hcmpi_master.yml

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from datetime import date, datetime

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
    with_transform,
)


@register_schema(
    name="hcmpi_master",
    version=2,
    tier="bronze",
    description="HarmonyCares Master Patient Index - Patient identifier crosswalk",
    file_patterns={"main": "hcmpi_master.csv"},
)
@with_parser(
    type="csv", delimiter="|", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"main": "hcmpi_master.csv"},
    medallion_layer="silver",
    silver={
        "output_name": "hcmpi_master.parquet",
        "refresh_frequency": "weekly",
        "last_updated_by": "aco transform hcmpi",
    },
    gold={"output_name": None, "refresh_frequency": None, "last_updated_by": None},
)
@dataclass
class HcmpiMaster:
    """
    HarmonyCares Master Patient Index - Patient identifier crosswalk

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - HcmpiMaster.schema_name() -> str
        - HcmpiMaster.schema_metadata() -> dict
        - HcmpiMaster.parser_config() -> dict
        - HcmpiMaster.transform_config() -> dict
        - HcmpiMaster.lineage_config() -> dict
    """

    hcmpi: str = Field(description="HarmonyCares Master Patient Index")
    identifier_src_field: str | None = Field(
        alias="identifiersrcfield", default=None, description="Source field for identifier"
    )
    identifier_src: str | None = Field(
        alias="identifiersrc", default=None, description="Source system for identifier"
    )
    identifier: str = Field(description="Patient identifier (MRN, MBI, etc.)")
    data_source: str | None = Field(
        alias="datasource", default=None, description="Data source system"
    )
    rcd_active: bool | None = Field(default=None, description="Record active flag")
    eff_start_dt: date | None = Field(default=None, description="Effective start date")
    eff_end_dt: date | None = Field(default=None, description="Effective end date")
    last_touch_dttm: datetime | None = Field(default=None, description="Last known HCMG touchpoint")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "HcmpiMaster":
        """Create instance from dictionary."""
        return cls(**data)
