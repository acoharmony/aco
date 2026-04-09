# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for enterprise_crosswalk schema.

Generated from: _schemas/enterprise_crosswalk.yml

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

from acoharmony._registry import register_schema, with_parser, with_transform
from acoharmony._validators.field_validators import (
    MBI,
    mbi_validator,
)


@register_schema(name="enterprise_crosswalk", version=2, tier="silver", description="""\2""")
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform(name="enterprise_xwalk")
@dataclass
class EnterpriseCrosswalk:
    """
    Enterprise-wide patient identifier crosswalk combining MBI mappings from CCLF9 XREF,
    HCMPI master, and beneficiary demographics. Provides comprehensive MBI-to-MBI mappings,
    HCMPI linkage, and MRN resolution for patient matching across all data sources.

    This crosswalk enables:
    - Resolution of historical MBIs to current MBIs
    - Transitive closure for chain mappings
    - HCMPI and MRN linkage for enterprise patient matching
    - Self-mappings for all discovered MBIs
    - Validation and integrity checking

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - EnterpriseCrosswalk.schema_name() -> str
        - EnterpriseCrosswalk.schema_metadata() -> dict
        - EnterpriseCrosswalk.parser_config() -> dict
        - EnterpriseCrosswalk.transform_config() -> dict
        - EnterpriseCrosswalk.lineage_config() -> dict
    """

    prvs_num: str = MBI(description="Previous Medicare Beneficiary Identifier")
    crnt_num: str = Field(description="Current Medicare Beneficiary Identifier")
    mapping_type: str = Field(
        description="Type of mapping (xref=CCLF9 crosswalk, self=identity mapping, chain=transitive)"
    )
    hcmpi: str | None = Field(
        default=None, description="Healthcare Member Patient Identifier for enterprise linkage"
    )
    mrn: str | None = Field(default=None, description="Medical Record Number from source systems")
    created_at: str = Field(description="Timestamp when this mapping was created")
    created_by: str = Field(description="System or process that created this mapping")
    is_valid_mbi_format: bool | None = Field(
        default=None,
        description="Whether both MBIs match the standard 11-character format",
    )
    has_circular_reference: bool | None = Field(
        default=None, description="Whether this mapping participates in a circular reference chain"
    )
    chain_depth: int | None = Field(
        default=None, description="Number of hops in the mapping chain (0 for direct mappings)"
    )
    source_system: str | None = Field(
        default=None, description="Source system for this mapping (CCLF9, HCMPI, Demographics)"
    )
    source_file: str | None = Field(default=None, description="Source file name for audit trail")
    load_date: date | None = Field(default=None, description="Date when source data was loaded")

    # Field Validators (from centralized _validators module)
    _validate_is_valid_mbi_format = mbi_validator("is_valid_mbi_format")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "EnterpriseCrosswalk":
        """Create instance from dictionary."""
        return cls(**data)
