# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for beneficiary_xref schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_storage,
)


@register_schema(name="beneficiary_xref", version=2, tier="silver", description="""\2""")
@with_storage(
    tier="silver",
    medallion_layer="silver",
    gold={"output_name": "beneficiary_xref.parquet"},
)
@dataclass
class BeneficiaryXref:
    """
    Beneficiary MBI crosswalk from CCLF9

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - BeneficiaryXref.schema_name() -> str
        - BeneficiaryXref.schema_metadata() -> dict
        - BeneficiaryXref.parser_config() -> dict
        - BeneficiaryXref.transform_config() -> dict
        - BeneficiaryXref.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "BeneficiaryXref":
        """Create instance from dictionary."""
        return cls(**data)
