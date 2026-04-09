# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for beneficiary_data_sharing_exclusion_file schema.

Generated from: _schemas/beneficiary_data_sharing_exclusion_file.yml

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
    with_four_icli,
    with_parser,
    with_storage,
    with_transform,
)


@register_schema(
    name="beneficiary_data_sharing_exclusion_file", version=2, tier="bronze", description="""\2"""
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="bronze",
    silver={
        "output_name": "beneficiary_data_sharing_exclusion_file.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_four_icli(
    category="Reports",
    file_type_code=114,
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class BeneficiaryDataSharingExclusionFile:
    """
    Beneficiary Data Sharing Exclusion File

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - BeneficiaryDataSharingExclusionFile.schema_name() -> str
        - BeneficiaryDataSharingExclusionFile.schema_metadata() -> dict
        - BeneficiaryDataSharingExclusionFile.parser_config() -> dict
        - BeneficiaryDataSharingExclusionFile.transform_config() -> dict
        - BeneficiaryDataSharingExclusionFile.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "BeneficiaryDataSharingExclusionFile":
        """Create instance from dictionary."""
        return cls(**data)
