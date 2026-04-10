# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for beneficiary_hedr_transparency_files schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_storage,
)


@register_schema(
    name="beneficiary_hedr_transparency_files",
    version=2,
    tier="bronze",
    description="Beneficiary HEDR-Transparency Files",
    file_patterns={"reach": ["*BDTF*"]},
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*BDTF*"]},
    silver={
        "output_name": "beneficiary_hedr_transparency_files.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_four_icli(
    category="Reports",
    file_type_code=272,
    file_pattern="P.D????.BDTF.A?.D??????.T*.zip",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class BeneficiaryHedrTransparencyFiles:
    """
    Beneficiary HEDR-Transparency Files

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - BeneficiaryHedrTransparencyFiles.schema_name() -> str
        - BeneficiaryHedrTransparencyFiles.schema_metadata() -> dict
        - BeneficiaryHedrTransparencyFiles.parser_config() -> dict
        - BeneficiaryHedrTransparencyFiles.transform_config() -> dict
        - BeneficiaryHedrTransparencyFiles.lineage_config() -> dict
    """

    aco_id: str | None = Field(default=None, description="ACO identifier")
    model_id: str | None = Field(default=None, description="Model identifier (e.g., ACOREACH)")
    mbi: str | None = Field(default=None, description="Medicare Beneficiary Identifier")
    included_initial_numerator: int | None = Field(
        default=None, description="Initial numerator inclusion flag"
    )
    included_initial_denominator: int | None = Field(
        default=None, description="Initial denominator inclusion flag"
    )
    included_final_numerator: int | None = Field(
        default=None, description="Final numerator inclusion flag"
    )
    included_final_denominator: int | None = Field(
        default=None, description="Final denominator inclusion flag"
    )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "BeneficiaryHedrTransparencyFiles":
        """Create instance from dictionary."""
        return cls(**data)
