# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for aco_financial_guarantee_amount schema.

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
)


@register_schema(
    name="aco_financial_guarantee_amount",
    version=2,
    tier="bronze",
    file_patterns={"reach": ["*FGL*.pdf", "*FGL*.xlsx"]},
)
@with_parser(type="pdf", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*FGL*.pdf", "*FGL*.xlsx"]},
    silver={
        "output_name": "aco_financial_guarantee_amount.parquet",
        "refresh_frequency": "annual",
    },
)
@with_four_icli(
    category="Reports",
    file_type_code=267,
    file_pattern="D????.FGL.PY????.D??????.T*.pdf",
    extract_zip=False,
    refresh_frequency="annual",
)
@dataclass
class AcoFinancialGuaranteeAmount:
    """
    ACO Financial Guarantee Amount

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - AcoFinancialGuaranteeAmount.schema_name() -> str
        - AcoFinancialGuaranteeAmount.schema_metadata() -> dict
        - AcoFinancialGuaranteeAmount.parser_config() -> dict
        - AcoFinancialGuaranteeAmount.transform_config() -> dict
        - AcoFinancialGuaranteeAmount.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "AcoFinancialGuaranteeAmount":
        """Create instance from dictionary."""
        return cls(**data)
