# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for prospective_plus_opportunity_report schema.

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
    name="prospective_plus_opportunity_report",
    version=2,
    tier="bronze",
    description="Prospective Plus Opportunity Report",
    file_patterns={"reach": ["*PPOPR*"]},
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*PPOPR*"]},
    silver={
        "output_name": "prospective_plus_opportunity_report.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_four_icli(
    category="Beneficiary List",
    file_type_code=170,
    file_pattern="P.D????.PPOPR.Q?.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class ProspectivePlusOpportunityReport:
    """
    Prospective Plus Opportunity Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - ProspectivePlusOpportunityReport.schema_name() -> str
        - ProspectivePlusOpportunityReport.schema_metadata() -> dict
        - ProspectivePlusOpportunityReport.parser_config() -> dict
        - ProspectivePlusOpportunityReport.transform_config() -> dict
        - ProspectivePlusOpportunityReport.lineage_config() -> dict
    """

    county: str | None = Field(default=None, description="County name")
    state: str | None = Field(default=None, description="State abbreviation (2-letter code)")
    fips: str | None = Field(default=None, description="FIPS county code (5-digit)")
    count_of_beneficiaries: str | None = Field(
        default=None,
        description="Total FFS beneficiaries eligible for Prospective Plus alignment in the county (may be 'SUPPRESSED' if 10 or fewer)",
    )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ProspectivePlusOpportunityReport":
        """Create instance from dictionary."""
        return cls(**data)
