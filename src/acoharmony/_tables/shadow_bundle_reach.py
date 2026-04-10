# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for shadow_bundle_reach schema.

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
    name="shadow_bundle_reach",
    version=2,
    tier="bronze",
    description="Shadow Bundle Reports for ACO Reach",
    file_patterns={
        "reach": ["D????.PY????.??.SBM*.D??????.T*.*", "D????.PY????.Q?.SBQR.D??????.T*.xlsx"]
    },
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["D????.PY????.??.SBM*.D??????.T*.*"]},
    silver={"output_name": "shadow_bundle_reach.parquet", "refresh_frequency": "monthly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=243,
    file_pattern="D????.PY????.??.SBM*.D??????.T*.*",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class ShadowBundleReach:
    """
    Shadow Bundle Reports for ACO Reach

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - ShadowBundleReach.schema_name() -> str
        - ShadowBundleReach.schema_metadata() -> dict
        - ShadowBundleReach.parser_config() -> dict
        - ShadowBundleReach.transform_config() -> dict
        - ShadowBundleReach.lineage_config() -> dict
    """

    pass  # No columns defined

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ShadowBundleReach":
        """Create instance from dictionary."""
        return cls(**data)
