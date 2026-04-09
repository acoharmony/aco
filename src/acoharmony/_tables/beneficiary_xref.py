# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for beneficiary_xref schema.

Generated from: _schemas/beneficiary_xref.yml

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
    with_deduplication,
    with_staging,
    with_standardization,
    with_storage,
    with_transform,
)


@register_schema(name="beneficiary_xref", version=2, tier="silver", description="""\2""")
@with_transform()
@with_storage(
    tier="silver",
    medallion_layer="silver",
    gold={"output_name": "beneficiary_xref.parquet"},
)
@with_staging(source="cclf9")
@with_deduplication(key=["prvs_num"], sort_by=["file_date", "prvs_id_efctv_dt"], keep="first")
@with_standardization(
    rename_columns={
        "xref_prvs_num": "previous_member_id",
        "xref_crnt_num": "current_member_id",
        "xref_efctv_bgn_dt": "effective_start_date",
        "xref_efctv_end_dt": "effective_end_date",
    },
    add_columns=[
        {"name": "crosswalk_type", "value": "MBI"},
        {"name": "processed_date", "value": "current_timestamp()"},
        {"name": "source_file", "value": "beneficiary_xref"},
    ],
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
