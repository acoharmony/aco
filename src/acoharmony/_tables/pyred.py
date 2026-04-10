# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for pyred schema.

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
    with_sheets,
    with_storage,
)


@register_schema(
    name="pyred",
    version=2,
    tier="bronze",
    description="Monthly Provider Specific Payment Reduction Report",
    file_patterns={"reach": ["P.D*.PYRED*.RP.D*.T*.xlsx"]},
)
@with_parser(
    type="excel_multi_sheet", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["P.D*.PYRED*.RP.D*.T*.xlsx"]},
    silver={"output_name": "pyred.parquet", "refresh_frequency": "monthly"},
)
@with_sheets(
    sheets=[
        {
            "sheet_index": 0,
            "sheet_type": "inpatient",
            "description": "Inpatient Expenditure from Participating Facilities",
            "columns": [
                {
                    "name": "provider_type",
                    "position": 0,
                    "data_type": "string",
                    "description": "Provider type classification",
                },
                {
                    "name": "organization_npi",
                    "position": 2,
                    "data_type": "string",
                    "description": "Organization NPI",
                },
                {
                    "name": "ccn",
                    "position": 3,
                    "data_type": "string",
                    "description": "CMS Certification Number",
                },
                {
                    "name": "provider_name",
                    "position": 4,
                    "data_type": "string",
                    "description": "Provider name",
                },
                {
                    "name": "beneficiaries_with_claims",
                    "position": 7,
                    "data_type": "integer",
                    "description": "Number of beneficiaries with claims",
                },
                {
                    "name": "total_incurred_claims",
                    "position": 9,
                    "data_type": "decimal",
                    "description": "Total incurred claims",
                },
                {
                    "name": "claims_incurred_by_optouts",
                    "position": 11,
                    "data_type": "decimal",
                    "description": "Claims incurred by opt-outs",
                },
                {
                    "name": "full_ffs_payment_amount",
                    "position": 12,
                    "data_type": "decimal",
                    "description": "Full FFS payment amount",
                },
                {
                    "name": "ffs_fee_reduction_amount",
                    "position": 13,
                    "data_type": "decimal",
                    "description": "FFS fee reduction amount",
                },
            ],
        },
        {
            "sheet_index": 1,
            "sheet_type": "snf",
            "description": "SNF Expenditure from Participating Facilities",
            "columns": [
                {
                    "name": "provider_type",
                    "position": 0,
                    "data_type": "string",
                    "description": "Provider type classification",
                },
                {
                    "name": "organization_npi",
                    "position": 2,
                    "data_type": "string",
                    "description": "Organization NPI",
                },
                {
                    "name": "ccn",
                    "position": 3,
                    "data_type": "string",
                    "description": "CMS Certification Number",
                },
                {
                    "name": "provider_name",
                    "position": 4,
                    "data_type": "string",
                    "description": "Provider name",
                },
                {
                    "name": "beneficiaries_with_claims",
                    "position": 7,
                    "data_type": "integer",
                    "description": "Number of beneficiaries with claims",
                },
                {
                    "name": "total_incurred_claims",
                    "position": 9,
                    "data_type": "decimal",
                    "description": "Total incurred claims",
                },
                {
                    "name": "claims_incurred_by_optouts",
                    "position": 11,
                    "data_type": "decimal",
                    "description": "Claims incurred by opt-outs",
                },
                {
                    "name": "full_ffs_payment_amount",
                    "position": 12,
                    "data_type": "decimal",
                    "description": "Full FFS payment amount",
                },
                {
                    "name": "ffs_fee_reduction_amount",
                    "position": 13,
                    "data_type": "decimal",
                    "description": "FFS fee reduction amount",
                },
            ],
        },
        {
            "sheet_index": 2,
            "sheet_type": "hh",
            "description": "Home Health Expenditure from Participating Facilities",
            "columns": [
                {
                    "name": "provider_type",
                    "position": 0,
                    "data_type": "string",
                    "description": "Provider type classification",
                },
                {
                    "name": "organization_npi",
                    "position": 2,
                    "data_type": "string",
                    "description": "Organization NPI",
                },
                {
                    "name": "ccn",
                    "position": 3,
                    "data_type": "string",
                    "description": "CMS Certification Number",
                },
                {
                    "name": "provider_name",
                    "position": 4,
                    "data_type": "string",
                    "description": "Provider name",
                },
                {
                    "name": "beneficiaries_with_claims",
                    "position": 7,
                    "data_type": "integer",
                    "description": "Number of beneficiaries with claims",
                },
                {
                    "name": "total_incurred_claims",
                    "position": 9,
                    "data_type": "decimal",
                    "description": "Total incurred claims",
                },
                {
                    "name": "claims_incurred_by_optouts",
                    "position": 11,
                    "data_type": "decimal",
                    "description": "Claims incurred by opt-outs",
                },
                {
                    "name": "full_ffs_payment_amount",
                    "position": 12,
                    "data_type": "decimal",
                    "description": "Full FFS payment amount",
                },
                {
                    "name": "ffs_fee_reduction_amount",
                    "position": 13,
                    "data_type": "decimal",
                    "description": "FFS fee reduction amount",
                },
            ],
        },
        {
            "sheet_index": 3,
            "sheet_type": "hospice",
            "description": "Hospice Expenditure from Participating Facilities",
            "columns": [
                {
                    "name": "provider_type",
                    "position": 0,
                    "data_type": "string",
                    "description": "Provider type classification",
                },
                {
                    "name": "organization_npi",
                    "position": 2,
                    "data_type": "string",
                    "description": "Organization NPI",
                },
                {
                    "name": "ccn",
                    "position": 3,
                    "data_type": "string",
                    "description": "CMS Certification Number",
                },
                {
                    "name": "provider_name",
                    "position": 4,
                    "data_type": "string",
                    "description": "Provider name",
                },
                {
                    "name": "beneficiaries_with_claims",
                    "position": 7,
                    "data_type": "integer",
                    "description": "Number of beneficiaries with claims",
                },
                {
                    "name": "total_incurred_claims",
                    "position": 9,
                    "data_type": "decimal",
                    "description": "Total incurred claims",
                },
                {
                    "name": "claims_incurred_by_optouts",
                    "position": 11,
                    "data_type": "decimal",
                    "description": "Claims incurred by opt-outs",
                },
                {
                    "name": "full_ffs_payment_amount",
                    "position": 12,
                    "data_type": "decimal",
                    "description": "Full FFS payment amount",
                },
                {
                    "name": "ffs_fee_reduction_amount",
                    "position": 13,
                    "data_type": "decimal",
                    "description": "FFS fee reduction amount",
                },
            ],
        },
        {
            "sheet_index": 4,
            "sheet_type": "outpatient",
            "description": "Outpatient Expenditure from Participating Facilities",
            "columns": [
                {
                    "name": "provider_type",
                    "position": 0,
                    "data_type": "string",
                    "description": "Provider type classification",
                },
                {
                    "name": "organization_npi",
                    "position": 2,
                    "data_type": "string",
                    "description": "Organization NPI",
                },
                {
                    "name": "ccn",
                    "position": 3,
                    "data_type": "string",
                    "description": "CMS Certification Number",
                },
                {
                    "name": "provider_name",
                    "position": 4,
                    "data_type": "string",
                    "description": "Provider name",
                },
                {
                    "name": "beneficiaries_with_claims",
                    "position": 7,
                    "data_type": "integer",
                    "description": "Number of beneficiaries with claims",
                },
                {
                    "name": "total_incurred_claims",
                    "position": 9,
                    "data_type": "decimal",
                    "description": "Total incurred claims",
                },
                {
                    "name": "claims_incurred_by_optouts",
                    "position": 11,
                    "data_type": "decimal",
                    "description": "Claims incurred by opt-outs",
                },
                {
                    "name": "full_ffs_payment_amount",
                    "position": 12,
                    "data_type": "decimal",
                    "description": "Full FFS payment amount",
                },
                {
                    "name": "ffs_fee_reduction_amount",
                    "position": 13,
                    "data_type": "decimal",
                    "description": "FFS fee reduction amount",
                },
            ],
        },
        {
            "sheet_index": 5,
            "sheet_type": "physician",
            "description": "Physician Expenditure from Participating Facilities",
            "columns": [
                {
                    "name": "provider_type",
                    "position": 0,
                    "data_type": "string",
                    "description": "Provider type classification",
                },
                {
                    "name": "individual_npi",
                    "position": 2,
                    "data_type": "string",
                    "description": "Individual NPI",
                },
                {
                    "name": "first_name",
                    "position": 3,
                    "data_type": "string",
                    "description": "Provider first name",
                },
                {
                    "name": "last_name",
                    "position": 4,
                    "data_type": "string",
                    "description": "Provider last name",
                },
                {
                    "name": "beneficiaries_with_claims",
                    "position": 7,
                    "data_type": "integer",
                    "description": "Number of beneficiaries with claims",
                },
                {
                    "name": "total_incurred_claims",
                    "position": 9,
                    "data_type": "decimal",
                    "description": "Total incurred claims",
                },
                {
                    "name": "claims_incurred_by_optouts",
                    "position": 11,
                    "data_type": "decimal",
                    "description": "Claims incurred by opt-outs",
                },
                {
                    "name": "full_ffs_payment_amount",
                    "position": 12,
                    "data_type": "decimal",
                    "description": "Full FFS payment amount",
                },
                {
                    "name": "ffs_fee_reduction_amount",
                    "position": 13,
                    "data_type": "decimal",
                    "description": "FFS fee reduction amount",
                },
            ],
        },
    ],
    matrix_fields=[
        {
            "matrix": [None, 1, 0],
            "field_name": "performance_year",
            "data_type": "string",
            "extract_pattern": "\\d{4}",
            "default_value": None,
            "description": "Performance year extracted from report header",
        },
        {
            "matrix": [None, 1, 4],
            "field_name": "report_period",
            "data_type": "string",
            "extract_pattern": "([A-Za-z]+\\s+\\d{4})",
            "default_value": None,
            "description": "Report period extracted from column header",
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=None,
    file_pattern="P.D????.PYRED??.RP.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class Pyred:
    """
    Monthly Provider Specific Payment Reduction Report

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Pyred.schema_name() -> str
        - Pyred.schema_metadata() -> dict
        - Pyred.parser_config() -> dict
        - Pyred.transform_config() -> dict
        - Pyred.lineage_config() -> dict
    """

    sheet_type: str | None = Field(default=None, description="Sheet type identifier")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Pyred":
        """Create instance from dictionary."""
        return cls(**data)
