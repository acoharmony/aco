# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for salesforce_account schema.

Generated from: _schemas/salesforce_account.yml

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
    NPI,
    TIN,
    ZIP5,
    npi_validator,
    tin_validator,
    zip5_validator,
)


@register_schema(name="salesforce_account", version=2, tier="bronze", description="""\2""")
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@dataclass
class SalesforceAccount:
    """
    Salesforce account data for providers and organizations

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - SalesforceAccount.schema_name() -> str
        - SalesforceAccount.schema_metadata() -> dict
        - SalesforceAccount.parser_config() -> dict
        - SalesforceAccount.transform_config() -> dict
        - SalesforceAccount.lineage_config() -> dict
    """

    account_id: str = NPI(description="Salesforce Account ID")
    account_name: str | None = TIN(default=None, description="Account name")
    account_type: str | None = Field(default=None, description="Type of account")
    tin: str | None = Field(default=None, description="Tax Identification Number")
    npi: str | None = Field(default=None, description="National Provider Identifier")
    address_line_1: str | None = Field(default=None, description="Street address line 1")
    address_line_2: str | None = Field(default=None, description="Street address line 2")
    city: str | None = Field(default=None, description="City")
    state: str | None = Field(default=None, description="State code")
    zip_code: str | None = ZIP5(default=None, description="ZIP code")
    phone: str | None = Field(default=None, description="Phone number")
    specialty: str | None = Field(default=None, description="Provider specialty")
    active_flag: bool | None = Field(default=None, description="Active account flag")
    created_date: date | None = Field(default=None, description="Account creation date")
    updated_date: date | None = Field(default=None, description="Last updated date")
    parent_account_id: str | None = Field(
        default=None, description="Parent account ID for hierarchies"
    )

    # Field Validators (from centralized _validators module)
    _validate_tin = tin_validator("tin")
    _validate_npi = npi_validator("npi")
    _validate_zip_code = zip5_validator("zip_code")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SalesforceAccount":
        """Create instance from dictionary."""
        return cls(**data)
