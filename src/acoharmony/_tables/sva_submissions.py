# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for sva_submissions schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from datetime import date, datetime

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)
from acoharmony._validators.field_validators import (
    MBI,
    NPI,
    TIN,
    ZIP5,
    mbi_validator,
    npi_validator,
    tin_validator,
    zip5_validator,
)


@register_schema(
    name="sva_submissions",
    version="1.0.0",
    tier="bronze",
    description="Shared Voluntary Alignment (SVA) submissions from various sources (EarthClassMail, Dropbox, Jotform)",
    file_patterns={"main": "all_svas*.json"},
)
@with_parser(type="json", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"main": "all_svas*.json"},
    medallion_layer="bronze",
    silver={
        "output_name": "sva_submissions.parquet",
        "refresh_frequency": "monthly",
        "last_updated_by": "aco transform sva_submissions",
    },
)
@dataclass
class SvaSubmissions:
    """
    Shared Voluntary Alignment (SVA) submissions from various sources (EarthClassMail, Dropbox, Jotform)

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - SvaSubmissions.schema_name() -> str
        - SvaSubmissions.schema_metadata() -> dict
        - SvaSubmissions.parser_config() -> dict
        - SvaSubmissions.transform_config() -> dict
        - SvaSubmissions.lineage_config() -> dict
    """

    sva_id: str = MBI(
        description="Unique SVA submission identifier (UUID)",
        json_schema_extra={"source_name": "SVA ID"},
    )
    submission_id: str = NPI(
        description="Submission identifier from source system",
        json_schema_extra={"source_name": "Submission ID"},
    )
    submission_source: str = TIN(
        description="Source of submission (EarthClassMail, DropboxUpload, Jotform)",
        json_schema_extra={"source_name": "Submission Source"},
    )
    beneficiary_first_name: str = Field(
        description="Beneficiary first name",
        json_schema_extra={"source_name": "Beneficiary First Name"},
    )
    beneficiary_last_name: str = Field(
        description="Beneficiary last name",
        json_schema_extra={"source_name": "Beneficiary Last Name"},
    )
    provider_name_or_med_group: str = Field(
        description="Provider name or medical group",
        json_schema_extra={"source_name": "Provider Name Or Med Group"},
    )
    mbi: str | None = NPI(
        default=None,
        description="Medicare Beneficiary Identifier",
        json_schema_extra={"source_name": "MBI"},
    )
    updated_mbi: str | None = MBI(
        default=None,
        description="Updated MBI if corrected",
        json_schema_extra={"source_name": "Updated MBI"},
    )
    birth_date: date | None = Field(
        default=None,
        description="Beneficiary birth date (raw string)",
        json_schema_extra={"source_name": "Birth Date", "date_format": ["%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]},
    )
    transcriber_notes: str | None = Field(
        default=None,
        description="Notes from transcriber",
        json_schema_extra={"source_name": "Transcriber Notes"},
    )
    signature_date: date | None = Field(
        default=None,
        description="Date of signature on SVA form (raw string)",
        json_schema_extra={"source_name": "Signature Date", "date_format": ["%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"]},
    )
    address_primary_line: str | None = Field(
        default=None,
        description="Primary address line",
        json_schema_extra={"source_name": "Address Primary Line"},
    )
    city: str | None = Field(
        default=None,
        description="City",
        json_schema_extra={"source_name": "City"},
    )
    state: str | None = Field(
        default=None,
        description="State code",
        json_schema_extra={"source_name": "State"},
    )
    zip: str | None = ZIP5(
        default=None,
        description="ZIP code",
        json_schema_extra={"source_name": "Zip"},
    )
    provider_npi: str | None = Field(
        default=None,
        description="Provider National Provider Identifier",
        json_schema_extra={"source_name": "Provider NPI"},
    )
    updated_npi: str | None = NPI(
        default=None,
        description="Updated NPI if corrected",
        json_schema_extra={"source_name": "Updated NPI"},
    )
    provider_name: str | None = NPI(
        default=None,
        description="Individual provider name",
        json_schema_extra={"source_name": "Provider Name"},
    )
    tin: str | None = Field(
        default=None,
        description="Tax Identification Number",
        json_schema_extra={"source_name": "TIN"},
    )
    dc_id: str | None = Field(
        default=None,
        description="DC (Direct Contracting) identifier",
        json_schema_extra={"source_name": "DC ID"},
    )
    network_number: str | None = Field(
        default=None,
        description="Network number",
        json_schema_extra={"source_name": "Network Number"},
    )
    created_at: str = Field(
        description='Timestamp when record was created (formatted as "Month DD, YYYY, H:MM AM/PM")',
        json_schema_extra={"source_name": "Created At"},
    )
    letter_email_id: str | None = Field(
        default=None,
        description="Associated letter or email identifier",
        json_schema_extra={"source_name": "Letter/Email ID"},
    )
    network_id: str = Field(
        description="Network identifier",
        json_schema_extra={"source_name": "Network ID"},
    )
    practice_name: str | None = Field(
        default=None,
        description="Practice name",
        json_schema_extra={"source_name": "Practice Name"},
    )
    status: str = Field(
        description="Submission status",
        json_schema_extra={"source_name": "Status"},
    )
    signature_date_parsed: date | None = Field(default=None, description="Parsed signature date")
    created_date: date = Field(description="Parsed creation date")
    created_timestamp: datetime = Field(description="Parsed creation timestamp")
    birth_date_parsed: date | None = Field(default=None, description="Parsed birth date")

    # Field Validators (from centralized _validators module)
    _validate_provider_name_or_med_group = npi_validator("provider_name_or_med_group")
    _validate_mbi = mbi_validator("mbi")
    _validate_updated_mbi = mbi_validator("updated_mbi")
    _validate_zip = zip5_validator("zip")
    _validate_provider_npi = npi_validator("provider_npi")
    _validate_updated_npi = npi_validator("updated_npi")
    _validate_provider_name = npi_validator("provider_name")
    _validate_tin = tin_validator("tin")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SvaSubmissions":
        """Create instance from dictionary."""
        return cls(**data)
