# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for emails schema.

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
    mbi_validator,
)


@register_schema(
    name="emails",
    version="1.0.0",
    tier="bronze",
    description="ACO email campaign tracking and engagement metrics",
    file_patterns={"main": "all_sent_emails*.json"},
)
@with_parser(type="json", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"main": "all_sent_emails*.json"},
    medallion_layer="bronze",
    silver={
        "output_name": "emails.parquet",
        "refresh_frequency": "monthly",
        "last_updated_by": "aco transform emails",
    },
)
@dataclass
class Emails:
    """
    ACO email campaign tracking and engagement metrics

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Emails.schema_name() -> str
        - Emails.schema_metadata() -> dict
        - Emails.parser_config() -> dict
        - Emails.transform_config() -> dict
        - Emails.lineage_config() -> dict
    """

    aco_id: str = Field(
        description="ACO identifier",
        json_schema_extra={"source_name": "ACO ID"},
    )
    campaign: str = Field(
        description='Email campaign name (e.g., "CAHPS Reminder")',
        json_schema_extra={"source_name": "Campaign"},
    )
    email_id: str = Field(
        description="Unique email identifier (UUID)",
        json_schema_extra={"source_name": "Email ID"},
    )
    has_been_clicked: str = Field(
        description="Whether email links were clicked",
        json_schema_extra={"source_name": "Has Been Clicked"},
    )
    has_been_opened: str = Field(
        description="Whether email was opened",
        json_schema_extra={"source_name": "Has Been Opened"},
    )
    mbi: str | None = Field(
        default=None,
        description="Medicare Beneficiary Identifier",
        json_schema_extra={"source_name": "MBI"},
    )
    network_id: str = Field(
        description="Network identifier (UUID)",
        json_schema_extra={"source_name": "Network ID"},
    )
    network_name: str = Field(
        description="Network/ACO name",
        json_schema_extra={"source_name": "Network Name"},
    )
    patient_id: str = Field(
        description="Patient identifier (UUID)",
        json_schema_extra={"source_name": "Patient ID"},
    )
    external_patient_id: str | None = Field(
        default=None,
        description="External patient identifier (source system)",
        json_schema_extra={"source_name": "External Patient ID"},
    )
    patient_name: str = Field(
        description="Patient full name",
        json_schema_extra={"source_name": "Patient Name"},
    )
    practice: str = Field(
        description="Medical practice name",
        json_schema_extra={"source_name": "Practice"},
    )
    send_datetime: date | None = Field(
        description='Email send timestamp (formatted as "Month DD, YYYY, H:MM AM/PM")',
        json_schema_extra={
            "source_name": "Send Datetime",
            "date_format": ["%B %d, %Y, %I:%M %p", "%Y-%m-%d"],
        },
    )
    status: str = Field(
        description="Delivery status (Delivered, Opened, Failed, etc.)",
        json_schema_extra={"source_name": "Status"},
    )
    send_date: date = Field(description="Parsed send date")
    send_timestamp: datetime = Field(description="Parsed send timestamp")
    opened_flag: bool = Field(description="Whether email was opened")
    clicked_flag: bool = Field(description="Whether email links were clicked")

    # Field Validators (from centralized _validators module)
    _validate_mbi = mbi_validator("mbi")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Emails":
        """Create instance from dictionary."""
        return cls(**data)
