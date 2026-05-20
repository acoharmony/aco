# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for mailed schema.

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
    name="mailed",
    version="1.0.0",
    tier="bronze",
    description="ACO voluntary alignment campaign mailed letters tracking",
    file_patterns={"main": "aco_compliance___all_sent_letters*.json"},
)
@with_parser(type="json", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"main": "aco_compliance___all_sent_letters*.json"},
    medallion_layer="bronze",
    silver={
        "output_name": "mailed.parquet",
        "refresh_frequency": "monthly",
        "last_updated_by": "aco transform mailed",
    },
)
@dataclass
class Mailed:
    """
    ACO voluntary alignment campaign mailed letters tracking

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Mailed.schema_name() -> str
        - Mailed.schema_metadata() -> dict
        - Mailed.parser_config() -> dict
        - Mailed.transform_config() -> dict
        - Mailed.lineage_config() -> dict
    """

    aco_id: str = Field(
        description="ACO identifier",
        json_schema_extra={"source_name": "ACO ID"},
    )
    campaign_name: str = Field(
        description='Campaign name and language (e.g., "2024 Q2 ACO Voluntary Alignment Campaign (EN)")',
        json_schema_extra={"source_name": "Campaign Name"},
    )
    letter_id: str = Field(
        description="Unique letter identifier (UUID)",
        json_schema_extra={"source_name": "Letter ID"},
    )
    mbi: str = Field(
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
    practice_name: str = Field(
        description="Medical practice name",
        json_schema_extra={"source_name": "Practice Name"},
    )
    send_datetime: date | None = Field(
        description='Letter send timestamp (formatted as "Month DD, YYYY, H:MM AM/PM")',
        json_schema_extra={
            "source_name": "Send Datetime",
            "date_format": ["%B %d, %Y, %I:%M %p", "%Y-%m-%d"],
        },
    )
    send_date: date = Field(description="Parsed send date")
    send_timestamp: datetime = Field(description="Parsed send timestamp")
    status: str = Field(
        description="Delivery status (Delivered, In Transit, etc.)",
        json_schema_extra={"source_name": "Status"},
    )

    # Field Validators (from centralized _validators module)
    _validate_mbi = mbi_validator("mbi")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Mailed":
        """Create instance from dictionary."""
        return cls(**data)
