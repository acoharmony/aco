# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for email_unsubscribes schema.

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
    with_parser,
    with_storage,
)


@register_schema(
    name="email_unsubscribes",
    version="1.0.0",
    tier="bronze",
    description="Email unsubscribe and complaint events tracking",
    file_patterns={"main": "aco_compliance___email_unsubscribes*.json"},
)
@with_parser(type="json", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"main": "aco_compliance___email_unsubscribes*.json"},
    medallion_layer="bronze",
    silver={
        "output_name": "email_unsubscribes.parquet",
        "refresh_frequency": "monthly",
        "last_updated_by": "aco transform email_unsubscribes",
    },
)
@dataclass
class EmailUnsubscribes:
    """
    Email unsubscribe and complaint events tracking

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - EmailUnsubscribes.schema_name() -> str
        - EmailUnsubscribes.schema_metadata() -> dict
        - EmailUnsubscribes.parser_config() -> dict
        - EmailUnsubscribes.transform_config() -> dict
        - EmailUnsubscribes.lineage_config() -> dict
    """

    campaign_name: str = Field(
        description='Campaign name (e.g., "2023 Q3 ACO Voluntary Alignment Campaign (EN)")'
    )
    email: str = Field(description="Patient email address")
    email_id: str = Field(description="Unique email identifier (UUID)")
    event_name: str = Field(description="Type of event (unsubscribed, complained)")
    network_id: str = Field(description="Network/ACO name")
    patient_id: str = Field(description="Patient identifier (UUID)")
    patient_name: str = Field(description="Patient full name")
    practice_id: str = Field(description="Medical practice identifier")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "EmailUnsubscribes":
        """Create instance from dictionary."""
        return cls(**data)
