# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for voluntary_alignment schema.

Generated from: _schemas/voluntary_alignment.yml

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
    with_sources,
    with_storage,
    with_transform,
)
from acoharmony._validators.field_validators import (
    MBI,
    NPI,
    mbi_validator,
    npi_validator,
)


@register_schema(name="voluntary_alignment", version=2, tier="silver", description="""\2""")
@with_transform()
@with_storage(
    tier="silver",
    medallion_layer="silver",
    gold={"output_name": "voluntary_alignment.parquet"},
)
@with_sources("sva", "pbvar", "emails", "mailed", "email_unsubscribes")
@dataclass
class VoluntaryAlignment:
    """
    Comprehensive voluntary alignment tracking consolidating all beneficiary touchpoints (emails, mailings, SVA, PBVAR)

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - VoluntaryAlignment.schema_name() -> str
        - VoluntaryAlignment.schema_metadata() -> dict
        - VoluntaryAlignment.parser_config() -> dict
        - VoluntaryAlignment.transform_config() -> dict
        - VoluntaryAlignment.lineage_config() -> dict
    """

    bene_mbi: str = MBI(
        description="Original Medicare Beneficiary Identifier",
    )
    normalized_mbi: str = MBI(
        description="Crosswalk-normalized MBI for consistent tracking",
    )
    hcmpi: str | None = MBI(
        default=None, description="Master patient identifier from enterprise crosswalk"
    )
    previous_mbi_count: str = NPI(
        description="Number of previous MBIs for this beneficiary",
    )
    email_campaigns_sent: str = Field(
        description="Total number of email campaigns sent to beneficiary"
    )
    emails_opened: str = Field(description="Number of emails opened by beneficiary")
    emails_clicked: str = Field(description="Number of emails with clicked links")
    email_open_rate: str | None = Field(
        default=None, description="Percentage of emails opened (0-100)"
    )
    email_click_rate: str | None = Field(
        default=None, description="Percentage of emails clicked (0-100)"
    )
    email_unsubscribed: bool = Field(description="Whether beneficiary has unsubscribed from emails")
    email_complained: bool = Field(description="Whether beneficiary has complained about emails")
    last_email_date: date | None = Field(default=None, description="Date of most recent email sent")
    mailed_campaigns_sent: str = Field(
        description="Total number of mailed campaigns sent to beneficiary"
    )
    mailed_delivered: str = Field(description="Number of successfully delivered mailings")
    mailed_delivery_rate: str | None = Field(
        default=None, description="Percentage of mailings delivered (0-100)"
    )
    last_mailed_date: date | None = Field(default=None, description="Date of most recent mailing")
    mailing_campaigns: str | None = Field(
        default=None, description="Comma-separated list of mailing campaign names"
    )
    sva_signature_count: str = Field(description="Total number of SVA signatures on record")
    first_sva_date: date | None = Field(default=None, description="Date of first SVA signature")
    most_recent_sva_date: date | None = Field(
        default=None, description="Date of most recent SVA signature"
    )
    sva_provider_npi: str | None = Field(
        default=None, description="NPI of most recent SVA provider"
    )
    sva_provider_tin: str | None = NPI(default=None, description="TIN of most recent SVA provider")
    sva_provider_name: str | None = NPI(
        default=None, description="Name of most recent SVA provider"
    )
    sva_provider_valid: bool = Field(
        description="Whether most recent provider TIN/NPI combo is valid"
    )
    days_since_last_sva: str | None = Field(
        default=None, description="Days since most recent SVA signature"
    )
    sva_pending_cms: bool = Field(
        description="Whether beneficiary has signature pending CMS review"
    )
    has_ffs_service: bool = Field(description="Whether beneficiary has any FFS service record")
    ffs_first_date: date | None = Field(
        default=None, description="First date of FFS service for this beneficiary"
    )
    ffs_claim_count: str | None = Field(
        default=None, description="Total number of FFS claims from valid providers"
    )
    days_since_first_ffs: str | None = Field(
        default=None, description="Days since first FFS service"
    )
    ffs_before_alignment: bool = Field(description="Whether FFS service occurred before alignment")
    pbvar_aligned: bool = Field(description="Whether beneficiary is currently aligned in PBVAR")
    pbvar_aco_id: str | None = Field(default=None, description="ACO ID from PBVAR alignment")
    pbvar_response_codes: str | None = Field(
        default=None, description="SVA response codes from PBVAR"
    )
    pbvar_file_date: date | None = Field(default=None, description="Most recent PBVAR file date")
    first_outreach_date: date | None = Field(
        default=None, description="Earliest date of any outreach attempt (email or mail)"
    )
    last_outreach_date: date | None = Field(
        default=None, description="Most recent date of any outreach attempt"
    )
    days_in_funnel: str | None = Field(
        default=None, description="Total days from first to last outreach"
    )
    total_touchpoints: str = Field(
        description="Total number of all outreach attempts (emails + mailings)"
    )
    alignment_journey_status: str = Field(
        description="Current status in alignment journey (Never Contacted, Contacted No Response, Engaged, Signed, Aligned)"
    )
    signature_status: str = Field(
        description="Signature recency status (Never Signed, Current Year, Recent, Aging, Old, Invalid Provider)"
    )
    outreach_response_status: str = Field(
        description="Response to outreach (No Response, Email Engaged, Unsubscribed, Complained)"
    )
    chase_list_eligible: bool = Field(
        description="Whether beneficiary should be on signature chase list"
    )
    chase_reason: str | None = Field(
        default=None, description="Reason for being on chase list if eligible"
    )
    invalid_email_after_death: bool = Field(description="Whether emails were sent after death date")
    invalid_mail_after_death: bool = Field(
        description="Whether mailings were sent after death date"
    )
    invalid_outreach_after_termination: bool = Field(
        description="Whether outreach occurred after enrollment termination"
    )
    processed_at: datetime = Field(description="Timestamp when this record was processed")

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi = mbi_validator("bene_mbi")
    _validate_normalized_mbi = mbi_validator("normalized_mbi")
    _validate_previous_mbi_count = mbi_validator("previous_mbi_count")
    _validate_sva_provider_npi = npi_validator("sva_provider_npi")
    _validate_sva_provider_tin = npi_validator("sva_provider_tin")
    _validate_sva_provider_name = npi_validator("sva_provider_name")
    _validate_sva_provider_valid = npi_validator("sva_provider_valid")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "VoluntaryAlignment":
        """Create instance from dictionary."""
        return cls(**data)
