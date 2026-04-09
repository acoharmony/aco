# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for consolidated_alignment schema.

Generated from: _schemas/consolidated_alignment.yml

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

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
    with_transform,
    with_xref,
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


@register_schema(name="consolidated_alignment", version=3, tier="silver", description="""\2""")
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="gold",
    medallion_layer="silver",
    gold={
        "output_name": "consolidated_alignment.parquet",
        "refresh_frequency": "continuous",
        "last_updated_by": "aco pipeline aco_alignment",
    },
    tracking={"enable_state": True, "track_processed_files": True},
)
@with_xref(
    table="beneficiary_xref",
    join_key="bene_mbi",
    xref_key="prvs_num",
    current_column="crnt_num",
    output_column="current_bene_mbi_id",
    description="Ensure current MBI is applied",
)
@dataclass
class ConsolidatedAlignment:
    """
    Consolidated beneficiary alignment across MSSP and REACH programs with
    idempotent temporal processing and comprehensive data lineage tracking.

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - ConsolidatedAlignment.schema_name() -> str
        - ConsolidatedAlignment.schema_metadata() -> dict
        - ConsolidatedAlignment.parser_config() -> dict
        - ConsolidatedAlignment.transform_config() -> dict
        - ConsolidatedAlignment.lineage_config() -> dict
    """

    bene_mbi: str = MBI(
        description="Medicare Beneficiary Identifier (primary key)",
    )
    bene_first_name: str | None = Field(default=None, description="Beneficiary first name")
    bene_last_name: str | None = Field(default=None, description="Beneficiary last name")
    bene_middle_initial: str | None = Field(default=None, description="Beneficiary middle initial")
    bene_state: str | None = Field(default=None, description="Beneficiary state code (FIPS)")
    bene_zip_5: str | None = ZIP5(default=None, description="Beneficiary 5-digit ZIP code")
    bene_county: str | None = Field(default=None, description="Beneficiary county code (FIPS)")
    office_location: str | None = Field(
        default=None, description="Office location/market assigned based on beneficiary ZIP code"
    )
    bene_death_date: date | None = Field(
        default=None, description="Beneficiary date of death (from BAR, ALR, or CCLF8)"
    )
    enrollment_blocks: str = Field(
        description="List of monthly enrollment blocks covered by this record"
    )
    block_year: str | None = Field(default=None, description="Year of the enrollment block")
    block_month: str | None = Field(
        default=None, description="Month of the enrollment block (1-12)"
    )
    block_year_month: str | None = Field(
        default=None, description="Year-month identifier for the block (YYYY-MM)"
    )
    block_start: date = Field(description="First day of the enrollment block")
    block_end: date = Field(
        description="Last day of the enrollment block (may be truncated by death)"
    )
    enrollment_start: date | None = Field(
        default=None, description="Start of continuous enrollment period"
    )
    enrollment_end: date | None = Field(
        default=None, description="End of continuous enrollment period"
    )
    current_program: str = Field(description="Current or most recent program (MSSP or REACH)")
    current_source_type: str | None = Field(
        default=None, description="Type of current alignment source (BAR/ALR/etc)"
    )
    file_temporal: str | None = Field(
        default=None, description="Structured temporal metadata from source file"
    )
    source_file_type: str | None = Field(
        default=None, description="File type classification (reconciliation/current)"
    )
    source_period: str | None = Field(
        default=None, description="Temporal period from file (annual/Q1-Q4/M01-M12)"
    )
    blocks_per_file: str | None = Field(
        default=None, description="Number of monthly blocks provided by source file type"
    )
    current_alignment_source: str | None = Field(
        default=None, description="Filename of current alignment source"
    )
    is_currently_aligned: bool = Field(
        description="Whether beneficiary is currently aligned (enrollment_end is null)"
    )
    reach_attribution_type: str | None = Field(
        default=None, description="REACH attribution type (Claims-based, Voluntary, etc.)"
    )
    reach_tin: str | None = TIN(default=None, description="REACH Tax Identification Number")
    reach_npi: str | None = NPI(default=None, description="REACH National Provider Identifier")
    reach_provider_name: str | None = NPI(default=None, description="REACH provider/practice name")
    mssp_tin: str | None = TIN(default=None, description="MSSP Tax Identification Number")
    mssp_npi: str | None = NPI(default=None, description="MSSP National Provider Identifier")
    mssp_provider_name: str | None = NPI(default=None, description="MSSP provider/practice name")
    has_voluntary_alignment: bool | None = Field(
        default=None, description="Whether beneficiary has any voluntary alignment"
    )
    has_valid_voluntary_alignment: bool | None = Field(
        default=None, description="Whether beneficiary has valid voluntary alignment"
    )
    voluntary_alignment_date: date | None = Field(
        default=None, description="Most recent voluntary alignment date"
    )
    voluntary_alignment_type: str | None = Field(
        default=None, description="Type of voluntary alignment (PALMR, PBVAR, SVA)"
    )
    voluntary_provider_npi: str | None = NPI(
        default=None, description="NPI from voluntary alignment"
    )
    voluntary_provider_tin: str | None = NPI(
        default=None, description="TIN from voluntary alignment"
    )
    voluntary_provider_name: str | None = NPI(
        default=None, description="Provider name from voluntary alignment"
    )
    first_valid_signature_date: date | None = Field(
        default=None, description="First valid SVA signature date"
    )
    last_valid_signature_date: date | None = Field(
        default=None, description="Most recent valid SVA signature date"
    )
    last_signature_expiry_date: date | None = Field(
        default=None, description="Expiry date of last signature (Jan 1 of signature year + 3)"
    )
    signature_expiry_date: date | None = Field(
        default=None, description="Calculated signature expiry (Jan 1 of year X+3)"
    )
    signature_validity: str | None = Field(
        default=None, description="Structured signature validity information per block"
    )
    signature_valid_for_block: bool | None = Field(
        default=None, description="Whether signature is valid for this specific enrollment block"
    )
    first_sva_submission_date: date | None = Field(
        default=None, description="First SVA submission date"
    )
    last_sva_submission_date: date | None = Field(
        default=None, description="Most recent SVA submission date"
    )
    last_sva_provider_name: str | None = NPI(
        default=None, description="Provider name from last SVA"
    )
    last_sva_provider_npi: str | None = NPI(default=None, description="Provider NPI from last SVA")
    last_sva_provider_tin: str | None = NPI(default=None, description="Provider TIN from last SVA")
    aligned_practitioner_name: str | None = Field(
        default=None, description="Name of aligned practitioner"
    )
    aligned_provider_tin: str | None = NPI(default=None, description="TIN of aligned provider")
    aligned_provider_npi: str | None = NPI(default=None, description="NPI of aligned provider")
    aligned_provider_org: str | None = NPI(
        default=None, description="Organization name of aligned provider"
    )
    response_code_list: str | None = Field(
        default=None,
        description="All unique SVA response codes received (A0/A1/A2 accepted, V0-V2 validation, P0-P2 precedence, E0-E5 eligibility)",
    )
    latest_response_codes: str | None = Field(
        default=None, description="Most recent SVA response code for tracking current status"
    )
    latest_response_detail: str | None = Field(
        default=None, description="Human-readable interpretation of latest response code"
    )
    error_category: str | None = Field(
        default=None,
        description="Primary issue category (eligibility_issues/precedence_issues/validation_errors)",
    )
    eligibility_issues: str | None = Field(
        default=None,
        description="Comma-separated list of eligibility problems (E-codes: deceased/not_enrolled/medicare_advantage/outside_area)",
    )
    precedence_issues: str | None = Field(
        default=None,
        description="Comma-separated list of precedence problems (P-codes: duplicate/superseded/in_another_model)",
    )
    previous_invalids: str | None = Field(
        default=None, description="Historical invalid response codes for audit trail"
    )
    previous_program: str | None = Field(
        default=None, description="Previous ACO program if transitioned"
    )
    enrollment_transition_date: date | None = Field(
        default=None, description="Date of transition between programs"
    )
    previous_source_type: str | None = Field(
        default=None, description="Type of previous alignment source"
    )
    previous_alignment_source: str | None = Field(
        default=None, description="Filename of previous alignment source"
    )
    program_transitions: int | None = Field(
        default=None, description="Number of program transitions"
    )
    transition_history: str | None = Field(
        default=None, description="JSON string of program transition history"
    )
    days_in_current_program: int | None = Field(
        default=None, description="Days in current/most recent program"
    )
    total_days_aligned: int | None = Field(
        default=None, description="Total days aligned across all programs"
    )
    alignment_days: str | None = Field(default=None, description="Total days of alignment")
    current_program_days: str | None = Field(default=None, description="Days in current program")
    previous_program_days: str | None = Field(default=None, description="Days in previous program")
    reach_months: str | None = Field(default=None, description="Number of months aligned to REACH")
    mssp_months: str | None = Field(default=None, description="Number of months aligned to MSSP")
    first_reach_date: date | None = Field(
        default=None, description="First month beneficiary was aligned to REACH"
    )
    last_reach_date: date | None = Field(
        default=None, description="Last month beneficiary was aligned to REACH"
    )
    first_mssp_date: date | None = Field(
        default=None, description="First month beneficiary was aligned to MSSP"
    )
    last_mssp_date: date | None = Field(
        default=None, description="Last month beneficiary was aligned to MSSP"
    )
    total_aligned_months: str | None = Field(
        default=None, description="Total months aligned across all programs"
    )
    latest_aco_id: str | None = Field(default=None, description="Latest ACO identifier")
    source_files: str | None = Field(
        default=None, description="List of source files contributing to this record"
    )
    num_source_files: str | None = Field(
        default=None, description="Number of source files contributing to record"
    )
    most_recent_file: str | None = Field(default=None, description="Most recent source file")
    data_as_of_date: date | None = Field(
        default=None, description="Latest data date from source files (not processing date)"
    )
    last_updated: str | None = Field(default=None, description="Timestamp of last update")
    lineage_source: str | None = Field(
        default=None, description="Complete source file identification with temporal context"
    )
    lineage_processed_at: str | None = Field(
        default=None, description="ISO timestamp when this record was processed"
    )
    lineage_transform: str | None = Field(
        default=None, description="Transform version that created this record"
    )
    temporal_context: str | None = Field(
        default=None, description="Structured temporal metadata for the record"
    )
    transition_info: str | None = Field(
        default=None, description="Structured info about program transitions and gaps"
    )
    gap_months: str | None = Field(
        default=None, description="Number of months between enrollment blocks"
    )
    is_transition: bool | None = Field(
        default=None, description="Whether this block represents a program transition"
    )
    continuous_enrollment: str | None = TIN(
        default=None, description="Continuous enrollment span information"
    )
    enrollment_span_id: str | None = Field(
        default=None, description="ID for continuous enrollment span"
    )
    reconciliation_info: str | None = Field(
        default=None, description="Information for reconciling overlapping enrollments"
    )
    precedence_score: str | None = Field(
        default=None, description="Precedence for overlapping enrollments (lower=higher priority)"
    )
    max_observable_date: date | None = Field(
        default=None, description="Maximum date we can observe from available data (idempotent)"
    )
    max_recon_date: date | None = Field(
        default=None, description="Maximum date from reconciliation files"
    )
    max_current_date: date | None = Field(
        default=None, description="Maximum date from current files"
    )
    sva_tin_match: bool | None = Field(default=None, description="Whether SVA TIN matches alignment")
    sva_npi_match: bool | None = Field(default=None, description="Whether SVA NPI matches alignment")
    provider_on_current_list: bool | None = Field(
        default=None,
        description="Whether SVA TIN/NPI combination exists in current provider list (required for valid attribution)",
    )
    needs_provider_refresh: bool | None = Field(
        default=None,
        description="Flag indicating SVA has valid response code but provider is no longer on list, requiring re-submission",
    )
    invalid_provider_count: int | None = Field(
        default=None,
        description="Count of SVA submissions with providers not on current list",
        ge=0,
    )
    prvs_num: str | None = Field(default=None, description="Previous Beneficiary MBI")
    mapping_type: str | None = Field(
        default=None,
        description="Type of MBI mapping used; xref means a mapping, direct means no crosswalk needed from new to old",
    )
    hcmpi: str | None = Field(default=None, description="Healthcare member patient identifier")
    has_multiple_prvs_mbi: bool | None = MBI(
        default=None,
        description="Whether beneficiary has multiple provider MBI mappings",
    )
    sva_submitted_after_pbvar: bool | None = Field(
        default=None,
        description="Whether our SVA submission is more recent than PBVAR report (true means PBVAR response may be stale)",
    )
    needs_sva_refresh_from_pbvar: bool | None = Field(
        default=None,
        description="PBVAR shows response but no SVA submitted after that report, indicating need for refresh submission",
    )
    pbvar_report_date: date | None = Field(
        default=None,
        description="Date of PBVAR report for temporal comparison with SVA submissions",
    )
    pbvar_response_codes: str | None = Field(
        default=None,
        description="Response codes from PBVAR (may differ from our submitted SVA if timing misaligned)",
    )
    signature_valid_for_current_py: bool | None = Field(
        default=None,
        description="Whether signature is valid for current Performance Year (within 2-year lookback)",
    )
    days_until_signature_expiry: int | None = Field(
        default=None,
        description="Days remaining until signature expires (Jan 1 of year X+3 per Section 5.02)",
    )
    sva_outreach_priority: str | None = Field(
        default=None,
        description="Outreach prioritization based on signature expiry (expired/urgent/upcoming/active/no_signature)",
    )
    signature_valid_for_pys: str | None = Field(
        default=None,
        description='Performance Years covered by current signature (e.g., "2024-2025")',
    )
    mssp_sva_recruitment_target: bool | None = Field(
        default=None,
        description="MSSP beneficiary without valid SVA, eligible for REACH recruitment",
    )
    mssp_to_reach_status: str | None = Field(
        default=None,
        description="MSSP beneficiary's readiness for REACH transition (ready_for_reach/needs_renewal/needs_initial_sva)",
    )
    sva_action_needed: str | None = Field(
        default=None,
        description="Primary action needed for SVA (provider_not_on_list/pbvar_indicates_refresh/has_a2_code/expiring_soon/mssp_recruitment)",
    )
    has_ineligible_alignment: bool | None = Field(
        default=None,
        description="Has A2 response code (accepted but remains ineligible for performance year)",
    )
    sva_used_crosswalk: bool | None = Field(
        default=None,
        description="Whether SVA MBI was resolved through crosswalk (important for validation accuracy)",
    )
    crosswalked_sva_count: int | None = Field(
        default=None,
        description="Number of SVA records that used MBI crosswalk for resolution",
        ge=0,
    )

    # Field Validators (from centralized _validators module)
    _validate_bene_mbi = mbi_validator("bene_mbi")
    _validate_bene_zip_5 = zip5_validator("bene_zip_5")
    _validate_reach_tin = tin_validator("reach_tin")
    _validate_reach_npi = npi_validator("reach_npi")
    _validate_reach_provider_name = npi_validator("reach_provider_name")
    _validate_mssp_tin = tin_validator("mssp_tin")
    _validate_mssp_npi = npi_validator("mssp_npi")
    _validate_mssp_provider_name = npi_validator("mssp_provider_name")
    _validate_voluntary_provider_npi = npi_validator("voluntary_provider_npi")
    _validate_voluntary_provider_tin = npi_validator("voluntary_provider_tin")
    _validate_voluntary_provider_name = npi_validator("voluntary_provider_name")
    _validate_last_sva_provider_name = npi_validator("last_sva_provider_name")
    _validate_last_sva_provider_npi = npi_validator("last_sva_provider_npi")
    _validate_last_sva_provider_tin = npi_validator("last_sva_provider_tin")
    _validate_aligned_provider_tin = npi_validator("aligned_provider_tin")
    _validate_aligned_provider_npi = npi_validator("aligned_provider_npi")
    _validate_aligned_provider_org = npi_validator("aligned_provider_org")
    _validate_continuous_enrollment = tin_validator("continuous_enrollment")
    _validate_sva_tin_match = tin_validator("sva_tin_match")
    _validate_sva_npi_match = npi_validator("sva_npi_match")
    _validate_provider_on_current_list = npi_validator("provider_on_current_list")
    _validate_needs_provider_refresh = npi_validator("needs_provider_refresh")
    _validate_invalid_provider_count = npi_validator("invalid_provider_count")
    _validate_has_multiple_prvs_mbi = mbi_validator("has_multiple_prvs_mbi")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ConsolidatedAlignment":
        """Create instance from dictionary."""
        return cls(**data)
