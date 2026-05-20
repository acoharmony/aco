# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic models for CMS Public Use Files (PUF) metadata and validation.

These models ensure data quality and provide type safety for CMS data inventories:
- FileMetadata: Individual file/download metadata
- RuleMetadata: Federal Register rule metadata (proposed, final, correction)
- YearInventory: All rules and files for a calendar year
- DatasetInventory: Complete inventory across all years

Models integrate with:
- _cite module for download/citation tracking
- _schemas for data validation
- StorageBackend for file management
"""

from datetime import date, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, model_validator


class RuleType(StrEnum):
    """Type of CMS rulemaking document."""

    PROPOSED = "Proposed"
    FINAL = "Final"
    CORRECTION = "Correction"
    INTERIM_FINAL = "Interim Final"


class FileCategory(StrEnum):
    """Category of PUF data file."""

    # RVU and Payment Files
    ADDENDA = "addenda"
    PPRVU = "pprvu"  # Physician fee schedule RVUs
    PE_RVU = "pe_rvu"  # Practice expense RVUs
    RVU_QUARTERLY = "rvu_quarterly"  # Quarterly RVU files (A/B/C/D releases)
    CONVERSION_FACTOR = "conversion_factor"

    # Geographic Files
    GPCI = "gpci"  # Geographic practice cost indices
    GAF = "gaf"  # Geographic adjustment factors
    LOCALITY = "locality"

    # Practice Expense Inputs
    DIRECT_PE_INPUTS = "direct_pe_inputs"
    CLINICAL_LABOR = "clinical_labor"
    EQUIPMENT = "equipment"
    SUPPLIES = "supplies"
    PE_WORKSHEET = "pe_worksheet"
    PE_SUMMARY = "pe_summary"
    ALT_METHODOLOGY_PE = "alt_methodology_pe"  # Alternative PE methodology
    INDIRECT_COST_INDICES = "indirect_cost_indices"

    # Physician Time and Work
    PHYSICIAN_TIME = "physician_time"
    PHYSICIAN_WORK = "physician_work"
    PEHR = "pehr"  # Physician/practitioner time

    # Malpractice
    MALPRACTICE = "malpractice"
    MALPRACTICE_OVERRIDE = "malpractice_override"

    # Crosswalks and Utilization
    ANALYTIC_CROSSWALK = "analytic_crosswalk"
    UTILIZATION_CROSSWALK = "utilization_crosswalk"
    CPT_CODES = "cpt_codes"
    PLACEHOLDER = "placeholder"

    # Policy Lists
    TELEHEALTH = "telehealth"
    DESIGNATED_CARE = "designated_care"
    INVASIVE_CARDIOLOGY = "invasive_cardiology"
    MPPR = "mppr"  # Multiple procedure payment reduction
    OPPS_CAP = "opps_cap"
    PHASE_IN = "phase_in"

    # Impact and Specialty
    IMPACT = "impact"
    SPECIALTY_ASSIGNMENT = "specialty_assignment"
    SPECIALTY_IMPACTS = "specialty_impacts"

    # Misvalued Codes
    MISVALUED_CODES = "misvalued_codes"

    # E&M Specific
    EM_GUIDELINES = "em_guidelines"  # 1995/1997 E&M guidelines
    EM_CODES = "em_codes"  # E&M code specific data
    EM_IMPACT = "em_impact"  # E&M payment impact

    # Other
    MARKET_BASED_SUPPLY = "market_based_supply"
    OTP_PAYMENT_RATES = "otp_payment_rates"  # Opioid treatment program
    ANESTHESIA = "anesthesia"
    PREVENTIVE = "preventive"
    VITAL_SIGNS = "vital_signs"
    NONEXCEPTED_ITEMS = "nonexcepted_items"
    REDUCTION = "reduction"  # Fee reductions
    LOW_VOLUME = "low_volume"  # Low volume services
    USAGE_RATE = "usage_rate"  # Usage rate files
    EFFICIENCY_ADJUSTMENT = "efficiency_adjustment"  # CY 2026+ efficiency adjustments
    PROCEDURE_SHARES = "procedure_shares"  # Estimated procedure shares
    RADIATION_SERVICES = "radiation_services"  # Radiation oncology services
    SKIN_SUBSTITUTE = "skin_substitute"  # Skin substitute products

    # Federal Register
    FEDERAL_REGISTER = "federal_register"
    XML = "xml"

    # General
    OTHER = "other"


class FileFormat(StrEnum):
    """File format for downloads."""

    ZIP = "zip"
    PDF = "pdf"
    XLSX = "xlsx"
    CSV = "csv"
    XML = "xml"
    HTML = "html"
    JSON = "json"


class FileMetadata(BaseModel):
    """Metadata for a single downloadable file."""

    key: str = Field(..., description="Short key identifier for the file")
    url: HttpUrl = Field(..., description="Download URL")
    category: FileCategory = Field(default=FileCategory.OTHER, description="File category")
    format: FileFormat | None = Field(None, description="File format")
    description: str | None = Field(None, description="Human-readable description")
    file_size_mb: float | None = Field(None, description="File size in MB if known")
    last_updated: date | None = Field(None, description="Last update date")
    schema_mapping: str | None = Field(
        None, description="Maps to schema in _schemas (e.g., 'pprvu_inputs')"
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Additional metadata (e.g., quarter for RVU files)"
    )

    @model_validator(mode="after")
    def infer_format(self) -> "FileMetadata":
        """Infer format from URL if not provided."""
        if self.format is not None:
            return self

        url = str(self.url).lower()

        if url.endswith(".zip"):
            self.format = FileFormat.ZIP
        elif url.endswith(".pdf"):
            self.format = FileFormat.PDF
        elif url.endswith((".xlsx", ".xls")):
            self.format = FileFormat.XLSX
        elif url.endswith(".csv"):
            self.format = FileFormat.CSV
        elif url.endswith(".xml"):
            self.format = FileFormat.XML
        elif url.endswith((".html", ".htm")):
            self.format = FileFormat.HTML
        elif url.endswith(".json"):
            self.format = FileFormat.JSON

        return self

    class Config:
        """Pydantic config."""

        use_enum_values = True


class RuleMetadata(BaseModel):
    """Metadata for a Federal Register rule or notice."""

    rule_type: RuleType = Field(..., description="Type of rule")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Rule-level metadata (citation, dates, etc.)"
    )
    files: dict[str, FileMetadata] = Field(
        default_factory=dict, description="Associated data files"
    )

    class Config:
        """Pydantic config."""

        use_enum_values = True


class YearInventory(BaseModel):
    """Inventory of all rules and files for a calendar year."""

    year: str = Field(..., description="Calendar year as string (e.g., '2024')")
    metadata: dict[str, Any] = Field(
        default_factory=dict, description="Year-level metadata (e.g., quarters_available)"
    )
    rules: dict[str, RuleMetadata] = Field(
        default_factory=dict, description="Rules by type (Proposed, Final, Correction)"
    )

    def get_rule(self, rule_type: RuleType | str) -> RuleMetadata | None:
        """Get rule by type."""
        if isinstance(rule_type, RuleType):
            rule_type = rule_type.value
        return self.rules.get(rule_type)

    def get_all_files(self) -> list[FileMetadata]:
        """Get all files across all rules for this year."""
        all_files = []
        for rule in self.rules.values():
            all_files.extend(rule.files.values())
        return all_files

    class Config:
        """Pydantic config."""

        use_enum_values = True


class DatasetInventory(BaseModel):
    """Complete inventory across all years for a dataset."""

    dataset_name: str = Field(..., description="Name of dataset (e.g., 'Physician Fee Schedule')")
    dataset_key: str = Field(..., description="Short key (e.g., 'pfs')")
    source_agency: str = Field(default="CMS", description="Source agency")
    description: str | None = Field(None, description="Dataset description")
    base_url: str | None = Field(None, description="Base URL for dataset resources")
    years: dict[str, YearInventory] = Field(
        default_factory=dict, description="Year inventories by year string"
    )
    last_updated: datetime = Field(
        default_factory=datetime.now, description="Last inventory update"
    )

    def get_year(self, year: str) -> YearInventory | None:
        """Get inventory for a specific year."""
        return self.years.get(year)

    def list_available_years(self) -> list[str]:
        """Get sorted list of available years."""
        return sorted(self.years.keys())

    def get_latest_year(self) -> YearInventory | None:
        """Get inventory for most recent year."""
        years = self.list_available_years()
        if not years:
            return None
        return self.years[years[-1]]

    class Config:
        """Pydantic config."""

        use_enum_values = True


class DownloadTask(BaseModel):
    """Represents a file download task for batch processing."""

    file_metadata: FileMetadata
    year: str
    rule_type: RuleType
    priority: int = Field(default=5, description="Priority 1-10, lower is higher priority")
    force_refresh: bool = Field(default=False, description="Force re-download if already cached")
    tags: list[str] = Field(default_factory=list, description="Tags for citation tracking")
    note: str | None = Field(None, description="Note for citation tracking")

    def to_cite_kwargs(self) -> dict[str, Any]:
        """Convert to kwargs for _cite.transform_cite()."""
        return {
            "url": str(self.file_metadata.url),
            "force_refresh": self.force_refresh,
            "note": self.note or f"{self.year} {self.rule_type} - {self.file_metadata.key}",
            "tags": self.tags
            + [
                "puf",
                "cms",
                self.year,
                str(self.rule_type).lower(),
                str(self.file_metadata.category),
            ],
        }

    class Config:
        """Pydantic config."""

        use_enum_values = True
