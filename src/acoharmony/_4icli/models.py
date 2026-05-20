# © 2025 HarmonyCares
# All rights reserved.

"""Data models for 4icli integration."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum, StrEnum
from pathlib import Path


class DataHubCategory(StrEnum):
    """DataHub file categories."""

    BENEFICIARY_LIST = "Beneficiary List"
    CCLF = "CCLF"
    REPORTS = "Reports"


class FileTypeCode(int, Enum):
    """File type codes used by 4icli."""

    # Beneficiary List
    PROSPECTIVE_PLUS_OPPORTUNITY_REPORT = 170
    PROVIDER_ALIGNMENT_REPORT = 165
    BENEFICIARY_ALIGNMENT_REPORT = 159
    BENEFICIARY_LIST = 111
    SIGNED_ATTESTATION_VOLUNTARY_ALIGNMENT = 175

    # CCLF Files
    CCLF = 113  # Default
    CLAIM_REPROCESSING_FILE = 237
    CCLF_MANAGEMENT_REPORT = 198

    # Monthly Exclusion Files
    BENEFICIARY_DATA_SHARING_EXCLUSION = 114
    EXCLUDED_BENEFICIARY_MBI_XREF = 183

    # Reports
    LETTER_TO_ACO_PREPAYMENT = 266
    PRELIM_FULL_ALT_PAYMENT_ARRANGEMENT_UNREDACTED = 161
    WEEKLY_CLAIMS_REDUCTION = 157
    PRELIM_ALT_PAYMENT_ARRANGEMENT = 156
    RISK_ADJUSTMENT_DATA = 140
    ADMINISTRATIVELY_SUPPRESSED_BENEFICIARIES = 141
    OTHER_REPORTS = 112
    EXPENDITURE_FROM_PARTICIPATING_FACILITIES = 201
    SHADOW_BUNDLE_REPORTS = 243
    ESTIMATED_CI_SEP_CHANGE_THRESHOLD = 265
    PRELIM_BENCHMARK_UNREDACTED = 160
    ACO_FINANCIAL_GUARANTEE_AMOUNT = 267
    QUARTERLY_BENEFICIARY_LEVEL_QUALITY = 268
    ANNUAL_BENEFICIARY_LEVEL_QUALITY = 269
    BENEFICIARY_HEDR_TRANSPARENCY_FILES = 272
    ADHOC_FILE = 274
    PECOS_TERMINATIONS_MONTHLY = 298
    PRELIM_ALT_PAYMENT_ARRANGEMENT_UNREDACTED = 220
    PRELIM_ALIGNMENT_ESTIMATE = 221
    PROVISIONAL_ALIGNMENT_ESTIMATE = 177
    QUARTERLY_QUALITY_REPORT = 218
    ANNUAL_QUALITY_REPORT_218 = 217
    ALT_PAYMENT_ARRANGEMENT_REPORT = 216
    BENCHMARK_REPORT = 215
    MONTHLY_EXPENDITURE_REPORT = 214
    PRELIM_ALT_PAYMENT_ARRANGEMENT_213 = 213
    PRELIM_BENCHMARK_DC = 212
    RISK_SCORE_REPORT = 211
    PRELIM_BENCHMARK_DC_179 = 179
    PRELIM_BENCHMARK_UNREDACTED_219 = 219
    QUARTERLY_QUALITY_REPORT_176 = 176
    PROVISIONAL_RECONCILIATION = 173
    FINAL_RECONCILIATION = 167
    PRELIM_RECONCILIATION = 166
    ANNUAL_QUALITY_REPORT_169 = 169
    ALT_PAYMENT_ARRANGEMENT_164 = 164
    QUARTERLY_BENCHMARK = 163
    MONTHLY_EXPENDITURE_162 = 162


class FileType(StrEnum):
    """ACO REACH file types based on naming patterns."""

    # Alignment files
    PROVIDER_ALIGNMENT = "PALMR"  # Provider Alignment Report (PAR)
    VOLUNTARY_ALIGNMENT = "PBVAR"  # Voluntary Alignment Response Files
    BENEFICIARY_ALIGNMENT = "ALG"  # Beneficiary Alignment Reports (BAR) - ALGC/ALGR

    # CCLF files
    CCLF = "CCLF"  # Claim and Claim Line Feed Files

    # Reports
    WEEKLY_CLAIMS_REDUCTION = "TPARC"  # Weekly Payment Reduction Files
    RISK_ADJUSTMENT = "RAP"  # Risk Adjustment Payment Reports
    QUALITY_REPORT = "BLQQR"  # Beneficiary Level Quarterly Quality Reports


@dataclass
class FileInfo:
    """Information about a DataHub file."""

    name: str
    file_type: FileType | None
    size: int | None = None
    created_date: datetime | None = None
    updated_date: datetime | None = None
    download_url: str | None = None

    @classmethod
    def from_filename(cls, filename: str) -> "FileInfo":
        """Detect file type from filename pattern."""
        file_type = None

        # Pattern matching based on documented file patterns
        if "PALMR" in filename:
            file_type = FileType.PROVIDER_ALIGNMENT
        elif "PBVAR" in filename:
            file_type = FileType.VOLUNTARY_ALIGNMENT
        elif "ALG" in filename:  # ALGC or ALGR
            file_type = FileType.BENEFICIARY_ALIGNMENT
        elif "TPARC" in filename:
            file_type = FileType.WEEKLY_CLAIMS_REDUCTION
        elif filename.startswith("CCLF"):
            file_type = FileType.CCLF
        elif "RAP" in filename:
            file_type = FileType.RISK_ADJUSTMENT
        elif "BLQQR" in filename:
            file_type = FileType.QUALITY_REPORT

        return cls(name=filename, file_type=file_type)


@dataclass
class DownloadResult:
    """Result of a file download operation."""

    success: bool
    files_downloaded: list[Path]
    errors: list[str]
    download_path: Path
    started_at: datetime
    completed_at: datetime | None = None

    @property
    def duration(self) -> float | None:
        """Duration of download in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    @property
    def file_count(self) -> int:
        """Number of files downloaded."""
        return len(self.files_downloaded)


@dataclass
class DateFilter:
    """Date filtering options for file queries."""

    created_after: str | None = None  # YYYY-MM-DD
    created_between: tuple[str, str] | None = None  # (start, end)
    created_within_last_month: bool = False
    created_within_last_week: bool = False
    updated_after: str | None = None  # YYYY-MM-DD
    updated_between: tuple[str, str] | None = None  # (start, end)

    def to_cli_args(self) -> list[str]:
        """Convert to 4icli command line arguments."""
        args = []

        if self.created_after:
            args.extend(["--createdAfter", self.created_after])
        if self.created_between:
            args.extend(
                ["--createdBetween", f"{self.created_between[0]},{self.created_between[1]}"]
            )
        if self.created_within_last_month:
            args.append("--createdWithinLastMonth")
        if self.created_within_last_week:
            args.append("--createdWithinLastWeek")
        if self.updated_after:
            args.extend(["--updatedAfter", self.updated_after])
        if self.updated_between:
            args.extend(
                ["--updatedBetween", f"{self.updated_between[0]},{self.updated_between[1]}"]
            )

        return args


@dataclass
class DataHubQuery:
    """Query parameters for DataHub operations."""

    apm_id: str | None = None
    year: int = 2025
    category: DataHubCategory | None = None
    file_type_code: FileTypeCode | None = None
    date_filter: DateFilter | None = None

    def to_cli_args(self) -> list[str]:
        """Convert to 4icli command line arguments."""
        args = []

        if self.apm_id:
            args.extend(["-a", self.apm_id])
        args.extend(["-y", str(self.year)])

        if self.category:
            args.extend(["-c", self.category.value])
        if self.file_type_code is not None:
            args.extend(["-f", str(self.file_type_code.value)])

        if self.date_filter:
            args.extend(self.date_filter.to_cli_args())

        return args
