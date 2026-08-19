# © 2025 HarmonyCares
# All rights reserved.

"""Data models for ACOMS CLI integration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path


class AcomsCategory(StrEnum):
    """ACOMS DataHub categories accepted by the vendor CLI."""

    CCLF = "CCLF"
    REPORTS = "Reports"
    MONTHLY_EXCLUSION = "Monthly Exclusion Files"
    SHADOW_BUNDLES = "Shadow Bundles Data Files"
    PC_FLEX = "PC Flex Reports"


def normalize_category(category: str | AcomsCategory) -> AcomsCategory:
    """Normalize a user/catalog category string to an AcomsCategory."""
    if isinstance(category, AcomsCategory):
        return category

    normalized = category.strip().lower()
    aliases = {
        "claim and claim line feed (cclf) files": AcomsCategory.CCLF,
        "claim and claim line feeds": AcomsCategory.CCLF,
        "cclf files": AcomsCategory.CCLF,
    }
    if normalized in aliases:
        return aliases[normalized]

    for candidate in AcomsCategory:
        if candidate.value.lower() == normalized or candidate.name.lower() == normalized:
            return candidate

    valid = ", ".join(c.value for c in AcomsCategory)
    raise ValueError(f"Invalid ACOMS category '{category}'. Valid options: {valid}")


def infer_file_type_code(filename: str, category: str | AcomsCategory) -> int | None:
    """
    Infer an ACOMS file-type code from category and filename conventions.

    The vendor CLI currently does not include file-type code in ``--view``
    output, and ``--view --file <code>`` can still return the whole category.
    These rules keep downloads precise for known ACOMS naming tokens.
    """
    normalized_category = normalize_category(category)
    upper = filename.upper()
    tokens = {token for token in upper.replace("-", ".").replace("_", ".").split(".") if token}

    if normalized_category is AcomsCategory.CCLF:
        if "ZCWY" in upper:
            return 305
        if "ZCY" in upper or "ZCR" in upper or "CCLF" in upper:
            return 113

    if normalized_category is AcomsCategory.MONTHLY_EXCLUSION:
        if "MBI" in upper:
            return 183
        if "BNEX" in tokens or "BNEX" in upper:
            return 114

    if normalized_category is AcomsCategory.SHADOW_BUNDLES:
        return 244

    if normalized_category is AcomsCategory.REPORTS:
        report_tokens = {
            "BNMRK": 115,
            "HASSGN": 116,
            "QEXPU": 118,
            "AEXPU": 125,
            "CAHPS": 128,
            "OPIOID": 119,
            "QQR": 133,
            "QREC": 133,
            "NCBPQ": 132,
            "NCBPA": 126,
            "RIDFRP": 281,
            "FRP": 120,
            "STLMT": 120,
            "AALR": 129,
            "QALR": 131,
            "PATB": 134,
            "QMCQM": 306,
        }
        for token, code in report_tokens.items():
            if token in tokens or token in upper:
                return code

    if normalized_category is AcomsCategory.PC_FLEX:
        pc_flex_tokens = {
            "PCFASF": 308,
            "PPCP": 295,
            "PCFPSC": 286,
            "PCFWCR": 285,
            "PCPCM": 282,
            "PCFMP": 273,
        }
        for token, code in pc_flex_tokens.items():
            if token in tokens or token in upper:
                return code

    return None


@dataclass(frozen=True)
class FileTypeDefinition:
    """A file-type code advertised by ``acoms datahub --list``."""

    category: str
    name: str
    code: int


DEFAULT_FILE_TYPES: tuple[FileTypeDefinition, ...] = (
    FileTypeDefinition("CCLF", "Claim and Claim Line Feeds (CCLF) - Weekly", 305),
    FileTypeDefinition("CCLF", "Claim and Claim Line Feeds (CCLF)", 113),
    FileTypeDefinition(
        "Monthly Exclusion Files",
        "Excluded Beneficiary MBI XREF File",
        183,
    ),
    FileTypeDefinition(
        "Monthly Exclusion Files",
        "Beneficiary Data Sharing Exclusion File",
        114,
    ),
    FileTypeDefinition("PC Flex Reports", "PC Flex Annual Financial Settlement Reports", 308),
    FileTypeDefinition("PC Flex Reports", "Weekly PPCP Claims Reduction File", 295),
    FileTypeDefinition(
        "PC Flex Reports",
        "PC Flex Monthly Provider Summary of Claims Reductions Report",
        286,
    ),
    FileTypeDefinition("PC Flex Reports", "PC Flex Weekly Claims Reductions File", 285),
    FileTypeDefinition(
        "PC Flex Reports",
        "PC Flex Quality Person-Centered Primary Care Measure (PCPCM) Report",
        282,
    ),
    FileTypeDefinition("PC Flex Reports", "PC Flex Monthly Payment Report", 273),
    FileTypeDefinition("Reports", "Adhoc Report", 124),
    FileTypeDefinition("Reports", "CAHPS Survey Results Report", 128),
    FileTypeDefinition("Reports", "Opioid Measures Report", 119),
    FileTypeDefinition("Reports", "Assignment List Report - Annual", 129),
    FileTypeDefinition("Reports", "Financial Reconciliation Package", 120),
    FileTypeDefinition("Reports", "Assignment Summary Report - Quarterly", 130),
    FileTypeDefinition("Reports", "Assignment Summary Report - Annual", 123),
    FileTypeDefinition(
        "Reports",
        "Medicare Clinical Quality Measures (CQM) Beneficiary List",
        306,
    ),
    FileTypeDefinition(
        "Reports",
        "Revised Initial Determination Financial Reconciliation Package",
        281,
    ),
    FileTypeDefinition("Reports", "NCBP Data File - Quarterly", 132),
    FileTypeDefinition("Reports", "NCBP Data File - Annual", 126),
    FileTypeDefinition("Reports", "Historical Benchmark Report", 115),
    FileTypeDefinition("Reports", "Assignment Report", 116),
    FileTypeDefinition("Reports", "Other Reports", 112),
    FileTypeDefinition("Reports", "ACO Provider/Supplier List Report", 134),
    FileTypeDefinition("Reports", "Adhoc Report", 152),
    FileTypeDefinition("Reports", "EXPU Report - Quarterly", 118),
    FileTypeDefinition("Reports", "Quality Measures Audit Report", 127),
    FileTypeDefinition("Reports", "Quality Reconciliation Report", 133),
    FileTypeDefinition("Reports", "Web Interface Patient Ranking Report", 117),
    FileTypeDefinition("Reports", "EXPU Report - Annual", 125),
    FileTypeDefinition("Reports", "Assignment List Report - Quarterly", 131),
    FileTypeDefinition("Shadow Bundles Data Files", "Shadow Bundle Reports for SSP", 244),
)


@dataclass
class FileInfo:
    """Information about a file returned by ACOMS."""

    name: str
    size: int | None = None
    last_updated: str | None = None


@dataclass
class DownloadResult:
    """Result of an ACOMS download operation."""

    success: bool
    files_downloaded: list[Path]
    errors: list[str]
    download_path: Path
    started_at: datetime
    completed_at: datetime | None = None

    @property
    def duration(self) -> float | None:
        """Duration of the operation in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    @property
    def file_count(self) -> int:
        """Number of newly detected files."""
        return len(self.files_downloaded)


@dataclass
class DateFilter:
    """Date filtering options for ACOMS DataHub queries."""

    created_after: str | None = None
    created_between: tuple[str, str] | None = None
    created_within_last_month: bool = False
    created_within_last_week: bool = False
    updated_after: str | None = None
    updated_between: tuple[str, str] | None = None

    def to_cli_args(self) -> list[str]:
        """Convert the filter to ACOMS CLI arguments."""
        args: list[str] = []
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
    """Query parameters for ACOMS DataHub operations."""

    aco_id: str
    year: int
    category: AcomsCategory | None = None
    file_type_code: int | None = None
    date_filter: DateFilter | None = None

    def to_cli_args(self) -> list[str]:
        """Convert query parameters to ACOMS CLI arguments."""
        args = ["--aco", self.aco_id, "--year", str(self.year)]
        if self.category:
            args.extend(["--category", self.category.value])
        if self.file_type_code is not None:
            args.extend(["--file", str(self.file_type_code)])
        if self.date_filter:
            args.extend(self.date_filter.to_cli_args())
        return args
