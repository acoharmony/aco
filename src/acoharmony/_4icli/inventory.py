# © 2025 HarmonyCares
# All rights reserved.

"""
Inventory discovery for 4icli DataHub files across multiple years.

 functionality to discover and catalog all available files
in the CMS DataHub across multiple performance years without downloading them.
"""

import json
import re
import subprocess
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from acoharmony._exceptions import ACOHarmonyException

from .._log import LogWriter
from .._registry import SchemaRegistry as CentralRegistry
from .config import FourICLIConfig, get_current_year
from .models import DataHubCategory, FileTypeCode
from .parser import parse_datahub_output


def _parse_size_to_bytes(size_str: str) -> int | None:
    """
    Convert size string like '64.66 MB' to bytes.

        Args:
            size_str: Size string from 4icli output (e.g., "64.66 MB", "7.77 KB")

        Returns:
            Size in bytes, or None if parsing fails
    """
    try:
        # Match pattern like "64.66 MB"
        match = re.match(r"([\d.]+)\s*(KB|MB|GB|TB)", size_str, re.IGNORECASE)
        if not match:
            return None

        value = float(match.group(1))
        unit = match.group(2).upper()

        multipliers = {
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4,
        }

        return int(value * multipliers.get(unit, 1))
    except (ValueError, AttributeError):  # ALLOWED: Returns None to indicate error
        return None


def _load_schema_patterns() -> list[dict[str, Any]]:
    """
    Load file patterns and type codes from the central SchemaRegistry.

        Returns:
            List of pattern dictionaries with keys: pattern, file_type_code, schema_name
    """
    # Ensure _tables models are imported so CentralRegistry is populated
    from acoharmony import _tables as _  # noqa: F401

    patterns = []

    for schema_name in CentralRegistry.list_schemas():
        fouricli_block = CentralRegistry.get_four_icli_config(schema_name)
        if not fouricli_block:
            continue

        file_type_code = fouricli_block.get("fileTypeCode")
        file_pattern = fouricli_block.get("filePattern")

        if file_pattern and file_type_code is not None:
            # Handle multiple patterns separated by comma
            for pat in file_pattern.split(","):
                patterns.append(
                    {
                        "pattern": pat.strip(),
                        "file_type_code": file_type_code,
                        "schema_name": schema_name,
                    }
                )

    return patterns


def _match_file_type_code(filename: str, patterns: list[dict[str, Any]]) -> int | None:
    """
    Match filename against schema patterns to determine file type code.

        Patterns are sorted by specificity (most specific first) to ensure
        accurate matching. A pattern like 'P.D????.ACO.*.zip' is more specific
        than '*' and will be tried first.

        Args:
            filename: Filename to match
            patterns: List of pattern dictionaries from _load_schema_patterns()

        Returns:
            Matched file type code, or None if no match
    """

    def pattern_specificity(pattern: str) -> int:
        """Calculate pattern specificity score (higher = more specific)."""
        # Pure wildcard has lowest priority
        if pattern == "*":
            return 0

        # Count non-wildcard characters
        non_wildcard = sum(1 for c in pattern if c not in ["*", "?"])

        # Longer patterns with more fixed characters are more specific
        return len(pattern) * 10 + non_wildcard

    # Sort patterns by specificity (most specific first)
    sorted_patterns = sorted(
        patterns, key=lambda p: pattern_specificity(p["pattern"]), reverse=True
    )

    for pattern_info in sorted_patterns:
        if fnmatch(filename, pattern_info["pattern"]):
            return pattern_info["file_type_code"]
    return None


@dataclass
class FileInventoryEntry:
    """Represents a single file in the inventory."""

    filename: str
    category: str
    file_type_code: int | None
    year: int
    size_bytes: int | None = None
    last_updated: str | None = None
    discovered_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class InventoryResult:
    """Result of an inventory discovery operation."""

    apm_id: str
    categories: list[str]
    years: list[int]
    total_files: int
    files_by_year: dict[int, int]
    files_by_category: dict[str, int]
    files: list[FileInventoryEntry]
    started_at: datetime
    completed_at: datetime | None = None
    errors: list[str] | None = None

    @property
    def duration_seconds(self) -> float | None:
        """Duration of inventory operation in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "apm_id": self.apm_id,
            "categories": self.categories,
            "years": self.years,
            "total_files": self.total_files,
            "files_by_year": self.files_by_year,
            "files_by_category": self.files_by_category,
            "files": [f.to_dict() for f in self.files],
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors,
        }

    def save_to_json(self, output_path: Path) -> None:
        """Save inventory to JSON file."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load_from_json(cls, input_path: Path) -> "InventoryResult":
        """Load inventory from JSON file."""
        with open(input_path) as f:
            data = json.load(f)

        # Reconstruct file entries
        files = [FileInventoryEntry(**entry) for entry in data["files"]]

        # Convert string keys to int for files_by_year (JSON serialization converts int keys to strings)
        files_by_year = {int(k): v for k, v in data["files_by_year"].items()}

        return cls(
            apm_id=data["apm_id"],
            categories=data["categories"],
            years=data["years"],
            total_files=data["total_files"],
            files_by_year=files_by_year,
            files_by_category=data["files_by_category"],
            files=files,
            started_at=datetime.fromisoformat(data["started_at"]),
            completed_at=datetime.fromisoformat(data["completed_at"])
            if data["completed_at"]
            else None,
            errors=data.get("errors"),
        )


class InventoryDiscovery:
    """
    Discovers and catalogs all available files in CMS DataHub.

        Uses 4icli view (-v) functionality to list files without downloading.
    """

    def __init__(
        self,
        config: FourICLIConfig | None = None,
        log_writer: LogWriter | None = None,
        request_delay: float = 2.0,
    ):
        """
        Initialize inventory discovery.

                Args:
                    config: Configuration object. If None, uses profile configuration.
                    log_writer: LogWriter instance. If None, creates new one.
                    request_delay: Delay in seconds between API requests to avoid rate limits (default: 2.0)
        """
        self.config = config or FourICLIConfig.from_profile()
        self.log_writer = log_writer or LogWriter(name="4icli-inventory")
        self.request_delay = request_delay

    def get_inventory_path(self) -> Path:
        """
        Get default path for inventory file.

                Inventory file is stored in workspace logs/tracking directory:
                {workspace}/logs/tracking/4icli_inventory_state.json

                Returns:
                    Path to inventory file
        """
        tracking_dir = self.config.log_dir / "tracking"
        tracking_dir.mkdir(parents=True, exist_ok=True)

        return tracking_dir / "4icli_inventory_state.json"

    def _run_view_command(
        self,
        apm_id: str,
        category: DataHubCategory | None = None,
        file_type_code: FileTypeCode | None = None,
        year: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Run 4icli view command to list files.

                Args:
                    apm_id: APM Entity ID
                    category: DataHub category
                    file_type_code: Specific file type code
                    year: Performance year

                Returns:
                    List of file metadata dictionaries with keys: filename, size_bytes, last_updated
        """
        year = year or get_current_year()
        args = ["4icli", "datahub", "-v", "-a", apm_id, "-y", str(year)]

        if category:
            args.extend(["-c", category.value])
        if file_type_code is not None:
            args.extend(["-f", str(file_type_code.value)])

        self.log_writer.info(
            f"Viewing files for year {year}",
            category=category.value if category else "all",
            file_type_code=file_type_code.value if file_type_code else None,
        )

        try:
            # Use docker exec to run 4icli
            docker_cmd = ["docker", "exec", "4icli"] + args

            result = subprocess.run(
                docker_cmd,
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )

            if result.returncode != 0:
                # Check if it's an authentication error
                if (
                    "Error authenticating" in result.stderr
                    or "This key is no longer active" in result.stderr
                ):
                    self.log_writer.error(
                        f"Authentication failed for year {year}. Refresh creds via deploy/images/4icli/bootstrap.sh after a portal rotation.",
                        year=year,
                        stderr=result.stderr,
                    )
                else:
                    self.log_writer.warning(
                        f"Command failed for year {year}: {result.stderr}", year=year
                    )
                return []

            # Add delay to avoid rate limits
            time.sleep(self.request_delay)

            # Parse output using centralized parser
            parsed = parse_datahub_output(result.stdout, result.stderr)

            # Convert to expected format
            files = []
            for file_entry in parsed.files:
                files.append(
                    {
                        "filename": file_entry.filename,
                        "size_bytes": file_entry.size_bytes,
                        "last_updated": file_entry.last_updated,
                    }
                )

            # Log parsing warnings if any
            if parsed.errors:
                for error in parsed.errors:
                    self.log_writer.warning(f"Parser warning: {error}")

            return files

        except subprocess.TimeoutExpired as e:
            self.log_writer.error(f"Command timed out for year {year}", year=year)
            raise ACOHarmonyException(
                f"4icli datahub view command timed out for year {year}",
                original_error=e,
                why="Command exceeded 120 second timeout",
                how="Check if DataHub is responsive, consider increasing timeout",
                metadata={"year": year, "apm_id": apm_id},
            ) from e
        except Exception as e:
            self.log_writer.error(
                f"Error running view command for year {year}: {str(e)}", year=year
            )
            raise ACOHarmonyException(
                f"4icli datahub view command failed for year {year}: {e}",
                original_error=e,
                why="DataHub command execution failed",
                how="Check docker container is running and 4icli is properly configured",
                metadata={"year": year, "apm_id": apm_id},
            ) from e

    def discover_year(
        self,
        apm_id: str,
        year: int,
        category: DataHubCategory | None = None,
        file_type_code: FileTypeCode | None = None,
    ) -> list[FileInventoryEntry]:
        """
        Discover all files for a specific year.

                Args:
                    apm_id: APM Entity ID
                    year: Performance year
                    category: Optional category filter
                    file_type_code: Optional file type filter

                Returns:
                    List of file inventory entries
        """
        file_metadata_list = self._run_view_command(
            apm_id=apm_id, category=category, file_type_code=file_type_code, year=year
        )

        entries = []
        for file_metadata in file_metadata_list:
            entry = FileInventoryEntry(
                filename=file_metadata["filename"],
                category=category.value if category else "unknown",
                file_type_code=file_type_code.value if file_type_code else None,
                year=year,
                size_bytes=file_metadata.get("size_bytes"),
                last_updated=file_metadata.get("last_updated"),
                discovered_at=datetime.now().isoformat(),
            )
            entries.append(entry)

        self.log_writer.info(f"Discovered {len(entries)} files for year {year}", year=year)
        return entries

    def discover_years(
        self,
        apm_id: str,
        start_year: int = 2022,
        end_year: int | None = None,
        categories: list[DataHubCategory] | None = None,
        file_type_codes: list[FileTypeCode] | None = None,
    ) -> InventoryResult:
        """
        Discover all files across multiple years.

                Args:
                    apm_id: APM Entity ID
                    start_year: Starting year (inclusive)
                    end_year: Ending year (inclusive)
                    categories: List of categories to scan. If None, scans common categories.
                    file_type_codes: List of file type codes to scan. If None, scans all.

                Returns:
                    InventoryResult with complete inventory
        """
        end_year = end_year or get_current_year()
        started_at = datetime.now()
        self.log_writer.info(
            f"Starting inventory discovery for APM {apm_id}",
            apm_id=apm_id,
            start_year=start_year,
            end_year=end_year,
        )

        # Default to common categories if not specified
        if categories is None:
            categories = [
                DataHubCategory.BENEFICIARY_LIST,
                DataHubCategory.CCLF,
                DataHubCategory.REPORTS,
            ]

        all_files: list[FileInventoryEntry] = []
        errors: list[str] = []
        years = list(range(start_year, end_year + 1))

        for year_idx, year in enumerate(years, 1):
            self.log_writer.info(
                f"Processing year {year} ({year_idx}/{len(years)})",
                year=year,
                progress=f"{year_idx}/{len(years)}",
            )

            for cat_idx, category in enumerate(categories, 1):
                try:
                    self.log_writer.info(
                        f"  Scanning {category.value} ({cat_idx}/{len(categories)})",
                        year=year,
                        category=category.value,
                    )

                    if file_type_codes:
                        # Query specific file types
                        for file_type_code in file_type_codes:
                            entries = self.discover_year(
                                apm_id=apm_id,
                                year=year,
                                category=category,
                                file_type_code=file_type_code,
                            )
                            all_files.extend(entries)
                    else:
                        # Query entire category
                        entries = self.discover_year(apm_id=apm_id, year=year, category=category)
                        all_files.extend(entries)

                except Exception as e:  # ALLOWED: Batch processing - collect error, continue with remaining years/categories
                    error_msg = f"Error discovering {category.value} for year {year}: {str(e)}"
                    self.log_writer.error(error_msg, year=year, category=category.value)
                    errors.append(error_msg)

        # Build statistics
        files_by_year: dict[int, int] = defaultdict(int)
        files_by_category: dict[str, int] = defaultdict(int)

        for file_entry in all_files:
            files_by_year[file_entry.year] += 1
            files_by_category[file_entry.category] += 1

        completed_at = datetime.now()
        result = InventoryResult(
            apm_id=apm_id,
            categories=[c.value for c in categories],
            years=years,
            total_files=len(all_files),
            files_by_year=dict(files_by_year),
            files_by_category=dict(files_by_category),
            files=all_files,
            started_at=started_at,
            completed_at=completed_at,
            errors=errors if errors else None,
        )

        self.log_writer.info(
            "Inventory discovery completed",
            total_files=result.total_files,
            duration_seconds=result.duration_seconds,
        )

        # Warn if inventory is empty - likely authentication or configuration issue
        if result.total_files == 0:
            self.log_writer.warning(
                "Inventory returned 0 files. This may indicate authentication issues. "
                "Refresh creds via deploy/images/4icli/bootstrap.sh after a portal rotation, "
                "or check that the APM ID and categories are correct.",
                apm_id=apm_id,
                categories=[c.value for c in categories],
            )

        return result

    def discover_all_cclf_years(
        self,
        apm_id: str,
        start_year: int = 2020,
        end_year: int | None = None,
    ) -> InventoryResult:
        """
        Discover all CCLF files across multiple years.

                Convenience method for CCLF-only inventory.

                Args:
                    apm_id: APM Entity ID
                    start_year: Starting year (inclusive)
                    end_year: Ending year (inclusive)

                Returns:
                    InventoryResult with CCLF files only
        """
        end_year = end_year or get_current_year()
        return self.discover_years(
            apm_id=apm_id,
            start_year=start_year,
            end_year=end_year,
            categories=[DataHubCategory.CCLF],
            file_type_codes=[FileTypeCode.CCLF],
        )

    def enrich_with_file_type_codes(self, result: InventoryResult) -> InventoryResult:
        """
        Enrich inventory with file type codes based on schema pattern matching.

                This is run as a separate step after inventory discovery completes.
                It matches filenames against patterns in schema files to populate the
                file_type_code field.

                Args:
                    result: InventoryResult to enrich

                Returns:
                    Updated InventoryResult with file_type_codes populated
        """
        self.log_writer.info("Enriching inventory with file type codes from schema patterns")

        # Load patterns from schema files
        patterns = _load_schema_patterns()
        self.log_writer.info(f"Loaded {len(patterns)} file patterns from schemas")

        # Match each file against patterns
        matched_count = 0
        for file_entry in result.files:
            if file_entry.file_type_code is None:
                matched_code = _match_file_type_code(file_entry.filename, patterns)
                if matched_code:
                    file_entry.file_type_code = matched_code
                    matched_count += 1

        self.log_writer.info(
            f"Matched {matched_count} files to file type codes",
            matched_count=matched_count,
            total_files=result.total_files,
        )

        return result

    def get_summary(self, result: InventoryResult) -> dict[str, Any]:
        """
        Generate a summary of the inventory.

                Args:
                    result: InventoryResult to summarize

                Returns:
                    Dictionary with summary statistics
        """
        return {
            "apm_id": result.apm_id,
            "total_files": result.total_files,
            "years_scanned": len(result.years),
            "year_range": f"{min(result.years)}-{max(result.years)}",
            "categories_scanned": len(result.categories),
            "files_by_year": result.files_by_year,
            "files_by_category": result.files_by_category,
            "duration_seconds": result.duration_seconds,
            "errors": len(result.errors) if result.errors else 0,
        }

    def find_files_by_pattern(
        self, result: InventoryResult, pattern: str
    ) -> list[FileInventoryEntry]:
        """
        Find files matching a pattern in the inventory.

                Args:
                    result: InventoryResult to search
                    pattern: String pattern to match (case-insensitive)

                Returns:
                    List of matching file entries
        """
        pattern_lower = pattern.lower()
        return [f for f in result.files if pattern_lower in f.filename.lower()]

    def get_files_by_year(self, result: InventoryResult, year: int) -> list[FileInventoryEntry]:
        """
        Get all files for a specific year.

                Args:
                    result: InventoryResult to filter
                    year: Year to filter by

                Returns:
                    List of file entries for the specified year
        """
        return [f for f in result.files if f.year == year]

    def get_files_by_category(
        self, result: InventoryResult, category: str
    ) -> list[FileInventoryEntry]:
        """
        Get all files for a specific category.

                Args:
                    result: InventoryResult to filter
                    category: Category to filter by

                Returns:
                    List of file entries for the specified category
        """
        return [f for f in result.files if f.category == category]
