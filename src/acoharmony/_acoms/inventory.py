# © 2025 HarmonyCares
# All rights reserved.

"""Inventory discovery for ACOMS DataHub files."""

from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from acoharmony._exceptions import ACOHarmonyException

from .._log import LogWriter
from .client import Acoms, AcomsError
from .config import AcomsConfig, get_current_year
from .models import (
    DEFAULT_FILE_TYPES,
    AcomsCategory,
    FileTypeDefinition,
    infer_file_type_code,
    normalize_category,
)


def _load_schema_acoms_patterns() -> list[dict[str, Any]]:
    """Load ACOMS file patterns and type codes from registered _tables schemas."""
    from acoharmony import _tables as _  # noqa: F401
    from acoharmony._registry import SchemaRegistry as CentralRegistry

    patterns: list[dict[str, Any]] = []
    for schema_name in CentralRegistry.list_schemas():
        acoms_block = CentralRegistry.get_acoms_config(schema_name)
        if not acoms_block:
            continue

        file_pattern = acoms_block.get("filePattern")
        if not file_pattern:
            continue

        file_type_code = acoms_block.get("fileTypeCode")
        file_type_codes = acoms_block.get("fileTypeCodes") or []
        category = acoms_block.get("category")

        for pattern in str(file_pattern).split(","):
            cleaned = pattern.strip()
            if not cleaned:
                continue
            patterns.append(
                {
                    "schema_name": schema_name,
                    "category": category,
                    "pattern": cleaned,
                    "file_type_code": file_type_code,
                    "file_type_codes": list(file_type_codes),
                }
            )

    return patterns


def _match_schema_acoms_file_type_code(
    filename: str,
    category: str,
    patterns: list[dict[str, Any]],
) -> int | None:
    """Match a filename against registered ACOMS schema metadata."""
    try:
        normalized_category = normalize_category(category).value
    except ValueError:
        normalized_category = category

    upper = filename.upper()
    for pattern_info in patterns:
        pattern_category = pattern_info.get("category")
        if pattern_category:
            try:
                normalized_pattern_category = normalize_category(pattern_category).value
            except ValueError:
                normalized_pattern_category = pattern_category
            if normalized_pattern_category != normalized_category:
                continue

        if not fnmatch(filename, pattern_info["pattern"]):
            continue

        file_type_codes = pattern_info.get("file_type_codes") or []
        if 305 in file_type_codes and "ZCWY" in upper:
            return 305

        file_type_code = pattern_info.get("file_type_code")
        if file_type_code is not None:
            return int(file_type_code)

        if len(file_type_codes) == 1:
            return int(file_type_codes[0])

    return None


@dataclass
class FileInventoryEntry:
    """A single file in the ACOMS inventory."""

    filename: str
    category: str
    file_type_code: int | None
    year: int
    size_bytes: int | None = None
    last_updated: str | None = None
    discovered_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary."""
        return asdict(self)


@dataclass
class InventoryResult:
    """Result of an ACOMS inventory discovery operation."""

    aco_id: str
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
        """Convert to a JSON-serializable dictionary."""
        return {
            "aco_id": self.aco_id,
            "categories": self.categories,
            "years": self.years,
            "total_files": self.total_files,
            "files_by_year": self.files_by_year,
            "files_by_category": self.files_by_category,
            "files": [file_entry.to_dict() for file_entry in self.files],
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors,
        }

    def save_to_json(self, output_path: Path) -> None:
        """Save inventory to JSON."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load_from_json(cls, input_path: Path) -> InventoryResult:
        """Load inventory from JSON."""
        with open(input_path) as f:
            data = json.load(f)

        return cls(
            aco_id=data["aco_id"],
            categories=data["categories"],
            years=[int(year) for year in data["years"]],
            total_files=data["total_files"],
            files_by_year={int(k): v for k, v in data["files_by_year"].items()},
            files_by_category=data["files_by_category"],
            files=[FileInventoryEntry(**entry) for entry in data["files"]],
            started_at=datetime.fromisoformat(data["started_at"]),
            completed_at=datetime.fromisoformat(data["completed_at"])
            if data["completed_at"]
            else None,
            errors=data.get("errors"),
        )


class InventoryDiscovery:
    """Discover and catalog ACOMS DataHub files across years/categories."""

    def __init__(
        self,
        config: AcomsConfig | None = None,
        log_writer: LogWriter | None = None,
        request_delay: float | None = None,
    ):
        self.config = config or AcomsConfig.from_profile()
        self.log_writer = log_writer or LogWriter(name="acoms-inventory")
        self.request_delay = (
            request_delay if request_delay is not None else self.config.request_delay
        )
        self._file_type_catalog: list[FileTypeDefinition] | None = None

    def get_inventory_path(self) -> Path:
        """Return the default ACOMS inventory state path."""
        tracking_dir = self.config.log_dir / "tracking"
        tracking_dir.mkdir(parents=True, exist_ok=True)
        return tracking_dir / "acoms_inventory_state.json"

    def _client(self) -> Acoms:
        return Acoms(
            config=self.config,
            log_writer=self.log_writer,
            enable_duplicate_detection=False,
        )

    def _load_file_type_catalog(self) -> list[FileTypeDefinition]:
        if self._file_type_catalog is not None:
            return self._file_type_catalog

        try:
            catalog = self._client().list_file_types()
            if catalog:
                self._file_type_catalog = catalog
                return catalog
        except AcomsError as e:
            self.log_writer.warning(
                "Could not load live ACOMS file-type catalog; using bundled defaults",
                error=str(e),
            )

        self._file_type_catalog = list(DEFAULT_FILE_TYPES)
        return self._file_type_catalog

    def _run_view_command(
        self,
        aco_id: str,
        category: AcomsCategory | None = None,
        file_type_code: int | None = None,
        year: int | None = None,
    ) -> list[dict[str, Any]]:
        """Run ``acoms datahub --view`` and return parsed file metadata."""
        resolved_year = year or get_current_year()
        client = self._client()

        try:
            files = client.view_files(
                category=category,
                year=resolved_year,
                aco_id=aco_id,
                file_type_code=file_type_code,
            )
            time.sleep(self.request_delay)
            return [
                {
                    "filename": file_info.name,
                    "size_bytes": file_info.size,
                    "last_updated": file_info.last_updated,
                }
                for file_info in files
            ]
        except AcomsError as e:
            self.log_writer.error(
                f"ACOMS view command failed for year {resolved_year}: {e}",
                year=resolved_year,
                category=category.value if category else None,
                file_type_code=file_type_code,
            )
            raise ACOHarmonyException(
                f"ACOMS datahub view command failed for year {resolved_year}: {e}",
                original_error=e,
                why="ACOMS command execution failed",
                how="Check that the acoms container is running and authenticated.",
                metadata={"year": resolved_year, "aco_id": aco_id},
            ) from e

    def discover_year(
        self,
        aco_id: str,
        year: int,
        category: AcomsCategory | None = None,
        file_type_code: int | None = None,
    ) -> list[FileInventoryEntry]:
        """Discover ACOMS files for one year/category/type query."""
        file_metadata = self._run_view_command(
            aco_id=aco_id,
            category=category,
            file_type_code=file_type_code,
            year=year,
        )

        entries = [
            FileInventoryEntry(
                filename=item["filename"],
                category=category.value if category else "unknown",
                file_type_code=file_type_code,
                year=year,
                size_bytes=item.get("size_bytes"),
                last_updated=item.get("last_updated"),
                discovered_at=datetime.now().isoformat(),
            )
            for item in file_metadata
        ]
        self.log_writer.info(
            f"Discovered {len(entries)} ACOMS files for year {year}",
            year=year,
            category=category.value if category else None,
            file_type_code=file_type_code,
        )
        return entries

    def discover_years(
        self,
        aco_id: str,
        start_year: int = 2022,
        end_year: int | None = None,
        categories: list[AcomsCategory] | None = None,
        file_type_codes: list[int] | None = None,
    ) -> InventoryResult:
        """Discover ACOMS files across multiple performance years."""
        end_year = end_year or get_current_year()
        started_at = datetime.now()
        years = list(range(start_year, end_year + 1))
        categories = categories or list(AcomsCategory)

        all_files: list[FileInventoryEntry] = []
        errors: list[str] = []

        for year_idx, year in enumerate(years, 1):
            self.log_writer.info(
                f"Processing ACOMS year {year} ({year_idx}/{len(years)})",
                year=year,
            )
            for category in categories:
                try:
                    if file_type_codes:
                        for file_type_code in file_type_codes:
                            entries = self.discover_year(
                                aco_id=aco_id,
                                year=year,
                                category=category,
                                file_type_code=file_type_code,
                            )
                            all_files.extend(entries)
                    else:
                        all_files.extend(
                            self.discover_year(
                                aco_id=aco_id,
                                year=year,
                                category=category,
                            )
                        )
                except Exception as e:
                    message = f"Error discovering ACOMS {category.value} for year {year}: {e}"
                    self.log_writer.error(message, year=year, category=category.value)
                    errors.append(message)

        files_by_year: dict[int, int] = defaultdict(int)
        files_by_category: dict[str, int] = defaultdict(int)
        for file_entry in all_files:
            files_by_year[file_entry.year] += 1
            files_by_category[file_entry.category] += 1

        result = InventoryResult(
            aco_id=aco_id,
            categories=[category.value for category in categories],
            years=years,
            total_files=len(all_files),
            files_by_year=dict(files_by_year),
            files_by_category=dict(files_by_category),
            files=all_files,
            started_at=started_at,
            completed_at=datetime.now(),
            errors=errors if errors else None,
        )

        if result.total_files == 0:
            self.log_writer.warning(
                "ACOMS inventory returned 0 files. Check ACOMS_API_ID and credentials.",
                aco_id=aco_id,
                categories=[category.value for category in categories],
            )

        return result

    def enrich_with_file_type_codes(self, result: InventoryResult) -> InventoryResult:
        """
        Populate file_type_code by querying each advertised ACOMS file type.

        ACOMS category-level ``--view`` output does not include a type code, but
        precise download orchestration needs it. This method only queries
        year/category pairs that already returned files.
        """
        matched_count = 0
        schema_patterns = _load_schema_acoms_patterns()
        for file_entry in result.files:
            if file_entry.file_type_code is None:
                matched = _match_schema_acoms_file_type_code(
                    file_entry.filename,
                    file_entry.category,
                    schema_patterns,
                )
                if matched is not None:
                    file_entry.file_type_code = matched
                    matched_count += 1

        for file_entry in result.files:
            if file_entry.file_type_code is None:
                inferred = infer_file_type_code(file_entry.filename, file_entry.category)
                if inferred is not None:
                    file_entry.file_type_code = inferred
                    matched_count += 1

        pairs = sorted(
            {
                (file_entry.year, file_entry.category)
                for file_entry in result.files
                if file_entry.file_type_code is None
            }
        )
        filename_to_code: dict[tuple[int, str, str], int] = {}
        original_names_by_pair: dict[tuple[int, str], set[str]] = defaultdict(set)
        for file_entry in result.files:
            original_names_by_pair[(file_entry.year, file_entry.category)].add(file_entry.filename)

        catalog_by_category: dict[str, list[FileTypeDefinition]] = defaultdict(list)
        if pairs:
            catalog = self._load_file_type_catalog()
            for definition in catalog:
                try:
                    category = normalize_category(definition.category).value
                except ValueError:
                    category = definition.category
                catalog_by_category[category].append(definition)

        for year, category_name in pairs:
            try:
                category = normalize_category(category_name)
            except ValueError:
                continue

            definitions = catalog_by_category.get(category.value, [])
            unknown_files = [
                file_entry
                for file_entry in result.files
                if file_entry.year == year
                and file_entry.category == category.value
                and file_entry.file_type_code is None
            ]

            if len(definitions) == 1:
                for file_entry in unknown_files:
                    file_entry.file_type_code = definitions[0].code
                    matched_count += 1
                continue

            original_names = original_names_by_pair[(year, category.value)]
            for definition in definitions:
                try:
                    entries = self.discover_year(
                        aco_id=result.aco_id,
                        year=year,
                        category=category,
                        file_type_code=definition.code,
                    )
                except Exception as e:
                    self.log_writer.warning(
                        "Could not enrich ACOMS file type code",
                        year=year,
                        category=category.value,
                        file_type_code=definition.code,
                        error=str(e),
                    )
                    continue

                entry_names = {entry.filename for entry in entries}
                if not entry_names:
                    continue

                # ACOMS currently accepts --file on --view but may return the
                # full category. Do not let an unfiltered response relabel
                # every file as the probed code.
                if entry_names >= original_names:
                    continue

                for entry in entries:
                    filename_to_code[(year, category.value, entry.filename)] = definition.code

        for file_entry in result.files:
            key = (file_entry.year, file_entry.category, file_entry.filename)
            matched_code = filename_to_code.get(key)
            if matched_code is not None:
                file_entry.file_type_code = matched_code
                matched_count += 1

        unknown_count = sum(1 for file_entry in result.files if file_entry.file_type_code is None)
        if unknown_count:
            self.log_writer.warning(
                f"{unknown_count} ACOMS files still have unknown file type codes",
                unknown_count=unknown_count,
            )

        self.log_writer.info(
            f"Matched {matched_count} ACOMS files to file type codes",
            matched_count=matched_count,
            total_files=result.total_files,
        )
        return result

    def get_summary(self, result: InventoryResult) -> dict[str, Any]:
        """Generate summary statistics for an inventory result."""
        return {
            "aco_id": result.aco_id,
            "total_files": result.total_files,
            "years_scanned": len(result.years),
            "year_range": f"{min(result.years)}-{max(result.years)}" if result.years else "n/a",
            "categories_scanned": len(result.categories),
            "files_by_year": result.files_by_year,
            "files_by_category": result.files_by_category,
            "duration_seconds": result.duration_seconds,
            "errors": result.errors,
        }
