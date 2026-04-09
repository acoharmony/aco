# © 2025 HarmonyCares
# All rights reserved.

"""
PUF download state tracking and inventory management.

This module provides state tracking for PUF files, similar to the 4icli inventory
pattern. It tracks:
- Which files have been downloaded
- Which files need downloading
- File metadata and download timestamps
- Integration with _cite state tracking

"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from . import pfs_inventory
from .models import DownloadTask, FileCategory, RuleType


def scan_directory(directory: Path, label: str = "directory", recursive: bool = False) -> set[str]:
    """
    Scan directory for existing files.

    Args:
        directory: Path to directory
        label: Label for logging
        recursive: If True, scan subdirectories recursively (default: False)

    Returns:
        Set of filenames in directory
    """
    files = set()

    if not directory.exists():
        return files

    # Scan for files (recursive or non-recursive based on parameter)
    if recursive:
        # Recursively scan all subdirectories
        for file_path in directory.rglob("*"):
            if file_path.is_file():
                files.add(file_path.name)
    else:
        # Only scan top-level files (non-recursive)
        for file_path in directory.glob("*"):
            if file_path.is_file():
                files.add(file_path.name)

    return files


def get_workspace_path() -> Path:
    """Get workspace path."""
    return Path("/opt/s3/data/workspace")


@dataclass
class PUFFileEntry:
    """Tracking entry for a single PUF file."""

    year: str
    rule_type: str
    file_key: str
    url: str
    category: str
    dataset_key: str = "pfs"  # Dataset identifier (pfs, rvu, zipcarrier)
    metadata: dict[str, Any] = field(default_factory=dict)  # Additional metadata (e.g., quarter)
    schema_mapping: str | None = None
    downloaded: bool = False
    download_timestamp: str | None = None
    corpus_path: str | None = None
    file_size_bytes: int | None = None
    error_message: str | None = None
    # Extraction tracking
    extracted: bool = False
    extraction_timestamp: str | None = None
    extracted_files: list[str] = field(default_factory=list)
    # File location tracking
    zip_filename: str | None = None  # Expected ZIP filename
    found_in_archive: bool = False
    found_in_bronze: bool = False
    found_in_cite_corpus: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PUFFileEntry:
        """Create from dictionary."""
        return cls(**data)

    @classmethod
    def from_download_task(cls, task: DownloadTask, downloaded: bool = False, dataset_key: str = "pfs") -> PUFFileEntry:
        """Create from DownloadTask."""
        # Convert enums to strings if needed
        rule_type_str = task.rule_type.value if isinstance(task.rule_type, RuleType) else task.rule_type
        category_str = (
            task.file_metadata.category.value
            if isinstance(task.file_metadata.category, FileCategory)
            else task.file_metadata.category
        )

        # Extract expected ZIP filename from URL
        # Handle URLs with slashes in filename (e.g., rvu25a-updated-01/10/2025.zip)
        url_str = str(task.file_metadata.url)
        if url_str:
            # Get the part after /files/zip/
            if "/files/zip/" in url_str:
                after_zip = url_str.split("/files/zip/")[1]
                # Replace slashes with hyphens to get actual filename
                zip_filename = after_zip.replace("/", "-")
            else:
                zip_filename = url_str.split("/")[-1]
        else:
            zip_filename = None

        return cls(
            year=task.year,
            rule_type=rule_type_str,
            file_key=task.file_metadata.key,
            url=str(task.file_metadata.url),
            category=category_str,
            dataset_key=dataset_key,
            metadata=dict(task.file_metadata.metadata) if task.file_metadata.metadata else {},
            schema_mapping=task.file_metadata.schema_mapping,
            downloaded=downloaded,
            zip_filename=zip_filename,
        )


@dataclass
class PUFInventoryState:
    """Complete inventory state for PUF downloads."""

    dataset_name: str = "Medicare Physician Fee Schedule"
    dataset_key: str = "pfs"
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    total_files: int = 0
    downloaded_files: int = 0
    pending_files: int = 0
    failed_files: int = 0
    files: dict[str, PUFFileEntry] = field(default_factory=dict)

    def __post_init__(self):
        """Convert dict entries to PUFFileEntry objects if needed."""
        # Handle loading from JSON where files are dicts
        converted_files = {}
        for key, file_entry in self.files.items():
            if isinstance(file_entry, dict):
                converted_files[key] = PUFFileEntry.from_dict(file_entry)
            else:
                converted_files[key] = file_entry
        self.files = converted_files

    def _make_file_key(self, year: str, rule_type: str, file_key: str) -> str:
        """Create unique key for file entry."""
        return f"{year}:{rule_type}:{file_key}"

    def add_file(self, entry: PUFFileEntry) -> None:
        """Add or update file entry."""
        key = self._make_file_key(entry.year, entry.rule_type, entry.file_key)
        self.files[key] = entry
        self._update_stats()

    def get_file(self, year: str, rule_type: str, file_key: str) -> PUFFileEntry | None:
        """Get file entry."""
        key = self._make_file_key(year, rule_type, file_key)
        return self.files.get(key)

    def mark_downloaded(
        self,
        year: str,
        rule_type: str,
        file_key: str,
        corpus_path: str | None = None,
        file_size_bytes: int | None = None,
    ) -> None:
        """Mark file as downloaded."""
        entry = self.get_file(year, rule_type, file_key)
        if entry:
            entry.downloaded = True
            entry.download_timestamp = datetime.now().isoformat()
            entry.corpus_path = corpus_path
            entry.file_size_bytes = file_size_bytes
            entry.error_message = None
            self._update_stats()

    def mark_failed(self, year: str, rule_type: str, file_key: str, error_message: str) -> None:
        """Mark file download as failed."""
        entry = self.get_file(year, rule_type, file_key)
        if entry:
            entry.error_message = error_message
            self._update_stats()

    def is_downloaded(self, year: str, rule_type: str, file_key: str) -> bool:
        """Check if file is downloaded."""
        entry = self.get_file(year, rule_type, file_key)
        return entry.downloaded if entry else False

    def _update_stats(self) -> None:
        """Update statistics."""
        self.total_files = len(self.files)
        self.downloaded_files = sum(1 for f in self.files.values() if f.downloaded)
        self.failed_files = sum(1 for f in self.files.values() if f.error_message)
        self.pending_files = self.total_files - self.downloaded_files

    def get_downloaded(self) -> list[PUFFileEntry]:
        """Get all downloaded files."""
        return [f for f in self.files.values() if f.downloaded]

    def get_pending(self) -> list[PUFFileEntry]:
        """Get all pending files."""
        return [f for f in self.files.values() if not f.downloaded and not f.error_message]

    def get_failed(self) -> list[PUFFileEntry]:
        """Get all failed files."""
        return [f for f in self.files.values() if f.error_message]

    def get_by_year(self, year: str) -> list[PUFFileEntry]:
        """Get all files for a specific year."""
        return [f for f in self.files.values() if f.year == year]

    def get_by_category(self, category: str | FileCategory) -> list[PUFFileEntry]:
        """Get all files for a specific category."""
        if isinstance(category, FileCategory):
            category = category.value
        return [f for f in self.files.values() if f.category == category]

    def get_by_schema(self, schema_name: str) -> list[PUFFileEntry]:
        """Get all files that map to a specific schema."""
        results = []
        for file_entry in self.files.values():
            if not file_entry.schema_mapping:
                continue
            schemas = [s.strip() for s in file_entry.schema_mapping.split(",")]
            if schema_name in schemas:
                results.append(file_entry)
        return results

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "dataset_name": self.dataset_name,
            "dataset_key": self.dataset_key,
            "last_updated": self.last_updated,
            "total_files": self.total_files,
            "downloaded_files": self.downloaded_files,
            "pending_files": self.pending_files,
            "failed_files": self.failed_files,
            "files": {key: entry.to_dict() for key, entry in self.files.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PUFInventoryState:
        """Create from dictionary."""
        return cls(**data)

    def get_summary(self) -> dict[str, Any]:
        """Get summary statistics."""
        return {
            "dataset_name": self.dataset_name,
            "dataset_key": self.dataset_key,
            "last_updated": self.last_updated,
            "total_files": self.total_files,
            "downloaded_files": self.downloaded_files,
            "pending_files": self.pending_files,
            "failed_files": self.failed_files,
            "download_percentage": (
                (self.downloaded_files / self.total_files * 100) if self.total_files > 0 else 0
            ),
        }


class PUFStateTracker:
    """State tracker for PUF file downloads."""

    def __init__(self, state: PUFInventoryState | None = None):
        """Initialize tracker."""
        self.state = state or PUFInventoryState()

    @classmethod
    def get_state_path(cls) -> Path:
        """Get path to state file."""
        # Use same pattern as 4icli inventory
        workspace = Path("/opt/s3/data/workspace")
        state_dir = workspace / "logs" / "tracking"
        state_dir.mkdir(parents=True, exist_ok=True)
        return state_dir / "puf_inventory_state.json"

    @classmethod
    def load(cls) -> PUFStateTracker:
        """Load state from JSON file."""
        state_path = cls.get_state_path()

        if not state_path.exists():
            return cls()

        with open(state_path) as f:
            data = json.load(f)

        state = PUFInventoryState.from_dict(data)
        return cls(state)

    def save(self) -> None:
        """Save state to JSON file."""
        self.state.last_updated = datetime.now().isoformat()
        self.state._update_stats()

        state_path = self.get_state_path()
        state_path.parent.mkdir(parents=True, exist_ok=True)

        with open(state_path, "w") as f:
            json.dump(self.state.to_dict(), f, indent=2)

    def sync_with_inventory(
        self,
        year: str | None = None,
        rule_type: str | RuleType | None = None,
        force_refresh: bool = False,
        dataset_key: str = "pfs",
    ) -> int:
        """
        Sync state with current inventory.

        Args:
            year: Optional year filter
            rule_type: Optional rule type filter
            force_refresh: If True, re-add all files even if already in state
            dataset_key: Dataset to sync with ('pfs', 'rvu', 'zipcarrier')

        Returns:
            Number of new files added
        """
        # Get all files from inventory based on dataset
        if dataset_key == "pfs":
            tasks = pfs_inventory.create_download_tasks(year=year, rule_type=rule_type)
        else:
            # Use generic puf_inventory for rvu and zipcarrier
            from . import puf_inventory as generic_puf_inventory
            tasks = generic_puf_inventory.create_download_tasks(
                dataset_key=dataset_key,
                year=year,
                rule_type=rule_type,
            )

        new_count = 0
        for task in tasks:
            # Convert rule_type to string if it's an enum
            rule_type_str = task.rule_type.value if isinstance(task.rule_type, RuleType) else task.rule_type
            file_key = self.state._make_file_key(task.year, rule_type_str, task.file_metadata.key)

            # Skip if already in state and not forcing refresh
            if file_key in self.state.files and not force_refresh:
                continue

            # Add new entry
            entry = PUFFileEntry.from_download_task(task, dataset_key=dataset_key)
            self.state.add_file(entry)
            new_count += 1

        self.state._update_stats()
        return new_count

    def get_needed_downloads(
        self,
        year: str | None = None,
        rule_type: str | RuleType | None = None,
        category: str | FileCategory | None = None,
        schema_name: str | None = None,
    ) -> list[DownloadTask]:
        """
        Get download tasks for files that need downloading.

        Args:
            year: Optional year filter
            rule_type: Optional rule type filter
            category: Optional category filter
            schema_name: Optional schema mapping filter

        Returns:
            List of DownloadTask objects for pending files
        """
        pending = self.state.get_pending()

        # Apply filters
        if year:
            pending = [f for f in pending if f.year == year]

        if rule_type:
            rule_type_str = rule_type.value if isinstance(rule_type, RuleType) else rule_type
            pending = [f for f in pending if f.rule_type == rule_type_str]

        if category:
            category_str = category.value if isinstance(category, FileCategory) else category
            pending = [f for f in pending if f.category == category_str]

        if schema_name:
            filtered = []
            for file_entry in pending:
                if not file_entry.schema_mapping:
                    continue
                schemas = [s.strip() for s in file_entry.schema_mapping.split(",")]
                if schema_name in schemas:
                    filtered.append(file_entry)
            pending = filtered

        # Convert to download tasks
        tasks = []
        for entry in pending:
            # Get file metadata from inventory
            file_meta = None
            year_inv = pfs_inventory.get_year(entry.year)
            if year_inv:
                rule = year_inv.get_rule(entry.rule_type)
                if rule:
                    file_meta = rule.files.get(entry.file_key)

            if file_meta:
                task = DownloadTask(
                    file_metadata=file_meta,
                    year=entry.year,
                    rule_type=RuleType(entry.rule_type),
                    priority=5,
                    force_refresh=False,
                    tags=["puf", "cms", entry.year, entry.rule_type.lower(), entry.category],
                )
                tasks.append(task)

        return tasks

    def mark_downloaded(
        self,
        year: str,
        rule_type: str,
        file_key: str,
        corpus_path: str | None = None,
        file_size_bytes: int | None = None,
    ) -> None:
        """Mark file as downloaded and save state."""
        self.state.mark_downloaded(year, rule_type, file_key, corpus_path, file_size_bytes)
        self.save()

    def mark_failed(self, year: str, rule_type: str, file_key: str, error_message: str) -> None:
        """Mark file download as failed and save state."""
        self.state.mark_failed(year, rule_type, file_key, error_message)
        self.save()

    def is_downloaded(self, year: str, rule_type: str, file_key: str) -> bool:
        """Check if file is downloaded."""
        return self.state.is_downloaded(year, rule_type, file_key)

    def get_summary(self) -> dict[str, Any]:
        """Get summary statistics."""
        return self.state.get_summary()

    def scan_filesystem(self, verbose: bool = False) -> dict[str, int]:
        """
        Scan filesystem to update download and extraction status.

        Checks:
        - Archive directory for downloaded ZIPs
        - Bronze directory for downloaded ZIPs
        - Bronze/pufs directory for extracted files
        - _cite corpus for tracked downloads

        Args:
            verbose: If True, print detailed scan results

        Returns:
            Dictionary with scan statistics
        """
        workspace = get_workspace_path()
        bronze_dir = workspace / "bronze"
        archive_dir = workspace / "archive"
        pufs_dir = bronze_dir / "pufs"
        cite_corpus = workspace / "cites" / "corpus"

        # Scan directories
        archive_files = scan_directory(archive_dir, "archive", recursive=True)
        bronze_files = scan_directory(bronze_dir, "bronze")
        pufs_files = scan_directory(pufs_dir, "pufs", recursive=True) if pufs_dir.exists() else set()
        cite_files = scan_directory(cite_corpus, "cite_corpus", recursive=True) if cite_corpus.exists() else set()

        if verbose:
            print(f"Archive files: {len(archive_files)}")
            print(f"Bronze files: {len(bronze_files)}")
            print(f"PUFs extracted files: {len(pufs_files)}")
            print(f"Cite corpus files: {len(cite_files)}")

        stats = {
            "scanned": len(self.state.files),
            "found_in_archive": 0,
            "found_in_bronze": 0,
            "found_in_cite": 0,
            "marked_downloaded": 0,
            "marked_extracted": 0,
        }

        # Update state for each tracked file
        for entry in self.state.files.values():

            # Check if ZIP is in archive
            if entry.zip_filename and entry.zip_filename in archive_files:
                entry.found_in_archive = True
                stats["found_in_archive"] += 1
                if not entry.downloaded:
                    entry.downloaded = True
                    entry.download_timestamp = datetime.now().isoformat()
                    stats["marked_downloaded"] += 1

            # Check if ZIP is in bronze
            if entry.zip_filename and entry.zip_filename in bronze_files:
                entry.found_in_bronze = True
                stats["found_in_bronze"] += 1
                if not entry.downloaded:
                    entry.downloaded = True
                    entry.download_timestamp = datetime.now().isoformat()
                    stats["marked_downloaded"] += 1

            # Check if file is in cite corpus (by looking for similar filenames)
            if entry.zip_filename:
                # Check for exact match or similar matches in cite corpus
                base_name = entry.zip_filename.replace(".zip", "")
                matching_cite_files = [f for f in cite_files if base_name in f or entry.file_key in f]
                if matching_cite_files:
                    entry.found_in_cite_corpus = True
                    stats["found_in_cite"] += 1
                    if not entry.downloaded:
                        entry.downloaded = True
                        entry.download_timestamp = datetime.now().isoformat()
                        stats["marked_downloaded"] += 1

            # Check for extracted files in pufs directory
            # Look for files that might have been extracted from this ZIP
            if entry.zip_filename and pufs_files:
                # Check if any extracted files match this PUF's pattern
                base_name = entry.zip_filename.replace(".zip", "").replace("-", "_").lower()
                matching_extracted = [f for f in pufs_files if base_name in f.lower() or entry.file_key in f.lower()]
                if matching_extracted:
                    if not entry.extracted:
                        entry.extracted = True
                        entry.extraction_timestamp = datetime.now().isoformat()
                        stats["marked_extracted"] += 1
                    entry.extracted_files = list(matching_extracted)

        # Update statistics
        self.state._update_stats()

        if verbose:
            print("\nScan results:")
            print(f"  Files scanned: {stats['scanned']}")
            print(f"  Found in archive: {stats['found_in_archive']}")
            print(f"  Found in bronze: {stats['found_in_bronze']}")
            print(f"  Found in cite corpus: {stats['found_in_cite']}")
            print(f"  Marked as downloaded: {stats['marked_downloaded']}")
            print(f"  Marked as extracted: {stats['marked_extracted']}")

        return stats
