#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Compare inventory to local bronze/archive directories.

 functionality to:
- Scan bronze and archive directories for existing files
- Compare DataHub inventory to local files
- Identify files available in DataHub but not downloaded
- Generate tracking state for missing files
"""

import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from .config import FourICLIConfig
from .inventory import InventoryResult


def scan_directory(directory: Path, label: str = "directory", recursive: bool = False) -> set[str]:
    """
    Scan directory for existing files.

        Args:
            directory: Path to directory
            label: Label for logging
            recursive: If True, scan subdirectories recursively (default: False)

        Returns:
            Set of filenames in directory

        Note:
            Both bronze and archive use flat file structures (no subdirectories):
            - Bronze: Downloaded files + extracted files (flat)
            - Archive: ZIP files moved after extraction (flat)

            Use recursive=False (default) to:
            1. Match 4icli download detection behavior (glob("*"))
            2. Avoid collision with unrelated subdirectories (e.g., tuva_seeds/)
            3. Match the actual file structure created by the unpack process
    """
    files = set()

    if not directory.exists():
        print(f"Warning: {label} does not exist: {directory}")
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


def scan_all_storage_locations(config: FourICLIConfig) -> set[str]:
    """
    Scan all storage locations for existing files.

        Locations scanned (in order):
        - Bronze directory (downloads + extracted files)
        - Archive directory (processed ZIPs)

        Args:
            config: FourICLI configuration

        Returns:
            Unified set of all filenames found across all storage locations
    """
    all_files = set()

    # Scan bronze directory
    bronze_dir = config.data_path / "bronze"
    bronze_files = scan_directory(bronze_dir, "bronze")
    all_files.update(bronze_files)

    # Scan archive directory
    archive_dir = config.data_path / "archive"
    archive_files = scan_directory(archive_dir, "archive")
    all_files.update(archive_files)

    return all_files


def compare_inventory(
    inventory: InventoryResult,
    local_files: set[str],
    year_filter: int | None = None,
    category_filter: str | None = None,
    state_tracker=None,
) -> dict:
    """
    Compare inventory to local files with hash-aware duplicate detection.

        Args:
            inventory: Inventory result from DataHub
            local_files: Set of filenames in local directories
            year_filter: Optional year filter
            category_filter: Optional category filter
            state_tracker: Optional FourICLIStateTracker for hash-based deduplication

        Returns:
            Dictionary with comparison results
    """
    # Filter inventory
    filtered_files = inventory.files
    if year_filter:
        filtered_files = [f for f in filtered_files if f.year == year_filter]
    if category_filter:
        filtered_files = [f for f in filtered_files if f.category == category_filter]

    # Get hash-tracked files if state tracker provided
    hash_tracked_files = set()
    if state_tracker:
        hash_tracked_files = {state.filename for state in state_tracker._file_cache.values()}

    # Union of local files + hash-tracked files
    all_known_files = local_files | hash_tracked_files

    # Separate into have and missing
    have = []
    missing = []

    for file_entry in filtered_files:
        if file_entry.filename in all_known_files:
            have.append(file_entry)
        else:
            missing.append(file_entry)

    # Calculate statistics
    total_size = sum(f.size_bytes for f in missing if f.size_bytes is not None)
    missing_by_year = Counter(f.year for f in missing)
    missing_by_category = Counter(f.category for f in missing)
    missing_by_type_code = Counter(
        f.file_type_code for f in missing if f.file_type_code is not None
    )

    return {
        "total_inventory": len(filtered_files),
        "have": have,
        "missing": missing,
        "have_count": len(have),
        "missing_count": len(missing),
        "total_size_bytes": total_size,
        "missing_by_year": dict(missing_by_year),
        "missing_by_category": dict(missing_by_category),
        "missing_by_type_code": dict(missing_by_type_code),
    }


def format_size(size_bytes: int | None) -> str:
    """Format size in human-readable format."""
    if size_bytes is None:
        return "N/A"

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def save_not_downloaded_state(missing_files: list, output_path: Path) -> None:
    """
    Save not-downloaded state to JSON tracking file.

        Args:
            missing_files: List of FileInventoryEntry objects for missing files
            output_path: Path to save state file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build statistics
    missing_by_year = Counter(f.year for f in missing_files)
    missing_by_category = Counter(f.category for f in missing_files)
    missing_by_type_code = Counter(
        f.file_type_code for f in missing_files if f.file_type_code is not None
    )

    total_size = sum(f.size_bytes for f in missing_files if f.size_bytes is not None)

    state = {
        "generated_at": datetime.now().isoformat(),
        "total_missing": len(missing_files),
        "total_size_bytes": total_size,
        "total_size_formatted": format_size(total_size),
        "missing_by_year": dict(missing_by_year),
        "missing_by_category": dict(missing_by_category),
        "missing_by_type_code": dict(missing_by_type_code),
        "files": [f.to_dict() for f in missing_files],
    }

    with open(output_path, "w") as f:
        json.dump(state, f, indent=2)


def export_to_csv(missing_files: list, output_path: Path) -> None:
    """Export missing files to CSV."""
    import csv

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="") as csvfile:
        fieldnames = [
            "filename",
            "category",
            "file_type_code",
            "year",
            "size_bytes",
            "size_formatted",
            "last_updated",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for file_entry in missing_files:
            writer.writerow(
                {
                    "filename": file_entry.filename,
                    "category": file_entry.category,
                    "file_type_code": file_entry.file_type_code or "",
                    "year": file_entry.year,
                    "size_bytes": file_entry.size_bytes or "",
                    "size_formatted": format_size(file_entry.size_bytes),
                    "last_updated": file_entry.last_updated or "",
                }
            )
