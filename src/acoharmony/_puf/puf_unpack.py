# © 2025 HarmonyCares
# All rights reserved.

"""
PUF ZIP file unpacking utility with metadata-aware file renaming.

Extracts PUF ZIP files and renames extracted files to include metadata
about their source (year, rule type, category) for easy parsing and tracking.

Naming convention:
    pfs_{year}_{rule_type}_{category}_{original_filename}

This makes it easy to:
- Know which ZIP a file came from
- Parse file metadata from filename
- Track file provenance

"""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path
from typing import Any

from .puf_state import PUFStateTracker, get_workspace_path


def get_puf_directories() -> tuple[Path, Path, Path, Path]:
    """
    Get PUF-related directories.

    Returns:
        Tuple of (bronze_dir, archive_dir, pufs_dir, cite_corpus_dir)
    """
    workspace = get_workspace_path()
    bronze_dir = workspace / "bronze"
    archive_dir = workspace / "archive"
    pufs_dir = bronze_dir / "pufs"
    cite_corpus = workspace / "cites" / "corpus"

    return bronze_dir, archive_dir, pufs_dir, cite_corpus


def make_puf_filename(
    dataset_key: str,
    year: str,
    rule_type: str,
    category: str,
    original_filename: str,
    metadata: dict[str, Any] | None = None,
) -> str:
    """
    Create metadata-enriched filename for extracted PUF file.

    Args:
        dataset_key: Dataset identifier (e.g., "pfs", "rvu", "zipcarrier")
        year: Year (e.g., "2024")
        rule_type: Rule type (e.g., "Final", "Proposed")
        category: File category (e.g., "addenda", "gpci", "rvu_quarterly")
        original_filename: Original filename from ZIP
        metadata: Optional metadata dict (e.g., {"quarter": "A"} for RVU)

    Returns:
        New filename with metadata prefix

    """
    metadata = metadata or {}

    # Normalize category
    category_slug = category.lower().replace(" ", "_")

    # Build filename based on dataset type
    if dataset_key == "rvu":
        # RVU: Include quarter instead of rule_type
        quarter = metadata.get("quarter", "")
        if quarter:
            quarter_map = {"A": "q1", "B": "q2", "C": "q3", "D": "q4"}
            quarter_slug = quarter_map.get(quarter.upper(), f"q{quarter.lower()}")
            return f"rvu_{year}_{quarter_slug}_{category_slug}_{original_filename}"
        else:
            # Fallback if no quarter
            return f"rvu_{year}_{category_slug}_{original_filename}"

    elif dataset_key == "zipcarrier":
        # ZipCarrier: Simple format without rule_type
        return f"zipcarrier_{year}_{category_slug}_{original_filename}"

    else:
        # PFS (default): Include rule_type
        rule_type_slug = rule_type.lower().replace(" ", "_")
        return f"{dataset_key}_{year}_{rule_type_slug}_{category_slug}_{original_filename}"


def extract_puf_zip(
    zip_path: Path,
    dest_dir: Path,
    dataset_key: str,
    year: str,
    rule_type: str,
    category: str,
    file_key: str,
    metadata: dict[str, Any] | None = None,
    dry_run: bool = False,
) -> list[tuple[str, str]]:
    """
    Extract PUF ZIP file with metadata-aware renaming.

    Args:
        zip_path: Path to ZIP file
        dest_dir: Destination directory
        dataset_key: Dataset identifier (e.g., "pfs", "rvu", "zipcarrier")
        year: Year metadata
        rule_type: Rule type metadata
        category: Category metadata
        file_key: File key from inventory
        metadata: Optional metadata dict (e.g., {"quarter": "A"} for RVU)
        dry_run: If True, only simulate extraction

    Returns:
        List of tuples: (original_filename, renamed_filename)

    Raises:
        zipfile.BadZipFile: If ZIP is corrupted
        Exception: For other extraction errors
    """
    extracted_files = []

    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")

    # Ensure destination exists
    if not dry_run:
        dest_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Get list of files (excluding directories)
        members = [m for m in zf.namelist() if not m.endswith("/")]

        for member in members:
            # Get just the filename without directory structure
            original_filename = Path(member).name

            # Create metadata-enriched filename
            renamed_filename = make_puf_filename(
                dataset_key, year, rule_type, category, original_filename, metadata
            )

            if dry_run:
                extracted_files.append((original_filename, renamed_filename))
                continue

            # Extract to target path
            source = zf.open(member)
            target = dest_dir / renamed_filename

            # Check if file already exists
            if target.exists():
                # File exists, skip
                extracted_files.append((original_filename, renamed_filename))
                source.close()
                continue

            # Write file
            try:
                with open(target, "wb") as f:
                    shutil.copyfileobj(source, f)

                # Check if extracted file is empty (0 bytes)
                if target.stat().st_size == 0:
                    target.unlink()
                else:
                    extracted_files.append((original_filename, renamed_filename))
            finally:
                source.close()

    return extracted_files


def unpack_puf_zips(
    year: str | None = None,
    rule_type: str | None = None,
    category: str | None = None,
    dry_run: bool = False,
    verbose: bool = True,
) -> dict[str, Any]:
    """
    Unpack PUF ZIP files with metadata-aware renaming.

    This function:
    1. Loads PUF state tracker to find downloaded ZIPs
    2. Scans bronze and archive directories for PUF ZIP files
    3. Extracts each ZIP to bronze/pufs with renamed files
    4. Updates state tracker with extraction status
    5. Returns statistics

    Args:
        year: Optional year filter
        rule_type: Optional rule type filter
        category: Optional category filter
        dry_run: If True, simulate without extracting
        verbose: If True, print progress messages

    Returns:
        Dictionary with statistics:
            - found: Number of ZIPs found
            - processed: Number successfully extracted
            - failed: Number that failed
            - extracted: Total files extracted
            - skipped_already_extracted: Number already extracted

    """
    bronze_dir, archive_dir, pufs_dir, cite_corpus = get_puf_directories()

    # Ensure pufs directory exists
    if not dry_run:
        pufs_dir.mkdir(parents=True, exist_ok=True)

    # Load state tracker
    tracker = PUFStateTracker.load()

    # Scan filesystem to find ZIPs
    if verbose:
        print("Scanning for PUF ZIP files...")
    tracker.scan_filesystem(verbose=False)

    stats = {
        "found": 0,
        "processed": 0,
        "failed": 0,
        "extracted": 0,
        "skipped_already_extracted": 0,
        "files_by_category": {},
    }

    # Filter tracked files
    files_to_process = []
    for entry in tracker.state.files.values():
        # Apply filters
        if year and entry.year != year:
            continue
        if rule_type and entry.rule_type != rule_type:
            continue
        if category and entry.category != category:
            continue

        # Only process downloaded files
        if not entry.downloaded:
            continue

        # Skip if already extracted (unless forcing)
        if entry.extracted and not dry_run:
            stats["skipped_already_extracted"] += 1
            continue

        files_to_process.append(entry)

    stats["found"] = len(files_to_process)

    if verbose:
        print(f"Found {stats['found']} PUF ZIPs to process")
        if stats["skipped_already_extracted"] > 0:
            print(f"Skipping {stats['skipped_already_extracted']} already extracted")
        print()

    # Process each file
    for idx, entry in enumerate(files_to_process, 1):
        if verbose:
            print(f"[{idx}/{len(files_to_process)}] {entry.zip_filename}")
            print(f"  Year: {entry.year}, Rule: {entry.rule_type}, Category: {entry.category}")

        # Find ZIP file location
        zip_path = None

        # Check archive with year/rule_type or year/dataset_key subdirectories
        # For RVU/zipcarrier: archive/{year}/{dataset_key}/
        # For PFS: archive/{year}/{rule_type}/
        if entry.dataset_key in ("rvu", "zipcarrier"):
            archive_subdir_path = archive_dir / entry.year / entry.dataset_key / entry.zip_filename
        else:
            rule_type_slug = entry.rule_type.lower().replace(" ", "_")
            archive_subdir_path = archive_dir / entry.year / rule_type_slug / entry.zip_filename
        if archive_subdir_path.exists():
            zip_path = archive_subdir_path
        elif entry.found_in_archive:
            # Fallback: check flat archive structure
            zip_path = archive_dir / entry.zip_filename
        elif entry.found_in_bronze:
            zip_path = bronze_dir / entry.zip_filename
        elif entry.corpus_path:
            # Try corpus path
            corpus_file = Path(entry.corpus_path)
            if corpus_file.exists():
                zip_path = corpus_file

        if not zip_path or not zip_path.exists():
            if verbose:
                print(f"  [ERROR] ZIP file not found: {entry.zip_filename}")
            stats["failed"] += 1
            continue

        try:
            # Get dataset_key and metadata from entry
            dataset_key = getattr(entry, "dataset_key", "pfs")  # Default to pfs for backward compat
            metadata = getattr(entry, "metadata", {})

            # Extract ZIP
            if dry_run:
                if verbose:
                    print(f"  [DRY RUN] Would extract to: {pufs_dir}")
                extracted = extract_puf_zip(
                    zip_path,
                    pufs_dir,
                    dataset_key,
                    entry.year,
                    entry.rule_type,
                    entry.category,
                    entry.file_key,
                    metadata=metadata,
                    dry_run=True,
                )
                if verbose:
                    print(f"  [DRY RUN] Would extract {len(extracted)} files:")
                    for orig, renamed in extracted[:5]:
                        print(f"    {orig} → {renamed}")
                    if len(extracted) > 5:
                        print(f"    ... and {len(extracted) - 5} more")
                stats["processed"] += 1
                stats["extracted"] += len(extracted)
            else:
                extracted = extract_puf_zip(
                    zip_path,
                    pufs_dir,
                    dataset_key,
                    entry.year,
                    entry.rule_type,
                    entry.category,
                    entry.file_key,
                    metadata=metadata,
                    dry_run=False,
                )

                # Update state tracker
                entry.extracted = True
                from datetime import datetime

                entry.extraction_timestamp = datetime.now().isoformat()
                entry.extracted_files = [renamed for _, renamed in extracted]

                stats["processed"] += 1
                stats["extracted"] += len(extracted)
                stats["files_by_category"][entry.category] = (
                    stats["files_by_category"].get(entry.category, 0) + len(extracted)
                )

                if verbose:
                    print(f"  [OK] Extracted {len(extracted)} files")

        except zipfile.BadZipFile as e:
            if verbose:
                print(f"  [ERROR] Invalid ZIP file: {e}")
            stats["failed"] += 1
            entry.error_message = f"Invalid ZIP: {e}"

        except Exception as e:  # ALLOWED: Continue processing remaining files
            if verbose:
                print(f"  [ERROR] Failed: {e}")
            stats["failed"] += 1
            entry.error_message = f"Extraction failed: {e}"

        if verbose:
            print()

    # Save updated state
    if not dry_run:
        tracker.save()

    # Summary
    if verbose:
        print("=" * 80)
        print("Unpack Summary")
        print("=" * 80)
        print(f"Found:      {stats['found']} ZIPs")
        print(f"Processed:  {stats['processed']} successfully")
        print(f"Failed:     {stats['failed']}")
        print(f"Extracted:  {stats['extracted']} total files")
        print(f"Skipped:    {stats['skipped_already_extracted']} already extracted")

        if stats["files_by_category"]:
            print()
            print("Files extracted by category:")
            for cat, count in sorted(stats["files_by_category"].items()):
                print(f"  {cat:30s}: {count:>3} files")

        print()
        print(f"Extracted files location: {pufs_dir}")
        print("=" * 80)

    return stats
