# © 2025 HarmonyCares
# All rights reserved.

"""
Bronze ZIP file unpacking utility.

Automatically discovers ZIP files in the bronze storage tier, extracts them
with a flat directory structure, and archives the original ZIP files. Integrates
with ACOHarmony's configuration system and provides schema-aware logging.

Features:
    - Config/profile aware using get_config()
    - Integrated logging using LogWriter
    - Flat extraction (no parent directories)
    - Schema pattern matching and logging
    - Safe error handling (preserves ZIP on failure)
    - Atomic archive operations

"""

import shutil
import zipfile
from pathlib import Path
from typing import Any

from .._log import get_logger
from .._registry import SchemaRegistry
from ..config import get_config


def _load_schemas() -> dict[str, dict[str, Any]]:
    """
    Load all schema definitions from the SchemaRegistry.

        Returns:
            Dictionary mapping schema name to schema configuration.
    """
    logger = get_logger("unpack")

    # Ensure _tables models are imported so SchemaRegistry is populated
    from .. import _tables as _  # noqa: F401

    schemas = {}
    for schema_name in SchemaRegistry.list_schemas():
        config = SchemaRegistry.get_full_table_config(schema_name)
        if config:
            schemas[schema_name] = config
            logger.debug(f"Loaded schema: {schema_name}")

    logger.info(f"Loaded {len(schemas)} schemas")
    return schemas


def _match_file_to_schemas(filename: str, schemas: dict[str, dict[str, Any]]) -> list[str]:
    """
    Match a filename against schema file patterns.

        Args:
            filename: Name of the file to match.
            schemas: Dictionary of schema configurations.

        Returns:
            List of schema names that match the filename.
    """
    from fnmatch import fnmatch

    matches = []

    for schema_name, schema in schemas.items():
        if schema is None:
            continue

        # Check if schema has file patterns
        storage = schema.get("storage", {})
        if storage is None:
            continue

        file_patterns = storage.get("file_patterns", {})
        if file_patterns is None:
            continue

        # Check each pattern type (mssp, reach, etc.)
        for pattern_type, pattern in file_patterns.items():
            if isinstance(pattern, str) and fnmatch(filename, pattern):
                matches.append(f"{schema_name}:{pattern_type}")
                break

    return matches


def _extract_zip_flat(zip_path: Path, dest_dir: Path, logger) -> list[Path]:
    """
    Extract ZIP file with flat directory structure.

        Extracts all files directly to the destination directory, removing
        any parent directory structure from the ZIP archive.

        Args:
            zip_path: Path to the ZIP file.
            dest_dir: Destination directory for extracted files.
            logger: Logger instance.

        Returns:
            List of extracted file paths.

        Raises:
            zipfile.BadZipFile: If ZIP file is corrupted.
            Exception: For other extraction errors.
    """
    extracted_files = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        # Get list of files (excluding directories)
        members = [m for m in zf.namelist() if not m.endswith("/")]
        logger.info(f"ZIP contains {len(members)} files")

        for member in members:
            # Extract to temporary location
            source = zf.open(member)

            # Get just the filename without directory structure
            filename = Path(member).name
            target = dest_dir / filename

            # Check if file already exists
            if target.exists():
                logger.warning(f"File already exists, skipping: {filename}")
                continue

            # Write file
            try:
                with open(target, "wb") as f:
                    shutil.copyfileobj(source, f)

                # Check if extracted file is empty (0 bytes)
                if target.stat().st_size == 0:
                    logger.warning(f"Extracted empty file (0 bytes), deleting: {filename}")
                    target.unlink()
                else:
                    extracted_files.append(target)
                    logger.debug(f"Extracted: {filename}")
            finally:
                source.close()

    return extracted_files


def unpack_bronze_zips(dry_run: bool = False, state_tracker=None) -> dict[str, Any]:
    """
    Discover and extract all ZIP files in the bronze directory.

        This function:
        1. Finds all .zip files in the bronze storage tier
        2. Extracts each ZIP with a flat structure (no parent directories)
        3. Matches extracted files against known schemas
        4. Moves successful ZIPs to archive (original filename)
        5. Updates state tracker with new file locations (if provided)
        6. Logs all operations and schema matches

        Args:
            dry_run: If True, only log operations without executing them.
            state_tracker: Optional FourICLIStateTracker instance to update file locations.

        Returns:
            Dictionary with operation statistics:
                - found: Number of ZIP files found
                - processed: Number of ZIPs successfully processed
                - failed: Number of ZIPs that failed
                - extracted: Total number of files extracted
                - schema_matches: Dictionary of schema match counts

    """
    logger = get_logger("unpack")
    config = get_config()

    # Get paths from config
    storage = config.storage
    bronze_path = storage.base_path / storage.bronze_dir
    archive_path = storage.base_path / storage.archive_dir

    # Ensure directories exist
    bronze_path.mkdir(parents=True, exist_ok=True)
    archive_path.mkdir(parents=True, exist_ok=True)

    # Find all ZIP files first (by extension)
    zip_files = list(bronze_path.glob("*.zip"))

    # Also check for nested zips without real file extension (common in MSSP files)
    # These are detected by magic bytes, not extension
    # Skip known file extensions to avoid xlsx/docx which are technically zips
    known_extensions = {
        ".zip",
        ".csv",
        ".txt",
        ".xlsx",
        ".xls",
        ".docx",
        ".doc",
        ".pdf",
        ".parquet",
        ".json",
        ".xml",
        ".html",
    }
    for file_path in bronze_path.iterdir():
        if file_path.is_file() and file_path.suffix.lower() not in known_extensions:
            if zipfile.is_zipfile(file_path):
                logger.info(f"Found zip without extension: {file_path.name}")
                zip_files.append(file_path)

    # If no ZIP files, log briefly and return
    if not zip_files:
        logger.debug(f"No ZIP files found in {bronze_path}")
        return {
            "found": 0,
            "processed": 0,
            "failed": 0,
            "extracted": 0,
            "schema_matches": {},
        }

    # ZIP files found - log full header
    logger.info("=" * 80)
    logger.info("Starting bronze ZIP extraction")
    logger.info(f"Bronze directory: {bronze_path}")
    logger.info(f"Archive directory: {archive_path}")
    logger.info(f"Dry run: {dry_run}")
    logger.info("=" * 80)

    # Load schemas for pattern matching
    schemas = _load_schemas()
    logger.info(f"Found {len(zip_files)} ZIP files in bronze")

    # Statistics
    stats = {
        "found": len(zip_files),
        "processed": 0,
        "failed": 0,
        "extracted": 0,
        "schema_matches": {},
    }

    # Process each ZIP file
    for zip_path in zip_files:
        logger.info("-" * 80)
        logger.info(f"Processing: {zip_path.name}")

        try:
            if dry_run:
                logger.info(f"[DRY RUN] Would extract {zip_path.name}")
                with zipfile.ZipFile(zip_path, "r") as zf:
                    members = [m for m in zf.namelist() if not m.endswith("/")]
                    logger.info(f"[DRY RUN] Would extract {len(members)} files")
                    for member in members:
                        filename = Path(member).name
                        logger.info(f"[DRY RUN]   - {filename}")
                stats["processed"] += 1
                continue

            # Extract ZIP
            logger.info("Extracting ZIP file...")
            extracted_files = _extract_zip_flat(zip_path, bronze_path, logger)
            stats["extracted"] += len(extracted_files)
            logger.info(f"Successfully extracted {len(extracted_files)} files")

            # Match files against schemas
            logger.info("Matching extracted files against schemas...")
            for file_path in extracted_files:
                matches = _match_file_to_schemas(file_path.name, schemas)

                if matches:
                    logger.info(f"  {file_path.name} → {', '.join(matches)}")
                    for match in matches:
                        stats["schema_matches"][match] = stats["schema_matches"].get(match, 0) + 1
                else:
                    logger.info(f"  {file_path.name} → No schema match")

            # Move ZIP to archive
            archive_dest = archive_path / zip_path.name

            # Check if archive destination already exists
            if archive_dest.exists():
                logger.warning(f"Archive file already exists: {archive_dest.name}")
                logger.warning("Deleting original ZIP without archiving")
                zip_path.unlink()
            else:
                logger.info(f"Moving ZIP to archive: {archive_dest.name}")
                shutil.move(str(zip_path), str(archive_dest))

                # Update state tracker with new location if provided
                if state_tracker is not None:
                    try:
                        state_tracker.update_file_location(zip_path.name, archive_dest)
                        logger.debug(f"Updated state tracker: {zip_path.name} → {archive_dest}")
                    except (
                        Exception
                    ) as e:  # ALLOWED: State update failure - log and continue processing
                        logger.warning(f"Failed to update state tracker: {e}")

            stats["processed"] += 1
            logger.info(f"[OK] Successfully processed {zip_path.name}")

        except (
            zipfile.BadZipFile
        ) as e:  # ALLOWED: Batch processing - log error, continue with remaining ZIPs
            stats["failed"] += 1
            logger.error(f"[ERROR] Invalid ZIP file: {zip_path.name}")
            logger.error(f"  Error: {e}")
            logger.error("  ZIP file kept in bronze for manual review")

        except (
            Exception
        ) as e:  # ALLOWED: Batch processing - log error, continue with remaining ZIPs
            stats["failed"] += 1
            logger.error(f"[ERROR] Failed to process {zip_path.name}")
            logger.error(f"  Error: {e}")
            logger.error("  ZIP file kept in bronze for manual review")

    # Summary
    logger.info("=" * 80)
    logger.info("Extraction complete")
    logger.info(f"Found: {stats['found']} ZIP files")
    logger.info(f"Processed: {stats['processed']} successfully")
    logger.info(f"Failed: {stats['failed']} with errors")
    logger.info(f"Extracted: {stats['extracted']} total files")

    if stats["schema_matches"]:
        logger.info("Schema matches:")
        for schema, count in sorted(stats["schema_matches"].items()):
            logger.info(f"  {schema}: {count} files")
    else:
        logger.info("No schema matches found")

    logger.info("=" * 80)

    return stats


if __name__ == "__main__":
    # CLI entry point for standalone execution
    import sys

    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv

    if dry_run:
        print("Running in DRY RUN mode - no files will be modified")

    result = unpack_bronze_zips(dry_run=dry_run)

    # Exit with error code if any failures
    sys.exit(1 if result["failed"] > 0 else 0)
