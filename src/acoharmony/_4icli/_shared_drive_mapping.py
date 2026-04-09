# © 2025 HarmonyCares
# All rights reserved.

"""
Optional shared drive file mapping for legacy compatibility.

 mapping downloaded files from bronze tier to a shared drive
location using fsspec for flexible filesystem operations. Only used when a
shared drive path is explicitly specified.
"""

import logging
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

try:
    import fsspec  # pragma: no cover

    FSSPEC_AVAILABLE = True  # pragma: no cover
except ImportError:
    FSSPEC_AVAILABLE = False


logger = logging.getLogger(__name__)


@dataclass
class SharedDriveMapping:
    """Configuration for mapping files to shared drive locations."""

    # Shared drive base path (can be local, S3, etc.)
    shared_drive_path: str

    # File pattern to directory mappings based on ACO REACH file types
    mappings: dict[str, str]

    @classmethod
    def default_mappings(cls, shared_drive_path: str) -> "SharedDriveMapping":
        """
        Create default ACO REACH file mappings.

                Args:
                    shared_drive_path: Base path for shared drive (e.g., '/mnt/shared', 's3://bucket/path')
        """
        return cls(
            shared_drive_path=shared_drive_path,
            mappings={
                # Alignment files
                "PALMR": "Provider Alignment Report (PAR)",
                "PBVAR": "Voluntary Alignment Response Files",
                "TPARC": "Beneficiary Alignment Reports (BAR)",
                # CCLF files
                "CCLF": "Claim and Claim Line Feed File (CCLF)",
                # Other reports
                "RAP": "Risk Score Reports",
                "BLQQR": "Beneficiary Level Quarterly Quality Reports",
            },
        )


def detect_file_pattern(filename: str) -> str | None:
    """
    Detect file pattern from filename.

        Args:
            filename: Name of the file

        Returns:
            Pattern key if matched, None otherwise
    """
    # Provider Alignment Report (PAR)
    if "PALMR" in filename:
        return "PALMR"

    # Voluntary Alignment Response Files
    if "PBVAR" in filename:
        return "PBVAR"

    # Beneficiary Alignment Reports (BAR)
    if "TPARC" in filename:
        return "TPARC"

    # CCLF files
    if filename.startswith("CCLF"):
        return "CCLF"

    # Risk Adjustment Payment Reports
    if "RAP" in filename:
        return "RAP"

    # Beneficiary Level Quarterly Quality Reports
    if "BLQQR" in filename:
        return "BLQQR"

    return None


def copy_to_shared_drive(
    source_file: Path,
    mapping: SharedDriveMapping,
    filesystem: str | None = None,
) -> str | None:
    """
    Copy file to shared drive location based on file pattern.

        Args:
            source_file: Source file path
            mapping: Shared drive mapping configuration
            filesystem: Optional filesystem specification (e.g., 's3', 'gcs')

        Returns:
            Destination path if successful, None if pattern not recognized
    """
    if not FSSPEC_AVAILABLE and filesystem:
        logger.error("fsspec not available but filesystem specified")
        raise ImportError("fsspec is required for non-local filesystems")

    # Detect file pattern
    pattern = detect_file_pattern(source_file.name)
    if not pattern or pattern not in mapping.mappings:
        logger.warning(f"No mapping found for file: {source_file.name}")
        return None

    # Build destination path
    dest_dir = mapping.mappings[pattern]
    dest_path = f"{mapping.shared_drive_path}/{dest_dir}/{source_file.name}"

    try:
        if filesystem:
            # Use fsspec for remote filesystems
            fs = fsspec.filesystem(filesystem)
            fs.makedirs(f"{mapping.shared_drive_path}/{dest_dir}", exist_ok=True)
            fs.put(str(source_file), dest_path)
            logger.info(f"Copied {source_file.name} to {dest_path} via {filesystem}")
        else:
            # Use standard shutil for local paths
            dest_path_obj = Path(dest_path)
            dest_path_obj.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, dest_path_obj)
            logger.info(f"Copied {source_file.name} to {dest_path}")

        return dest_path

    except Exception as e:
        logger.error(f"Failed to copy {source_file.name} to shared drive: {e}")
        raise


def copy_cclf_to_shared_drive(
    source_file: Path,
    mapping: SharedDriveMapping,
    filesystem: str | None = None,
) -> str | None:
    """
    Copy CCLF ZIP file to shared drive with month-based subdirectory.

        Args:
            source_file: Source CCLF ZIP file path
            mapping: Shared drive mapping configuration
            filesystem: Optional filesystem specification

        Returns:
            Destination path if successful
    """
    if not source_file.name.startswith("CCLF"):
        logger.warning(f"Not a CCLF file: {source_file.name}")
        return None

    # Create month-based subdirectory
    month_subdir = f"CCLF Delivered in {datetime.now():%B.%Y}"
    cclf_dir = mapping.mappings.get("CCLF", "Claim and Claim Line Feed File (CCLF)")
    dest_path = f"{mapping.shared_drive_path}/{cclf_dir}/{month_subdir}/{source_file.name}"

    try:
        if filesystem:
            fs = fsspec.filesystem(filesystem)
            fs.makedirs(f"{mapping.shared_drive_path}/{cclf_dir}/{month_subdir}", exist_ok=True)
            fs.put(str(source_file), dest_path)
            logger.info(f"Copied CCLF {source_file.name} to {dest_path} via {filesystem}")
        else:
            dest_path_obj = Path(dest_path)
            dest_path_obj.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, dest_path_obj)
            logger.info(f"Copied CCLF {source_file.name} to {dest_path}")

        return dest_path

    except Exception as e:
        logger.error(f"Failed to copy CCLF {source_file.name} to shared drive: {e}")
        raise


def sync_to_shared_drive(
    bronze_files: list[Path],
    mapping: SharedDriveMapping,
    filesystem: str | None = None,
) -> dict[Path, str | None]:
    """
    Sync multiple files from bronze tier to shared drive.

        Args:
            bronze_files: List of files in bronze tier
            mapping: Shared drive mapping configuration
            filesystem: Optional filesystem specification

        Returns:
            Dictionary mapping source files to destination paths
    """
    results = {}

    for file_path in bronze_files:
        if not file_path.is_file():
            continue

        try:
            # Special handling for CCLF files
            if file_path.name.startswith("CCLF") and file_path.suffix.upper() == ".ZIP":
                dest = copy_cclf_to_shared_drive(file_path, mapping, filesystem)
            else:
                dest = copy_to_shared_drive(file_path, mapping, filesystem)

            results[file_path] = dest

        except (
            Exception
        ) as e:  # ALLOWED: Logs error and returns, caller handles the error condition
            logger.error(f"Error syncing {file_path.name}: {e}")
            results[file_path] = None

    return results
