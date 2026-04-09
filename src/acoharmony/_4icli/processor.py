# © 2025 HarmonyCares
# All rights reserved.

"""File processor for organizing downloaded 4icli files."""

import logging
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .config import FourICLIConfig
from .models import FileInfo, FileType

logger = logging.getLogger(__name__)


@dataclass
class ProcessingResult:
    """Result of file processing operation."""

    files_processed: list[Path]
    files_moved: dict[Path, Path]  # source -> destination
    files_extracted: list[Path]
    errors: list[str]
    started_at: datetime
    completed_at: datetime | None = None

    @property
    def success(self) -> bool:
        """Whether processing completed without errors."""
        return len(self.errors) == 0

    @property
    def file_count(self) -> int:
        """Total number of files processed."""
        return len(self.files_processed)


class FileProcessor:
    """
    Processes downloaded files by organizing them into appropriate directories.

        This replicates the logic from the shell script 01_process_incoming.sh
    """

    def __init__(self, config: FourICLIConfig | None = None):
        """
        Initialize the file processor.

                Args:
                    config: Configuration object. If None, uses default configuration.
        """
        self.config = config or FourICLIConfig.from_profile()
        # Ensure storage directories exist when processor is initialized
        self.config.ensure_storage_directories()
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Configure logging for file processing."""
        if self.config.enable_logging:
            log_file = self.config.log_dir / f"file_processor_{datetime.now():%Y%m%d_%H%M%S}.log"
            handler = logging.FileHandler(log_file)
            handler.setFormatter(
                logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

    def process_all(self) -> ProcessingResult:
        """
        Process all files in the download directory.

                Returns:
                    ProcessingResult with details of the operation
        """
        started_at = datetime.now()
        logger.info("Starting file processing")

        files_processed = []
        files_moved = {}
        files_extracted = []
        errors = []

        # Get all files in download directory
        download_files = list(self.config.download_dir.glob("*"))
        logger.info(f"Found {len(download_files)} files to process")

        for file_path in download_files:
            if file_path.is_file():
                try:
                    result = self.process_file(file_path)
                    files_processed.append(file_path)
                    if result["moved"]:
                        files_moved.update(result["moved"])
                    if result["extracted"]:
                        files_extracted.extend(result["extracted"])
                except Exception as e:  # ALLOWED: Batch file processing - log error, collect error, continue with remaining files
                    error_msg = f"Error processing {file_path.name}: {str(e)}"
                    logger.error(error_msg)
                    errors.append(error_msg)

        completed_at = datetime.now()
        logger.info(
            f"Processing completed: {len(files_processed)} files processed, "
            f"{len(files_moved)} moved, {len(files_extracted)} extracted, "
            f"{len(errors)} errors"
        )

        return ProcessingResult(
            files_processed=files_processed,
            files_moved=files_moved,
            files_extracted=files_extracted,
            errors=errors,
            started_at=started_at,
            completed_at=completed_at,
        )

    def process_file(self, file_path: Path) -> dict[str, any]:
        """
        Process a single file by determining its type and moving it to the appropriate location.

                Args:
                    file_path: Path to the file to process

                Returns:
                    Dictionary with 'moved' and 'extracted' keys containing processing results
        """
        logger.info(f"Processing file: {file_path.name}")

        file_info = FileInfo.from_filename(file_path.name)
        result = {"moved": {}, "extracted": []}

        if file_info.file_type == FileType.PROVIDER_ALIGNMENT:
            result["moved"] = self._process_alignment_file(file_path, "PALMR")

        elif file_info.file_type == FileType.VOLUNTARY_ALIGNMENT:
            result["moved"] = self._process_alignment_file(file_path, "PBVAR")

        elif file_info.file_type == FileType.BENEFICIARY_ALIGNMENT:
            result["moved"] = self._process_alignment_file(file_path, "PBAR")

        elif file_info.file_type == FileType.CCLF:
            if file_path.suffix.upper() == ".ZIP":
                result["extracted"] = self._process_cclf_zip(file_path)
            else:
                result["moved"] = self._process_cclf_file(file_path)

        elif file_info.file_type == FileType.RISK_ADJUSTMENT:
            result["moved"] = self._process_risk_adjustment(file_path)

        elif file_info.file_type == FileType.QUALITY_REPORT:
            result["moved"] = self._process_quality_report(file_path)

        else:
            # Unrecognized file type
            result["moved"] = self._process_other_file(file_path)

        return result

    def _process_alignment_file(self, file_path: Path, alignment_type: str) -> dict[Path, Path]:
        """Process alignment files (PALMR, PBVAR, TPARC)."""
        destinations = {}

        # Move to raw data directory
        raw_dest = self.config.get_alignment_dir(alignment_type)
        raw_dest.mkdir(parents=True, exist_ok=True)
        raw_file = raw_dest / file_path.name

        logger.info(f"Moving {file_path.name} to {raw_dest}")
        shutil.move(str(file_path), str(raw_file))
        destinations[file_path] = raw_file

        # Copy to reports directory
        report_dest = self.config.get_report_dir(alignment_type)
        report_dest.mkdir(parents=True, exist_ok=True)
        report_file = report_dest / file_path.name

        logger.info(f"Copying to {report_dest}")
        shutil.copy2(str(raw_file), str(report_file))

        return destinations

    def _process_cclf_zip(self, file_path: Path) -> list[Path]:
        """Process CCLF ZIP files by extracting them."""
        extracted_files = []

        # Extract CCLF number from filename (e.g., CCLF8)
        # Assuming format like CCLF8.D240101.T1234567.zip
        cclf_number = None
        if file_path.name.startswith("CCLF"):
            cclf_number = file_path.name.split(".")[0].replace("CCLF", "")

        # Determine extraction directory
        extract_dir = self.config.get_cclf_dir(cclf_number)
        extract_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Extracting {file_path.name} to {extract_dir}")

        try:
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
                extracted_files = [extract_dir / name for name in zip_ref.namelist()]

            # Copy ZIP to reports directory
            report_dest = self.config.get_report_dir("CCLF")
            # Create month-based subdirectory
            month_dir = report_dest / f"CCLF Delivered in {datetime.now():%B.%Y}"
            month_dir.mkdir(parents=True, exist_ok=True)

            report_file = month_dir / file_path.name
            logger.info(f"Copying ZIP to {month_dir}")
            shutil.copy2(str(file_path), str(report_file))

            # Remove original ZIP from download directory
            file_path.unlink()

        except (
            zipfile.BadZipFile
        ) as e:  # ALLOWED: Logs error and returns, caller handles the error condition
            logger.error(f"Invalid ZIP file {file_path.name}: {str(e)}")
            raise

        return extracted_files

    def _process_cclf_file(self, file_path: Path) -> dict[Path, Path]:
        """Process individual CCLF files (non-ZIP)."""
        destinations = {}

        cclf_dir = self.config.get_cclf_dir()
        cclf_dir.mkdir(parents=True, exist_ok=True)

        dest_file = cclf_dir / file_path.name
        logger.info(f"Moving {file_path.name} to {cclf_dir}")
        shutil.move(str(file_path), str(dest_file))
        destinations[file_path] = dest_file

        return destinations

    def _process_risk_adjustment(self, file_path: Path) -> dict[Path, Path]:
        """Process Risk Adjustment Payment Reports."""
        destinations = {}

        # Move to expenditure directory
        expenditure_dir = self.config.raw_data_dir / "expenditure"
        expenditure_dir.mkdir(parents=True, exist_ok=True)

        dest_file = expenditure_dir / file_path.name
        logger.info(f"Moving {file_path.name} to {expenditure_dir}")
        shutil.move(str(file_path), str(dest_file))
        destinations[file_path] = dest_file

        # Copy to reports directory
        report_dest = self.config.get_report_dir("RAP")
        report_dest.mkdir(parents=True, exist_ok=True)
        report_file = report_dest / file_path.name

        logger.info(f"Copying to {report_dest}")
        shutil.copy2(str(dest_file), str(report_file))

        return destinations

    def _process_quality_report(self, file_path: Path) -> dict[Path, Path]:
        """Process Beneficiary Level Quarterly Quality Reports."""
        destinations = {}

        quality_dir = self.config.raw_data_dir / "quality" / "blqqr"
        quality_dir.mkdir(parents=True, exist_ok=True)

        dest_file = quality_dir / file_path.name
        logger.info(f"Moving {file_path.name} to {quality_dir}")
        shutil.move(str(file_path), str(dest_file))
        destinations[file_path] = dest_file

        return destinations

    def _process_other_file(self, file_path: Path) -> dict[Path, Path]:
        """Process unrecognized files."""
        destinations = {}

        other_dir = self.config.raw_data_dir / "other"
        other_dir.mkdir(parents=True, exist_ok=True)

        dest_file = other_dir / file_path.name
        logger.info(f"Moving unrecognized file {file_path.name} to {other_dir}")
        shutil.move(str(file_path), str(dest_file))
        destinations[file_path] = dest_file

        return destinations

    def cleanup_download_dir(self, keep_files: bool = False) -> int:
        """
        Clean up the download directory.

                Args:
                    keep_files: If True, keeps files but logs them. If False, removes them.

                Returns:
                    Number of files processed
        """
        files = list(self.config.download_dir.glob("*"))
        count = 0

        for file_path in files:
            if file_path.is_file():
                if keep_files:
                    logger.info(f"Keeping file: {file_path.name}")
                else:
                    logger.info(f"Removing file: {file_path.name}")
                    file_path.unlink()
                count += 1

        logger.info(f"Processed {count} files in download directory")
        return count
