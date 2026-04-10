# © 2025 HarmonyCares
# All rights reserved.

"""Python client wrapper for 4icli binary."""

import subprocess
import time
from datetime import datetime
from typing import Any

from .._log import LogWriter
from .config import FourICLIConfig, get_current_year
from .models import (
    DataHubCategory,
    DataHubQuery,
    DateFilter,
    DownloadResult,
    FileInfo,
    FileTypeCode,
)
from .parser import parse_datahub_output
from .state import FourICLIStateTracker


class FourICLIError(Exception):
    """Base exception for 4icli operations."""

    pass


class FourICLIConfigurationError(FourICLIError):
    """Raised when 4icli is not properly configured."""

    pass


class FourICLIDownloadError(FourICLIError):
    """Raised when download operation fails."""

    pass


class FourICLI:
    """Python wrapper for the 4icli binary."""

    def __init__(
        self,
        config: FourICLIConfig | None = None,
        log_writer: LogWriter | None = None,
        enable_duplicate_detection: bool = True,
    ):
        """
        Initialize the 4icli client.

                Args:
                    config: Configuration object. If None, uses profile configuration.
                    log_writer: LogWriter instance for state tracking. If None, creates new one.
                    enable_duplicate_detection: Whether to track state and prevent duplicate downloads.
        """
        self.config = config or FourICLIConfig.from_profile()
        self.config.validate()

        # Use ACO Harmony LogWriter for state tracking
        self.log_writer = log_writer or LogWriter(name="4icli")

        # State tracker for duplicate detection (rooted in this config's tracking_dir)
        self.enable_duplicate_detection = enable_duplicate_detection
        if enable_duplicate_detection:
            self.state_tracker = FourICLIStateTracker(
                log_writer=self.log_writer,
                state_file=self.config.tracking_dir / "4icli_state.json",
                search_paths=[self.config.bronze_dir, self.config.archive_dir],
            )
        else:
            self.state_tracker = None

        # Track last request time for rate limiting
        self._last_request_time: float | None = None

    def _run_command(
        self,
        args: list[str],
        timeout: int | None = None,
        capture_output: bool = True,
    ) -> subprocess.CompletedProcess:
        """
        Run a 4icli command via Docker container.

                Runs 4icli in an isolated container with:
                - Storage backend mounted to /workspace
                - config.txt created from env vars or bind-mounted
                - Working directory set to bronze tier
                - Rate limiting to prevent API errors

                Args:
                    args: Command arguments (not including the binary path)
                    timeout: Command timeout in seconds
                    capture_output: Whether to capture stdout/stderr

                Returns:
                    CompletedProcess object

                Raises:
                    FourICLIError: If command fails
        """
        # Rate limiting - wait between requests to avoid API limits
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.config.request_delay:
                wait_time = self.config.request_delay - elapsed
                self.log_writer.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                time.sleep(wait_time)

        self._last_request_time = time.time()

        # Use persistent 4icli container via docker exec
        # The container is already running with proper volume mounts from compose
        docker_cmd = [
            "docker",
            "exec",
            # Use the named container from compose
            "4icli",
        ] + args

        self.log_writer.info(f"Running 4icli via Docker: {' '.join(args)}")

        try:
            result = subprocess.run(
                docker_cmd,
                capture_output=capture_output,
                text=True,
                timeout=timeout or self.config.command_timeout,
                check=False,
            )

            if result.returncode != 0:
                # Check if this is a permission denied error (file already exists)
                # EACCES errors mean the file was previously downloaded and is readonly
                if result.stderr and "EACCES: permission denied" in result.stderr:
                    self.log_writer.info(
                        "Permission denied on existing file - skipping (file already downloaded)"
                    )
                    # Return successful result with empty output
                    # This allows the download to continue and be marked as successful
                    return result

                error_msg = f"Command failed with exit code {result.returncode}"
                if result.stderr:
                    error_msg += f": {result.stderr}"
                self.log_writer.error(error_msg)
                raise FourICLIError(error_msg)

            # Log successful command completion with output
            self.log_writer.info("Command completed successfully")

            # Parse and log stdout if it's a datahub command
            if result.stdout and "datahub" in " ".join(args):
                try:
                    parsed = parse_datahub_output(result.stdout, result.stderr)

                    # Log summary
                    self.log_writer.info(
                        f"4icli reported {parsed.total_files} files"
                        + (f" in {parsed.session_duration}s" if parsed.session_duration else "")
                    )

                    # Log file details
                    if parsed.files:
                        self.log_writer.info("Files from 4icli:")
                        for file_entry in parsed.files[:10]:  # Show first 10
                            self.log_writer.info(
                                f"  - {file_entry.filename}"
                                + (f" ({file_entry.size_str})" if file_entry.size_str else "")
                            )
                        if len(parsed.files) > 10:
                            self.log_writer.info(f"  ... and {len(parsed.files) - 10} more files")

                    # Log any parsing errors/warnings
                    if parsed.errors:
                        for error in parsed.errors:
                            self.log_writer.warning(f"Parser warning: {error}")

                    # Attach parsed data to result for downstream use
                    result.parsed_output = parsed

                except Exception as e:  # ALLOWED: Parse failure - fall back to raw output logging
                    self.log_writer.warning(f"Failed to parse command output: {e}")
                    # Fall back to raw output logging
                    self.log_writer.info(f"Command output:\n{result.stdout}")
            elif result.stdout:
                # Non-datahub commands: log raw output
                self.log_writer.info(f"Command output:\n{result.stdout}")

            # Log stderr if present (even on success, for warnings)
            if result.stderr:
                self.log_writer.warning(f"Command stderr:\n{result.stderr}")

            return result

        except subprocess.TimeoutExpired as e:
            error_msg = f"Command timed out after {timeout} seconds"
            self.log_writer.error(error_msg)
            raise FourICLIError(error_msg) from e
        except Exception as e:
            error_msg = f"Command execution failed: {str(e)}"
            self.log_writer.error(error_msg)
            raise FourICLIError(error_msg) from e

    def configure(self, interactive: bool = True) -> None:
        """
        Configure 4icli credentials.

                Args:
                    interactive: Whether to run in interactive mode

                Raises:
                    FourICLIConfigurationError: If configuration fails
        """
        try:
            self.log_writer.info("Configuring 4icli credentials")
            self._run_command(["configure"], capture_output=not interactive)
            self.log_writer.info("Configuration completed successfully")
        except FourICLIError as e:
            raise FourICLIConfigurationError(f"Failed to configure 4icli: {str(e)}") from e

    def rotate_credentials(self) -> None:
        """
        Rotate API credentials.

                Raises:
                    FourICLIConfigurationError: If rotation fails
        """
        try:
            self.log_writer.info("Rotating 4icli credentials")
            self._run_command(["rotate"])
            self.log_writer.info("Credentials rotated successfully")
        except FourICLIError as e:
            raise FourICLIConfigurationError(f"Failed to rotate credentials: {str(e)}") from e

    def list_categories(self) -> dict[str, Any]:
        """
        List available DataHub folders and file types.

                Returns:
                    Dictionary with available categories and file types
        """
        self.log_writer.info("Listing DataHub categories")
        result = self._run_command(["4icli", "datahub", "-l"], timeout=self.config.list_timeout)

        # Parse the output
        # Note: Actual parsing depends on 4icli output format
        return {"output": result.stdout}

    def view_files(
        self,
        category: DataHubCategory | None = None,
        year: int | None = None,
        apm_id: str | None = None,
        date_filter: DateFilter | None = None,
    ) -> list[FileInfo]:
        """
        View list of files available for download.

                Args:
                    category: DataHub category to query
                    year: Performance year
                    apm_id: APM Entity ID
                    date_filter: Date filtering options

                Returns:
                    List of available files
        """
        query = DataHubQuery(
            category=category,
            year=year or self.config.default_year,
            apm_id=apm_id or self.config.default_apm_id,
            date_filter=date_filter,
        )

        args = ["4icli", "datahub", "-v"] + query.to_cli_args()

        self.log_writer.info(f"Viewing files with query: {query}")
        result = self._run_command(args, timeout=self.config.list_timeout)

        # Parse output to FileInfo objects
        # Note: Actual parsing depends on 4icli output format
        files = []
        for line in result.stdout.splitlines():
            if line.strip():
                files.append(FileInfo.from_filename(line.strip()))

        self.log_writer.info(f"Found {len(files)} files")
        return files

    def download(
        self,
        category: DataHubCategory | None = None,
        year: int | None = None,
        apm_id: str | None = None,
        file_type_code: FileTypeCode | None = None,
        date_filter: DateFilter | None = None,
    ) -> DownloadResult:
        """
        Download files from DataHub.

                Args:
                    category: DataHub category to download from
                    year: Performance year
                    apm_id: APM Entity ID
                    file_type_code: Specific file type to download
                    date_filter: Date filtering options

                Returns:
                    DownloadResult with information about downloaded files

                Raises:
                    FourICLIDownloadError: If download fails
        """
        query = DataHubQuery(
            category=category,
            year=year or self.config.default_year,
            apm_id=apm_id or self.config.default_apm_id,
            file_type_code=file_type_code,
            date_filter=date_filter,
        )

        args = ["4icli", "datahub", "-d"] + query.to_cli_args()

        started_at = datetime.now()
        self.log_writer.info(
            "Starting download",
            category=query.category.value if query.category else None,
            file_type_code=query.file_type_code.value if query.file_type_code else None,
            year=query.year,
        )

        try:
            # Get list of files before download in bronze directory
            files_before = set(self.config.bronze_dir.glob("*"))

            # Run download (4icli downloads to bronze tier)
            result = self._run_command(args)

            # Get list of files after download
            files_after = set(self.config.bronze_dir.glob("*"))
            all_new_files = list(files_after - files_before)

            # Extract parsed output if available
            parsed_output = getattr(result, "parsed_output", None)
            reported_files = []
            if parsed_output:
                reported_files = [f.filename for f in parsed_output.files]
                self.log_writer.info(
                    f"4icli reported downloading {len(reported_files)} files, "
                    f"detected {len(all_new_files)} new files in bronze directory"
                )

            # Filter out duplicates if enabled
            if self.enable_duplicate_detection and self.state_tracker:
                new_files = self.state_tracker.get_new_files(all_new_files)
                duplicate_files = self.state_tracker.get_duplicate_files(all_new_files)

                # Log duplicates
                if duplicate_files:
                    self.log_writer.warning(
                        f"Skipped {len(duplicate_files)} duplicate files",
                        duplicate_count=len(duplicate_files),
                        duplicate_files=[f.name for f in duplicate_files],
                    )

                # Mark new files as downloaded
                if new_files:
                    self.state_tracker.mark_multiple_downloaded(
                        new_files,
                        category=query.category.value if query.category else "unknown",
                        file_type_code=query.file_type_code.value if query.file_type_code else 0,
                    )
            else:
                new_files = all_new_files

            completed_at = datetime.now()

            download_result = DownloadResult(
                success=True,
                files_downloaded=new_files,
                errors=[],
                download_path=self.config.bronze_dir,
                started_at=started_at,
                completed_at=completed_at,
            )

            self.log_writer.info(
                "Download completed",
                files_downloaded=len(new_files),
                duration_seconds=download_result.duration,
                total_size_mb=sum(f.stat().st_size for f in new_files) / (1024 * 1024)
                if new_files
                else 0,
            )

            return download_result

        except (
            FourICLIError
        ) as e:  # ALLOWED: Returns DownloadResult with success=False, caller checks result object
            completed_at = datetime.now()
            error_msg = f"Download failed: {str(e)}"
            self.log_writer.error(error_msg)

            return DownloadResult(
                success=False,
                files_downloaded=[],
                errors=[error_msg],
                download_path=self.config.bronze_dir,
                started_at=started_at,
                completed_at=completed_at,
            )

    def download_cclf(
        self,
        year: int | None = None,
        created_within_last_week: bool = False,
    ) -> DownloadResult:
        """
        Convenience method to download CCLF files.

                Args:
                    year: Performance year
                    created_within_last_week: Only download files from last week

                Returns:
                    DownloadResult
        """
        date_filter = None
        if created_within_last_week:
            date_filter = DateFilter(created_within_last_week=True)

        return self.download(
            category=DataHubCategory.CCLF,
            year=year,
            date_filter=date_filter,
        )

    def download_alignment_files(
        self,
        year: int | None = None,
        created_after: str | None = None,
    ) -> DownloadResult:
        """
        Convenience method to download alignment files.

                Args:
                    year: Performance year
                    created_after: Only download files created after this date (YYYY-MM-DD)

                Returns:
                    DownloadResult
        """
        date_filter = None
        if created_after:
            date_filter = DateFilter(created_after=created_after)

        return self.download(
            category=DataHubCategory.BENEFICIARY_LIST,
            year=year,
            date_filter=date_filter,
        )

    def view_all_files(
        self,
        category: DataHubCategory | None = None,
        year: int | None = None,
        apm_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        View all files available in DataHub for a specific year.

                Uses 'datahub -v' to get complete file listing with metadata.
                REQUIRES: -a (APM ID), -y (year), -v (view mode)

                Args:
                    category: DataHub category to query
                    year: Performance year (REQUIRED)
                    apm_id: APM Entity ID (defaults to config)

                Returns:
                    List of file metadata dictionaries with keys:
                        - filename: File name
                        - size: File size in bytes
                        - created: Creation date (ISO format)
                        - modified: Modification date (ISO format)
        """
        query = DataHubQuery(
            category=category,
            year=year or self.config.default_year,
            apm_id=apm_id or self.config.default_apm_id,
        )

        args = ["4icli", "datahub", "-v"] + query.to_cli_args()

        self.log_writer.info(f"Discovering remote files: {query}")
        result = self._run_command(args, timeout=self.config.list_timeout)

        # Parse output to file metadata dicts
        # Format: " N of M - filename.zip (XX.XX MB) Last Updated: YYYY-MM-DDTHH:MM:SS.000Z"
        files = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line or "of" not in line or ".zip" not in line:
                continue

            try:
                # Extract filename (between " - " and " (")
                if " - " in line and " (" in line:
                    filename_start = line.index(" - ") + 3
                    filename_end = line.index(" (", filename_start)
                    filename = line[filename_start:filename_end].strip()

                    # Extract size (between "(" and "MB)")
                    size_mb = 0
                    if " MB)" in line:
                        size_start = line.index("(", filename_end) + 1
                        size_end = line.index(" MB)", size_start)
                        size_str = line[size_start:size_end].strip()
                        size_mb = float(size_str)

                    # Extract last updated timestamp
                    modified = None
                    if "Last Updated:" in line:
                        timestamp_start = line.index("Last Updated:") + 13
                        timestamp_str = line[timestamp_start:].strip()
                        modified = timestamp_str

                    files.append(
                        {
                            "filename": filename,
                            "name": filename,  # Alias for compatibility
                            "size": int(size_mb * 1024 * 1024),  # Convert MB to bytes
                            "size_mb": size_mb,
                            "created": modified,  # Use modified as created (4icli only shows one date)
                            "modified": modified,
                        }
                    )
            except (
                ValueError,
                IndexError,
            ) as e:  # ALLOWED: Continues processing remaining items despite error
                # Skip lines we can't parse
                self.log_writer.debug(f"Could not parse line: {line}: {e}")
                continue

        self.log_writer.info(f"Discovered {len(files)} remote files for year {query.year}")
        return files

    def discover_remote_inventory(
        self,
        category: DataHubCategory | None = None,
        start_year: int = 2022,
        end_year: int | None = None,
    ) -> int:
        """
        Discover and update state with ALL files across ALL years.

                IMPORTANT: Files from any year can be updated at any time, so we must
                check ALL years on each sync by running 'datahub -v -a <apm_id> -y <year>'
                for each year from start_year to end_year.

                Args:
                    category: DataHub category to discover
                    start_year: Starting year (default: 2022)
                    end_year: Ending year (default: current year from config)

                Returns:
                    Total number of files discovered across all years
        """
        if not self.enable_duplicate_detection or not self.state_tracker:
            raise FourICLIError("State tracking must be enabled for inventory discovery")

        end_year = end_year or get_current_year()
        total_files = 0

        self.log_writer.info(
            f"Starting full inventory discovery for {category.value if category else 'all categories'} "
            f"from {start_year} to {end_year}"
        )

        # Loop through ALL years - files from any year could have been updated
        for year in range(start_year, end_year + 1):
            self.log_writer.info(f"Discovering remote files for year {year}")

            remote_files = self.view_all_files(
                category=category,
                year=year,
            )

            if remote_files:
                # Update state tracker with remote inventory
                if category:
                    self.state_tracker.update_remote_inventory(
                        remote_files=remote_files,
                        category=category.value,
                        file_type_code=0,  # All file types for this category
                    )
                else:
                    # No category specified - treat as generic discovery
                    self.state_tracker.update_remote_inventory(
                        remote_files=remote_files,
                        category="all",
                        file_type_code=0,
                    )

                total_files += len(remote_files)
                self.log_writer.info(f"Year {year}: found {len(remote_files)} files")

        self.log_writer.info(f"Total files discovered across all years: {total_files}")
        return total_files

    def sync_incremental(
        self,
        category: DataHubCategory | None = None,
        file_type_codes: list[FileTypeCode] | None = None,
        start_year: int = 2022,
        end_year: int | None = None,
    ) -> DownloadResult:
        """
        Perform incremental sync - download only new/updated files.

                Workflow:
                1. Discovery: Run 'datahub -v -a <apm> -y <year> [-f <type>]' for ALL years (2022-current)
                   and ALL specified file types because files can be updated at any time
                2. Comparison: Compare remote inventory vs local storage
                3. Download: Fetch only files that are new or updated since last download

                Args:
                    category: DataHub category to sync
                    file_type_codes: List of file type codes to sync (if None, syncs all types)
                    start_year: Starting year for discovery (default: 2022)
                    end_year: Ending year for discovery (default: current year)

                Returns:
                    DownloadResult with downloaded files
        """
        if not self.enable_duplicate_detection or not self.state_tracker:
            raise FourICLIError("State tracking must be enabled for incremental sync")

        started_at = datetime.now()

        # Step 1: Discovery - get remote inventory across ALL years
        self.log_writer.info(
            f"Starting incremental sync - discovery phase for all years {start_year}-{end_year or get_current_year()}"
        )
        self.discover_remote_inventory(
            category=category,
            start_year=start_year,
            end_year=end_year,
        )

        # Step 2: Comparison - determine what to download
        self.log_writer.info("Comparison phase - checking what to download")
        files_to_download = self.state_tracker.get_files_to_download(
            storage_backend_path=self.config.bronze_dir,
            category=category.value if category else None,
        )

        if not files_to_download:
            self.log_writer.info("No new or updated files to download")
            return DownloadResult(
                success=True,
                files_downloaded=[],
                errors=[],
                download_path=self.config.bronze_dir,
                started_at=started_at,
                completed_at=datetime.now(),
            )

        self.log_writer.info(f"Found {len(files_to_download)} files to download (new or updated)")

        # Step 3: Download - fetch files by year and file type
        # Note: 4icli doesn't support downloading specific filenames,
        # only by category/year/file_type, so we iterate through all combinations
        # The state tracker filters duplicates during download

        all_downloaded = []
        end_year = end_year or get_current_year()

        # If no file types specified, download entire category
        if not file_type_codes:
            for year in range(start_year, end_year + 1):
                self.log_writer.info(
                    f"Downloading all files for {category.value if category else 'all'} year {year}"
                )
                result = self.download(
                    category=category,
                    year=year,
                )
                all_downloaded.extend(result.files_downloaded)
        else:
            # Download specific file types
            for year in range(start_year, end_year + 1):
                for file_type_code in file_type_codes:
                    self.log_writer.info(
                        f"Downloading {category.value if category else 'all'} "
                        f"file type {file_type_code.value} for year {year}"
                    )
                    result = self.download(
                        category=category,
                        year=year,
                        file_type_code=file_type_code,
                    )
                    all_downloaded.extend(result.files_downloaded)

        completed_at = datetime.now()

        return DownloadResult(
            success=True,
            files_downloaded=all_downloaded,
            errors=[],
            download_path=self.config.bronze_dir,
            started_at=started_at,
            completed_at=completed_at,
        )

    def sync_all_years(
        self,
        category: DataHubCategory,
        file_type_codes: list[FileTypeCode] | None = None,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> DownloadResult:
        """
        Sync a category across multiple years.

                This is just an alias for sync_incremental() since that method
                already handles all years.

                Args:
                    category: DataHub category to sync
                    file_type_codes: List of file type codes to sync (if None, syncs all types)
                    start_year: Starting year (default: 2022)
                    end_year: Ending year (default: current year)

                Returns:
                    DownloadResult with all files downloaded
        """
        start_year = start_year or 2022
        end_year = end_year or get_current_year()

        self.log_writer.info(f"Syncing {category.value} from {start_year} to {end_year}")

        result = self.sync_incremental(
            category=category,
            file_type_codes=file_type_codes,
            start_year=start_year,
            end_year=end_year,
        )

        total_files = len(result.files_downloaded)
        self.log_writer.info(f"Completed multi-year sync: {total_files} total files downloaded")

        return result

    def get_sync_status(
        self,
        category: DataHubCategory | None = None,
    ) -> dict[str, Any]:
        """
        Get current sync status and statistics.

                Args:
                    category: Optional category to filter stats

                Returns:
                    Dictionary with sync statistics
        """
        if not self.enable_duplicate_detection or not self.state_tracker:
            return {"state_tracking": "disabled"}

        stats = self.state_tracker.get_download_stats()
        last_sync = self.state_tracker.get_last_sync_time(
            category=category.value if category else None
        )

        return {
            "state_tracking": "enabled",
            "last_sync": last_sync.isoformat() if last_sync else None,
            "total_files_tracked": stats.get("total_files", 0),
            "total_size_mb": stats.get("total_size_mb", 0),
            "categories": stats.get("categories", {}),
            "file_types": stats.get("file_types", {}),
        }

    def help(self, command: str | None = None) -> str:
        """
        Get help information.

                Args:
                    command: Specific command to get help for

                Returns:
                    Help text
        """
        args = ["help"]
        if command:
            args.append(command)

        result = self._run_command(args, timeout=30)
        return result.stdout
