# © 2025 HarmonyCares
# All rights reserved.

"""Python client wrapper for the ACOMS CLI container."""

from __future__ import annotations

import subprocess
import time
from datetime import datetime
from typing import Any

from .._log import LogWriter
from .config import AcomsConfig
from .models import (
    AcomsCategory,
    DataHubQuery,
    DateFilter,
    DownloadResult,
    FileInfo,
    FileTypeDefinition,
)
from .parser import (
    detect_auth_errors,
    parse_datahub_file_types,
    parse_datahub_output,
)
from .state import AcomsStateTracker


class AcomsError(Exception):
    """Base exception for ACOMS operations."""


class AcomsConfigurationError(AcomsError):
    """Raised when ACOMS is not properly configured."""


class AcomsDownloadError(AcomsError):
    """Raised when an ACOMS download operation fails."""


class Acoms:
    """Docker-backed wrapper for the ACOMS CLI binary."""

    def __init__(
        self,
        config: AcomsConfig | None = None,
        log_writer: LogWriter | None = None,
        enable_duplicate_detection: bool = True,
    ):
        self.config = config or AcomsConfig.from_profile()
        self.config.validate()
        self.log_writer = log_writer or LogWriter(name="acoms")
        self.enable_duplicate_detection = enable_duplicate_detection

        if enable_duplicate_detection:
            self.state_tracker = AcomsStateTracker(
                log_writer=self.log_writer,
                state_file=self.config.tracking_dir / "acoms_state.json",
                search_paths=[self.config.bronze_dir, self.config.archive_dir],
            )
        else:
            self.state_tracker = None

        self._last_request_time: float | None = None

    def _run_command(
        self,
        args: list[str],
        timeout: int | None = None,
        capture_output: bool = True,
    ) -> subprocess.CompletedProcess:
        """Run an ACOMS command inside the persistent compose container."""
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.config.request_delay:
                time.sleep(self.config.request_delay - elapsed)

        self._last_request_time = time.time()
        docker_cmd = ["docker", "exec", self.config.container_name] + args
        self.log_writer.info(f"Running ACOMS via Docker: {' '.join(args)}")

        try:
            result = subprocess.run(
                docker_cmd,
                capture_output=capture_output,
                text=True,
                timeout=timeout or self.config.command_timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as e:
            message = f"ACOMS command timed out after {timeout} seconds"
            self.log_writer.error(message)
            raise AcomsError(message) from e

        combined_output = "\n".join(part for part in (result.stdout, result.stderr) if part)

        if result.returncode != 0:
            message = f"ACOMS command failed with exit code {result.returncode}"
            if combined_output:
                message += f": {combined_output}"
            self.log_writer.error(message)
            raise AcomsError(message)

        auth_hits = detect_auth_errors(combined_output)
        if auth_hits:
            message = (
                f"ACOMS authentication failed: {auth_hits[0]}. "
                "Refresh ACOMS_API_KEY, ACOMS_API_SECRET, and ACOMS_API_ID in deploy/.env."
            )
            self.log_writer.error(message)
            raise AcomsConfigurationError(message)

        parses_file_listing = any(flag in args for flag in ("--view", "-v", "--download", "-d"))
        if result.stdout and "datahub" in args and parses_file_listing:
            parsed = parse_datahub_output(result.stdout, result.stderr)
            self.log_writer.info(f"ACOMS reported {parsed.total_files} files")
            if parsed.errors:
                for error in parsed.errors:
                    self.log_writer.warning(f"ACOMS parser warning: {error}")

        if result.stderr:
            self.log_writer.warning(f"ACOMS stderr:\n{result.stderr}")

        return result

    def list_file_types(self) -> list[FileTypeDefinition]:
        """List ACOMS DataHub categories and file-type codes."""
        result = self._run_command(
            ["acoms", "datahub", "--list"],
            timeout=self.config.list_timeout,
        )
        return parse_datahub_file_types(result.stdout)

    def view_files(
        self,
        category: AcomsCategory | None = None,
        year: int | None = None,
        aco_id: str | None = None,
        file_type_code: int | None = None,
        date_filter: DateFilter | None = None,
    ) -> list[FileInfo]:
        """View files available for download from ACOMS."""
        query = DataHubQuery(
            aco_id=self.config.resolve_aco_id(aco_id),
            year=year or self.config.default_year,
            category=category,
            file_type_code=file_type_code,
            date_filter=date_filter,
        )
        result = self._run_command(
            ["acoms", "datahub", "--view"] + query.to_cli_args(),
            timeout=self.config.list_timeout,
        )
        parsed = parse_datahub_output(
            result.stdout,
            result.stderr,
        )
        return [
            FileInfo(
                name=file_entry.filename,
                size=file_entry.size_bytes,
                last_updated=file_entry.last_updated,
            )
            for file_entry in parsed.files
        ]

    def download(
        self,
        category: AcomsCategory | None = None,
        year: int | None = None,
        aco_id: str | None = None,
        file_type_code: int | None = None,
        date_filter: DateFilter | None = None,
    ) -> DownloadResult:
        """Download files from ACOMS into the bronze workspace."""
        query = DataHubQuery(
            aco_id=self.config.resolve_aco_id(aco_id),
            year=year or self.config.default_year,
            category=category,
            file_type_code=file_type_code,
            date_filter=date_filter,
        )

        args = ["acoms", "datahub", "--download"] + query.to_cli_args()
        started_at = datetime.now()

        try:
            before = {path for path in self.config.bronze_dir.glob("*") if path.is_file()}
            result = self._run_command(args)
            after = {path for path in self.config.bronze_dir.glob("*") if path.is_file()}
            detected_new_files = list(after - before)

            reported_files: list[str] = []
            if result.stdout:
                parsed = parse_datahub_output(result.stdout, result.stderr)
                reported_files = [file_entry.filename for file_entry in parsed.files]

            if self.enable_duplicate_detection and self.state_tracker:
                new_files = self.state_tracker.get_new_files(detected_new_files)
                duplicates = self.state_tracker.get_duplicate_files(detected_new_files)
                if duplicates:
                    self.log_writer.warning(
                        f"Skipped {len(duplicates)} duplicate ACOMS files",
                        duplicate_files=[path.name for path in duplicates],
                    )
                if new_files:
                    self.state_tracker.mark_multiple_downloaded(
                        new_files,
                        category=query.category.value if query.category else "unknown",
                        file_type_code=query.file_type_code or 0,
                    )
            else:
                new_files = detected_new_files

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
                "ACOMS download completed",
                reported_files=reported_files,
                files_downloaded=len(new_files),
                duration_seconds=download_result.duration,
            )
            return download_result
        except AcomsError as e:
            completed_at = datetime.now()
            message = f"ACOMS download failed: {e}"
            self.log_writer.error(message)
            return DownloadResult(
                success=False,
                files_downloaded=[],
                errors=[message],
                download_path=self.config.bronze_dir,
                started_at=started_at,
                completed_at=completed_at,
            )

    def raw(self, args: list[str], timeout: int | None = None) -> dict[str, Any]:
        """Run a raw ACOMS command and return stdout/stderr/returncode."""
        result = self._run_command(args, timeout=timeout)
        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
