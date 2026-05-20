# © 2025 HarmonyCares
# All rights reserved.

"""
State tracking for 4icli downloads to prevent duplicates.

Uses the ACO Harmony LogWriter to track download history and file states,
similar to transform state tracking.

State is persisted in tracking/4icli_state.json and logged to daily JSONL files.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .._log import LogWriter


@dataclass
class FileDownloadState:
    """State information for a downloaded file."""

    filename: str
    file_hash: str  # SHA256 of file content
    download_timestamp: datetime
    category: str
    file_type_code: int
    file_size: int
    source_path: str  # Where it was downloaded to
    last_seen_remote: datetime | None = None  # Last time we saw this file in datahub -v
    remote_metadata: dict[str, any] | None = None  # Metadata from 4icli (date, size, etc)

    def to_dict(self) -> dict[str, any]:
        """Convert to dictionary for logging."""
        return {
            "filename": self.filename,
            "file_hash": self.file_hash,
            "download_timestamp": self.download_timestamp.isoformat(),
            "category": self.category,
            "file_type_code": self.file_type_code,
            "file_size": self.file_size,
            "source_path": self.source_path,
            "last_seen_remote": self.last_seen_remote.isoformat()
            if self.last_seen_remote
            else None,
            "remote_metadata": self.remote_metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, any]) -> FileDownloadState:
        """Create from dictionary."""
        return cls(
            filename=data["filename"],
            file_hash=data["file_hash"],
            download_timestamp=datetime.fromisoformat(data["download_timestamp"]),
            category=data["category"],
            file_type_code=data["file_type_code"],
            file_size=data["file_size"],
            source_path=data["source_path"],
            last_seen_remote=datetime.fromisoformat(data["last_seen_remote"])
            if data.get("last_seen_remote")
            else None,
            remote_metadata=data.get("remote_metadata"),
        )


class FourICLIStateTracker:
    """
    Tracks 4icli download state to prevent duplicate downloads.

        Uses LogWriter to persist state information as structured log entries.
    """

    def __init__(
        self,
        log_writer: LogWriter | None = None,
        state_file: Path | None = None,
        search_paths: list[Path] | None = None,
    ):
        """
        Initialize state tracker.

                Args:
                    log_writer: LogWriter instance. If None, creates new one with name '4icli'.
                    state_file: Path to state JSON file. If None, uses tracking/4icli_state.json
                    search_paths: Directories to search for files. If None, uses bronze and archive from config.
        """
        self.log_writer = log_writer or LogWriter(name="4icli")

        # State file in tracking directory
        if state_file is None:
            from .config import FourICLIConfig

            config = FourICLIConfig.from_profile()
            self.state_file = config.tracking_dir / "4icli_state.json"

            # Set up search paths from config if not provided
            if search_paths is None:
                self.search_paths = [
                    config.data_path / "bronze",
                    config.data_path / "archive",
                ]
            else:
                self.search_paths = search_paths
        else:
            self.state_file = state_file
            self.search_paths = search_paths or []

        self._file_cache: dict[str, FileDownloadState] = {}
        self._load_state()

    def _load_state(self) -> None:
        """Load existing state from tracking/4icli_state.json."""
        try:
            if not self.state_file.exists():
                self.log_writer.info(f"No state file found at {self.state_file}, starting fresh")
                return

            with open(self.state_file) as f:
                state_data = json.load(f)

            # Build cache from state file
            for filename, data in state_data.items():
                state = FileDownloadState.from_dict(data)
                self._file_cache[filename] = state

            self.log_writer.info(f"Loaded {len(self._file_cache)} files from state")

        except Exception as e:  # ALLOWED: State loading failure - start fresh, app continues
            # If we can't load state, start fresh
            self.log_writer.warning(f"Could not load state: {e}")

    def _save_state(self) -> None:
        """Save current state to tracking/4icli_state.json."""
        try:
            # Ensure tracking directory exists
            self.state_file.parent.mkdir(parents=True, exist_ok=True)

            # Convert cache to JSON-serializable format
            state_data = {filename: state.to_dict() for filename, state in self._file_cache.items()}

            # Write to state file
            with open(self.state_file, "w") as f:
                json.dump(state_data, f, indent=2)

            self.log_writer.debug(f"Saved state: {len(self._file_cache)} files")

        except Exception as e:  # ALLOWED: State saving failure - log error, app continues
            self.log_writer.error(f"Could not save state: {e}")

    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute SHA256 hash of file content."""
        sha256 = hashlib.sha256()

        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)

        return sha256.hexdigest()

    def _find_file_location(self, filename: str) -> Path | None:
        """
        Search for file across multiple storage locations.

                Search order:
                1. Cached source_path from state (if exists in cache)
                2. Each directory in search_paths

                Returns:
                    Path to file if found, None otherwise
        """
        # First check cached location if we have it
        if filename in self._file_cache:
            cached_path = Path(self._file_cache[filename].source_path)
            if cached_path.exists():
                return cached_path

        # Search in configured directories
        for search_dir in self.search_paths:
            file_path = search_dir / filename
            if file_path.exists():
                return file_path

        return None

    def update_file_location(self, filename: str, new_path: Path) -> None:
        """
        Update state when file is moved to new location.

                Called by processor after moving file to archive or other location.
                Updates source_path and saves state.

                Args:
                    filename: Name of the file
                    new_path: New path to the file
        """
        if filename in self._file_cache:
            old_path = self._file_cache[filename].source_path
            self._file_cache[filename].source_path = str(new_path)
            self._save_state()
            self.log_writer.info(
                f"Updated file location: {filename}",
                old_path=old_path,
                new_path=str(new_path),
            )

    def is_file_downloaded(self, filename: str, file_path: Path | None = None) -> bool:
        """
        Check if a file has already been downloaded.

                Now with multi-location search: if file is not at cached location,
                automatically searches other configured directories and updates state if found.

                Args:
                    filename: Name of the file
                    file_path: Optional path to compute hash for content-based duplicate detection.
                              If None, will search configured directories automatically.

                Returns:
                    True if file was already downloaded, False otherwise
        """
        # Check by filename first
        if filename not in self._file_cache:
            return False

        cached_state = self._file_cache[filename]

        # If no file path provided, try to find the file
        if file_path is None:
            file_path = self._find_file_location(filename)

            # If we found it in a different location than cached, update state
            if file_path and str(file_path) != cached_state.source_path:
                self.update_file_location(filename, file_path)

        # If we have a file path, verify content hasn't changed
        if file_path and file_path.exists():
            file_hash = self._compute_file_hash(file_path)

            # If hash matches, it's a duplicate
            if file_hash == cached_state.file_hash:
                return True

            # Hash doesn't match - file content changed, not a duplicate
            self.log_writer.warning(
                f"File hash mismatch for {filename}: cached vs actual",
                cached_hash=cached_state.file_hash,
                actual_hash=file_hash,
            )
            return False

        # Filename exists in cache but no file found
        # Could mean file was deleted or moved to unknown location
        return True

    def get_downloaded_files(
        self,
        category: str | None = None,
        file_type_code: int | None = None,
    ) -> list[FileDownloadState]:
        """
        Get list of previously downloaded files.

                Args:
                    category: Optional filter by category
                    file_type_code: Optional filter by file type code

                Returns:
                    List of FileDownloadState objects
        """
        results = []

        for state in self._file_cache.values():
            if category and state.category != category:
                continue
            if file_type_code is not None and state.file_type_code != file_type_code:
                continue
            results.append(state)

        return results

    def mark_file_downloaded(
        self,
        file_path: Path,
        category: str,
        file_type_code: int,
    ) -> FileDownloadState:
        """
        Mark a file as downloaded and track its state.

                Args:
                    file_path: Path to the downloaded file
                    category: 4icli category
                    file_type_code: 4icli file type code

                Returns:
                    FileDownloadState object
        """
        # Compute file hash
        file_hash = self._compute_file_hash(file_path)
        file_size = file_path.stat().st_size

        # Create state object
        state = FileDownloadState(
            filename=file_path.name,
            file_hash=file_hash,
            download_timestamp=datetime.now(),
            category=category,
            file_type_code=file_type_code,
            file_size=file_size,
            source_path=str(file_path),
        )

        # Log the download to daily JSONL
        self.log_writer.info("File downloaded", **state.to_dict())

        # Update cache
        self._file_cache[state.filename] = state

        # Save state to tracking/4icli_state.json
        self._save_state()

        return state

    def mark_multiple_downloaded(
        self,
        file_paths: list[Path],
        category: str,
        file_type_code: int,
    ) -> list[FileDownloadState]:
        """
        Mark multiple files as downloaded.

                Args:
                    file_paths: List of paths to downloaded files
                    category: 4icli category
                    file_type_code: 4icli file type code

                Returns:
                    List of FileDownloadState objects
        """
        states = []
        for file_path in file_paths:
            if file_path.is_file():
                state = self.mark_file_downloaded(file_path, category, file_type_code)
                states.append(state)

        return states

    def get_new_files(
        self,
        file_paths: list[Path],
    ) -> list[Path]:
        """
        Filter a list of files to only new (not previously downloaded) files.

                Args:
                    file_paths: List of file paths to check

                Returns:
                    List of file paths that are new (not duplicates)
        """
        new_files = []

        for file_path in file_paths:
            if not file_path.is_file():
                continue

            # Check if already downloaded
            if not self.is_file_downloaded(file_path.name, file_path):
                new_files.append(file_path)

        return new_files

    def get_duplicate_files(
        self,
        file_paths: list[Path],
    ) -> list[Path]:
        """
        Filter a list of files to only duplicates (previously downloaded).

                Args:
                    file_paths: List of file paths to check

                Returns:
                    List of file paths that are duplicates
        """
        duplicates = []

        for file_path in file_paths:
            if not file_path.is_file():
                continue

            # Check if already downloaded
            if self.is_file_downloaded(file_path.name, file_path):
                duplicates.append(file_path)

        return duplicates

    def get_download_stats(self) -> dict[str, any]:
        """
        Get statistics about downloads.

                Returns:
                    Dictionary with download statistics
        """
        if not self._file_cache:
            return {
                "total_files": 0,
                "total_size": 0,
                "categories": {},
                "file_types": {},
            }

        total_size = sum(state.file_size for state in self._file_cache.values())

        # Count by category
        categories: dict[str, int] = {}
        for state in self._file_cache.values():
            categories[state.category] = categories.get(state.category, 0) + 1

        # Count by file type
        file_types: dict[int, int] = {}
        for state in self._file_cache.values():
            file_types[state.file_type_code] = file_types.get(state.file_type_code, 0) + 1

        return {
            "total_files": len(self._file_cache),
            "total_size": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "categories": categories,
            "file_types": file_types,
        }

    def update_remote_inventory(
        self, remote_files: list[dict[str, any]], category: str, file_type_code: int
    ) -> None:
        """
        Update state with files seen in remote datahub -v output.

                This tracks what files exist remotely even if not downloaded yet.

                Args:
                    remote_files: List of file metadata from 4icli datahub -v
                    category: Category these files belong to
                    file_type_code: File type code
        """
        now = datetime.now()

        for remote_file in remote_files:
            filename = remote_file.get("filename") or remote_file.get("name")
            if not filename:
                continue

            # Check if we already have this file
            if filename in self._file_cache:
                # Update last seen time
                self._file_cache[filename].last_seen_remote = now
                self._file_cache[filename].remote_metadata = remote_file
            else:
                # New file we haven't seen before - create placeholder state
                self._file_cache[filename] = FileDownloadState(
                    filename=filename,
                    file_hash="",  # Not downloaded yet
                    download_timestamp=datetime.min,  # Not downloaded yet
                    category=category,
                    file_type_code=file_type_code,
                    file_size=remote_file.get("size", 0),
                    source_path="",  # Not downloaded yet
                    last_seen_remote=now,
                    remote_metadata=remote_file,
                )

        # Save updated state
        self._save_state()
        self.log_writer.info(f"Updated remote inventory: {len(remote_files)} files for {category}")

    def get_files_to_download(
        self, storage_backend_path: Path, category: str | None = None
    ) -> list[str]:
        """
        Get list of files that should be downloaded.

                Compares remote inventory against local storage to find:
                - Files that exist remotely but not locally
                - Files that may have been updated (based on metadata)

                Args:
                    storage_backend_path: Path to check for existing files
                    category: Optional filter by category

                Returns:
                    List of filenames that should be downloaded
        """
        to_download = []

        for filename, state in self._file_cache.items():
            # Filter by category if specified
            if category and state.category != category:
                continue

            # Skip if no remote metadata (not seen in datahub -v)
            if not state.last_seen_remote:
                continue

            # Check if file exists locally
            local_file = storage_backend_path / filename

            # Download if:
            # 1. Never downloaded before (no hash)
            # 2. File doesn't exist in storage
            # 3. Remote metadata suggests it was updated
            if not state.file_hash or not local_file.exists():
                to_download.append(filename)
            elif state.remote_metadata:
                # Check if remote file is newer than our download
                remote_date_str = state.remote_metadata.get("created") or state.remote_metadata.get(
                    "modified"
                )
                if remote_date_str:
                    try:
                        remote_date = datetime.fromisoformat(remote_date_str)
                        if remote_date > state.download_timestamp:
                            to_download.append(filename)
                    except (ValueError, TypeError):
                        pass

        self.log_writer.info(f"Files to download: {len(to_download)}")
        return to_download

    def get_last_sync_time(self, category: str | None = None) -> datetime | None:
        """
        Get the timestamp of the last successful sync.

                Args:
                    category: Optional filter by category

                Returns:
                    DateTime of last sync, or None if never synced
        """
        last_sync = None

        for state in self._file_cache.values():
            if category and state.category != category:
                continue

            if state.last_seen_remote:
                if not last_sync or state.last_seen_remote > last_sync:
                    last_sync = state.last_seen_remote

        return last_sync
