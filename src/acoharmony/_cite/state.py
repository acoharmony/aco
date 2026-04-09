# © 2025 HarmonyCares
# All rights reserved.

"""
State tracking for citation data processing.

Uses the ACO Harmony LogWriter to track citation processing history and file states,
similar to transform state tracking.

State is persisted in tracking/cite_state.json and logged to daily JSONL files.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .._log import LogWriter


@dataclass
class CitationFileState:
    """State information for a processed citation file."""

    filename: str
    file_hash: str  # SHA256 of file content
    process_timestamp: datetime
    source_type: str  # Type of citation source (e.g., 'pubmed', 'semantic_scholar', 'crossref')
    file_size: int
    source_path: str  # Where it was processed from
    corpus_path: str | None = None  # Where it was written in corpus
    record_count: int | None = None  # Number of citations in file
    last_modified: datetime | None = None  # Last time file was modified
    metadata: dict[str, any] | None = None  # Additional metadata

    def to_dict(self) -> dict[str, any]:
        """Convert to dictionary for logging."""
        return {
            "filename": self.filename,
            "file_hash": self.file_hash,
            "process_timestamp": self.process_timestamp.isoformat(),
            "source_type": self.source_type,
            "file_size": self.file_size,
            "source_path": self.source_path,
            "corpus_path": self.corpus_path,
            "record_count": self.record_count,
            "last_modified": self.last_modified.isoformat() if self.last_modified else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, any]) -> CitationFileState:
        """Create from dictionary."""
        return cls(
            filename=data["filename"],
            file_hash=data["file_hash"],
            process_timestamp=datetime.fromisoformat(data["process_timestamp"]),
            source_type=data["source_type"],
            file_size=data["file_size"],
            source_path=data["source_path"],
            corpus_path=data.get("corpus_path"),
            record_count=data.get("record_count"),
            last_modified=datetime.fromisoformat(data["last_modified"])
            if data.get("last_modified")
            else None,
            metadata=data.get("metadata"),
        )


class CiteStateTracker:
    """
    Tracks citation processing state to prevent duplicate processing.

        Uses LogWriter to persist state information as structured log entries.
    """

    def __init__(
        self,
        log_writer: LogWriter | None = None,
        state_file: Path | None = None,
        search_paths: list[Path] | None = None,
    ):
        """
        Initialize citation state tracker.

                Args:
                    log_writer: LogWriter instance. If None, creates new one with name 'cite'.
                    state_file: Path to state JSON file. If None, uses tracking/cite_state.json
                    search_paths: Directories to search for files. If None, uses cites paths from config.
        """
        self.log_writer = log_writer or LogWriter(name="cite")

        # State file in tracking directory
        if state_file is None:
            from .._store import StorageBackend

            storage = StorageBackend()
            tracking_path = storage.get_path("logs")
            if isinstance(tracking_path, Path):
                self.state_file = tracking_path / "tracking" / "cite_state.json"
            else:
                # For cloud storage, use string path
                self.state_file = Path(tracking_path) / "tracking" / "cite_state.json"

            # Set up search paths from config if not provided
            if search_paths is None:
                cites_raw = storage.get_path("cites/raw")
                cites_corpus = storage.get_path("cites/corpus")
                self.search_paths = [
                    Path(cites_raw) if isinstance(cites_raw, str) else cites_raw,
                    Path(cites_corpus) if isinstance(cites_corpus, str) else cites_corpus,
                ]
            else:
                self.search_paths = search_paths
        else:
            self.state_file = state_file
            self.search_paths = search_paths or []

        self._file_cache: dict[str, CitationFileState] = {}
        self._load_state()

    def _load_state(self) -> None:
        """Load existing state from tracking/cite_state.json."""
        try:
            if not self.state_file.exists():
                self.log_writer.info(f"No state file found at {self.state_file}, starting fresh")
                return

            with open(self.state_file) as f:
                state_data = json.load(f)

            # Build cache from state file
            for filename, data in state_data.items():
                state = CitationFileState.from_dict(data)
                self._file_cache[filename] = state

            self.log_writer.info(f"Loaded {len(self._file_cache)} files from state")

        except Exception as e:  # ALLOWED: State loading failure - start fresh, app continues
            # If we can't load state, start fresh
            self.log_writer.warning(f"Could not load state: {e}")

    def _save_state(self) -> None:
        """Save current state to tracking/cite_state.json."""
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

                Called by processor after moving file to corpus or other location.
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

    def is_file_processed(self, filename: str, file_path: Path | None = None) -> bool:
        """
        Check if a file has already been processed.

                Args:
                    filename: Name of the file
                    file_path: Optional path to compute hash for content-based duplicate detection.
                              If None, will search configured directories automatically.

                Returns:
                    True if file was already processed, False otherwise
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

            # If hash matches, it's already processed
            if file_hash == cached_state.file_hash:
                return True

            # Hash doesn't match - file content changed, not processed yet
            self.log_writer.warning(
                f"File hash mismatch for {filename}: cached vs actual",
                cached_hash=cached_state.file_hash,
                actual_hash=file_hash,
            )
            return False

        # Filename exists in cache but no file found
        # Could mean file was deleted or moved to unknown location
        return True

    def get_processed_files(
        self,
        source_type: str | None = None,
    ) -> list[CitationFileState]:
        """
        Get list of previously processed files.

                Args:
                    source_type: Optional filter by source type

                Returns:
                    List of CitationFileState objects
        """
        results = []

        for state in self._file_cache.values():
            if source_type and state.source_type != source_type:
                continue
            results.append(state)

        return results

    def mark_file_processed(
        self,
        file_path: Path,
        source_type: str,
        corpus_path: Path | None = None,
        record_count: int | None = None,
        metadata: dict[str, any] | None = None,
    ) -> CitationFileState:
        """
        Mark a file as processed and track its state.

                Args:
                    file_path: Path to the processed file
                    source_type: Type of citation source (e.g., 'pubmed', 'semantic_scholar')
                    corpus_path: Optional path where file was written in corpus
                    record_count: Optional number of citations processed
                    metadata: Optional additional metadata

                Returns:
                    CitationFileState object
        """
        # Compute file hash
        file_hash = self._compute_file_hash(file_path)
        file_size = file_path.stat().st_size
        last_modified = datetime.fromtimestamp(file_path.stat().st_mtime)

        # Create state object
        state = CitationFileState(
            filename=file_path.name,
            file_hash=file_hash,
            process_timestamp=datetime.now(),
            source_type=source_type,
            file_size=file_size,
            source_path=str(file_path),
            corpus_path=str(corpus_path) if corpus_path else None,
            record_count=record_count,
            last_modified=last_modified,
            metadata=metadata,
        )

        # Log the processing to daily JSONL
        self.log_writer.info("File processed", **state.to_dict())

        # Update cache
        self._file_cache[state.filename] = state

        # Save state to tracking/cite_state.json
        self._save_state()

        return state

    def mark_multiple_processed(
        self,
        file_paths: list[Path],
        source_type: str,
    ) -> list[CitationFileState]:
        """
        Mark multiple files as processed.

                Args:
                    file_paths: List of paths to processed files
                    source_type: Type of citation source

                Returns:
                    List of CitationFileState objects
        """
        states = []
        for file_path in file_paths:
            if file_path.is_file():
                state = self.mark_file_processed(file_path, source_type)
                states.append(state)

        return states

    def get_new_files(
        self,
        file_paths: list[Path],
    ) -> list[Path]:
        """
        Filter a list of files to only new (not previously processed) files.

                Args:
                    file_paths: List of file paths to check

                Returns:
                    List of file paths that are new (not processed)
        """
        new_files = []

        for file_path in file_paths:
            if not file_path.is_file():
                continue

            # Check if already processed
            if not self.is_file_processed(file_path.name, file_path):
                new_files.append(file_path)

        return new_files

    def get_processing_stats(self) -> dict[str, any]:
        """
        Get statistics about processed files.

                Returns:
                    Dictionary with processing statistics
        """
        if not self._file_cache:
            return {
                "total_files": 0,
                "total_size": 0,
                "total_records": 0,
                "source_types": {},
            }

        total_size = sum(state.file_size for state in self._file_cache.values())
        total_records = sum(
            state.record_count for state in self._file_cache.values() if state.record_count
        )

        # Count by source type
        source_types: dict[str, int] = {}
        for state in self._file_cache.values():
            source_types[state.source_type] = source_types.get(state.source_type, 0) + 1

        return {
            "total_files": len(self._file_cache),
            "total_size": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "total_records": total_records,
            "source_types": source_types,
        }

    def clear_file_status(self, filename: str) -> None:
        """
        Remove a file from tracking.

                This allows the file to be reprocessed as if it was never seen before.

                Args:
                    filename: Name of the file to clear from tracking
        """
        if filename in self._file_cache:
            del self._file_cache[filename]
            self._save_state()
            self.log_writer.info(f"Cleared status for file: {filename}")
