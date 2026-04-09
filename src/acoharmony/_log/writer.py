# © 2025 HarmonyCares
# All rights reserved.

"""
Simple log writer for ACO Harmony.

Provides a unified logging interface that writes to a single log file.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .config import LogConfig, get_logger


class LogWriter:
    """
    Simple writer for all ACO Harmony logs.

        Writes to a single daily log file with JSON entries for structured logging.
    """

    def __init__(self, name: str, config: LogConfig | None = None):
        """
        Initialize log writer.

                Parameters

                name : str
                    Name of this component (e.g., "crosswalk", "enrollment").
                config : LogConfig, optional
                    Logging configuration. Uses default if not provided.
        """
        self.name = name
        self.config = config or LogConfig()
        self.logger = get_logger(name)

        # Use daily log files
        self.log_date = datetime.now().strftime("%Y%m%d")
        self.entries: list[dict[str, Any]] = []

    def _get_log_file(self) -> Path | str:
        """Get the current log file path."""
        base_path = self.config.get_base_path()

        # Use a single daily log file
        filename = f"acoharmony_{self.log_date}.jsonl"

        if isinstance(base_path, str) and base_path.startswith(("s3://", "az://", "gs://")):
            # Cloud storage
            return f"{base_path.rstrip('/')}/{filename}"
        else:
            # Local filesystem
            log_path = Path(base_path)
            log_path.mkdir(parents=True, exist_ok=True)
            return log_path / filename

    def log(self, level: str, message: str, **kwargs):
        """
        Write a log entry.

                Parameters

                level : str
                    Log level (INFO, WARNING, ERROR, DEBUG)
                message : str
                    Log message
                **kwargs
                    Additional structured data to include
        """
        entry = {
            "timestamp": datetime.now().isoformat(),
            "component": self.name,
            "level": level,
            "message": message,
            **kwargs,
        }

        # Write to file immediately (append mode)
        log_file = self._get_log_file()

        if isinstance(log_file, Path):
            # Local filesystem - append to file
            with open(log_file, "a") as f:
                f.write(json.dumps(entry) + "\n")
        else:
            # Cloud storage - batch writes
            self.entries.append(entry)
            if len(self.entries) >= 100:  # Batch size
                self.flush()

        # Also log to Python logger
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(message)

    def info(self, message: str, **kwargs):
        """Log info message."""
        self.log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self.log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message."""
        self.log("ERROR", message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self.log("DEBUG", message, **kwargs)

    def flush(self):
        """Flush any pending entries (for cloud storage)."""
        if not self.entries:
            return

        log_file = self._get_log_file()

        if isinstance(log_file, str) and log_file.startswith(("s3://", "az://", "gs://")):
            # For cloud storage, would need to implement batch upload
            # For now, just clear the buffer
            self.logger.debug(f"Would upload {len(self.entries)} entries to {log_file}")
            self.entries = []

    def write_metadata(self, metadata: dict[str, Any]) -> None:
        """
        Write metadata as a log entry.

                Parameters

                metadata : dict
                    Metadata to log.
        """
        self.info("Metadata", **metadata)

    def add_entry(self, entry: dict[str, Any]):
        """
        Add a structured log entry.

                Parameters

                entry : dict
                    Log entry to add.
        """
        self.info("Entry", **entry)

    def write_session_log(self) -> Path | None:
        """
        Compatibility method - just flushes the log.

                Returns

                Path
                    Path to log file.
        """
        self.flush()
        log_file = self._get_log_file()
        return log_file if isinstance(log_file, Path) else None

    def get_recent_logs(self, limit: int = 100) -> pl.DataFrame:
        """
        Get recent log entries.

                Parameters

                limit : int
                    Number of recent logs to retrieve.

                Returns

                DataFrame
                    Recent log entries.
        """
        log_file = self._get_log_file()

        if not isinstance(log_file, Path) or not log_file.exists():
            return pl.DataFrame()

        # Read last N lines from file
        entries = []
        with open(log_file) as f:
            # Read all lines (could optimize for large files)
            lines = f.readlines()
            for line in lines[-limit:]:
                try:
                    entry = json.loads(line.strip())
                    entries.append(entry)
                except:  # ALLOWED: Continues processing remaining items despite error  # noqa: E722
                    continue

        return pl.DataFrame(entries) if entries else pl.DataFrame()
