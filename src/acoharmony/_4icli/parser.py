# © 2025 HarmonyCares
# All rights reserved.

"""
4icli stdout/stderr parser module.

Provides robust, modular, and extensible parsing of 4icli command outputs.
This module centralizes all parsing logic for 4icli commands to ensure
consistency and maintainability.
"""

import re
from dataclasses import dataclass


def _parse_size_to_bytes(size_str: str) -> int | None:
    """
    Convert size string like '64.66 MB' to bytes.

        Args:
            size_str: Size string from 4icli output (e.g., "64.66 MB", "7.77 KB")

        Returns:
            Size in bytes, or None if parsing fails
    """
    try:
        # Match pattern like "64.66 MB"
        match = re.match(r"([\d.]+)\s*(KB|MB|GB|TB)", size_str, re.IGNORECASE)
        if not match:
            return None

        value = float(match.group(1))
        unit = match.group(2).upper()

        multipliers = {
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4,
        }

        return int(value * multipliers.get(unit, 1))
    except (ValueError, AttributeError):  # ALLOWED: Returns None to indicate error
        return None


@dataclass
class ParsedFileEntry:
    """Represents a parsed file entry from 4icli output."""

    filename: str
    size_bytes: int | None = None
    size_str: str | None = None
    last_updated: str | None = None
    position: int | None = None  # Position in list (e.g., "1" of "1 of 10")
    total_count: int | None = None  # Total count (e.g., "10" of "1 of 10")


@dataclass
class ParsedCommandOutput:
    """Complete parsed output from a 4icli command."""

    files: list[ParsedFileEntry]
    total_files: int
    session_duration: float | None = None  # Duration in seconds
    raw_output: str | None = None
    errors: list[str] | None = None


def parse_datahub_output(stdout: str, stderr: str | None = None) -> ParsedCommandOutput:
    """
    Parse stdout from 4icli datahub commands (view or download).

        This function is idempotent and handles various output formats robustly.

        Expected format:
            4icli - 4Innovation CLI

            Found 87 files.
            List of Files
            1 of 87 - REACH.D0259.PAER.PY2025.D241111.T1051370.xlsx (6.57 KB) Last Updated: 2024-11-18T19:26:50.000Z
            2 of 87 - P.D0259.ACO.ZCY25.D250210.T1550060.zip (64.66 MB) Last Updated: 2025-02-10T21:47:21.000Z
            87 of 87 - P.D0259.TPARC.RP.D251025.T2136026.txt (2.38 KB) Last Updated: 2025-10-26T02:17:07.000Z

            Session closed, lasted about 4.4s.

        Args:
            stdout: Standard output from 4icli command
            stderr: Optional standard error output

        Returns:
            ParsedCommandOutput with extracted file information

        Note:
            This parser is bulletproof and handles:
            - Missing fields (size, timestamp)
            - Malformed lines
            - Different line formats
            - Empty output
            - Error messages
    """
    files = []
    total_files = 0
    session_duration = None
    errors = []

    if not stdout:
        return ParsedCommandOutput(
            files=[],
            total_files=0,
            raw_output=stdout,
            errors=["Empty stdout"],
        )

    # Extract total file count from "Found X files" line
    found_match = re.search(r"Found (\d+) files?", stdout)
    if found_match:
        total_files = int(found_match.group(1))

    # Extract session duration from "Session closed, lasted about Xs" line
    duration_match = re.search(r"lasted about ([\d.]+)s", stdout)
    if duration_match:
        session_duration = float(duration_match.group(1))

    # Parse file entries
    # Pattern: "{num} of {total} - {filename} ({size}) Last Updated: {timestamp}"
    # More flexible pattern that handles optional parts
    file_pattern = re.compile(
        r"(\d+)\s+of\s+(\d+)\s+-\s+(.+?)(?:\s+\((.+?)\))?(?:\s+Last Updated:\s+(.+?))?(?:\s|$)"
    )

    for line in stdout.splitlines():
        line = line.strip()

        # Skip empty lines, headers, separators
        if not line or line.startswith("-") or line.startswith("="):
            continue

        # Skip known non-file lines
        if any(
            keyword in line.lower()
            for keyword in [
                "4icli",
                "4innovation",
                "found",
                "list of files",
                "session closed",
            ]
        ):
            continue

        # Try primary pattern match
        match = file_pattern.search(line)
        if match:
            position = int(match.group(1))
            total = int(match.group(2))
            filename = match.group(3).strip()
            size_str = match.group(4).strip() if match.group(4) else None
            last_updated = match.group(5).strip() if match.group(5) else None

            files.append(
                ParsedFileEntry(
                    filename=filename,
                    size_bytes=_parse_size_to_bytes(size_str) if size_str else None,
                    size_str=size_str,
                    last_updated=last_updated,
                    position=position,
                    total_count=total,
                )
            )
        elif " - " in line and " of " in line:
            # Fallback: try simpler parsing for malformed lines
            # split(" - ", 1) always yields 2 parts since " - " is known to be in line
            filename_part = line.split(" - ", 1)[1].split(" (")[0].strip()
            if filename_part:
                files.append(
                    ParsedFileEntry(
                        filename=filename_part,
                        size_bytes=None,
                        last_updated=None,
                    )
                )

    # Check stderr for errors
    if stderr:
        error_lines = [line.strip() for line in stderr.splitlines() if line.strip()]
        if error_lines:
            errors.extend(error_lines)

    # Validate parsed data
    if total_files > 0 and len(files) != total_files:
        errors.append(f"Parsed {len(files)} files but 4icli reported {total_files} files")

    return ParsedCommandOutput(
        files=files,
        total_files=total_files or len(files),
        session_duration=session_duration,
        raw_output=stdout,
        errors=errors if errors else None,
    )


def extract_filenames(stdout: str) -> list[str]:
    """
    Quick extraction of filenames from stdout without full parsing.

        Use this for simple cases where only filenames are needed.

        Args:
            stdout: Standard output from 4icli command

        Returns:
            List of filenames
    """
    parsed = parse_datahub_output(stdout)
    return [f.filename for f in parsed.files]


def extract_file_count(stdout: str) -> int:
    """
    Extract total file count from stdout.

        Args:
            stdout: Standard output from 4icli command

        Returns:
            Total file count reported by 4icli
    """
    match = re.search(r"Found (\d+) files?", stdout)
    return int(match.group(1)) if match else 0
