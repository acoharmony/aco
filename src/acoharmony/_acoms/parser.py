# © 2025 HarmonyCares
# All rights reserved.

"""Parsers for ACOMS CLI stdout/stderr."""

from __future__ import annotations

import re
from dataclasses import dataclass

from .models import FileTypeDefinition, normalize_category

_AUTH_ERROR_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"Error authenticating", re.IGNORECASE),
    re.compile(r"authentication failed", re.IGNORECASE),
    re.compile(r"unauthorized", re.IGNORECASE),
    re.compile(r"forbidden", re.IGNORECASE),
    re.compile(r"invalid client", re.IGNORECASE),
    re.compile(r"invalid key", re.IGNORECASE),
    re.compile(r"no configuration", re.IGNORECASE),
    re.compile(r"Request failed with status code \d{3}", re.IGNORECASE),
)


def detect_auth_errors(text: str | None) -> list[str]:
    """Return lines that look like ACOMS authentication/configuration failures."""
    if not text:
        return []

    hits: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and any(pattern.search(stripped) for pattern in _AUTH_ERROR_PATTERNS):
            hits.append(stripped)
    return hits


def _parse_size_to_bytes(size_str: str) -> int | None:
    """Convert a size string like ``68.8 MB`` or ``620.1 KB`` to bytes."""
    try:
        match = re.match(r"([\d.]+)\s*(B|KB|MB|GB|TB)", size_str, re.IGNORECASE)
        if not match:
            return None

        value = float(match.group(1))
        unit = match.group(2).upper()
        multipliers = {
            "B": 1,
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4,
        }
        return int(value * multipliers[unit])
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


@dataclass
class ParsedFileEntry:
    """A file line parsed from ACOMS output."""

    filename: str
    size_bytes: int | None = None
    size_str: str | None = None
    last_updated: str | None = None
    position: int | None = None
    total_count: int | None = None


@dataclass
class ParsedCommandOutput:
    """Parsed ACOMS datahub command output."""

    files: list[ParsedFileEntry]
    total_files: int
    session_duration: float | None = None
    raw_output: str | None = None
    errors: list[str] | None = None


def parse_datahub_output(stdout: str, stderr: str | None = None) -> ParsedCommandOutput:
    """Parse ACOMS ``datahub --view`` or ``--download`` output."""
    files: list[ParsedFileEntry] = []
    total_files = 0
    session_duration = None
    errors: list[str] = []

    if not stdout:
        return ParsedCommandOutput(
            files=[],
            total_files=0,
            raw_output=stdout,
            errors=["Empty stdout"],
        )

    auth_hits = detect_auth_errors(stdout)
    if auth_hits:
        errors.extend(f"Authentication error: {hit}" for hit in auth_hits)

    found_match = re.search(r"Found\s+(\d+)\s+files?", stdout, re.IGNORECASE)
    if found_match:
        total_files = int(found_match.group(1))

    duration_match = re.search(r"lasted about ([\d.]+)s", stdout, re.IGNORECASE)
    if duration_match:
        session_duration = float(duration_match.group(1))

    file_pattern = re.compile(
        r"(\d+)\s+of\s+(\d+)\s+-\s+(.+?)"
        r"(?:\s+\((.+?)\))?"
        r"(?:\s+Last Updated:\s+(.+?))?"
        r"(?:\s|$)"
    )

    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("-") or stripped.startswith("="):
            continue

        lowered = stripped.lower()
        if any(
            marker in lowered
            for marker in (
                "acoms-cli",
                "aco-ms cli",
                "found",
                "list of files",
                "session closed",
            )
        ):
            continue

        match = file_pattern.search(stripped)
        if match:
            size_str = match.group(4).strip() if match.group(4) else None
            files.append(
                ParsedFileEntry(
                    filename=match.group(3).strip(),
                    size_bytes=_parse_size_to_bytes(size_str) if size_str else None,
                    size_str=size_str,
                    last_updated=match.group(5).strip() if match.group(5) else None,
                    position=int(match.group(1)),
                    total_count=int(match.group(2)),
                )
            )
        elif " - " in stripped and " of " in stripped:
            filename = stripped.split(" - ", 1)[1].split(" (", 1)[0].strip()
            if filename:
                files.append(ParsedFileEntry(filename=filename))

    if stderr:
        error_lines = [line.strip() for line in stderr.splitlines() if line.strip()]
        errors.extend(error_lines)

    if total_files > 0 and len(files) != total_files:
        errors.append(f"Parsed {len(files)} files but ACOMS reported {total_files} files")

    return ParsedCommandOutput(
        files=files,
        total_files=total_files or len(files),
        session_duration=session_duration,
        raw_output=stdout,
        errors=errors if errors else None,
    )


def _is_catalog_heading(line: str) -> bool:
    lowered = line.strip().lower()
    if not lowered:
        return False
    return not any(
        marker in lowered
        for marker in (
            "acoms-cli",
            "aco-ms cli",
            "list of datahub",
            "usage:",
            "flags:",
        )
    )


def parse_datahub_file_types(stdout: str) -> list[FileTypeDefinition]:
    """Parse ``acoms datahub --list`` into category/name/code definitions."""
    definitions: list[FileTypeDefinition] = []
    current_category: str | None = None
    type_pattern = re.compile(r"^-+\s*(?P<name>.+?),\s*Code\s+(?P<code>\d+)\s*$")

    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped or set(stripped) <= {"-"}:
            continue

        match = type_pattern.match(stripped)
        if match and current_category:
            category = normalize_category(current_category).value
            definitions.append(
                FileTypeDefinition(
                    category=category,
                    name=match.group("name").strip(),
                    code=int(match.group("code")),
                )
            )
            continue

        if stripped.startswith("-"):
            continue

        if _is_catalog_heading(stripped):
            try:
                current_category = normalize_category(stripped).value
            except ValueError:
                current_category = stripped

    return definitions


def extract_filenames(stdout: str) -> list[str]:
    """Extract filenames from ACOMS output."""
    return [file_entry.filename for file_entry in parse_datahub_output(stdout).files]


def extract_file_count(stdout: str) -> int:
    """Extract the reported file count from ACOMS output."""
    match = re.search(r"Found\s+(\d+)\s+files?", stdout, re.IGNORECASE)
    return int(match.group(1)) if match else 0
