# © 2025 HarmonyCares
# All rights reserved.

"""
Mabel SFTP log file parser.

Parses the structured log output from the Mabel document transfer service
(CDS_Folders REACH SVA). The log format consists of sessions delimited by
dashed separator lines with date headers, containing timestamped events
for SFTP connections, authentication, file uploads, and disconnections.

Log Format
==========

Sessions are delimited by separator lines::

    ------------------------------------------------------------------------
    Date : M/D/YYYY H:MM:SS AM/PM
    ------------------------------------------------------------------------
    M/D/YYYY H:MM:SS AM/PM : Event message text.
    ...

Event Types
-----------

- **connection**: ``Connecting to <host> connection type is <type>.``
- **server_key**: ``Server key [<hash>] received.``
- **auth**: ``Authentication type [<type>] used`` / ``Authentication succeeded``
- **protocol**: SFTP version, encryption, MAC, key exchange info
- **folder_check**: ``Is folder exist <path>.``
- **upload**: ``Upload file <source> to <destination>.``
- **disconnect**: ``Disconnect from server <host>.`` / ``SFTP connection closed``

Output Schema
-------------

One row per timestamped log line:

- ``session_id``: Integer session counter (increments per date separator)
- ``session_date``: Session header date as string
- ``timestamp``: Event timestamp as datetime
- ``event_type``: Classified event type (connection, auth, protocol, folder_check, upload, disconnect)
- ``message``: Raw event message text
- ``server``: SFTP server hostname (extracted from connection events)
- ``source_path``: Local file path for upload events
- ``destination_path``: Remote file path for upload events
- ``filename``: Uploaded filename for upload events
"""

import re
from datetime import date, datetime
from pathlib import Path

import polars as pl

from ._registry import register_parser

# Patterns for parsing log lines
_SEPARATOR_PATTERN = re.compile(r"^-{10,}$")
_DATE_HEADER_PATTERN = re.compile(r"^Date\s*:\s*(.+)$")
_TIMESTAMP_LINE_PATTERN = re.compile(r"^(\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)\s*:\s*(.+)$")
_UPLOAD_PATTERN = re.compile(r"Upload file\s+(.+?)\s+to\s+(.+)\.$")
_CONNECT_PATTERN = re.compile(r"Connecting to\s+(\S+)\s+connection type is\s+(.+)\.$")
_TIMESTAMP_FORMAT = "%m/%d/%Y %I:%M:%S %p"

# Patterns for extracting submission date from SVA filenames (ordered by specificity)
_SVA_DATE_PATTERNS = [
    # MM.DD.YYYY (e.g., 03.08.2026)
    re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})"),
    # MM.DDYYYY (e.g., 02.182026) — no separator before year
    re.compile(r"(\d{1,2})\.(\d{2})(\d{4})"),
    # MM.DD.YY (e.g., 03.12.26)
    re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{2})(?!\d)"),
    # M.DD.YYYY with typo prefix (e.g., )3.16.2026)
    re.compile(r"[^0-9]?(\d{1,2})\.(\d{1,2})\.(\d{4})"),
]


def _classify_event(message: str) -> str:
    """Classify a log message into an event type."""
    msg = message.strip()
    if msg.startswith("Connecting to"):
        return "connection"
    if msg.startswith("Server key"):
        return "server_key"
    if msg.startswith("Authentication"):
        return "auth"
    if any(
        msg.startswith(prefix)
        for prefix in ("SFTP version", "Encryption algorithm", "MAC algorithm", "Key exchange", "Public key")
    ):
        return "protocol"
    if msg.startswith("Is folder exist"):
        return "folder_check"
    if msg.startswith("Upload file"):
        return "upload"
    if msg.startswith("Disconnect") or msg.startswith("SFTP connection closed"):
        return "disconnect"
    return "other"


def _extract_submission_date(filename: str | None) -> date | None:
    """Best-effort extraction of submission date from SVA filename variants."""
    if not filename:
        return None
    # Strip .pdf extension for matching
    stem = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    for pattern in _SVA_DATE_PATTERNS:
        m = pattern.search(stem)
        if m:
            month_s, day_s, year_s = m.group(1), m.group(2), m.group(3)
            try:
                month = int(month_s)
                day = int(day_s)
                year = int(year_s)
                # Handle 2-digit year
                if year < 100:
                    year += 2000
                # Sanity check
                if 1 <= month <= 12 and 1 <= day <= 31 and 2020 <= year <= 2030:
                    return date(year, month, day)
            except (ValueError, OverflowError):
                continue
    return None


def _parse_log_lines(file_path: Path, limit: int | None = None) -> list[dict]:
    """Parse Mabel log file into a list of row dicts."""
    rows: list[dict] = []
    session_id = 0
    session_date = ""
    current_server = ""

    with open(file_path, encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n\r")

            # Skip blank lines
            if not line.strip():
                continue

            # Separator line
            if _SEPARATOR_PATTERN.match(line.strip()):
                continue

            # Date header line
            date_match = _DATE_HEADER_PATTERN.match(line.strip())
            if date_match:
                session_id += 1
                session_date = date_match.group(1).strip()
                current_server = ""
                continue

            # Timestamped event line
            ts_match = _TIMESTAMP_LINE_PATTERN.match(line.strip())
            if not ts_match:
                continue

            ts_str = ts_match.group(1)
            message = ts_match.group(2).strip()

            try:
                timestamp = datetime.strptime(ts_str, _TIMESTAMP_FORMAT)
            except ValueError:
                timestamp = None

            event_type = _classify_event(message)

            # Extract connection server
            source_path = None
            destination_path = None
            filename = None

            if event_type == "connection":
                conn_match = _CONNECT_PATTERN.match(message)
                if conn_match:
                    current_server = conn_match.group(1)

            elif event_type == "upload":
                upload_match = _UPLOAD_PATTERN.match(message)
                if upload_match:
                    source_path = upload_match.group(1).strip()
                    destination_path = upload_match.group(2).strip()
                    # Extract filename from destination path
                    filename = destination_path.rsplit("/", 1)[-1] if "/" in destination_path else destination_path

            rows.append(
                {
                    "session_id": session_id,
                    "session_date": session_date,
                    "timestamp": timestamp,
                    "event_type": event_type,
                    "message": message,
                    "server": current_server,
                    "source_path": source_path,
                    "destination_path": destination_path,
                    "filename": filename,
                    "submission_date": _extract_submission_date(filename),
                }
            )

            if limit and len(rows) >= limit:
                break

    return rows


@register_parser("mabel_log")
def parse_mabel_log(
    file_path: Path,
    schema: object = None,
    limit: int | None = None,
    **kwargs,
) -> pl.LazyFrame:
    """
    Parse Mabel SFTP log files into a structured LazyFrame.

    Reads the Mabel log format (session-delimited, timestamped event lines)
    and produces one row per event with session grouping, event classification,
    and extracted upload metadata.

    Args:
        file_path: Path to the Mabel log file
        schema: Optional schema (unused - format is fixed)
        limit: Optional maximum number of event rows to parse
        **kwargs: Additional keyword arguments (ignored)

    Returns:
        pl.LazyFrame with columns: session_id, session_date, timestamp,
        event_type, message, server, source_path, destination_path, filename
    """
    file_path = Path(file_path)
    rows = _parse_log_lines(file_path, limit=limit)

    if not rows:
        return pl.LazyFrame(
            schema={
                "session_id": pl.Int64,
                "session_date": pl.Utf8,
                "timestamp": pl.Datetime,
                "event_type": pl.Utf8,
                "message": pl.Utf8,
                "server": pl.Utf8,
                "source_path": pl.Utf8,
                "destination_path": pl.Utf8,
                "filename": pl.Utf8,
                "submission_date": pl.Date,
            }
        )

    return pl.DataFrame(rows).lazy()
