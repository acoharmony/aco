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

# Patterns for extracting submission date from SVA filenames.
# Each returns (month, day, year) groups. Order = tried first-to-last, but all
# candidates are scored and the best match near the upload date wins.
_SVA_DATE_PATTERNS: list[re.Pattern[str]] = [
    # MM.DD.YYYY (e.g., 03.08.2026)
    re.compile(r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{4})(?!\d)"),
    # MM.DDYYYY — no separator before year (e.g., 02.182026)
    re.compile(r"(?<!\d)(\d{1,2})\.(\d{2})(\d{4})(?!\d)"),
    # MM.DD.YY — 2-digit year (e.g., 03.12.26)
    re.compile(r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{2})(?!\d)"),
    # MM DD YYYY — space-separated, 4-digit year (e.g., SVA 03 30 2026)
    re.compile(r"(?<!\d)(\d{1,2})\s+(\d{1,2})\s+(\d{4})(?!\d)"),
    # MM DD YY — space-separated, 2-digit year (e.g., SVA 03 26 26)
    re.compile(r"(?<!\d)(\d{1,2})\s+(\d{1,2})\s+(\d{2})(?!\d)"),
    # MMDDYYYY — fully concatenated 8-digit blob (e.g., SVA 03302026)
    re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{4})(?!\d)"),
]

# SVA submission window: a submission date is plausible if it falls within
# this many days before the upload, and never more than a day after.
_MAX_DAYS_BEFORE_UPLOAD = 60
_MAX_DAYS_AFTER_UPLOAD = 1


def _is_sva_filename(filename: str) -> bool:
    """True if the filename follows SVA naming convention."""
    return "sva" in filename.lower()


def _score_candidate(candidate: date, uploaded_at: datetime | None) -> float:
    """
    Lower score = better. Scores measure distance (in days) from the upload
    date, with asymmetric penalty: submissions happen *before* upload, so
    dates after the upload are heavily penalized.
    """
    if uploaded_at is None:
        return 0.0
    delta = (uploaded_at.date() - candidate).days
    if delta < 0:
        return abs(delta) * 100.0  # Future dates: very unlikely
    return float(delta)


def _candidate_is_plausible(candidate: date, uploaded_at: datetime | None) -> bool:
    """Validity window relative to upload timestamp."""
    if uploaded_at is None:
        return 2020 <= candidate.year <= 2030
    delta = (uploaded_at.date() - candidate).days
    return -_MAX_DAYS_AFTER_UPLOAD <= delta <= _MAX_DAYS_BEFORE_UPLOAD


def _build_candidates(stem: str) -> list[tuple[int, int, int]]:
    """
    Generate (month, day, year) candidates from a filename stem by running
    each pattern and also trying common malformed-input recoveries:

    - Single-dot ``MM.DDDYY`` style (e.g. ``03.19026``): interpret as
      ``MM.DD`` followed by year fragment ``YY`` or ``YYY``, padding to 20YY.
    - Leading-dot ``.DDYYYY`` (missing month glyph): extract DDYYYY and
      leave month to be filled by the caller from uploaded_at.
    - Partial ``MM.`` or ``MM..`` (only month glyph present): caller falls
      back to uploaded_at entirely.
    """
    candidates: list[tuple[int, int, int]] = []
    seen: set[tuple[int, int, int]] = set()

    def _push(m: int, d: int, y: int) -> None:
        if y < 100:
            y += 2000
        key = (m, d, y)
        if key in seen:
            return
        seen.add(key)
        candidates.append(key)

    for pattern in _SVA_DATE_PATTERNS:
        for match in pattern.finditer(stem):
            try:
                _push(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            except (ValueError, IndexError):
                continue

    # Typo recovery: MM.NNNNN where the year is short by one or two digits.
    # Examples:
    #   03.19026  → month=3, day=19, year-fragment="026"   → 2026
    #   03.20226  → month=3, day=20, year-fragment="226"   → 2026 (middle digit typo)
    for match in re.finditer(r"(?<!\d)(\d{1,2})\.(\d{2})(\d{3})(?!\d)", stem):
        month = int(match.group(1))
        day = int(match.group(2))
        frag = match.group(3)
        # "026" → 2026; "226" → 2026 (drop leading transposed digit)
        if frag.startswith("0"):
            year = 2000 + int(frag[1:])
        else:
            year = 2000 + int(frag[-2:])
        _push(month, day, year)

    return candidates


def _extract_submission_date(filename: str | None, uploaded_at: datetime | None = None) -> date | None:
    """
    Best-effort extraction of the SVA submission date from a filename.

    Humans typing SVA filenames make predictable mistakes: dropped digits,
    spaces instead of dots, concatenated dates, transposed year digits,
    month-only truncations. This function generates candidate dates from
    multiple patterns and picks the one closest to (but not after) the
    upload timestamp, which is a strong contextual signal.

    If the filename contains an SVA marker but no parseable date, and an
    upload timestamp is available, falls back to the upload date — SVAs
    are transferred on the same day or the next business day, so the
    upload date is a reasonable proxy.

    Filenames with neither an SVA marker nor any date-looking digits
    (e.g. ``Cabb.pdf``) return ``None`` — those are legitimate unresolved.
    """
    if not filename:
        return None

    stem = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    has_sva = _is_sva_filename(stem)

    candidates = _build_candidates(stem)

    # Leading-dot "missing month" recovery: SVA .162026 → use upload month
    # with day=16, year=2026.
    if uploaded_at is not None:
        for match in re.finditer(r"(?<![\d.])\.(\d{2})(\d{4})(?!\d)", stem):
            day = int(match.group(1))
            year = int(match.group(2))
            candidates.append((uploaded_at.month, day, year))

    valid: list[tuple[float, date]] = []
    for month, day, year in candidates:
        try:
            cand = date(year, month, day)
        except (ValueError, OverflowError):
            continue
        if not _candidate_is_plausible(cand, uploaded_at):
            continue
        valid.append((_score_candidate(cand, uploaded_at), cand))

    if valid:
        valid.sort(key=lambda x: x[0])
        return valid[0][1]

    # Fallback: SVA-marked file with no salvageable date → use upload date.
    # Only when there's a partial date hint (dot or digits) or the SVA marker
    # is present, so we don't claim dates for truly naked filenames.
    if uploaded_at is not None and has_sva:
        return uploaded_at.date()

    return None


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
                    "submission_date": _extract_submission_date(filename, timestamp),
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
