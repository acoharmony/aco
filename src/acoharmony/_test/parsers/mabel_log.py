# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for Mabel SFTP log parser.

Tests parsing of Mabel SFTP transfer log files including session detection,
event classification, and metadata extraction.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import textwrap
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def sample_mabel_log() -> str:
    """Sample Mabel SFTP log content for testing."""
    return """
------------------------------------------------------------------------
Date : 3/15/2026 10:00:00 AM
------------------------------------------------------------------------
3/15/2026 10:00:15 AM : Connecting to sftp.example.com connection type is SFTP.
3/15/2026 10:00:16 AM : Server key [abc123def456] received.
3/15/2026 10:00:17 AM : Authentication type [password] used
3/15/2026 10:00:17 AM : Authentication succeeded
3/15/2026 10:00:18 AM : SFTP version 3 negotiated.
3/15/2026 10:00:19 AM : Encryption algorithm: aes256-ctr
3/15/2026 10:00:20 AM : Is folder exist /remote/path/.
3/15/2026 10:00:21 AM : Upload file C:\\local\\data.csv to /remote/path/data.csv.
3/15/2026 10:00:25 AM : Disconnect from server sftp.example.com.
3/15/2026 10:00:26 AM : SFTP connection closed
------------------------------------------------------------------------
Date : 3/16/2026 2:00:00 PM
------------------------------------------------------------------------
3/16/2026 2:00:10 PM : Connecting to sftp.backup.com connection type is SFTP.
3/16/2026 2:00:15 PM : Upload file C:\\backup\\report.xlsx to /backup/reports/report.xlsx.
3/16/2026 2:00:20 PM : Disconnect from server sftp.backup.com.
"""


@pytest.fixture
def mabel_log_file(tmp_path: Path, sample_mabel_log: str) -> Path:
    """Create a Mabel log file for testing."""
    log_path = tmp_path / "mabel_transfer.log"
    log_path.write_text(sample_mabel_log)
    return log_path


class TestMabelLogParser:
    """Tests for Mabel SFTP log parsing."""

    @pytest.mark.unit
    def test_parse_mabel_log_basic(self, mabel_log_file: Path) -> None:
        """Test basic Mabel log parsing from file."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        result = parse_mabel_log(mabel_log_file)

        # Should return LazyFrame
        assert isinstance(result, pl.LazyFrame)

        # Check schema
        schema = result.collect_schema()
        assert "session_id" in schema
        assert "session_date" in schema
        assert "timestamp" in schema
        assert "event_type" in schema
        assert "message" in schema
        assert "server" in schema
        assert "source_path" in schema
        assert "destination_path" in schema
        assert "filename" in schema

    @pytest.mark.unit
    def test_parse_mabel_log_from_file(self, mabel_log_file: Path) -> None:
        """Test parsing Mabel log from file path."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        result = parse_mabel_log(mabel_log_file)
        df = result.collect()

        # Should have multiple rows (one per event)
        assert len(df) > 0
        assert df["session_id"].min() >= 1

    @pytest.mark.unit
    def test_parse_mabel_log_session_detection(self, mabel_log_file: Path) -> None:
        """Test session ID assignment from date headers."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        result = parse_mabel_log(mabel_log_file)
        df = result.collect()

        # Should have 2 sessions
        unique_sessions = df["session_id"].unique().sort()
        assert len(unique_sessions) == 2
        assert unique_sessions[0] == 1
        assert unique_sessions[1] == 2

    @pytest.mark.unit
    def test_parse_mabel_log_connection_event(self, mabel_log_file: Path) -> None:
        """Test connection event parsing and server extraction."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        result = parse_mabel_log(mabel_log_file)
        df = result.collect()

        # Find connection events
        connection_events = df.filter(pl.col("event_type") == "connection")
        assert len(connection_events) == 2

        # Check server extraction
        session1_connections = connection_events.filter(pl.col("session_id") == 1)
        assert session1_connections["server"][0] == "sftp.example.com"

        session2_connections = connection_events.filter(pl.col("session_id") == 2)
        assert session2_connections["server"][0] == "sftp.backup.com"

    @pytest.mark.unit
    def test_parse_mabel_log_upload_event(self, mabel_log_file: Path) -> None:
        """Test upload event parsing with path extraction."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        result = parse_mabel_log(mabel_log_file)
        df = result.collect()

        # Find upload events
        upload_events = df.filter(pl.col("event_type") == "upload")
        assert len(upload_events) == 2

        # Check first upload
        upload1 = upload_events.filter(pl.col("session_id") == 1)
        assert upload1["source_path"][0] == "C:\\local\\data.csv"
        assert upload1["destination_path"][0] == "/remote/path/data.csv"
        assert upload1["filename"][0] == "data.csv"

        # Check second upload
        upload2 = upload_events.filter(pl.col("session_id") == 2)
        assert upload2["source_path"][0] == "C:\\backup\\report.xlsx"
        assert upload2["destination_path"][0] == "/backup/reports/report.xlsx"
        assert upload2["filename"][0] == "report.xlsx"

    @pytest.mark.unit
    def test_parse_mabel_log_event_classification(self, mabel_log_file: Path) -> None:
        """Test event type classification."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        result = parse_mabel_log(mabel_log_file)
        df = result.collect()

        # Check various event types are detected
        event_types = df["event_type"].unique().sort()
        assert "connection" in event_types
        assert "server_key" in event_types
        assert "auth" in event_types
        assert "protocol" in event_types
        assert "folder_check" in event_types
        assert "upload" in event_types
        assert "disconnect" in event_types

    @pytest.mark.unit
    def test_parse_mabel_log_timestamp_parsing(self, mabel_log_file: Path) -> None:
        """Test timestamp parsing from log lines."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        result = parse_mabel_log(mabel_log_file)
        df = result.collect()

        # All events should have timestamps
        assert df["timestamp"].is_not_null().all()

        # Check timestamp format
        first_timestamp = df["timestamp"][0]
        assert first_timestamp.month == 3
        assert first_timestamp.day == 15
        assert first_timestamp.year == 2026

    @pytest.mark.unit
    def test_parse_mabel_log_server_persistence(self, mabel_log_file: Path) -> None:
        """Test that server name persists across events in a session."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        result = parse_mabel_log(mabel_log_file)
        df = result.collect()

        # All events in session 1 should have same server
        session1 = df.filter(pl.col("session_id") == 1)
        session1_servers = session1["server"].unique()
        assert len(session1_servers) == 1
        assert session1_servers[0] == "sftp.example.com"

    @pytest.mark.unit
    def test_parse_mabel_log_missing_file(self, tmp_path: Path) -> None:
        """Test parsing non-existent log file raises error."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        non_existent = tmp_path / "nonexistent.log"

        with pytest.raises(FileNotFoundError):
            parse_mabel_log(non_existent)

    @pytest.mark.unit
    def test_parse_mabel_log_empty_file(self, tmp_path: Path) -> None:
        """Test parsing empty log file."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        empty_log = tmp_path / "empty.log"
        empty_log.write_text("")

        result = parse_mabel_log(empty_log)
        df = result.collect()

        # Should return empty dataframe with correct schema
        assert len(df) == 0
        assert "session_id" in df.collect_schema()


class TestMabelLogCoverageGaps:
    """Additional tests for _mabel_log coverage gaps."""

    @pytest.mark.unit
    def test_extract_submission_date_invalid_month(self):
        """Cover invalid month/day that raises ValueError."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        assert _extract_submission_date("SVA_13.32.2026.pdf") is None

    @pytest.mark.unit
    def test_parse_log_invalid_timestamp(self, tmp_path):
        """Cover invalid timestamp parsing (line 164-165)."""
        from acoharmony._parsers._mabel_log import _parse_log_lines

        p = tmp_path / "mabel.log"
        content = textwrap.dedent(
            "            ------------------------------------------------------------------------\n            Date : 3/8/2026 10:00:00 AM\n            ------------------------------------------------------------------------\n            99/99/9999 10:00:00 AM : Some event message.\n        "
        )
        p.write_text(content)
        rows = _parse_log_lines(p)
        assert len(rows) == 1
        assert rows[0]["timestamp"] is None

    @pytest.mark.unit
    def test_extract_submission_date_overflow(self):
        """Cover OverflowError in date construction."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        assert _extract_submission_date("SVA_01.01.0001.pdf") is None

    @pytest.mark.unit
    def test_extract_submission_date_invalid_day_for_month(self):
        """Cover ValueError: day is out of range for month (e.g., Feb 30)."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        result = _extract_submission_date("SVA_02.30.2026.pdf")
        assert result is None

class TestMabelLog:
    """Tests for acoharmony._parsers._mabel_log."""

    SAMPLE_LOG = textwrap.dedent(
        "        ------------------------------------------------------------------------\n        Date : 3/8/2026 10:00:00 AM\n        ------------------------------------------------------------------------\n        3/8/2026 10:00:01 AM : Connecting to sftp.example.com connection type is SFTP.\n        3/8/2026 10:00:02 AM : Server key [abc123] received.\n        3/8/2026 10:00:03 AM : Authentication type [password] used.\n        3/8/2026 10:00:04 AM : SFTP version 3 ready.\n        3/8/2026 10:00:05 AM : Is folder exist /remote/path.\n        3/8/2026 10:00:06 AM : Upload file C:\\local\\file.pdf to /remote/path/file.pdf.\n        3/8/2026 10:00:07 AM : Disconnect from server sftp.example.com.\n        3/8/2026 10:00:08 AM : SFTP connection closed.\n    "
    )

    @pytest.mark.unit
    def test_classify_event(self):
        from acoharmony._parsers._mabel_log import _classify_event

        assert _classify_event("Connecting to server.com connection type is SFTP.") == "connection"
        assert _classify_event("Server key [abc] received.") == "server_key"
        assert _classify_event("Authentication type [password] used") == "auth"
        assert _classify_event("Authentication succeeded") == "auth"
        assert _classify_event("SFTP version 3 ready.") == "protocol"
        assert _classify_event("Encryption algorithm AES-256.") == "protocol"
        assert _classify_event("MAC algorithm hmac-sha256.") == "protocol"
        assert _classify_event("Key exchange algorithm dh-group14.") == "protocol"
        assert _classify_event("Public key RSA-2048.") == "protocol"
        assert _classify_event("Is folder exist /path.") == "folder_check"
        assert _classify_event("Upload file src to dest.") == "upload"
        assert _classify_event("Disconnect from server host.") == "disconnect"
        assert _classify_event("SFTP connection closed") == "disconnect"
        assert _classify_event("Something else entirely") == "other"

    @pytest.mark.unit
    def test_extract_submission_date_none(self):
        from acoharmony._parsers._mabel_log import _extract_submission_date

        assert _extract_submission_date(None) is None
        assert _extract_submission_date("") is None

    @pytest.mark.unit
    def test_extract_submission_date_mm_dd_yyyy(self):
        from acoharmony._parsers._mabel_log import _extract_submission_date

        result = _extract_submission_date("SVA_03.08.2026.pdf")
        assert result == date(2026, 3, 8)

    @pytest.mark.unit
    def test_extract_submission_date_mm_ddyyyy(self):
        from acoharmony._parsers._mabel_log import _extract_submission_date

        result = _extract_submission_date("SVA_02.182026.pdf")
        assert result == date(2026, 2, 18)

    @pytest.mark.unit
    def test_extract_submission_date_mm_dd_yy(self):
        from acoharmony._parsers._mabel_log import _extract_submission_date

        result = _extract_submission_date("SVA_03.12.26.pdf")
        assert result == date(2026, 3, 12)

    @pytest.mark.unit
    def test_extract_submission_date_typo_prefix(self):
        from acoharmony._parsers._mabel_log import _extract_submission_date

        result = _extract_submission_date(")3.16.2026.pdf")
        assert result == date(2026, 3, 16)

    @pytest.mark.unit
    def test_extract_submission_date_invalid(self):
        from acoharmony._parsers._mabel_log import _extract_submission_date

        assert _extract_submission_date("no_date_here.pdf") is None

    @pytest.mark.unit
    def test_extract_submission_date_out_of_range_year(self):
        from acoharmony._parsers._mabel_log import _extract_submission_date

        assert _extract_submission_date("SVA_03.08.2019.pdf") is None

    @pytest.mark.unit
    def test_parse_log_lines(self, tmp_path):
        from acoharmony._parsers._mabel_log import _parse_log_lines

        p = tmp_path / "mabel.log"
        p.write_text(self.SAMPLE_LOG)
        rows = _parse_log_lines(p)
        assert len(rows) == 8
        assert rows[0]["event_type"] == "connection"
        assert rows[0]["server"] == "sftp.example.com"
        assert rows[5]["event_type"] == "upload"
        assert rows[5]["filename"] == "file.pdf"

    @pytest.mark.unit
    def test_parse_log_lines_limit(self, tmp_path):
        from acoharmony._parsers._mabel_log import _parse_log_lines

        p = tmp_path / "mabel.log"
        p.write_text(self.SAMPLE_LOG)
        rows = _parse_log_lines(p, limit=3)
        assert len(rows) == 3

    @pytest.mark.unit
    def test_parse_mabel_log(self, tmp_path):
        from acoharmony._parsers._mabel_log import parse_mabel_log

        p = tmp_path / "mabel.log"
        p.write_text(self.SAMPLE_LOG)
        df = parse_mabel_log(p).collect()
        assert df.height == 8
        assert "session_id" in df.columns
        assert "event_type" in df.columns

    @pytest.mark.unit
    def test_parse_mabel_log_empty(self, tmp_path):
        from acoharmony._parsers._mabel_log import parse_mabel_log

        p = tmp_path / "mabel.log"
        p.write_text("")
        df = parse_mabel_log(p).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_parse_log_lines_blank_lines(self, tmp_path):
        from acoharmony._parsers._mabel_log import _parse_log_lines

        p = tmp_path / "mabel.log"
        p.write_text("\n\n\n")
        rows = _parse_log_lines(p)
        assert len(rows) == 0

    @pytest.mark.unit
    def test_parse_log_lines_non_matching_lines(self, tmp_path):
        from acoharmony._parsers._mabel_log import _parse_log_lines

        p = tmp_path / "mabel.log"
        p.write_text("This is not a log line\nNeither is this\n")
        rows = _parse_log_lines(p)
        assert len(rows) == 0

    @pytest.mark.unit
    def test_parse_log_upload_no_slash(self, tmp_path):
        from acoharmony._parsers._mabel_log import _parse_log_lines

        p = tmp_path / "mabel.log"
        content = textwrap.dedent(
            "            ------------------------------------------------------------------------\n            Date : 3/8/2026 10:00:00 AM\n            ------------------------------------------------------------------------\n            3/8/2026 10:00:06 AM : Upload file C:\\local\\file.pdf to file.pdf.\n        "
        )
        p.write_text(content)
        rows = _parse_log_lines(p)
        assert len(rows) == 1
        assert rows[0]["filename"] == "file.pdf"

    @pytest.mark.unit
    def test_connection_event_no_regex_match(self, tmp_path):
        """Cover branch 176->187: connection event where _CONNECT_PATTERN fails to match."""
        from acoharmony._parsers._mabel_log import _parse_log_lines

        p = tmp_path / "mabel.log"
        content = textwrap.dedent(
            "            ------------------------------------------------------------------------\n            Date : 3/8/2026 10:00:00 AM\n            ------------------------------------------------------------------------\n            3/8/2026 10:00:01 AM : Connecting to somewhere but malformed\n        "
        )
        p.write_text(content)
        rows = _parse_log_lines(p)
        assert len(rows) == 1
        assert rows[0]["event_type"] == "connection"
        assert rows[0]["server"] == ""

    @pytest.mark.unit
    def test_upload_event_no_regex_match(self, tmp_path):
        """Cover branch 181->187: upload event where _UPLOAD_PATTERN fails to match."""
        from acoharmony._parsers._mabel_log import _parse_log_lines

        p = tmp_path / "mabel.log"
        content = textwrap.dedent(
            "            ------------------------------------------------------------------------\n            Date : 3/8/2026 10:00:00 AM\n            ------------------------------------------------------------------------\n            3/8/2026 10:00:06 AM : Upload file but malformed entry\n        "
        )
        p.write_text(content)
        rows = _parse_log_lines(p)
        assert len(rows) == 1
        assert rows[0]["event_type"] == "upload"
        assert rows[0]["source_path"] is None
        assert rows[0]["destination_path"] is None
        assert rows[0]["filename"] is None


class TestSvaDateMalformedVariants:
    """
    Real-world malformed SVA filenames — submitters hand-enter dates and
    make predictable mistakes. The extractor must use the upload timestamp
    as context to disambiguate, because SVAs are uploaded within a few days
    of submission.
    """

    @pytest.mark.unit
    def test_concatenated_mmddyyyy(self):
        """SVA 03302026.pdf → Mar 30, 2026 (no separators at all)."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 3, 30, 18, 15, 18)
        assert _extract_submission_date("Erica Milam SVA 03302026.pdf", uploaded) == date(2026, 3, 30)

    @pytest.mark.unit
    def test_concatenated_mmddyyyy_april(self):
        """SVA 04012026.pdf → Apr 1, 2026."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 4, 1, 10, 15, 35)
        assert _extract_submission_date("Edward Campbell SVA 04012026.pdf", uploaded) == date(2026, 4, 1)

    @pytest.mark.unit
    def test_space_separated_mm_dd_yy(self):
        """SVA 03 26 26.pdf → Mar 26, 2026 (spaces instead of dots, 2-digit year)."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 4, 1, 10, 15, 13)
        assert _extract_submission_date("Anne L Scarce SVA 03 26 26.pdf", uploaded) == date(2026, 3, 26)

    @pytest.mark.unit
    def test_space_separated_mm_dd_yyyy(self):
        """SVA 03 30 2026.pdf → Mar 30, 2026 (spaces, 4-digit year)."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 4, 1, 10, 15, 19)
        assert _extract_submission_date("Carol Koeppel SVA 03 30 2026.pdf", uploaded) == date(2026, 3, 30)

    @pytest.mark.unit
    def test_single_dot_dropped_digit(self):
        """
        SVA 03.19026.pdf → Mar 19, 2026.
        The user typed ``03.192026`` but dropped one '2', leaving '19026'.
        Upload date of Mar 19 2026 confirms Mar 19 interpretation.
        """
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 3, 19, 10, 15, 11)
        assert _extract_submission_date("Josephine Fernandez SVA 03.19026.pdf", uploaded) == date(2026, 3, 19)

    @pytest.mark.unit
    def test_single_dot_transposed_year_digit(self):
        """SVA 03.20226.pdf → Mar 20, 2026 (user typed 20 then '226' for '2026')."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 3, 20, 18, 15, 15)
        assert _extract_submission_date("Christy C Christman SVA 03.20226.pdf", uploaded) == date(2026, 3, 20)

    @pytest.mark.unit
    def test_concatenated_nospace_typo(self):
        """SVA 023262026.pdf → Feb 26, 2026 (9-digit blob, MMDDDYYYY with extra digit)."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 2, 26, 9, 15, 9)
        assert _extract_submission_date("Judith Devine SVA 023262026.pdf", uploaded) == date(2026, 2, 26)

    @pytest.mark.unit
    def test_leading_dot_missing_month(self):
        """
        SVA .162026.pdf → Mar 16, 2026.
        Month glyph missing; use upload month to fill. Day 16, year 2026 are explicit.
        """
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 3, 16, 18, 15, 40)
        assert _extract_submission_date("Roberta Smith SVA .162026.pdf", uploaded) == date(2026, 3, 16)

    @pytest.mark.unit
    def test_truncated_month_only_double_dot(self):
        """SVA 04..pdf → fall back to uploaded_at date (only partial date present)."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 4, 6, 18, 15, 49)
        assert _extract_submission_date("Ruth L Vanderstelt SVA 04..pdf", uploaded) == date(2026, 4, 6)

    @pytest.mark.unit
    def test_truncated_month_only_single_dot(self):
        """SVA 04.pdf → fall back to uploaded_at date."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 4, 13, 18, 15, 28)
        result = _extract_submission_date("Mary Emerson Scheel SVA 04.pdf", uploaded)
        assert result == date(2026, 4, 13)

    @pytest.mark.unit
    def test_missing_sva_keyword_but_has_date(self):
        """LORRIE MITCHELL 04 07 26.pdf → Apr 7, 2026 (no SVA keyword, still has date)."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 4, 8, 10, 15, 22)
        assert _extract_submission_date("LORRIE MITCHELL 04 07 26.pdf", uploaded) == date(2026, 4, 7)

    @pytest.mark.unit
    def test_invalid_dob_year_falls_back_to_upload(self):
        """
        SVA 05.21.1955.pdf — user keyed a DOB instead of submission date.
        Year 1955 is outside plausible window; fall back to upload date.
        """
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 3, 11, 18, 15, 16)
        assert _extract_submission_date("GEORGIA JONES SVA 05.21.1955.pdf", uploaded) == date(2026, 3, 11)

    @pytest.mark.unit
    def test_invalid_day_37_falls_back_to_upload(self):
        """SVA 03.372026.pdf — day 37 is impossible; fall back to upload date."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 3, 27, 18, 15, 16)
        assert _extract_submission_date("Patricia Howell SVA 03.372026.pdf", uploaded) == date(2026, 3, 27)

    @pytest.mark.unit
    def test_no_sva_marker_no_date(self):
        """
        Cabb.pdf — last-name-only, no SVA, no date. Unparseable; return None.
        These are the legitimately "unresolved" cases.
        """
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 3, 17, 10, 15, 12)
        assert _extract_submission_date("Cabb.pdf", uploaded) is None

    @pytest.mark.unit
    def test_no_sva_marker_no_date_returns_none(self):
        """Pongratz.pdf — no SVA marker, nothing to infer."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 3, 18, 10, 15, 17)
        assert _extract_submission_date("Pongratz.pdf", uploaded) is None

    @pytest.mark.unit
    def test_standard_mm_dd_yyyy_still_works(self):
        """Regression: well-formed MM.DD.YYYY must still parse correctly."""
        from acoharmony._parsers._mabel_log import _extract_submission_date

        uploaded = datetime(2026, 3, 8, 10, 0, 0)
        assert _extract_submission_date("HELEN BILLINGS SVA 03.08.2026.pdf", uploaded) == date(2026, 3, 8)

    @pytest.mark.unit
    def test_backcompat_no_uploaded_at_arg(self):
        """
        Calling without uploaded_at must still work (preserves old signature for callers).
        Well-formed dates parse; ambiguous/malformed ones return None without context.
        """
        from acoharmony._parsers._mabel_log import _extract_submission_date

        assert _extract_submission_date("SVA_03.08.2026.pdf") == date(2026, 3, 8)
        assert _extract_submission_date("Cabb.pdf") is None
