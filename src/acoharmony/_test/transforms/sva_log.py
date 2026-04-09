"""Tests for _transforms.sva_log module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date, datetime  # noqa: F811

import polars as pl
import pytest
import acoharmony


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestSvaLogTransform:
    """Tests for SVA Log transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._sva_log is not None

    @pytest.mark.unit
    def test_session_summary_func(self):
        assert callable(build_session_summary)

    @pytest.mark.unit
    def test_upload_detail_func(self):
        assert callable(build_upload_detail)


class TestSvaLogTransformV2:
    """Tests for SVA log session/upload/daily summary transforms."""

    def _make_log_data(self):
        return pl.DataFrame({
            "session_id": [1, 1, 1, 2, 2],
            "session_date": ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-16", "2025-01-16"],
            "server": ["sftp.example.com"] * 5,
            "timestamp": [
                datetime.datetime(2025, 1, 15, 10, 0, 0),
                datetime.datetime(2025, 1, 15, 10, 5, 0),
                datetime.datetime(2025, 1, 15, 10, 10, 0),
                datetime.datetime(2025, 1, 16, 9, 0, 0),
                datetime.datetime(2025, 1, 16, 9, 3, 0),
            ],
            "event_type": ["auth", "upload", "disconnect", "auth", "upload"],
            "source_path": [None, "/local/file1.pdf", None, None, "/local/file2.pdf"],
            "destination_path": [None, "/remote/file1.pdf", None, None, "/remote/file2.pdf"],
            "filename": [None, "SVA_Smith_John_20250115.pdf", None, None, "report.pdf"],
            "submission_date": [None, "20250115", None, None, None],
        }).lazy()


class TestBuildSessionSummary:
    """Tests for build_session_summary transform."""

    def _make_log_df(self):
        return pl.DataFrame({
            "session_id": [1, 1, 1, 2, 2],
            "session_date": ["2026-03-01", "2026-03-01", "2026-03-01", "2026-03-02", "2026-03-02"],
            "server": ["sftp.example.com"] * 5,
            "timestamp": [
                datetime(2026, 3, 1, 10, 0, 0),
                datetime(2026, 3, 1, 10, 5, 0),
                datetime(2026, 3, 1, 10, 10, 0),
                datetime(2026, 3, 2, 14, 0, 0),
                datetime(2026, 3, 2, 14, 1, 0),
            ],
            "event_type": ["connection", "upload", "disconnect", "connection", "upload"],
            "message": [
                "Authentication succeeded",
                "File uploaded",
                "SFTP connection closed",
                "Authentication succeeded",
                "File uploaded",
            ],
            "filename": [None, "John Doe SVA 03.012026.pdf", None, None, "Jane Smith SVA 03.022026.pdf"],
            "source_path": [None, "/local/file", None, None, "/local/file2"],
            "destination_path": [None, "/remote/file", None, None, "/remote/file2"],
            "submission_date": [None, date(2026, 3, 1), None, None, date(2026, 3, 2)],
        }).lazy()

    @pytest.mark.unit
    def test_session_summary_basic(self):
        lf = self._make_log_df()
        result = build_session_summary(lf).collect()
        assert result.height == 2
        assert "session_id" in result.columns
        assert "files_uploaded" in result.columns
        assert "auth_succeeded" in result.columns
        assert "disconnected_cleanly" in result.columns
        assert "event_count" in result.columns
        # Session 1 had 1 upload
        s1 = result.filter(pl.col("session_id") == 1)
        assert s1["files_uploaded"][0] == 1
        assert s1["auth_succeeded"][0] is True
        assert s1["disconnected_cleanly"][0] is True
        assert s1["event_count"][0] == 3

    @pytest.mark.unit
    def test_session_summary_no_clean_disconnect(self):
        lf = pl.DataFrame({
            "session_id": [1, 1],
            "session_date": ["2026-03-01", "2026-03-01"],
            "server": ["sftp.example.com", "sftp.example.com"],
            "timestamp": [datetime(2026, 3, 1, 10, 0), datetime(2026, 3, 1, 10, 5)],
            "event_type": ["connection", "upload"],
            "message": ["Authentication succeeded", "File uploaded"],
            "filename": [None, "test.pdf"],
            "source_path": [None, "/local"],
            "destination_path": [None, "/remote"],
            "submission_date": [None, None],
        }).lazy()
        result = build_session_summary(lf).collect()
        assert result["disconnected_cleanly"][0] is False


class TestBuildUploadDetail:
    """Tests for build_upload_detail transform."""

    @pytest.mark.unit
    def test_upload_detail_basic(self):
        lf = pl.DataFrame({
            "session_id": [1, 1, 1],
            "timestamp": [
                datetime(2026, 3, 1, 10, 0),
                datetime(2026, 3, 1, 10, 5),
                datetime(2026, 3, 1, 10, 10),
            ],
            "server": ["sftp.example.com"] * 3,
            "event_type": ["connection", "upload", "disconnect"],
            "message": ["Auth", "Upload", "Close"],
            "filename": [None, "Andrew Weigert Jr SVA 02.182026.pdf", None],
            "source_path": [None, "/local/file", None],
            "destination_path": [None, "/remote/file", None],
            "submission_date": [None, date(2026, 2, 18), None],
        }).lazy()
        result = build_upload_detail(lf).collect()
        assert result.height == 1
        assert result["patient_name"][0] == "Andrew Weigert Jr"
        assert result["is_sva_form"][0] is True

    @pytest.mark.unit
    def test_upload_detail_non_sva_file(self):
        lf = pl.DataFrame({
            "session_id": [1],
            "timestamp": [datetime(2026, 3, 1, 10, 0)],
            "server": ["sftp.example.com"],
            "event_type": ["upload"],
            "message": ["Upload"],
            "filename": ["random_document.pdf"],
            "source_path": ["/local"],
            "destination_path": ["/remote"],
            "submission_date": [None],
        }).lazy()
        result = build_upload_detail(lf).collect()
        assert result.height == 1
        assert result["is_sva_form"][0] is False


class TestBuildDailySummary:
    """Tests for build_daily_summary transform."""

    @pytest.mark.unit
    def test_daily_summary(self):
        lf = pl.DataFrame({
            "session_id": [1, 1, 2, 2],
            "timestamp": [
                datetime(2026, 3, 1, 10, 0),
                datetime(2026, 3, 1, 10, 5),
                datetime(2026, 3, 2, 14, 0),
                datetime(2026, 3, 2, 14, 5),
            ],
            "event_type": ["upload", "upload", "connection", "upload"],
            "message": ["Upload", "Upload", "Auth", "Upload"],
            "filename": [
                "John Doe SVA 03.012026.pdf",
                "Jane Smith SVA 03.012026.pdf",
                None,
                "Bob Jones SVA 03.022026.pdf",
            ],
            "source_path": ["/a", "/b", None, "/c"],
            "destination_path": ["/x", "/y", None, "/z"],
            "submission_date": [date(2026, 3, 1), date(2026, 3, 1), None, date(2026, 3, 2)],
        }).lazy()
        result = build_daily_summary(lf).collect()
        assert result.height == 2
        assert "upload_date" in result.columns
        assert "total_uploads" in result.columns
        assert "unique_patients" in result.columns

    @pytest.mark.unit
    def test_daily_summary_single_day(self):
        lf = pl.DataFrame({
            "session_id": [1, 1],
            "timestamp": [datetime(2026, 3, 1, 10, 0), datetime(2026, 3, 1, 10, 5)],
            "event_type": ["upload", "upload"],
            "message": ["Upload", "Upload"],
            "filename": [
                "John Doe SVA 03.012026.pdf",
                "John Doe SVA 03.012026.pdf",
            ],
            "source_path": ["/a", "/b"],
            "destination_path": ["/x", "/y"],
            "submission_date": [date(2026, 3, 1), date(2026, 3, 1)],
        }).lazy()
        result = build_daily_summary(lf).collect()
        assert result.height == 1
        assert result["total_uploads"][0] == 2
