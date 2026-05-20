# © 2025 HarmonyCares
"""Tests for acoharmony._notes._calendar (CalendarPlugins)."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from acoharmony._notes import CalendarPlugins


def _calendar_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "py": [2024, 2024, 2025, 2025, 2025, None],
            "category": [
                "Reporting",
                " Reporting ",
                "Audit",
                "Audit",
                "Reporting",
                None,
            ],
            "type": ["Deadline", "Event", "Deadline", "Report", "Event", None],
            "start_date": [
                date(2024, 1, 15),
                date(2024, 6, 1),
                date(2025, 3, 31),
                date(2025, 4, 1),
                date(2025, 5, 1),
                None,
            ],
            "end_date": [None] * 6,
            "description": ["a", "b", "c", "d", "e", None],
            "links": ["", "", "", "", "", None],
            "notes": ["", "", "", "", "", None],
            "file_date": ["20240101"] * 6,
        }
    )


# ---------------------------------------------------------------------------
# load_latest_snapshot
# ---------------------------------------------------------------------------


class TestLoadLatestSnapshot:
    @pytest.mark.unit
    def test_missing(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="Calendar silver parquet"):
            CalendarPlugins().load_latest_snapshot(tmp_path)

    @pytest.mark.unit
    def test_keeps_latest_file_date_and_strips_category(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "py": [2024, 2024],
                "category": [" Reporting ", " Audit "],
                "type": ["Deadline", "Report"],
                "start_date": [date(2024, 1, 1), date(2024, 6, 1)],
                "end_date": [None, None],
                "description": ["a", "b"],
                "links": ["", ""],
                "notes": ["", ""],
                "file_date": ["20240101", "20240601"],  # newer wins
            }
        )
        df.write_parquet(tmp_path / "reach_calendar.parquet")
        out = CalendarPlugins().load_latest_snapshot(tmp_path)
        assert out.height == 1
        assert out["category"][0] == "Audit"  # latest + stripped

    @pytest.mark.unit
    def test_no_file_date_column(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "py": [2024],
                "category": [" Reporting "],
                "type": ["Deadline"],
                "start_date": [date(2024, 1, 1)],
                "end_date": [None],
                "description": ["a"],
                "links": [""],
                "notes": [""],
            }
        )
        df.write_parquet(tmp_path / "reach_calendar.parquet")
        out = CalendarPlugins().load_latest_snapshot(tmp_path)
        assert out.height == 1
        assert out["category"][0] == "Reporting"


# ---------------------------------------------------------------------------
# rollups
# ---------------------------------------------------------------------------


class TestRollups:
    @pytest.mark.unit
    def test_by_performance_year(self) -> None:
        out = CalendarPlugins().by_performance_year(_calendar_df())
        # 2024(2), 2025(3), null(1) — null sorted last
        assert out["py"].to_list() == [2024, 2025, None]
        assert out["event_count"].to_list() == [2, 3, 1]

    @pytest.mark.unit
    def test_by_category(self) -> None:
        # Note: leading/trailing whitespace not stripped here (only load_latest_snapshot does)
        out = CalendarPlugins().by_category(_calendar_df())
        assert "event_count" in out.columns
        assert out["event_count"][0] >= out["event_count"][-1]

    @pytest.mark.unit
    def test_by_type(self) -> None:
        out = CalendarPlugins().by_type(_calendar_df())
        assert "event_count" in out.columns


# ---------------------------------------------------------------------------
# filter_events
# ---------------------------------------------------------------------------


class TestFilterEvents:
    @pytest.mark.unit
    def test_no_filters_returns_all(self) -> None:
        df = _calendar_df()
        assert CalendarPlugins().filter_events(df).height == df.height

    @pytest.mark.unit
    def test_category_filter(self) -> None:
        out = CalendarPlugins().filter_events(_calendar_df(), categories=["Audit"])
        assert out.height == 2

    @pytest.mark.unit
    def test_py_filter_casts_strings_to_int(self) -> None:
        out = CalendarPlugins().filter_events(_calendar_df(), pys=["2024"])
        assert out["py"].drop_nulls().unique().to_list() == [2024]

    @pytest.mark.unit
    def test_type_filter(self) -> None:
        out = CalendarPlugins().filter_events(_calendar_df(), types=["Deadline"])
        assert out.height == 2


# ---------------------------------------------------------------------------
# date-window slices
# ---------------------------------------------------------------------------


class TestUpcoming:
    @pytest.mark.unit
    def test_filters_to_future(self) -> None:
        # Reference date well before all the fixture dates
        out = CalendarPlugins().upcoming(_calendar_df(), today=date(2023, 1, 1), n=10)
        # 5 non-null start_date rows, all in the future relative to 2023-01-01
        assert out.height == 5

    @pytest.mark.unit
    def test_default_today(self) -> None:
        # Past dates → no upcoming
        out = CalendarPlugins().upcoming(_calendar_df(), today=date(2099, 1, 1))
        assert out.height == 0


class TestRecent:
    @pytest.mark.unit
    def test_lookback(self) -> None:
        # 2025-04-15 reference, 90-day lookback → captures 2025-03-31 + 2025-04-01
        out = CalendarPlugins().recent(_calendar_df(), today=date(2025, 4, 15), lookback_days=90)
        dates = sorted(out["start_date"].to_list())
        assert dates == [date(2025, 3, 31), date(2025, 4, 1)]


class TestMonthlyTimeline:
    @pytest.mark.unit
    def test_groups_by_year_month(self) -> None:
        out = CalendarPlugins().monthly_timeline(_calendar_df())
        assert "year" in out.columns and "month" in out.columns


class TestDeadlines:
    @pytest.mark.unit
    def test_filters_to_deadline(self) -> None:
        out = CalendarPlugins().deadlines(_calendar_df())
        assert out.height == 2
        assert "links" in out.columns


class TestNullCounts:
    @pytest.mark.unit
    def test_returns_per_column(self) -> None:
        out = CalendarPlugins().null_counts(_calendar_df())
        assert out["start_date"] == 1
        assert out["category"] == 1
        assert out["py"] == 1
