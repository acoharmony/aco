# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._transforms._member_months_asof."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from acoharmony._transforms._member_months_asof import build_member_months_asof


# ---------------------------------------------------------------------------
# Test helpers: construct BAR/ALR-shaped LazyFrames with the columns the
# transform actually consumes. Extra columns present in real silver files
# are omitted; the transform must not require them.
# ---------------------------------------------------------------------------

_BAR_SCHEMA = {
    "bene_mbi": pl.Utf8,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "source_filename": pl.Utf8,
    "file_date": pl.Utf8,
}

_ALR_SCHEMA = {
    "bene_mbi": pl.Utf8,
    "source_filename": pl.Utf8,
    "file_date": pl.Utf8,
}


def _bar_frame(rows: list[dict]) -> pl.LazyFrame:
    defaults = {
        "bene_mbi": "1A01NE1EX37",
        "start_date": date(2024, 1, 1),
        "end_date": None,
        "source_filename": "P.D0259.ALGC24.RP.D240119.T1735222.xlsx",
        "file_date": "2024-01-19",
    }
    return pl.LazyFrame([{**defaults, **r} for r in rows], schema=_BAR_SCHEMA)


def _alr_frame(rows: list[dict]) -> pl.LazyFrame:
    defaults = {
        "bene_mbi": "1A01NE1EX37",
        "source_filename": "P.A2671.ACO.QALR.2024Q1.D240101.T0100000_1-2.csv",
        "file_date": "2024-01-01",
    }
    return pl.LazyFrame([{**defaults, **r} for r in rows], schema=_ALR_SCHEMA)


class TestOutputShape:
    """Output schema invariants."""

    @pytest.mark.unit
    def test_output_columns(self):
        """Emits (aco_id, program, performance_year, year_month, member_months)."""
        bar = _bar_frame([{}])
        alr = _alr_frame([{}])
        result = build_member_months_asof(bar, alr, as_of_cutoff="2024-12-31").collect()
        assert set(result.columns) == {
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "member_months",
        }


class TestReachMemberMonthsFromBar:
    """REACH member-months are derived from BAR ``start_date`` / ``end_date``."""

    @pytest.mark.unit
    def test_single_bene_open_ended_counts_every_py_month_through_cutoff(self):
        """One bene, start Jan 1 2024, no end, cutoff 2024-06-30 → 6 member-months."""
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "1AAA",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                    "file_date": "2024-06-01",
                }
            ]
        )
        alr = _alr_frame([])  # empty MSSP side
        result = (
            build_member_months_asof(bar, alr, as_of_cutoff="2024-06-30")
            .collect()
            .filter(pl.col("program") == "REACH")
            .sort("year_month")
        )
        # PY2024 months Jan..June = 202401..202406
        assert result["year_month"].to_list() == [
            202401, 202402, 202403, 202404, 202405, 202406,
        ]
        assert result["member_months"].to_list() == [1, 1, 1, 1, 1, 1]

    @pytest.mark.unit
    def test_bene_churns_out_mid_py(self):
        """Bene with end_date 2024-05-31 is NOT counted in May.

        Expect Jan..Apr populated (4 months), May onward empty.
        """
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "1BBB",
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 5, 31),
                    "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                    "file_date": "2024-06-01",
                }
            ]
        )
        alr = _alr_frame([])
        reach = (
            build_member_months_asof(bar, alr, as_of_cutoff="2024-06-30")
            .collect()
            .filter(pl.col("program") == "REACH")
            .sort("year_month")
        )
        # Only months where member_months > 0 appear in output
        assert reach["year_month"].to_list() == [202401, 202402, 202403, 202404]
        assert reach["member_months"].to_list() == [1, 1, 1, 1]

    @pytest.mark.unit
    def test_multiple_benes_same_aco_and_py_sum_correctly(self):
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "A",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                    "file_date": "2024-06-01",
                },
                {
                    "bene_mbi": "B",
                    "start_date": date(2024, 3, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                    "file_date": "2024-06-01",
                },
                {
                    "bene_mbi": "C",
                    "start_date": date(2024, 5, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                    "file_date": "2024-06-01",
                },
            ]
        )
        alr = _alr_frame([])
        reach = (
            build_member_months_asof(bar, alr, as_of_cutoff="2024-06-30")
            .collect()
            .filter(pl.col("program") == "REACH")
            .sort("year_month")
        )
        expected = {
            202401: 1,  # A
            202402: 1,  # A
            202403: 2,  # A, B
            202404: 2,  # A, B
            202405: 3,  # A, B, C
            202406: 3,  # A, B, C
        }
        got = dict(zip(reach["year_month"].to_list(), reach["member_months"].to_list(), strict=True))
        assert got == expected

    @pytest.mark.unit
    def test_latest_delivery_asof_is_selected_per_aco_and_py(self):
        """Two BAR deliveries for same (ACO, PY): earlier and later. Use latest ≤ cutoff.

        Earlier delivery says bene A was aligned Jan 1. Later delivery (still
        ≤ cutoff) says bene A was retroactively terminated Feb 1. The
        reconciliation at cutoff must reflect the LATER delivery's view.
        """
        bar = _bar_frame(
            [
                # Earlier delivery: no termination
                {
                    "bene_mbi": "A",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240115.T1111111.xlsx",
                    "file_date": "2024-01-15",
                },
                # Later delivery (same PY, same ACO): retroactive termination Feb 1
                {
                    "bene_mbi": "A",
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "source_filename": "P.D0259.ALGC24.RP.D240315.T2222222.xlsx",
                    "file_date": "2024-03-15",
                },
            ]
        )
        alr = _alr_frame([])
        reach = (
            build_member_months_asof(bar, alr, as_of_cutoff="2024-06-30")
            .collect()
            .filter(pl.col("program") == "REACH")
            .sort("year_month")
        )
        # Later delivery wins: A active only in January (end_date = Feb 1 exclusive)
        assert reach["year_month"].to_list() == [202401]
        assert reach["member_months"].to_list() == [1]

    @pytest.mark.unit
    def test_cutoff_before_later_delivery_keeps_earlier_view(self):
        """The same scenario but cutoff is BEFORE the retroactive termination
        delivery. We must use the earlier delivery's view (no termination known yet).

        Cutoff is set to 2024-02-29 (last day of Feb in a leap year) so that
        both Jan and Feb count as complete months. Partial-month cutoffs are
        dropped by the ``_month_ends`` helper to match CMS's convention of
        reconciling only completed months.
        """
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "A",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240115.T1111111.xlsx",
                    "file_date": "2024-01-15",
                },
                # This delivery is AFTER the cutoff — must not be consulted
                {
                    "bene_mbi": "A",
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "source_filename": "P.D0259.ALGC24.RP.D240315.T2222222.xlsx",
                    "file_date": "2024-03-15",
                },
            ]
        )
        alr = _alr_frame([])
        reach = (
            build_member_months_asof(bar, alr, as_of_cutoff="2024-02-29")
            .collect()
            .filter(pl.col("program") == "REACH")
            .sort("year_month")
        )
        # Earlier delivery view: A open-ended from Jan 1 → Jan and Feb
        assert reach["year_month"].to_list() == [202401, 202402]
        assert reach["member_months"].to_list() == [1, 1]

    @pytest.mark.unit
    def test_multiple_acos_scoped_separately(self):
        """Benes from ACO D0259 and D1234 must not bleed into each other's counts."""
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "A",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240115.T1111111.xlsx",
                    "file_date": "2024-01-15",
                },
                {
                    "bene_mbi": "B",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D1234.ALGC24.RP.D240115.T2222222.xlsx",
                    "file_date": "2024-01-15",
                },
            ]
        )
        alr = _alr_frame([])
        reach = (
            build_member_months_asof(bar, alr, as_of_cutoff="2024-01-31")
            .collect()
            .filter(pl.col("program") == "REACH")
            .sort("aco_id")
        )
        # Each ACO has 1 bene in Jan
        assert reach["aco_id"].to_list() == ["D0259", "D1234"]
        assert reach["member_months"].to_list() == [1, 1]


class TestMsspMemberMonthsFromAlr:
    """MSSP member-months come from ALR: roster-style, no start/end window."""

    @pytest.mark.unit
    def test_single_alr_delivery_covers_all_months_through_cutoff(self):
        """An ALR delivery represents the full PY attribution roster.

        A bene in the delivery contributes 1 member-month for each PY
        month from January through the cutoff month.
        """
        alr = _alr_frame(
            [
                {
                    "bene_mbi": "M1",
                    "source_filename": "P.A2671.ACO.QALR.2024Q1.D240101.T0000000_1-2.csv",
                    "file_date": "2024-01-01",
                }
            ]
        )
        bar = _bar_frame([])
        mssp = (
            build_member_months_asof(bar, alr, as_of_cutoff="2024-04-30")
            .collect()
            .filter(pl.col("program") == "MSSP")
            .sort("year_month")
        )
        assert mssp["year_month"].to_list() == [202401, 202402, 202403, 202404]
        assert mssp["member_months"].to_list() == [1, 1, 1, 1]

    @pytest.mark.unit
    def test_latest_alr_delivery_wins(self):
        """Same (ACO, PY) but two ALR deliveries: use the one with max file_date ≤ cutoff."""
        alr = _alr_frame(
            [
                # Earlier delivery: M1 only
                {
                    "bene_mbi": "M1",
                    "source_filename": "P.A2671.ACO.QALR.2024Q1.D240101.T0000000_1-2.csv",
                    "file_date": "2024-01-01",
                },
                # Later delivery: M1 plus M2 (M2 added retroactively)
                {
                    "bene_mbi": "M1",
                    "source_filename": "P.A2671.ACO.QALR.2024Q2.D240401.T0100000_1-2.csv",
                    "file_date": "2024-04-01",
                },
                {
                    "bene_mbi": "M2",
                    "source_filename": "P.A2671.ACO.QALR.2024Q2.D240401.T0100000_1-2.csv",
                    "file_date": "2024-04-01",
                },
            ]
        )
        bar = _bar_frame([])
        mssp = (
            build_member_months_asof(bar, alr, as_of_cutoff="2024-06-30")
            .collect()
            .filter(pl.col("program") == "MSSP")
            .sort("year_month")
        )
        # Latest delivery (2024-04-01) wins: two benes per month
        assert all(v == 2 for v in mssp["member_months"].to_list())

    @pytest.mark.unit
    def test_cutoff_before_later_alr_uses_earlier(self):
        alr = _alr_frame(
            [
                {
                    "bene_mbi": "M1",
                    "source_filename": "P.A2671.ACO.QALR.2024Q1.D240101.T0000000_1-2.csv",
                    "file_date": "2024-01-01",
                },
                # After cutoff — must not be used
                {
                    "bene_mbi": "M1",
                    "source_filename": "P.A2671.ACO.QALR.2024Q2.D240401.T0100000_1-2.csv",
                    "file_date": "2024-04-01",
                },
                {
                    "bene_mbi": "M2",
                    "source_filename": "P.A2671.ACO.QALR.2024Q2.D240401.T0100000_1-2.csv",
                    "file_date": "2024-04-01",
                },
            ]
        )
        bar = _bar_frame([])
        mssp = (
            build_member_months_asof(bar, alr, as_of_cutoff="2024-02-28")
            .collect()
            .filter(pl.col("program") == "MSSP")
            .sort("year_month")
        )
        # Earlier delivery view: only M1
        assert all(v == 1 for v in mssp["member_months"].to_list())


class TestMixedRechAndMsspInSameRun:
    """A single call with both BAR and ALR data produces both programs."""

    @pytest.mark.unit
    def test_both_programs_appear(self):
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "R1",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240115.T1111111.xlsx",
                    "file_date": "2024-01-15",
                }
            ]
        )
        alr = _alr_frame(
            [
                {
                    "bene_mbi": "M1",
                    "source_filename": "P.A2671.ACO.QALR.2024Q1.D240101.T0000000_1-2.csv",
                    "file_date": "2024-01-01",
                }
            ]
        )
        result = build_member_months_asof(bar, alr, as_of_cutoff="2024-01-31").collect()
        programs = set(result["program"].to_list())
        assert programs == {"REACH", "MSSP"}


class TestEmptyInputs:
    """Covers the no-data-at-all fallback branch."""

    @pytest.mark.unit
    def test_both_inputs_empty_after_filtering_returns_empty_frame(self):
        """When every BAR and ALR row is dropped by the cutoff, the transform
        must still return an empty, correctly-schemaed LazyFrame instead of
        raising or returning None. This exercises the fallback branch.
        """
        # Both rows are AFTER the cutoff, so nothing survives.
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "X",
                    "start_date": date(2099, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC99.RP.D990101.T1111111.xlsx",
                    "file_date": "2099-01-01",
                }
            ]
        )
        alr = _alr_frame(
            [
                {
                    "bene_mbi": "Y",
                    "source_filename": "P.A2671.ACO.QALR.2099Q1.D990101.T0000000_1-2.csv",
                    "file_date": "2099-01-01",
                }
            ]
        )
        result = build_member_months_asof(bar, alr, as_of_cutoff="2024-12-31").collect()
        assert result.height == 0
        assert set(result.columns) == {
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "member_months",
        }


class TestRealFixtureSmoke:
    """Smoke test against committed generate-mocks ALR/BAR fixtures."""

    FIXTURE_DIR = Path(__file__).parent.parent / "_fixtures" / "reconciliation"

    @pytest.mark.unit
    def test_runs_against_committed_fixtures(self):
        alr_path = self.FIXTURE_DIR / "alr.parquet"
        bar_path = self.FIXTURE_DIR / "bar.parquet"
        if not (alr_path.exists() and bar_path.exists()):
            pytest.skip("ALR/BAR fixtures not generated")
        alr = pl.scan_parquet(alr_path)
        bar = pl.scan_parquet(bar_path)
        result = build_member_months_asof(bar, alr, as_of_cutoff="2099-12-31").collect()
        # We don't assert specific values — the fixtures are distribution-matched
        # but not business-coherent. Just assert the pipeline does not crash
        # and the output schema is right.
        assert set(result.columns) == {
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "member_months",
        }
