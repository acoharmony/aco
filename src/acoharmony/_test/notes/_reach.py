# © 2025 HarmonyCares
"""Tests for acoharmony._notes._reach (ReachPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from acoharmony._notes import ReachPlugins


def _bar_lf(rows: list[dict]) -> pl.LazyFrame:
    schema = {
        "bene_mbi": pl.Utf8,
        "start_date": pl.Date,
        "end_date": pl.Date,
        "bene_date_of_death": pl.Date,
        "voluntary_alignment_type": pl.Utf8,
    }
    return pl.LazyFrame(rows, schema=schema)


def _bar_with_year_month(rows: list[dict]) -> pl.LazyFrame:
    """Rows already have year_month — used after load_bar."""
    return pl.LazyFrame(
        rows,
        schema={
            "bene_mbi": pl.Utf8,
            "start_date": pl.Date,
            "end_date": pl.Date,
            "bene_date_of_death": pl.Date,
            "voluntary_alignment_type": pl.Utf8,
            "year_month": pl.Int64,
        },
    )


# ---------------------------------------------------------------------------
# load_bar
# ---------------------------------------------------------------------------


class TestLoadBar:
    @pytest.mark.unit
    def test_missing_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="BAR file not found"):
            ReachPlugins().load_bar(tmp_path)

    @pytest.mark.unit
    def test_adds_year_month(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "bene_mbi": ["M"],
                "start_date": [date(2024, 6, 15)],
                "end_date": [None],
                "bene_date_of_death": [None],
                "voluntary_alignment_type": [None],
            }
        )
        df.write_parquet(tmp_path / "bar.parquet")
        lf = ReachPlugins().load_bar(tmp_path)
        out = lf.collect()
        assert "year_month" in out.columns
        assert out["year_month"][0] == 202406


# ---------------------------------------------------------------------------
# benes_for_window / benes_for_month
# ---------------------------------------------------------------------------


def _ym_rows() -> list[dict]:
    return [
        {"bene_mbi": "A", "start_date": date(2025, 1, 15), "end_date": None,
         "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202501},
        {"bene_mbi": "A", "start_date": date(2025, 6, 1), "end_date": None,
         "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202506},
        {"bene_mbi": "B", "start_date": date(2025, 7, 1), "end_date": None,
         "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202507},
        {"bene_mbi": "C", "start_date": date(2026, 1, 5), "end_date": None,
         "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202601},
    ]


class TestBenesForWindow:
    @pytest.mark.unit
    def test_distinct_mbis_in_range(self) -> None:
        lf = _bar_with_year_month(_ym_rows())
        out = ReachPlugins().benes_for_window(lf, 202501, 202512)
        # A appears twice → distinct → 2 rows total (A, B)
        assert sorted(out["mbi"].to_list()) == ["A", "B"]

    @pytest.mark.unit
    def test_single_month_window(self) -> None:
        lf = _bar_with_year_month(_ym_rows())
        out = ReachPlugins().benes_for_window(lf, 202507)
        assert out.height == 1
        assert out["mbi"][0] == "B"


class TestBenesForMonth:
    @pytest.mark.unit
    def test_filters_to_month(self) -> None:
        lf = _bar_with_year_month(_ym_rows())
        out = ReachPlugins().benes_for_month(lf, 202601)
        assert out.height == 1
        assert out["mbi"][0] == "C"


# ---------------------------------------------------------------------------
# attribution_loss
# ---------------------------------------------------------------------------


class TestAttributionLoss:
    @pytest.mark.unit
    def test_diff(self) -> None:
        prev = pl.DataFrame({"mbi": ["A", "B", "C"], "year_month": [202501, 202506, 202507]})
        nxt = pl.DataFrame({"mbi": ["B", "D"], "start_date": [None, None]})
        out = ReachPlugins().attribution_loss(prev, nxt)
        assert out["lost_mbis"] == {"A", "C"}
        assert out["total_lost"] == 2
        assert out["total_prev"] == 3
        assert out["total_next"] == 2
        # lost_benes preserves prev rows for A and C
        assert sorted(out["lost_benes"]["mbi"].to_list()) == ["A", "C"]


# ---------------------------------------------------------------------------
# load_crr_for_lost
# ---------------------------------------------------------------------------


class TestLoadCrrForLost:
    @pytest.mark.unit
    def test_missing_returns_none(self, tmp_path: Path) -> None:
        assert ReachPlugins().load_crr_for_lost(tmp_path, ["A"]) is None

    @pytest.mark.unit
    def test_filters_to_lost(self, tmp_path: Path) -> None:
        crr = pl.DataFrame(
            {
                "bene_mbi": ["A", "B", "C"],
                "bene_death_dt": [date(2024, 12, 1), None, None],
            }
        )
        crr.write_parquet(tmp_path / "crr.parquet")
        out = ReachPlugins().load_crr_for_lost(tmp_path, {"A", "B"})
        assert sorted(out["mbi"].to_list()) == ["A", "B"]
        assert "bene_death_dt" in out.columns


# ---------------------------------------------------------------------------
# lost_bar_records / categorize_term_reasons / breakdown_stats
# ---------------------------------------------------------------------------


def _bar_with_lost() -> pl.LazyFrame:
    return _bar_with_year_month(
        [
            {"bene_mbi": "A", "start_date": date(2025, 1, 1), "end_date": date(2025, 6, 1),
             "bene_date_of_death": date(2025, 6, 1), "voluntary_alignment_type": None, "year_month": 202501},
            {"bene_mbi": "A", "start_date": date(2025, 6, 1), "end_date": date(2025, 6, 1),
             "bene_date_of_death": date(2025, 6, 1), "voluntary_alignment_type": None, "year_month": 202506},
            {"bene_mbi": "B", "start_date": date(2025, 1, 1), "end_date": None,
             "bene_date_of_death": None, "voluntary_alignment_type": "Voluntary", "year_month": 202501},
            {"bene_mbi": "C", "start_date": date(2025, 1, 1), "end_date": None,
             "bene_date_of_death": None, "voluntary_alignment_type": None, "year_month": 202501},
        ]
    )


class TestLostBarRecords:
    @pytest.mark.unit
    def test_takes_latest_per_mbi(self) -> None:
        out = ReachPlugins().lost_bar_records(_bar_with_lost(), {"A", "B", "C"})
        as_dict = {row["mbi"]: row for row in out.iter_rows(named=True)}
        assert as_dict["A"]["last_alignment_month"] == 202506
        assert as_dict["B"]["voluntary_type"] == "Voluntary"


class TestCategorizeTermReasons:
    @pytest.mark.unit
    def test_with_crr(self) -> None:
        lost_bar = ReachPlugins().lost_bar_records(_bar_with_lost(), {"A", "B", "C"})
        crr = pl.DataFrame({"mbi": ["A"], "bene_death_dt": [date(2025, 6, 1)]})
        cat, summary = ReachPlugins().categorize_term_reasons(lost_bar, crr)
        # A → Expired (death_date in BAR), B → Lost Provider, C → Other/Unknown
        as_dict = {row["mbi"]: row for row in cat.iter_rows(named=True)}
        assert as_dict["A"]["term_category"] == "Expired"
        assert as_dict["B"]["term_category"] == "Lost Provider"
        assert as_dict["C"]["term_category"] == "Other/Unknown"
        # Summary sums match
        sums = dict(zip(summary["term_category"].to_list(), summary["count"].to_list()))
        assert sums == {"Expired": 1, "Lost Provider": 1, "Other/Unknown": 1}

    @pytest.mark.unit
    def test_without_crr(self) -> None:
        # Synthesize a case where death_date in BAR is None but voluntary set
        lost_bar = pl.DataFrame(
            {
                "mbi": ["B"],
                "last_alignment_month": [202501],
                "end_date": [None],
                "death_date": [None],
                "voluntary_type": ["Voluntary"],
            }
        )
        cat, summary = ReachPlugins().categorize_term_reasons(lost_bar, None)
        assert cat["term_category"][0] == "Lost Provider"


class TestBreakdownStats:
    @pytest.mark.unit
    def test_flattens(self) -> None:
        summary = pl.DataFrame(
            {"term_category": ["Expired", "Other/Unknown"], "count": [3, 2]}
        )
        out = ReachPlugins().breakdown_stats(summary, total_lost=10, has_end_date=8)
        assert out["Total Lost"] == 10
        assert out["Expired (SVA)"] == 3
        assert out["Other/Unknown Reason"] == 2
        assert out["Lost Provider"] == 0
        assert out["No End Date"] == 2
        assert out["Moved to MA"] == 0


class TestTemporalDistribution:
    @pytest.mark.unit
    def test_sorts_by_month(self) -> None:
        df = pl.DataFrame(
            {
                "mbi": ["A", "B", "C"],
                "last_alignment_month": [202506, 202501, 202506],
            }
        )
        out = ReachPlugins().temporal_distribution(df)
        assert out["last_month_str"].to_list() == ["202501", "202506"]
        assert out["count"].to_list() == [1, 2]
