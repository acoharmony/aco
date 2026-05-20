# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._transforms._cclf_mer_spend."""

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

from acoharmony._transforms._cclf_mer_spend import build_cclf_mer_spend


_CCLF1_SCHEMA = {
    "bene_mbi_id": pl.Utf8,
    "clm_type_cd": pl.Utf8,
    "clm_from_dt": pl.Date,
    "clm_pmt_amt": pl.Float64,
    "clm_adjsmt_type_cd": pl.Utf8,
    "file_date": pl.Utf8,
}

_CCLF_LINE_SCHEMA = {
    "bene_mbi_id": pl.Utf8,
    "clm_type_cd": pl.Utf8,
    "clm_from_dt": pl.Date,
    "clm_line_cvrd_pd_amt": pl.Float64,
    "clm_adjsmt_type_cd": pl.Utf8,
    "file_date": pl.Utf8,
}


def _cclf1_frame(rows: list[dict]) -> pl.LazyFrame:
    """Build a CCLF1-shaped LazyFrame. Empty lists still get the full schema."""
    defaults = {
        "bene_mbi_id": "1AW3F64YJ92",
        "clm_type_cd": "60",
        "clm_from_dt": date(2025, 1, 15),
        "clm_pmt_amt": 1000.00,
        "clm_adjsmt_type_cd": "0",
        "file_date": "2025-06-30",
    }
    filled = [{**defaults, **r} for r in rows]
    return pl.LazyFrame(filled, schema=_CCLF1_SCHEMA)


def _cclf5_frame(rows: list[dict]) -> pl.LazyFrame:
    """Build a CCLF5-shaped LazyFrame (Part B professional, line-level)."""
    defaults = {
        "bene_mbi_id": "1AW3F64YJ92",
        "clm_type_cd": "71",
        "clm_from_dt": date(2025, 1, 15),
        "clm_line_cvrd_pd_amt": 100.00,
        "clm_adjsmt_type_cd": "0",
        "file_date": "2025-06-30",
    }
    filled = [{**defaults, **r} for r in rows]
    return pl.LazyFrame(filled, schema=_CCLF_LINE_SCHEMA)


def _cclf6_frame(rows: list[dict]) -> pl.LazyFrame:
    """Build a CCLF6-shaped LazyFrame (Part B DME, line-level)."""
    defaults = {
        "bene_mbi_id": "1AW3F64YJ92",
        "clm_type_cd": "82",
        "clm_from_dt": date(2025, 1, 15),
        "clm_line_cvrd_pd_amt": 50.00,
        "clm_adjsmt_type_cd": "0",
        "file_date": "2025-06-30",
    }
    filled = [{**defaults, **r} for r in rows]
    return pl.LazyFrame(filled, schema=_CCLF_LINE_SCHEMA)


class TestOutputShape:
    """Output shape invariants."""

    @pytest.mark.unit
    def test_output_columns(self):
        """Emits (bene_mbi_id, year_month, clm_type_cd, total_spend)."""
        cclf1 = _cclf1_frame([{}])
        cclf5 = _cclf5_frame([{}])
        cclf6 = _cclf6_frame([{}])
        result = build_cclf_mer_spend(cclf1, cclf5, cclf6).collect()
        assert set(result.columns) == {
            "bene_mbi_id",
            "year_month",
            "clm_type_cd",
            "total_spend",
        }

    @pytest.mark.unit
    def test_one_row_per_bene_month_clm_type_tuple(self):
        """The grain is (bene, month, claim_type). No duplicates, no line explosion."""
        cclf1 = _cclf1_frame(
            [
                # Same bene, month, clm_type — should collapse to one row
                {"bene_mbi_id": "AAA", "clm_type_cd": "60",
                 "clm_from_dt": date(2025, 1, 5), "clm_pmt_amt": 100.0},
                {"bene_mbi_id": "AAA", "clm_type_cd": "60",
                 "clm_from_dt": date(2025, 1, 20), "clm_pmt_amt": 200.0},
            ]
        )
        cclf5 = _cclf5_frame([])
        cclf6 = _cclf6_frame([])
        result = build_cclf_mer_spend(cclf1, cclf5, cclf6).collect()
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["bene_mbi_id"] == "AAA"
        assert row["year_month"] == 202501
        assert row["clm_type_cd"] == "60"
        assert float(row["total_spend"]) == pytest.approx(300.00)


class TestCclf1PartAAggregation:
    """CCLF1 rows land in Part A buckets (10/20/30/40/50/60)."""

    @pytest.mark.unit
    def test_each_part_a_code_produces_its_own_bucket(self):
        cclf1 = _cclf1_frame(
            [
                {"clm_type_cd": "10", "clm_pmt_amt": 10.0},
                {"clm_type_cd": "20", "clm_pmt_amt": 20.0},
                {"clm_type_cd": "30", "clm_pmt_amt": 30.0},
                {"clm_type_cd": "40", "clm_pmt_amt": 40.0},
                {"clm_type_cd": "50", "clm_pmt_amt": 50.0},
                {"clm_type_cd": "60", "clm_pmt_amt": 60.0},
            ]
        )
        result = (
            build_cclf_mer_spend(cclf1, _cclf5_frame([]), _cclf6_frame([]))
            .collect()
            .sort("clm_type_cd")
        )
        assert result["clm_type_cd"].to_list() == ["10", "20", "30", "40", "50", "60"]
        spends = [float(v) for v in result["total_spend"].to_list()]
        assert spends == [
            pytest.approx(10.0),
            pytest.approx(20.0),
            pytest.approx(30.0),
            pytest.approx(40.0),
            pytest.approx(50.0),
            pytest.approx(60.0),
        ]

    @pytest.mark.unit
    def test_stray_code_is_dropped(self):
        """Code 61 ("inpatient denied") is outside MER taxonomy — drop, don't crash."""
        cclf1 = _cclf1_frame(
            [
                {"clm_type_cd": "60", "clm_pmt_amt": 100.0},
                {"clm_type_cd": "61", "clm_pmt_amt": 999.0},
            ]
        )
        result = build_cclf_mer_spend(cclf1, _cclf5_frame([]), _cclf6_frame([])).collect()
        assert result.height == 1
        assert result["clm_type_cd"][0] == "60"
        assert float(result["total_spend"][0]) == pytest.approx(100.0)

    @pytest.mark.unit
    def test_cancellation_nets_against_original(self):
        """Original + cancellation of the same dollar amount = 0 in the bucket."""
        cclf1 = _cclf1_frame(
            [
                {"clm_adjsmt_type_cd": "0", "clm_pmt_amt": 500.0},
                {"clm_adjsmt_type_cd": "1", "clm_pmt_amt": 500.0},
            ]
        )
        result = build_cclf_mer_spend(cclf1, _cclf5_frame([]), _cclf6_frame([])).collect()
        assert result.height == 1
        assert float(result["total_spend"][0]) == pytest.approx(0.0)


class TestCclf5PartBPhysician:
    """CCLF5 rows land in Part B physician buckets (71, 72)."""

    @pytest.mark.unit
    def test_physician_71_and_dme_physician_72(self):
        cclf5 = _cclf5_frame(
            [
                {"clm_type_cd": "71", "clm_line_cvrd_pd_amt": 123.45},
                {"clm_type_cd": "72", "clm_line_cvrd_pd_amt": 67.89},
            ]
        )
        result = (
            build_cclf_mer_spend(_cclf1_frame([]), cclf5, _cclf6_frame([]))
            .collect()
            .sort("clm_type_cd")
        )
        assert result["clm_type_cd"].to_list() == ["71", "72"]
        spends = [float(v) for v in result["total_spend"].to_list()]
        assert spends == [pytest.approx(123.45), pytest.approx(67.89)]

    @pytest.mark.unit
    def test_line_level_cancellation_nets(self):
        cclf5 = _cclf5_frame(
            [
                {"clm_adjsmt_type_cd": "0", "clm_line_cvrd_pd_amt": 100.0},
                {"clm_adjsmt_type_cd": "1", "clm_line_cvrd_pd_amt": 40.0},
            ]
        )
        result = build_cclf_mer_spend(_cclf1_frame([]), cclf5, _cclf6_frame([])).collect()
        assert result.height == 1
        assert float(result["total_spend"][0]) == pytest.approx(60.0)


class TestCclf6PartBDmerc:
    """CCLF6 rows land in Part B DMERC buckets (81, 82)."""

    @pytest.mark.unit
    def test_dmerc_81_and_82(self):
        cclf6 = _cclf6_frame(
            [
                {"clm_type_cd": "81", "clm_line_cvrd_pd_amt": 11.11},
                {"clm_type_cd": "82", "clm_line_cvrd_pd_amt": 22.22},
            ]
        )
        result = (
            build_cclf_mer_spend(_cclf1_frame([]), _cclf5_frame([]), cclf6)
            .collect()
            .sort("clm_type_cd")
        )
        assert result["clm_type_cd"].to_list() == ["81", "82"]
        spends = [float(v) for v in result["total_spend"].to_list()]
        assert spends == [pytest.approx(11.11), pytest.approx(22.22)]


class TestMonthGrouping:
    """Rows falling in different months of the same year produce distinct buckets."""

    @pytest.mark.unit
    def test_different_months_separate(self):
        cclf1 = _cclf1_frame(
            [
                {"clm_from_dt": date(2025, 1, 15), "clm_pmt_amt": 100.0},
                {"clm_from_dt": date(2025, 2, 15), "clm_pmt_amt": 200.0},
                {"clm_from_dt": date(2025, 3, 15), "clm_pmt_amt": 300.0},
            ]
        )
        result = (
            build_cclf_mer_spend(cclf1, _cclf5_frame([]), _cclf6_frame([]))
            .collect()
            .sort("year_month")
        )
        assert result["year_month"].to_list() == [202501, 202502, 202503]
        spends = [float(v) for v in result["total_spend"].to_list()]
        assert spends == [
            pytest.approx(100.0),
            pytest.approx(200.0),
            pytest.approx(300.0),
        ]


class TestPointInTimeFilter:
    """``as_of_cutoff`` filters CCLF rows whose file_date exceeds the cutoff."""

    @pytest.mark.unit
    def test_future_delivery_rows_are_dropped(self):
        cclf1 = _cclf1_frame(
            [
                {"file_date": "2025-06-30", "clm_pmt_amt": 100.0},
                {"file_date": "2025-12-31", "clm_pmt_amt": 999.0},
            ]
        )
        result = build_cclf_mer_spend(
            cclf1, _cclf5_frame([]), _cclf6_frame([]), as_of_cutoff="2025-06-30"
        ).collect()
        assert result.height == 1
        assert float(result["total_spend"][0]) == pytest.approx(100.0)

    @pytest.mark.unit
    def test_no_cutoff_keeps_everything(self):
        cclf1 = _cclf1_frame(
            [
                {"file_date": "2025-06-30", "clm_pmt_amt": 100.0},
                {"file_date": "2025-12-31", "clm_pmt_amt": 200.0},
            ]
        )
        result = build_cclf_mer_spend(cclf1, _cclf5_frame([]), _cclf6_frame([])).collect()
        assert result.height == 1
        assert float(result["total_spend"][0]) == pytest.approx(300.0)


class TestMixedSourceAggregation:
    """A single run with all three CCLF sources still produces clean buckets."""

    @pytest.mark.unit
    def test_all_three_sources_merged(self):
        """Same bene and month, different claim types across CCLF1/5/6."""
        cclf1 = _cclf1_frame(
            [{"clm_type_cd": "60", "clm_pmt_amt": 1000.0}]
        )
        cclf5 = _cclf5_frame(
            [{"clm_type_cd": "71", "clm_line_cvrd_pd_amt": 500.0}]
        )
        cclf6 = _cclf6_frame(
            [{"clm_type_cd": "82", "clm_line_cvrd_pd_amt": 50.0}]
        )
        result = (
            build_cclf_mer_spend(cclf1, cclf5, cclf6).collect().sort("clm_type_cd")
        )
        assert result["clm_type_cd"].to_list() == ["60", "71", "82"]
        spends = [float(v) for v in result["total_spend"].to_list()]
        assert spends == [
            pytest.approx(1000.0),
            pytest.approx(500.0),
            pytest.approx(50.0),
        ]


class TestRealFixtureSmoke:
    """End-to-end smoke test against committed generate-mocks CCLF fixtures."""

    FIXTURE_DIR = Path(__file__).parent.parent / "_fixtures" / "reconciliation"

    @pytest.mark.unit
    def test_runs_against_committed_fixtures(self):
        if not (self.FIXTURE_DIR / "cclf1.parquet").exists():
            pytest.skip("reconciliation CCLF fixtures not generated")
        cclf1 = pl.scan_parquet(self.FIXTURE_DIR / "cclf1.parquet")
        cclf5 = pl.scan_parquet(self.FIXTURE_DIR / "cclf5.parquet")
        cclf6 = pl.scan_parquet(self.FIXTURE_DIR / "cclf6.parquet")
        result = build_cclf_mer_spend(cclf1, cclf5, cclf6).collect()
        assert result.height > 0
        assert set(result.columns) == {
            "bene_mbi_id",
            "year_month",
            "clm_type_cd",
            "total_spend",
        }
        # Every emitted claim type is in the canonical MER taxonomy.
        valid = {"10", "20", "30", "40", "50", "60", "71", "72", "81", "82"}
        assert set(result["clm_type_cd"].unique().to_list()) <= valid
