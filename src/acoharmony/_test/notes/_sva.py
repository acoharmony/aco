# © 2025 HarmonyCares
"""Tests for acoharmony._notes._sva (SvaPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._notes import SvaPlugins


def _write(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


# ---------------------------------------------------------------------------
# load_sva_last_n
# ---------------------------------------------------------------------------


class TestLoadSvaLastN:
    @pytest.mark.unit
    def test_keeps_top_n_distinct_dates(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "bene_mbi": ["A", "B", "C", "D", "E"],
                "file_date": ["2024-01-01", "2024-01-01", "2024-03-01", "2024-06-01", "2024-08-01"],
            }
        )
        _write(tmp_path / "sva.parquet", df)
        rows, counts, dates = SvaPlugins().load_sva_last_n(tmp_path, n=2)
        # Keep dates 2024-08-01 and 2024-06-01
        assert sorted(dates, reverse=True) == [date(2024, 8, 1), date(2024, 6, 1)]
        assert rows.height == 2
        assert counts.height == 2


# ---------------------------------------------------------------------------
# load_bar_latest
# ---------------------------------------------------------------------------


class TestLoadBarLatest:
    @pytest.mark.unit
    def test_filters_to_max_date(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "bene_mbi": ["A", "B", "C"],
                "file_date": ["2024-01-01", "2024-06-01", "2024-06-01"],
            }
        )
        _write(tmp_path / "bar.parquet", df)
        rows, bar_date = SvaPlugins().load_bar_latest(tmp_path)
        assert bar_date == date(2024, 6, 1)
        assert rows.height == 2
        assert sorted(rows["mbi"].to_list()) == ["B", "C"]


# ---------------------------------------------------------------------------
# load_pbvar
# ---------------------------------------------------------------------------


class TestLoadPbvar:
    @pytest.mark.unit
    def test_per_mbi_aggregation(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "bene_mbi": ["A", "A", "B"],
                "file_date": ["2024-01-01", "2024-06-01", "2024-03-01"],
                "sva_response_code_list": ["x", "y", "z"],
                "sva_signature_date": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 3, 1)],
            }
        )
        _write(tmp_path / "pbvar.parquet", df)
        out = SvaPlugins().load_pbvar(tmp_path)
        as_dict = {row["mbi"]: row for row in out.iter_rows(named=True)}
        # 'last' for A is unspecified order in group_by(file_date asc), but the file_date max == 2024-06-01.
        assert as_dict["A"]["pbvar_file_date"] == date(2024, 6, 1)
        assert as_dict["A"]["most_recent_sva_date"] == date(2024, 6, 1)
        assert as_dict["B"]["pbvar_response_codes"] == "z"


# ---------------------------------------------------------------------------
# consolidate / enrich / source_breakdown
# ---------------------------------------------------------------------------


def _sva_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["A", "B"],
            "submission_date": [date(2024, 6, 1), date(2024, 6, 1)],
        }
    )


def _bar_rows() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["B", "C"],
            "file_date": [date(2024, 7, 1), date(2024, 7, 1)],
        }
    )


class TestConsolidate:
    @pytest.mark.unit
    def test_combines_sources(self) -> None:
        out = SvaPlugins().consolidate(_sva_rows(), _bar_rows())
        as_dict = {row["mbi"]: row for row in out.iter_rows(named=True)}
        assert as_dict["A"]["sources"] == "SVA"
        assert as_dict["B"]["sources"] == "BAR, SVA"
        assert as_dict["C"]["sources"] == "BAR"
        assert as_dict["B"]["latest_date"] == date(2024, 7, 1)


class TestEnrich:
    @pytest.mark.unit
    def test_joins_hcmpi_and_pbvar(self) -> None:
        consolidated = pl.DataFrame(
            {
                "mbi": ["A", "B", "C"],
                "sources": ["SVA", "BAR, SVA", "BAR"],
                "latest_date": [date(2024, 6, 1), date(2024, 7, 1), date(2024, 7, 1)],
            }
        )
        pbvar = pl.DataFrame(
            {
                "mbi": ["A"],
                "pbvar_response_codes": ["x"],
                "pbvar_file_date": [date(2024, 6, 1)],
                "most_recent_sva_date": [date(2024, 6, 1)],
            }
        )
        xwalk = pl.LazyFrame(
            {"crnt_num": ["A", "C"], "hcmpi": ["HC_A", "HC_C"]}
        )
        with patch(
            "acoharmony._transforms._identity_timeline.current_mbi_with_hcmpi_lookup_lazy",
            return_value=xwalk,
        ):
            out = SvaPlugins().enrich(consolidated, pbvar, Path("/dev/null"))
        as_dict = {row["mbi"]: row for row in out.iter_rows(named=True)}
        assert as_dict["A"]["hcmpi"] == "HC_A"
        assert as_dict["B"]["hcmpi"] is None
        assert as_dict["A"]["pbvar_response_codes"] == "x"


class TestSourceBreakdown:
    @pytest.mark.unit
    def test_counts(self) -> None:
        df = pl.DataFrame(
            {
                "mbi": ["A", "B", "C", "D"],
                "sources": ["SVA", "BAR, SVA", "BAR", "BAR, SVA"],
            }
        )
        out = SvaPlugins().source_breakdown(df)
        as_dict = {row["Source"]: row["Count"] for row in out}
        # SVA in {A, B, D} = 3, BAR in {B, C, D} = 3, both = {B, D} = 2
        assert as_dict["SVA Only"] == 1
        assert as_dict["BAR Only"] == 1
        assert as_dict["Both SVA and BAR"] == 2
        assert as_dict["**Total**"] == 4
