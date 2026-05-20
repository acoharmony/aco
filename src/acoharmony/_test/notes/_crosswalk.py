# © 2025 HarmonyCares
"""Tests for acoharmony._notes._crosswalk."""

from __future__ import annotations

import polars as pl
import pytest

from acoharmony._notes import CrosswalkPlugins


def _cclf8(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema={"file_date": pl.Utf8, "bene_mbi_id": pl.Utf8})


def _cclf9(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        rows,
        schema={
            "file_date": pl.Utf8,
            "prvs_num": pl.Utf8,
            "crnt_num": pl.Utf8,
        },
    )


class TestCoverageByYear:
    @pytest.mark.unit
    def test_basic_join(self) -> None:
        cclf8 = _cclf8(
            [
                {"file_date": "20240101", "bene_mbi_id": "M1"},
                {"file_date": "20240601", "bene_mbi_id": "M2"},
                {"file_date": "20250101", "bene_mbi_id": "M3"},
            ]
        )
        cclf9 = _cclf9(
            [
                {"file_date": "20240101", "prvs_num": "M1", "crnt_num": "M2"},
                {"file_date": "20240101", "prvs_num": "M1", "crnt_num": "M1"},
                {"file_date": "20250101", "prvs_num": "M3", "crnt_num": "M3"},
            ]
        )
        out = CrosswalkPlugins().coverage_by_year(cclf8, cclf9)
        row24 = out.filter(pl.col("year") == 2024).to_dicts()[0]
        assert row24["total_unique_mbis"] == 2
        assert row24["unique_mbis_with_xwalk"] == 1
        assert row24["actual_crosswalks"] == 1
        # 1 / 2 * 100
        assert row24["crosswalk_percentage"] == 50.0
        row25 = out.filter(pl.col("year") == 2025).to_dicts()[0]
        # cclf9 row exists but it's a self-mapping → unique_mbis_with_xwalk=1, actual=0
        assert row25["unique_mbis_with_xwalk"] == 1
        assert row25["actual_crosswalks"] == 0

    @pytest.mark.unit
    def test_year_with_no_cclf9(self) -> None:
        cclf8 = _cclf8([{"file_date": "20240101", "bene_mbi_id": "M1"}])
        cclf9 = _cclf9([])
        out = CrosswalkPlugins().coverage_by_year(cclf8, cclf9)
        assert out.height == 1
        assert out["unique_mbis_with_xwalk"][0] is None
        # Division by null → null pct
        assert out["crosswalk_percentage"][0] is None


class TestMappingDetailByYear:
    @pytest.mark.unit
    def test_self_vs_actual_split(self) -> None:
        cclf9 = _cclf9(
            [
                {"file_date": "20240101", "prvs_num": "A", "crnt_num": "B"},
                {"file_date": "20240101", "prvs_num": "A", "crnt_num": "A"},
                {"file_date": "20240101", "prvs_num": "C", "crnt_num": "C"},
            ]
        )
        out = CrosswalkPlugins().mapping_detail_by_year(cclf9)
        row = out.to_dicts()[0]
        assert row["year"] == 2024
        assert row["unique_prvs_mbis"] == 2  # A, C
        assert row["unique_crnt_mbis"] == 3  # B, A, C
        assert row["total_xref_records"] == 3
        assert row["actual_crosswalks"] == 1
        assert row["self_mappings"] == 2

    @pytest.mark.unit
    def test_empty_input(self) -> None:
        out = CrosswalkPlugins().mapping_detail_by_year(_cclf9([]))
        assert out.height == 0
