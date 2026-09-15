"""Tests for _transforms.notebook_trends module."""

from __future__ import annotations

import polars as pl
import pytest

from acoharmony._transforms import _notebook_trends as notebook_trends
from acoharmony._transforms._notebook_trends import calculate_alignment_trends_over_time


class TestNotebookTrends:
    """Tests for notebook trends."""

    @pytest.mark.unit
    def test_import_module(self):
        assert notebook_trends is not None

    @pytest.mark.unit
    def test_calculate_alignment_trends_exists(self):
        assert callable(calculate_alignment_trends_over_time)

    @pytest.mark.unit
    def test_calculate_alignment_trends_empty(self):
        df = pl.LazyFrame({"col": [1]})
        result = calculate_alignment_trends_over_time(df, [])
        assert result is None


class TestNotebookTrendsDeep:
    """Deeper tests for notebook trends."""

    @pytest.mark.unit
    def test_trends_with_ym_columns(self):
        df = pl.LazyFrame(
            {
                "ym_202401_reach": [True, False, True],
                "ym_202401_mssp": [False, True, False],
                "ym_202402_reach": [True, True, True],
                "ym_202402_mssp": [False, False, True],
            }
        )
        calculate_alignment_trends_over_time(df, ["202401", "202402"])

    @pytest.mark.unit
    def test_trends_count_distinct_beneficiaries(self):
        df = pl.LazyFrame(
            {
                "current_mbi": ["M001", "M001", "M002", "M002", "M003"],
                "processed_at": [1, 2, 1, 2, 2],
                "ym_202401_reach": [True, True, False, False, False],
                "ym_202401_mssp": [False, False, True, True, False],
            }
        )

        result = calculate_alignment_trends_over_time(df, ["202401"])

        assert result is not None
        assert result.to_dicts() == [
            {
                "year_month": "2024-01",
                "REACH": 1,
                "MSSP": 1,
                "Total Aligned": 2,
            }
        ]

    @pytest.mark.unit
    def test_trends_keep_latest_duplicate_beneficiary_row(self):
        df = pl.LazyFrame(
            {
                "current_mbi": ["M001", "M001", "M002"],
                "processed_at": [1, 2, 2],
                "ym_202401_reach": [True, False, False],
                "ym_202401_mssp": [False, False, True],
            }
        )

        result = calculate_alignment_trends_over_time(df, ["202401"])

        assert result is not None
        assert result.to_dicts() == [
            {
                "year_month": "2024-01",
                "REACH": 0,
                "MSSP": 1,
                "Total Aligned": 1,
            }
        ]

    @pytest.mark.unit
    def test_trends_start_when_visible_program_series_have_data(self):
        df = pl.LazyFrame(
            {
                "current_mbi": ["M001", "M002"],
                "ym_202401_reach": [False, False],
                "ym_202401_mssp": [True, False],
                "ym_202402_reach": [True, False],
                "ym_202402_mssp": [True, False],
                "ym_202403_reach": [False, False],
                "ym_202403_mssp": [False, False],
            }
        )

        result = calculate_alignment_trends_over_time(df, ["202401", "202402", "202403"])

        assert result is not None
        assert result["year_month"].to_list() == ["2024-02"]


# ---------------------------------------------------------------------------
# Coverage gap tests: _notebook_trends.py line 65
# ---------------------------------------------------------------------------


class TestNotebookTrendsNoData:
    """Cover empty trend_data returning None."""

    @pytest.mark.unit
    def test_no_matching_columns_returns_none(self):
        """Line 65: no matching columns means empty trend_data -> returns None."""

        df = pl.LazyFrame({"unrelated_col": [1, 2, 3]})
        result = calculate_alignment_trends_over_time(df, ["202401"])
        assert result is None
