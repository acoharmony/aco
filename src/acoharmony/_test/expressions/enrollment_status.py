# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._enrollment_status module."""

import polars as pl
import pytest

from acoharmony._expressions._enrollment_status import (
    build_active_enrollment_expr,
    build_enrollment_counts_exprs,
    build_enrollment_status_expr,
    build_living_beneficiary_expr,
)


class TestBuildLivingBeneficiaryExpr:
    """Tests for build_living_beneficiary_expr."""

    @pytest.mark.unit
    def test_no_death_columns(self):
        """No death columns → all alive."""
        df = pl.DataFrame({"id": [1, 2]})
        expr = build_living_beneficiary_expr(df.columns)
        result = df.with_columns(expr.alias("alive"))
        assert result["alive"].to_list() == [True, True]

    @pytest.mark.unit
    def test_death_date_null_means_alive(self):
        """death_date null → alive."""
        df = pl.DataFrame({"death_date": [None, "2024-01-01"]})
        expr = build_living_beneficiary_expr(df.columns)
        result = df.select(expr.alias("alive"))
        assert result["alive"].to_list() == [True, False]


class TestBuildActiveEnrollmentExpr:
    """Tests for build_active_enrollment_expr."""

    @pytest.mark.unit
    def test_column_missing_returns_false(self):
        """Missing enrollment column → lit(False)."""
        df = pl.DataFrame({"id": [1, 2]})
        expr = build_active_enrollment_expr("202401", "reach", df.columns)
        result = df.with_columns(expr.alias("active"))
        assert result["active"].to_list() == [False, False]

    @pytest.mark.unit
    def test_enrolled_and_alive(self):
        """Enrolled and no death columns → active."""
        df = pl.DataFrame({"ym_202401_reach": [True, False, True]})
        expr = build_active_enrollment_expr("202401", "reach", df.columns)
        result = df.select(expr.alias("active"))
        assert result["active"].to_list() == [True, False, True]

    @pytest.mark.unit
    def test_enrolled_but_deceased(self):
        """Enrolled but has death_date → not active."""
        df = pl.DataFrame({
            "ym_202401_reach": [True, True],
            "death_date": [None, "2024-01-15"],
        })
        expr = build_active_enrollment_expr("202401", "reach", df.columns)
        result = df.select(expr.alias("active"))
        assert result["active"].to_list() == [True, False]


class TestBuildEnrollmentStatusExpr:
    """Tests for build_enrollment_status_expr."""

    @pytest.mark.unit
    def test_status_categories(self):
        """Correct status assignment by priority."""
        df = pl.DataFrame({
            "ym_202401_reach": [True, False, False, False, False],
            "ym_202401_mssp": [False, True, False, False, False],
            "ym_202401_ffs": [False, False, True, False, False],
            "death_date": [None, None, None, "2024-01-01", None],
        })
        expr = build_enrollment_status_expr("202401", df.columns)
        result = df.select(expr.alias("status"))
        statuses = result["status"].to_list()
        assert statuses == ["REACH", "MSSP", "FFS", "Deceased", "Unknown"]

    @pytest.mark.unit
    def test_deceased_overrides_enrollment(self):
        """Deceased has highest priority even if enrolled."""
        df = pl.DataFrame({
            "ym_202401_reach": [True],
            "ym_202401_mssp": [False],
            "ym_202401_ffs": [False],
            "death_date": ["2024-01-01"],
        })
        expr = build_enrollment_status_expr("202401", df.columns)
        result = df.select(expr.alias("status"))
        assert result["status"][0] == "Deceased"

    @pytest.mark.unit
    def test_no_enrollment_columns(self):
        """No enrollment columns → Unknown for living, Deceased for dead."""
        df = pl.DataFrame({"death_date": [None, "2024-01-01"]})
        expr = build_enrollment_status_expr("202401", df.columns)
        result = df.select(expr.alias("status"))
        assert result["status"].to_list() == ["Unknown", "Deceased"]


class TestBuildEnrollmentCountsExprs:
    """Tests for build_enrollment_counts_exprs."""

    @pytest.mark.unit
    def test_counts(self):
        """Count expressions produce correct aggregates."""
        df = pl.DataFrame({
            "ym_202401_reach": [True, True, False, False],
            "ym_202401_mssp": [False, False, True, False],
            "ym_202401_ffs": [False, False, False, True],
            "death_date": [None, None, None, None],
        })
        exprs = build_enrollment_counts_exprs("202401", df.columns)
        result = df.select(exprs)
        assert result["reach_count"][0] == 2
        assert result["mssp_count"][0] == 1
        assert result["ffs_count"][0] == 1
        assert result["deceased_count"][0] == 0
        assert result["living_count"][0] == 4
        assert result["total_count"][0] == 4

    @pytest.mark.unit
    def test_counts_with_deceased(self):
        """Deceased counted correctly."""
        df = pl.DataFrame({
            "ym_202401_reach": [True, False],
            "ym_202401_mssp": [False, False],
            "ym_202401_ffs": [False, False],
            "death_date": [None, "2024-01-01"],
        })
        exprs = build_enrollment_counts_exprs("202401", df.columns)
        result = df.select(exprs)
        assert result["reach_count"][0] == 1
        assert result["deceased_count"][0] == 1
        assert result["living_count"][0] == 1
