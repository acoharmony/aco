"""Tests for acoharmony._transforms._notebook_enrollment_stats module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._notebook_enrollment_stats is not None


class TestCalculateCurrentEnrollmentStats:
    """Cover calculate_current_enrollment_stats lines 52-84."""

    @pytest.mark.unit
    def test_basic_stats(self):
        import polars as pl

        from acoharmony._transforms._notebook_enrollment_stats import (
            calculate_current_enrollment_stats,
        )

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2", "M3", "M4", "M5"],
            "ym_202401_reach": [True, False, False, False, False],
            "ym_202401_mssp": [False, True, False, False, False],
            "ym_202401_ffs": [False, False, True, False, False],
            "death_date": [None, None, None, "2024-01-15", None],
        }).lazy()

        stats = calculate_current_enrollment_stats(df, "202401")

        assert stats["reach"] == 1
        assert stats["mssp"] == 1
        assert stats["ffs"] == 1
        assert stats["deceased"] == 1
        assert stats["unknown"] == 1
        assert stats["total"] == 5
        # Validation: reach + mssp + ffs + deceased + unknown == total
        assert (stats["reach"] + stats["mssp"] + stats["ffs"]
                + stats["deceased"] + stats["unknown"]) == stats["total"]


class TestCalculateEnrollmentBreakdown:
    """Cover calculate_enrollment_breakdown lines 110-124."""

    @pytest.mark.unit
    def test_breakdown_returns_per_bene_status(self):
        import polars as pl

        from acoharmony._transforms._notebook_enrollment_stats import (
            calculate_enrollment_breakdown,
        )

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2", "M3"],
            "ym_202401_reach": [True, False, False],
            "ym_202401_mssp": [False, True, False],
            "ym_202401_ffs": [False, False, True],
            "death_date": [None, None, None],
        }).lazy()

        result = calculate_enrollment_breakdown(df, "202401", include_status_column=True)
        assert "current_mbi" in result.columns
        assert "is_reach" in result.columns
        assert "enrollment_status" in result.columns
        assert result.height == 3

    @pytest.mark.unit
    def test_breakdown_without_status_column(self):
        import polars as pl

        from acoharmony._transforms._notebook_enrollment_stats import (
            calculate_enrollment_breakdown,
        )

        df = pl.DataFrame({
            "current_mbi": ["M1"],
            "ym_202401_reach": [True],
            "ym_202401_mssp": [False],
            "ym_202401_ffs": [False],
            "death_date": [None],
        }).lazy()

        result = calculate_enrollment_breakdown(df, "202401", include_status_column=False)
        assert "enrollment_status" not in result.columns


class TestGetActivelyEnrolledDf:
    """Cover get_actively_enrolled_df lines 155-166."""

    @pytest.mark.unit
    def test_filter_specific_program(self):
        import polars as pl

        from acoharmony._transforms._notebook_enrollment_stats import (
            get_actively_enrolled_df,
        )

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2", "M3"],
            "ym_202401_reach": [True, False, False],
            "ym_202401_mssp": [False, True, False],
            "death_date": [None, None, None],
        }).lazy()

        result = get_actively_enrolled_df(df, "202401", "reach").collect()
        assert result.height == 1
        assert result["current_mbi"][0] == "M1"

    @pytest.mark.unit
    def test_filter_any_aco(self):
        import polars as pl

        from acoharmony._transforms._notebook_enrollment_stats import (
            get_actively_enrolled_df,
        )

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2", "M3"],
            "ym_202401_reach": [True, False, False],
            "ym_202401_mssp": [False, True, False],
            "death_date": [None, None, None],
        }).lazy()

        result = get_actively_enrolled_df(df, "202401", None).collect()
        assert result.height == 2


class TestGetLivingBeneficiariesDf:
    """Cover get_living_beneficiaries_df lines 183-185."""

    @pytest.mark.unit
    def test_filters_deceased(self):
        import polars as pl

        from acoharmony._transforms._notebook_enrollment_stats import (
            get_living_beneficiaries_df,
        )

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2"],
            "death_date": [None, "2024-01-15"],
        }).lazy()

        result = get_living_beneficiaries_df(df).collect()
        assert result.height == 1
        assert result["current_mbi"][0] == "M1"


class TestEnrollmentStatsValidationError:
    """Cover line 78: ValueError when enrollment counts don't sum to total."""

    @pytest.mark.unit
    def test_validation_mismatch_raises(self):
        from unittest.mock import patch as _patch
        import polars as pl
        from acoharmony._transforms._notebook_enrollment_stats import calculate_current_enrollment_stats

        df = pl.DataFrame({
            "current_mbi": ["M1"],
            "ym_202401_reach": [True],
            "ym_202401_mssp": [False],
            "ym_202401_ffs": [False],
            "death_date": [None],
        }).lazy()

        with _patch(
            "acoharmony._transforms._notebook_enrollment_stats.build_enrollment_counts_exprs"
        ) as mock_exprs:
            mock_exprs.return_value = [
                pl.lit(1).alias("reach_count"),
                pl.lit(1).alias("mssp_count"),
                pl.lit(1).alias("ffs_count"),
                pl.lit(1).alias("deceased_count"),
                pl.lit(1).alias("living_count"),
                pl.lit(0).alias("total_count"),
            ]
            with pytest.raises(ValueError, match="don't sum"):
                calculate_current_enrollment_stats(df, "202401")
