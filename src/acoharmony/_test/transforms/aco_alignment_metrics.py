"""Tests for acoharmony._transforms._aco_alignment_metrics module."""



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
        assert acoharmony._transforms._aco_alignment_metrics is not None


class TestApplyTransformMetrics:
    """Cover apply_transform lines 65-102."""

    @pytest.mark.unit
    def test_full_metrics_calculation(self):
        """Full metrics transform with all required columns."""
        from datetime import date
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_metrics import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1", "M2"],
            "bene_mbi": ["M1", "M2_OLD"],
            "ever_reach": [True, False],
            "ever_mssp": [False, True],
            "months_in_reach": [6, 0],
            "months_in_mssp": [0, 12],
            "has_valid_voluntary_alignment": [True, False],
            "has_voluntary_alignment": [True, False],
            "current_program": ["REACH", "MSSP"],
            "enrollment_gaps": [0, 1],
            "death_date": [None, None],
            "last_sva_submission_date": [date(2024, 6, 1), None],
            "pbvar_report_date": [date(2024, 3, 1), None],
            "latest_response_codes": ["A01", None],
            "voluntary_provider_tin": ["TIN1", None],
            "voluntary_provider_npi": ["NPI1", None],
            "aligned_provider_tin": ["TIN1", None],
            "aligned_provider_npi": ["NPI2", None],
            "program_switches": [0, 1],
        }).lazy()

        result = apply_transform(df, {}, MagicMock(), MagicMock(), force=True).collect()

        assert "consolidated_program" in result.columns
        assert "total_aligned_months" in result.columns
        assert "is_currently_aligned" in result.columns
        assert "has_program_transition" in result.columns
        assert "prvs_num" in result.columns
        assert "_metrics_calculated" in result.columns
        assert result.height == 2

    @pytest.mark.unit
    def test_idempotency_skip(self):
        """Cover lines 65-67: already calculated → skip."""
        from unittest.mock import MagicMock

        import polars as pl

        from acoharmony._transforms._aco_alignment_metrics import apply_transform

        df = pl.DataFrame({
            "current_mbi": ["M1"],
            "_metrics_calculated": [True],
        }).lazy()

        result = apply_transform(df, {}, MagicMock(), MagicMock(), force=False)
        assert result.collect().height == 1
