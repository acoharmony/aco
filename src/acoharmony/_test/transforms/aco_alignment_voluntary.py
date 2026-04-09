"""Tests for _transforms.aco_alignment_voluntary module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import acoharmony


class TestAcoAlignmentVoluntary:
    """Tests for ACO alignment voluntary transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._aco_alignment_voluntary is not None

    @pytest.mark.unit
    def test_apply_transform_exists(self):
        assert callable(apply_transform)


class TestAcoAlignmentVoluntaryTransform:
    """Tests exercising the apply_transform function code paths."""

    def _make_base_df(self):
        return pl.DataFrame({
            "current_mbi": ["MBI1", "MBI2", "MBI3"],
            "current_program": ["REACH", "MSSP", "REACH"],
        }).lazy()

    def _make_voluntary_df(self):
        """Create voluntary alignment DataFrame with current_mbi column."""
        data = {
            "current_mbi": ["MBI1", "MBI3"],
            "sva_signature_count": [2, 0],
            "pbvar_aligned": [False, True],
            "most_recent_sva_date": [None, None],
            "sva_provider_name": ["Dr. A", None],
            "sva_provider_npi": ["NPI1", None],
            "sva_provider_tin": ["TIN1", None],
            "sva_provider_valid": [True, False],
            "first_sva_date": [None, None],
            "pbvar_response_codes": [None, "RC1"],
            "signature_status": ["active", None],
            "pbvar_file_date": [None, None],
            "email_unsubscribed": [False, False],
            "email_complained": [False, False],
            "ffs_first_date": [None, None],
        }
        return pl.DataFrame(data).lazy()

    @pytest.mark.unit
    def test_idempotency_skip(self):
        """When _voluntary_aligned already in schema, skips."""

        df = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "current_program": ["REACH"],
            "_voluntary_aligned": [True],
        }).lazy()
        catalog = MagicMock()
        logger = MagicMock()

        result = apply_transform(df, {}, catalog, logger, force=False)
        collected = result.collect()
        assert collected.height == 1

    @pytest.mark.unit
    def test_idempotency_force(self):
        """When force=True, processes even if _voluntary_aligned present."""

        df = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "current_program": ["REACH"],
            "_voluntary_aligned": [True],
        }).lazy()
        catalog = MagicMock()
        catalog.scan_table.return_value = self._make_voluntary_df()
        logger = MagicMock()

        result = apply_transform(df, {}, catalog, logger, force=True)
        collected = result.collect()
        assert "_voluntary_aligned" in collected.columns
        # Should have the right suffix columns from the left join
        assert "has_valid_voluntary_alignment" in collected.columns

    @pytest.mark.unit
    def test_voluntary_df_none_raises(self):
        """Raises ValueError when voluntary_alignment source not found."""

        catalog = MagicMock()
        catalog.scan_table.return_value = None
        logger = MagicMock()

        with pytest.raises(ValueError, match="voluntary_alignment source not found"):
            apply_transform(self._make_base_df(), {}, catalog, logger)

    @pytest.mark.unit
    def test_join_with_current_mbi_column(self):
        """Joins voluntary data when it has 'current_mbi' column (no rename needed)."""

        catalog = MagicMock()
        catalog.scan_table.return_value = self._make_voluntary_df()
        logger = MagicMock()

        result = apply_transform(self._make_base_df(), {}, catalog, logger)
        collected = result.collect()
        assert "_voluntary_aligned" in collected.columns
        assert "has_valid_voluntary_alignment" in collected.columns
        assert collected.height == 3

    @pytest.mark.unit
    def test_join_produces_correct_flags(self):
        """Validates the join result has expected voluntary alignment data."""

        catalog = MagicMock()
        catalog.scan_table.return_value = self._make_voluntary_df()
        logger = MagicMock()

        result = apply_transform(self._make_base_df(), {}, catalog, logger)
        collected = result.collect()
        # MBI1 has sva_signature_count=2, sva_provider_valid=True, program=REACH -> valid
        mbi1 = collected.filter(pl.col("current_mbi") == "MBI1")
        assert mbi1["has_valid_voluntary_alignment"][0] is True
        # MBI2 has no voluntary data (left join null) -> not valid
        mbi2 = collected.filter(pl.col("current_mbi") == "MBI2")
        assert mbi2["has_valid_voluntary_alignment"][0] is False

    @pytest.mark.unit
    def test_transform_marks_processed(self):
        """Transform adds _voluntary_aligned flag."""

        catalog = MagicMock()
        catalog.scan_table.return_value = self._make_voluntary_df()
        logger = MagicMock()

        result = apply_transform(self._make_base_df(), {}, catalog, logger)
        collected = result.collect()
        assert all(v is True for v in collected["_voluntary_aligned"].to_list())


# ---------------------------------------------------------------------------
# Coverage gap tests: _aco_alignment_voluntary.py lines 64, 66
# ---------------------------------------------------------------------------


class TestVoluntaryAlignmentMBIRename:
    """Cover the mbi rename branches (lines 63->64, 65->66)."""

    def _base_df(self):
        return pl.DataFrame({
            "current_mbi": ["MBI001"],
            "current_program": ["REACH"],
        }).lazy()

    @pytest.mark.unit
    def test_mbi_rename_branch(self):
        """Line 63->64: 'mbi' column is renamed to 'current_mbi'."""

        # Voluntary source with 'mbi' (not 'current_mbi')
        voluntary_df = pl.DataFrame({
            "mbi": ["MBI001"],
            "sva_signature_count": [1],
            "pbvar_aligned": [False],
            "most_recent_sva_date": [None],
            "sva_provider_name": ["Dr. X"],
            "sva_provider_npi": ["1234567890"],
            "sva_provider_tin": ["TIN1"],
            "sva_provider_valid": [True],
            "first_sva_date": [None],
            "pbvar_response_codes": [None],
            "signature_status": ["active"],
            "pbvar_file_date": [None],
            "email_unsubscribed": [False],
            "email_complained": [False],
            "ffs_first_date": [None],
        }).lazy()

        # Patch build_voluntary_alignment_select_expr to use 'mbi' instead of 'current_mbi'
        patched_exprs = [
            pl.col("mbi"),  # keep 'mbi' so the rename branch triggers
            (pl.col("sva_signature_count") > 0).alias("has_voluntary_alignment"),
            pl.when(pl.col("sva_signature_count") > 0).then(pl.lit("SVA")).when(pl.col("pbvar_aligned")).then(pl.lit("PBVAR")).otherwise(None).alias("voluntary_alignment_type"),
            pl.col("most_recent_sva_date").alias("voluntary_alignment_date"),
            pl.col("sva_provider_name").alias("voluntary_provider_name"),
            pl.col("sva_provider_npi").alias("voluntary_provider_npi"),
            pl.col("sva_provider_tin").alias("voluntary_provider_tin"),
            pl.col("sva_provider_valid").alias("sva_provider_valid"),
            pl.col("first_sva_date").alias("first_valid_signature_date"),
            pl.col("most_recent_sva_date").alias("last_valid_signature_date"),
            pl.col("first_sva_date").alias("first_sva_submission_date"),
            pl.col("most_recent_sva_date").alias("last_sva_submission_date"),
            pl.col("pbvar_response_codes").alias("latest_response_codes"),
            pl.col("signature_status").alias("latest_response_detail"),
            pl.col("pbvar_file_date").alias("pbvar_report_date"),
            pl.col("email_unsubscribed").alias("has_email_opt_out"),
            pl.col("email_complained").alias("has_mail_opt_out"),
            pl.col("ffs_first_date").alias("voluntary_ffs_date"),
        ]

        catalog = MagicMock()
        catalog.scan_table.return_value = voluntary_df
        logger = MagicMock()

        with patch(
            "acoharmony._transforms._aco_alignment_voluntary.build_voluntary_alignment_select_expr",
            return_value=patched_exprs,
        ):
            result = apply_transform(self._base_df(), {}, catalog, logger)
            collected = result.collect()
            assert "current_mbi" in collected.columns
            assert "has_valid_voluntary_alignment" in collected.columns

    @pytest.mark.unit
    def test_normalized_mbi_rename_branch(self):
        """Line 65->66: 'normalized_mbi' column is renamed to 'current_mbi'."""

        # Voluntary source with 'normalized_mbi' (not 'current_mbi' or 'mbi')
        voluntary_df = pl.DataFrame({
            "normalized_mbi": ["MBI001"],
            "sva_signature_count": [1],
            "pbvar_aligned": [False],
            "most_recent_sva_date": [None],
            "sva_provider_name": ["Dr. X"],
            "sva_provider_npi": ["1234567890"],
            "sva_provider_tin": ["TIN1"],
            "sva_provider_valid": [True],
            "first_sva_date": [None],
            "pbvar_response_codes": [None],
            "signature_status": ["active"],
            "pbvar_file_date": [None],
            "email_unsubscribed": [False],
            "email_complained": [False],
            "ffs_first_date": [None],
        }).lazy()

        # Patch build_voluntary_alignment_select_expr to use 'normalized_mbi'
        patched_exprs = [
            pl.col("normalized_mbi"),  # keep 'normalized_mbi' so the rename branch triggers
            (pl.col("sva_signature_count") > 0).alias("has_voluntary_alignment"),
            pl.when(pl.col("sva_signature_count") > 0).then(pl.lit("SVA")).when(pl.col("pbvar_aligned")).then(pl.lit("PBVAR")).otherwise(None).alias("voluntary_alignment_type"),
            pl.col("most_recent_sva_date").alias("voluntary_alignment_date"),
            pl.col("sva_provider_name").alias("voluntary_provider_name"),
            pl.col("sva_provider_npi").alias("voluntary_provider_npi"),
            pl.col("sva_provider_tin").alias("voluntary_provider_tin"),
            pl.col("sva_provider_valid").alias("sva_provider_valid"),
            pl.col("first_sva_date").alias("first_valid_signature_date"),
            pl.col("most_recent_sva_date").alias("last_valid_signature_date"),
            pl.col("first_sva_date").alias("first_sva_submission_date"),
            pl.col("most_recent_sva_date").alias("last_sva_submission_date"),
            pl.col("pbvar_response_codes").alias("latest_response_codes"),
            pl.col("signature_status").alias("latest_response_detail"),
            pl.col("pbvar_file_date").alias("pbvar_report_date"),
            pl.col("email_unsubscribed").alias("has_email_opt_out"),
            pl.col("email_complained").alias("has_mail_opt_out"),
            pl.col("ffs_first_date").alias("voluntary_ffs_date"),
        ]

        catalog = MagicMock()
        catalog.scan_table.return_value = voluntary_df
        logger = MagicMock()

        with patch(
            "acoharmony._transforms._aco_alignment_voluntary.build_voluntary_alignment_select_expr",
            return_value=patched_exprs,
        ):
            result = apply_transform(self._base_df(), {}, catalog, logger)
            collected = result.collect()
            assert "current_mbi" in collected.columns
            assert "has_valid_voluntary_alignment" in collected.columns
