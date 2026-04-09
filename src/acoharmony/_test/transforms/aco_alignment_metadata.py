"""Tests for _transforms.aco_alignment_metadata module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch as mock_patch

import polars as pl
import pytest
import acoharmony


class TestAcoAlignmentMetadata:
    """Tests for ACO alignment metadata transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._aco_alignment_metadata is not None

    @pytest.mark.unit
    def test_apply_transform_exists(self):
        assert callable(apply_transform)


class TestAcoAlignmentMetadataTransform:
    """Tests exercising the apply_transform function code paths."""

    def _make_df(self):
        return pl.DataFrame({
            "current_mbi": ["MBI1", "MBI2"],
            "last_valid_signature_date": [None, None],
            "latest_response_codes": [None, "RC01"],
            "has_voluntary_alignment": [True, False],
            "sva_provider_valid": [True, False],
            "current_program": ["REACH", "MSSP"],
            "is_currently_aligned": [True, True],
            "sva_action_needed": [False, True],
            "has_demographics": [True, True],
            "data_effective_date": [None, None],
            "data_through_date": [None, None],
        }).lazy()

    @pytest.mark.unit
    def test_idempotency_skip(self):
        """When _metadata_added already in schema, skips."""

        df = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "_metadata_added": [True],
        }).lazy()
        catalog = MagicMock()
        logger = MagicMock()

        result = apply_transform(df, {}, catalog, logger, force=False)
        collected = result.collect()
        assert collected.height == 1

    @pytest.mark.unit
    def test_idempotency_force(self):
        """When force=True, processes even if _metadata_added present."""


        df = self._make_df().with_columns(pl.lit(True).alias("_metadata_added"))
        catalog = MagicMock()
        logger = MagicMock()

        # Mock the expression builders to avoid needing all columns
        with mock_patch("acoharmony._transforms._aco_alignment_metadata.SignatureLifecycleExpression") as mock_sig, \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.ResponseCodeParserExpression") as mock_rc, \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_data_completeness_expr", return_value=pl.lit("complete").alias("data_completeness")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_lineage_transform_expr", return_value=pl.lit("metadata").alias("lineage_transform")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_lineage_processed_at_expr", return_value=pl.lit("now").alias("lineage_processed_at")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_data_date_exprs", return_value=[pl.lit(None).alias("data_start_date"), pl.lit(None).alias("data_end_date")]), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_source_tables_expr", return_value=pl.lit("tables").alias("source_tables")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_last_updated_expr", return_value=pl.lit("now").alias("last_updated")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_has_opt_out_expr", return_value=pl.lit(False).alias("has_opt_out")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_sva_action_needed_expr", return_value=pl.lit(False).alias("sva_action_needed")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_outreach_priority_expr", return_value=pl.lit("none").alias("outreach_priority")):
            mock_sig.calculate_signature_lifecycle.return_value = [pl.lit(None).alias("sig_lifecycle")]
            mock_rc.parse_response_codes.return_value = [pl.lit(None).alias("parsed_codes")]

            result = apply_transform(df, {}, catalog, logger, force=True)
            collected = result.collect()
            assert "_metadata_added" in collected.columns

    @pytest.mark.unit
    def test_fresh_transform(self):
        """Applies all metadata when no _metadata_added flag."""


        catalog = MagicMock()
        logger = MagicMock()

        with mock_patch("acoharmony._transforms._aco_alignment_metadata.SignatureLifecycleExpression") as mock_sig, \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.ResponseCodeParserExpression") as mock_rc, \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_data_completeness_expr", return_value=pl.lit("complete").alias("data_completeness")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_lineage_transform_expr", return_value=pl.lit("metadata").alias("lineage_transform")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_lineage_processed_at_expr", return_value=pl.lit("now").alias("lineage_processed_at")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_data_date_exprs", return_value=[pl.lit(None).alias("data_start_date"), pl.lit(None).alias("data_end_date")]), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_source_tables_expr", return_value=pl.lit("tables").alias("source_tables")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_last_updated_expr", return_value=pl.lit("now").alias("last_updated")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_has_opt_out_expr", return_value=pl.lit(False).alias("has_opt_out")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_sva_action_needed_expr", return_value=pl.lit(False).alias("sva_action_needed")), \
             mock_patch("acoharmony._transforms._aco_alignment_metadata.build_outreach_priority_expr", return_value=pl.lit("none").alias("outreach_priority")):
            mock_sig.calculate_signature_lifecycle.return_value = [pl.lit(None).alias("sig_lifecycle")]
            mock_rc.parse_response_codes.return_value = [pl.lit(None).alias("parsed_codes")]

            result = apply_transform(self._make_df(), {}, catalog, logger)
            collected = result.collect()
            assert "_metadata_added" in collected.columns
            assert collected["_metadata_added"][0] is True
