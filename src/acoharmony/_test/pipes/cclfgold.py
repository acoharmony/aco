"""Tests for acoharmony._pipes._cclf_gold module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._test.pipes import _make_executor


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._cclf_gold is not None


# ============================================================================
# 10. CCLF Gold Pipeline
# ============================================================================


class TestCCLFGoldPipeline:
    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._cclf_gold.execute_stage")
    @patch("acoharmony._pipes._cclf_gold.pl.scan_parquet")
    @pytest.mark.unit
    def test_successful_run(self, mock_scan, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._cclf_gold import apply_cclf_gold_pipeline

        executor = _make_executor(tmp_path)
        mock_exec.return_value = (None, 0)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark_complete(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark_complete
        mock_cp_cls.return_value = checkpoint

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=50))
        mock_scan.return_value = mock_lf

        result = apply_cclf_gold_pipeline(executor, logger)
        assert len(result) == 3
        checkpoint.mark_pipeline_complete.assert_called_once()

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._cclf_gold.execute_stage")
    @pytest.mark.unit
    def test_stage_failure(self, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._cclf_gold import apply_cclf_gold_pipeline

        executor = _make_executor(tmp_path)
        mock_exec.side_effect = RuntimeError("boom")

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")
        mock_cp_cls.return_value = checkpoint

        with pytest.raises(RuntimeError, match="boom"):
            apply_cclf_gold_pipeline(executor, logger)


class TestCCLFGoldPipeSkipStage:
    """Cover skip stage in CCLF gold pipeline."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._cclf_gold.execute_stage")
    @patch("acoharmony._pipes._cclf_gold.pl.scan_parquet")
    @pytest.mark.unit
    def test_cclf_gold_skip_completed_stage(self, mock_scan, mock_exec, mock_cp_cls, tmp_path):
        """Lines 93-95: skip stage when already completed."""
        from acoharmony._pipes._cclf_gold import apply_cclf_gold_pipeline

        executor = _make_executor(tmp_path)
        mock_logger = MagicMock()

        checkpoint = MagicMock()
        checkpoint.should_skip_stage.return_value = (True, 200)
        checkpoint.completed_stages = []
        checkpoint.get_tracking_file_path.return_value = "/tmp/tracking"
        mock_cp_cls.return_value = checkpoint

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=50))
        mock_scan.return_value = mock_lf

        apply_cclf_gold_pipeline(
            executor=executor,
            logger=mock_logger,
            force=False,
        )
        assert len(checkpoint.completed_stages) > 0
