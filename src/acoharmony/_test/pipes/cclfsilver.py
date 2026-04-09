"""Tests for acoharmony._pipes._cclf_silver module."""


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
        assert acoharmony._pipes._cclf_silver is not None


class TestCCLFSilverPipeline:
    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._cclf_silver.execute_stage")
    @patch("acoharmony._pipes._cclf_silver.pl.scan_parquet")
    @pytest.mark.unit
    def test_successful_run(self, mock_scan, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._cclf_silver import apply_cclf_silver_pipeline

        executor = _make_executor(tmp_path)
        mock_exec.return_value = (None, 0)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/test.json")

        def mark_complete(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark_complete
        mock_cp_cls.return_value = checkpoint

        # Mock scan_parquet for row counting
        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=100))
        mock_scan.return_value = mock_lf

        result = apply_cclf_silver_pipeline(executor, logger, force=False)
        assert len(result) == 16  # 16 stages
        checkpoint.mark_pipeline_complete.assert_called_once()

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._cclf_silver.execute_stage")
    @pytest.mark.unit
    def test_skip_stage(self, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._cclf_silver import apply_cclf_silver_pipeline

        executor = _make_executor(tmp_path)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (True, 50)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/test.json")
        mock_cp_cls.return_value = checkpoint

        with patch("acoharmony._pipes._cclf_silver.pl.scan_parquet") as mock_scan:
            mock_lf = MagicMock()
            mock_lf.select.return_value = mock_lf
            mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=50))
            mock_scan.return_value = mock_lf

            apply_cclf_silver_pipeline(executor, logger)

        # execute_stage should not be called if all skipped
        mock_exec.assert_not_called()
        assert len(checkpoint.completed_stages) == 16

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._cclf_silver.execute_stage")
    @pytest.mark.unit
    def test_stage_failure(self, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._cclf_silver import apply_cclf_silver_pipeline

        executor = _make_executor(tmp_path)
        mock_exec.side_effect = RuntimeError("stage failed")

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/test.json")
        mock_cp_cls.return_value = checkpoint

        with pytest.raises(RuntimeError, match="stage failed"):
            apply_cclf_silver_pipeline(executor, logger)

        # Error logged
        logger.error.assert_called()
