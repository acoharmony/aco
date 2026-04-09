"""Tests for acoharmony._pipes._wound_care module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._test.pipes import _make_executor, _write_parquet


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._wound_care is not None


class TestWoundCarePipeline:
    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._wound_care.execute_stage")
    @patch("acoharmony._pipes._wound_care.pl.scan_parquet")
    @pytest.mark.unit
    def test_successful_run(self, mock_scan, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._wound_care import apply_wound_care_pipeline

        executor = _make_executor(tmp_path)
        mock_exec.return_value = (None, 0)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark
        mock_cp_cls.return_value = checkpoint

        gold = tmp_path / "gold"
        _write_parquet(gold / "wound_care_claims.parquet", 5)
        _write_parquet(gold / "skin_substitute_claims.parquet", 5)

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=5))
        mock_scan.return_value = mock_lf

        result = apply_wound_care_pipeline(executor, logger)
        assert len(result) == 2
        checkpoint.mark_pipeline_complete.assert_called_once()

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._wound_care.execute_stage")
    @pytest.mark.unit
    def test_stage_failure(self, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._wound_care import apply_wound_care_pipeline

        executor = _make_executor(tmp_path)
        mock_exec.side_effect = RuntimeError("wc fail")

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")
        mock_cp_cls.return_value = checkpoint

        with pytest.raises(RuntimeError, match="wc fail"):
            apply_wound_care_pipeline(executor, logger)


class TestWoundCarePipeSkipStage:
    """Cover skip stage in wound care pipeline."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._wound_care.execute_stage")
    @patch("acoharmony._pipes._wound_care.pl.scan_parquet")
    @pytest.mark.unit
    def test_wound_care_skip_completed_stage(self, mock_scan, mock_exec, mock_cp_cls, tmp_path):
        """Lines 96-98: skip stage when already completed."""
        from acoharmony._pipes._wound_care import apply_wound_care_pipeline

        executor = _make_executor(tmp_path)
        mock_logger = MagicMock()

        checkpoint = MagicMock()
        checkpoint.should_skip_stage.return_value = (True, 150)
        checkpoint.completed_stages = []
        checkpoint.get_tracking_file_path.return_value = "/tmp/tracking"
        mock_cp_cls.return_value = checkpoint

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=50))
        mock_scan.return_value = mock_lf

        apply_wound_care_pipeline(
            executor=executor,
            logger=mock_logger,
            force=False,
        )
        assert len(checkpoint.completed_stages) > 0
