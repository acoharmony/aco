"""Tests for acoharmony._pipes._bronze_all module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._pipes._bronze_all import apply_bronze_all_pipeline
from acoharmony.result import ResultStatus, TransformResult


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._bronze_all is not None


class TestBronzeAllPipeline:
    @patch("acoharmony._pipes._bronze_all.execute_bronze_stage")
    @pytest.mark.unit
    def test_runs_all_stages(self, mock_exec, logger):
        ok = TransformResult(status=ResultStatus.SUCCESS, message="ok")
        mock_exec.side_effect = lambda stage, runner, log, force=False: (stage.name, ok)

        from acoharmony._pipes._bronze_all import apply_bronze_all_pipeline

        results = apply_bronze_all_pipeline(MagicMock(), logger, force=False)
        assert len(results) > 40  # many stages
        assert mock_exec.call_count == len(results)

    @patch("acoharmony._pipes._bronze_all.execute_bronze_stage")
    @pytest.mark.unit
    def test_force_mode(self, mock_exec, logger):
        ok = TransformResult(status=ResultStatus.SUCCESS, message="ok")
        mock_exec.side_effect = lambda stage, runner, log, force=False: (stage.name, ok)

        apply_bronze_all_pipeline(MagicMock(), logger, force=True)
        assert any("FORCE" in str(c) for c in logger.info.call_args_list)
