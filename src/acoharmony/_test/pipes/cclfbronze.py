"""Tests for acoharmony._pipes._cclf_bronze module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._pipes._cclf_bronze import apply_cclf_bronze_pipeline
from acoharmony.result import ResultStatus, TransformResult


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._cclf_bronze is not None


# ============================================================================
# 5. execute_bronze_stage (_cclf_bronze.py)
# ============================================================================


class TestExecuteBronzeStage:
    @pytest.mark.unit
    def test_success(self, logger):
        from acoharmony._pipes._builder import BronzeStage
        from acoharmony._pipes._cclf_bronze import execute_bronze_stage

        stage = BronzeStage("cclf1", "inst", 1, description="CCLF1 desc")
        runner = MagicMock()
        result = TransformResult(status=ResultStatus.SUCCESS, message="done")
        runner.transform_schema.return_value = result

        name, res = execute_bronze_stage(stage, runner, logger, force=True)
        assert name == "cclf1"
        assert res.success
        runner.transform_schema.assert_called_once_with("cclf1", force=True)
        # description logged
        assert any("CCLF1 desc" in str(c) for c in logger.info.call_args_list)

    @pytest.mark.unit
    def test_success_no_description(self, logger):
        from acoharmony._pipes._builder import BronzeStage
        from acoharmony._pipes._cclf_bronze import execute_bronze_stage

        stage = BronzeStage("cclf1", "inst", 1)
        runner = MagicMock()
        result = TransformResult(status=ResultStatus.SUCCESS, message="ok")
        runner.transform_schema.return_value = result

        name, res = execute_bronze_stage(stage, runner, logger)
        assert res.success

    @pytest.mark.unit
    def test_failure_result(self, logger):
        from acoharmony._pipes._builder import BronzeStage
        from acoharmony._pipes._cclf_bronze import execute_bronze_stage

        stage = BronzeStage("cclf1", "inst", 1, description="d")
        runner = MagicMock()
        result = TransformResult(status=ResultStatus.FAILURE, message="bad")
        runner.transform_schema.return_value = result

        name, res = execute_bronze_stage(stage, runner, logger)
        assert not res.success
        # Warning icon should appear
        assert any("\u26a0" in str(c) or "⚠" in str(c) for c in logger.info.call_args_list)

    @pytest.mark.unit
    def test_exception_optional_stage(self, logger):
        from acoharmony._pipes._builder import BronzeStage
        from acoharmony._pipes._cclf_bronze import execute_bronze_stage

        stage = BronzeStage("opt", "meta", 2, optional=True)
        runner = MagicMock()
        runner.transform_schema.side_effect = FileNotFoundError("no file")

        name, res = execute_bronze_stage(stage, runner, logger)
        assert name == "opt"
        assert res.status == ResultStatus.SKIPPED
        assert "Optional stage skipped" in res.message
        logger.warning.assert_called_once()

    @pytest.mark.unit
    def test_exception_required_stage(self, logger):
        from acoharmony._pipes._builder import BronzeStage
        from acoharmony._pipes._cclf_bronze import execute_bronze_stage

        stage = BronzeStage("req", "inst", 1, optional=False)
        runner = MagicMock()
        runner.transform_schema.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            execute_bronze_stage(stage, runner, logger)
        logger.error.assert_called_once()


# ============================================================================
# 6. CCLF Bronze Pipeline
# ============================================================================


class TestCCLFBronzePipeline:
    @patch("acoharmony._pipes._cclf_bronze.execute_bronze_stage")
    @pytest.mark.unit
    def test_all_succeed(self, mock_exec, logger):
        from acoharmony._pipes._cclf_bronze import apply_cclf_bronze_pipeline

        ok_result = TransformResult(status=ResultStatus.SUCCESS, message="ok")
        mock_exec.side_effect = lambda stage, runner, log, force=False: (stage.name, ok_result)

        results = apply_cclf_bronze_pipeline(MagicMock(), logger, force=False)
        assert len(results) == 13  # 13 stages
        assert all(r.success for r in results.values())

    @patch("acoharmony._pipes._cclf_bronze.execute_bronze_stage")
    @pytest.mark.unit
    def test_force_mode_logs(self, mock_exec, logger):
        ok_result = TransformResult(status=ResultStatus.SUCCESS, message="ok")
        mock_exec.side_effect = lambda stage, runner, log, force=False: (stage.name, ok_result)

        from acoharmony._pipes._cclf_bronze import apply_cclf_bronze_pipeline

        apply_cclf_bronze_pipeline(MagicMock(), logger, force=True)
        assert any("FORCE" in str(c) for c in logger.info.call_args_list)

    @patch("acoharmony._pipes._cclf_bronze.execute_bronze_stage")
    @pytest.mark.unit
    def test_mixed_results(self, mock_exec, logger):
        results_cycle = [
            TransformResult(status=ResultStatus.SUCCESS, message="ok"),
            TransformResult(status=ResultStatus.SKIPPED, message="skipped"),
            TransformResult(status=ResultStatus.FAILURE, message="fail"),
        ]
        call_count = [0]

        def side_effect(stage, runner, log, force=False):
            r = results_cycle[call_count[0] % len(results_cycle)]
            call_count[0] += 1
            return (stage.name, r)

        mock_exec.side_effect = side_effect

        results = apply_cclf_bronze_pipeline(MagicMock(), logger, force=False)
        assert len(results) == 13
        success_count = sum(1 for r in results.values() if r.success)
        skipped_count = sum(1 for r in results.values() if r.status.value == "skipped")
        failed_count = sum(
            1 for r in results.values() if not r.success and r.status.value != "skipped"
        )
        assert success_count > 0
        assert skipped_count > 0
        assert failed_count > 0
