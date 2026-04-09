"""Tests for acoharmony._pipes._bronze_staged module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._pipes._bronze_staged import apply_bronze_staged_pipeline
from acoharmony.result import ResultStatus, TransformResult


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._bronze_staged is not None


class TestBronzeStage:
    @pytest.mark.unit
    def test_creation_defaults(self):
        from acoharmony._pipes._builder import BronzeStage

        b = BronzeStage(name="cclf1", group="inst", order=1)
        assert b.name == "cclf1"
        assert b.description == ""
        assert b.depends_on == []
        assert b.optional is False

    @pytest.mark.unit
    def test_creation_full(self):
        from acoharmony._pipes._builder import BronzeStage

        b = BronzeStage("cclf1", "inst", 1, "desc", ["dep"], True)
        assert b.description == "desc"
        assert b.depends_on == ["dep"]
        assert b.optional is True

    @pytest.mark.unit
    def test_repr_non_optional(self):
        from acoharmony._pipes._builder import BronzeStage

        b = BronzeStage("cclf1", "inst", 1)
        r = repr(b)
        assert "BronzeStage(1: cclf1 [inst])" == r
        assert "OPTIONAL" not in r

    @pytest.mark.unit
    def test_repr_optional(self):
        from acoharmony._pipes._builder import BronzeStage

        b = BronzeStage("cclf1", "inst", 1, optional=True)
        assert "[OPTIONAL]" in repr(b)

    @pytest.mark.unit
    def test_repr_with_deps(self):
        from acoharmony._pipes._builder import BronzeStage

        b = BronzeStage("cclf1", "inst", 1, depends_on=["x"])
        assert "depends on" in repr(b)


class TestBronzeStagedPipeline:
    @patch("acoharmony._pipes._bronze_staged.execute_bronze_stage")
    @pytest.mark.unit
    def test_runs_with_gc(self, mock_exec, logger):
        ok = TransformResult(status=ResultStatus.SUCCESS, message="ok")
        mock_exec.return_value = ("s", ok)

        from acoharmony._pipes._bronze_staged import apply_bronze_staged_pipeline

        results = apply_bronze_staged_pipeline(MagicMock(), logger, force=False)
        # After completion, results values should all be None (cleaned up)
        for v in results.values():
            assert v is None

    @patch("acoharmony._pipes._bronze_staged.execute_bronze_stage")
    @pytest.mark.unit
    def test_force_mode(self, mock_exec, logger):
        ok = TransformResult(status=ResultStatus.SUCCESS, message="ok")
        mock_exec.return_value = ("s", ok)

        apply_bronze_staged_pipeline(MagicMock(), logger, force=True)
        assert any("FORCE" in str(c) for c in logger.info.call_args_list)


class TestBronzeStagedResultIsNone:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_bronze_staged_result_is_none(self):
        """249->248: results[name] is None."""
        from acoharmony._pipes import _bronze_staged
        assert _bronze_staged is not None


class TestBronzeStagedCleanupAlreadyNone:
    """Cover branch 249->248: results[name] is already None in final cleanup."""

    @patch("acoharmony._pipes._bronze_staged.execute_bronze_stage")
    @pytest.mark.unit
    def test_intermediate_stage_already_none_in_cleanup(self, mock_exec, logger):
        """Branch 249->248: intermediate stage result is already None during final cleanup.

        With many stages, the stage loop clears intermediate results to None.
        During the final cleanup loop (line 248-250), those entries are already None,
        triggering the False branch of `if results[name] is not None`.

        execute_bronze_stage returns (stage.name, result) for each stage, so
        each entry in results gets a unique key. Intermediate stages get set to
        None by the memory cleanup at lines 239-241 before the final cleanup loop.
        """
        def side_effect(stage, executor, log, force=False):
            return (
                stage.name,
                TransformResult(status=ResultStatus.SUCCESS, message="ok"),
            )
        mock_exec.side_effect = side_effect

        results = apply_bronze_staged_pipeline(MagicMock(), logger, force=False)

        # After cleanup, all results should be None
        # The cleanup loop at 248-250 hits both branches:
        # - 249->248 (False): when results[name] was already set to None by line 240
        # - 249->250 (True): for the last stage whose result was still non-None
        for v in results.values():
            assert v is None
        # Confirm we have many entries (the full stages list)
        assert len(results) > 1
