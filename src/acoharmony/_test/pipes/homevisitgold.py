"""Tests for _pipes/_home_visit_gold.py (83.8% covered, 12 missing lines).

Focus on stage skip, stage failure, and force flag paths not covered
by test_coverage.py.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from acoharmony._pipes._registry import PipelineRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_executor(tmp_path):
    """Create a mock executor with storage paths rooted in tmp_path."""
    from acoharmony.medallion import MedallionLayer

    executor = MagicMock()
    storage = MagicMock()

    silver_path = tmp_path / "silver"
    silver_path.mkdir(parents=True, exist_ok=True)
    gold_path = tmp_path / "gold"
    gold_path.mkdir(parents=True, exist_ok=True)

    def get_path(tier):
        if tier == MedallionLayer.SILVER or tier == "silver":
            return silver_path
        if tier == MedallionLayer.GOLD or tier == "gold":
            return gold_path
        return tmp_path

    storage.get_path.side_effect = get_path
    executor.storage_config = storage
    return executor


def _write_parquet(path, n_rows=5):
    """Write a simple parquet file with n_rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"col_a": list(range(n_rows))}).write_parquet(path)


@pytest.fixture(autouse=True)
def _clean_registry():
    PipelineRegistry.clear()
    yield
    PipelineRegistry.clear()


@pytest.fixture
def logger():
    return MagicMock(spec=logging.Logger)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHomeVisitGoldSkipPath:
    """Cover the should_skip=True branch (lines ~106-110)."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._home_visit_gold.execute_stage")
    @patch("acoharmony._pipes._home_visit_gold.pl.scan_parquet")
    @pytest.mark.unit
    def test_stage_skipped_when_already_complete(
        self, mock_scan, mock_exec, mock_cp_cls, tmp_path, logger
    ):
        from acoharmony._pipes._home_visit_gold import (
            apply_home_visit_gold_pipeline,
        )

        executor = _make_executor(tmp_path)
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        _write_parquet(silver / "int_physician_claim_deduped.parquet")
        _write_parquet(gold / "medical_claim.parquet")

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        # Stage is already complete - should skip
        checkpoint.should_skip_stage.return_value = (True, 5)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark_skip(name):
            checkpoint.completed_stages.append(name)

        # The skipped stage still gets appended via the skip branch
        checkpoint.should_skip_stage.side_effect = lambda name, path, lg: (
            True,
            5,
        )

        mock_cp_cls.return_value = checkpoint

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=5))
        mock_scan.return_value = mock_lf

        # The skip branch appends to checkpoint.completed_stages in source code
        # We need to simulate that the code does
        # checkpoint.completed_stages.append(stage.name)
        # Since completed_stages is a real list, the code mutates it directly
        result = apply_home_visit_gold_pipeline(executor, logger)

        # execute_stage should NOT be called since stage was skipped
        mock_exec.assert_not_called()
        assert isinstance(result, dict)


class TestHomeVisitGoldStageFail:
    """Cover the stage failure + logging path (lines ~117-125)."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._home_visit_gold.execute_stage")
    @pytest.mark.unit
    def test_stage_failure_raises_and_logs(self, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._home_visit_gold import (
            apply_home_visit_gold_pipeline,
        )

        executor = _make_executor(tmp_path)
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        _write_parquet(silver / "int_physician_claim_deduped.parquet")
        _write_parquet(gold / "medical_claim.parquet")

        mock_exec.side_effect = RuntimeError("transform exploded")

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")
        mock_cp_cls.return_value = checkpoint

        with pytest.raises(RuntimeError, match="transform exploded"):
            apply_home_visit_gold_pipeline(executor, logger)

        # Verify error logging happened
        logger.error.assert_called()
        # Verify the "Pipeline STOPPED" info messages
        info_calls = [str(c) for c in logger.info.call_args_list]
        assert any("STOPPED" in s for s in info_calls)


class TestHomeVisitGoldForceFlag:
    """Cover the force=True path."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._home_visit_gold.execute_stage")
    @patch("acoharmony._pipes._home_visit_gold.pl.scan_parquet")
    @pytest.mark.unit
    def test_force_reprocess(self, mock_scan, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._home_visit_gold import (
            apply_home_visit_gold_pipeline,
        )

        executor = _make_executor(tmp_path)
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        _write_parquet(silver / "int_physician_claim_deduped.parquet")
        _write_parquet(gold / "medical_claim.parquet")

        mock_exec.return_value = (None, 0)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark
        mock_cp_cls.return_value = checkpoint

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=5))
        mock_scan.return_value = mock_lf

        result = apply_home_visit_gold_pipeline(executor, logger, force=True)
        assert "home_visit_claims" in result
        # PipelineCheckpoint was created with force=True
        mock_cp_cls.assert_called_with("home_visit_gold", force=True)


# ============================================================================
# 14. Home Visit Gold Pipeline
# ============================================================================


class TestHomeVisitGoldPipeline:
    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._home_visit_gold.execute_stage")
    @patch("acoharmony._pipes._home_visit_gold.pl.scan_parquet")
    @pytest.mark.unit
    def test_successful_run(self, mock_scan, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._home_visit_gold import apply_home_visit_gold_pipeline

        executor = _make_executor(tmp_path)
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        _write_parquet(silver / "int_physician_claim_deduped.parquet", 5)
        _write_parquet(gold / "medical_claim.parquet", 5)

        mock_exec.return_value = (None, 0)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark
        mock_cp_cls.return_value = checkpoint

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=5))
        mock_scan.return_value = mock_lf

        result = apply_home_visit_gold_pipeline(executor, logger)
        assert "home_visit_claims" in result
        checkpoint.mark_pipeline_complete.assert_called_once()

    @pytest.mark.unit
    def test_missing_silver_prerequisite(self, tmp_path, logger):
        from acoharmony._pipes._home_visit_gold import apply_home_visit_gold_pipeline

        executor = _make_executor(tmp_path)
        # No silver prereq file
        gold = tmp_path / "gold"
        _write_parquet(gold / "medical_claim.parquet", 5)

        with pytest.raises(FileNotFoundError, match="int_physician_claim_deduped"):
            apply_home_visit_gold_pipeline(executor, logger)

    @pytest.mark.unit
    def test_missing_gold_prerequisite(self, tmp_path, logger):
        from acoharmony._pipes._home_visit_gold import apply_home_visit_gold_pipeline

        executor = _make_executor(tmp_path)
        silver = tmp_path / "silver"
        _write_parquet(silver / "int_physician_claim_deduped.parquet", 5)
        # No gold prereq file

        with pytest.raises(FileNotFoundError, match="medical_claim"):
            apply_home_visit_gold_pipeline(executor, logger)
