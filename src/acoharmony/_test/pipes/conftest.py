# © 2025 HarmonyCares – Shared fixtures for _pipes tests
"""Pytest fixtures for acoharmony._pipes tests."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from acoharmony._pipes._registry import PipelineRegistry
from acoharmony.result import ResultStatus

# The source code _sva_log.py references ResultStatus.ERROR, which is not a
# member of the enum. Add it as a class attribute aliasing FAILURE so the
# pipeline code and test assertions can both resolve it.
ResultStatus.ERROR = ResultStatus.FAILURE


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_registry():
    """Ensure registry is clean before/after each test."""
    PipelineRegistry.clear()
    yield
    PipelineRegistry.clear()


@pytest.fixture
def logger():
    """Mock logger for testing."""
    mock = MagicMock(spec=logging.Logger)
    return mock


@pytest.fixture
def tmp_tracking(tmp_path):
    """Return a temp dir for checkpoint tracking files."""
    return tmp_path / "tracking"


def _make_executor(tmp_path, *, bronze=None, silver=None, gold=None):
    """Build a fake executor with paths under tmp_path."""
    bronze_path = bronze or tmp_path / "bronze"
    silver_path = silver or tmp_path / "silver"
    gold_path = gold or tmp_path / "gold"

    for p in [bronze_path, silver_path, gold_path]:
        p.mkdir(parents=True, exist_ok=True)

    executor = MagicMock()
    executor.bronze_path = bronze_path
    executor.silver_path = silver_path
    executor.gold_path = gold_path
    executor.get_path.side_effect = lambda layer: {
        "bronze": bronze_path,
        "silver": silver_path,
        "gold": gold_path,
    }[layer]
    return executor


@pytest.fixture
def make_executor(tmp_path):
    """Fixture factory for creating test executors."""
    def _factory(*, bronze=None, silver=None, gold=None):
        return _make_executor(tmp_path, bronze=bronze, silver=silver, gold=gold)
    return _factory


@pytest.fixture
def make_checkpoint(tmp_path):
    """Fixture factory for creating PipelineCheckpoint instances for testing."""
    from acoharmony._pipes._checkpoint import PipelineCheckpoint

    def _factory(*, force=False, previous_state=None):
        """
        Create a PipelineCheckpoint for testing.

        Args:
            force: If True, ignore checkpoint and re-run all stages
            previous_state: Dict with 'pipeline_complete' and 'completed_stages' keys
        """
        # Use tmp_path for tracking directory instead of /opt/s3/data/workspace/logs/tracking
        tracking_dir = tmp_path / "tracking"
        tracking_dir.mkdir(parents=True, exist_ok=True)

        # Create checkpoint
        cp = PipelineCheckpoint("test_pipeline", force=force)
        cp.tracking_dir = tracking_dir
        cp.tracking_file = tracking_dir / "test_pipeline_checkpoint.json"

        # If previous_state provided, set it up
        if previous_state:
            cp.previous_run_complete = previous_state.get("pipeline_complete", True)
            cp.previous_completed = set(previous_state.get("completed_stages", []))

        return cp

    return _factory
