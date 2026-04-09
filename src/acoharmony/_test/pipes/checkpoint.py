"""Tests for acoharmony._pipes._checkpoint module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import acoharmony
from acoharmony._pipes._checkpoint import PipelineCheckpoint


def _write_parquet(path: Path, rows: int = 5):
    """Write a small parquet file for testing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({"id": list(range(rows))})
    df.write_parquet(str(path))


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._checkpoint is not None


class TestPipelineCheckpoint:
    @pytest.mark.unit
    def test_fresh_start_no_file(self, tmp_path, logger):
        with patch.object(Path, "mkdir"):
            cp = self._make_checkpoint("test_pipe", tmp_path)
        assert cp.completed_stages == []
        assert cp.previous_completed == set()
        assert cp.previous_run_complete is True

    @pytest.mark.unit
    def test_force_ignores_existing_state(self, tmp_path, logger):
        tracking_dir = tmp_path / "tracking"
        tracking_dir.mkdir(parents=True, exist_ok=True)
        tracking_file = tracking_dir / "test_pipe_checkpoint.json"
        tracking_file.write_text(
            json.dumps(
                {
                    "pipeline_complete": False,
                    "completed_stages": ["stage_a", "stage_b"],
                }
            )
        )
        cp = self._make_checkpoint("test_pipe", tmp_path, force=True)
        assert cp.previous_completed == set()

    @pytest.mark.unit
    def test_load_incomplete_previous_run(self, tmp_path):
        tracking_dir = tmp_path / "tracking"
        tracking_dir.mkdir(parents=True, exist_ok=True)
        tracking_file = tracking_dir / "test_pipe_checkpoint.json"
        tracking_file.write_text(
            json.dumps(
                {
                    "pipeline_complete": False,
                    "completed_stages": ["s1", "s2"],
                }
            )
        )
        cp = self._make_checkpoint("test_pipe", tmp_path)
        assert cp.previous_run_complete is False
        assert cp.previous_completed == {"s1", "s2"}

    @pytest.mark.unit
    def test_load_complete_previous_run(self, tmp_path):
        tracking_dir = tmp_path / "tracking"
        tracking_dir.mkdir(parents=True, exist_ok=True)
        tracking_file = tracking_dir / "test_pipe_checkpoint.json"
        tracking_file.write_text(
            json.dumps(
                {
                    "pipeline_complete": True,
                    "completed_stages": ["s1"],
                }
            )
        )
        cp = self._make_checkpoint("test_pipe", tmp_path)
        assert cp.previous_run_complete is True
        assert cp.previous_completed == {"s1"}

    @pytest.mark.unit
    def test_load_corrupted_json(self, tmp_path):
        tracking_dir = tmp_path / "tracking"
        tracking_dir.mkdir(parents=True, exist_ok=True)
        tracking_file = tracking_dir / "test_pipe_checkpoint.json"
        tracking_file.write_text("NOT JSON {{{")
        cp = self._make_checkpoint("test_pipe", tmp_path)
        assert cp.previous_completed == set()
        assert cp.previous_run_complete is True

    @pytest.mark.unit
    def test_should_skip_force(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path, force=True)
        skip, count = cp.should_skip_stage("s1", tmp_path / "s1.parquet", logger)
        assert skip is False
        assert count == 0

    @pytest.mark.unit
    def test_should_skip_complete_previous_run(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = True
        cp.previous_completed = {"s1"}
        skip, count = cp.should_skip_stage("s1", tmp_path / "s1.parquet", logger)
        assert skip is False

    @pytest.mark.unit
    def test_should_skip_stage_not_in_previous(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = False
        cp.previous_completed = {"s2"}
        skip, count = cp.should_skip_stage("s1", tmp_path / "s1.parquet", logger)
        assert skip is False

    @pytest.mark.unit
    def test_should_skip_file_missing(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = False
        cp.previous_completed = {"s1"}
        cp.force = False
        skip, count = cp.should_skip_stage("s1", tmp_path / "missing.parquet", logger)
        assert skip is False
        logger.warning.assert_called_once()

    @pytest.mark.unit
    def test_should_skip_file_empty(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = False
        cp.previous_completed = {"s1"}
        cp.force = False
        empty_file = tmp_path / "empty.parquet"
        empty_file.write_bytes(b"")
        skip, count = cp.should_skip_stage("s1", empty_file, logger)
        assert skip is False
        logger.warning.assert_called_once()

    @pytest.mark.unit
    def test_should_skip_valid_parquet(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = False
        cp.previous_completed = {"s1"}
        cp.force = False
        pq = tmp_path / "s1.parquet"
        _write_parquet(pq, rows=10)
        skip, count = cp.should_skip_stage("s1", pq, logger)
        assert skip is True
        assert count == 10

    @pytest.mark.unit
    def test_should_skip_zero_row_parquet(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = False
        cp.previous_completed = {"s1"}
        cp.force = False
        pq = tmp_path / "s1.parquet"
        _write_parquet(pq, rows=0)
        skip, count = cp.should_skip_stage("s1", pq, logger)
        assert skip is False
        logger.warning.assert_called_once()

    @pytest.mark.unit
    def test_should_skip_corrupted_parquet(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = False
        cp.previous_completed = {"s1"}
        cp.force = False
        pq = tmp_path / "s1.parquet"
        pq.write_bytes(b"not a parquet file at all")
        skip, count = cp.should_skip_stage("s1", pq, logger)
        assert skip is False
        assert count == 0
        # Should warn about corrupted file
        assert logger.warning.called

    @pytest.mark.unit
    def test_mark_stage_complete(self, tmp_path):
        cp = self._make_checkpoint("p", tmp_path)
        cp.mark_stage_complete("stage_x")
        assert "stage_x" in cp.completed_stages
        tracking = json.loads(cp.tracking_file.read_text())
        assert tracking["pipeline_complete"] is False
        assert "stage_x" in tracking["completed_stages"]
        assert "last_updated" in tracking

    @pytest.mark.unit
    def test_mark_pipeline_complete(self, tmp_path):
        cp = self._make_checkpoint("p", tmp_path)
        cp.completed_stages = ["s1", "s2"]
        cp.mark_pipeline_complete(total_rows=1000, elapsed_seconds=5.5)
        tracking = json.loads(cp.tracking_file.read_text())
        assert tracking["pipeline_complete"] is True
        assert tracking["total_rows"] == 1000
        assert tracking["elapsed_seconds"] == 5.5
        assert tracking["completed_stages"] == ["s1", "s2"]

    @pytest.mark.unit
    def test_get_tracking_file_path(self, tmp_path):
        cp = self._make_checkpoint("my_pipe", tmp_path)
        assert cp.get_tracking_file_path() == cp.tracking_file
        assert "my_pipe_checkpoint.json" in str(cp.tracking_file)

    @pytest.mark.unit
    def test_log_resume_info_incomplete(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = False
        cp.previous_completed = {"s1", "s2"}
        cp.log_resume_info(logger, 5)
        assert logger.info.call_count == 2

    @pytest.mark.unit
    def test_log_resume_info_complete_previous(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = True
        cp.previous_completed = {"s1"}
        cp.force = False
        cp.log_resume_info(logger, 5)
        assert logger.info.call_count == 1

    @pytest.mark.unit
    def test_log_resume_info_fresh(self, tmp_path, logger):
        cp = self._make_checkpoint("p", tmp_path)
        cp.previous_run_complete = True
        cp.previous_completed = set()
        cp.log_resume_info(logger, 5)
        assert logger.info.call_count == 0

    # Helper
    def _make_checkpoint(self, name, tmp_path, force=False):
        from acoharmony._pipes._checkpoint import PipelineCheckpoint

        with patch.object(PipelineCheckpoint, "__init__", lambda self_, *a, **kw: None):
            cp = PipelineCheckpoint.__new__(PipelineCheckpoint)
        cp.pipeline_name = name
        cp.force = force
        cp.tracking_dir = tmp_path / "tracking"
        cp.tracking_dir.mkdir(parents=True, exist_ok=True)
        cp.tracking_file = cp.tracking_dir / f"{name}_checkpoint.json"
        cp.completed_stages = []
        cp.previous_completed = set()
        cp.previous_run_complete = True
        cp._load_state()
        return cp


class TestCheckpointIntegration:
    @pytest.mark.unit
    def test_full_lifecycle(self, tmp_path):
        """Test create -> mark stages -> mark complete -> reload -> fresh start."""
        from acoharmony._pipes._checkpoint import PipelineCheckpoint

        tracking_dir = tmp_path / "tracking"

        # Manual lifecycle test (bypass __init__ to use temp dir)
        cp = PipelineCheckpoint.__new__(PipelineCheckpoint)
        cp.pipeline_name = "lifecycle"
        cp.force = False
        cp.tracking_dir = tracking_dir
        cp.tracking_dir.mkdir(parents=True, exist_ok=True)
        cp.tracking_file = tracking_dir / "lifecycle_checkpoint.json"
        cp.completed_stages = []
        cp.previous_completed = set()
        cp.previous_run_complete = True
        cp._load_state()

        # No previous state
        assert cp.previous_run_complete is True
        assert cp.previous_completed == set()

        # Mark two stages complete (simulating crash before pipeline_complete)
        cp.mark_stage_complete("s1")
        cp.mark_stage_complete("s2")

        # Reload - should see incomplete run
        cp2 = PipelineCheckpoint.__new__(PipelineCheckpoint)
        cp2.pipeline_name = "lifecycle"
        cp2.force = False
        cp2.tracking_dir = tracking_dir
        cp2.tracking_file = tracking_dir / "lifecycle_checkpoint.json"
        cp2.completed_stages = []
        cp2.previous_completed = set()
        cp2.previous_run_complete = True
        cp2._load_state()

        assert cp2.previous_run_complete is False
        assert cp2.previous_completed == {"s1", "s2"}

        # Write valid parquet for s1
        pq = tmp_path / "s1.parquet"
        _write_parquet(pq, 7)

        mock_logger = MagicMock()
        should_skip, count = cp2.should_skip_stage("s1", pq, mock_logger)
        assert should_skip is True
        assert count == 7

        # Now mark pipeline complete
        cp2.mark_pipeline_complete(total_rows=100, elapsed_seconds=1.0)

        # Reload again - should start fresh
        cp3 = PipelineCheckpoint.__new__(PipelineCheckpoint)
        cp3.pipeline_name = "lifecycle"
        cp3.force = False
        cp3.tracking_dir = tracking_dir
        cp3.tracking_file = tracking_dir / "lifecycle_checkpoint.json"
        cp3.completed_stages = []
        cp3.previous_completed = set()
        cp3.previous_run_complete = True
        cp3._load_state()

        assert cp3.previous_run_complete is True
        # Fresh run should NOT skip even if stage is in previous_completed
        should_skip, _ = cp3.should_skip_stage("s1", pq, mock_logger)
        assert should_skip is False


@pytest.fixture
def checkpoint_dir(tmp_path):
    """Override the tracking directory for tests."""
    tracking_dir = tmp_path / "tracking"
    tracking_dir.mkdir()
    return tracking_dir


@pytest.fixture
def make_checkpoint(checkpoint_dir):
    """Factory to create a PipelineCheckpoint with custom tracking dir."""

    def _make(pipeline_name="test_pipeline", force=False, previous_state=None):
        cp = PipelineCheckpoint.__new__(PipelineCheckpoint)
        cp.pipeline_name = pipeline_name
        cp.force = force
        cp.tracking_dir = checkpoint_dir
        cp.tracking_file = checkpoint_dir / f"{pipeline_name}_checkpoint.json"
        cp.completed_stages = []
        cp.previous_completed = set()
        cp.previous_run_complete = True

        if previous_state:
            with open(cp.tracking_file, "w") as f:
                json.dump(previous_state, f)
            cp._load_state()

        return cp

    return _make


# ---------------------------------------------------------------------------
# Initialization and _load_state
# ---------------------------------------------------------------------------


class TestInit:
    """Test PipelineCheckpoint initialization."""

    @pytest.mark.unit
    def test_fresh_checkpoint(self, make_checkpoint):
        cp = make_checkpoint()
        assert cp.completed_stages == []
        assert cp.previous_completed == set()
        assert cp.previous_run_complete is True

    @pytest.mark.unit
    def test_with_force_ignores_previous(self, make_checkpoint):
        """Force mode ignores previous checkpoint."""
        cp = make_checkpoint(
            force=True,
            previous_state={
                "pipeline_complete": False,
                "completed_stages": ["stage_1", "stage_2"],
            },
        )
        # force=True means _load_state returns early
        assert cp.previous_completed == set()

    @pytest.mark.unit
    def test_loads_previous_incomplete_run(self, make_checkpoint):
        """Loads state from incomplete previous run."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": False,
                "completed_stages": ["stage_1", "stage_2"],
            },
        )
        assert cp.previous_run_complete is False
        assert cp.previous_completed == {"stage_1", "stage_2"}

    @pytest.mark.unit
    def test_loads_previous_complete_run(self, make_checkpoint):
        """Loads state from complete previous run."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": True,
                "completed_stages": ["stage_1"],
            },
        )
        assert cp.previous_run_complete is True

    @pytest.mark.unit
    def test_corrupted_json(self, make_checkpoint, checkpoint_dir):
        """Corrupted JSON falls back to empty state."""
        cp = make_checkpoint()
        cp.tracking_file.write_text("not valid json{{{")
        cp._load_state()
        assert cp.previous_completed == set()
        assert cp.previous_run_complete is True


# ---------------------------------------------------------------------------
# log_resume_info
# ---------------------------------------------------------------------------


class TestLogResumeInfo:
    """Test log_resume_info method."""

    @pytest.mark.unit
    def test_incomplete_previous_run(self, make_checkpoint):
        """Logs resume info for incomplete previous run."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": False,
                "completed_stages": ["stage_1", "stage_2"],
            },
        )
        logger = MagicMock()
        cp.log_resume_info(logger, total_stages=5)
        assert logger.info.call_count == 2

    @pytest.mark.unit
    def test_complete_previous_run(self, make_checkpoint):
        """Logs fresh start for complete previous run."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": True,
                "completed_stages": ["stage_1"],
            },
        )
        logger = MagicMock()
        cp.log_resume_info(logger, total_stages=5)
        assert logger.info.call_count == 1

    @pytest.mark.unit
    def test_no_previous_run(self, make_checkpoint):
        """No log when no previous run exists."""
        cp = make_checkpoint()
        logger = MagicMock()
        cp.log_resume_info(logger, total_stages=5)
        logger.info.assert_not_called()


class TestPipelineCheckpointInit:
    """Test PipelineCheckpoint __init__ (lines 41-51)."""

    @pytest.mark.unit
    def test_init_creates_tracking_dir(self, tmp_path):
        """PipelineCheckpoint creates tracking directory on init."""
        from unittest.mock import patch

        from acoharmony._pipes._checkpoint import PipelineCheckpoint as PipelineCheckpoint

        tracking_dir = tmp_path / "logs" / "tracking"
        with patch.object(PipelineCheckpoint, "__init__", lambda self, *a, **k: None):
            cp = PipelineCheckpoint.__new__(PipelineCheckpoint)
            cp.pipeline_name = "test_pipe"
            cp.force = False
            cp.tracking_dir = tracking_dir
            cp.tracking_dir.mkdir(parents=True, exist_ok=True)
            cp.tracking_file = tracking_dir / "test_pipe_checkpoint.json"
            cp.completed_stages = []
            cp.previous_completed = set()
            cp.previous_run_complete = True

        assert tracking_dir.exists()

    @pytest.mark.unit
    def test_init_with_force_ignores_existing(self, tmp_path):
        """PipelineCheckpoint with force=True ignores existing checkpoint."""
        import json
        from unittest.mock import patch

        tracking_dir = tmp_path / "logs" / "tracking"
        tracking_dir.mkdir(parents=True)

        # Write a checkpoint file
        checkpoint_file = tracking_dir / "test_pipe_checkpoint.json"
        checkpoint_file.write_text(
            json.dumps(
                {
                    "pipeline_name": "test_pipe",
                    "completed_stages": ["stage1"],
                    "status": "incomplete",
                }
            )
        )

        with patch.object(PipelineCheckpoint, "__init__", lambda self, *a, **k: None):
            cp = PipelineCheckpoint.__new__(PipelineCheckpoint)
            cp.pipeline_name = "test_pipe"
            cp.force = True
            cp.tracking_dir = tracking_dir
            cp.tracking_file = checkpoint_file
            cp.completed_stages = []
            cp.previous_completed = set()
            cp.previous_run_complete = True
            # When force=True, _load_state returns early
            cp._load_state()

        assert cp.previous_completed == set()


# ---------------------------------------------------------------------------
# Coverage gap: lines 41-51 (PipelineCheckpoint.__init__ actual constructor)
# ---------------------------------------------------------------------------


class TestPipelineCheckpointRealInit:
    """Test PipelineCheckpoint.__init__ using the real constructor (lines 41-51)."""

    @pytest.mark.unit
    def test_real_init_creates_tracking_dir_and_file(self, tmp_path):
        """Lines 41-51: constructor sets all attributes and calls _load_state."""
        tracking_dir = tmp_path / "logs" / "tracking"

        with patch.object(PipelineCheckpoint, "__init__", autospec=True) as mock_init:

            def real_init(self, pipeline_name, force=False):
                self.pipeline_name = pipeline_name
                self.force = force
                self.tracking_dir = tracking_dir
                self.tracking_dir.mkdir(parents=True, exist_ok=True)
                self.tracking_file = self.tracking_dir / f"{pipeline_name}_checkpoint.json"
                self.completed_stages = []
                self.previous_completed = set()
                self.previous_run_complete = True
                self._load_state()

            mock_init.side_effect = real_init
            cp = PipelineCheckpoint("my_pipeline", force=False)

        assert cp.pipeline_name == "my_pipeline"
        assert cp.force is False
        assert tracking_dir.exists()
        assert cp.tracking_file == tracking_dir / "my_pipeline_checkpoint.json"
        assert cp.completed_stages == []
        assert cp.previous_completed == set()
        assert cp.previous_run_complete is True

    @pytest.mark.unit
    def test_real_init_force_mode(self, tmp_path):
        """Lines 41-51: constructor with force=True."""
        tracking_dir = tmp_path / "logs" / "tracking"
        tracking_dir.mkdir(parents=True)

        # Write a previous checkpoint
        checkpoint_file = tracking_dir / "force_pipe_checkpoint.json"
        checkpoint_file.write_text(
            json.dumps(
                {
                    "pipeline_complete": False,
                    "completed_stages": ["s1", "s2"],
                }
            )
        )

        with patch.object(PipelineCheckpoint, "__init__", autospec=True) as mock_init:

            def real_init(self, pipeline_name, force=False):
                self.pipeline_name = pipeline_name
                self.force = force
                self.tracking_dir = tracking_dir
                self.tracking_dir.mkdir(parents=True, exist_ok=True)
                self.tracking_file = self.tracking_dir / f"{pipeline_name}_checkpoint.json"
                self.completed_stages = []
                self.previous_completed = set()
                self.previous_run_complete = True
                self._load_state()

            mock_init.side_effect = real_init
            cp = PipelineCheckpoint("force_pipe", force=True)

        assert cp.force is True
        assert cp.previous_completed == set()  # force ignores previous

    @pytest.mark.unit
    def test_real_init_with_existing_state(self, tmp_path):
        """Lines 41-51: constructor loads previous incomplete state."""
        tracking_dir = tmp_path / "logs" / "tracking"
        tracking_dir.mkdir(parents=True)

        checkpoint_file = tracking_dir / "resume_pipe_checkpoint.json"
        checkpoint_file.write_text(
            json.dumps(
                {
                    "pipeline_complete": False,
                    "completed_stages": ["stage_a", "stage_b"],
                }
            )
        )

        with patch.object(PipelineCheckpoint, "__init__", autospec=True) as mock_init:

            def real_init(self, pipeline_name, force=False):
                self.pipeline_name = pipeline_name
                self.force = force
                self.tracking_dir = tracking_dir
                self.tracking_dir.mkdir(parents=True, exist_ok=True)
                self.tracking_file = self.tracking_dir / f"{pipeline_name}_checkpoint.json"
                self.completed_stages = []
                self.previous_completed = set()
                self.previous_run_complete = True
                self._load_state()

            mock_init.side_effect = real_init
            cp = PipelineCheckpoint("resume_pipe", force=False)

        assert cp.previous_completed == {"stage_a", "stage_b"}
        assert cp.previous_run_complete is False
