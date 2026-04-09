"""Tests for acoharmony._pipes._stage module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
import os
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._stage is not None


class TestStageModule:
    @pytest.mark.unit
    def test_pipeline_stage_creation(self):
        from acoharmony._pipes._stage import PipelineStage

        mod = MagicMock()
        s = PipelineStage("name", mod, "grp", 5, ["dep1"])
        assert s.name == "name"
        assert s.module is mod
        assert s.group == "grp"
        assert s.order == 5
        assert s.depends_on == ["dep1"]

    @pytest.mark.unit
    def test_pipeline_stage_repr_no_deps(self):
        from acoharmony._pipes._stage import PipelineStage

        s = PipelineStage("out", None, "claims", 3)
        assert repr(s) == "Stage(3: out [claims])"

    @pytest.mark.unit
    def test_pipeline_stage_repr_with_deps(self):
        from acoharmony._pipes._stage import PipelineStage

        s = PipelineStage("out", None, "claims", 3, ["d1"])
        r = repr(s)
        assert "depends on" in r
        assert "d1" in r

    @patch("acoharmony.config.get_config")
    @patch("acoharmony._pipes._stage.pl.disable_string_cache")
    @patch("acoharmony._pipes._stage.gc.collect")
    @pytest.mark.unit
    def test_execute_stage_normal(self, mock_gc, mock_disable, mock_config, tmp_path, logger):
        from acoharmony._pipes._stage import PipelineStage, execute_stage

        config = MagicMock()
        config.transform.compression = "zstd"
        config.transform.row_group_size = 100000
        mock_config.return_value = config

        mock_lf = MagicMock()
        module = MagicMock()
        module.execute.return_value = mock_lf

        stage = PipelineStage("test_out", module, "grp", 1)
        result, count = execute_stage(stage, MagicMock(), logger, tmp_path)

        assert result is None
        assert count == 0
        module.execute.assert_called_once()
        mock_lf.sink_parquet.assert_called_once()
        sink_kwargs = mock_lf.sink_parquet.call_args
        assert sink_kwargs[1]["compression"] == "zstd"
        assert sink_kwargs[1]["engine"] == "streaming"
        assert mock_gc.call_count == 2  # gc.collect called twice
        mock_disable.assert_called_once()

    @patch.dict(os.environ, {"ACO_FORCE_STREAMING": "1"})
    @patch("acoharmony.config.get_config")
    @patch("acoharmony._pipes._stage.pl.disable_string_cache")
    @patch("acoharmony._pipes._stage.gc.collect")
    @pytest.mark.unit
    def test_execute_stage_force_streaming(
        self, mock_gc, mock_disable, mock_config, tmp_path, logger
    ):
        from acoharmony._pipes._stage import PipelineStage, execute_stage

        config = MagicMock()
        config.transform.compression = "zstd"
        config.transform.row_group_size = 100000
        mock_config.return_value = config

        mock_lf = MagicMock()
        module = MagicMock()
        module.execute.return_value = mock_lf

        stage = PipelineStage("test_out", module, "grp", 1)
        execute_stage(stage, MagicMock(), logger, tmp_path)

        sink_kwargs = mock_lf.sink_parquet.call_args
        assert sink_kwargs[1]["row_group_size"] == 10000


class TestShouldSkipStage:
    """Test should_skip_stage method."""

    @pytest.mark.unit
    def test_force_never_skips(self, make_checkpoint, tmp_path):
        """Force mode always returns (False, 0)."""
        cp = make_checkpoint(force=True)
        skip, count = cp.should_skip_stage("stage_1", tmp_path / "out.parquet", MagicMock())
        assert skip is False
        assert count == 0

    @pytest.mark.unit
    def test_complete_previous_run_never_skips(self, make_checkpoint, tmp_path):
        """Complete previous run starts fresh, never skips."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": True,
                "completed_stages": ["stage_1"],
            },
        )
        skip, count = cp.should_skip_stage("stage_1", tmp_path / "out.parquet", MagicMock())
        assert skip is False

    @pytest.mark.unit
    def test_stage_not_in_previous_doesnt_skip(self, make_checkpoint, tmp_path):
        """Stage not in previous completed set doesn't skip."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": False,
                "completed_stages": ["stage_1"],
            },
        )
        skip, count = cp.should_skip_stage("stage_2", tmp_path / "out.parquet", MagicMock())
        assert skip is False

    @pytest.mark.unit
    def test_skip_with_valid_parquet(self, make_checkpoint, tmp_path):
        """Skips stage when file is valid parquet with data."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": False,
                "completed_stages": ["stage_1"],
            },
        )
        output_file = tmp_path / "out.parquet"
        pl.DataFrame({"a": [1, 2, 3]}).write_parquet(output_file)

        logger = MagicMock()
        skip, count = cp.should_skip_stage("stage_1", output_file, logger)
        assert skip is True
        assert count == 3

    @pytest.mark.unit
    def test_missing_file_doesnt_skip(self, make_checkpoint, tmp_path):
        """Missing output file means re-run."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": False,
                "completed_stages": ["stage_1"],
            },
        )
        logger = MagicMock()
        skip, count = cp.should_skip_stage("stage_1", tmp_path / "missing.parquet", logger)
        assert skip is False
        logger.warning.assert_called_once()

    @pytest.mark.unit
    def test_empty_file_doesnt_skip(self, make_checkpoint, tmp_path):
        """Empty output file means re-run."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": False,
                "completed_stages": ["stage_1"],
            },
        )
        output_file = tmp_path / "empty.parquet"
        output_file.touch()  # 0 bytes

        logger = MagicMock()
        skip, count = cp.should_skip_stage("stage_1", output_file, logger)
        assert skip is False

    @pytest.mark.unit
    def test_corrupted_parquet_doesnt_skip(self, make_checkpoint, tmp_path):
        """Corrupted parquet file means re-run."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": False,
                "completed_stages": ["stage_1"],
            },
        )
        output_file = tmp_path / "corrupt.parquet"
        output_file.write_bytes(b"not valid parquet" * 20)

        logger = MagicMock()
        skip, count = cp.should_skip_stage("stage_1", output_file, logger)
        assert skip is False
        assert logger.warning.called

    @pytest.mark.unit
    def test_zero_row_parquet_doesnt_skip(self, make_checkpoint, tmp_path):
        """Parquet with 0 rows means re-run."""
        cp = make_checkpoint(
            previous_state={
                "pipeline_complete": False,
                "completed_stages": ["stage_1"],
            },
        )
        output_file = tmp_path / "empty_rows.parquet"
        pl.DataFrame({"a": []}, schema={"a": pl.Int64}).write_parquet(output_file)

        logger = MagicMock()
        skip, count = cp.should_skip_stage("stage_1", output_file, logger)
        assert skip is False


class TestMarkStageComplete:
    """Test mark_stage_complete method."""

    @pytest.mark.unit
    def test_marks_and_writes_file(self, make_checkpoint):
        cp = make_checkpoint()
        cp.mark_stage_complete("stage_1")
        assert "stage_1" in cp.completed_stages
        assert cp.tracking_file.exists()
        data = json.loads(cp.tracking_file.read_text())
        assert data["pipeline_complete"] is False
        assert "stage_1" in data["completed_stages"]


class TestMarkPipelineComplete:
    """Test mark_pipeline_complete method."""

    @pytest.mark.unit
    def test_marks_complete(self, make_checkpoint):
        cp = make_checkpoint()
        cp.mark_stage_complete("stage_1")
        cp.mark_pipeline_complete(total_rows=1000, elapsed_seconds=30.5)
        data = json.loads(cp.tracking_file.read_text())
        assert data["pipeline_complete"] is True
        assert data["total_rows"] == 1000
        assert data["elapsed_seconds"] == 30.5


class TestGetTrackingFilePath:
    """Test get_tracking_file_path method."""

    @pytest.mark.unit
    def test_returns_path(self, make_checkpoint):
        cp = make_checkpoint()
        assert cp.get_tracking_file_path() == cp.tracking_file
