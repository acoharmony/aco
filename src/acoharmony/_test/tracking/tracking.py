"""Tests for tracking.py — TransformState and TransformTracker."""


# Magic auto-import: brings in ALL exports from module under test
from dataclasses import dataclass
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest


class TestTransformState:
    """Tests for TransformState dataclass."""

    @pytest.mark.unit
    def test_defaults(self):

        state = TransformState(transform_name="test")
        assert state.transform_name == "test"
        assert state.last_run is None
        assert state.last_success is None
        assert state.total_runs == 0
        assert state.successful_runs == 0
        assert state.failed_runs == 0
        assert state.files_processed == {}
        assert state.metadata == {}

    @pytest.mark.unit
    def test_asdict(self):

        state = TransformState(transform_name="x", total_runs=5)
        d = asdict(state)
        assert d["transform_name"] == "x"
        assert d["total_runs"] == 5


class TestTransformTracker:
    """Tests for TransformTracker."""

    @pytest.fixture
    def tracker(self, tmp_path):

        with patch("acoharmony.tracking.LogWriter") as MockLogger:
            mock_logger_instance = MagicMock()
            MockLogger.return_value = mock_logger_instance
            t = TransformTracker("test_schema", tracking_dir=tmp_path)
        return t

    @pytest.mark.unit
    def test_init_creates_dir(self, tmp_path):

        tracking_subdir = tmp_path / "subdir"
        with patch("acoharmony.tracking.LogWriter"):
            TransformTracker("test", tracking_dir=tracking_subdir)
        assert tracking_subdir.exists()

    @pytest.mark.unit
    def test_init_state_fresh(self, tracker):
        assert tracker.state.transform_name == "test_schema"
        assert tracker.state.total_runs == 0

    @pytest.mark.unit
    def test_init_loads_existing_state(self, tmp_path):

        state_data = {
            "transform_name": "loaded",
            "last_run": "2025-01-01T00:00:00",
            "last_success": None,
            "total_runs": 3,
            "successful_runs": 2,
            "failed_runs": 1,
            "files_processed": {"processed": ["a.csv"]},
            "metadata": {},
        }
        state_file = tmp_path / "loaded_state.json"
        state_file.write_text(json.dumps(state_data))
        with patch("acoharmony.tracking.LogWriter"):
            t = TransformTracker("loaded", tracking_dir=tmp_path)
        assert t.state.total_runs == 3
        assert t.state.successful_runs == 2
        assert t.state.files_processed == {"processed": ["a.csv"]}

    @pytest.mark.unit
    def test_init_corrupted_state(self, tmp_path):

        state_file = tmp_path / "bad_state.json"
        state_file.write_text("NOT VALID JSON")
        with patch("acoharmony.tracking.LogWriter"):
            t = TransformTracker("bad", tracking_dir=tmp_path)
        assert t.state.transform_name == "bad"
        assert t.state.total_runs == 0

    @pytest.mark.unit
    def test_start_transform(self, tracker):
        tracker.start_transform(pipeline="medical", stage="parsing")
        assert tracker.state.total_runs == 1
        assert tracker.state.last_run is not None
        assert tracker.state_file.exists()

    @pytest.mark.unit
    def test_start_transform_no_args(self, tracker):
        tracker.start_transform()
        assert tracker.state.total_runs == 1

    @pytest.mark.unit
    def test_track_file(self, tracker):
        tracker.track_file("data/file1.csv", status="processed")
        assert "file1.csv" in tracker.state.files_processed["processed"][0]

    @pytest.mark.unit
    def test_track_file_no_duplicates(self, tracker):
        tracker.track_file("file.csv", "processed")
        tracker.track_file("file.csv", "processed")
        assert len(tracker.state.files_processed["processed"]) == 1

    @pytest.mark.unit
    def test_track_file_different_statuses(self, tracker):
        tracker.track_file("file.csv", "processed")
        tracker.track_file("file2.csv", "failed")
        assert "processed" in tracker.state.files_processed
        assert "failed" in tracker.state.files_processed

    @pytest.mark.unit
    def test_complete_transform_success(self, tracker):
        tracker.start_transform()
        tracker.complete_transform(
            success=True, records=1000, files=5, output="/out/data.parquet", message="done"
        )
        assert tracker.state.successful_runs == 1
        assert tracker.state.failed_runs == 0
        assert tracker.state.last_success is not None
        assert tracker.state.metadata["last_run_records"] == 1000
        assert tracker.state.metadata["last_run_files"] == 5
        assert tracker.state.metadata["last_run_output"] == "/out/data.parquet"
        assert tracker.state.metadata["last_run_message"] == "done"

    @pytest.mark.unit
    def test_complete_transform_failure(self, tracker):
        tracker.start_transform()
        tracker.complete_transform(success=False, message="oom")
        assert tracker.state.failed_runs == 1
        assert tracker.state.successful_runs == 0
        assert tracker.state.last_success is None

    @pytest.mark.unit
    def test_complete_transform_with_pipeline(self, tracker):
        tracker.start_transform(pipeline="test_pipe")
        tracker.complete_transform(success=True, pipeline="test_pipe")
        assert tracker.state.successful_runs == 1

    @pytest.mark.unit
    def test_get_unprocessed_files(self, tracker):
        tracker.track_file("a.csv", "processed")
        tracker.track_file("b.csv", "failed")
        all_files = ["a.csv", "b.csv", "c.csv", "d.csv"]
        unprocessed = tracker.get_unprocessed_files(all_files)
        assert unprocessed == ["c.csv", "d.csv"]

    @pytest.mark.unit
    def test_get_unprocessed_files_all_processed(self, tracker):
        tracker.track_file("a.csv", "processed")
        assert tracker.get_unprocessed_files(["a.csv"]) == []

    @pytest.mark.unit
    def test_get_unprocessed_files_none_processed(self, tracker):
        assert tracker.get_unprocessed_files(["x.csv", "y.csv"]) == ["x.csv", "y.csv"]

    @pytest.mark.unit
    def test_has_processed_file_true(self, tracker):
        tracker.track_file("file.csv", "processed")
        assert tracker.has_processed_file("file.csv") is True

    @pytest.mark.unit
    def test_has_processed_file_false(self, tracker):
        assert tracker.has_processed_file("missing.csv") is False

    @pytest.mark.unit
    def test_has_processed_file_any_status(self, tracker):
        tracker.track_file("file.csv", "failed")
        assert tracker.has_processed_file("file.csv") is True

    @pytest.mark.unit
    def test_get_stats(self, tracker):
        tracker.start_transform()
        tracker.track_file("a.csv", "processed")
        tracker.track_file("b.csv", "failed")
        tracker.complete_transform(success=True, records=100)
        stats = tracker.get_stats()
        assert stats["transform"] == "test_schema"
        assert stats["total_runs"] == 1
        assert stats["successful_runs"] == 1
        assert stats["failed_runs"] == 0
        assert stats["total_files_processed"] == 2
        assert stats["last_run"] is not None
        assert stats["last_success"] is not None

    @pytest.mark.unit
    def test_get_failed_files(self, tracker):
        tracker.track_file("ok.csv", "processed")
        tracker.track_file("bad.csv", "failed")
        tracker.track_file("bad2.csv", "failed")
        failed = tracker.get_failed_files()
        assert set(failed) == {"bad.csv", "bad2.csv"}

    @pytest.mark.unit
    def test_get_failed_files_empty(self, tracker):
        assert tracker.get_failed_files() == []

    @pytest.mark.unit
    def test_clear_file_status(self, tracker):
        tracker.track_file("file.csv", "processed")
        tracker.track_file("file.csv", "failed")
        tracker.clear_file_status("file.csv")
        assert tracker.has_processed_file("file.csv") is False

    @pytest.mark.unit
    def test_save_state_error_handling(self, tracker):
        with patch("builtins.open", side_effect=PermissionError("denied")):
            tracker._save_state()  # Should not raise

    @pytest.mark.unit
    def test_state_persistence_round_trip(self, tmp_path):

        with patch("acoharmony.tracking.LogWriter"):
            t1 = TransformTracker("persist_test", tracking_dir=tmp_path)
        t1.start_transform()
        t1.track_file("file.csv", "processed")
        t1.complete_transform(success=True, records=50)

        with patch("acoharmony.tracking.LogWriter"):
            t2 = TransformTracker("persist_test", tracking_dir=tmp_path)
        assert t2.state.total_runs == 1
        assert t2.state.successful_runs == 1
        assert t2.has_processed_file("file.csv")


class TestTransformTrackerDeeper:
    """Cover TransformTracker branches."""

    @pytest.mark.unit
    def test_start_transform_with_pipeline_stage(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        tracker.start_transform(pipeline="my_pipeline", stage="stage1")
        assert tracker.state.total_runs == 1
        assert tracker.state.last_run is not None

    @pytest.mark.unit
    def test_complete_transform_failure(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        tracker.start_transform()
        tracker.complete_transform(
            success=False,
            records=0,
            files=0,
            message="Memory error",
            pipeline="pipe1",
        )
        assert tracker.state.failed_runs == 1
        assert tracker.state.metadata["last_run_message"] == "Memory error"

    @pytest.mark.unit
    def test_has_processed_file(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        tracker.track_file("/data/file1.csv", "processed")
        assert tracker.has_processed_file("/data/file1.csv") is True
        assert tracker.has_processed_file("/data/file2.csv") is False

    @pytest.mark.unit
    def test_get_unprocessed_files(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        tracker.track_file("/data/a.csv", "processed")
        tracker.track_file("/data/b.csv", "failed")
        unprocessed = tracker.get_unprocessed_files(["/data/a.csv", "/data/b.csv", "/data/c.csv"])
        assert unprocessed == ["/data/c.csv"]

    @pytest.mark.unit
    def test_get_failed_files(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        tracker.track_file("/data/bad.csv", "failed")
        assert tracker.get_failed_files() == ["/data/bad.csv"]

    @pytest.mark.unit
    def test_get_failed_files_empty(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        assert tracker.get_failed_files() == []

    @pytest.mark.unit
    def test_clear_file_status(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        tracker.track_file("/data/a.csv", "processed")
        tracker.track_file("/data/a.csv", "failed")
        tracker.clear_file_status("/data/a.csv")
        assert not tracker.has_processed_file("/data/a.csv")

    @pytest.mark.unit
    def test_get_stats(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        tracker.start_transform()
        tracker.complete_transform(success=True, records=100, files=2)
        stats = tracker.get_stats()
        assert stats["transform"] == "test"
        assert stats["total_runs"] == 1
        assert stats["successful_runs"] == 1

    @pytest.mark.unit
    def test_load_corrupted_state(self, tmp_path):

        state_file = tmp_path / "bad_state.json"
        state_file.write_text("not json")

        tracker = TransformTracker("bad", tracking_dir=tmp_path)
        assert tracker.state.transform_name == "bad"

    @pytest.mark.unit
    def test_save_state_failure(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        with patch("builtins.open", side_effect=OSError("disk full")):
            tracker._save_state()  # Should not crash

    @pytest.mark.unit
    def test_track_file_idempotent(self, tmp_path):

        tracker = TransformTracker("test", tracking_dir=tmp_path)
        tracker.track_file("/data/a.csv", "processed")
        tracker.track_file("/data/a.csv", "processed")
        assert tracker.state.files_processed["processed"].count("/data/a.csv") == 1


class TestTrackingClearFileStatus:
    """Cover clear_file_status edge cases."""

    @pytest.mark.unit
    def test_clear_file_status_removes_from_processed(self, tmp_path):

        tracker = TransformTracker("test_schema", tracking_dir=tmp_path)
        tracker.track_file("file_a.csv", status="processed")
        tracker.track_file("file_b.csv", status="processed")
        tracker.track_file("file_c.csv", status="failed")

        assert tracker.has_processed_file("file_a.csv")
        tracker.clear_file_status("file_a.csv")

        assert not tracker.has_processed_file("file_a.csv")
        assert tracker.has_processed_file("file_b.csv")
        assert tracker.has_processed_file("file_c.csv")

    @pytest.mark.unit
    def test_clear_file_status_removes_from_multiple_statuses(self, tmp_path):

        tracker = TransformTracker("test_schema2", tracking_dir=tmp_path)
        tracker.state.files_processed["processed"] = ["file_x.csv"]
        tracker.state.files_processed["failed"] = ["file_x.csv"]

        tracker.clear_file_status("file_x.csv")

        assert "file_x.csv" not in tracker.state.files_processed.get("processed", [])
        assert "file_x.csv" not in tracker.state.files_processed.get("failed", [])
