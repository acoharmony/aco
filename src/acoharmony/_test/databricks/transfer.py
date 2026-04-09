# � 2025 HarmonyCares
# All rights reserved.
"""Tests for acoharmony._databricks._transfer module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
import tempfile
from pathlib import Path

import polars as pl
import pytest

from acoharmony import _databricks
from acoharmony._databricks._transfer import DatabricksTransferManager


class TestModuleImports:
    """Test module-level imports and exports."""

    @pytest.mark.unit
    def test_databricks_init_exports_transfer_manager(self):
        """DatabricksTransferManager is exported from __init__."""
        assert DatabricksTransferManager is not None

    @pytest.mark.unit
    def test_databricks_all_contains_transfer_manager(self):
        """__all__ contains DatabricksTransferManager."""
        assert "DatabricksTransferManager" in _databricks.__all__

    @pytest.mark.unit
    def test_transfer_manager_importable_from_submodule(self):
        """Can import DatabricksTransferManager from _transfer submodule."""
        assert DatabricksTransferManager is not None


class TestDatabricksTransferManagerInit:
    """Tests for DatabricksTransferManager initialization."""

    @pytest.mark.unit
    def test_init_with_defaults(self):
        """Test initialization with default paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            dest = tmppath / "dest"
            tracking = tmppath / "tracking"

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=dest, tracking_dir=tracking
            )

            assert manager.dest_dir == dest
            assert manager.tracking_dir == tracking
            assert dest.exists()
            assert tracking.exists()

    @pytest.mark.unit
    def test_init_creates_directories(self):
        """Test that init creates directories if they don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            dest = tmppath / "new_dest"
            tracking = tmppath / "new_tracking"

            DatabricksTransferManager(source_dirs=[], dest_dir=dest, tracking_dir=tracking)

            assert dest.exists()
            assert tracking.exists()

    @pytest.mark.unit
    def test_init_loads_state(self):
        """Test that init loads existing state file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            tracking = tmppath / "tracking"
            tracking.mkdir()

            # Create a state file
            state_file = tracking / "transfer_state.json"
            state_data = {
                "files": {"test.parquet": {"mtime": 123, "size": 456}},
                "last_run": "2025-01-01T00:00:00",
                "total_transfers": 5,
            }
            state_file.write_text(json.dumps(state_data))

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tracking
            )

            assert manager.state["total_transfers"] == 5
            assert "test.parquet" in manager.state["files"]


class TestStateManagement:
    """Tests for state loading and saving."""

    @pytest.mark.unit
    def test_load_state_missing_file(self):
        """Test loading state when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tmppath / "tracking"
            )

            assert manager.state == {"files": {}, "last_run": None, "total_transfers": 0}

    @pytest.mark.unit
    def test_load_state_corrupted_file(self):
        """Test loading state with corrupted JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            tracking = tmppath / "tracking"
            tracking.mkdir()

            # Create corrupted JSON file
            state_file = tracking / "transfer_state.json"
            state_file.write_text("{ invalid json }")

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tracking
            )

            # Should return default state on error
            assert manager.state == {"files": {}, "last_run": None, "total_transfers": 0}

    @pytest.mark.unit
    def test_save_state(self):
        """Test saving state to file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tmppath / "tracking"
            )

            manager.state["test_key"] = "test_value"
            manager._save_state()

            # Verify file was written
            with open(manager.state_file) as f:
                saved_state = json.load(f)

            assert saved_state["test_key"] == "test_value"


class TestFileSignature:
    """Tests for file signature methods."""

    @pytest.mark.unit
    def test_get_file_signature(self, tmp_path):
        """Test getting file signature."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data")
            f.flush()
            path = Path(f.name)

        try:
            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
            )
            sig = manager._get_file_signature(path)

            assert "mtime" in sig
            assert "size" in sig
            assert "path" in sig
            assert sig["size"] == 9  # len(b"test data")
        finally:
            path.unlink()

    @pytest.mark.unit
    def test_has_file_changed_new_file(self, tmp_path):
        """Test change detection for new file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data")
            f.flush()
            path = Path(f.name)

        try:
            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
            )
            assert manager._has_file_changed(path) is True
        finally:
            path.unlink()

    @pytest.mark.unit
    def test_has_file_changed_unchanged_file(self, tmp_path):
        """Test change detection for unchanged file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data")
            f.flush()
            path = Path(f.name)

        try:
            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
            )

            # Record the signature
            sig = manager._get_file_signature(path)
            manager.state["files"][str(path)] = sig

            # Should not detect change
            assert manager._has_file_changed(path) is False
        finally:
            path.unlink()

    @pytest.mark.unit
    def test_has_file_changed_modified_file(self, tmp_path):
        """Test change detection for modified file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("original")
            f.flush()
            path = Path(f.name)

        try:
            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
            )

            # Record original signature
            sig = manager._get_file_signature(path)
            manager.state["files"][str(path)] = sig

            # Modify the file
            import time

            time.sleep(0.01)  # Ensure mtime changes
            with open(path, "w") as f:
                f.write("modified content")

            # Should detect change
            assert manager._has_file_changed(path) is True
        finally:
            path.unlink()


class TestParquetCodec:
    """Tests for parquet codec detection."""

    @pytest.mark.unit
    def test_get_parquet_codec(self, tmp_path):
        """Test getting compression codec from parquet file."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = Path(f.name)

        try:
            # Create a test parquet file with known compression
            df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
            df.write_parquet(path, compression="snappy")

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
            )
            codec = manager._get_parquet_codec(path)

            assert codec == "SNAPPY"
        finally:
            path.unlink()

    @pytest.mark.unit
    def test_get_parquet_codec_error(self, tmp_path):
        """Test codec detection with invalid file."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"not a parquet file")
            path = Path(f.name)

        try:
            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
            )
            codec = manager._get_parquet_codec(path)

            assert codec == "ERROR"
        finally:
            path.unlink()


class TestConvertAndTransfer:
    """Tests for file conversion and transfer."""

    @pytest.mark.unit
    def test_convert_and_transfer_success(self):
        """Test successful file conversion and transfer."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            source_file = tmppath / "source.parquet"
            dest_dir = tmppath / "dest"
            dest_dir.mkdir()
            dest_file = dest_dir / "source.parquet"

            # Create source parquet file
            df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
            df.write_parquet(source_file, compression="zstd")

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=dest_dir, tracking_dir=tmppath / "tracking"
            )

            result = manager._convert_and_transfer(source_file, dest_file)

            assert result is True
            assert dest_file.exists()

            # Verify it's SNAPPY compressed
            codec = manager._get_parquet_codec(dest_file)
            assert codec == "SNAPPY"

    @pytest.mark.unit
    def test_convert_and_transfer_error(self, tmp_path):
        """Test conversion with invalid source file."""
        source_file = tmp_path / "invalid.parquet"
        dest_file = tmp_path / "dest.parquet"

        # Create invalid parquet file
        source_file.write_text("invalid data")

        manager = DatabricksTransferManager(
            source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
        )
        result = manager._convert_and_transfer(source_file, dest_file)

        assert result is False


class TestTransfer:
    """Tests for main transfer method."""

    @pytest.mark.unit
    def test_transfer_no_files(self):
        """Test transfer with no source files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            source_dir = tmppath / "source"
            source_dir.mkdir()

            manager = DatabricksTransferManager(
                source_dirs=[source_dir],
                dest_dir=tmppath / "dest",
                tracking_dir=tmppath / "tracking",
            )

            stats = manager.transfer()

            assert stats["total_files"] == 0
            assert stats["transferred"] == 0
            assert stats["skipped"] == 0
            assert stats["failed"] == 0

    @pytest.mark.unit
    def test_transfer_with_files(self):
        """Test transfer with actual parquet files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            source_dir = tmppath / "source"
            source_dir.mkdir()

            # Create test parquet files
            for i in range(3):
                df = pl.DataFrame({"a": [i], "b": [f"val{i}"]})
                df.write_parquet(source_dir / f"file{i}.parquet")

            manager = DatabricksTransferManager(
                source_dirs=[source_dir],
                dest_dir=tmppath / "dest",
                tracking_dir=tmppath / "tracking",
            )

            stats = manager.transfer()

            assert stats["total_files"] == 3
            assert stats["transferred"] == 3
            assert stats["skipped"] == 0
            assert stats["failed"] == 0

    @pytest.mark.unit
    def test_transfer_skips_unchanged_files(self):
        """Test that unchanged files are skipped."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            source_dir = tmppath / "source"
            source_dir.mkdir()

            # Create test file
            df = pl.DataFrame({"a": [1], "b": ["val"]})
            df.write_parquet(source_dir / "file.parquet")

            manager = DatabricksTransferManager(
                source_dirs=[source_dir],
                dest_dir=tmppath / "dest",
                tracking_dir=tmppath / "tracking",
            )

            # First transfer
            stats1 = manager.transfer()
            assert stats1["transferred"] == 1

            # Second transfer (should skip)
            stats2 = manager.transfer()
            assert stats2["skipped"] == 1
            assert stats2["transferred"] == 0

    @pytest.mark.unit
    def test_transfer_force_retransfers_all(self):
        """Test that force=True retransfers all files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            source_dir = tmppath / "source"
            source_dir.mkdir()

            # Create test file
            df = pl.DataFrame({"a": [1], "b": ["val"]})
            df.write_parquet(source_dir / "file.parquet")

            manager = DatabricksTransferManager(
                source_dirs=[source_dir],
                dest_dir=tmppath / "dest",
                tracking_dir=tmppath / "tracking",
            )

            # First transfer
            manager.transfer()

            # Force retransfer
            stats = manager.transfer(force=True)
            assert stats["transferred"] == 1
            assert stats["skipped"] == 0


class TestStatus:
    """Tests for status method."""

    @pytest.mark.unit
    def test_status_initial(self):
        """Test status with no transfers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tmppath / "tracking"
            )
            status = manager.status()

            assert status["last_run"] is None
            assert status["total_transfers"] == 0
            assert status["total_files_tracked"] == 0

    @pytest.mark.unit
    def test_status_after_transfer(self):
        """Test status after a transfer."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            source_dir = tmppath / "source"
            source_dir.mkdir()

            # Create test file
            df = pl.DataFrame({"a": [1], "b": ["val"]})
            df.write_parquet(source_dir / "file.parquet")

            manager = DatabricksTransferManager(
                source_dirs=[source_dir],
                dest_dir=tmppath / "dest",
                tracking_dir=tmppath / "tracking",
            )

            manager.transfer()
            status = manager.status()

            assert status["last_run"] is not None
            assert status["total_transfers"] == 1
            assert status["total_files_tracked"] == 1


class TestAggregateLogs:
    """Tests for log aggregation."""

    @pytest.mark.unit
    def test_aggregate_logs_no_files(self):
        """Test aggregation with no state files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            tracking_dir = tmppath / "tracking"
            tracking_dir.mkdir()

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tmppath / "tracking"
            )

            result = manager.aggregate_logs(
                tracking_dir=tracking_dir, output_file=tmppath / "output.parquet"
            )

            assert result is None

    @pytest.mark.unit
    def test_aggregate_logs_with_transform_state(self):
        """Test aggregation with transform state files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            tracking_dir = tmppath / "tracking"
            tracking_dir.mkdir()

            # Create a transform state file
            state_file = tracking_dir / "cclf1_state.json"
            state_data = {
                "transform_name": "cclf1",
                "last_run": "2025-01-01T00:00:00",
                "last_success": "2025-01-01T00:00:00",
                "total_runs": 5,
                "successful_runs": 4,
                "failed_runs": 1,
                "metadata": {
                    "last_run_records": 100,
                    "last_run_files": 5,
                },
                "files_processed": {"processed": ["file1", "file2"]},
            }
            state_file.write_text(json.dumps(state_data))

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tmppath / "tracking"
            )

            output_file = tmppath / "output.parquet"
            result = manager.aggregate_logs(tracking_dir=tracking_dir, output_file=output_file)

            assert result == output_file
            assert output_file.exists()

            # Read and verify
            df = pl.read_parquet(output_file)
            assert len(df) == 1
            assert df["transform_name"][0] == "cclf1"

    @pytest.mark.unit
    def test_aggregate_logs_with_4icli_state(self):
        """Test aggregation with 4icli state file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            tracking_dir = tmppath / "tracking"
            tracking_dir.mkdir()

            # Create 4icli state file
            state_file = tracking_dir / "4icli_state.json"
            state_data = {
                "file1.zip": {
                    "filename": "file1.zip",
                    "category": "CCLF",
                    "file_type_code": "P",
                    "file_size": 1024,
                    "file_hash": "abc123",
                    "download_timestamp": "2025-01-01T00:00:00",
                    "source_path": "/path/to/file1.zip",
                }
            }
            state_file.write_text(json.dumps(state_data))

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tmppath / "tracking"
            )

            output_file = tmppath / "output.parquet"
            result = manager.aggregate_logs(tracking_dir=tracking_dir, output_file=output_file)

            assert result == output_file
            assert output_file.exists()

            # Read and verify
            df = pl.read_parquet(output_file)
            assert len(df) == 1
            assert df["filename"][0] == "file1.zip"

    @pytest.mark.unit
    def test_aggregate_logs_with_4icli_inventory(self):
        """Test aggregation with 4icli inventory state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            tracking_dir = tmppath / "tracking"
            tracking_dir.mkdir()

            # Create 4icli inventory state file
            state_file = tracking_dir / "4icli_inventory_state.json"
            state_data = {
                "apm_id": "12345",
                "total_files": 10,
                "categories": ["CCLF", "BAR"],
                "years": [2024, 2025],
                "files_by_category": {"CCLF": 5, "BAR": 5},
                "files_by_year": {"2024": 5, "2025": 5},
                "files": [
                    {
                        "filename": "file1.zip",
                        "category": "CCLF",
                        "file_type_code": "P",
                        "size_bytes": 1024,
                        "year": 2025,
                        "last_updated": "2025-01-01",
                        "discovered_at": "2025-01-01T00:00:00",
                    }
                ],
            }
            state_file.write_text(json.dumps(state_data))

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tmppath / "tracking"
            )

            output_file = tmppath / "output.parquet"
            result = manager.aggregate_logs(tracking_dir=tracking_dir, output_file=output_file)

            assert result == output_file
            assert output_file.exists()

            # Read and verify
            df = pl.read_parquet(output_file)
            assert len(df) == 2  # 1 inventory summary + 1 file record

    @pytest.mark.unit
    def test_aggregate_logs_handles_errors(self):
        """Test aggregation handles corrupted files gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            tracking_dir = tmppath / "tracking"
            tracking_dir.mkdir()

            # Create corrupted state file
            state_file = tracking_dir / "bad_state.json"
            state_file.write_text("{ invalid json }")

            # Create valid state file
            valid_state = tracking_dir / "good_state.json"
            valid_state.write_text(
                json.dumps(
                    {
                        "transform_name": "test",
                        "last_run": "2025-01-01T00:00:00",
                        "total_runs": 1,
                        "files_processed": {"processed": []},
                    }
                )
            )

            manager = DatabricksTransferManager(
                source_dirs=[], dest_dir=tmppath / "dest", tracking_dir=tmppath / "tracking"
            )

            output_file = tmppath / "output.parquet"
            result = manager.aggregate_logs(tracking_dir=tracking_dir, output_file=output_file)

            # Should still work with valid file
            assert result == output_file
            assert output_file.exists()


class TestTransferEdgeCases:
    """Cover remaining edge cases in DatabricksTransferManager."""

    @pytest.mark.unit
    def test_save_state_exception(self, tmp_path):
        """Cover lines 75-76: _save_state fails gracefully."""
        manager = DatabricksTransferManager(
            source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
        )
        manager.state_file = tmp_path / "readonly" / "state.json"
        # Parent doesn't exist → writing fails
        manager._save_state()  # Should log error but not raise

    @pytest.mark.unit
    def test_get_parquet_codec_no_row_groups(self, tmp_path):
        """Cover line 111: parquet file with 0 row groups → 'UNKNOWN'."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = tmp_path / "empty.parquet"
        # Write empty parquet (0 rows, but schema exists)
        table = pa.table({"a": pa.array([], type=pa.int64())})
        pq.write_table(table, path)

        manager = DatabricksTransferManager(
            source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
        )
        codec = manager._get_parquet_codec(path)
        # Empty table may still have 1 row group; if not, returns UNKNOWN
        assert codec in ("UNKNOWN", "SNAPPY", "NONE", "UNCOMPRESSED")

    @pytest.mark.unit
    def test_transfer_failure_path(self, tmp_path):
        """Cover lines 213-214: _convert_and_transfer returns False → failure stats."""
        from unittest.mock import patch

        source_dir = tmp_path / "source"
        source_dir.mkdir()
        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        # Create a source parquet file
        df = pl.DataFrame({"a": [1]})
        src_file = source_dir / "test.parquet"
        df.write_parquet(src_file)

        manager = DatabricksTransferManager(
            source_dirs=[source_dir], dest_dir=dest_dir
        )

        # Mock _convert_and_transfer to return False (failure)
        with patch.object(manager, "_convert_and_transfer", return_value=False):
            stats = manager.transfer()
            assert stats["failed"] >= 1
            assert "test.parquet" in stats["failed_files"]


class TestDatabricksSourceDirNotExists:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_databricks_source_dir_not_exists(self, tmp_path):
        """173->172: source_dir.exists() is False."""
        from acoharmony._databricks._transfer import DatabricksTransferManager
        mgr = DatabricksTransferManager(
            source_dirs=[tmp_path / "nonexistent"],
            dest_dir=tmp_path / "dest",
            tracking_dir=tmp_path / "tracking",
        )
        stats = mgr.transfer()
        assert stats["total_files"] == 0


class TestGetParquetCodecZeroRowGroups:
    """Cover lines 109->111, 111: _get_parquet_codec returns 'UNKNOWN' for 0 row groups."""

    @pytest.mark.unit
    def test_parquet_zero_row_groups_returns_unknown(self, tmp_path):
        """Lines 109->111, 111: ParquetFile with 0 row groups returns 'UNKNOWN'."""
        from unittest.mock import MagicMock, patch as _patch
        from acoharmony._databricks._transfer import DatabricksTransferManager

        manager = DatabricksTransferManager(
            source_dirs=[], dest_dir=tmp_path / "dest", tracking_dir=tmp_path / "tracking"
        )
        fake_path = tmp_path / "empty.parquet"
        fake_path.write_bytes(b"fake")

        # Mock ParquetFile to return 0 row groups
        mock_pf = MagicMock()
        mock_pf.metadata.num_row_groups = 0

        with _patch("acoharmony._databricks._transfer.pq.ParquetFile", return_value=mock_pf):
            codec = manager._get_parquet_codec(fake_path)
        assert codec == "UNKNOWN"
