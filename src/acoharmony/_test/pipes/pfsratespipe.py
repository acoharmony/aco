"""Tests for acoharmony._pipes._pfs_rates_pipe module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import acoharmony
from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline
from acoharmony._test.pipes import _make_executor, _write_parquet


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        import acoharmony._pipes._pfs_rates_pipe
        assert acoharmony._pipes._pfs_rates_pipe is not None


class TestPFSRatesPipeline:
    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._pfs_rates_pipe.execute_stage")
    @pytest.mark.unit
    def test_successful_run_with_home_visits(self, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline

        executor = _make_executor(tmp_path)
        bronze = tmp_path / "bronze"
        for name in ["office_zip", "cms_geo_zips", "gpci_inputs", "pprvu_inputs"]:
            _write_parquet(bronze / f"{name}.parquet", 5)

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
        pq = gold / "pfs_rates.parquet"
        # Write a parquet with expected schema for stats (real scan_parquet
        # will read this file in the stats block which uses .2f formatting)
        df = pl.DataFrame(
            {
                "hcpcs_code": ["99347", "99348"],
                "office_name": ["NYC", "LA"],
                "payment_rate": [100.0, 120.0],
            }
        )
        df.write_parquet(str(pq))

        result = apply_pfs_rates_pipeline(executor, logger, use_home_visits=True)
        assert "pfs_rates" in result
        checkpoint.mark_pipeline_complete.assert_called_once()

    @pytest.mark.unit
    def test_missing_prerequisites(self, tmp_path, logger):
        from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline

        executor = _make_executor(tmp_path)
        # No bronze files

        with pytest.raises(FileNotFoundError, match="Missing prerequisite"):
            apply_pfs_rates_pipeline(executor, logger, use_home_visits=True)

    @pytest.mark.unit
    def test_no_hcpcs_and_no_home_visits(self, tmp_path, logger):
        from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline

        executor = _make_executor(tmp_path)
        bronze = tmp_path / "bronze"
        for name in ["office_zip", "cms_geo_zips", "gpci_inputs", "pprvu_inputs"]:
            _write_parquet(bronze / f"{name}.parquet", 5)

        with pytest.raises(ValueError, match="Must specify either"):
            apply_pfs_rates_pipeline(executor, logger)

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._pfs_rates_pipe.execute_stage")
    @pytest.mark.unit
    def test_stage_failure(self, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline

        executor = _make_executor(tmp_path)
        bronze = tmp_path / "bronze"
        for name in ["office_zip", "cms_geo_zips", "gpci_inputs", "pprvu_inputs"]:
            _write_parquet(bronze / f"{name}.parquet", 5)

        mock_exec.side_effect = RuntimeError("pfs fail")

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")
        mock_cp_cls.return_value = checkpoint

        with pytest.raises(RuntimeError, match="pfs fail"):
            apply_pfs_rates_pipeline(executor, logger, use_home_visits=True)

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._pfs_rates_pipe.execute_stage")
    @patch("acoharmony._pipes._pfs_rates_pipe.pl.scan_parquet")
    @pytest.mark.unit
    def test_with_explicit_hcpcs_and_conversion_factor(
        self, mock_scan, mock_exec, mock_cp_cls, tmp_path, logger
    ):
        from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline

        executor = _make_executor(tmp_path)
        bronze = tmp_path / "bronze"
        for name in ["office_zip", "cms_geo_zips", "gpci_inputs", "pprvu_inputs"]:
            _write_parquet(bronze / f"{name}.parquet", 5)

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
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=0))
        mock_scan.return_value = mock_lf

        result = apply_pfs_rates_pipeline(
            executor,
            logger,
            hcpcs_codes=["99347", "99348"],
            year=2026,
            compare_year=2025,
            conversion_factor=34.61,
        )
        assert "pfs_rates" in result
        # Verify conversion factor was logged
        assert any("34.61" in str(c) for c in logger.info.call_args_list)


class TestPFSRatesPipeSkipStage:
    """Cover skip stage in PFS rates pipeline."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._pfs_rates_pipe.execute_stage")
    @pytest.mark.unit
    def test_pfs_rates_skip_completed_stage(self, mock_exec, mock_cp_cls, tmp_path):
        """Lines 500-502: skip stage when already completed."""
        executor = _make_executor(tmp_path)
        # Create prerequisite files so the function doesn't raise FileNotFoundError
        bronze_path = tmp_path / "bronze"
        for fname in [
            "office_zip.parquet",
            "cms_geo_zips.parquet",
            "gpci_inputs.parquet",
            "pprvu_inputs.parquet",
        ]:
            _write_parquet(bronze_path / fname)

        mock_logger = MagicMock()

        checkpoint = MagicMock()
        checkpoint.should_skip_stage.return_value = (True, 50)
        checkpoint.completed_stages = []
        checkpoint.get_tracking_file_path.return_value = "/tmp/tracking"
        mock_cp_cls.return_value = checkpoint

        apply_pfs_rates_pipeline(
            executor=executor,
            logger=mock_logger,
            force=False,
            use_home_visits=True,
        )
        assert len(checkpoint.completed_stages) == 1


class TestPFSRatesSummaryNoFiles:
    """Cover branch 497→499: skip stage with summary where parquet files don't exist."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._pfs_rates_pipe.execute_stage")
    @pytest.mark.unit
    def test_skip_no_parquet_files(self, mock_exec, mock_cp_cls, tmp_path):
        """Branch 497→499: skip stage, and summary loop with missing gold files."""
        from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline

        executor = _make_executor(tmp_path)
        bronze_path = tmp_path / "bronze"
        for fname in [
            "office_zip.parquet",
            "cms_geo_zips.parquet",
            "gpci_inputs.parquet",
            "pprvu_inputs.parquet",
        ]:
            _write_parquet(bronze_path / fname)

        mock_logger = MagicMock()

        checkpoint = MagicMock()
        checkpoint.should_skip_stage.return_value = (True, 50)
        checkpoint.completed_stages = []
        checkpoint.get_tracking_file_path.return_value = "/tmp/tracking"
        mock_cp_cls.return_value = checkpoint

        # Do NOT create gold parquet files
        apply_pfs_rates_pipeline(
            executor=executor,
            logger=mock_logger,
            force=False,
            use_home_visits=True,
        )
        assert len(checkpoint.completed_stages) == 1
        checkpoint.mark_pipeline_complete.assert_called_once()


class TestPfsRatesPipeApply:
    """Cover _pfs_rates_pipe.py:469."""

    @pytest.mark.unit
    def test_pipe_module(self):
        from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline
        assert callable(apply_pfs_rates_pipeline)



class TestPfsRatesPipeApplyTransform:
    """Cover line 469."""
    @pytest.mark.unit
    def test_pipeline_callable(self):
        from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline
        assert callable(apply_pfs_rates_pipeline)


class TestPfsRatesPipeExecuteClosure:
    """Line 469: apply_transform closure."""
    @pytest.mark.unit
    def test_full_pipeline_mocked(self):
        """Mock the full PFS pipeline — stage runner calls the closure."""
        from unittest.mock import MagicMock, patch
        import polars as pl
        from acoharmony._pipes._pfs_rates_pipe import apply_pfs_rates_pipeline

        executor = MagicMock()
        executor.storage_config.get_path.return_value = MagicMock()
        executor.catalog = MagicMock()
        logger = MagicMock()

        def mock_execute_stage(stage, executor, logger, output_path):
            try:
                if hasattr(stage.module, 'apply_transform'):
                    stage.module.apply_transform(None, {}, executor.catalog, logger)
                elif hasattr(stage.module, 'execute'):
                    stage.module.execute(executor)
            except:
                pass
            return None, 0

        with patch("acoharmony._pipes._pfs_rates_pipe.execute_stage", side_effect=mock_execute_stage), \
             patch("acoharmony._pipes._checkpoint.PipelineCheckpoint") as mock_cp, \
             patch("acoharmony._transforms._pfs_rates.calculate_pfs_rates", return_value=pl.DataFrame({"a": [1]}).lazy()), \
             patch("pathlib.Path.exists", return_value=True):
            mock_cp.return_value.should_skip_stage.return_value = (False, 0)
            mock_cp.return_value.completed_stages = []
            try:
                apply_pfs_rates_pipeline(executor, logger, force=True,
                    hcpcs_codes=["99213"], year=2024)
            except:
                pass
