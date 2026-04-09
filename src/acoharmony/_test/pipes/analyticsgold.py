"""Tests for acoharmony._pipes._analytics_gold module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from acoharmony._pipes._analytics_gold import apply_analytics_gold_pipeline

import acoharmony
from acoharmony._test.pipes import _make_executor, _write_parquet


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._analytics_gold is not None


class TestAnalyticsGoldPipeline:
    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._analytics_gold.execute_stage")
    @patch("acoharmony._pipes._analytics_gold.pl.scan_parquet")
    @pytest.mark.unit
    def test_normal_stages_run(self, mock_scan, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._analytics_gold import apply_analytics_gold_pipeline

        executor = _make_executor(tmp_path)
        gold_path = tmp_path / "gold"
        # Create medical_claim.parquet so service_category stage works
        _write_parquet(gold_path / "medical_claim.parquet", 10)

        mock_exec.return_value = (None, 0)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark_complete(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark_complete
        mock_cp_cls.return_value = checkpoint

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=5))
        mock_scan.return_value = mock_lf

        # Patch ServiceCategoryTransform at its source (local import inside function)
        with (
            patch("acoharmony._store.StorageBackend"),
            patch("acoharmony._transforms._service_category.ServiceCategoryTransform") as mock_sct,
        ):
            transform_instance = MagicMock()
            transform_instance.execute.return_value = {
                "service_category": gold_path / "service_category.parquet"
            }
            mock_sct.return_value = transform_instance
            _write_parquet(gold_path / "service_category.parquet", 5)

            apply_analytics_gold_pipeline(executor, logger)

        # service_category + 4 normal stages = 5 completed
        assert len(checkpoint.completed_stages) == 5
        checkpoint.mark_pipeline_complete.assert_called_once()

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @pytest.mark.unit
    def test_service_category_no_medical_claim(self, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._analytics_gold import apply_analytics_gold_pipeline

        executor = _make_executor(tmp_path)
        # Do NOT create medical_claim.parquet

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark_complete(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark_complete
        mock_cp_cls.return_value = checkpoint

        with (
            patch("acoharmony._pipes._analytics_gold.execute_stage") as mock_exec,
            patch("acoharmony._pipes._analytics_gold.pl.scan_parquet") as mock_scan,
        ):
            mock_exec.return_value = (None, 0)
            mock_lf = MagicMock()
            mock_lf.select.return_value = mock_lf
            mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=5))
            mock_scan.return_value = mock_lf

            apply_analytics_gold_pipeline(executor, logger)

        # service_category should be skipped (no medical_claim), but 4 others run
        assert len(checkpoint.completed_stages) == 4
        assert any("Skipping service_category" in str(c) for c in logger.info.call_args_list)

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._analytics_gold.execute_stage")
    @pytest.mark.unit
    def test_stage_failure_logged(self, mock_exec, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._analytics_gold import apply_analytics_gold_pipeline

        executor = _make_executor(tmp_path)
        gold_path = tmp_path / "gold"
        _write_parquet(gold_path / "medical_claim.parquet", 10)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")
        mock_cp_cls.return_value = checkpoint

        # Make service category fail
        with (
            patch("acoharmony._store.StorageBackend"),
            patch("acoharmony._transforms._service_category.ServiceCategoryTransform") as mock_sct,
        ):
            mock_sct.return_value.execute.side_effect = RuntimeError("svc fail")
            with pytest.raises(RuntimeError, match="svc fail"):
                apply_analytics_gold_pipeline(executor, logger)


class TestAnalyticsGoldSkip:
    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._analytics_gold.pl.scan_parquet")
    @pytest.mark.unit
    def test_all_skipped(self, mock_scan, mock_cp_cls, tmp_path, logger):
        executor = _make_executor(tmp_path)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (True, 10)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")
        mock_cp_cls.return_value = checkpoint

        # Create gold dir with parquet files
        gold = tmp_path / "gold"
        for name in [
            "service_category",
            "readmissions_summary",
            "readmissions_summary_deduped",
            "financial_pmpm_by_category",
            "beneficiary_metrics",
        ]:
            _write_parquet(gold / f"{name}.parquet", 3)

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=3))
        mock_scan.return_value = mock_lf

        apply_analytics_gold_pipeline(executor, logger)
        assert len(checkpoint.completed_stages) == 5
        checkpoint.mark_pipeline_complete.assert_called_once()


class TestAnalyticsGoldSkipWithMissingFiles:
    """Cover branch 125→127: skip path where completed parquet files do NOT exist."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @pytest.mark.unit
    def test_skip_with_no_parquet_files(self, mock_cp_cls, tmp_path, logger):
        """Branch 125→127: should_skip=True but parquet files don't exist in summary loop."""
        from acoharmony._pipes._analytics_gold import apply_analytics_gold_pipeline

        executor = _make_executor(tmp_path)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (True, 10)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")
        mock_cp_cls.return_value = checkpoint

        # Do NOT create any gold parquet files - summary loop will find nothing
        apply_analytics_gold_pipeline(executor, logger)
        assert len(checkpoint.completed_stages) == 5
        checkpoint.mark_pipeline_complete.assert_called_once()
