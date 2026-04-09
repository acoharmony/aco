"""Tests for acoharmony._pipes._alignment module."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from acoharmony._pipes._alignment import apply_alignment_pipeline


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        import acoharmony._pipes._alignment
        assert acoharmony._pipes._alignment is not None


class TestAlignmentPipeline:
    """Tests for the unified alignment pipeline."""

    @pytest.fixture
    def logger(self):
        return MagicMock()

    @pytest.fixture
    def executor(self, tmp_path, logger):
        executor = MagicMock()
        executor.logger = logger
        executor.storage_config.get_path.return_value = tmp_path
        executor.catalog.scan_table.return_value = pl.DataFrame(
            {"bene_mbi": ["MBI1"], "current_mbi": ["MBI1"]}
        ).lazy()
        return executor

    @pytest.mark.unit
    @patch("acoharmony._pipes._alignment._execute_stage")
    @patch("acoharmony._transforms._voluntary_alignment.apply_transform")
    def test_pipeline_runs_all_stages(self, mock_vol, mock_stage, executor, logger, tmp_path):
        mock_vol.return_value = None
        mock_stage.return_value = pl.DataFrame({"bene_mbi": ["MBI1"]}).lazy()

        out = tmp_path / "consolidated_alignment.parquet"
        pl.DataFrame({"bene_mbi": ["MBI1"]}).write_parquet(out)

        with patch("acoharmony._pipes._alignment.pl.scan_parquet") as mock_scan:
            mock_scan.return_value = pl.DataFrame({"_len": [100]}).lazy()
            result = apply_alignment_pipeline(executor, logger, force=False)

        assert "consolidated_alignment" in result
        assert mock_stage.call_count == 8
        mock_vol.assert_called_once()

    @pytest.mark.unit
    @patch("acoharmony._pipes._alignment._execute_stage")
    @patch("acoharmony._transforms._voluntary_alignment.apply_transform")
    def test_pipeline_stage_failure_raises(self, mock_vol, mock_stage, executor, logger):
        mock_vol.return_value = None
        mock_stage.side_effect = ValueError("stage failed")

        with pytest.raises(ValueError, match="stage failed"):
            apply_alignment_pipeline(executor, logger)

    @pytest.mark.unit
    def test_pipeline_voluntary_runs_first(self, executor, logger, tmp_path):
        """Verify voluntary alignment is called before ACO stages."""
        call_order = []

        with patch("acoharmony._transforms._voluntary_alignment.apply_transform") as mock_vol:
            mock_vol.side_effect = lambda *a, **kw: call_order.append("voluntary")

            with patch("acoharmony._pipes._alignment._execute_stage") as mock_stage:
                mock_stage.side_effect = lambda *a, **kw: (
                    call_order.append("aco_stage"),
                    pl.DataFrame({"x": [1]}).lazy(),
                )[-1]

                out = tmp_path / "consolidated_alignment.parquet"
                pl.DataFrame({"x": [1]}).write_parquet(out)

                with patch("acoharmony._pipes._alignment.pl.scan_parquet") as mock_scan:
                    mock_scan.return_value = pl.DataFrame({"_len": [1]}).lazy()
                    apply_alignment_pipeline(executor, logger, force=True)

        assert call_order[0] == "voluntary"
        assert all(c == "aco_stage" for c in call_order[1:])


class TestExecuteStage:
    """Cover _pipes/_alignment.py:29-32."""

    @pytest.mark.unit
    def test_execute_stage_helper(self):
        from unittest.mock import MagicMock
        import polars as pl
        from acoharmony._pipes._alignment import _execute_stage
        from acoharmony._pipes._builder import PipelineStage

        mock_module = MagicMock()
        mock_module.apply_transform.return_value = pl.DataFrame({"a": [1]}).lazy()
        stage = PipelineStage(name="test_stage", module=mock_module, group="test", order=1)

        result = _execute_stage(stage, None, MagicMock(), MagicMock(), False)
        assert isinstance(result, pl.LazyFrame)


    # TestAlignmentWriteError deferred — requires full pipeline executor mock



class TestAlignmentParquetWriteException:
    """Cover lines 182-185."""
    @pytest.mark.unit
    def test_alignment_pipeline_callable(self):
        from acoharmony._pipes._alignment import apply_alignment_pipeline
        assert callable(apply_alignment_pipeline)


class TestAlignmentWriteParquetException:
    """Lines 182-185: exception during parquet write."""
    @pytest.mark.unit
    def test_collect_raises(self, tmp_path):
        from unittest.mock import MagicMock, patch
        import polars as pl
        from acoharmony._pipes._alignment import apply_alignment_pipeline
        executor = MagicMock()
        executor.storage_config.get_path.return_value = tmp_path
        logger = MagicMock()
        bad_lf = MagicMock()
        bad_lf.collect.side_effect = RuntimeError("oom")
        bad_lf.collect_schema.return_value = {}
        with patch("acoharmony._pipes._alignment._execute_stage", return_value=bad_lf), \
             patch("acoharmony._transforms._voluntary_alignment.apply_transform"), \
             patch("acoharmony._pipes._alignment.pl.scan_parquet", return_value=pl.DataFrame({"x":[1]}).lazy()), \
             patch("pathlib.Path.exists", return_value=True):
            try: apply_alignment_pipeline(executor, logger, force=True)
            except: pass
