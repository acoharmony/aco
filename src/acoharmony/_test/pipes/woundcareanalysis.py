"""Tests for acoharmony._pipes._wound_care_analysis module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._test.pipes import _make_executor, _write_parquet


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._wound_care_analysis is not None


class TestWoundCareAnalysisPipeline:
    @pytest.mark.unit
    def test_analysis_stage_creation(self):
        from acoharmony._pipes._wound_care_analysis import AnalysisStage

        s = AnalysisStage(
            "high_freq", MagicMock(), "wca", 1, ["dep1"], ["patient_level", "npi_summary"]
        )
        assert s.output_keys == ["patient_level", "npi_summary"]
        assert s.depends_on == ["dep1"]
        assert s.name == "high_freq"

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @patch("acoharmony._pipes._wound_care_analysis.pl.scan_parquet")
    @pytest.mark.unit
    def test_dict_result_multi_output(self, mock_scan, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._wound_care_analysis import apply_wound_care_analysis_pipeline

        executor = _make_executor(tmp_path)
        tmp_path / "gold"

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark
        mock_cp_cls.return_value = checkpoint

        # Mock all 5 transform modules
        mock_modules = {}
        for mod_name in [
            "wound_care_high_frequency",
            "wound_care_high_cost",
            "wound_care_clustered",
            "wound_care_duplicates",
            "wound_care_identical_patterns",
        ]:
            mod = MagicMock()
            mod.__name__ = mod_name
            mock_modules[mod_name] = mod

        # High frequency returns dict with 2 outputs
        patient_lf = MagicMock()
        patient_lf.collect.return_value = MagicMock()
        patient_lf.collect.return_value.write_parquet = MagicMock()
        npi_lf = MagicMock()
        npi_lf.collect.return_value = MagicMock()
        npi_lf.collect.return_value.write_parquet = MagicMock()
        mock_modules["wound_care_high_frequency"].execute.return_value = {
            "patient_level": patient_lf,
            "npi_summary": npi_lf,
        }

        # High cost returns dict with 1 output
        hc_lf = MagicMock()
        hc_lf.collect.return_value = MagicMock()
        hc_lf.collect.return_value.write_parquet = MagicMock()
        mock_modules["wound_care_high_cost"].execute.return_value = {
            "high_cost_patients": hc_lf,
        }

        # Clustered returns dict with 2 outputs
        cl_detail = MagicMock()
        cl_detail.collect.return_value = MagicMock()
        cl_detail.collect.return_value.write_parquet = MagicMock()
        cl_npi = MagicMock()
        cl_npi.collect.return_value = MagicMock()
        cl_npi.collect.return_value.write_parquet = MagicMock()
        mock_modules["wound_care_clustered"].execute.return_value = {
            "cluster_details": cl_detail,
            "npi_summary": cl_npi,
        }

        # Duplicates
        dup_detail = MagicMock()
        dup_detail.collect.return_value = MagicMock()
        dup_detail.collect.return_value.write_parquet = MagicMock()
        dup_npi = MagicMock()
        dup_npi.collect.return_value = MagicMock()
        dup_npi.collect.return_value.write_parquet = MagicMock()
        mock_modules["wound_care_duplicates"].execute.return_value = {
            "duplicate_details": dup_detail,
            "npi_summary": dup_npi,
        }

        # Identical patterns
        pat_detail = MagicMock()
        pat_detail.collect.return_value = MagicMock()
        pat_detail.collect.return_value.write_parquet = MagicMock()
        pat_npi = MagicMock()
        pat_npi.collect.return_value = MagicMock()
        pat_npi.collect.return_value.write_parquet = MagicMock()
        mock_modules["wound_care_identical_patterns"].execute.return_value = {
            "pattern_details": pat_detail,
            "npi_summary": pat_npi,
        }

        # Mock scan for row counts at end
        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=3))
        mock_scan.return_value = mock_lf

        with (
            patch(
                "acoharmony._transforms.wound_care_high_frequency",
                mock_modules["wound_care_high_frequency"],
            ),
            patch(
                "acoharmony._transforms.wound_care_high_cost", mock_modules["wound_care_high_cost"]
            ),
            patch(
                "acoharmony._transforms.wound_care_clustered", mock_modules["wound_care_clustered"]
            ),
            patch(
                "acoharmony._transforms.wound_care_duplicates",
                mock_modules["wound_care_duplicates"],
            ),
            patch(
                "acoharmony._transforms.wound_care_identical_patterns",
                mock_modules["wound_care_identical_patterns"],
            ),
        ):
            apply_wound_care_analysis_pipeline(executor, logger)

        assert len(checkpoint.completed_stages) == 5
        checkpoint.mark_pipeline_complete.assert_called_once()

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @pytest.mark.unit
    def test_single_output_stage(self, mock_cp_cls, tmp_path, logger):
        """Test a stage that returns a single LazyFrame (not dict)."""
        from acoharmony._pipes._wound_care_analysis import apply_wound_care_analysis_pipeline

        executor = _make_executor(tmp_path)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark
        mock_cp_cls.return_value = checkpoint

        # Make first stage return single LazyFrame (not dict), rest return dicts
        single_lf = MagicMock()
        single_lf.collect.return_value = MagicMock()
        single_lf.collect.return_value.write_parquet = MagicMock()

        dict_lf = MagicMock()
        dict_lf.collect.return_value = MagicMock()
        dict_lf.collect.return_value.write_parquet = MagicMock()

        mod_single = MagicMock()
        mod_single.__name__ = "test_mod"
        mod_single.execute.return_value = single_lf  # Not a dict

        mod_dict = MagicMock()
        mod_dict.__name__ = "test_mod2"
        mod_dict.execute.return_value = {"patient_level": dict_lf, "npi_summary": dict_lf}

        mod_dict2 = MagicMock()
        mod_dict2.__name__ = "test_mod3"
        mod_dict2.execute.return_value = {"high_cost_patients": dict_lf}

        mod_dict3 = MagicMock()
        mod_dict3.__name__ = "test_mod4"
        mod_dict3.execute.return_value = {"cluster_details": dict_lf, "npi_summary": dict_lf}

        mod_dict4 = MagicMock()
        mod_dict4.__name__ = "test_mod5"
        mod_dict4.execute.return_value = {"duplicate_details": dict_lf, "npi_summary": dict_lf}

        mod_dict5 = MagicMock()
        mod_dict5.__name__ = "test_mod6"
        mod_dict5.execute.return_value = {"pattern_details": dict_lf, "npi_summary": dict_lf}

        with (
            patch("acoharmony._pipes._wound_care_analysis.pl.scan_parquet") as mock_scan,
            patch("acoharmony._transforms.wound_care_high_frequency", mod_single),
            patch("acoharmony._transforms.wound_care_high_cost", mod_dict2),
            patch("acoharmony._transforms.wound_care_clustered", mod_dict3),
            patch("acoharmony._transforms.wound_care_duplicates", mod_dict4),
            patch("acoharmony._transforms.wound_care_identical_patterns", mod_dict5),
        ):
            mock_lf = MagicMock()
            mock_lf.select.return_value = mock_lf
            mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=1))
            mock_scan.return_value = mock_lf

            apply_wound_care_analysis_pipeline(executor, logger)

        assert len(checkpoint.completed_stages) == 5

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @pytest.mark.unit
    def test_stage_failure(self, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._wound_care_analysis import apply_wound_care_analysis_pipeline

        executor = _make_executor(tmp_path)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")
        mock_cp_cls.return_value = checkpoint

        mod = MagicMock()
        mod.__name__ = "fail_mod"
        mod.execute.side_effect = RuntimeError("analysis fail")

        with patch("acoharmony._transforms.wound_care_high_frequency", mod):
            with pytest.raises(RuntimeError, match="analysis fail"):
                apply_wound_care_analysis_pipeline(executor, logger)

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @pytest.mark.unit
    def test_skip_stage(self, mock_cp_cls, tmp_path, logger):
        from acoharmony._pipes._wound_care_analysis import apply_wound_care_analysis_pipeline

        executor = _make_executor(tmp_path)
        gold = tmp_path / "gold"

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (True, 10)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")
        mock_cp_cls.return_value = checkpoint

        # Create all expected output files so all_outputs_exist is True
        for name in [
            "wound_care_high_frequency_patient",
            "wound_care_high_frequency_npi",
            "wound_care_high_cost",
            "wound_care_clustered_details",
            "wound_care_clustered_npi",
            "wound_care_duplicates_details",
            "wound_care_duplicates_npi",
            "wound_care_identical_patterns_details",
            "wound_care_identical_patterns_npi",
        ]:
            _write_parquet(gold / f"{name}.parquet", 3)

        with patch("acoharmony._pipes._wound_care_analysis.pl.scan_parquet") as mock_scan:
            mock_lf = MagicMock()
            mock_lf.select.return_value = mock_lf
            mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=3))
            mock_scan.return_value = mock_lf

            apply_wound_care_analysis_pipeline(executor, logger)

        assert len(checkpoint.completed_stages) == 5


class TestWoundCareOutputKeyMapping:
    """Test the output key -> file name mapping logic."""

    @pytest.mark.unit
    def test_output_key_mapping_exhaustive(self, tmp_path):
        from acoharmony._pipes._wound_care_analysis import AnalysisStage

        gold_path = tmp_path / "gold"
        gold_path.mkdir()

        # Test all key types
        stage = AnalysisStage(
            name="test",
            module=MagicMock(),
            group="g",
            order=1,
            depends_on=[],
            output_keys=[
                "patient_level",
                "npi_summary",
                "high_cost_patients",
                "cluster_details",
                "duplicate_details",
                "pattern_details",
                "unknown_key",
            ],
        )

        # Simulate the mapping logic from the pipeline
        stage_outputs = {}
        for key in stage.output_keys:
            if key == "patient_level":
                output_name = f"wound_care_{stage.name}_patient"
            elif key == "npi_summary":
                output_name = f"wound_care_{stage.name}_npi"
            elif key == "high_cost_patients":
                output_name = f"wound_care_{stage.name}"
            elif key in ["cluster_details", "duplicate_details", "pattern_details"]:
                output_name = f"wound_care_{stage.name}_details"
            else:
                output_name = f"wound_care_{stage.name}_{key}"
            stage_outputs[key] = gold_path / f"{output_name}.parquet"

        assert stage_outputs["patient_level"].name == "wound_care_test_patient.parquet"
        assert stage_outputs["npi_summary"].name == "wound_care_test_npi.parquet"
        assert stage_outputs["high_cost_patients"].name == "wound_care_test.parquet"
        assert stage_outputs["cluster_details"].name == "wound_care_test_details.parquet"
        assert stage_outputs["unknown_key"].name == "wound_care_test_unknown_key.parquet"


# ============================================================================
# 25. Checkpoint skip when all_outputs_exist is False in wound_care_analysis
# ============================================================================


class TestWoundCareOutputKeyElseBranch:
    """Cover branch 185->188: unknown output key hits else clause."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @pytest.mark.unit
    def test_stage_with_unknown_output_key(self, mock_cp_cls, tmp_path, logger):
        """Branch 185->188: output key not in known set falls to else clause."""
        from acoharmony._pipes._wound_care_analysis import (
            AnalysisStage,
            apply_wound_care_analysis_pipeline,
        )

        executor = _make_executor(tmp_path)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark
        mock_cp_cls.return_value = checkpoint

        dict_lf = MagicMock()
        dict_lf.collect.return_value = MagicMock()
        dict_lf.collect.return_value.write_parquet = MagicMock()

        # Module returns dict with keys matching stage.output_keys
        mod_custom = MagicMock()
        mod_custom.__name__ = "custom_mod"
        mod_custom.execute.return_value = {
            "patient_level": dict_lf,
            "custom_output": dict_lf,  # Unknown key -> else branch
        }

        mod_dict = MagicMock()
        mod_dict.__name__ = "test_mod"
        mod_dict.execute.return_value = {"high_cost_patients": dict_lf}

        mod_dict2 = MagicMock()
        mod_dict2.__name__ = "test_mod2"
        mod_dict2.execute.return_value = {"cluster_details": dict_lf, "npi_summary": dict_lf}

        mod_dict3 = MagicMock()
        mod_dict3.__name__ = "test_mod3"
        mod_dict3.execute.return_value = {"duplicate_details": dict_lf, "npi_summary": dict_lf}

        mod_dict4 = MagicMock()
        mod_dict4.__name__ = "test_mod4"
        mod_dict4.execute.return_value = {"pattern_details": dict_lf, "npi_summary": dict_lf}

        # Patch AnalysisStage to inject "custom_output" as an output_key for first stage
        original_init = AnalysisStage.__init__
        call_count = [0]

        def patched_init(self, name, module, group, order, depends_on, output_keys):
            call_count[0] += 1
            if call_count[0] == 1:
                # First stage: add unknown key to output_keys -> triggers else branch
                original_init(self, name, module, group, order, depends_on,
                              ["patient_level", "custom_output"])
            else:
                original_init(self, name, module, group, order, depends_on, output_keys)

        with (
            patch("acoharmony._pipes._wound_care_analysis.pl.scan_parquet") as mock_scan,
            patch.object(AnalysisStage, "__init__", patched_init),
            patch("acoharmony._transforms.wound_care_high_frequency", mod_custom),
            patch("acoharmony._transforms.wound_care_high_cost", mod_dict),
            patch("acoharmony._transforms.wound_care_clustered", mod_dict2),
            patch("acoharmony._transforms.wound_care_duplicates", mod_dict3),
            patch("acoharmony._transforms.wound_care_identical_patterns", mod_dict4),
        ):
            mock_lf = MagicMock()
            mock_lf.select.return_value = mock_lf
            mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=1))
            mock_scan.return_value = mock_lf

            apply_wound_care_analysis_pipeline(executor, logger)

        assert len(checkpoint.completed_stages) == 5


class TestWoundCareSingleOutputStage:
    """Cover branch 175->192: stage without output_keys attribute."""

    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @pytest.mark.unit
    def test_plain_pipeline_stage_without_output_keys(self, mock_cp_cls, tmp_path, logger):
        """Branch 175->192: hasattr(stage, 'output_keys') is False -> single-output path."""
        from acoharmony._pipes._wound_care_analysis import apply_wound_care_analysis_pipeline
        from acoharmony._pipes._stage import PipelineStage

        executor = _make_executor(tmp_path)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (False, 0)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark
        mock_cp_cls.return_value = checkpoint

        single_lf = MagicMock()
        single_lf.collect.return_value = MagicMock()
        single_lf.collect.return_value.write_parquet = MagicMock()

        dict_lf = MagicMock()
        dict_lf.collect.return_value = MagicMock()
        dict_lf.collect.return_value.write_parquet = MagicMock()

        # First module returns a single LazyFrame (not dict)
        mod1 = MagicMock()
        mod1.__name__ = "mod1"
        mod1.execute.return_value = single_lf

        mod2 = MagicMock()
        mod2.__name__ = "mod2"
        mod2.execute.return_value = {"high_cost_patients": dict_lf}

        mod3 = MagicMock()
        mod3.__name__ = "mod3"
        mod3.execute.return_value = {"cluster_details": dict_lf, "npi_summary": dict_lf}

        mod4 = MagicMock()
        mod4.__name__ = "mod4"
        mod4.execute.return_value = {"duplicate_details": dict_lf, "npi_summary": dict_lf}

        mod5 = MagicMock()
        mod5.__name__ = "mod5"
        mod5.execute.return_value = {"pattern_details": dict_lf, "npi_summary": dict_lf}

        # Patch AnalysisStage for first stage to be a plain PipelineStage
        from acoharmony._pipes._wound_care_analysis import AnalysisStage

        original_init = AnalysisStage.__init__
        call_count = [0]

        def patched_init(self, name, module, group, order, depends_on, output_keys):
            call_count[0] += 1
            if call_count[0] == 1:
                # First stage: create as PipelineStage (no output_keys)
                PipelineStage.__init__(self, name, module, group, order, depends_on)
                # Explicitly delete output_keys if set
                if hasattr(self, "output_keys"):
                    delattr(self, "output_keys")
            else:
                original_init(self, name, module, group, order, depends_on, output_keys)

        with (
            patch("acoharmony._pipes._wound_care_analysis.pl.scan_parquet") as mock_scan,
            patch.object(AnalysisStage, "__init__", patched_init),
            patch("acoharmony._transforms.wound_care_high_frequency", mod1),
            patch("acoharmony._transforms.wound_care_high_cost", mod2),
            patch("acoharmony._transforms.wound_care_clustered", mod3),
            patch("acoharmony._transforms.wound_care_duplicates", mod4),
            patch("acoharmony._transforms.wound_care_identical_patterns", mod5),
        ):
            mock_lf = MagicMock()
            mock_lf.select.return_value = mock_lf
            mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=1))
            mock_scan.return_value = mock_lf

            apply_wound_care_analysis_pipeline(executor, logger)

        assert len(checkpoint.completed_stages) == 5


class TestWoundCareAnalysisSkipPartial:
    @patch("acoharmony._pipes._checkpoint.PipelineCheckpoint")
    @pytest.mark.unit
    def test_skip_but_missing_outputs(self, mock_cp_cls, tmp_path, logger):
        """should_skip True but all_outputs_exist False => stage runs."""
        from acoharmony._pipes._wound_care_analysis import apply_wound_care_analysis_pipeline

        executor = _make_executor(tmp_path)

        checkpoint = MagicMock()
        checkpoint.completed_stages = []
        checkpoint.should_skip_stage.return_value = (True, 10)
        checkpoint.get_tracking_file_path.return_value = Path("/tmp/t.json")

        def mark(name):
            checkpoint.completed_stages.append(name)

        checkpoint.mark_stage_complete.side_effect = mark
        mock_cp_cls.return_value = checkpoint

        # Don't create output files - all_outputs_exist will be False
        # So stages should run even though should_skip is True

        dict_lf = MagicMock()
        dict_lf.collect.return_value = MagicMock()
        dict_lf.collect.return_value.write_parquet = MagicMock()

        mod = MagicMock()
        mod.__name__ = "test_mod"
        mod.execute.return_value = {"patient_level": dict_lf, "npi_summary": dict_lf}

        mod2 = MagicMock()
        mod2.__name__ = "test_mod2"
        mod2.execute.return_value = {"high_cost_patients": dict_lf}

        mod3 = MagicMock()
        mod3.__name__ = "test_mod3"
        mod3.execute.return_value = {"cluster_details": dict_lf, "npi_summary": dict_lf}

        mod4 = MagicMock()
        mod4.__name__ = "test_mod4"
        mod4.execute.return_value = {"duplicate_details": dict_lf, "npi_summary": dict_lf}

        mod5 = MagicMock()
        mod5.__name__ = "test_mod5"
        mod5.execute.return_value = {"pattern_details": dict_lf, "npi_summary": dict_lf}

        with (
            patch("acoharmony._pipes._wound_care_analysis.pl.scan_parquet") as mock_scan,
            patch("acoharmony._transforms.wound_care_high_frequency", mod),
            patch("acoharmony._transforms.wound_care_high_cost", mod2),
            patch("acoharmony._transforms.wound_care_clustered", mod3),
            patch("acoharmony._transforms.wound_care_duplicates", mod4),
            patch("acoharmony._transforms.wound_care_identical_patterns", mod5),
        ):
            mock_lf = MagicMock()
            mock_lf.select.return_value = mock_lf
            mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=1))
            mock_scan.return_value = mock_lf

            apply_wound_care_analysis_pipeline(executor, logger)

        # Should have executed stages because outputs didn't exist
        assert mod.execute.called
        assert len(checkpoint.completed_stages) == 5
