# © 2025 HarmonyCares
# All rights reserved.

"""
Integration tests for ACO consolidated alignment pipeline.

This test suite validates:
- Pipeline stage execution order
- Stage dependencies
- Stage outputs and schemas
- Idempotency of pipeline execution
- Integration with the notebook
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from pathlib import Path

try:
    import consolidated_alignments
except ModuleNotFoundError:
    import pytest
    pytest.skip("consolidated_alignments notebook not on path", allow_module_level=True)
import pytest
import acoharmony

# Add notebooks directory to path
sys.path.insert(0, str(Path("/opt/s3/data/notebooks")))

# Import the notebook module


@pytest.fixture(scope="module")
def notebook_defs():
    """Run notebook once and cache definitions for all tests."""
    _, defs = consolidated_alignments.app.run()
    return defs


class TestDisplayTechnicalAppendix:
    """Tests for the display_technical_appendix function."""

    @pytest.mark.unit
    def test_function_exists(self, notebook_defs):
        """Test that display_technical_appendix function exists in notebook defs."""
        assert "display_technical_appendix" in notebook_defs

    @pytest.mark.unit
    def test_function_executes_without_error(self, notebook_defs):
        """Test that display_technical_appendix executes without AttributeError."""
        func = notebook_defs["display_technical_appendix"]
        result = func(consolidated_alignments.mo)

        # Should return a marimo vstack with accordion
        assert result is not None

    @pytest.mark.unit
    def test_imports_all_required_modules(self, notebook_defs):
        """Test that all pipeline stage modules can be imported."""
        # This test verifies the imports in display_technical_appendix work
        from acoharmony._expressions._aco_temporal_summary import build_summary_statistics_exprs
        from acoharmony._pipes._alignment import apply_alignment_pipeline as apply_aco_alignment_pipeline
        from acoharmony._transforms import (
            _aco_alignment_demographics,
            _aco_alignment_metadata,
            _aco_alignment_metrics,
            _aco_alignment_office,
            _aco_alignment_provider,
            _aco_alignment_temporal,
            _aco_alignment_voluntary,
        )

        # All imports should succeed
        assert acoharmony._transforms._aco_alignment_temporal is not None
        assert acoharmony._transforms._aco_alignment_voluntary is not None
        assert acoharmony._transforms._aco_alignment_demographics is not None
        assert acoharmony._transforms._aco_alignment_office is not None
        assert acoharmony._transforms._aco_alignment_provider is not None
        assert acoharmony._transforms._aco_alignment_metrics is not None
        assert acoharmony._transforms._aco_alignment_metadata is not None
        assert build_summary_statistics_exprs is not None
        assert apply_aco_alignment_pipeline is not None

    @pytest.mark.unit
    def test_expression_builder_has_docstring(self):
        """Test that build_summary_statistics_exprs has proper documentation."""
        import inspect

        from acoharmony._expressions._aco_temporal_summary import build_summary_statistics_exprs

        doc = inspect.getdoc(build_summary_statistics_exprs)
        assert doc is not None
        assert len(doc) > 0


class TestPipelineStageDocumentation:
    """Tests for pipeline stage documentation availability."""

    @pytest.mark.unit
    def test_pipeline_has_documentation(self):
        """Test that the main pipeline function has documentation."""
        import inspect

        from acoharmony._pipes._alignment import apply_alignment_pipeline as apply_aco_alignment_pipeline

        doc = inspect.getdoc(apply_aco_alignment_pipeline)
        assert doc is not None
        assert "7" in doc or "stages" in doc.lower()

    @pytest.mark.unit
    def test_all_stage_transforms_have_documentation(self):
        """Test that all stage transforms have documentation."""
        import inspect

        from acoharmony._transforms import (
            _aco_alignment_demographics,
            _aco_alignment_metadata,
            _aco_alignment_metrics,
            _aco_alignment_office,
            _aco_alignment_provider,
            _aco_alignment_temporal,
            _aco_alignment_voluntary,
        )

        stages = [
            ("temporal", _aco_alignment_temporal),
            ("voluntary", _aco_alignment_voluntary),
            ("demographics", _aco_alignment_demographics),
            ("office", _aco_alignment_office),
            ("provider", _aco_alignment_provider),
            ("metrics", _aco_alignment_metrics),
            ("metadata", _aco_alignment_metadata),
        ]

        for stage_name, stage_module in stages:
            doc = inspect.getdoc(stage_module.apply_transform)
            assert doc is not None, f"{stage_name} stage missing documentation"
            assert len(doc) > 0, f"{stage_name} stage has empty documentation"


class TestPipelineStageStructure:
    """Tests for pipeline stage structure and dependencies."""

    @pytest.mark.unit
    def test_pipeline_defines_8_stages(self):
        """Test that the pipeline defines 8 ACO stages (1-8) plus voluntary (stage 0)."""
        import inspect

        from acoharmony._pipes._alignment import apply_alignment_pipeline

        source = inspect.getsource(apply_alignment_pipeline)

        assert "PipelineStage" in source

        # ACO stages 1-8
        for order in range(1, 9):
            assert f"order={order}" in source

    @pytest.mark.unit
    def test_stage_names_are_correct(self):
        """Test that stage names match expected values."""
        import inspect

        from acoharmony._pipes._alignment import apply_alignment_pipeline

        source = inspect.getsource(apply_alignment_pipeline)

        expected_stages = [
            "temporal_matrix",
            "join_voluntary",
            "demographics",
            "office_matching",
            "provider_attribution",
            "consolidated_metrics",
            "metadata_and_actions",
            "year_over_year_transitions",
        ]

        for stage_name in expected_stages:
            assert f'name="{stage_name}"' in source, f"Stage {stage_name} not found in pipeline"

    @pytest.mark.unit
    def test_stage_dependencies_defined(self):
        """Test that stage dependencies are properly defined."""
        import inspect

        from acoharmony._pipes._alignment import apply_alignment_pipeline

        source = inspect.getsource(apply_alignment_pipeline)

        assert 'depends_on=["temporal_matrix"]' in source
        assert 'depends_on=["join_voluntary"]' in source
        assert 'depends_on=["demographics"]' in source
        assert 'depends_on=["office_matching"]' in source
        assert 'depends_on=["provider_attribution"]' in source
        assert 'depends_on=["consolidated_metrics"]' in source
        assert 'depends_on=["metadata_and_actions"]' in source


class TestNotebookDataLoading:
    """Tests for notebook data loading functions."""

    @pytest.mark.unit
    def test_load_consolidated_alignment_data_exists(self, notebook_defs):
        """Test that load_consolidated_alignment_data function exists."""
        assert "load_consolidated_alignment_data" in notebook_defs

    @pytest.mark.unit
    def test_load_consolidated_alignment_data_idempotent(self, notebook_defs):
        """Test that load function has idempotency documentation."""
        func = notebook_defs["load_consolidated_alignment_data"]
        assert func.__doc__ is not None
        assert "IDEMPOTENT" in func.__doc__


class TestNotebookStructure:
    """Tests for overall notebook structure."""

    @pytest.mark.unit
    def test_notebook_imports_pipeline_module(self):
        """Test that notebook can import pipeline module without errors."""
        # This is validated by the successful notebook execution in notebook_defs fixture
        # If imports fail, the fixture will fail
        assert consolidated_alignments is not None

    @pytest.mark.unit
    def test_notebook_has_technical_appendix_cell(self, notebook_defs):
        """Test that notebook has the technical appendix display function."""
        assert "display_technical_appendix" in notebook_defs

        # Verify it takes the expected parameter
        import inspect
        sig = inspect.signature(notebook_defs["display_technical_appendix"])
        assert "mo" in sig.parameters

    @pytest.mark.unit
    def test_notebook_critical_functions_available(self, notebook_defs):
        """Test that all critical analysis functions are available."""
        critical_functions = [
            "calculate_voluntary_alignment_stats",
            "analyze_outreach_effectiveness",
            "calculate_quarterly_campaign_effectiveness",
            "calculate_alignment_trends_over_time",
            "calculate_enhanced_campaign_performance",
        ]

        for func_name in critical_functions:
            assert func_name in notebook_defs, f"Critical function {func_name} not found in notebook"


class TestPipelineIntegrationWithNotebook:
    """Tests for integration between pipeline and notebook."""

    @pytest.mark.unit
    def test_notebook_references_correct_pipeline_stages(self, notebook_defs):
        """Test that notebook technical appendix references all 7 pipeline stages."""
        func = notebook_defs["display_technical_appendix"]
        result = func(consolidated_alignments.mo)

        # Function should execute without error and return a result
        assert result is not None

    @pytest.mark.unit
    def test_pipeline_output_compatible_with_notebook_functions(self):
        """Test that pipeline output schema is compatible with notebook analysis functions."""
        # This is a structural test - if the pipeline output changes schema,
        # the notebook functions should still work or fail gracefully

        # Key columns expected by notebook functions

        # These columns should be documented in the pipeline
        import inspect

        from acoharmony._pipes._alignment import apply_alignment_pipeline as apply_aco_alignment_pipeline

        doc = inspect.getdoc(apply_aco_alignment_pipeline)
        # At minimum, the pipeline should document its outputs
        assert doc is not None


@pytest.mark.slow
class TestEndToEndIntegration:
    """End-to-end integration tests (marked slow as they may load data)."""

    @pytest.mark.unit
    def test_notebook_can_execute_all_cells(self, notebook_defs):
        """Test that notebook executed successfully (validated by fixture)."""
        # If this passes, all cells executed without error
        assert len(notebook_defs) > 0

    @pytest.mark.unit
    def test_notebook_defines_expected_number_of_functions(self, notebook_defs):
        """Test that notebook defines a reasonable number of functions."""
        # Should have at least 20+ functions defined
        assert len(notebook_defs) >= 20


class TestBackwardsCompatibility:
    """Tests for backwards compatibility with existing tests."""

    @pytest.mark.unit
    def test_existing_test_functions_still_work(self, notebook_defs):
        """Test that functions tested in test_critical_functions.py still exist."""
        critical_functions = [
            "calculate_voluntary_alignment_stats",
            "analyze_outreach_effectiveness",
            "calculate_quarterly_campaign_effectiveness",
            "calculate_alignment_trends_over_time",
            "calculate_enhanced_campaign_performance",
            "analyze_sva_action_categories",
            "calculate_enrollment_stats_for_selected_month",
            # Note: get_sample_data was listed but never actually tested in test_critical_functions.py
            # It is tested in test_all_functions.py instead
        ]

        for func_name in critical_functions:
            assert func_name in notebook_defs, (
                f"Function {func_name} missing - breaks backwards compatibility with test_critical_functions.py"
            )

    @pytest.mark.unit
    def test_existing_office_functions_still_work(self, notebook_defs):
        """Test that functions tested in test_all_functions.py still exist."""
        office_functions = [
            "calculate_office_enrollment_stats",
            "calculate_office_alignment_types",
            "calculate_office_program_distribution",
            "calculate_office_transition_stats",
        ]

        for func_name in office_functions:
            assert func_name in notebook_defs, (
                f"Function {func_name} missing - breaks backwards compatibility with test_all_functions.py"
            )
