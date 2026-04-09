# © 2025 HarmonyCares
# All rights reserved.
"""Tests for acoharmony.__main__ module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
from unittest.mock import patch, MagicMock

import acoharmony.__main__
from acoharmony._runner import TransformRunner


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""

        assert acoharmony.__main__ is not None


class TestRun:
    """Tests for the run() function."""

    @pytest.mark.unit
    def test_run_returns_transform_runner_instance(self):
        """Test that run() returns a TransformRunner instance."""
        result = acoharmony.__main__.run()

        # Verify the result is a TransformRunner instance
        assert isinstance(result, TransformRunner)

    @pytest.mark.unit
    def test_run_creates_new_runner_each_call(self):
        """Test that run() creates a new TransformRunner instance on each call."""
        result1 = acoharmony.__main__.run()
        result2 = acoharmony.__main__.run()

        # Verify both are TransformRunner instances
        assert isinstance(result1, TransformRunner)
        assert isinstance(result2, TransformRunner)

        # Verify they are different instances
        assert result1 is not result2

    @pytest.mark.unit
    def test_run_returned_runner_has_list_pipelines_method(self):
        """Test that returned runner has list_pipelines method."""
        result = acoharmony.__main__.run()

        # Verify the runner has list_pipelines method
        assert hasattr(result, 'list_pipelines')
        assert callable(result.list_pipelines)

    @pytest.mark.unit
    def test_run_returned_runner_is_operational(self):
        """Test that returned runner can call list_pipelines."""
        result = acoharmony.__main__.run()

        # Call list_pipelines to verify runner is operational
        pipelines = result.list_pipelines()

        # Verify we get a list (even if empty)
        assert isinstance(pipelines, list)
