"""Tests for acoharmony._pipes._registry module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._registry is not None


# ============================================================================
# 1. PipelineRegistry
# ============================================================================


class TestPipelineRegistry:
    @pytest.mark.unit
    def test_register_no_metadata(self):
        from acoharmony._pipes._registry import PipelineRegistry

        @PipelineRegistry.register(name="no_meta")
        def no_meta_func():
            pass

        assert PipelineRegistry.get_metadata("no_meta") is None

    @pytest.mark.unit
    def test_list_pipelines(self):
        from acoharmony._pipes._registry import PipelineRegistry

        @PipelineRegistry.register(name="a")
        def fa():
            pass

        @PipelineRegistry.register(name="b")
        def fb():
            pass

        names = PipelineRegistry.list_pipelines()
        assert "a" in names
        assert "b" in names

    @pytest.mark.unit
    def test_get_metadata_missing(self):
        from acoharmony._pipes._registry import PipelineRegistry

        assert PipelineRegistry.get_metadata("nonexistent") is None

    @pytest.mark.unit
    def test_clear(self):
        from acoharmony._pipes._registry import PipelineRegistry

        @PipelineRegistry.register(name="temp", metadata={"x": 1})
        def tmp():
            pass

        PipelineRegistry.clear()
        assert PipelineRegistry.list_pipelines() == []
        assert PipelineRegistry.get_metadata("temp") is None


