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
    def test_register_with_name(self):
        from acoharmony._pipes._registry import PipelineRegistry

        @PipelineRegistry.register(name="my_pipe", metadata={"version": "1.0"})
        def my_func():
            return [1, 2, 3]

        assert PipelineRegistry.get_pipeline("my_pipe") is my_func
        assert PipelineRegistry.get_metadata("my_pipe") == {"version": "1.0"}

    @pytest.mark.unit
    def test_register_defaults_to_func_name(self):
        from acoharmony._pipes._registry import PipelineRegistry

        @PipelineRegistry.register()
        def auto_named():
            return "hello"

        assert PipelineRegistry.get_pipeline("auto_named") is auto_named

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
    def test_get_pipeline_missing(self):
        from acoharmony._pipes._registry import PipelineRegistry

        assert PipelineRegistry.get_pipeline("nonexistent") is None

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


class TestRegisterPipelineDecorator:
    @pytest.mark.unit
    def test_convenience_decorator(self):
        from acoharmony._pipes._registry import PipelineRegistry, register_pipeline

        @register_pipeline(name="conv_pipe", author="test")
        def conv():
            pass

        assert PipelineRegistry.get_pipeline("conv_pipe") is conv
        meta = PipelineRegistry.get_metadata("conv_pipe")
        assert meta["author"] == "test"

    @pytest.mark.unit
    def test_convenience_decorator_no_name(self):
        from acoharmony._pipes._registry import PipelineRegistry, register_pipeline

        @register_pipeline()
        def my_auto():
            pass

        assert PipelineRegistry.get_pipeline("my_auto") is my_auto
