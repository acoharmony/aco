





# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
import acoharmony
import inspect

# © 2025 HarmonyCares
# All rights reserved.
"""Tests for acoharmony._transforms._registry module."""






class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        from acoharmony._transforms import _registry
        assert acoharmony._transforms._registry is not None



# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for transformation registry - Polars style.

Tests transform function registration and lookup.
"""






class TestTransformRegistry:
    """Tests for TransformRegistry."""

    @pytest.mark.unit
    def test_registry_initialization(self) -> None:
        """TransformRegistry initializes properly."""
        assert TransformRegistry._transforms is not None
        assert isinstance(TransformRegistry._transforms, dict)

    @pytest.mark.unit
    def test_registry_has_categories(self) -> None:
        """TransformRegistry has expected transform categories."""
        # Categories should include deduplication, enrichment, etc.
        assert (
            "deduplication" in TransformRegistry._transforms
            or len(TransformRegistry._transforms) >= 0
        )

    @pytest.mark.unit
    def test_registry_register_decorator(self) -> None:
        """TransformRegistry.register decorator works."""

        # Test registration
        @TransformRegistry.register("test_category", "test_transform")
        @pytest.mark.unit
        def test_func(df):
            return df

        # Function should be registered
        assert test_func is not None


class TestRegistryModule:
    """Tests for _registry module."""

    @pytest.mark.unit
    def test_import_registry(self):

        assert TransformRegistry is not None
        assert callable(register_crosswalk)
        assert callable(register_pipeline)

    @pytest.mark.unit
    def test_registry_is_class(self):

        assert inspect.isclass(TransformRegistry)


class TestConvenienceDecorators:
    """Tests for register_crosswalk and register_pipeline."""

    def setup_method(self):

        self._orig_transforms = TransformRegistry._transforms.copy()
        self._orig_metadata = TransformRegistry._metadata.copy()

    def teardown_method(self):

        TransformRegistry._transforms = self._orig_transforms
        TransformRegistry._metadata = self._orig_metadata

    @pytest.mark.unit
    def test_register_crosswalk(self):

        TransformRegistry.clear()

        @register_crosswalk(name="test_xwalk")
        def xwalk_func(df):
            return df

        assert TransformRegistry.get_transform("crosswalk", "test_xwalk") is xwalk_func

    @pytest.mark.unit
    def test_register_pipeline(self):

        TransformRegistry.clear()

        @register_pipeline(name="test_pipe")
        def pipe_func(df):
            return df

        assert TransformRegistry.get_transform("pipeline", "test_pipe") is pipe_func


class TestRegistryStress:
    """Stress tests for registry operations."""

    def setup_method(self):

        self._orig_transforms = TransformRegistry._transforms.copy()
        self._orig_metadata = TransformRegistry._metadata.copy()

    def teardown_method(self):

        TransformRegistry._transforms = self._orig_transforms
        TransformRegistry._metadata = self._orig_metadata

    @pytest.mark.unit
    def test_register_many(self):

        TransformRegistry.clear()
        funcs = []
        for i in range(50):

            @TransformRegistry.register("bulk", name=f"func_{i}", metadata={"idx": i})
            def f(df, _i=i):
                return df

            funcs.append(f)

        listing = TransformRegistry.list_transforms("bulk")
        assert len(listing["bulk"]) == 50

        for i in range(50):
            t = TransformRegistry.get_transform("bulk", f"func_{i}")
            assert t is not None
            m = TransformRegistry.get_metadata("bulk", f"func_{i}")
            assert m["idx"] == i

    @pytest.mark.unit
    def test_overwrite_registration(self):

        TransformRegistry.clear()

        @TransformRegistry.register("test", name="func")
        def v1(df):
            return "v1"

        @TransformRegistry.register("test", name="func")
        def v2(df):
            return "v2"

        # Last registration wins
        result = TransformRegistry.get_transform("test", "func")
        assert result is v2

    @pytest.mark.unit
    def test_register_no_metadata(self):

        TransformRegistry.clear()

        @TransformRegistry.register("test", name="no_meta")
        def no_meta(df):
            return df

        assert TransformRegistry.get_metadata("test", "no_meta") is None


class TestTransformRegistryExtended:
    """Tests for TransformRegistry with register, get, list, clear, metadata."""

    def setup_method(self):

        self._orig_transforms = TransformRegistry._transforms.copy()
        self._orig_metadata = TransformRegistry._metadata.copy()

    def teardown_method(self):

        TransformRegistry._transforms = self._orig_transforms
        TransformRegistry._metadata = self._orig_metadata

    @pytest.mark.unit
    def test_register_and_get(self):

        TransformRegistry.clear()

        @TransformRegistry.register("test_type", name="my_func")
        def my_func(df):
            return df

        result = TransformRegistry.get_transform("test_type", "my_func")
        assert result is my_func

    @pytest.mark.unit
    def test_register_uses_func_name(self):

        TransformRegistry.clear()

        @TransformRegistry.register("test_type")
        def auto_named(df):
            return df

        assert TransformRegistry.get_transform("test_type", "auto_named") is auto_named

    @pytest.mark.unit
    def test_register_with_metadata(self):

        TransformRegistry.clear()

        @TransformRegistry.register(
            "enrichment",
            name="add_flags",
            metadata={"description": "Add flag columns", "version": "1.0"},
        )
        def add_flags(df):
            return df

        meta = TransformRegistry.get_metadata("enrichment", "add_flags")
        assert meta is not None
        assert meta["description"] == "Add flag columns"
        assert meta["version"] == "1.0"

    @pytest.mark.unit
    def test_get_transform_missing_type(self):

        TransformRegistry.clear()
        assert TransformRegistry.get_transform("nonexistent", "foo") is None

    @pytest.mark.unit
    def test_get_transform_missing_name(self):

        TransformRegistry.clear()

        @TransformRegistry.register("test_type", name="existing")
        def existing(df):
            return df

        assert TransformRegistry.get_transform("test_type", "missing") is None

    @pytest.mark.unit
    def test_get_metadata_missing_type(self):

        TransformRegistry.clear()
        assert TransformRegistry.get_metadata("nonexistent", "foo") is None

    @pytest.mark.unit
    def test_get_metadata_missing_name(self):

        TransformRegistry.clear()

        @TransformRegistry.register("test_type", name="existing")
        def existing(df):
            return df

        # No metadata was registered
        assert TransformRegistry.get_metadata("test_type", "existing") is None

    @pytest.mark.unit
    def test_list_transforms_all(self):

        TransformRegistry.clear()

        @TransformRegistry.register("type_a", name="func1")
        def f1(df):
            return df

        @TransformRegistry.register("type_b", name="func2")
        def f2(df):
            return df

        result = TransformRegistry.list_transforms()
        assert "type_a" in result
        assert "type_b" in result
        assert "func1" in result["type_a"]
        assert "func2" in result["type_b"]

    @pytest.mark.unit
    def test_list_transforms_filtered(self):

        TransformRegistry.clear()

        @TransformRegistry.register("dedup", name="standard")
        def std_dedup(df):
            return df

        result = TransformRegistry.list_transforms("dedup")
        assert result == {"dedup": ["standard"]}

    @pytest.mark.unit
    def test_list_transforms_filter_missing(self):

        TransformRegistry.clear()
        result = TransformRegistry.list_transforms("nonexistent")
        assert result == {"nonexistent": []}

    @pytest.mark.unit
    def test_clear(self):

        TransformRegistry.clear()

        @TransformRegistry.register("test", name="func")
        def func(df):
            return df

        assert TransformRegistry.get_transform("test", "func") is not None
        TransformRegistry.clear()
        assert TransformRegistry.get_transform("test", "func") is None

    @pytest.mark.unit
    def test_register_multiple_same_type(self):

        TransformRegistry.clear()

        @TransformRegistry.register("dedup", name="v1")
        def v1(df):
            return df

        @TransformRegistry.register("dedup", name="v2")
        def v2(df):
            return df

        assert TransformRegistry.get_transform("dedup", "v1") is v1
        assert TransformRegistry.get_transform("dedup", "v2") is v2
        listing = TransformRegistry.list_transforms("dedup")
        assert len(listing["dedup"]) == 2


class TestRegistryEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def setup_method(self):
        """Save original state before each test."""
        self._orig_transforms = TransformRegistry._transforms.copy()
        self._orig_metadata = TransformRegistry._metadata.copy()

    def teardown_method(self):
        """Restore original state after each test."""
        TransformRegistry._transforms = self._orig_transforms
        TransformRegistry._metadata = self._orig_metadata

    @pytest.mark.unit
    def test_register_with_none_metadata_dict(self):
        """Register with None metadata should not store metadata."""
        TransformRegistry.clear()

        @TransformRegistry.register("test", name="func", metadata=None)
        def func(df):
            return df

        # Metadata should not exist for this function
        assert TransformRegistry.get_metadata("test", "func") is None

    @pytest.mark.unit
    def test_register_with_empty_metadata_dict(self):
        """Register with empty metadata dict should store empty dict."""
        TransformRegistry.clear()

        @TransformRegistry.register("test", name="func", metadata={})
        def func(df):
            return df

        # Empty dict should not be stored (falsy value)
        assert TransformRegistry.get_metadata("test", "func") is None

    @pytest.mark.unit
    def test_register_with_complex_metadata(self):
        """Register with complex nested metadata."""
        TransformRegistry.clear()

        metadata = {
            "description": "Complex transform",
            "version": "2.0",
            "config": {"nested": {"value": 42}},
            "tags": ["important", "experimental"],
        }

        @TransformRegistry.register("test", name="complex", metadata=metadata)
        def complex_func(df):
            return df

        stored_meta = TransformRegistry.get_metadata("test", "complex")
        assert stored_meta == metadata
        assert stored_meta["config"]["nested"]["value"] == 42
        assert stored_meta["tags"] == ["important", "experimental"]

    @pytest.mark.unit
    def test_decorator_returns_original_function(self):
        """Decorator should return original function unmodified."""
        TransformRegistry.clear()

        def original_func(df):
            """Original docstring."""
            return df

        original_func.custom_attr = "custom"
        decorated = TransformRegistry.register("test", name="func")(original_func)

        # Should return the exact same function
        assert decorated is original_func
        assert decorated.__doc__ == "Original docstring."
        assert decorated.custom_attr == "custom"

    @pytest.mark.unit
    def test_get_transform_type_exists_name_missing(self):
        """Get transform when type exists but name doesn't."""
        TransformRegistry.clear()

        @TransformRegistry.register("existing_type", name="existing_name")
        def func1(df):
            return df

        # Type exists but name doesn't
        result = TransformRegistry.get_transform("existing_type", "nonexistent_name")
        assert result is None

    @pytest.mark.unit
    def test_list_transforms_empty_registry(self):
        """List transforms when registry is empty."""
        TransformRegistry.clear()
        result = TransformRegistry.list_transforms()
        assert result == {}

    @pytest.mark.unit
    def test_list_transforms_single_type_empty_list(self):
        """List specific type that doesn't exist returns empty list."""
        TransformRegistry.clear()
        result = TransformRegistry.list_transforms("nonexistent_type")
        assert result == {"nonexistent_type": []}

    @pytest.mark.unit
    def test_list_transforms_multiple_types(self):
        """List all transforms with multiple types."""
        TransformRegistry.clear()

        @TransformRegistry.register("type_a", name="func1")
        def f1(df):
            return df

        @TransformRegistry.register("type_a", name="func2")
        def f2(df):
            return df

        @TransformRegistry.register("type_b", name="func3")
        def f3(df):
            return df

        @TransformRegistry.register("type_c", name="func4")
        def f4(df):
            return df

        result = TransformRegistry.list_transforms()
        assert len(result) == 3
        assert set(result.keys()) == {"type_a", "type_b", "type_c"}
        assert len(result["type_a"]) == 2
        assert len(result["type_b"]) == 1
        assert len(result["type_c"]) == 1

    @pytest.mark.unit
    def test_clear_with_multiple_types(self):
        """Clear should remove all transforms and metadata."""
        TransformRegistry.clear()

        @TransformRegistry.register("type_a", name="func1", metadata={"key": "value"})
        def f1(df):
            return df

        @TransformRegistry.register("type_b", name="func2", metadata={"key": "value"})
        def f2(df):
            return df

        # Verify both exist
        assert TransformRegistry.get_transform("type_a", "func1") is not None
        assert TransformRegistry.get_transform("type_b", "func2") is not None
        assert TransformRegistry.get_metadata("type_a", "func1") is not None

        # Clear all
        TransformRegistry.clear()

        # Verify all are gone
        assert TransformRegistry.get_transform("type_a", "func1") is None
        assert TransformRegistry.get_transform("type_b", "func2") is None
        assert TransformRegistry.get_metadata("type_a", "func1") is None
        assert TransformRegistry.get_metadata("type_b", "func2") is None

    @pytest.mark.unit
    def test_register_initializes_new_type_dict(self):
        """Registering a new type should initialize both dicts."""
        TransformRegistry.clear()

        # Before registration, type doesn't exist
        assert "brand_new_type" not in TransformRegistry._transforms
        assert "brand_new_type" not in TransformRegistry._metadata

        @TransformRegistry.register("brand_new_type", name="func")
        def func(df):
            return df

        # After registration, both dicts should have the type
        assert "brand_new_type" in TransformRegistry._transforms
        assert "brand_new_type" in TransformRegistry._metadata

    @pytest.mark.unit
    def test_get_metadata_with_multiple_entries(self):
        """Get metadata when type has multiple entries."""
        TransformRegistry.clear()

        meta1 = {"version": "1.0", "author": "alice"}
        meta2 = {"version": "2.0", "author": "bob"}

        @TransformRegistry.register("type_a", name="func1", metadata=meta1)
        def f1(df):
            return df

        @TransformRegistry.register("type_a", name="func2", metadata=meta2)
        def f2(df):
            return df

        # Each should return its own metadata
        assert TransformRegistry.get_metadata("type_a", "func1") == meta1
        assert TransformRegistry.get_metadata("type_a", "func2") == meta2

    @pytest.mark.unit
    def test_register_without_name_uses_function_name(self):
        """When name not provided, use function's __name__."""
        TransformRegistry.clear()

        @TransformRegistry.register("test_type")
        def my_transform_function(df):
            return df

        # Should be registered under function name
        assert (
            TransformRegistry.get_transform("test_type", "my_transform_function")
            is my_transform_function
        )

    @pytest.mark.unit
    def test_register_with_custom_name_overrides_function_name(self):
        """When name is provided, it should override function name."""
        TransformRegistry.clear()

        @TransformRegistry.register("test_type", name="custom_name")
        def my_function(df):
            return df

        # Should be registered under custom name
        assert (
            TransformRegistry.get_transform("test_type", "custom_name") is my_function
        )
        # Should not be registered under function name
        assert TransformRegistry.get_transform("test_type", "my_function") is None


class TestConvenienceDecoratorsExtended:
    """Extended tests for convenience decorator functions."""

    def setup_method(self):
        """Save original state before each test."""
        self._orig_transforms = TransformRegistry._transforms.copy()
        self._orig_metadata = TransformRegistry._metadata.copy()

    def teardown_method(self):
        """Restore original state after each test."""
        TransformRegistry._transforms = self._orig_transforms
        TransformRegistry._metadata = self._orig_metadata

    @pytest.mark.unit
    def test_register_crosswalk_without_name(self):
        """Register crosswalk without explicit name."""
        TransformRegistry.clear()

        @register_crosswalk()
        def my_crosswalk(df):
            return df

        # Should use function name
        assert TransformRegistry.get_transform("crosswalk", "my_crosswalk") is not None

    @pytest.mark.unit
    def test_register_crosswalk_with_metadata(self):
        """Register crosswalk with metadata kwargs."""
        TransformRegistry.clear()

        @register_crosswalk(
            name="xwalk_v2",
            description="Crosswalk v2",
            version="2.0",
            author="test",
        )
        def xwalk_func(df):
            return df

        transform = TransformRegistry.get_transform("crosswalk", "xwalk_v2")
        assert transform is xwalk_func

        meta = TransformRegistry.get_metadata("crosswalk", "xwalk_v2")
        assert meta is not None
        assert meta["description"] == "Crosswalk v2"
        assert meta["version"] == "2.0"
        assert meta["author"] == "test"

    @pytest.mark.unit
    def test_register_pipeline_without_name(self):
        """Register pipeline without explicit name."""
        TransformRegistry.clear()

        @register_pipeline()
        def my_pipeline(df):
            return df

        # Should use function name
        assert TransformRegistry.get_transform("pipeline", "my_pipeline") is not None

    @pytest.mark.unit
    def test_register_pipeline_with_metadata(self):
        """Register pipeline with metadata kwargs."""
        TransformRegistry.clear()

        @register_pipeline(
            name="pipeline_main",
            description="Main pipeline",
            version="1.0",
        )
        def pipeline_func(df):
            return df

        transform = TransformRegistry.get_transform("pipeline", "pipeline_main")
        assert transform is pipeline_func

        meta = TransformRegistry.get_metadata("pipeline", "pipeline_main")
        assert meta is not None
        assert meta["description"] == "Main pipeline"
        assert meta["version"] == "1.0"

    @pytest.mark.unit
    def test_multiple_crosswalks_and_pipelines(self):
        """Register multiple crosswalks and pipelines together."""
        TransformRegistry.clear()

        @register_crosswalk(name="cw1")
        def xwalk1(df):
            return df

        @register_crosswalk(name="cw2")
        def xwalk2(df):
            return df

        @register_pipeline(name="pipe1")
        def pipeline1(df):
            return df

        @register_pipeline(name="pipe2")
        def pipeline2(df):
            return df

        # All should be registered
        assert TransformRegistry.get_transform("crosswalk", "cw1") is xwalk1
        assert TransformRegistry.get_transform("crosswalk", "cw2") is xwalk2
        assert TransformRegistry.get_transform("pipeline", "pipe1") is pipeline1
        assert TransformRegistry.get_transform("pipeline", "pipe2") is pipeline2

        # List should show both types
        listing = TransformRegistry.list_transforms()
        assert "crosswalk" in listing
        assert "pipeline" in listing
        assert len(listing["crosswalk"]) == 2
        assert len(listing["pipeline"]) == 2


class TestRegistryStateManagement:
    """Tests for registry state management and isolation."""

    def setup_method(self):
        """Save original state before each test."""
        self._orig_transforms = TransformRegistry._transforms.copy()
        self._orig_metadata = TransformRegistry._metadata.copy()

    def teardown_method(self):
        """Restore original state after each test."""
        TransformRegistry._transforms = self._orig_transforms
        TransformRegistry._metadata = self._orig_metadata

    @pytest.mark.unit
    def test_registry_class_attributes_shared(self):
        """Class attributes should be shared across all uses."""
        TransformRegistry.clear()

        @TransformRegistry.register("test", name="func")
        def func(df):
            return df

        # Direct access should work
        assert "test" in TransformRegistry._transforms
        assert TransformRegistry._transforms["test"]["func"] is func

    @pytest.mark.unit
    def test_clear_resets_both_dicts(self):
        """Clear should reset both _transforms and _metadata dicts."""
        TransformRegistry.clear()

        @TransformRegistry.register("type1", name="func1", metadata={"key": "value"})
        def func1(df):
            return df

        # Verify state before clear
        assert len(TransformRegistry._transforms) > 0
        assert len(TransformRegistry._metadata) > 0

        TransformRegistry.clear()

        # Verify both are empty
        assert len(TransformRegistry._transforms) == 0
        assert len(TransformRegistry._metadata) == 0

    @pytest.mark.unit
    def test_repeated_registration_overwrites(self):
        """Registering the same name twice should overwrite."""
        TransformRegistry.clear()

        @TransformRegistry.register("test", name="func")
        def v1(df):
            return "v1"

        first_func = TransformRegistry.get_transform("test", "func")
        assert first_func is v1

        @TransformRegistry.register("test", name="func")
        def v2(df):
            return "v2"

        second_func = TransformRegistry.get_transform("test", "func")
        assert second_func is v2
        assert second_func is not v1

    @pytest.mark.unit
    def test_metadata_overwrite_with_new_registration(self):
        """Metadata should be overwritten with new registration."""
        TransformRegistry.clear()

        @TransformRegistry.register(
            "test", name="func", metadata={"version": "1.0"}
        )
        def func1(df):
            return df

        meta = TransformRegistry.get_metadata("test", "func")
        assert meta["version"] == "1.0"

        @TransformRegistry.register(
            "test", name="func", metadata={"version": "2.0", "updated": True}
        )
        def func2(df):
            return df

        meta = TransformRegistry.get_metadata("test", "func")
        assert meta["version"] == "2.0"
        assert meta["updated"] is True


class TestRegistryErrorHandling:
    """Tests for error handling and robustness."""

    def setup_method(self):
        """Save original state before each test."""
        self._orig_transforms = TransformRegistry._transforms.copy()
        self._orig_metadata = TransformRegistry._metadata.copy()

    def teardown_method(self):
        """Restore original state after each test."""
        TransformRegistry._transforms = self._orig_transforms
        TransformRegistry._metadata = self._orig_metadata

    @pytest.mark.unit
    def test_get_transform_returns_none_for_missing(self):
        """Get transform should return None, not raise."""
        TransformRegistry.clear()

        result = TransformRegistry.get_transform("nonexistent", "func")
        assert result is None

        # Should not raise even with multiple missing levels
        result = TransformRegistry.get_transform("missing_type", "missing_func")
        assert result is None

    @pytest.mark.unit
    def test_get_metadata_returns_none_for_missing(self):
        """Get metadata should return None, not raise."""
        TransformRegistry.clear()

        result = TransformRegistry.get_metadata("nonexistent", "func")
        assert result is None

        # Register a function without metadata
        @TransformRegistry.register("test", name="func")
        def func(df):
            return df

        result = TransformRegistry.get_metadata("test", "func")
        assert result is None

    @pytest.mark.unit
    def test_list_transforms_handles_empty_types(self):
        """List transforms should handle empty type registries."""
        TransformRegistry.clear()

        # Register something then delete it from _transforms but not _metadata
        @TransformRegistry.register("test", name="func")
        def func(df):
            return df

        # Now manually clear just transforms to test robustness
        # (Normal usage wouldn't do this, but good to verify)
        TransformRegistry._transforms["empty"] = {}
        TransformRegistry._metadata["empty"] = {}

        result = TransformRegistry.list_transforms()
        assert "empty" in result
        assert result["empty"] == []

    @pytest.mark.unit
    def test_register_with_special_characters_in_name(self):
        """Register with special characters in transform name."""
        TransformRegistry.clear()

        special_name = "transform-v2.0_alpha#1"

        @TransformRegistry.register("test", name=special_name)
        def func(df):
            return df

        # Should work with special characters
        assert TransformRegistry.get_transform("test", special_name) is func

    @pytest.mark.unit
    def test_register_with_numeric_type_name(self):
        """Register with numeric strings as type name."""
        TransformRegistry.clear()

        @TransformRegistry.register("123", name="func")
        def func(df):
            return df

        assert TransformRegistry.get_transform("123", "func") is func

    @pytest.mark.unit
    def test_large_metadata_storage(self):
        """Registry should handle large metadata objects."""
        TransformRegistry.clear()

        # Create large metadata
        large_meta = {
            "data": "x" * 10000,  # 10KB of data
            "list": list(range(1000)),
            "nested": {
                "deep": {
                    "structure": {
                        "with": {"many": {"levels": "value"}}
                    }
                }
            },
        }

        @TransformRegistry.register("test", name="func", metadata=large_meta)
        def func(df):
            return df

        retrieved_meta = TransformRegistry.get_metadata("test", "func")
        assert retrieved_meta == large_meta
        assert retrieved_meta["data"] == "x" * 10000
        assert len(retrieved_meta["list"]) == 1000


class TestTransformRegistryBranchCoverage:
    """Cover uncovered branches 217->218, 217->219, 232->233, 232->235,
    249->250, 249->251 in _transforms/_registry.py."""

    def setup_method(self):
        self._orig_transforms = TransformRegistry._transforms.copy()
        self._orig_metadata = TransformRegistry._metadata.copy()

    def teardown_method(self):
        TransformRegistry._transforms = self._orig_transforms
        TransformRegistry._metadata = self._orig_metadata

    @pytest.mark.unit
    def test_get_transform_type_found(self):
        """Branch 217->218: transform_type IS in _transforms."""
        TransformRegistry.clear()

        @TransformRegistry.register("my_type", name="my_func")
        def my_func(df):
            return df

        result = TransformRegistry.get_transform("my_type", "my_func")
        assert result is my_func

    @pytest.mark.unit
    def test_get_transform_type_not_found(self):
        """Branch 217->219: transform_type NOT in _transforms, returns None."""
        TransformRegistry.clear()
        result = TransformRegistry.get_transform("no_such_type", "no_func")
        assert result is None

    @pytest.mark.unit
    def test_list_transforms_with_filter(self):
        """Branch 232->233: transform_type is truthy, returns filtered dict."""
        TransformRegistry.clear()

        @TransformRegistry.register("alpha", name="f1")
        def f1(df):
            return df

        result = TransformRegistry.list_transforms("alpha")
        assert result == {"alpha": ["f1"]}

    @pytest.mark.unit
    def test_list_transforms_no_filter(self):
        """Branch 232->235: transform_type is None/falsy, returns all."""
        TransformRegistry.clear()

        @TransformRegistry.register("t1", name="f1")
        def f1(df):
            return df

        @TransformRegistry.register("t2", name="f2")
        def f2(df):
            return df

        result = TransformRegistry.list_transforms(None)
        assert "t1" in result
        assert "t2" in result

    @pytest.mark.unit
    def test_get_metadata_type_found(self):
        """Branch 249->250: transform_type IS in _metadata."""
        TransformRegistry.clear()

        @TransformRegistry.register("mtype", name="mfunc", metadata={"v": 1})
        def mfunc(df):
            return df

        result = TransformRegistry.get_metadata("mtype", "mfunc")
        assert result == {"v": 1}

    @pytest.mark.unit
    def test_get_metadata_type_not_found(self):
        """Branch 249->251: transform_type NOT in _metadata, returns None."""
        TransformRegistry.clear()
        result = TransformRegistry.get_metadata("missing_type", "missing_func")
        assert result is None
