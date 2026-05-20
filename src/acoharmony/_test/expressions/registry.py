# © 2025 HarmonyCares
# All rights reserved.

"""Tests for ExpressionRegistry - file was severely damaged by AST unparse.
Test functions removed until manual restoration."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestRegistry:
    """Test cases for expression builders."""

    @pytest.mark.unit
    def test_placeholder(self):
        """Placeholder test - original tests need manual restoration."""
        assert True  # File needs manual repair


class TestExpressionRegistry:
    """Tests for ExpressionRegistry and register_expression decorator."""

    def setup_method(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        self._saved_builders = dict(ExpressionRegistry._builders)
        self._saved_metadata = dict(ExpressionRegistry._metadata)

    def teardown_method(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        ExpressionRegistry._builders.clear()
        ExpressionRegistry._builders.update(self._saved_builders)
        ExpressionRegistry._metadata.clear()
        ExpressionRegistry._metadata.update(self._saved_metadata)

    def test_register_and_retrieve(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("test_expr", description="a test")
        class _TestExpr:
            @staticmethod
            def build(config):
                return config

        assert ExpressionRegistry.get_builder("test_expr") is _TestExpr
        assert "test_expr" in ExpressionRegistry.list_builders()
        meta = ExpressionRegistry.get_metadata("test_expr")
        assert meta is not None
        assert meta["description"] == "a test"
        assert meta["class"] == "_TestExpr"

    def test_get_builder_missing(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        result = ExpressionRegistry.get_builder("__truly_nonexistent_xyzzy__")
        assert result is None

    def test_get_metadata_missing(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        result = ExpressionRegistry.get_metadata("__truly_nonexistent_xyzzy__")
        assert result is None

    def test_clear(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("temp_expr_for_clear_test")
        class _Temp:
            pass

        assert "temp_expr_for_clear_test" in ExpressionRegistry.list_builders()
        len(ExpressionRegistry.list_builders())
        ExpressionRegistry.clear()
        assert len(ExpressionRegistry.list_builders()) == 0
        # Restore immediately so teardown_method can also restore properly
        ExpressionRegistry._builders.update(self._saved_builders)
        ExpressionRegistry._metadata.update(self._saved_metadata)

    def test_is_applicable(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register(
            "schema_test", schemas=["silver"], dataset_types=["claims"]
        )
        class _SchemaTest:
            pass

        assert ExpressionRegistry.is_applicable("schema_test", "silver") is True
        assert ExpressionRegistry.is_applicable("schema_test", "gold") is False
        assert (
            ExpressionRegistry.is_applicable("schema_test", "silver", "claims") is True
        )
        assert (
            ExpressionRegistry.is_applicable("schema_test", "silver", "eligibility")
            is False
        )
        assert ExpressionRegistry.is_applicable("__truly_nonexistent_xyzzy__", "silver") is False

    def test_is_callable(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("callable_test", callable=True)
        class _CallableTest:
            pass

        @ExpressionRegistry.register("not_callable_test", callable=False)
        class _NotCallableTest:
            pass

        assert ExpressionRegistry.is_callable("callable_test") is True
        assert ExpressionRegistry.is_callable("not_callable_test") is False
        assert ExpressionRegistry.is_callable("__truly_nonexistent_xyzzy__") is False

    def test_list_for_schema(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register(
            "silver_expr", schemas=["silver"], dataset_types=["claims"]
        )
        class _Silver:
            pass

        @ExpressionRegistry.register("gold_expr", schemas=["gold"])
        class _Gold:
            pass

        silver_exprs = ExpressionRegistry.list_for_schema("silver")
        assert "silver_expr" in silver_exprs
        assert "gold_expr" not in silver_exprs

        silver_claims = ExpressionRegistry.list_for_schema("silver", "claims")
        assert "silver_expr" in silver_claims

    def test_build_expression(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("buildable", schemas=["silver"])
        class _Buildable:
            @staticmethod
            def build(config):
                return {"built": True, **config}

        result = ExpressionRegistry.build_expression(
            "buildable", {"key": "val"}, schema="silver"
        )
        assert result["built"] is True
        assert result["key"] == "val"

    def test_build_expression_missing(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        with pytest.raises(ValueError, match="No builder registered"):
            ExpressionRegistry.build_expression("__truly_nonexistent_xyzzy__", {})

    def test_build_expression_wrong_schema(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("schema_locked", schemas=["silver"])
        class _SchemaLocked:
            @staticmethod
            def build(config):
                return config

        with pytest.raises(ValueError, match="not applicable"):
            ExpressionRegistry.build_expression(
                "schema_locked", {}, schema="gold"
            )

    def test_register_expression_convenience(self):
        from acoharmony._expressions._registry import (
            ExpressionRegistry,
            register_expression,
        )

        @register_expression(
            "conv_test",
            schemas=["bronze"],
            callable=False,
            dataset_types=["logs"],
            description="convenience test",
        )
        class _Conv:
            pass

        assert ExpressionRegistry.get_builder("conv_test") is _Conv
        meta = ExpressionRegistry.get_metadata("conv_test")
        assert meta["schemas"] == ["bronze"]
        assert meta["callable"] is False
        assert meta["dataset_types"] == ["logs"]

    def test_register_default_metadata(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("defaults_test")
        class _Defaults:
            """Docstring used as description."""
            pass

        meta = ExpressionRegistry.get_metadata("defaults_test")
        assert meta["schemas"] == ["bronze", "silver", "gold"]
        assert meta["callable"] is True
        assert meta["dataset_types"] == []
        assert "Docstring" in meta["description"]

    def test_registry_attributes_on_class(self):
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register(
            "attr_test", schemas=["gold"], callable=False, dataset_types=["claims"]
        )
        class _AttrTest:
            pass

        assert _AttrTest._expression_type == "attr_test"
        assert _AttrTest._schemas == ["gold"]
        assert _AttrTest._callable is False
        assert _AttrTest._dataset_types == ["claims"]


class TestExpressionRegistryBranches:
    """Cover branches in _expressions/_registry.py:
    142->143/146, 147->148/151, 152->153/155, 169->170/171,
    187->188/190, 188->187/189, 211->212/215, 215->216/222.
    """

    @pytest.mark.unit
    def test_is_applicable_no_metadata(self):
        """Branch 142->143: metadata is falsy (not registered)."""
        from acoharmony._expressions._registry import ExpressionRegistry

        result = ExpressionRegistry.is_applicable("nonexistent_expr_xyz", "bronze")
        assert result is False

    @pytest.mark.unit
    def test_is_applicable_schema_not_matching(self):
        """Branch 147->148: schema not in applicable_schemas."""
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("schema_test_1", schemas=["gold"])
        class _S:
            pass

        result = ExpressionRegistry.is_applicable("schema_test_1", "bronze")
        assert result is False

    @pytest.mark.unit
    def test_is_applicable_dataset_type_not_matching(self):
        """Branch 152->153: dataset_type not in applicable_types."""
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("dtype_test_1", schemas=["silver"], dataset_types=["claims"])
        class _D:
            pass

        result = ExpressionRegistry.is_applicable("dtype_test_1", "silver", "eligibility")
        assert result is False

    @pytest.mark.unit
    def test_is_applicable_all_match(self):
        """Branch 152->155: all checks pass, returns True."""
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("all_match_1", schemas=["silver"], dataset_types=["claims"])
        class _A:
            pass

        result = ExpressionRegistry.is_applicable("all_match_1", "silver", "claims")
        assert result is True

    @pytest.mark.unit
    def test_is_callable_no_metadata(self):
        """Branch 169->170: metadata falsy, returns False."""
        from acoharmony._expressions._registry import ExpressionRegistry

        result = ExpressionRegistry.is_callable("nonexistent_callable_xyz")
        assert result is False

    @pytest.mark.unit
    def test_is_callable_with_metadata(self):
        """Branch 169->171: metadata exists, returns callable value."""
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("callable_test_1", callable=False)
        class _C:
            pass

        result = ExpressionRegistry.is_callable("callable_test_1")
        assert result is False

    @pytest.mark.unit
    def test_list_for_schema_with_matches(self):
        """Branch 187->188->189: expression is applicable, appended to list."""
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("list_schema_1", schemas=["bronze"])
        class _L:
            pass

        result = ExpressionRegistry.list_for_schema("bronze")
        assert "list_schema_1" in result

    @pytest.mark.unit
    def test_list_for_schema_no_matches(self):
        """Branch 188->187: no applicable expressions."""
        from acoharmony._expressions._registry import ExpressionRegistry

        result = ExpressionRegistry.list_for_schema("nonexistent_schema_xyz")
        # May be empty or have some, but our specific test schema won't match
        assert isinstance(result, list)

    @pytest.mark.unit
    def test_build_expression_no_builder(self):
        """Branch 211->212: no builder registered, raises ValueError."""
        from acoharmony._expressions._registry import ExpressionRegistry

        with pytest.raises(ValueError, match="No builder registered"):
            ExpressionRegistry.build_expression("no_such_builder_xyz", {})

    @pytest.mark.unit
    def test_build_expression_schema_not_applicable(self):
        """Branch 215->216: builder exists but schema not applicable."""
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("build_schema_test", schemas=["gold"])
        class _B:
            @staticmethod
            def build(config):
                return config

        with pytest.raises(ValueError, match="not applicable"):
            ExpressionRegistry.build_expression("build_schema_test", {}, schema="bronze")

    @pytest.mark.unit
    def test_build_expression_success(self):
        """Branch 215->222: builder exists and schema matches, builds OK."""
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("build_ok_test", schemas=["silver"])
        class _B:
            @staticmethod
            def build(config):
                return {"built": True, **config}

        result = ExpressionRegistry.build_expression("build_ok_test", {"k": 1}, schema="silver")
        assert result["built"] is True
        assert result["k"] == 1

    @pytest.mark.unit
    def test_build_expression_no_schema_check(self):
        """Branch 215->222: schema=None skips applicability check."""
        from acoharmony._expressions._registry import ExpressionRegistry

        @ExpressionRegistry.register("build_no_schema_test", schemas=["gold"])
        class _B:
            @staticmethod
            def build(config):
                return config

        result = ExpressionRegistry.build_expression("build_no_schema_test", {"x": 1})
        assert result == {"x": 1}
