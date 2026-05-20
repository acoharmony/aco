# © 2025 HarmonyCares
# All rights reserved.

"""
Branch coverage tests for ExpressionRegistry (_expressions/_registry.py).

Covers uncovered branches:
  142->143, 142->146, 147->148, 147->151, 152->153, 152->155,
  169->170, 169->171, 187->188, 187->190, 188->187, 188->189,
  211->212, 211->215, 215->216, 215->222
"""

from __future__ import annotations

import pytest

from acoharmony._expressions._registry import ExpressionRegistry, register_expression


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear the registry before and after each test to avoid cross-talk."""
    saved_builders = dict(ExpressionRegistry._builders)
    saved_metadata = dict(ExpressionRegistry._metadata)
    ExpressionRegistry.clear()
    yield
    ExpressionRegistry._builders.clear()
    ExpressionRegistry._metadata.clear()
    ExpressionRegistry._builders.update(saved_builders)
    ExpressionRegistry._metadata.update(saved_metadata)


# ---------------------------------------------------------------------------
# is_applicable branches
# ---------------------------------------------------------------------------


class TestIsApplicableBranches:
    """Cover is_applicable branches (142-155)."""

    @pytest.mark.unit
    def test_no_metadata_returns_false(self):
        """Branch 142->143: expression not registered -> False."""
        result = ExpressionRegistry.is_applicable("nonexistent", "bronze")
        assert result is False

    @pytest.mark.unit
    def test_schema_not_applicable(self):
        """Branch 147->148: schema not in applicable schemas -> False."""

        @ExpressionRegistry.register("test_schema_check", schemas=["silver", "gold"])
        class _Builder:
            pass

        assert ExpressionRegistry.is_applicable("test_schema_check", "bronze") is False

    @pytest.mark.unit
    def test_schema_applicable(self):
        """Branch 147->151: schema is in applicable schemas -> proceed."""

        @ExpressionRegistry.register("test_schema_ok", schemas=["bronze", "silver"])
        class _Builder:
            pass

        assert ExpressionRegistry.is_applicable("test_schema_ok", "bronze") is True

    @pytest.mark.unit
    def test_dataset_type_not_applicable(self):
        """Branch 152->153: dataset_type not in applicable types -> False."""

        @ExpressionRegistry.register(
            "test_dtype_check",
            schemas=["bronze"],
            dataset_types=["claims", "eligibility"],
        )
        class _Builder:
            pass

        assert (
            ExpressionRegistry.is_applicable("test_dtype_check", "bronze", "provider")
            is False
        )

    @pytest.mark.unit
    def test_dataset_type_applicable(self):
        """Branch 152->155: dataset_type in applicable types -> True."""

        @ExpressionRegistry.register(
            "test_dtype_ok",
            schemas=["bronze"],
            dataset_types=["claims"],
        )
        class _Builder:
            pass

        assert (
            ExpressionRegistry.is_applicable("test_dtype_ok", "bronze", "claims")
            is True
        )

    @pytest.mark.unit
    def test_no_dataset_types_means_all(self):
        """Branch 152->155: empty dataset_types list means all types match."""

        @ExpressionRegistry.register(
            "test_dtype_empty", schemas=["bronze"], dataset_types=[]
        )
        class _Builder:
            pass

        assert (
            ExpressionRegistry.is_applicable("test_dtype_empty", "bronze", "anything")
            is True
        )

    @pytest.mark.unit
    def test_no_dataset_type_arg(self):
        """dataset_type=None skips dataset check -> True."""

        @ExpressionRegistry.register(
            "test_no_dtype_arg",
            schemas=["bronze"],
            dataset_types=["claims"],
        )
        class _Builder:
            pass

        # dataset_type is None by default, so dataset check is skipped
        assert ExpressionRegistry.is_applicable("test_no_dtype_arg", "bronze") is True

    @pytest.mark.unit
    def test_metadata_found_all_applicable(self):
        """Branch 142->146: metadata exists, everything matches -> True."""

        @ExpressionRegistry.register(
            "test_all_ok",
            schemas=["bronze", "silver", "gold"],
            dataset_types=["claims"],
        )
        class _Builder:
            pass

        assert (
            ExpressionRegistry.is_applicable("test_all_ok", "silver", "claims") is True
        )


# ---------------------------------------------------------------------------
# is_callable branches
# ---------------------------------------------------------------------------


class TestIsCallableBranches:
    """Cover is_callable branches (169-171)."""

    @pytest.mark.unit
    def test_no_metadata_returns_false(self):
        """Branch 169->170: expression not registered -> False."""
        assert ExpressionRegistry.is_callable("nonexistent") is False

    @pytest.mark.unit
    def test_callable_true(self):
        """Branch 169->171: registered with callable=True."""

        @ExpressionRegistry.register("test_callable_yes", callable=True)
        class _Builder:
            pass

        assert ExpressionRegistry.is_callable("test_callable_yes") is True

    @pytest.mark.unit
    def test_callable_false(self):
        """Branch 169->171: registered with callable=False."""

        @ExpressionRegistry.register("test_callable_no", callable=False)
        class _Builder:
            pass

        assert ExpressionRegistry.is_callable("test_callable_no") is False


# ---------------------------------------------------------------------------
# list_for_schema branches
# ---------------------------------------------------------------------------


class TestListForSchemaBranches:
    """Cover list_for_schema branches (187-190)."""

    @pytest.mark.unit
    def test_empty_registry(self):
        """Branch 187->190: no builders registered -> empty list."""
        result = ExpressionRegistry.list_for_schema("bronze")
        assert result == []

    @pytest.mark.unit
    def test_multiple_builders_some_match(self):
        """Branches 187->188, 188->187, 188->189: iterate, some match, some don't."""

        @ExpressionRegistry.register("expr_bronze", schemas=["bronze"])
        class _A:
            pass

        @ExpressionRegistry.register("expr_silver", schemas=["silver"])
        class _B:
            pass

        @ExpressionRegistry.register("expr_both", schemas=["bronze", "silver"])
        class _C:
            pass

        result = ExpressionRegistry.list_for_schema("bronze")
        assert "expr_bronze" in result
        assert "expr_both" in result
        assert "expr_silver" not in result

    @pytest.mark.unit
    def test_list_for_schema_with_dataset_type(self):
        """list_for_schema filters by dataset_type too."""

        @ExpressionRegistry.register(
            "expr_claims", schemas=["bronze"], dataset_types=["claims"]
        )
        class _A:
            pass

        @ExpressionRegistry.register(
            "expr_elig", schemas=["bronze"], dataset_types=["eligibility"]
        )
        class _B:
            pass

        result = ExpressionRegistry.list_for_schema("bronze", dataset_type="claims")
        assert "expr_claims" in result
        assert "expr_elig" not in result

    @pytest.mark.unit
    def test_all_builders_match(self):
        """Branch 188->189: all builders match the schema."""

        @ExpressionRegistry.register("expr_a", schemas=["gold"])
        class _A:
            pass

        @ExpressionRegistry.register("expr_b", schemas=["gold"])
        class _B:
            pass

        result = ExpressionRegistry.list_for_schema("gold")
        assert len(result) == 2
        assert "expr_a" in result
        assert "expr_b" in result

    @pytest.mark.unit
    def test_no_builders_match(self):
        """All builders registered but none match the schema."""

        @ExpressionRegistry.register("expr_x", schemas=["silver"])
        class _A:
            pass

        result = ExpressionRegistry.list_for_schema("bronze")
        assert result == []


# ---------------------------------------------------------------------------
# build_expression branches
# ---------------------------------------------------------------------------


class TestBuildExpressionBranches:
    """Cover build_expression branches (211-222)."""

    @pytest.mark.unit
    def test_no_builder_raises(self):
        """Branch 211->212: no builder registered for expression type."""
        with pytest.raises(ValueError, match="No builder registered"):
            ExpressionRegistry.build_expression("nonexistent", {})

    @pytest.mark.unit
    def test_schema_not_applicable_raises(self):
        """Branch 215->216: schema provided but expression not applicable."""

        @ExpressionRegistry.register("test_build_schema", schemas=["silver"])
        class _Builder:
            @staticmethod
            def build(config):
                return "built"

        with pytest.raises(ValueError, match="not applicable for schema"):
            ExpressionRegistry.build_expression(
                "test_build_schema", {}, schema="bronze"
            )

    @pytest.mark.unit
    def test_build_success_with_schema(self):
        """Branch 215->222: schema provided and expression is applicable."""

        @ExpressionRegistry.register("test_build_ok", schemas=["bronze", "silver"])
        class _Builder:
            @staticmethod
            def build(config):
                return {"result": "success", **config}

        result = ExpressionRegistry.build_expression(
            "test_build_ok", {"key": "val"}, schema="bronze"
        )
        assert result["result"] == "success"
        assert result["key"] == "val"

    @pytest.mark.unit
    def test_build_success_no_schema(self):
        """Branch 211->215 skipped (schema is None): build without schema check."""

        @ExpressionRegistry.register("test_build_no_schema", schemas=["gold"])
        class _Builder:
            @staticmethod
            def build(config):
                return "no_schema_check"

        result = ExpressionRegistry.build_expression("test_build_no_schema", {})
        assert result == "no_schema_check"


# ---------------------------------------------------------------------------
# register_expression convenience function
# ---------------------------------------------------------------------------


class TestRegisterExpressionFunction:
    """Ensure the convenience function register_expression works."""

    @pytest.mark.unit
    def test_register_expression_decorator(self):
        """register_expression wraps ExpressionRegistry.register."""

        @register_expression(
            "conv_test",
            schemas=["bronze"],
            callable=True,
            dataset_types=["claims"],
            description="A test expression",
        )
        class _Builder:
            @staticmethod
            def build(config):
                return config

        assert ExpressionRegistry.get_builder("conv_test") is _Builder
        meta = ExpressionRegistry.get_metadata("conv_test")
        assert meta["description"] == "A test expression"
        assert meta["callable"] is True
        assert meta["schemas"] == ["bronze"]
        assert meta["dataset_types"] == ["claims"]

    @pytest.mark.unit
    def test_register_expression_with_extra_metadata(self):
        """register_expression passes **metadata kwargs through."""

        @register_expression(
            "conv_meta",
            schemas=["silver"],
            description="meta test",
            version="1.0",
            author="test",
        )
        class _Builder:
            @staticmethod
            def build(config):
                return config

        meta = ExpressionRegistry.get_metadata("conv_meta")
        assert meta["version"] == "1.0"
        assert meta["author"] == "test"
