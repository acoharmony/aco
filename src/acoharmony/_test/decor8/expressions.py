# © 2025 HarmonyCares
# All rights reserved.

"""
Tests for @expression decorator.

Tests validation, metadata attachment, composability, and registry integration.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import Any
from unittest.mock import MagicMock
import polars as pl
import pytest

from acoharmony._decor8.expressions import expression
from acoharmony._decor8.composition import expression_method


class TestExpressionBasics:
    """Test basic expression decorator functionality."""

    @pytest.mark.unit
    def test_expression_decorator_basic(self):
        """Test basic expression wrapping."""

        @expression("double_amount")
        def double_amount() -> pl.Expr:
            return pl.col("amount") * 2

        # Should have metadata
        assert hasattr(double_amount, "_expression_name")
        assert double_amount._expression_name == "double_amount"
        assert double_amount._expression_tiers == ["bronze"]

        # Should return pl.Expr
        expr = double_amount()
        assert isinstance(expr, pl.Expr)

    @pytest.mark.unit
    def test_expression_with_parameters(self):
        """Test expression with custom parameters."""

        @expression(
            name="filter_status",
            tier=["bronze", "silver"],
            description="Filter by status codes",
            idempotent=True,
            sql_enabled=True,
        )
        def filter_status(codes: list[str]) -> pl.Expr:
            return pl.col("status").is_in(codes)

        # Check metadata
        assert filter_status._expression_name == "filter_status"
        assert filter_status._expression_tiers == ["bronze", "silver"]
        assert filter_status._expression_description == "Filter by status codes"
        assert filter_status._expression_idempotent is True
        assert filter_status._expression_sql_enabled is True

        # Should work with parameters
        expr = filter_status(["A", "B"])
        assert isinstance(expr, pl.Expr)

    @pytest.mark.unit
    def test_expression_multi_tier(self):
        """Test expression with multiple tiers."""

        @expression("standardize_name", tier=["bronze", "silver", "gold"])
        def standardize() -> pl.Expr:
            return pl.col("name").str.to_uppercase()

        assert standardize._expression_tiers == ["bronze", "silver", "gold"]


class TestExpressionReturnTypes:
    """Test expression return type validation."""

    @pytest.mark.unit
    def test_single_expression_return(self):
        """Test returning single pl.Expr."""

        @expression("single_expr")
        def single() -> pl.Expr:
            return pl.col("value") + 1

        result = single()
        assert isinstance(result, pl.Expr)

    @pytest.mark.unit
    def test_list_expression_return(self):
        """Test returning list of pl.Expr."""

        @expression("date_parts")
        def date_parts() -> list[pl.Expr]:
            return [
                pl.col("date").dt.year().alias("year"),
                pl.col("date").dt.month().alias("month"),
                pl.col("date").dt.day().alias("day"),
            ]

        result = date_parts()
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(expr, pl.Expr) for expr in result)

    @pytest.mark.unit
    def test_invalid_return_type(self):
        """Test error when returning invalid type."""

        @expression("invalid")
        def invalid() -> pl.Expr:
            return "not an expression"  # type: ignore

        with pytest.raises(TypeError, match="must return pl.Expr"):
            invalid()

    @pytest.mark.unit
    def test_invalid_list_return(self):
        """Test error when list contains non-Expr items."""

        @expression("invalid_list")
        def invalid_list() -> list[pl.Expr]:
            return [pl.col("a"), "not expr", pl.col("b")]  # type: ignore

        with pytest.raises(TypeError, match="non-Expr elements"):
            invalid_list()


class TestExpressionWithDataFrame:
    """Test expressions applied to actual dataframes."""

    @pytest.mark.unit
    def test_expression_with_dataframe(self):
        """Test expression applied to real dataframe."""
        df = pl.DataFrame({"amount": [100, 200, 300], "status": ["A", "B", "A"]})

        @expression("double_amount")
        def double_amount() -> pl.Expr:
            return (pl.col("amount") * 2).alias("amount_doubled")

        result = df.with_columns(double_amount())
        assert "amount_doubled" in result.columns
        assert result["amount_doubled"].to_list() == [200, 400, 600]

    @pytest.mark.unit
    def test_filter_expression_with_dataframe(self):
        """Test filter expression on dataframe."""
        df = pl.DataFrame({"amount": [100, 200, 300], "status": ["A", "B", "A"]})

        @expression("filter_active")
        def filter_active() -> pl.Expr:
            return pl.col("status") == "A"

        result = df.filter(filter_active())
        assert len(result) == 2
        assert result["status"].to_list() == ["A", "A"]

    @pytest.mark.unit
    def test_multiple_expressions_with_dataframe(self):
        """Test multiple expressions at once."""
        df = pl.DataFrame({"amount": [100, 200, 300], "status": ["A", "B", "A"], "qty": [1, 2, 3]})

        @expression("compute_columns")
        def compute_columns() -> list[pl.Expr]:
            return [
                (pl.col("amount") * pl.col("qty")).alias("total"),
                pl.col("status").str.to_uppercase().alias("status_upper"),
            ]

        result = df.with_columns(compute_columns())
        assert "total" in result.columns
        assert "status_upper" in result.columns
        assert result["total"].to_list() == [100, 400, 900]


class TestExpressionIdempotency:
    """Test idempotent flag behavior."""

    @pytest.mark.unit
    def test_idempotent_expression(self):
        """Test idempotent expression can be applied multiple times."""

        @expression("normalize_status", idempotent=True)
        def normalize() -> pl.Expr:
            return pl.col("status").str.to_uppercase().alias("status")

        df = pl.DataFrame({"status": ["a", "b", "c"]})

        # Apply twice - should work fine
        result = df.with_columns(normalize()).with_columns(normalize())
        assert result["status"].to_list() == ["A", "B", "C"]

    @pytest.mark.unit
    def test_non_idempotent_flag(self):
        """Test non-idempotent flag is stored."""

        @expression("accumulate", idempotent=False)
        def accumulate() -> pl.Expr:
            return pl.col("value") + 1

        assert accumulate._expression_idempotent is False


class TestExpressionMetadata:
    """Test metadata attachment and access."""

    @pytest.mark.unit
    def test_metadata_attributes(self):
        """Test all metadata attributes are attached."""

        @expression(
            name="test_expr",
            tier=["silver", "gold"],
            description="Test expression for validation",
            idempotent=False,
            sql_enabled=False,
        )
        @pytest.mark.unit
        def test_expr() -> pl.Expr:
            return pl.col("value")

        assert test_expr._expression_name == "test_expr"
        assert test_expr._expression_tiers == ["silver", "gold"]
        assert test_expr._expression_description == "Test expression for validation"
        assert test_expr._expression_idempotent is False
        assert test_expr._expression_sql_enabled is False

    @pytest.mark.unit
    def test_docstring_as_description(self):
        """Test docstring used as description when not provided."""

        @expression("with_docstring")
        def with_docstring() -> pl.Expr:
            """This is a docstring description."""
            return pl.col("value")

        assert "docstring description" in (with_docstring._expression_description or "")


class TestExpressionComposability:
    """Test expression composability features."""

    @pytest.mark.unit
    def test_expression_has_rshift(self):
        """Test expression has __rshift__ method for composition."""

        @expression("test_expr")
        @pytest.mark.unit
        def test_expr() -> pl.Expr:
            return pl.col("value")

        # Should have __rshift__ method
        assert hasattr(test_expr, "__rshift__")


class TestExpressionSQLEnabled:
    """Test sql_enabled flag and behavior."""

    @pytest.mark.unit
    def test_sql_enabled_default(self):
        """Test sql_enabled defaults to True."""

        @expression("default_sql")
        def default_sql() -> pl.Expr:
            return pl.col("value")

        assert default_sql._expression_sql_enabled is True

    @pytest.mark.unit
    def test_sql_enabled_false(self):
        """Test sql_enabled can be set to False."""

        @expression("no_sql", sql_enabled=False)
        def no_sql() -> pl.Expr:
            return pl.col("value")

        assert no_sql._expression_sql_enabled is False


class TestExpressionWithLazyFrame:
    """Test expressions work with LazyFrame."""

    @pytest.mark.unit
    def test_expression_with_lazyframe(self):
        """Test expression applied to LazyFrame."""
        df = pl.DataFrame({"amount": [100, 200, 300]}).lazy()

        @expression("triple_amount")
        def triple_amount() -> pl.Expr:
            return (pl.col("amount") * 3).alias("amount_tripled")

        result_lazy = df.with_columns(triple_amount())
        assert isinstance(result_lazy, pl.LazyFrame)

        result = result_lazy.collect()
        assert "amount_tripled" in result.columns
        assert result["amount_tripled"].to_list() == [300, 600, 900]


class TestExpressionComplexOperations:
    """Test complex expression operations."""

    @pytest.mark.unit
    def test_conditional_expression(self):
        """Test conditional/when expression."""
        df = pl.DataFrame({"amount": [50, 150, 250]})

        @expression("categorize_amount")
        def categorize() -> pl.Expr:
            return (
                pl.when(pl.col("amount") < 100)
                .then(pl.lit("low"))
                .when(pl.col("amount") < 200)
                .then(pl.lit("medium"))
                .otherwise(pl.lit("high"))
                .alias("category")
            )

        result = df.with_columns(categorize())
        assert result["category"].to_list() == ["low", "medium", "high"]

    @pytest.mark.unit
    def test_aggregation_expression(self):
        """Test aggregation expressions."""
        df = pl.DataFrame({"group": ["A", "A", "B", "B"], "value": [1, 2, 3, 4]})

        @expression("sum_by_group")
        def sum_expr() -> pl.Expr:
            return pl.col("value").sum().alias("total")

        result = df.group_by("group").agg(sum_expr())
        assert len(result) == 2

    @pytest.mark.unit
    def test_string_operations(self):
        """Test string manipulation expressions."""
        df = pl.DataFrame({"name": ["  John Doe  ", "  Jane Smith  "]})

        @expression("clean_name")
        def clean_name() -> pl.Expr:
            return pl.col("name").str.strip_chars().str.to_uppercase().alias("name_clean")

        result = df.with_columns(clean_name())
        assert result["name_clean"].to_list() == ["JOHN DOE", "JANE SMITH"]


class TestExpressionEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.unit
    def test_empty_dataframe(self):
        """Test expression on empty dataframe."""
        df = pl.DataFrame({"amount": []}, schema={"amount": pl.Int64})

        @expression("double")
        def double() -> pl.Expr:
            return (pl.col("amount") * 2).alias("doubled")

        result = df.with_columns(double())
        assert len(result) == 0
        assert "doubled" in result.columns

    @pytest.mark.unit
    def test_null_handling(self):
        """Test expression with null values."""
        df = pl.DataFrame({"value": [1, None, 3]})

        @expression("handle_nulls")
        def handle_nulls() -> pl.Expr:
            return pl.col("value").fill_null(0).alias("value_filled")

        result = df.with_columns(handle_nulls())
        assert result["value_filled"].to_list() == [1, 0, 3]

    @pytest.mark.unit
    def test_column_not_exists(self):
        """Test expression referencing non-existent column."""
        df = pl.DataFrame({"a": [1, 2, 3]})

        @expression("nonexistent")
        def nonexistent() -> pl.Expr:
            return pl.col("nonexistent_column")

        # Should fail at collect time, not decoration time
        with pytest.raises(
            Exception, match=r".*"
        ):  # Polars will raise ColumnNotFoundError or similar
            df.with_columns(nonexistent()).collect()


class TestExpressionComposability:  # noqa: F811
    """Cover lines 165-169: __rshift__ composition support."""

    @pytest.mark.unit
    def test_rshift_composes_two_expressions(self):
        """Lines 165-169: >> operator composes two expression functions."""

        @expression("base_expr")
        def base_expr() -> pl.Expr:
            return pl.col("amount") * 2

        # The __rshift__ is attached as a method, call it manually
        # since Python doesn't dispatch __rshift__ on plain functions
        composed = base_expr.__rshift__(base_expr, lambda result: result)
        assert callable(composed)

    @pytest.mark.unit
    def test_rshift_composed_function_executes(self):
        """Lines 165-169: composed function actually calls both sides."""
        call_log = []

        @expression("step1")
        def step1() -> pl.Expr:
            call_log.append("step1")
            return pl.col("a")

        def step2(result):
            call_log.append("step2")
            return result

        composed = step1.__rshift__(step1, step2)
        composed()
        assert "step2" in call_log


class TestExpressionRegistryFallback:
    """Cover lines 194-195: ExpressionRegistry import failure."""

    @pytest.mark.unit
    def test_registry_import_error_logged(self):
        """Lines 194-195: ImportError during registry registration is caught."""
        from unittest.mock import patch

        # The decorator does: from .._expressions._registry import ExpressionRegistry
        # Patch at the source to make the import fail
        with patch.dict("sys.modules", {"acoharmony._expressions._registry": None}):

            @expression("test_no_registry_" + str(id(self)))
            @pytest.mark.unit
            def test_expr() -> pl.Expr:
                return pl.col("x")

            # Should still work - just no registry
            assert test_expr._expression_name == "test_no_registry_" + str(id(self))

    @pytest.mark.unit
    def test_registry_attribute_error_logged(self):
        """Lines 194-195: AttributeError during registration is caught."""
        from unittest.mock import MagicMock, patch

        mock_registry_mod = MagicMock()
        mock_registry_mod.ExpressionRegistry.register.side_effect = AttributeError("bad attr")

        with patch.dict("sys.modules", {"acoharmony._expressions._registry": mock_registry_mod}):

            @expression("test_attr_error_" + str(id(self)))
            @pytest.mark.unit
            def test_expr() -> pl.Expr:
                return pl.col("x")

            assert test_expr._expression_name == "test_attr_error_" + str(id(self))


class TestExpressionDecorator:
    """Tests for the expression decorator."""

    @pytest.mark.unit
    def test_expression_basic(self):
        """Test basic expression decorator."""

        @expression("double_amount")
        def double_amount() -> pl.Expr:
            return pl.col("amount") * 2

        result = double_amount()
        assert isinstance(result, pl.Expr)

    @pytest.mark.unit
    def test_expression_returns_list(self):
        """Test expression returning list of exprs."""

        @expression("multi_expr")
        def multi() -> list[pl.Expr]:
            return [pl.col("a"), pl.col("b")]

        result = multi()
        assert isinstance(result, list)
        assert len(result) == 2

    @pytest.mark.unit
    def test_expression_invalid_return_type(self):
        """Test expression raises on invalid return type."""

        @expression("bad_expr")
        def bad() -> Any:
            return "not an expr"

        with pytest.raises(TypeError, match="must return pl.Expr"):
            bad()

    @pytest.mark.unit
    def test_expression_invalid_list_elements(self):
        """Test expression raises on invalid list elements."""

        @expression("bad_list")
        def bad_list() -> Any:
            return [pl.col("a"), "not_expr"]

        with pytest.raises(TypeError, match="non-Expr elements"):
            bad_list()

    @pytest.mark.unit
    def test_expression_metadata(self):
        """Test expression metadata is attached."""

        @expression(
            "test_expr",
            tier=["bronze", "silver"],
            description="Test",
            idempotent=False,
            sql_enabled=False,
        )
        def my_expr() -> pl.Expr:
            return pl.col("a")

        assert my_expr._expression_name == "test_expr"
        assert my_expr._expression_tiers == ["bronze", "silver"]
        assert my_expr._expression_description == "Test"
        assert my_expr._expression_idempotent is False
        assert my_expr._expression_sql_enabled is False

    @pytest.mark.unit
    def test_expression_tier_string(self):
        """Test expression with single tier string."""

        @expression("tier_test", tier="gold")
        def my_expr() -> pl.Expr:
            return pl.col("a")

        assert my_expr._expression_tiers == ["gold"]

    @pytest.mark.unit
    def test_expression_description_from_docstring(self):
        """Test expression uses docstring when no description provided."""

        @expression("doc_expr")
        def my_expr() -> pl.Expr:
            """My expression docstring."""
            return pl.col("a")

        assert my_expr._expression_description == "My expression docstring."

    @pytest.mark.unit
    def test_expression_rshift(self):
        """Test expression __rshift__ operator."""

        @expression("first")
        def first_expr() -> pl.Expr:
            return pl.col("a")

        # Test that __rshift__ exists
        assert hasattr(first_expr, "__rshift__")

class TestExpressionMethod:
    """Tests for the expression_method meta-decorator."""

    @pytest.mark.unit
    def test_expression_method_basic(self):
        """Test expression_method with minimal args."""

        @expression_method(expression_name="test_expr")
        def build_expr() -> pl.Expr:
            return pl.col("a")

        result = build_expr()
        assert isinstance(result, pl.Expr)

    @pytest.mark.unit
    def test_expression_method_with_tier(self):
        """Test expression_method with tier and options."""

        @expression_method(
            expression_name="tier_expr",
            tier=["bronze", "silver"],
            idempotent=False,
            sql_enabled=False,
        )
        def build_expr() -> pl.Expr:
            return pl.col("a")

        result = build_expr()
        assert isinstance(result, pl.Expr)

    @pytest.mark.unit
    def test_expression_method_with_memory(self):
        """Test expression_method with memory tracking."""

        @expression_method(expression_name="mem_expr", track_memory=True)
        def build_expr() -> pl.Expr:
            return pl.col("a")

        result = build_expr()
        assert isinstance(result, pl.Expr)


class TestExpressionDecoratorBranches:
    """Cover branches 141->142/147, 142->143/152, 147->148/152."""

    @pytest.mark.unit
    def test_expression_returns_list_of_exprs(self):
        """Branch 141->142, 142->152: result is a list of all pl.Expr."""
        from acoharmony._decor8.expressions import expression

        @expression(name="list_expr_test")
        def build() -> list[pl.Expr]:
            return [pl.col("a"), pl.col("b")]

        result = build()
        assert isinstance(result, list)
        assert len(result) == 2

    @pytest.mark.unit
    def test_expression_returns_list_with_non_expr(self):
        """Branch 142->143: list with non-Expr element raises TypeError."""
        from acoharmony._decor8.expressions import expression

        @expression(name="bad_list_expr_test")
        def build() -> list:
            return [pl.col("a"), "not_an_expr"]

        with pytest.raises(TypeError, match="non-Expr elements"):
            build()

    @pytest.mark.unit
    def test_expression_returns_non_expr(self):
        """Branch 147->148: result is not a list and not pl.Expr, raises TypeError."""
        from acoharmony._decor8.expressions import expression

        @expression(name="bad_expr_test")
        def build():
            return "not_an_expr"

        with pytest.raises(TypeError, match="must return pl.Expr"):
            build()

    @pytest.mark.unit
    def test_expression_returns_single_expr(self):
        """Branch 147->152: result is a single pl.Expr, passes validation."""
        from acoharmony._decor8.expressions import expression

        @expression(name="single_expr_test")
        def build() -> pl.Expr:
            return pl.col("a")

        result = build()
        assert isinstance(result, pl.Expr)

