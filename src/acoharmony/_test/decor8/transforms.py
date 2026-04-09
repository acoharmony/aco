# © 2025 HarmonyCares
# All rights reserved.

"""
Tests for @transform decorator.

Tests validation, auto-composition, metadata, and LazyFrame operations.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
import polars as pl
import pytest

from acoharmony._decor8.expressions import expression
from acoharmony._decor8.transforms import transform
from acoharmony._decor8.composition import transform_method


class TestTransformBasics:
    """Test basic transform decorator functionality."""

    @pytest.mark.unit
    def test_transform_decorator_basic(self):
        """Test basic transform wrapping."""

        @transform("standardize_claims")
        def standardize(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(pl.col("status").str.to_uppercase().alias("status_std"))

        # Should have metadata
        assert hasattr(standardize, "_transform_name")
        assert standardize._transform_name == "standardize_claims"
        assert standardize._transform_tiers == ["bronze"]

    @pytest.mark.unit
    def test_transform_with_parameters(self):
        """Test transform with custom parameters."""

        @transform(
            name="dedupe_claims",
            tier=["bronze", "silver"],
            description="Remove duplicate claims",
            sql_enabled=True,
        )
        def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.unique(subset=["claim_id"], maintain_order=True)

        # Check metadata
        assert dedupe._transform_name == "dedupe_claims"
        assert dedupe._transform_tiers == ["bronze", "silver"]
        assert dedupe._transform_description == "Remove duplicate claims"
        assert dedupe._transform_sql_enabled is True

    @pytest.mark.unit
    def test_transform_execution(self):
        """Test transform executes correctly."""
        df = pl.DataFrame({"amount": [100, 200, 300]}).lazy()

        @transform("double_amounts")
        def double_amounts(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("amount") * 2).alias("amount_doubled"))

        result = double_amounts(df).collect()
        assert "amount_doubled" in result.columns
        assert result["amount_doubled"].to_list() == [200, 400, 600]


class TestTransformInputValidation:
    """Test input validation for transforms."""

    @pytest.mark.unit
    def test_non_lazyframe_input(self):
        """Test error when input is not LazyFrame."""

        @transform("test_transform")
        @pytest.mark.unit
        def test_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        with pytest.raises(TypeError, match="expects pl.LazyFrame"):
            test_transform("not a dataframe")  # type: ignore

    @pytest.mark.unit
    def test_eager_dataframe_input(self):
        """Test error when passing eager DataFrame instead of LazyFrame."""

        @transform("test_transform")
        @pytest.mark.unit
        def test_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]})
        with pytest.raises(TypeError, match="expects pl.LazyFrame"):
            test_transform(df)  # type: ignore


class TestTransformReturnValidation:
    """Test return type validation for transforms."""

    @pytest.mark.unit
    def test_valid_lazyframe_return(self):
        """Test valid LazyFrame return."""

        @transform("valid_transform")
        def valid_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = valid_transform(df)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_invalid_return_type(self):
        """Test error when returning invalid type."""

        @transform("invalid_transform")
        def invalid_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return "not a lazyframe"  # type: ignore

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        with pytest.raises(TypeError, match="must return pl.LazyFrame"):
            invalid_transform(df)


class TestTransformAutoComposition:
    """Test auto-composition from expressions."""

    @pytest.mark.unit
    def test_auto_compose_single_expression(self):
        """Test auto-composition with single expression."""

        @expression("status_upper")
        def status_expr() -> pl.Expr:
            return pl.col("status").str.to_uppercase().alias("status_upper")

        @transform("standardize", expressions=[status_expr])
        def standardize(df: pl.LazyFrame) -> pl.LazyFrame:
            # Expression should be auto-applied
            return df

        df = pl.DataFrame({"status": ["a", "b", "c"]}).lazy()
        result = standardize(df).collect()
        assert "status_upper" in result.columns
        assert result["status_upper"].to_list() == ["A", "B", "C"]

    @pytest.mark.unit
    def test_auto_compose_multiple_expressions(self):
        """Test auto-composition with multiple expressions."""

        @expression("status_upper")
        def status_expr() -> pl.Expr:
            return pl.col("status").str.to_uppercase().alias("status_upper")

        @expression("amount_doubled")
        def amount_expr() -> pl.Expr:
            return (pl.col("amount") * 2).alias("amount_doubled")

        @transform("process", expressions=[status_expr, amount_expr])
        def process(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"status": ["a", "b"], "amount": [100, 200]}).lazy()
        result = process(df).collect()
        assert "status_upper" in result.columns
        assert "amount_doubled" in result.columns
        assert result["amount_doubled"].to_list() == [200, 400]

    @pytest.mark.unit
    def test_auto_compose_with_list_expression(self):
        """Test auto-composition when expression returns list."""

        @expression("date_parts")
        def date_parts() -> list[pl.Expr]:
            return [
                pl.col("date").dt.year().alias("year"),
                pl.col("date").dt.month().alias("month"),
            ]

        @transform("extract_dates", expressions=[date_parts])
        def extract_dates(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        import datetime

        df = pl.DataFrame(
            {"date": [datetime.date(2025, 1, 15), datetime.date(2025, 2, 20)]},
            schema={"date": pl.Date},
        ).lazy()
        result = extract_dates(df).collect()
        assert "year" in result.columns
        assert "month" in result.columns

    @pytest.mark.unit
    def test_expressions_plus_transform_logic(self):
        """Test expressions applied before transform logic."""

        @expression("add_ten")
        def add_ten() -> pl.Expr:
            return (pl.col("value") + 10).alias("value_plus_ten")

        @transform("then_double", expressions=[add_ten])
        def then_double(df: pl.LazyFrame) -> pl.LazyFrame:
            # Expressions are already applied, now double the result
            return df.with_columns((pl.col("value_plus_ten") * 2).alias("doubled"))

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()
        result = then_double(df).collect()
        assert "value_plus_ten" in result.columns
        assert "doubled" in result.columns
        assert result["doubled"].to_list() == [22, 24, 26]  # (1+10)*2, (2+10)*2, (3+10)*2


class TestTransformWithRealOperations:
    """Test transforms with realistic operations."""

    @pytest.mark.unit
    def test_filter_transform(self):
        """Test filtering transform."""

        @transform("filter_active")
        def filter_active(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("status") == "A")

        df = pl.DataFrame({"status": ["A", "B", "A", "C"], "value": [1, 2, 3, 4]}).lazy()
        result = filter_active(df).collect()
        assert len(result) == 2
        assert result["value"].to_list() == [1, 3]

    @pytest.mark.unit
    def test_aggregation_transform(self):
        """Test aggregation transform."""

        @transform("aggregate_by_group")
        def aggregate(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.group_by("group").agg(pl.col("value").sum().alias("total"))

        df = pl.DataFrame({"group": ["A", "A", "B", "B"], "value": [1, 2, 3, 4]}).lazy()
        result = aggregate(df).collect()
        assert len(result) == 2

    @pytest.mark.unit
    def test_join_transform(self):
        """Test join transform."""

        @transform("enrich_with_lookup")
        def enrich(df: pl.LazyFrame) -> pl.LazyFrame:
            lookup = pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}).lazy()
            return df.join(lookup, on="id", how="left")

        df = pl.DataFrame({"id": [1, 2, 3], "value": [100, 200, 300]}).lazy()
        result = enrich(df).collect()
        assert "name" in result.columns
        assert result["name"].to_list() == ["Alice", "Bob", "Charlie"]

    @pytest.mark.unit
    def test_deduplication_transform(self):
        """Test deduplication transform."""

        @transform("dedupe_by_id")
        def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.unique(subset=["id"], maintain_order=True, keep="last")

        df = pl.DataFrame({"id": [1, 2, 1, 3, 2], "value": [100, 200, 150, 300, 250]}).lazy()
        result = dedupe(df).collect()
        assert len(result) == 3


class TestTransformMetadata:
    """Test metadata attachment and access."""

    @pytest.mark.unit
    def test_metadata_attributes(self):
        """Test all metadata attributes are attached."""

        @transform(
            name="test_transform",
            tier=["silver", "gold"],
            description="Test transform for validation",
            sql_enabled=False,
        )
        @pytest.mark.unit
        def test_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert test_transform._transform_name == "test_transform"
        assert test_transform._transform_tiers == ["silver", "gold"]
        assert test_transform._transform_description == "Test transform for validation"
        assert test_transform._transform_sql_enabled is False

    @pytest.mark.unit
    def test_expressions_metadata(self):
        """Test expressions list is stored in metadata."""

        @expression("expr1")
        def expr1() -> pl.Expr:
            return pl.col("a")

        @expression("expr2")
        def expr2() -> pl.Expr:
            return pl.col("b")

        @transform("with_exprs", expressions=[expr1, expr2])
        def with_exprs(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert len(with_exprs._transform_expressions) == 2


class TestTransformComposability:
    """Test transform composability."""

    @pytest.mark.unit
    def test_transform_has_rshift(self):
        """Test transform has __rshift__ for composition."""

        @transform("test_transform")
        @pytest.mark.unit
        def test_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert hasattr(test_transform, "__rshift__")


class TestTransformWithParameters:
    """Test transforms with additional parameters."""

    @pytest.mark.unit
    def test_transform_with_kwargs(self):
        """Test transform with keyword arguments."""

        @transform("filter_by_threshold")
        def filter_by_threshold(
            df: pl.LazyFrame, threshold: int, column: str = "amount"
        ) -> pl.LazyFrame:
            return df.filter(pl.col(column) > threshold)

        df = pl.DataFrame({"amount": [50, 150, 250]}).lazy()
        result = filter_by_threshold(df, threshold=100).collect()
        assert len(result) == 2
        assert result["amount"].to_list() == [150, 250]

    @pytest.mark.unit
    def test_transform_with_multiple_args(self):
        """Test transform with multiple arguments."""

        @transform("add_columns")
        def add_columns(df: pl.LazyFrame, col1_name: str, col2_name: str) -> pl.LazyFrame:
            return df.with_columns(
                [pl.col("value").alias(col1_name), pl.col("value").alias(col2_name)]
            )

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()
        result = add_columns(df, "copy1", "copy2").collect()
        assert "copy1" in result.columns
        assert "copy2" in result.columns


class TestTransformEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.unit
    def test_empty_dataframe(self):
        """Test transform on empty dataframe."""

        @transform("process_empty")
        def process(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("doubled"))

        df = pl.DataFrame({"value": []}, schema={"value": pl.Int64}).lazy()
        result = process(df).collect()
        assert len(result) == 0
        assert "doubled" in result.columns

    @pytest.mark.unit
    def test_null_handling_in_transform(self):
        """Test transform with null values."""

        @transform("fill_nulls")
        def fill_nulls(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(pl.col("value").fill_null(0).alias("value_filled"))

        df = pl.DataFrame({"value": [1, None, 3]}).lazy()
        result = fill_nulls(df).collect()
        assert result["value_filled"].to_list() == [1, 0, 3]

    @pytest.mark.unit
    def test_transform_preserves_columns(self):
        """Test transform preserves original columns."""

        @transform("add_computed")
        def add_computed(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") + pl.col("b")).alias("c"))

        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]}).lazy()
        result = add_computed(df).collect()
        assert set(result.columns) == {"a", "b", "c"}


class TestTransformChaining:
    """Test chaining multiple transforms."""

    @pytest.mark.unit
    def test_manual_chaining(self):
        """Test manually chaining transforms."""

        @transform("step1")
        def step1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        @transform("step2")
        def step2(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()
        result = step2(step1(df)).collect()
        assert result["value"].to_list() == [4, 6, 8]  # (1+1)*2, (2+1)*2, (3+1)*2


class TestTransformDecorator:
    """Tests for the transform decorator."""

    @pytest.mark.unit
    def test_transform_basic(self):
        """Test basic transform decorator."""

        @transform("filter_active")
        def filter_active(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("status") == "A")

        lf = pl.LazyFrame({"status": ["A", "B", "A"]})
        result = filter_active(lf)
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().height == 2

    @pytest.mark.unit
    def test_transform_metadata(self):
        """Test transform metadata."""

        @transform("my_transform", tier=["bronze", "silver"], description="Test", sql_enabled=False)
        def my_func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert my_func._transform_name == "my_transform"
        assert my_func._transform_tiers == ["bronze", "silver"]
        assert my_func._transform_description == "Test"
        assert my_func._transform_sql_enabled is False

    @pytest.mark.unit
    def test_transform_single_tier(self):
        """Test transform with single tier."""

        @transform("t", tier="gold")
        def func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert func._transform_tiers == ["gold"]

    @pytest.mark.unit
    def test_transform_invalid_input(self):
        """Test transform raises on invalid input type."""

        @transform("bad_input")
        def func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        with pytest.raises(TypeError, match="expects pl.LazyFrame"):
            func("not_a_lazyframe")

    @pytest.mark.unit
    def test_transform_none_input_not_accepted(self):
        """Test transform raises on None input when not accepted."""

        @transform("no_none")
        def func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        with pytest.raises(TypeError, match="expects pl.LazyFrame"):
            func(None)

    @pytest.mark.unit
    def test_transform_none_input_rejected(self):
        """Test transform rejects None input (annotations are strings due to PEP 563)."""

        @transform("rejects_none")
        def func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        with pytest.raises(TypeError, match="expects pl.LazyFrame"):
            func(None)

    @pytest.mark.unit
    def test_transform_invalid_return(self):
        """Test transform raises on invalid return type."""

        @transform("bad_return")
        def func(df: pl.LazyFrame) -> pl.LazyFrame:
            return "not_a_lazyframe"  # type: ignore

        lf = pl.LazyFrame({"a": [1]})
        with pytest.raises(TypeError, match="must return pl.LazyFrame"):
            func(lf)

    @pytest.mark.unit
    def test_transform_with_expressions(self):
        """Test transform with auto-composed expressions."""

        def double_expr():
            return pl.col("a") * 2

        @transform("with_exprs", expressions=[double_expr])
        def func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1, 2, 3]})
        result = func(lf).collect()
        assert result["a"].to_list() == [2, 4, 6]

    @pytest.mark.unit
    def test_transform_with_expression_list(self):
        """Test transform with expression returning list."""

        def multi_exprs():
            return [
                pl.col("a").alias("a_copy"),
                (pl.col("a") * 2).alias("a_double"),
            ]

        @transform("multi_expr_transform", expressions=[multi_exprs])
        def func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1, 2]})
        result = func(lf).collect()
        assert "a_copy" in result.columns
        assert "a_double" in result.columns

    @pytest.mark.unit
    def test_transform_with_bad_expression(self):
        """Test transform with expression returning unexpected type."""

        def bad_expr():
            return "not_an_expr"

        @transform("bad_expr_transform", expressions=[bad_expr])
        def func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1]})
        # Should skip the bad expression with a warning
        result = func(lf)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_transform_rshift_composition(self):
        """Test >> operator for transform composition."""

        @transform("step1")
        def step1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") * 2).alias("a"))

        @transform("step2")
        def step2(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") + 1).alias("a"))

        composed = step1 >> step2
        lf = pl.LazyFrame({"a": [1, 2, 3]})
        result = composed(lf).collect()
        # step1: a*2 = [2,4,6], step2: a+1 = [3,5,7]
        assert result["a"].to_list() == [3, 5, 7]

    @pytest.mark.unit
    def test_transform_description_from_docstring(self):
        """Test transform uses docstring when no description provided."""

        @transform("doc_transform")
        def func(df: pl.LazyFrame) -> pl.LazyFrame:
            """My transform docstring."""
            return df

        assert func._transform_description == "My transform docstring."


class TestTransformFunction:
    """Tests for TransformFunction class."""

    @pytest.mark.unit
    def test_transform_function_no_params(self):
        """Test TransformFunction with no-param function."""

        def func() -> pl.LazyFrame:
            return pl.LazyFrame({"a": [1]})

        tf = TransformFunction(func, "test", ["bronze"], None, None, True)
        assert tf._accepts_none is False


class TestTransformMethod:
    """Tests for the transform_method meta-decorator."""

    @pytest.mark.unit
    def test_transform_method_basic(self):
        """Test transform_method with defaults."""

        @transform_method()
        def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1]})
        result = my_transform(lf)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_transform_method_no_composition(self):
        """Test transform_method without composition."""

        @transform_method(enable_composition=False)
        def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1]})
        result = my_transform(lf)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_transform_method_with_memory_and_check(self):
        """Test transform_method with memory tracking and empty check."""

        @transform_method(track_memory=True, check_not_empty="df")
        def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1]})
        result = my_transform(lf)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_transform_method_with_validate_args(self):
        """Test transform_method with type validation."""

        @transform_method(validate_args_types={"df": pl.LazyFrame})
        def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1]})
        result = my_transform(lf)
        assert isinstance(result, pl.LazyFrame)
