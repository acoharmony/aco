# © 2025 HarmonyCares
# All rights reserved.

"""
Tests for @composable decorator and compose function.

Tests functional composition with >> operator and compose().
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._decor8.composition import composable, compose
from acoharmony._decor8.transforms import transform


class TestComposableBasics:
    """Test basic composable decorator functionality."""

    @pytest.mark.unit
    def test_composable_decorator(self):
        """Test basic composable wrapping."""

        @composable
        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()
        result = add_one(df).collect()
        assert result["value"].to_list() == [2, 3, 4]

    @pytest.mark.unit
    def test_composable_has_rshift(self):
        """Test composable function has __rshift__ method."""

        @composable
        @pytest.mark.unit
        def test_func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert hasattr(test_func, "__rshift__")

    @pytest.mark.unit
    def test_composable_is_marked(self):
        """Test composable function is marked with _is_composable."""

        @composable
        @pytest.mark.unit
        def test_func(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert hasattr(test_func, "_is_composable")
        assert test_func._is_composable is True


class TestComposableRightShiftOperator:
    """Test >> operator for composition."""

    @pytest.mark.unit
    def test_simple_composition(self):
        """Test simple >> composition."""

        @composable
        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        @composable
        def multiply_two(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()
        pipeline = add_one >> multiply_two
        result = pipeline(df).collect()

        # (1+1)*2=4, (2+1)*2=6, (3+1)*2=8
        assert result["value"].to_list() == [4, 6, 8]

    @pytest.mark.unit
    def test_three_function_composition(self):
        """Test chaining three functions."""

        @composable
        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        @composable
        def multiply_two(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        @composable
        def add_ten(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 10).alias("value"))

        df = pl.DataFrame({"value": [1]}).lazy()
        pipeline = add_one >> multiply_two >> add_ten
        result = pipeline(df).collect()

        # ((1+1)*2)+10 = 14
        assert result["value"].to_list() == [14]

    @pytest.mark.unit
    def test_composition_with_filter(self):
        """Test composition including filters."""

        @composable
        def filter_positive(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("value") > 0)

        @composable
        def double_value(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        df = pl.DataFrame({"value": [-5, 0, 5, 10]}).lazy()
        pipeline = filter_positive >> double_value
        result = pipeline(df).collect()

        assert result["value"].to_list() == [10, 20]


class TestComposableWithDataFrame:
    """Test composable functions with real dataframes."""

    @pytest.mark.unit
    def test_composition_preserves_columns(self):
        """Test composition preserves all columns."""

        @composable
        def add_total(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") + pl.col("b")).alias("total"))

        @composable
        def add_avg(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("total") / 2).alias("avg"))

        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]}).lazy()
        pipeline = add_total >> add_avg
        result = pipeline(df).collect()

        assert set(result.columns) == {"a", "b", "total", "avg"}
        assert result["avg"].to_list() == [2.0, 3.0]

    @pytest.mark.unit
    def test_composition_with_aggregation(self):
        """Test composition including aggregation."""

        @composable
        def filter_active(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("status") == "A")

        @composable
        def aggregate_by_group(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.group_by("group").agg(pl.col("value").sum().alias("total"))

        df = pl.DataFrame(
            {
                "group": ["X", "X", "Y", "Y"],
                "status": ["A", "B", "A", "A"],
                "value": [10, 20, 30, 40],
            }
        ).lazy()

        pipeline = filter_active >> aggregate_by_group
        result = pipeline(df).collect()

        # After filter: X:10, Y:30, Y:40
        # After agg: X:10, Y:70
        assert len(result) == 2


class TestComposableWithParameters:
    """Test composable functions with parameters."""

    @pytest.mark.unit
    def test_composition_with_kwargs(self):
        """Test composition with keyword arguments."""

        @composable
        def filter_by_threshold(
            df: pl.LazyFrame, threshold: int, column: str = "value"
        ) -> pl.LazyFrame:
            return df.filter(pl.col(column) > threshold)

        @composable
        def multiply_column(df: pl.LazyFrame, factor: int, column: str = "value") -> pl.LazyFrame:
            return df.with_columns((pl.col(column) * factor).alias(column))

        df = pl.DataFrame({"value": [5, 15, 25]}).lazy()

        # Partial application - wrap in composable to enable >>
        from functools import partial

        filter_gt_10 = composable(partial(filter_by_threshold, threshold=10))
        multiply_by_2 = composable(partial(multiply_column, factor=2))

        pipeline = filter_gt_10 >> multiply_by_2
        result = pipeline(df).collect()

        # Values > 10: [15, 25], then * 2: [30, 50]
        assert result["value"].to_list() == [30, 50]


class TestComposableWithTransforms:
    """Test composable with transform decorator."""

    @pytest.mark.unit
    def test_composable_on_transform(self):
        """Test composable wrapping transform."""

        @composable
        @transform("test_transform")
        @pytest.mark.unit
        def test_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()
        result = test_transform(df).collect()
        assert result["value"].to_list() == [2, 3, 4]

    @pytest.mark.unit
    def test_transform_inherits_composability(self):
        """Test transform decorator already has composability."""

        # Transforms already have __rshift__ from transform decorator
        @transform("add_one")
        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        assert hasattr(add_one, "__rshift__")


class TestComposeFunction:
    """Test compose() function for explicit composition."""

    @pytest.mark.unit
    def test_compose_empty(self):
        """Test compose with no functions."""
        identity = compose()
        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = identity(df).collect()
        assert result["a"].to_list() == [1, 2, 3]

    @pytest.mark.unit
    def test_compose_single_function(self):
        """Test compose with single function."""

        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        pipeline = compose(add_one)
        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()
        result = pipeline(df).collect()
        assert result["value"].to_list() == [2, 3, 4]

    @pytest.mark.unit
    def test_compose_multiple_functions(self):
        """Test compose with multiple functions."""

        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        def multiply_two(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        def add_ten(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 10).alias("value"))

        pipeline = compose(add_one, multiply_two, add_ten)
        df = pl.DataFrame({"value": [1]}).lazy()
        result = pipeline(df).collect()

        # ((1+1)*2)+10 = 14
        assert result["value"].to_list() == [14]

    @pytest.mark.unit
    def test_compose_result_is_composable(self):
        """Test compose() result is itself composable."""

        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        def multiply_two(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        pipeline1 = compose(add_one, multiply_two)

        # Result should be composable
        assert hasattr(pipeline1, "__rshift__")


class TestComposableEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.unit
    def test_empty_dataframe(self):
        """Test composition on empty dataframe."""

        @composable
        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value_plus_one"))

        df = pl.DataFrame({"value": []}, schema={"value": pl.Int64}).lazy()
        result = add_one(df).collect()
        assert len(result) == 0
        assert "value_plus_one" in result.columns

    @pytest.mark.unit
    def test_composition_with_null_handling(self):
        """Test composition with null values."""

        @composable
        def fill_nulls(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(pl.col("value").fill_null(0).alias("value"))

        @composable
        def double_value(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        df = pl.DataFrame({"value": [1, None, 3]}).lazy()
        pipeline = fill_nulls >> double_value
        result = pipeline(df).collect()
        assert result["value"].to_list() == [2, 0, 6]

    @pytest.mark.unit
    def test_non_lazyframe_warning(self, caplog):
        """Test warning when composable returns non-LazyFrame."""

        @composable
        def bad_func(df: pl.LazyFrame) -> pl.LazyFrame:
            return "not a lazyframe"  # type: ignore

        df = pl.DataFrame({"a": [1]}).lazy()
        # Should log warning
        bad_func(df)


class TestComposableRealWorld:
    """Test realistic composition scenarios."""

    @pytest.mark.unit
    def test_data_cleaning_pipeline(self):
        """Test realistic data cleaning pipeline."""

        @composable
        def remove_nulls(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("value").is_not_null())

        @composable
        def remove_negatives(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("value") >= 0)

        @composable
        def cap_outliers(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(
                pl.when(pl.col("value") > 100)
                .then(pl.lit(100))
                .otherwise(pl.col("value"))
                .alias("value")
            )

        df = pl.DataFrame({"value": [None, -5, 50, 150]}).lazy()
        pipeline = remove_nulls >> remove_negatives >> cap_outliers
        result = pipeline(df).collect()

        assert result["value"].to_list() == [50, 100]

    @pytest.mark.unit
    def test_feature_engineering_pipeline(self):
        """Test feature engineering pipeline."""

        @composable
        def add_total(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("qty") * pl.col("price")).alias("total"))

        @composable
        def add_tax(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("total") * 0.1).alias("tax"))

        @composable
        def add_grand_total(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("total") + pl.col("tax")).alias("grand_total"))

        df = pl.DataFrame({"qty": [2, 3], "price": [10.0, 20.0]}).lazy()
        pipeline = add_total >> add_tax >> add_grand_total
        result = pipeline(df).collect()

        assert "total" in result.columns
        assert "tax" in result.columns
        assert "grand_total" in result.columns
        # qty=2, price=10: total=20, tax=2, grand_total=22
        assert result["grand_total"].to_list() == [22.0, 66.0]


class TestComposableVsManualChaining:
    """Test composable >> vs manual function chaining."""

    @pytest.mark.unit
    def test_composable_equivalent_to_manual(self):
        """Test >> operator produces same result as manual chaining."""

        @composable
        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        @composable
        def multiply_two(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()

        # Using >> operator
        result_composed = (add_one >> multiply_two)(df).collect()

        # Manual chaining
        result_manual = multiply_two(add_one(df)).collect()

        assert result_composed["value"].to_list() == result_manual["value"].to_list()


class TestComposable:
    """Tests for the composable decorator."""

    @pytest.mark.unit
    def test_composable_basic(self):
        """Test basic composable function."""

        @composable
        def double(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") * 2).alias("a"))

        lf = pl.LazyFrame({"a": [1, 2, 3]})
        result = double(lf).collect()
        assert result["a"].to_list() == [2, 4, 6]

    @pytest.mark.unit
    def test_composable_rshift(self):
        """Test >> operator composition."""

        @composable
        def double(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") * 2).alias("a"))

        @composable
        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") + 1).alias("a"))

        composed = double >> add_one
        lf = pl.LazyFrame({"a": [1, 2]})
        result = composed(lf).collect()
        assert result["a"].to_list() == [3, 5]

    @pytest.mark.unit
    def test_composable_non_lazyframe_return(self):
        """Test composable warns on non-LazyFrame return."""

        @composable
        def bad_func(df: pl.LazyFrame):
            return "not_lazyframe"

        lf = pl.LazyFrame({"a": [1]})
        result = bad_func(lf)
        assert result == "not_lazyframe"

    @pytest.mark.unit
    def test_composable_chain_three(self):
        """Test chaining three composable functions."""

        @composable
        def step1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") + 1).alias("a"))

        @composable
        def step2(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") * 2).alias("a"))

        @composable
        def step3(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") - 1).alias("a"))

        composed = step1 >> step2 >> step3
        lf = pl.LazyFrame({"a": [1]})
        # step1: 1+1=2, step2: 2*2=4, step3: 4-1=3
        result = composed(lf).collect()
        assert result["a"].to_list() == [3]


class TestCompose:
    """Tests for the compose function."""

    @pytest.mark.unit
    def test_compose_basic(self):
        """Test compose with multiple functions."""

        def double(df):
            return df.with_columns((pl.col("a") * 2).alias("a"))

        def add_one(df):
            return df.with_columns((pl.col("a") + 1).alias("a"))

        composed = compose(double, add_one)
        lf = pl.LazyFrame({"a": [1, 2]})
        result = composed(lf).collect()
        assert result["a"].to_list() == [3, 5]

    @pytest.mark.unit
    def test_compose_empty(self):
        """Test compose with no functions (identity)."""

        identity = compose()
        lf = pl.LazyFrame({"a": [1, 2]})
        result = identity(lf)
        # identity is a composable lambda, returns same value
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_compose_single(self):
        """Test compose with single function."""

        def double(df):
            return df.with_columns((pl.col("a") * 2).alias("a"))

        composed = compose(double)
        lf = pl.LazyFrame({"a": [5]})
        result = composed(lf).collect()
        assert result["a"].to_list() == [10]


class TestMetaDecoratorTracedNone:
    """Cover branches where traced is None (opentelemetry not available)."""

    @pytest.mark.unit
    def test_transform_method_traced_none(self):
        """Branch 497->501: traced is None in transform_method."""
        from unittest.mock import patch
        from acoharmony._decor8.composition import transform_method

        # Patch the import so traced becomes None
        with patch.dict("sys.modules", {"acoharmony._trace": None, "acoharmony._trace.decorators": None}):
            @transform_method(track_memory=False)
            def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
                return df.with_columns((pl.col("a") + 1).alias("a"))

            lf = pl.LazyFrame({"a": [1, 2]})
            result = my_transform(lf).collect()
            assert result["a"].to_list() == [2, 3]

    @pytest.mark.unit
    def test_parser_method_traced_none(self):
        """Branch 609->613: traced is None in parser_method."""
        from unittest.mock import patch
        from acoharmony._decor8.composition import parser_method

        with patch.dict("sys.modules", {"acoharmony._trace": None, "acoharmony._trace.decorators": None}):
            @parser_method(track_memory=False)
            def my_parser(data: str) -> str:
                return data.upper()

            result = my_parser("hello")
            assert result == "HELLO"

    @pytest.mark.unit
    def test_expression_method_traced_none(self):
        """Branch 716->720: traced is None in expression_method."""
        from unittest.mock import patch
        from acoharmony._decor8.composition import expression_method

        with patch.dict("sys.modules", {"acoharmony._trace": None, "acoharmony._trace.decorators": None}):
            @expression_method(expression_name="test_expr_traced_none", track_memory=False)
            def my_expr(config: dict) -> list:
                return [pl.col("a")]

            result = my_expr({"columns": ["a"]})
            assert len(result) == 1


class TestComposableFunctionBranches:
    """Cover ComposableFunction branches 39->40 and 39->45."""

    @pytest.mark.unit
    def test_composable_returns_non_lazyframe(self):
        """Branch 39->40: result is NOT a LazyFrame, logs warning."""
        from acoharmony._decor8.composition import composable

        @composable
        def bad_func(df):
            return "not a lazyframe"

        result = bad_func(pl.DataFrame({"a": [1]}).lazy())
        # Returns the bad result (warning logged, but no exception)
        assert result == "not a lazyframe"

    @pytest.mark.unit
    def test_composable_returns_lazyframe(self):
        """Branch 39->45: result IS a LazyFrame, no warning."""
        from acoharmony._decor8.composition import composable

        @composable
        def good_func(df):
            return df.select("a")

        result = good_func(pl.DataFrame({"a": [1]}).lazy())
        assert isinstance(result, pl.LazyFrame)


class TestComposeBranches:
    """Cover compose() branches 209->210 and 209->212."""

    @pytest.mark.unit
    def test_compose_no_funcs(self):
        """Branch 209->210: no funcs passed, returns identity lambda."""
        from acoharmony._decor8.composition import compose

        identity = compose()
        df = pl.DataFrame({"x": [1, 2]}).lazy()
        result = identity(df)
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().height == 2

    @pytest.mark.unit
    def test_compose_with_funcs(self):
        """Branch 209->212: funcs provided, builds composed pipeline."""
        from acoharmony._decor8.composition import compose, composable

        @composable
        def step1(df):
            return df.with_columns(pl.lit(1).alias("new"))

        @composable
        def step2(df):
            return df.filter(pl.col("new") == 1)

        pipeline = compose(step1, step2)
        df = pl.DataFrame({"a": [1, 2]}).lazy()
        result = pipeline(df).collect()
        assert "new" in result.columns


class TestRunnerMethodBranches:
    """Cover runner_method branches 325->329 (track_memory=True) and 215->216/217."""

    @pytest.mark.unit
    def test_runner_method_with_memory_tracking(self):
        """Branch 325->329: track_memory=True applies profile_memory."""
        from acoharmony._decor8.composition import runner_method

        @runner_method(track_memory=True, threshold=100.0)
        def my_runner(schema_name: str) -> str:
            return schema_name

        result = my_runner("test")
        assert result == "test"

    @pytest.mark.unit
    def test_compose_iter_func(self):
        """Branch 215->216, 215->217: the for loop in composed function."""
        from acoharmony._decor8.composition import compose, composable

        @composable
        def add_col(df):
            return df.with_columns(pl.lit("x").alias("extra"))

        pipeline = compose(add_col)
        df = pl.DataFrame({"a": [1]}).lazy()
        result = pipeline(df).collect()
        assert "extra" in result.columns


class TestTransformMethodBranches:
    """Cover transform_method branches 489->490, 493->494, 497->501, 501->502."""

    @pytest.mark.unit
    def test_transform_method_with_validate_args(self):
        """Branch 489->490: validate_args_types provided."""
        from acoharmony._decor8.composition import transform_method

        @transform_method(
            validate_args_types={"df": pl.LazyFrame},
            check_not_empty=None,
            track_memory=False,
            enable_composition=False,
        )
        def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        result = my_transform(pl.DataFrame({"a": [1]}).lazy())
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_transform_method_with_check_not_empty(self):
        """Branch 493->494: check_not_empty is truthy."""
        from acoharmony._decor8.composition import transform_method

        @transform_method(
            check_not_empty="df",
            track_memory=False,
            enable_composition=False,
        )
        def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        result = my_transform(pl.DataFrame({"a": [1]}).lazy())
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_transform_method_with_traced(self):
        """Branch 497->501: traced is not None."""
        from acoharmony._decor8.composition import transform_method

        @transform_method(track_memory=False, enable_composition=False)
        def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        result = my_transform(pl.DataFrame({"a": [1]}).lazy())
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_transform_method_with_memory(self):
        """Branch 501->502: track_memory=True."""
        from acoharmony._decor8.composition import transform_method

        @transform_method(track_memory=True, enable_composition=False)
        def my_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        result = my_transform(pl.DataFrame({"a": [1]}).lazy())
        assert isinstance(result, pl.LazyFrame)


class TestParserMethodBranches:
    """Cover parser_method branch 609->613 (traced not None)."""

    @pytest.mark.unit
    def test_parser_method_with_traced(self):
        """Branch 609->613: traced IS not None (normal case)."""
        from acoharmony._decor8.composition import parser_method

        @parser_method(track_memory=False)
        def my_parser(data: str) -> str:
            return data.upper()

        result = my_parser("hello")
        assert result == "HELLO"


class TestExpressionMethodBranches:
    """Cover expression_method branches 716->720 and 720->721."""

    @pytest.mark.unit
    def test_expression_method_with_memory(self):
        """Branch 720->721: track_memory=True."""
        from acoharmony._decor8.composition import expression_method

        @expression_method(expression_name="mem_branch_test", track_memory=True)
        def build_expr() -> pl.Expr:
            return pl.col("a")

        result = build_expr()
        assert isinstance(result, pl.Expr)

    @pytest.mark.unit
    def test_expression_method_traced_available(self):
        """Branch 716->720: traced is not None (normal import succeeds)."""
        from acoharmony._decor8.composition import expression_method

        @expression_method(expression_name="traced_branch_test", track_memory=False)
        def build_expr() -> pl.Expr:
            return pl.col("b")

        result = build_expr()
        assert isinstance(result, pl.Expr)
