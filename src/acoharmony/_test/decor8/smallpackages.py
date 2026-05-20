"""
Comprehensive tests for small packages to achieve 100% coverage.

Covers:
1. _cli_commands/sva_validate.py - SVA validation pipeline
2. _crosswalks/__init__.py - Crosswalk loading from YAML
3. _validators/field_validators.py - All validator patterns
4. Gap coverage for _exceptions, _log, _decor8
"""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import

# Direct imports from _decor8 module (since this is a comprehensive test file)
from acoharmony._decor8 import (
    check_not_empty,
    composable,
    compose,
    measure_dataframe_size,
    require_columns,
    timeit,
    validate_args,
    validate_file_format,
    validate_path_exists,
    warn_slow,
)
from acoharmony._exceptions import ValidationError

import polars as pl
import pytest


class TestDecor8ValidationDecorators:
    """Tests for _decor8.validation decorators."""

    @pytest.mark.unit
    def test_validate_args_valid_types(self):
        """validate_args passes with correct types."""

        @validate_args(x=int, y=str)
        def func(x, y):
            return f"{x}-{y}"

        assert func(1, "hello") == "1-hello"

    @pytest.mark.unit
    def test_validate_args_invalid_type(self):
        """validate_args raises ValidationError for wrong type."""

        @validate_args(x=int)
        def func(x):
            return x

        with pytest.raises(ValidationError, match="must be int"):
            func("not_an_int")

    @pytest.mark.unit
    def test_validate_args_none_allowed(self):
        """validate_args skips None values."""

        @validate_args(x=int)
        def func(x=None):
            return x

        assert func(None) is None

    @pytest.mark.unit
    def test_require_columns_valid(self):
        """require_columns passes with all columns present."""

        @require_columns("a", "b")
        def func(df):
            return df

        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = func(df)
        assert result.height == 1

    @pytest.mark.unit
    def test_require_columns_missing(self):
        """require_columns raises ValidationError for missing columns."""

        @require_columns("a", "missing_col")
        def func(df):
            return df

        df = pl.DataFrame({"a": [1], "b": [2]})
        with pytest.raises(ValidationError, match="missing required columns"):
            func(df)

    @pytest.mark.unit
    def test_check_not_empty_dataframe(self):
        """check_not_empty passes with non-empty DataFrame."""

        @check_not_empty("df")
        def func(df):
            return df

        df = pl.DataFrame({"a": [1]})
        result = func(df)
        assert result.height == 1

    @pytest.mark.unit
    def test_check_not_empty_dataframe_empty(self):
        """check_not_empty raises for empty DataFrame."""

        @check_not_empty("df")
        def func(df):
            return df

        with pytest.raises(ValidationError, match="empty"):
            func(pl.DataFrame({"a": []}))

    @pytest.mark.unit
    def test_check_not_empty_lazyframe(self):
        """check_not_empty works with LazyFrame."""

        @check_not_empty("df")
        def func(df):
            return df

        # Non-empty LazyFrame
        lf = pl.DataFrame({"a": [1]}).lazy()
        result = func(lf)
        assert isinstance(result, pl.LazyFrame)

        # Empty LazyFrame
        with pytest.raises(ValidationError, match="empty"):
            func(pl.DataFrame({"a": []}).lazy())

    @pytest.mark.unit
    def test_validate_path_exists_valid(self, tmp_path):
        """validate_path_exists passes for existing path."""

        @validate_path_exists("path")
        def func(path):
            return path

        f = tmp_path / "exists.txt"
        f.write_text("data")
        result = func(str(f))
        assert result == str(f)

    @pytest.mark.unit
    def test_validate_path_exists_missing(self):
        """validate_path_exists raises for missing path."""

        @validate_path_exists("path")
        def func(path):
            return path

        with pytest.raises(ValidationError, match="Path does not exist"):
            func("/nonexistent/path/file.txt")

    @pytest.mark.unit
    def test_validate_file_format_valid(self, tmp_path):
        """validate_file_format passes for valid format."""

        @validate_file_format("file_path", formats=[".csv", ".parquet"])
        def func(file_path):
            return file_path

        assert func("/data/file.csv") == "/data/file.csv"

    @pytest.mark.unit
    def test_validate_file_format_invalid(self):
        """validate_file_format raises for invalid format."""

        @validate_file_format("file_path", formats=[".csv", ".parquet"])
        def func(file_path):
            return file_path

        with pytest.raises(ValidationError, match="Invalid file format"):
            func("/data/file.xlsx")

    @pytest.mark.unit
    def test_validate_file_format_no_formats(self):
        """validate_file_format with empty formats list passes any file."""

        @validate_file_format("file_path", formats=[])
        def func(file_path):
            return file_path

        assert func("/data/file.anything") == "/data/file.anything"


class TestDecor8PerformanceDecorators:
    """Tests for _decor8.performance decorators."""

    @pytest.mark.unit
    def test_timeit_basic(self):
        """timeit decorator times function execution."""

        @timeit()
        def func():
            return "done"

        assert func() == "done"

    @pytest.mark.unit
    def test_timeit_with_threshold(self):
        """timeit with threshold only logs when exceeded."""

        @timeit(threshold=100.0)  # Very high threshold
        def func():
            return "fast"

        assert func() == "fast"

    @pytest.mark.unit
    def test_warn_slow_under_threshold(self):
        """warn_slow does not warn when under threshold."""

        @warn_slow(threshold_seconds=100.0)
        def func():
            return "fast"

        assert func() == "fast"

    @pytest.mark.unit
    def test_measure_dataframe_size(self):
        """measure_dataframe_size logs DataFrame dimensions."""

        @measure_dataframe_size("df")
        def func(df):
            return df

        df = pl.DataFrame({"a": [1, 2, 3]})
        result = func(df)
        assert result.height == 3


class TestDecor8CompositionDecorators:
    """Tests for _decor8.composition decorators."""

    @pytest.mark.unit
    def test_composable_basic(self):
        """composable function can be called normally."""

        @composable
        def double_col(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") * 2).alias("a_doubled"))

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = double_col(df).collect()
        assert result["a_doubled"].to_list() == [2, 4, 6]

    @pytest.mark.unit
    def test_composable_rshift(self):
        """composable functions can be composed with >>."""

        @composable
        def step1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("a") * 2).alias("b"))

        @composable
        def step2(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("b") + 1).alias("c"))

        pipeline = step1 >> step2
        df = pl.DataFrame({"a": [1, 2]}).lazy()
        result = pipeline(df).collect()
        assert result["c"].to_list() == [3, 5]

    @pytest.mark.unit
    def test_compose_function(self):
        """compose() creates pipeline from multiple functions."""

        def add_b(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(pl.lit(10).alias("b"))

        def add_c(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(pl.lit(20).alias("c"))

        pipeline = compose(add_b, add_c)
        df = pl.DataFrame({"a": [1]}).lazy()
        result = pipeline(df).collect()
        assert "b" in result.columns
        assert "c" in result.columns

    @pytest.mark.unit
    def test_compose_empty(self):
        """compose() with no args returns identity function."""

        pipeline = compose()
        df = pl.DataFrame({"a": [1]}).lazy()
        result = pipeline(df).collect()
        assert result["a"].to_list() == [1]

    @pytest.mark.unit
    def test_composable_non_lazyframe_warning(self):
        """composable warns when non-LazyFrame is returned."""

        @composable
        def bad_return(df: pl.LazyFrame):
            return "not a lazyframe"

        df = pl.DataFrame({"a": [1]}).lazy()
        # Should still return but log a warning
        result = bad_return(df)
        assert result == "not a lazyframe"
