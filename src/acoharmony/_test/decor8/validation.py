# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _decor8.validation module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch
from typing import TYPE_CHECKING

import pytest
from acoharmony._exceptions import ValidationError

if TYPE_CHECKING:
    pass


class TestValidateArgs:
    """Tests for the validate_args decorator."""

    @pytest.mark.unit
    def test_validate_args_pass(self):
        """Test validate_args with correct types."""

        @validate_args(name=str, count=int)
        def func(name, count):
            return f"{name}:{count}"

        assert func("hello", 5) == "hello:5"

    @pytest.mark.unit
    def test_validate_args_fail(self):
        """Test validate_args raises on wrong type."""

        @validate_args(name=str)
        def func(name):
            return name

        with pytest.raises(ValidationError, match="must be str"):
            func(123)

    @pytest.mark.unit
    def test_validate_args_none_ok(self):
        """Test validate_args allows None values."""

        @validate_args(name=str)
        def func(name=None):
            return name

        assert func(None) is None

    @pytest.mark.unit
    def test_validate_args_bad_call(self):
        """Test validate_args with invalid call signature."""

        @validate_args(name=str)
        def func(name):
            return name

        with pytest.raises(ValidationError, match="Invalid arguments"):
            func()  # missing required arg


class TestRequireColumns:
    """Tests for the require_columns decorator."""

    @pytest.mark.unit
    def test_require_columns_present(self):
        """Test require_columns passes when all columns present."""

        @require_columns("a", "b")
        def func(df):
            return df

        df = pl.DataFrame({"a": [1], "b": [2]})
        result = func(df)
        assert result.height == 1

    @pytest.mark.unit
    def test_require_columns_missing(self):
        """Test require_columns raises on missing columns."""

        @require_columns("a", "b", "c")
        def func(df):
            return df

        df = pl.DataFrame({"a": [1]})
        with pytest.raises(ValidationError, match="missing required columns"):
            func(df)

    @pytest.mark.unit
    def test_require_columns_lazyframe(self):
        """Test require_columns with LazyFrame."""

        @require_columns("x")
        def func(df):
            return df

        lf = pl.LazyFrame({"x": [1], "y": [2]})
        result = func(lf)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_require_columns_no_df_arg(self):
        """Test require_columns when no DataFrame argument."""

        @require_columns("a")
        def func(name):
            return name

        assert func("hello") == "hello"


class TestCheckNotEmpty:
    """Tests for the check_not_empty decorator."""

    @pytest.mark.unit
    def test_check_not_empty_dataframe_ok(self):
        """Test check_not_empty passes with non-empty DataFrame."""

        @check_not_empty("df")
        def func(df):
            return df

        df = pl.DataFrame({"a": [1]})
        assert func(df).height == 1

    @pytest.mark.unit
    def test_check_not_empty_dataframe_empty(self):
        """Test check_not_empty raises on empty DataFrame."""

        @check_not_empty("df")
        def func(df):
            return df

        df = pl.DataFrame({"a": []}).cast({"a": pl.Int64})
        with pytest.raises(ValidationError, match="is empty"):
            func(df)

    @pytest.mark.unit
    def test_check_not_empty_lazyframe_ok(self):
        """Test check_not_empty passes with non-empty LazyFrame."""

        @check_not_empty("df")
        def func(df):
            return df

        lf = pl.LazyFrame({"a": [1]})
        result = func(lf)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_check_not_empty_lazyframe_empty(self):
        """Test check_not_empty raises on empty LazyFrame."""

        @check_not_empty("df")
        def func(df):
            return df

        lf = pl.LazyFrame({"a": []}).cast({"a": pl.Int64})
        with pytest.raises(ValidationError, match="is empty"):
            func(lf)

    @pytest.mark.unit
    def test_check_not_empty_no_matching_param(self):
        """Test check_not_empty when param doesn't exist."""

        @check_not_empty("nonexistent")
        def func(df):
            return df

        df = pl.DataFrame({"a": [1]})
        assert func(df).height == 1


class TestValidatePathExists:
    """Tests for the validate_path_exists decorator."""

    @pytest.mark.unit
    def test_validate_path_exists_ok(self, tmp_path):
        """Test validate_path_exists passes for existing path."""

        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        @validate_path_exists("path")
        def func(path):
            return path

        assert func(str(test_file)) == str(test_file)

    @pytest.mark.unit
    def test_validate_path_exists_missing(self):
        """Test validate_path_exists raises for missing path."""

        @validate_path_exists("path")
        def func(path):
            return path

        with pytest.raises(ValidationError, match="does not exist"):
            func("/nonexistent/path/file.txt")

    @pytest.mark.unit
    def test_validate_path_exists_with_path_object(self, tmp_path):
        """Test validate_path_exists with Path object."""

        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        @validate_path_exists("path")
        def func(path):
            return path

        result = func(test_file)
        assert result == test_file

    @pytest.mark.unit
    def test_validate_path_no_matching_param(self):
        """Test validate_path_exists when param doesn't exist."""

        @validate_path_exists("nonexistent")
        def func(path):
            return path

        assert func("/whatever") == "/whatever"


class TestValidateFileFormat:
    """Tests for the validate_file_format decorator."""

    @pytest.mark.unit
    def test_validate_file_format_ok(self):
        """Test validate_file_format passes for valid format."""

        @validate_file_format("file_path", formats=[".csv", ".parquet"])
        def func(file_path):
            return file_path

        assert func("data.csv") == "data.csv"

    @pytest.mark.unit
    def test_validate_file_format_invalid(self):
        """Test validate_file_format raises for invalid format."""

        @validate_file_format("file_path", formats=[".csv", ".parquet"])
        def func(file_path):
            return file_path

        with pytest.raises(ValidationError, match="Invalid file format"):
            func("data.xlsx")

    @pytest.mark.unit
    def test_validate_file_format_no_formats(self):
        """Test validate_file_format with no format restrictions."""

        @validate_file_format("file_path")
        def func(file_path):
            return file_path

        assert func("data.anything") == "data.anything"

    @pytest.mark.unit
    def test_validate_file_format_path_object(self):
        """Test validate_file_format with Path object."""

        @validate_file_format("file_path", formats=[".csv"])
        def func(file_path):
            return file_path

        assert func(Path("data.csv")) == Path("data.csv")

        with pytest.raises(ValidationError):
            func(Path("data.json"))

    @pytest.mark.unit
    def test_validate_file_format_no_matching_param(self):
        """Test validate_file_format when param doesn't exist."""

        @validate_file_format("nonexistent", formats=[".csv"])
        def func(file_path):
            return file_path

        assert func("data.xlsx") == "data.xlsx"

    @pytest.mark.unit
    def test_validate_file_format_case_insensitive(self):
        """Test validate_file_format is case insensitive."""

        @validate_file_format("file_path", formats=[".CSV"])
        def func(file_path):
            return file_path

        assert func("data.csv") == "data.csv"


class TestValidateSchema:
    """Tests for the validate_schema decorator."""

    @pytest.mark.unit
    def test_validate_schema_found(self):
        """Test validate_schema passes when schema exists."""

        mock_catalog = MagicMock()
        mock_catalog.get_schema.return_value = {"name": "test"}

        @validate_schema("schema_name")
        def func(schema_name):
            return schema_name

        with patch("acoharmony._catalog.Catalog", return_value=mock_catalog):
            assert func("test") == "test"

    @pytest.mark.unit
    def test_validate_schema_not_found(self):
        """Test validate_schema raises when schema doesn't exist."""

        mock_catalog = MagicMock()
        mock_catalog.get_schema.return_value = None
        mock_catalog.list_tables.return_value = ["a", "b"]

        @validate_schema("schema_name")
        def func(schema_name):
            return schema_name

        with patch("acoharmony._catalog.Catalog", return_value=mock_catalog):
            with pytest.raises(ValidationError, match="not found"):
                func("nonexistent")

    @pytest.mark.unit
    def test_validate_schema_arg_not_in_bound_arguments(self):
        """Branch 149->171: schema_name_arg not in bound.arguments, skip validation."""

        @validate_schema("schema_name")
        def func(other_arg):
            return other_arg

        # No need to mock Catalog because schema_name_arg won't be in bound.arguments
        result = func("hello")
        assert result == "hello"


class TestValidatePathExistsParamMissing:
    """Cover branch 253->267: param_name not in bound.arguments for validate_path_exists."""

    @pytest.mark.unit
    def test_validate_path_exists_param_not_in_args(self):
        """Branch 253->267: param_name not in bound.arguments, skip validation."""

        @validate_path_exists("path")
        def func(other_arg):
            return other_arg

        result = func("some_value")
        assert result == "some_value"


class TestValidatePathExistsNonStringPath:
    """Cover branch 253->267: path is in args but not a str/Path (e.g., int or None)."""

    @pytest.mark.unit
    def test_validate_path_exists_non_string_skips(self):
        """Branch 253->267: path is not str|Path, skip isinstance check."""

        @validate_path_exists("path")
        def func(path):
            return path

        # Pass an integer - not str/Path, so isinstance is False, jumps to 267
        result = func(12345)
        assert result == 12345

    @pytest.mark.unit
    def test_validate_path_exists_none_value_skips(self):
        """Branch 253->267: path is None, skip isinstance check."""

        @validate_path_exists("path")
        def func(path=None):
            return path

        result = func(None)
        assert result is None


class TestValidateFileFormatParamMissing:
    """Cover branch 306->320: param_name not in bound.arguments for validate_file_format."""

    @pytest.mark.unit
    def test_validate_file_format_param_not_in_args(self):
        """Branch 306->320: param_name not in bound.arguments, skip validation."""

        @validate_file_format("file_path", formats=[".csv"])
        def func(other_arg):
            return other_arg

        result = func("some_value")
        assert result == "some_value"


class TestValidateFileFormatNonStringPath:
    """Cover branch 306->320: path is in args but not a str/Path."""

    @pytest.mark.unit
    def test_validate_file_format_non_string_skips(self):
        """Branch 306->320: path is not str|Path, skip isinstance check."""

        @validate_file_format("file_path", formats=[".csv"])
        def func(file_path):
            return file_path

        # Pass an integer - not str/Path
        result = func(99999)
        assert result == 99999

    @pytest.mark.unit
    def test_validate_file_format_none_value_skips(self):
        """Branch 306->320: path is None, skip isinstance check."""

        @validate_file_format("file_path", formats=[".csv"])
        def func(file_path=None):
            return file_path

        result = func(None)
        assert result is None
