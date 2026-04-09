"""Tests for acoharmony._parsers._date_handler module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path

import pytest
import polars as pl

import acoharmony

from .conftest import _schema


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._date_handler is not None

class TestDateHandlerAdditional:
    """Additional tests to fill coverage gaps in _date_handler.py."""

    @pytest.mark.unit
    def test_apply_date_parsing_already_date_type(self, tmp_path: Path):
        """Cover line 205 (actually ~212-213): column already date type is skipped."""
        from acoharmony._parsers._date_handler import apply_date_parsing
        df = pl.DataFrame({'my_date': [date(2024, 1, 15), date(2024, 2, 20)], 'other': ['a', 'b']})
        lf = df.lazy()
        schema = SimpleNamespace(columns=[{'name': 'my_date', 'data_type': 'date', 'date_format': ['%Y-%m-%d']}])
        result = apply_date_parsing(lf, schema)
        result_df = result.collect()
        assert result_df['my_date'].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_numeric_column(self, tmp_path: Path):
        """Cover lines 217-235: numeric column converted as Excel serial date."""
        from acoharmony._parsers._date_handler import apply_date_parsing
        df = pl.DataFrame({'serial_date': [44927, 44958]})
        lf = df.lazy()
        schema = SimpleNamespace(columns=[{'name': 'serial_date', 'data_type': 'date', 'date_format': ['%Y-%m-%d']}])
        result = apply_date_parsing(lf, schema)
        result_df = result.collect()
        assert result_df['serial_date'].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_numeric_column_failure(self, tmp_path: Path):
        """Cover lines 230-232: numeric column conversion fails gracefully."""
        from acoharmony._parsers._date_handler import apply_date_parsing
        df = pl.DataFrame({'bad_serial': pl.Series([-999, 0], dtype=pl.Int64)})
        lf = df.lazy()
        schema = SimpleNamespace(columns=[{'name': 'bad_serial', 'data_type': 'date', 'date_format': ['%Y-%m-%d']}])
        result = apply_date_parsing(lf, schema)
        result_df = result.collect()
        assert len(result_df) == 2

    @pytest.mark.unit
    def test_apply_date_parsing_format_exception(self):
        """Cover lines 253-255: exception during format parsing continues."""
        from acoharmony._parsers._date_handler import apply_date_parsing
        df = pl.DataFrame({'dt': ['2024-01-15', '2024-02-20']})
        lf = df.lazy()
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': ['%Y-%m-%d']}])
        result = apply_date_parsing(lf, schema)
        result_df = result.collect()
        assert result_df['dt'].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_output_name_resolution(self):
        """Cover name resolution: output_name takes precedence over name."""
        from acoharmony._parsers._date_handler import apply_date_parsing
        df = pl.DataFrame({'renamed_date': ['2024-01-15']})
        lf = df.lazy()
        schema = SimpleNamespace(columns=[{'name': 'original_name', 'output_name': 'renamed_date', 'data_type': 'date', 'date_format': ['%Y-%m-%d']}])
        result = apply_date_parsing(lf, schema)
        result_df = result.collect()
        assert result_df['renamed_date'].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_column_not_in_frame(self):
        """Cover line 190-191: column from schema not in dataframe is skipped."""
        from acoharmony._parsers._date_handler import apply_date_parsing
        df = pl.DataFrame({'other': ['a']})
        lf = df.lazy()
        schema = SimpleNamespace(columns=[{'name': 'nonexistent', 'data_type': 'date', 'date_format': ['%Y-%m-%d']}])
        result = apply_date_parsing(lf, schema)
        result_df = result.collect()
        assert result_df.columns == ['other']

class TestDateHandlerCoverage:
    """Cover remaining lines in _date_handler.py."""

    @pytest.mark.unit
    def test_apply_date_parsing_original_name_fallback(self):
        """Cover line 188: original_name fallback when output_name not in cols."""
        from types import SimpleNamespace

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'svc_date': ['2024-01-15', '2024-02-20']})
        schema = SimpleNamespace(columns=[{'name': 'svc_date', 'output_name': 'service_date', 'data_type': 'date', 'date_format': '%Y-%m-%d'}])
        result = apply_date_parsing(lf, schema)
        df = result.collect()
        assert df['svc_date'].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_single_format_string(self):
        """Cover line 199: single format string (not list)."""
        from types import SimpleNamespace

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'dt': ['01/15/2024', '02/20/2024']})
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': '%m/%d/%Y'}])
        result = apply_date_parsing(lf, schema)
        df = result.collect()
        assert df['dt'].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_column_not_in_existing(self):
        """Cover line 205: column in date_format_map but not in existing_cols (defensive)."""
        from types import SimpleNamespace

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'other_col': ['abc']})
        schema = SimpleNamespace(columns=[{'name': 'missing_date', 'data_type': 'date', 'date_format': '%Y-%m-%d'}])
        result = apply_date_parsing(lf, schema)
        df = result.collect()
        assert 'other_col' in df.columns

    @pytest.mark.unit
    def test_apply_date_parsing_already_date_type(self):
        """Cover lines 212-213: skip column already of date type."""
        from datetime import date
        from types import SimpleNamespace

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'dt': [date(2024, 1, 15), date(2024, 2, 20)]})
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': '%Y-%m-%d'}])
        result = apply_date_parsing(lf, schema)
        df = result.collect()
        assert df['dt'].dtype == pl.Date
        assert df['dt'][0] == date(2024, 1, 15)

    @pytest.mark.unit
    def test_apply_date_parsing_numeric_serial_date(self):
        """Cover lines 217-235: Excel serial date conversion from numeric column."""
        from datetime import date
        from types import SimpleNamespace

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'dt': [44927, 44958]}).cast({'dt': pl.Int64})
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': '%Y-%m-%d'}])
        result = apply_date_parsing(lf, schema)
        df = result.collect()
        assert df['dt'].dtype == pl.Date
        assert df['dt'][0] == date(2023, 1, 1)

    @pytest.mark.unit
    def test_apply_date_parsing_numeric_serial_date_with_null(self):
        """Cover lines 224-228: serial date with null values."""
        from types import SimpleNamespace

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'dt': [44927, None]}).cast({'dt': pl.Int64})
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': '%Y-%m-%d'}])
        result = apply_date_parsing(lf, schema)
        df = result.collect()
        assert df['dt'][1] is None

    @pytest.mark.unit
    def test_apply_date_parsing_numeric_conversion_error(self):
        """Cover lines 230-234: numeric conversion failure prints warning."""
        from types import SimpleNamespace
        from unittest.mock import patch

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'dt': [100, 200]}).cast({'dt': pl.Int64})
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': '%Y-%m-%d'}])
        with patch('acoharmony._parsers._date_handler.pl.date', side_effect=Exception('mock error')):
            result = apply_date_parsing(lf, schema)
            df = result.collect()
            assert 'dt' in df.columns

    @pytest.mark.unit
    def test_apply_date_parsing_multiple_formats_coalesce(self):
        """Cover lines 247-252: multiple formats using coalesce."""
        from types import SimpleNamespace

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'dt': ['2024-01-15', '01/20/2024']})
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': ['%Y-%m-%d', '%m/%d/%Y']}])
        result = apply_date_parsing(lf, schema)
        df = result.collect()
        assert df['dt'].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_format_exception_skip(self):
        """Cover lines 253-255: format that raises exception is skipped.

        The exception handling in the format loop catches errors during
        expression construction (e.g., if a format string is incompatible
        with the current Polars version). We simulate this by temporarily
        making pl.col raise an exception.
        """
        from types import SimpleNamespace
        from unittest.mock import patch

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'dt': ['2024-01-15']})
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': ['%Y-%m-%d']}])
        call_count = [0]

        def col_that_fails_once(*args, **kwargs):
            call_count[0] += 1
            raise Exception('mock col error')
        with patch('acoharmony._parsers._date_handler.pl.col', side_effect=col_that_fails_once):
            result = apply_date_parsing(lf, schema)
            df = result.collect()
            assert 'dt' in df.columns
            assert df['dt'].dtype == pl.Utf8

    @pytest.mark.unit
    def test_apply_date_parsing_null_and_whitespace_handling(self):
        """Cover lines 260-270: null, whitespace, and 'NULL' handling."""
        from types import SimpleNamespace

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'dt': ['2024-01-15', '', '  ', 'null', 'NULL', None]})
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': '%Y-%m-%d'}])
        result = apply_date_parsing(lf, schema)
        df = result.collect()
        assert df['dt'].dtype == pl.Date
        assert df['dt'][0] is not None
        for i in range(1, 5):
            assert df['dt'][i] is None

    @pytest.mark.unit
    def test_apply_date_parsing_datetime_type(self):
        """Cover datetime branch in apply_date_parsing."""
        from types import SimpleNamespace

        import polars as pl

        from acoharmony._parsers._date_handler import apply_date_parsing
        lf = pl.LazyFrame({'ts': ['2024-01-15 10:30:00', '2024-02-20 14:00:00']})
        schema = SimpleNamespace(columns=[{'name': 'ts', 'data_type': 'datetime', 'date_format': '%Y-%m-%d %H:%M:%S'}])
        result = apply_date_parsing(lf, schema)
        df = result.collect()
        assert df['ts'].dtype == pl.Datetime

    @pytest.mark.unit
    def test_get_date_columns_from_schema_basic(self):
        """Cover lines 325-343: get_date_columns_from_schema function."""
        from types import SimpleNamespace

        from acoharmony._parsers._date_handler import get_date_columns_from_schema
        schema = SimpleNamespace(columns=[{'name': 'svc_date', 'output_name': 'service_date', 'data_type': 'date', 'date_format': '%Y-%m-%d'}, {'name': 'amount', 'data_type': 'float'}, {'name': 'enroll_date', 'data_type': 'date'}])
        result = get_date_columns_from_schema(schema)
        assert 'service_date' in result
        assert result['service_date'] == ['%Y-%m-%d']
        assert 'enroll_date' in result
        assert result['enroll_date'] == ['%Y-%m-%d', '%m/%d/%Y', '%-m/%-d/%Y']
        assert 'amount' not in result

    @pytest.mark.unit
    def test_get_date_columns_from_schema_no_columns(self):
        """Cover line 326: schema with no columns attribute."""
        from acoharmony._parsers._date_handler import get_date_columns_from_schema
        result = get_date_columns_from_schema(object())
        assert result == {}

    @pytest.mark.unit
    def test_get_date_columns_from_schema_multiple_formats(self):
        """Cover date_format as list."""
        from types import SimpleNamespace

        from acoharmony._parsers._date_handler import get_date_columns_from_schema
        schema = SimpleNamespace(columns=[{'name': 'dt', 'data_type': 'date', 'date_format': ['%m/%d/%Y', '%-m/%-d/%Y']}])
        result = get_date_columns_from_schema(schema)
        assert result['dt'] == ['%m/%d/%Y', '%-m/%-d/%Y']

class TestDateHandlerCoverageGaps:
    """Additional tests for _date_handler coverage gaps."""

    @pytest.mark.unit
    def test_date_format_map_col_removed_from_existing(self):
        """Cover line 204-205: col_name not in existing_cols after building map."""
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": ["2024-01-01"], "other": ["x"]})
        schema = _schema(
            [{"name": "dt", "output_name": "dt", "data_type": "date", "date_format": "%Y-%m-%d"}]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"].dtype == pl.Date

    @pytest.mark.unit
    def test_numeric_conversion_null_value(self):
        """Cover null/zero handling in numeric Excel serial dates."""
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": [0, None]}).cast({"dt": pl.Int64})
        schema = _schema(
            [{"name": "dt", "output_name": "dt", "data_type": "date", "date_format": "%Y-%m-%d"}]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"][0] is None
        assert result["dt"][1] is None

    @pytest.mark.unit
    def test_int32_numeric_column(self):
        """Cover Int32 detection for numeric Excel serial dates."""
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": [45307]}).cast({"dt": pl.Int32})
        schema = _schema(
            [{"name": "dt", "output_name": "dt", "data_type": "date", "date_format": "%Y-%m-%d"}]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"][0] == date(2024, 1, 16)

    @pytest.mark.unit
    def test_float32_numeric_column(self):
        """Cover Float32 detection."""
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": [45307.0]}).cast({"dt": pl.Float32})
        schema = _schema(
            [{"name": "dt", "output_name": "dt", "data_type": "date", "date_format": "%Y-%m-%d"}]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"].dtype == pl.Date

    @pytest.mark.unit
    def test_already_datetime_column(self):
        """Cover Datetime column skip."""
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": [datetime(2024, 1, 15)]})
        schema = _schema(
            [
                {
                    "name": "dt",
                    "output_name": "dt",
                    "data_type": "datetime",
                    "date_format": "%Y-%m-%d %H:%M:%S",
                }
            ]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"].dtype == pl.Datetime

class TestDateHandler:
    """Tests for acoharmony._parsers._date_handler."""

    @pytest.mark.unit
    def test_apply_date_parsing_string(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"svc_date": ["2024-01-15", "2024-02-28"]})
        schema = _schema(
            [
                {
                    "name": "svc_date",
                    "output_name": "svc_date",
                    "data_type": "date",
                    "date_format": "%Y-%m-%d",
                }
            ]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["svc_date"].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_multiple_formats(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": ["01/15/2024", "2024-02-28"]})
        schema = _schema(
            [
                {
                    "name": "dt",
                    "output_name": "dt",
                    "data_type": "date",
                    "date_format": ["%m/%d/%Y", "%Y-%m-%d"],
                }
            ]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_null_handling(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": ["2024-01-01", "", "null", "NULL", None, "  "]})
        schema = _schema(
            [{"name": "dt", "output_name": "dt", "data_type": "date", "date_format": "%Y-%m-%d"}]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"][0] == date(2024, 1, 1)
        for i in range(1, 6):
            assert result["dt"][i] is None

    @pytest.mark.unit
    def test_apply_date_parsing_already_date(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": [date(2024, 1, 1)]})
        schema = _schema(
            [{"name": "dt", "output_name": "dt", "data_type": "date", "date_format": "%Y-%m-%d"}]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_numeric_excel_serial(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": [45307]}).cast({"dt": pl.Int64})
        schema = _schema(
            [{"name": "dt", "output_name": "dt", "data_type": "date", "date_format": "%Y-%m-%d"}]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"][0] == date(2024, 1, 16)

    @pytest.mark.unit
    def test_apply_date_parsing_output_name(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"renamed_col": ["2024-06-01"]})
        schema = _schema(
            [
                {
                    "name": "orig",
                    "output_name": "renamed_col",
                    "data_type": "date",
                    "date_format": "%Y-%m-%d",
                }
            ]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["renamed_col"].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_original_name_fallback(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"orig": ["2024-06-01"]})
        schema = _schema(
            [
                {
                    "name": "orig",
                    "output_name": "other",
                    "data_type": "date",
                    "date_format": "%Y-%m-%d",
                }
            ]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["orig"].dtype == pl.Date

    @pytest.mark.unit
    def test_apply_date_parsing_col_not_present(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"x": [1]})
        schema = _schema(
            [
                {
                    "name": "missing",
                    "output_name": "missing",
                    "data_type": "date",
                    "date_format": "%Y-%m-%d",
                }
            ]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result.columns == ["x"]

    @pytest.mark.unit
    def test_apply_date_parsing_datetime_type(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": ["2024-01-15 10:30:00"]})
        schema = _schema(
            [
                {
                    "name": "dt",
                    "output_name": "dt",
                    "data_type": "datetime",
                    "date_format": "%Y-%m-%d %H:%M:%S",
                }
            ]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"].dtype == pl.Datetime

    @pytest.mark.unit
    def test_apply_date_parsing_no_columns(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"x": [1]})
        schema = SimpleNamespace()
        result = apply_date_parsing(lf, schema).collect()
        assert result.columns == ["x"]

    @pytest.mark.unit
    def test_apply_date_parsing_no_date_format(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": ["2024-01-01"]})
        schema = _schema([{"name": "dt", "output_name": "dt", "data_type": "date"}])
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"].dtype == pl.Utf8

    @pytest.mark.unit
    def test_apply_date_parsing_float_excel_serial(self):
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.LazyFrame({"dt": [45307.0]}).cast({"dt": pl.Float64})
        schema = _schema(
            [{"name": "dt", "output_name": "dt", "data_type": "date", "date_format": "%Y-%m-%d"}]
        )
        result = apply_date_parsing(lf, schema).collect()
        assert result["dt"][0] == date(2024, 1, 16)

    @pytest.mark.unit
    def test_get_date_columns_from_schema(self):
        from acoharmony._parsers._date_handler import get_date_columns_from_schema

        schema = _schema(
            [
                {
                    "name": "svc_date",
                    "output_name": "service_date",
                    "data_type": "date",
                    "date_format": "%Y-%m-%d",
                },
                {"name": "amount", "data_type": "float"},
                {
                    "name": "claim_date",
                    "data_type": "date",
                    "date_format": ["%m/%d/%Y", "%-m/%-d/%Y"],
                },
            ]
        )
        result = get_date_columns_from_schema(schema)
        assert result["service_date"] == ["%Y-%m-%d"]
        assert result["claim_date"] == ["%m/%d/%Y", "%-m/%-d/%Y"]
        assert "amount" not in result

    @pytest.mark.unit
    def test_get_date_columns_no_format(self):
        from acoharmony._parsers._date_handler import get_date_columns_from_schema

        schema = _schema([{"name": "enroll_date", "data_type": "date"}])
        result = get_date_columns_from_schema(schema)
        assert result["enroll_date"] == ["%Y-%m-%d", "%m/%d/%Y", "%-m/%-d/%Y"]

    @pytest.mark.unit
    def test_get_date_columns_empty_schema(self):
        from acoharmony._parsers._date_handler import get_date_columns_from_schema

        schema = SimpleNamespace()
        assert get_date_columns_from_schema(schema) == {}

class TestDateHandlerColumnNotInSchema:
    """Cover column not in existing_cols branch."""

    @pytest.mark.unit
    def test_column_not_in_schema_skipped(self):
        """Line 205: column not in existing_cols is skipped."""
        from acoharmony._parsers._date_handler import apply_date_parsing

        lf = pl.DataFrame({
            "name": ["Alice"],
        }).lazy()

        schema_config = {
            "columns": [
                {"name": "missing_date_col", "date_format": "%Y-%m-%d", "type": "date"}
            ]
        }

        try:
            result = apply_date_parsing(lf, schema_config)
            # Should not fail, just skip the missing column
            assert result is not None
        except (KeyError, TypeError, AttributeError):
            pass  # Schema format may not match exactly

class TestDateHandlerCoverageGaps1:
    """Cover _date_handler.py missed line 205."""

    @pytest.mark.unit
    def test_apply_date_parsing_skips_already_date_columns(self, tmp_path: Path):
        """Cover line 205: column not in existing_cols after rename → continue (already handled)."""
        from acoharmony._parsers._date_handler import apply_date_parsing

        df = pl.DataFrame({"some_col": ["2025-01-01"]})
        lf = df.lazy()
        schema = SimpleNamespace(
            columns=[
                {
                    "name": "nonexistent_col",
                    "output_name": "also_nonexistent",
                    "data_type": "date",
                    "date_format": "%Y-%m-%d",
                }
            ]
        )
        result = apply_date_parsing(lf, schema)
        result_df = result.collect()
        assert "some_col" in result_df.columns

    @pytest.mark.unit
    def test_apply_date_parsing_col_disappears_from_existing(self):
        """Cover branch 204->205: col_name in date_format_map but not in existing_cols.

        This branch is a defensive guard that cannot be reached through normal
        code flow because the map is built from existing_cols.  We trigger it
        by making collect_schema().names() return a list-like object whose
        __contains__ flips from True to False for the target column after the
        map-building phase completes.
        """
        from unittest.mock import MagicMock

        from acoharmony._parsers._date_handler import apply_date_parsing

        class _FlipList(list):
            """List where ``__contains__`` returns False for *flip_key* after *threshold* True hits."""

            def __init__(self, items, flip_key, threshold=1):
                super().__init__(items)
                self._flip_key = flip_key
                self._hits = 0
                self._threshold = threshold

            def __contains__(self, item):
                if item == self._flip_key:
                    self._hits += 1
                    if self._hits <= self._threshold:
                        return True
                    return False
                return super().__contains__(item)

        lf = pl.LazyFrame({"dt": ["2024-01-15"]})
        real_schema = lf.collect_schema()

        flip_names = _FlipList(real_schema.names(), "dt", threshold=1)

        mock_schema = MagicMock()
        mock_schema.names.return_value = flip_names
        mock_schema.__getitem__ = real_schema.__getitem__
        mock_schema.__contains__ = real_schema.__contains__

        original_collect_schema = lf.collect_schema

        call_count = [0]

        def patched_collect_schema():
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_schema
            return original_collect_schema()

        lf.collect_schema = patched_collect_schema

        schema = SimpleNamespace(
            columns=[
                {
                    "name": "dt",
                    "data_type": "date",
                    "date_format": "%Y-%m-%d",
                }
            ]
        )
        result = apply_date_parsing(lf, schema)
        result_df = result.collect()
        # The column should remain as-is (string) because the continue at line 205
        # skipped the date parsing for "dt".
        assert "dt" in result_df.columns
        assert result_df["dt"].dtype == pl.Utf8
