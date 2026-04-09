# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._parsers._transformations module."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import pytest
from types import SimpleNamespace
import acoharmony
import polars as pl

from .conftest import _schema, create_mock_metadata


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._transformations is not None


class TestApplySchemaTransformations:
    """Test apply_schema_transformations function."""

    @pytest.mark.unit
    def test_apply_transformations_string_trim(self):
        """Test string trimming transformation."""
        df = pl.LazyFrame({"name": ["  test  ", " data ", "value   "]})
        mock_schema = create_mock_metadata(
            "test",
            columns=[{"name": "name", "output_name": "name", "data_type": "string"}],
            file_format={"type": "csv"},
        )
        mock_schema.polars = {"string_trim": True}
        transformed = apply_schema_transformations(df, mock_schema)
        result = transformed.collect()
        assert result["name"][0] == "test"
        assert result["name"][1] == "data"

    @pytest.mark.unit
    def test_apply_transformations_categorical(self):
        """Test categorical column conversion."""
        df = pl.LazyFrame({"status": ["active", "inactive", "active"]})
        mock_schema = create_mock_metadata(
            "test",
            columns=[{"name": "status", "output_name": "status", "data_type": "string"}],
            file_format={"type": "csv"},
        )
        mock_schema.polars = {"categorical_columns": ["status"]}
        transformed = apply_schema_transformations(df, mock_schema)
        result = transformed.collect()
        assert result["status"].dtype == pl.Categorical


class TestTransformations:
    """Tests for acoharmony._parsers._transformations."""

    @pytest.mark.unit
    def test_apply_column_types_int(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"val": ["1", "2", "3"]})
        schema = _schema([{"name": "val", "data_type": "int"}])
        df = apply_column_types(lf, schema).collect()
        assert df["val"].dtype == pl.Int64

    @pytest.mark.unit
    def test_apply_column_types_integer(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"val": ["1"]})
        schema = _schema([{"name": "val", "data_type": "integer"}])
        df = apply_column_types(lf, schema).collect()
        assert df["val"].dtype == pl.Int64

    @pytest.mark.unit
    def test_apply_column_types_int64(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"val": ["1"]})
        schema = _schema([{"name": "val", "data_type": "int64"}])
        df = apply_column_types(lf, schema).collect()
        assert df["val"].dtype == pl.Int64

    @pytest.mark.unit
    def test_apply_column_types_float(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"val": ["1.5"]})
        schema = _schema([{"name": "val", "data_type": "float"}])
        df = apply_column_types(lf, schema).collect()
        assert df["val"].dtype == pl.Float64

    @pytest.mark.unit
    def test_apply_column_types_float64(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"val": ["1.5"]})
        schema = _schema([{"name": "val", "data_type": "float64"}])
        df = apply_column_types(lf, schema).collect()
        assert df["val"].dtype == pl.Float64

    @pytest.mark.unit
    def test_apply_column_types_decimal(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"val": ["1.5"]})
        schema = _schema([{"name": "val", "data_type": "decimal"}])
        df = apply_column_types(lf, schema).collect()
        assert df["val"].dtype == pl.Float64

    @pytest.mark.unit
    def test_apply_column_types_date(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"dt": ["2024-01-01"]})
        schema = _schema([{"name": "dt", "data_type": "date"}])
        df = apply_column_types(lf, schema).collect()
        assert df["dt"].dtype == pl.Utf8

    @pytest.mark.unit
    def test_apply_column_types_boolean(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"flag": ["true", "false", "1", "0", "yes", "no"]})
        schema = _schema([{"name": "flag", "data_type": "boolean"}])
        df = apply_column_types(lf, schema).collect()
        assert df["flag"].to_list() == [True, False, True, False, True, False]

    @pytest.mark.unit
    def test_apply_column_types_no_columns(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"x": [1]})
        schema = SimpleNamespace()
        df = apply_column_types(lf, schema).collect()
        assert df.columns == ["x"]

    @pytest.mark.unit
    def test_apply_column_types_missing_col(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"x": [1]})
        schema = _schema([{"name": "missing", "data_type": "int"}])
        df = apply_column_types(lf, schema).collect()
        assert df.columns == ["x"]

    @pytest.mark.unit
    def test_apply_column_types_output_name(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"renamed": ["1"]})
        schema = _schema([{"name": "orig", "output_name": "renamed", "data_type": "int"}])
        df = apply_column_types(lf, schema).collect()
        assert df["renamed"].dtype == pl.Int64

    @pytest.mark.unit
    def test_apply_column_types_dtype_key(self):
        from acoharmony._parsers._transformations import apply_column_types

        lf = pl.LazyFrame({"val": ["1"]})
        schema = _schema([{"name": "val", "dtype": "int"}])
        df = apply_column_types(lf, schema).collect()
        assert df["val"].dtype == pl.Int64

    @pytest.mark.unit
    def test_apply_schema_transformations_categorical(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"status": ["A", "B", "A"]})
        schema = _schema([], polars={"categorical_columns": ["status"]})
        df = apply_schema_transformations(lf, schema).collect()
        assert df["status"].dtype == pl.Categorical

    @pytest.mark.unit
    def test_apply_schema_transformations_string_trim(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"name": ["  Alice  ", " Bob "]})
        schema = _schema([], polars={"string_trim": True})
        df = apply_schema_transformations(lf, schema).collect()
        assert df["name"].to_list() == ["Alice", "Bob"]

    @pytest.mark.unit
    def test_apply_schema_transformations_drop_columns(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"keep": [1], "drop_me": [2]})
        schema = _schema([], polars={"drop_columns": ["drop_me"]})
        df = apply_schema_transformations(lf, schema).collect()
        assert "drop_me" not in df.columns

    @pytest.mark.unit
    def test_apply_schema_transformations_decimal_columns(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"amount": ["1.5"]})
        schema = _schema([], polars={"decimal_columns": [{"amount": 2}]})
        df = apply_schema_transformations(lf, schema).collect()
        assert df["amount"].dtype == pl.Float64

    @pytest.mark.unit
    def test_apply_schema_transformations_no_polars_config(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"x": [1]})
        schema = _schema([])
        df = apply_schema_transformations(lf, schema).collect()
        assert df.columns == ["x"]

    @pytest.mark.unit
    def test_apply_schema_transformations_empty_polars(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"x": [1]})
        schema = _schema([], polars={})
        df = apply_schema_transformations(lf, schema).collect()
        assert df.columns == ["x"]

    @pytest.mark.unit
    def test_apply_schema_transformations_drop_nonexistent(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"x": [1]})
        schema = _schema([], polars={"drop_columns": ["nonexistent"]})
        df = apply_schema_transformations(lf, schema).collect()
        assert df.columns == ["x"]

    @pytest.mark.unit
    def test_apply_schema_transformations_categorical_nonexistent(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"x": [1]})
        schema = _schema([], polars={"categorical_columns": ["nonexistent"]})
        df = apply_schema_transformations(lf, schema).collect()
        assert df.columns == ["x"]

    @pytest.mark.unit
    def test_apply_schema_transformations_decimal_nonexistent(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"x": [1]})
        schema = _schema([], polars={"decimal_columns": [{"nonexistent": 2}]})
        df = apply_schema_transformations(lf, schema).collect()
        assert df.columns == ["x"]

    @pytest.mark.unit
    def test_apply_schema_transformations_decimal_non_dict(self):
        from acoharmony._parsers._transformations import apply_schema_transformations

        lf = pl.LazyFrame({"x": [1]})
        schema = _schema([], polars={"decimal_columns": ["not_a_dict"]})
        df = apply_schema_transformations(lf, schema).collect()
        assert df.columns == ["x"]


class TestApplyColumnTypes:
    """Test apply_column_types function."""

    @pytest.mark.unit
    def test_apply_column_types_integer(self):
        """Test applying integer type conversion."""
        import polars as pl

        from acoharmony._parsers import apply_column_types

        from .conftest import create_mock_metadata

        df = pl.LazyFrame({"id": ["1", "2", "3"], "name": ["a", "b", "c"]})
        mock_schema = create_mock_metadata(
            "test",
            columns=[
                {"name": "id", "output_name": "id", "data_type": "integer"},
                {"name": "name", "output_name": "name", "data_type": "string"},
            ],
            file_format={"type": "csv"},
        )
        typed = apply_column_types(df, mock_schema)
        result = typed.collect()
        assert result["id"].dtype == pl.Int64
        assert result["name"].dtype == pl.Utf8

    @pytest.mark.unit
    def test_apply_column_types_float(self):
        """Test applying float type conversion."""
        import polars as pl

        from acoharmony._parsers import apply_column_types

        from .conftest import create_mock_metadata

        df = pl.LazyFrame({"amount": ["10.5", "20.3", "30.1"]})
        mock_schema = create_mock_metadata(
            "test",
            columns=[{"name": "amount", "output_name": "amount", "data_type": "float"}],
            file_format={"type": "csv"},
        )
        typed = apply_column_types(df, mock_schema)
        result = typed.collect()
        assert result["amount"].dtype == pl.Float64

    @pytest.mark.unit
    def test_apply_column_types_boolean(self):
        """Test applying boolean type conversion from various string formats."""
        import polars as pl

        from acoharmony._parsers import apply_column_types

        from .conftest import create_mock_metadata

        df = pl.LazyFrame(
            {
                "flag1": ["1", "0", "1"],
                "flag2": ["true", "false", "true"],
                "flag3": ["True", "False", "True"],
            }
        )
        mock_schema = create_mock_metadata(
            "test",
            columns=[
                {"name": "flag1", "output_name": "flag1", "data_type": "boolean"},
                {"name": "flag2", "output_name": "flag2", "data_type": "boolean"},
                {"name": "flag3", "output_name": "flag3", "data_type": "boolean"},
            ],
            file_format={"type": "csv"},
        )
        typed = apply_column_types(df, mock_schema)
        result = typed.collect()
        assert result["flag1"].dtype == pl.Boolean
        assert result["flag2"].dtype == pl.Boolean
        assert result["flag3"].dtype == pl.Boolean
        assert result["flag1"][0] is True
        assert result["flag1"][1] is False
        assert result["flag2"][0] is True
        assert result["flag2"][1] is False

    @pytest.mark.unit
    def test_apply_column_types_no_columns(self):
        """Test apply_column_types with no column definitions."""
        import polars as pl

        from acoharmony._parsers import apply_column_types

        from .conftest import create_mock_metadata

        df = pl.LazyFrame({"col": [1, 2, 3]})
        mock_schema = create_mock_metadata("test", columns=[], file_format={"type": "csv"})
        typed = apply_column_types(df, mock_schema)
        result = typed.collect()
        assert "col" in result.columns
