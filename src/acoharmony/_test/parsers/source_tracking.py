"""Tests for acoharmony._parsers._source_tracking module."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path
from unittest.mock import MagicMock
import pytest
import tempfile
import polars as pl

import acoharmony
from acoharmony.medallion import MedallionLayer

from .conftest import create_mock_metadata


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._source_tracking is not None


class TestSourceTracking:
    """Tests for acoharmony._parsers._source_tracking."""

    @pytest.mark.unit
    def test_add_source_tracking_basic(self):
        from acoharmony._parsers._source_tracking import add_source_tracking

        lf = pl.LazyFrame({"x": [1, 2]})
        result = add_source_tracking(
            lf, source_file="/path/to/file.csv", schema_name="cclf1", file_date="2024-01-15"
        ).collect()
        assert "processed_at" in result.columns
        assert "source_file" in result.columns
        assert "source_filename" in result.columns
        assert "file_date" in result.columns
        assert "medallion_layer" in result.columns
        assert result["source_file"][0] == "cclf1"
        assert result["source_filename"][0] == "file.csv"
        assert result["file_date"][0] == "2024-01-15"
        assert result["medallion_layer"][0] is None

    @pytest.mark.unit
    def test_add_source_tracking_with_medallion_layer(self):
        from acoharmony._parsers._source_tracking import add_source_tracking

        lf = pl.LazyFrame({"x": [1]})
        layer = MagicMock()
        layer.value = "bronze"
        result = add_source_tracking(
            lf, source_file="file.csv", schema_name="bar", file_date=None, medallion_layer=layer
        ).collect()
        assert result["medallion_layer"][0] == "bronze"

    @pytest.mark.unit
    def test_add_source_tracking_no_file_date(self):
        from acoharmony._parsers._source_tracking import add_source_tracking

        lf = pl.LazyFrame({"x": [1]})
        result = add_source_tracking(lf, source_file="file.csv", schema_name="alr").collect()
        assert result["file_date"][0] is None

    @pytest.mark.unit
    def test_add_source_tracking_empty_source_file(self):
        from acoharmony._parsers._source_tracking import add_source_tracking

        lf = pl.LazyFrame({"x": [1]})
        result = add_source_tracking(lf, source_file="", schema_name="test").collect()
        assert "source_filename" in result.columns

class TestAddSourceTracking:
    """Test add_source_tracking function."""

    @pytest.mark.unit
    def test_add_source_tracking_basic(self):
        """Test adding basic source tracking columns."""
        df = pl.LazyFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        tracked = add_source_tracking(
            df, source_file="/path/to/test.csv", schema_name="test_table", file_date="2024-01-01"
        )
        result = tracked.collect()
        assert "processed_at" in result.columns
        assert "source_file" in result.columns
        assert "source_filename" in result.columns
        assert "file_date" in result.columns
        assert "medallion_layer" in result.columns
        assert result["source_file"][0] == "test_table"
        assert result["source_filename"][0] == "test.csv"
        assert result["file_date"][0] == "2024-01-01"

    @pytest.mark.unit
    def test_add_source_tracking_with_medallion_layer(self):
        """Test adding source tracking with medallion layer."""
        df = pl.LazyFrame({"id": [1, 2]})
        tracked = add_source_tracking(
            df,
            source_file="test.parquet",
            schema_name="test",
            medallion_layer=MedallionLayer.SILVER,
        )
        result = tracked.collect()
        assert "medallion_layer" in result.columns
        assert result["medallion_layer"][0] == "silver"

    @pytest.mark.unit
    def test_add_source_tracking_bronze_layer(self):
        """Test bronze layer tracking."""
        df = pl.LazyFrame({"col": [1]})
        tracked = add_source_tracking(
            df,
            source_file="raw.csv",
            schema_name="raw_table",
            medallion_layer=MedallionLayer.BRONZE,
        )
        result = tracked.collect()
        assert result["medallion_layer"][0] == "bronze"

    @pytest.mark.unit
    def test_add_source_tracking_gold_layer(self):
        """Test gold layer tracking."""
        df = pl.LazyFrame({"col": [1]})
        tracked = add_source_tracking(
            df,
            source_file="final.parquet",
            schema_name="final_table",
            medallion_layer=MedallionLayer.GOLD,
        )
        result = tracked.collect()
        assert result["medallion_layer"][0] == "gold"


class TestParseFile:
    """Test main parse_file function."""

    @pytest.mark.unit
    def test_parse_file_with_tracking(self):
        """Test parse_file adds tracking columns including medallion_layer."""
        import tempfile
        from pathlib import Path

        from acoharmony.medallion import MedallionLayer
        from acoharmony.parsers import parse_file

        from .conftest import create_mock_metadata

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,value\n")
            f.write("1,100\n")
            csv_path = Path(f.name)
        try:
            mock_schema = create_mock_metadata(
                "test_table",
                columns=[
                    {"name": "id", "output_name": "id", "data_type": "integer"},
                    {"name": "value", "output_name": "value", "data_type": "integer"},
                ],
                file_format={"type": "csv"},
                medallion_layer=MedallionLayer.BRONZE,
            )
            lf = parse_file(csv_path, mock_schema, add_tracking=True, schema_name="test_table")
            result = lf.collect()
            assert "id" in result.columns
            assert "value" in result.columns
            assert "processed_at" in result.columns
            assert "source_file" in result.columns
            assert "source_filename" in result.columns
            assert "medallion_layer" in result.columns
            assert result["medallion_layer"][0] == "bronze"
            assert result["source_file"][0] == "test_table"
        finally:
            csv_path.unlink()

    @pytest.mark.unit
    def test_parse_file_without_tracking(self):
        """Test parse_file without adding tracking."""
        import tempfile
        from pathlib import Path

        from acoharmony.parsers import parse_file

        from .conftest import create_mock_metadata

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col1\n")
            f.write("value\n")
            csv_path = Path(f.name)
        try:
            mock_schema = create_mock_metadata("test", [], {"type": "csv"})
            lf = parse_file(csv_path, mock_schema, add_tracking=False)
            result = lf.collect()
            assert "processed_at" not in result.columns
            assert "medallion_layer" not in result.columns
        finally:
            csv_path.unlink()

    @pytest.mark.unit
    def test_parse_file_auto_format_detection(self):
        """Test parse_file auto-detects format from extension."""
        import tempfile
        from pathlib import Path

        from acoharmony.parsers import parse_file

        from .conftest import create_mock_metadata

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("a,b\n1,2\n")
            csv_path = Path(f.name)
        try:
            mock_schema = create_mock_metadata("test", [], {"type": "auto"})
            lf = parse_file(csv_path, mock_schema, add_tracking=False)
            result = lf.collect()
            assert len(result) == 1
            assert "a" in result.columns
        finally:
            csv_path.unlink()

    @pytest.mark.unit
    def test_parse_file_nonexistent_raises_error(self):
        """Test parse_file raises error for nonexistent file."""
        from acoharmony.parsers import parse_file

        from .conftest import create_mock_metadata

        mock_schema = create_mock_metadata("test", [], {"type": "csv"})
        with pytest.raises(FileNotFoundError):
            parse_file("/nonexistent/file.csv", mock_schema)


class TestParserIntegration:
    """Integration tests for parsers with medallion architecture."""

    @pytest.mark.unit
    def test_parser_with_all_medallion_layers(self):
        """Test parsing with different medallion layers."""
        import tempfile
        from pathlib import Path

        from acoharmony.medallion import MedallionLayer
        from acoharmony.parsers import parse_file

        from .conftest import create_mock_metadata

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id\n1\n")
            csv_path = Path(f.name)
        try:
            for layer in [MedallionLayer.BRONZE, MedallionLayer.SILVER, MedallionLayer.GOLD]:
                mock_schema = create_mock_metadata(
                    f"{layer.value}_table",
                    [{"name": "id", "output_name": "id", "data_type": "integer"}],
                    {"type": "csv"},
                    medallion_layer=layer,
                )
                lf = parse_file(
                    csv_path, mock_schema, add_tracking=True, schema_name=f"{layer.value}_table"
                )
                result = lf.collect()
                assert result["medallion_layer"][0] == layer.value
        finally:
            csv_path.unlink()

    @pytest.mark.unit
    def test_parser_uses_table_metadata_not_schema(self):
        """Test that parsers work with TableMetadata (not old schema dict)."""
        import tempfile
        from pathlib import Path

        from acoharmony.medallion import MedallionLayer
        from acoharmony.parsers import parse_file

        from .conftest import create_mock_metadata

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col\nval\n")
            csv_path = Path(f.name)
        try:
            metadata = create_mock_metadata(
                "test",
                [{"name": "col", "output_name": "col", "data_type": "string"}],
                {"type": "csv"},
                medallion_layer=MedallionLayer.SILVER,
            )
            lf = parse_file(csv_path, metadata, add_tracking=True, schema_name="test")
            result = lf.collect()
            assert len(result) > 0
            assert hasattr(metadata, "medallion_layer")
            assert hasattr(metadata, "unity_catalog")
        finally:
            csv_path.unlink()
