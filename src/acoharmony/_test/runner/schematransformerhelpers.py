"""
Tests to boost coverage for:
- src/acoharmony/_catalog.py (lines 200, 204, 212-224, 239-240, 280, 300, 319-325,
  349-371, 375-391, 395-413, 451, 474-487, 491-498, 549-552)
- src/acoharmony/_runner/_schema_transformer.py (lines 79-141, 169-258)
- src/acoharmony/_runner/_file_processor.py (lines 79-144, 244-254, 271-321)
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import yaml


def _make_catalog_with_yaml(tmp_path, schema_dicts):
    """Create a Catalog whose _schemas/ dir contains the given YAML dicts."""
    schemas_dir = tmp_path / "_schemas"
    schemas_dir.mkdir(parents=True, exist_ok=True)
    for i, d in enumerate(schema_dicts):
        name = d.get("name", f"table_{i}")
        with open(schemas_dir / f"{name}.yml", "w") as f:
            yaml.dump(d, f)
    return schemas_dir


# ============================================================================
# Catalog – _load_table_metadata
# ============================================================================

def _bypass_validate_schema(func):
    """Passthrough replacement for validate_schema decorator in tests."""
    return func


class TestSchemaTransformer:
    """Cover lines 79-141 of _schema_transformer.py."""

    def _make_transformer(self, tmp_path):
        # Patch validate_schema before importing SchemaTransformer
        # to avoid it checking real catalog for schema existence
        with patch("acoharmony._decor8.validation.validate_schema", return_value=_bypass_validate_schema):
            # Force re-import with patched decorator
            import importlib

            import acoharmony._runner._schema_transformer as st_mod
            importlib.reload(st_mod)
            SchemaTransformer = st_mod.SchemaTransformer

        storage = MagicMock()
        silver_path = tmp_path / "silver"
        silver_path.mkdir(parents=True, exist_ok=True)
        bronze_path = tmp_path / "bronze"
        bronze_path.mkdir(parents=True, exist_ok=True)

        def get_path(tier):
            if tier == "silver":
                return silver_path
            return bronze_path

        storage.get_path.side_effect = get_path

        catalog = MagicMock()
        logger = MagicMock()

        transformer = SchemaTransformer(storage, catalog, logger)
        return transformer, catalog, storage, logger

    @pytest.mark.unit
    def test_transform_schema_not_found(self, tmp_path):
        """Line 84: returns error when schema not found."""
        transformer, catalog, _, _ = self._make_transformer(tmp_path)
        catalog.get_table_metadata.return_value = None

        tracker = MagicMock()
        df = pl.DataFrame({"a": [1, 2]}).lazy()

        result = transformer.transform_schema("missing", df, tracker)
        assert result.failed
        assert "not found" in result.message

    @pytest.mark.unit
    def test_transform_schema_success(self, tmp_path):
        """Lines 79-141: full successful transform path."""
        transformer, catalog, storage, logger = self._make_transformer(tmp_path)

        # Set up schema metadata
        schema_meta = MagicMock()
        schema_meta.name = "test_schema"
        catalog.get_table_metadata.return_value = schema_meta

        # Create tracker
        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.metadata = {"_pending_files": ["/path/to/file1"]}

        # Create test data
        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()

        # Mock the _transforms module so transform lookup works but finds nothing
        with patch.dict("sys.modules", {"acoharmony._transforms": MagicMock(spec=[])}):
            result = transformer.transform_schema("test_schema", df, tracker)

        assert result.success
        assert result.records_processed > 0

        # Verify tracker interactions
        tracker.start_transform.assert_called_once()
        tracker.complete_transform.assert_called_once()
        # Verify pending files were tracked
        tracker.track_file.assert_called_once_with("/path/to/file1", "processed")

    @pytest.mark.unit
    def test_transform_schema_no_tracking(self, tmp_path):
        """Lines 90, 126-133, 136-139: no_tracking=True skips tracker calls."""
        transformer, catalog, _, _ = self._make_transformer(tmp_path)

        schema_meta = MagicMock()
        schema_meta.name = "test_schema"
        catalog.get_table_metadata.return_value = schema_meta

        tracker = MagicMock()
        df = pl.DataFrame({"a": [1]}).lazy()

        with patch.dict("sys.modules", {"acoharmony._transforms": MagicMock(spec=[])}):
            result = transformer.transform_schema("test_schema", df, tracker, no_tracking=True)

        assert result.success
        tracker.start_transform.assert_not_called()
        tracker.complete_transform.assert_not_called()

    @pytest.mark.unit
    def test_transform_schema_with_transform_module(self, tmp_path):
        """Lines 98-109: apply_transform from a transform module."""
        transformer, catalog, _, _ = self._make_transformer(tmp_path)

        schema_meta = MagicMock()
        schema_meta.name = "myschema"
        catalog.get_table_metadata.return_value = schema_meta

        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.metadata = {}
        df = pl.DataFrame({"a": [1, 2]}).lazy()

        # Create mock transform module with apply_transform
        mock_transform_mod = MagicMock()
        transformed_df = pl.DataFrame({"a": [10, 20]}).lazy()
        mock_transform_mod.apply_transform.return_value = transformed_df

        # Use SimpleNamespace to allow hasattr checks
        mock_transforms = SimpleNamespace(_myschema=mock_transform_mod)

        with patch.dict("sys.modules", {"acoharmony._transforms": mock_transforms}):
            result = transformer.transform_schema("myschema", df, tracker)

        assert result.success

    @pytest.mark.unit
    def test_transform_schema_row_count_fails(self, tmp_path):
        """Lines 121-123: warning when row count collection fails."""
        transformer, catalog, _, logger = self._make_transformer(tmp_path)

        schema_meta = MagicMock()
        schema_meta.name = "test_schema"
        catalog.get_table_metadata.return_value = schema_meta

        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.metadata = {}

        # Create a lazy frame that will fail on select(pl.len()).collect()
        df = MagicMock(spec=pl.LazyFrame)
        # head(1).collect().height for check_not_empty
        mock_head = MagicMock()
        mock_head.collect.return_value = MagicMock(height=1)
        df.head.return_value = mock_head
        # select raises exception
        df.select.side_effect = Exception("row count fail")
        # sink_parquet for _write_output
        df.sink_parquet = MagicMock()

        with patch.dict("sys.modules", {"acoharmony._transforms": MagicMock(spec=[])}):
            result = transformer.transform_schema("test_schema", df, tracker)

        assert result.success
        assert result.records_processed == 0


# ============================================================================
# SchemaTransformer – _write_output
# ============================================================================


class TestSchemaTransformerWriteOutput:
    """Cover missing lines in _write_output (169-258)."""

    def _make_transformer(self, tmp_path):
        with patch("acoharmony._decor8.validation.validate_schema", return_value=_bypass_validate_schema):
            import importlib

            import acoharmony._runner._schema_transformer as st_mod


            importlib.reload(st_mod)
            SchemaTransformer = st_mod.SchemaTransformer

        storage = MagicMock()
        silver_path = tmp_path / "silver"
        silver_path.mkdir(parents=True, exist_ok=True)
        bronze_path = tmp_path / "bronze"
        bronze_path.mkdir(parents=True, exist_ok=True)

        def get_path(tier):
            if tier == "silver":
                return silver_path
            if tier == "bronze":
                return bronze_path
            return tmp_path

        storage.get_path.side_effect = get_path

        catalog = MagicMock()
        logger = MagicMock()
        transformer = SchemaTransformer(storage, catalog, logger)
        return transformer, silver_path, bronze_path, logger

    @pytest.mark.unit
    def test_write_output_new_file(self, tmp_path):
        """Write output to new file."""
        transformer, silver_path, _, logger = self._make_transformer(tmp_path)
        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        output = transformer._write_output(df, "test_table")
        assert output.exists()
        result = pl.read_parquet(output)
        assert result.height == 3

    @pytest.mark.unit
    def test_write_output_force_overwrite(self, tmp_path):
        """Force mode overwrites existing file."""
        transformer, silver_path, _, _ = self._make_transformer(tmp_path)
        # Create initial file
        initial = pl.DataFrame({"a": [1, 2]})
        initial.write_parquet(silver_path / "test_table.parquet")

        df = pl.DataFrame({"a": [10, 20, 30]}).lazy()
        output = transformer._write_output(df, "test_table", force=True)
        result = pl.read_parquet(output)
        assert result.height == 3

    @pytest.mark.unit
    def test_write_output_append_to_existing(self, tmp_path):
        """Appends new data to existing file."""
        transformer, silver_path, _, _ = self._make_transformer(tmp_path)
        # Create initial file
        initial = pl.DataFrame({"a": [1, 2]})
        initial.write_parquet(silver_path / "test_table.parquet")

        df = pl.DataFrame({"a": [3, 4]}).lazy()
        output = transformer._write_output(df, "test_table", force=False)
        result = pl.read_parquet(output)
        assert result.height == 4  # 2 original + 2 new

    @pytest.mark.unit
    def test_write_output_empty_file_overwritten(self, tmp_path):
        """Empty existing file is overwritten."""
        transformer, silver_path, _, _ = self._make_transformer(tmp_path)
        # Create empty file
        (silver_path / "test_table.parquet").write_bytes(b"")

        df = pl.DataFrame({"a": [1]}).lazy()
        output = transformer._write_output(df, "test_table")
        result = pl.read_parquet(output)
        assert result.height == 1

    @pytest.mark.unit
    def test_write_output_corrupted_file_overwritten(self, tmp_path):
        """Corrupted existing file (too small) is overwritten."""
        transformer, silver_path, _, _ = self._make_transformer(tmp_path)
        # Create tiny invalid file
        (silver_path / "test_table.parquet").write_bytes(b"x" * 50)

        df = pl.DataFrame({"a": [1]}).lazy()
        output = transformer._write_output(df, "test_table")
        result = pl.read_parquet(output)
        assert result.height == 1

    @pytest.mark.unit
    def test_write_output_with_chunk_size(self, tmp_path):
        """Write output with chunk_size parameter."""
        transformer, silver_path, _, _ = self._make_transformer(tmp_path)
        df = pl.DataFrame({"a": list(range(100))}).lazy()
        output = transformer._write_output(df, "test_table", chunk_size=50)
        result = pl.read_parquet(output)
        assert result.height == 100

    @pytest.mark.unit
    def test_is_processed(self, tmp_path):
        """_is_processed returns True when output file exists."""
        transformer, silver_path, _, _ = self._make_transformer(tmp_path)
        (silver_path / "existing.parquet").write_bytes(b"x" * 200)
        assert transformer._is_processed("existing") is True
        assert transformer._is_processed("nonexistent") is False
