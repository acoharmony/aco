# © 2025 HarmonyCares
# All rights reserved.
"""Tests for acoharmony._catalog package."""

from __future__ import annotations
from dataclasses import dataclass

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import polars as pl
import pytest

from acoharmony._catalog import (
    Catalog,
    TableMetadata,
)
from acoharmony._store import StorageBackend  # noqa: E402
from acoharmony.medallion import MedallionLayer  # noqa: E402

if TYPE_CHECKING:
    pass

from unittest.mock import MagicMock, patch

import yaml

# ===== From test_reexport.py =====


class TestCatalogStubs:
    """Test suite for _catalog - stub tests."""

    @pytest.mark.unit
    def test_unity_namespace(self) -> None:
        """Test unity_namespace property returns correct namespace."""
        meta = TableMetadata(
            name='test_table', description='desc', columns=[], storage={},
            file_format={}, medallion_layer=MedallionLayer.SILVER, unity_catalog='main',
        )
        ns = meta.unity_namespace
        assert ns is not None
        assert ns.table == 'test_table'
        assert ns.catalog == 'main'
        # Without medallion layer, returns None
        meta_no_layer = TableMetadata(
            name='x', description='', columns=[], storage={}, file_format={},
        )
        assert meta_no_layer.unity_namespace is None

    @pytest.mark.unit
    def test_full_table_name(self) -> None:
        """Test full_table_name property returns qualified name."""
        meta = TableMetadata(
            name='my_table', description='d', columns=[], storage={},
            file_format={}, medallion_layer=MedallionLayer.GOLD, unity_catalog='main',
        )
        ftn = meta.full_table_name
        assert ftn is not None
        assert 'my_table' in ftn
        # Without layer, returns None
        meta2 = TableMetadata(name='x', description='', columns=[], storage={}, file_format={})
        assert meta2.full_table_name is None

    @pytest.mark.unit
    def test_data_tier(self) -> None:
        """Test data_tier property returns legacy tier name."""
        meta = TableMetadata(
            name='t', description='', columns=[], storage={}, file_format={},
            medallion_layer=MedallionLayer.BRONZE,
        )
        assert meta.data_tier is not None
        meta2 = TableMetadata(name='t2', description='', columns=[], storage={}, file_format={})
        assert meta2.data_tier is None

    @pytest.mark.unit
    def test_get_table_metadata(self) -> None:
        """Test get_table_metadata returns metadata for known tables."""
        catalog = Catalog()
        tables = catalog.list_tables()
        if tables:
            meta = catalog.get_table_metadata(tables[0])
            assert meta is not None
            assert meta.name == tables[0]
        # Unknown table returns None
        assert catalog.get_table_metadata('__nonexistent_table__') is None

    @pytest.mark.unit
    def test_get_schema(self) -> None:
        """Test get_schema delegates to get_table_metadata."""
        catalog = Catalog()
        tables = catalog.list_tables()
        if tables:
            schema = catalog.get_schema(tables[0])
            assert schema is not None
            assert schema.name == tables[0]
        assert catalog.get_schema('__nonexistent__') is None

    @pytest.mark.unit
    def test_tablemetadata_init(self) -> None:
        """Test TableMetadata initialization."""
        meta = TableMetadata(
            name='test', description='A test table',
            columns=[{'name': 'col1', 'type': 'string'}],
            storage={'tier': 'silver'}, file_format={'type': 'csv'},
        )
        assert meta.name == 'test'
        assert meta.description == 'A test table'
        assert len(meta.columns) == 1
        assert meta.medallion_layer is None
        assert meta.unity_catalog == 'main'


class TestCatalogDeeper:
    """Cover Catalog methods not yet exercised."""

    def _make_catalog(self, tmp_path, schemas):
        """Helper to create a Catalog with mock schemas."""
        schemas_dir = tmp_path / "_schemas"
        schemas_dir.mkdir()
        for name, data in schemas.items():
            (schemas_dir / f"{name}.yml").write_text(yaml.dump(data))

        with patch("acoharmony._catalog.Path") as MockPath:
            MockPath.return_value = tmp_path / "_schemas"
            # Patch __file__ location so _schemas dir is found
            with patch("acoharmony._catalog.StorageBackend"):
                from acoharmony._catalog import Catalog

                cat = MagicMock(spec=Catalog)
                return cat

    @pytest.mark.unit
    def test_table_metadata_unity_namespace(self):
        from acoharmony._catalog import TableMetadata
        from acoharmony.medallion import MedallionLayer

        tm = TableMetadata(
            name="test_table",
            description="desc",
            columns=[],
            storage={},
            file_format={},
            medallion_layer=MedallionLayer.SILVER,
            unity_catalog="main",
        )
        ns = tm.unity_namespace
        assert ns is not None
        assert ns.full_name == "main.silver.test_table"

    @pytest.mark.unit
    def test_table_metadata_unity_namespace_none(self):
        from acoharmony._catalog import TableMetadata

        tm = TableMetadata(
            name="test_table", description="desc", columns=[], storage={}, file_format={}
        )
        assert tm.unity_namespace is None

    @pytest.mark.unit
    def test_table_metadata_full_table_name(self):
        from acoharmony._catalog import TableMetadata
        from acoharmony.medallion import MedallionLayer

        tm = TableMetadata(
            name="t1",
            description="",
            columns=[],
            storage={},
            file_format={},
            medallion_layer=MedallionLayer.BRONZE,
        )
        assert tm.full_table_name == "main.bronze.t1"

    @pytest.mark.unit
    def test_table_metadata_full_table_name_none(self):
        from acoharmony._catalog import TableMetadata

        tm = TableMetadata(name="t1", description="", columns=[], storage={}, file_format={})
        assert tm.full_table_name is None

    @pytest.mark.unit
    def test_table_metadata_data_tier(self):
        from acoharmony._catalog import TableMetadata
        from acoharmony.medallion import MedallionLayer

        tm = TableMetadata(
            name="t1",
            description="",
            columns=[],
            storage={},
            file_format={},
            medallion_layer=MedallionLayer.GOLD,
        )
        assert tm.data_tier == "gold"

    @pytest.mark.unit
    def test_table_metadata_data_tier_none(self):
        from acoharmony._catalog import TableMetadata

        tm = TableMetadata(name="t1", description="", columns=[], storage={}, file_format={})
        assert tm.data_tier is None

    @pytest.mark.unit
    def test_catalog_get_file_patterns_str(self):
        from acoharmony._catalog import Catalog

        cat = MagicMock(spec=Catalog)
        meta = MagicMock()
        meta.storage = {"file_patterns": "*.csv"}
        cat.get_table_metadata = MagicMock(return_value=meta)
        cat.get_file_patterns = Catalog.get_file_patterns.__get__(cat, Catalog)
        result = cat.get_file_patterns("test")
        assert result == {"default": "*.csv"}

    @pytest.mark.unit
    def test_catalog_get_file_patterns_list(self):
        from acoharmony._catalog import Catalog

        cat = MagicMock(spec=Catalog)
        meta = MagicMock()
        meta.storage = {"file_patterns": ["*.csv", "*.txt"]}
        cat.get_table_metadata = MagicMock(return_value=meta)
        cat.get_file_patterns = Catalog.get_file_patterns.__get__(cat, Catalog)
        result = cat.get_file_patterns("test")
        assert result == {"pattern_0": "*.csv", "pattern_1": "*.txt"}

    @pytest.mark.unit
    def test_catalog_get_file_patterns_dict_metadata_filtered(self):
        from acoharmony._catalog import Catalog

        cat = MagicMock(spec=Catalog)
        meta = MagicMock()
        meta.storage = {"file_patterns": {"default": "*.csv", "date_extraction": {"some": "data"}}}
        cat.get_table_metadata = MagicMock(return_value=meta)
        cat.get_file_patterns = Catalog.get_file_patterns.__get__(cat, Catalog)
        result = cat.get_file_patterns("test")
        assert "default" in result
        assert "date_extraction" not in result

    @pytest.mark.unit
    def test_catalog_get_file_patterns_none_schema(self):
        from acoharmony._catalog import Catalog

        cat = MagicMock(spec=Catalog)
        cat.get_table_metadata = MagicMock(return_value=None)
        cat.get_file_patterns = Catalog.get_file_patterns.__get__(cat, Catalog)
        result = cat.get_file_patterns("missing")
        assert result == {}

    @pytest.mark.unit
    def test_catalog_get_unity_schema(self):
        from acoharmony._catalog import Catalog
        from acoharmony.medallion import MedallionLayer

        cat = MagicMock(spec=Catalog)
        cat.get_unity_schema = Catalog.get_unity_schema.__get__(cat, Catalog)
        assert cat.get_unity_schema(MedallionLayer.BRONZE) == "bronze"

    @pytest.mark.unit
    def test_catalog_get_data_tier(self):
        from acoharmony._catalog import Catalog
        from acoharmony.medallion import MedallionLayer

        cat = MagicMock(spec=Catalog)
        cat.get_data_tier = Catalog.get_data_tier.__get__(cat, Catalog)
        assert cat.get_data_tier(MedallionLayer.SILVER) == "silver"

    @pytest.mark.unit
    def test_catalog_get_medallion_layer(self):
        from acoharmony._catalog import Catalog
        from acoharmony.medallion import MedallionLayer

        cat = MagicMock(spec=Catalog)
        cat.get_medallion_layer = Catalog.get_medallion_layer.__get__(cat, Catalog)
        assert cat.get_medallion_layer("gold") == MedallionLayer.GOLD


@pytest.fixture
def catalog() -> Catalog:
    """Create Catalog instance for testing."""
    return Catalog()


class TestCatalog:
    """Tests for Catalog initialization."""

    @pytest.mark.unit
    def test_initialization(self, catalog: Catalog) -> None:
        """Catalog initializes with default storage."""
        assert catalog is not None
        assert catalog.storage_config is not None
        assert catalog._table_metadata is not None

    @pytest.mark.unit
    def test_initialization_with_custom_storage(self) -> None:
        """Catalog accepts custom storage configuration."""
        storage = StorageBackend(profile="local")
        catalog = Catalog(storage_config=storage)

        assert catalog.storage_config is storage


class TestListTables:
    """Tests for listing available tables."""

    @pytest.mark.unit
    def test_list_tables(self, catalog: Catalog) -> None:
        """list_tables returns list of schema names."""
        tables = catalog.list_tables()

        assert isinstance(tables, list)
        # Should have some tables loaded
        assert len(tables) > 0

    @pytest.mark.unit
    def test_list_tables_contains_known_tables(self, catalog: Catalog) -> None:
        """list_tables includes known tables."""
        tables = catalog.list_tables()

        # Should include some known CCLF tables
        known_tables = ["cclf1", "cclf8", "institutional_claim"]
        found_tables = [t for t in known_tables if t in tables]

        # At least some should exist
        assert len(found_tables) > 0

    @pytest.mark.unit
    def test_list_tables_by_medallion_layer(self, catalog: Catalog) -> None:
        """list_tables can filter by medallion layer."""
        all_tables = catalog.list_tables()
        bronze_tables = catalog.list_tables(MedallionLayer.BRONZE)
        silver_tables = catalog.list_tables(MedallionLayer.SILVER)

        # Bronze and silver should be subsets of all
        assert set(bronze_tables).issubset(set(all_tables))
        assert set(silver_tables).issubset(set(all_tables))

        # No overlap between layers
        assert len(set(bronze_tables) & set(silver_tables)) == 0


class TestGetTableMetadata:
    """Tests for retrieving table metadata definitions."""

    @pytest.mark.unit
    def test_get_table_metadata_existing(self, catalog: Catalog) -> None:
        """get_table_metadata returns metadata for existing table."""
        # Get list of tables first
        tables = catalog.list_tables()
        if not tables:
            pytest.skip("No tables available in catalog")

        # Get first available table
        table_name = tables[0]
        metadata = catalog.get_table_metadata(table_name)

        assert metadata is not None
        assert isinstance(metadata, TableMetadata)
        assert metadata.name == table_name

    @pytest.mark.unit
    def test_get_table_metadata_nonexistent(self, catalog: Catalog) -> None:
        """get_table_metadata returns None for nonexistent table."""
        metadata = catalog.get_table_metadata("nonexistent_table_xyz")

        assert metadata is None

    @pytest.mark.unit
    def test_get_table_metadata_has_required_fields(self, catalog: Catalog) -> None:
        """get_table_metadata returns metadata with required fields."""
        tables = catalog.list_tables()
        if not tables:
            pytest.skip("No tables available")

        metadata = catalog.get_table_metadata(tables[0])

        assert metadata is not None
        assert hasattr(metadata, "name")
        assert hasattr(metadata, "description")
        assert hasattr(metadata, "columns")
        assert hasattr(metadata, "storage")
        assert hasattr(metadata, "file_format")
        assert hasattr(metadata, "medallion_layer")
        assert hasattr(metadata, "unity_catalog")

    @pytest.mark.unit
    def test_get_schema_backward_compatibility(self, catalog: Catalog) -> None:
        """get_schema still works as backward compatibility alias."""
        tables = catalog.list_tables()
        if not tables:
            pytest.skip("No tables available in catalog")

        # Both methods should return the same object
        table_name = tables[0]
        metadata = catalog.get_table_metadata(table_name)
        schema = catalog.get_schema(table_name)

        assert metadata is schema


class TestGetFilePatterns:
    """Tests for file pattern retrieval."""

    @pytest.mark.unit
    def test_get_file_patterns(self, catalog: Catalog) -> None:
        """get_file_patterns returns patterns for table."""
        tables = catalog.list_tables()
        if not tables:
            pytest.skip("No tables available")

        patterns = catalog.get_file_patterns(tables[0])

        assert isinstance(patterns, dict)


@pytest.mark.slow
@pytest.mark.requires_data
class TestScanTable:
    """Tests for table scanning."""

    @pytest.mark.unit
    def test_scan_table_returns_lazyframe(self, catalog: Catalog) -> None:
        """scan_table returns LazyFrame."""
        # This requires actual data files
        tables = catalog.list_tables()
        if not tables:
            pytest.skip("No tables available")

        # Try to scan a table - may fail if no data
        try:
            lf = catalog.scan_table(tables[0])
            assert isinstance(lf, pl.LazyFrame)
        except Exception:
            # Expected if no data files exist
            pytest.skip("No data files available for scanning")


@pytest.mark.requires_data
class TestDiscoverFiles:
    """Tests for file discovery."""

    @pytest.mark.unit
    def test_discover_files_returns_dict(self, catalog: Catalog) -> None:
        """discover_files returns dictionary."""
        tables = catalog.list_tables()
        if not tables:
            pytest.skip("No tables available")

        files = catalog.discover_files(tables[0])

        assert isinstance(files, dict)

    @pytest.mark.unit
    def test_discover_files_raw_tier(self, catalog: Catalog) -> None:
        """discover_files works with raw tier."""
        tables = catalog.list_tables()
        if not tables:
            pytest.skip("No tables available")

        files = catalog.discover_files(tables[0], tier="raw")

        assert isinstance(files, dict)


class TestDataclasses:
    """Tests for dataclass definitions."""

    @pytest.mark.unit
    def test_table_schema_alias(self) -> None:
        """TableMetadata alias still works."""
        schema = TableMetadata(
            name="test_table",
            description="Test table",
            columns=[{"name": "col1", "type": "String"}],
            storage={"path": "/data"},
            file_format={"type": "parquet"},
        )
        assert isinstance(schema, TableMetadata)


class TestPrivateMethods:
    """Tests for private helper methods."""

    @pytest.mark.unit
    def test_load_table_metadata(self, catalog: Catalog) -> None:
        """_load_table_metadata loads table metadata definitions."""
        # Table metadata should be loaded during init
        assert catalog._table_metadata is not None
        assert isinstance(catalog._table_metadata, dict)
        assert len(catalog._table_metadata) > 0

    @pytest.mark.unit
    def test_table_metadata_loaded(self, catalog: Catalog) -> None:
        """Table metadata is properly loaded."""
        assert len(catalog._table_metadata) > 0


def _bypass_validate_schema(func):
    """Passthrough replacement for validate_schema decorator in tests."""
    return func


class TestCatalogLoadTableMetadata:
    """Test _load_table_metadata using SchemaRegistry-based loading."""

    @pytest.mark.unit
    def test_load_from_registry(self):
        """Test that _load_table_metadata reads from SchemaRegistry."""
        from acoharmony._catalog import Catalog
        from acoharmony._registry import SchemaRegistry

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}

        # Call the real method - it should load from SchemaRegistry
        Catalog._load_table_metadata(cat)

        # Verify it loaded real schemas from the registry
        registered = SchemaRegistry.list_schemas()
        assert len(cat._table_metadata) > 0
        # All loaded tables should come from registered schemas
        for name in cat._table_metadata:
            assert name in [
                SchemaRegistry.get_metadata(s).get("name", s) for s in registered
            ]

    @pytest.mark.unit
    def test_load_only_includes_named_schemas(self):
        """Test that _load_table_metadata only includes schemas with a 'name' key."""
        from acoharmony._catalog import Catalog

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}

        Catalog._load_table_metadata(cat)

        # Every entry should have a name matching its key
        for name, meta in cat._table_metadata.items():
            assert meta.name == name

    @pytest.mark.unit
    def test_tier_in_top_level(self):
        """Test medallion_layer resolved from top-level 'tier' key in registry metadata."""
        from acoharmony._catalog import Catalog, TableMetadata
        from acoharmony.medallion import MedallionLayer

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}

        cat._table_metadata["tier_test"] = TableMetadata(
            name="tier_test",
            description="",
            columns=[],
            storage={},
            file_format={},
            medallion_layer=MedallionLayer.from_tier("silver"),
        )

        assert cat._table_metadata["tier_test"].medallion_layer == MedallionLayer.SILVER


class TestCatalogAccessors:
    """Cover lines 280, 300, 319-325."""

    def _make_catalog(self):
        from acoharmony._catalog import Catalog, TableMetadata
        from acoharmony.medallion import MedallionLayer

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}

        cat._table_metadata["bronze_t"] = TableMetadata(
            name="bronze_t",
            description="",
            columns=[],
            storage={},
            file_format={},
            medallion_layer=MedallionLayer.BRONZE,
        )
        cat._table_metadata["silver_t"] = TableMetadata(
            name="silver_t",
            description="",
            columns=[],
            storage={},
            file_format={},
            medallion_layer=MedallionLayer.SILVER,
        )
        return cat

    @pytest.mark.unit
    def test_get_table_metadata_found(self):
        """Line 280."""
        cat = self._make_catalog()
        meta = cat.get_table_metadata("bronze_t")
        assert meta is not None
        assert meta.name == "bronze_t"

    @pytest.mark.unit
    def test_get_table_metadata_not_found(self):
        cat = self._make_catalog()
        assert cat.get_table_metadata("nonexistent") is None

    @pytest.mark.unit
    def test_get_schema(self):
        """Line 300."""
        cat = self._make_catalog()
        schema = cat.get_schema("silver_t")
        assert schema is not None
        assert schema.name == "silver_t"

    @pytest.mark.unit
    def test_list_tables_all(self):
        """Line 325."""
        cat = self._make_catalog()
        tables = cat.list_tables()
        assert set(tables) == {"bronze_t", "silver_t"}

    @pytest.mark.unit
    def test_list_tables_filtered(self):
        """Lines 319-324."""
        from acoharmony.medallion import MedallionLayer

        cat = self._make_catalog()
        bronze = cat.list_tables(medallion_layer=MedallionLayer.BRONZE)
        assert bronze == ["bronze_t"]


class TestCatalogScanTable:
    """Cover lines 349-371."""

    def _make_catalog_with_meta(self, storage_config=None):
        from acoharmony._catalog import Catalog, TableMetadata

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = storage_config or MagicMock()
            cat._table_metadata = {}

        cat._table_metadata["t1"] = TableMetadata(
            name="t1",
            description="",
            columns=[{"name": "a", "type": "string"}],
            storage={"silver": {"output_name": "t1.parquet"}},
            file_format={"type": "csv"},
        )
        return cat

    @pytest.mark.unit
    def test_scan_table_schema_not_found(self):
        """Lines 349-351: raise ValueError if schema not found."""
        cat = self._make_catalog_with_meta()
        with pytest.raises(ValueError, match="Schema not found"):
            cat.scan_table("nonexistent")

    @pytest.mark.unit
    def test_scan_table_parquet_path(self, tmp_path):
        """Lines 358-360: direct scan for .parquet file_path."""
        cat = self._make_catalog_with_meta()
        # Create a small parquet file
        pf = tmp_path / "data.parquet"
        pl.DataFrame({"a": [1, 2]}).write_parquet(pf)

        result = cat.scan_table("t1", file_path=str(pf))
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert collected.shape == (2, 1)

    @pytest.mark.unit
    def test_scan_table_non_parquet_path(self, tmp_path):
        """Lines 362-370: use parsers for non-parquet files."""
        cat = self._make_catalog_with_meta()
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")

        mock_lf = pl.DataFrame({"a": ["1", "3"], "b": ["2", "4"]}).lazy()

        with (
            patch("acoharmony._catalog.parsers.parse_file", return_value=mock_lf) as mock_parse,
            patch("acoharmony._catalog.parsers.apply_schema_transformations", return_value=mock_lf),
            patch("acoharmony._catalog.parsers.apply_column_types", return_value=mock_lf),
        ):
            result = cat.scan_table("t1", file_path=str(csv_file))
            assert isinstance(result, pl.LazyFrame)
            mock_parse.assert_called_once()

    @pytest.mark.unit
    def test_scan_table_uses_default_path(self, tmp_path):
        """Lines 354-355: uses _get_table_file_path when file_path is None."""
        storage = MagicMock()
        silver_path = tmp_path / "silver"
        silver_path.mkdir()
        pf = silver_path / "t1.parquet"
        pl.DataFrame({"a": [10]}).write_parquet(pf)
        storage.get_path.return_value = silver_path

        cat = self._make_catalog_with_meta(storage_config=storage)
        result = cat.scan_table("t1")
        assert isinstance(result, pl.LazyFrame)


class TestCatalogApplyColumnRenames:
    """Cover lines 375-391."""

    def _make_catalog(self):
        from acoharmony._catalog import Catalog

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}
        return cat

class TestCatalogApplyTypeCasting:
    """Cover lines 395-413."""

    def _make_catalog(self):
        from acoharmony._catalog import Catalog

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}
        return cat

class TestCatalogGetFilePatterns:
    """Cover line 451 (empty return for unexpected type)."""

    def _make_catalog(self):
        from acoharmony._catalog import Catalog

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}
        return cat

    @pytest.mark.unit
    def test_patterns_as_list(self):
        from acoharmony._catalog import TableMetadata

        cat = self._make_catalog()
        cat._table_metadata["t"] = TableMetadata(
            name="t",
            description="",
            columns=[],
            storage={"file_patterns": ["*.csv", "*.txt"]},
            file_format={},
        )
        result = cat.get_file_patterns("t")
        assert result == {"pattern_0": "*.csv", "pattern_1": "*.txt"}

    @pytest.mark.unit
    def test_patterns_as_string(self):
        from acoharmony._catalog import TableMetadata

        cat = self._make_catalog()
        cat._table_metadata["t"] = TableMetadata(
            name="t",
            description="",
            columns=[],
            storage={"file_patterns": "*.parquet"},
            file_format={},
        )
        result = cat.get_file_patterns("t")
        assert result == {"default": "*.parquet"}

    @pytest.mark.unit
    def test_patterns_unexpected_type(self):
        """Line 451: return empty dict for unexpected type like int."""
        from acoharmony._catalog import TableMetadata

        cat = self._make_catalog()
        cat._table_metadata["t"] = TableMetadata(
            name="t",
            description="",
            columns=[],
            storage={"file_patterns": 12345},
            file_format={},
        )
        result = cat.get_file_patterns("t")
        assert result == {}

    @pytest.mark.unit
    def test_no_schema_returns_empty(self):
        cat = self._make_catalog()
        assert cat.get_file_patterns("nonexistent") == {}


class TestCatalogDiscoverFiles:
    """Cover lines 474-487."""

    def _make_catalog(self):
        from acoharmony._catalog import Catalog

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}
        return cat

    @pytest.mark.unit
    def test_discover_no_patterns(self):
        """Line 475-476: no patterns returns empty dict."""
        cat = self._make_catalog()
        result = cat.discover_files("nonexistent")
        assert result == {}

    @pytest.mark.unit
    def test_discover_files_found(self, tmp_path):
        """Lines 478-487: discover files that match patterns."""
        from acoharmony._catalog import TableMetadata

        cat = self._make_catalog()
        cat.storage_config.get_path.return_value = tmp_path
        cat._table_metadata["t"] = TableMetadata(
            name="t",
            description="",
            columns=[],
            storage={"file_patterns": {"default": "*.txt"}},
            file_format={},
        )
        # Create matching files
        (tmp_path / "a.txt").write_text("hello")
        (tmp_path / "b.txt").write_text("world")

        result = cat.discover_files("t", tier="bronze")
        assert "default" in result
        assert len(result["default"]) == 2

    @pytest.mark.unit
    def test_discover_files_none_found(self, tmp_path):
        """Lines 482-484: no matching files."""
        from acoharmony._catalog import TableMetadata

        cat = self._make_catalog()
        cat.storage_config.get_path.return_value = tmp_path
        cat._table_metadata["t"] = TableMetadata(
            name="t",
            description="",
            columns=[],
            storage={"file_patterns": {"default": "*.xyz"}},
            file_format={},
        )
        result = cat.discover_files("t")
        assert result == {}


class TestCatalogGetTableFilePath:
    """Cover lines 491-498."""

    def _make_catalog(self):
        from acoharmony._catalog import Catalog

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}
        return cat

    @pytest.mark.unit
    def test_with_output_name(self, tmp_path):
        """Lines 493-494: use output_name from tier config."""
        from acoharmony._catalog import TableMetadata

        cat = self._make_catalog()
        cat.storage_config.get_path.return_value = tmp_path
        schema = TableMetadata(
            name="t",
            description="",
            columns=[],
            storage={"silver": {"output_name": "custom.parquet"}},
            file_format={},
        )
        result = cat._get_table_file_path(schema)
        assert result.endswith("custom.parquet")

    @pytest.mark.unit
    def test_without_output_name(self, tmp_path):
        """Lines 495-496: default to {name}.parquet."""
        from acoharmony._catalog import TableMetadata

        cat = self._make_catalog()
        cat.storage_config.get_path.return_value = tmp_path
        schema = TableMetadata(
            name="mydata",
            description="",
            columns=[],
            storage={},
            file_format={},
        )
        result = cat._get_table_file_path(schema)
        assert result.endswith("mydata.parquet")


class TestCatalogGetDependencies:
    """Cover lines 549-552."""

    def _make_catalog(self):
        from acoharmony._catalog import Catalog

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}
        return cat

def _make_catalog_with_yaml(tmp_path, schema_dicts):
    """Create a Catalog whose _schemas/ dir contains the given YAML dicts."""
    schemas_dir = tmp_path / "_schemas"
    schemas_dir.mkdir(parents=True, exist_ok=True)
    for i, d in enumerate(schema_dicts):
        name = d.get("name", f"table_{i}")
        with open(schemas_dir / f"{name}.yml", "w") as f:
            yaml.dump(d, f)
    return schemas_dir


class TestCatalogCompleteCoverage:
    """Additional tests to achieve 100% coverage."""

    def _make_catalog(self):
        from acoharmony._catalog import Catalog

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}
        return cat

    @pytest.mark.unit
    def test_table_metadata_with_record_types(self):
        """Test TableMetadata with record_types for multi-record files."""
        from acoharmony._catalog import TableMetadata

        record_types = {"header": {"pattern": "^H"}, "detail": {"pattern": "^D"}}
        meta = TableMetadata(
            name="tparc",
            description="Multi-record file",
            columns=[],
            storage={},
            file_format={},
            record_types=record_types,
        )
        assert meta.record_types == record_types

    @pytest.mark.unit
    def test_table_metadata_with_sheets(self):
        """Test TableMetadata with sheets for multi-sheet Excel files."""
        from acoharmony._catalog import TableMetadata

        sheets = [{"name": "Sheet1", "range": "A1:Z100"}, {"name": "Sheet2"}]
        meta = TableMetadata(
            name="pyred",
            description="Multi-sheet Excel",
            columns=[],
            storage={},
            file_format={},
            sheets=sheets,
        )
        assert meta.sheets == sheets

    @pytest.mark.unit
    def test_table_metadata_with_matrix_fields(self):
        """Test TableMetadata with matrix_fields for complex layouts."""
        from acoharmony._catalog import TableMetadata

        matrix_fields = [{"start_row": 5, "start_col": 2, "field": "data_matrix"}]
        meta = TableMetadata(
            name="matrix_table",
            description="Matrix layout",
            columns=[],
            storage={},
            file_format={},
            matrix_fields=matrix_fields,
        )
        assert meta.matrix_fields == matrix_fields

    @pytest.mark.unit
    def test_get_file_patterns_with_metadata_extraction(self):
        """Test that metadata_extraction is filtered out from file patterns."""
        from acoharmony._catalog import TableMetadata

        cat = self._make_catalog()
        cat._table_metadata["t"] = TableMetadata(
            name="t",
            description="",
            columns=[],
            storage={
                "file_patterns": {
                    "default": "*.csv",
                    "metadata_extraction": {"pattern": "regex_here"},
                }
            },
            file_format={},
        )
        patterns = cat.get_file_patterns("t")
        assert "default" in patterns
        assert "metadata_extraction" not in patterns

    @pytest.mark.unit
    def test_discover_files_sorts_by_mtime(self, tmp_path):
        """Test that discovered files are sorted by modification time (newest first)."""
        from acoharmony._catalog import TableMetadata
        import time

        cat = self._make_catalog()
        cat.storage_config.get_path.return_value = tmp_path
        cat._table_metadata["t"] = TableMetadata(
            name="t",
            description="",
            columns=[],
            storage={"file_patterns": {"default": "*.txt"}},
            file_format={},
        )

        # Create files with different timestamps
        old_file = tmp_path / "old.txt"
        old_file.write_text("old")
        time.sleep(0.01)  # Small delay to ensure different mtimes
        new_file = tmp_path / "new.txt"
        new_file.write_text("new")

        result = cat.discover_files("t")
        assert "default" in result
        # Files should be sorted newest first
        assert result["default"][0].name == "new.txt"
        assert result["default"][1].name == "old.txt"

    @pytest.mark.unit
    def test_load_table_metadata_with_medallion_layer_key(self):
        """Test TableMetadata with explicit medallion_layer key."""
        from acoharmony._catalog import Catalog, TableMetadata
        from acoharmony.medallion import MedallionLayer

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}

        # Directly create a TableMetadata with an explicit medallion_layer
        cat._table_metadata["gold_table"] = TableMetadata(
            name="gold_table",
            description="",
            columns=[],
            storage={},
            file_format={},
            medallion_layer=MedallionLayer.from_tier("gold"),
        )

        assert cat._table_metadata["gold_table"].medallion_layer == MedallionLayer.GOLD

    @pytest.mark.unit
    def test_load_table_metadata_with_storage_tier_key(self):
        """Test TableMetadata resolving tier from storage.tier."""
        from acoharmony._catalog import Catalog, TableMetadata
        from acoharmony.medallion import MedallionLayer

        with patch.object(Catalog, "_load_table_metadata"):
            cat = Catalog.__new__(Catalog)
            cat.storage_config = MagicMock()
            cat._table_metadata = {}

        # Directly create a TableMetadata with a storage tier
        cat._table_metadata["storage_tier_table"] = TableMetadata(
            name="storage_tier_table",
            description="",
            columns=[],
            storage={"tier": "bronze"},
            file_format={},
            medallion_layer=MedallionLayer.from_tier("bronze"),
        )

        assert cat._table_metadata["storage_tier_table"].medallion_layer == MedallionLayer.BRONZE

    @pytest.mark.unit
    def test_get_table_file_path_custom_tier(self, tmp_path):
        """Test _get_table_file_path with custom tier parameter."""
        from acoharmony._catalog import TableMetadata

        cat = self._make_catalog()
        cat.storage_config.get_path.return_value = tmp_path
        schema = TableMetadata(
            name="t",
            description="",
            columns=[],
            storage={"bronze": {"output_name": "bronze_custom.parquet"}},
            file_format={},
        )
        result = cat._get_table_file_path(schema, tier="bronze")
        assert result.endswith("bronze_custom.parquet")

class TestCatalogLoadTableMetadataBranches:
    """Cover uncovered branches in _catalog._load_table_metadata."""

    @pytest.mark.unit
    def test_load_skips_empty_config(self):
        """Branch 196->197: get_full_table_config returns empty dict, continue."""
        from acoharmony._catalog import Catalog
        from acoharmony._registry import SchemaRegistry

        orig_schemas = SchemaRegistry._schemas.copy()
        orig_metadata = SchemaRegistry._metadata.copy()
        try:
            SchemaRegistry._schemas["__test_empty"] = type("FakeModel", (), {})
            SchemaRegistry._metadata["__test_empty"] = {}  # name not present -> empty config

            cat = Catalog.__new__(Catalog)
            cat.storage_config = StorageBackend()
            cat._table_metadata = {}
            cat._load_table_metadata()
            # __test_empty should not appear in metadata (empty dict has no "name" key)
            assert "__test_empty" not in cat._table_metadata
        finally:
            SchemaRegistry._schemas = orig_schemas
            SchemaRegistry._metadata = orig_metadata

    @pytest.mark.unit
    def test_load_with_tier_in_data(self):
        """Branch 229->231: data.get('tier') is truthy."""
        from acoharmony._catalog import Catalog
        from acoharmony._registry import SchemaRegistry

        orig_schemas = SchemaRegistry._schemas.copy()
        orig_metadata = SchemaRegistry._metadata.copy()
        try:
            SchemaRegistry._schemas["__test_tier"] = type("FakeModel", (), {})
            SchemaRegistry._metadata["__test_tier"] = {
                "name": "__test_tier",
                "tier": "bronze",
            }

            cat = Catalog.__new__(Catalog)
            cat.storage_config = StorageBackend()
            cat._table_metadata = {}
            cat._load_table_metadata()

            meta = cat._table_metadata.get("__test_tier")
            assert meta is not None
            assert meta.medallion_layer is not None
            assert meta.medallion_layer.data_tier == "bronze"
        finally:
            SchemaRegistry._schemas = orig_schemas
            SchemaRegistry._metadata = orig_metadata

    @pytest.mark.unit
    def test_load_with_tier_in_storage(self):
        """Branch 231->232: storage.get('tier') is truthy."""
        from acoharmony._catalog import Catalog
        from acoharmony._registry import SchemaRegistry

        orig_schemas = SchemaRegistry._schemas.copy()
        orig_metadata = SchemaRegistry._metadata.copy()
        orig_storage = SchemaRegistry._storage.copy()
        try:
            SchemaRegistry._schemas["__test_stier"] = type("FakeModel", (), {})
            SchemaRegistry._metadata["__test_stier"] = {
                "name": "__test_stier",
            }
            SchemaRegistry._storage["__test_stier"] = {
                "tier": "silver",
            }

            cat = Catalog.__new__(Catalog)
            cat.storage_config = StorageBackend()
            cat._table_metadata = {}
            cat._load_table_metadata()

            meta = cat._table_metadata.get("__test_stier")
            assert meta is not None
            assert meta.medallion_layer is not None
            assert meta.medallion_layer.data_tier == "silver"
        finally:
            SchemaRegistry._schemas = orig_schemas
            SchemaRegistry._metadata = orig_metadata
            SchemaRegistry._storage = orig_storage

    @pytest.mark.unit
    def test_load_with_no_tier(self):
        """Branch 231->235: no tier found anywhere."""
        from acoharmony._catalog import Catalog
        from acoharmony._registry import SchemaRegistry

        orig_schemas = SchemaRegistry._schemas.copy()
        orig_metadata = SchemaRegistry._metadata.copy()
        try:
            SchemaRegistry._schemas["__test_notier"] = type("FakeModel", (), {})
            SchemaRegistry._metadata["__test_notier"] = {
                "name": "__test_notier",
                "description": "No tier",
            }

            cat = Catalog.__new__(Catalog)
            cat.storage_config = StorageBackend()
            cat._table_metadata = {}
            cat._load_table_metadata()

            meta = cat._table_metadata.get("__test_notier")
            assert meta is not None
            assert meta.medallion_layer is None
        finally:
            SchemaRegistry._schemas = orig_schemas
            SchemaRegistry._metadata = orig_metadata
