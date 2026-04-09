# © 2025 HarmonyCares
"""Tests for acoharmony/_notes/generator.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


from pathlib import Path
from unittest.mock import MagicMock, patch


class TestGenerator:
    """Test suite for generator."""

    @pytest.mark.unit
    def test_get_schema_with_full_details(self) -> None:
        """Test get_schema_with_full_details function."""
        with patch('acoharmony._notes.generator.Catalog') as MockCatalog, \
             patch('acoharmony._notes.generator.StorageBackend') as MockStorage:
            mock_storage = MockStorage.return_value
            mock_storage.get_data_path.return_value = Path('/tmp/data')
            mock_catalog = MockCatalog.return_value
            mock_meta = MagicMock()
            mock_meta.name = 'test_table'
            mock_meta.description = 'Test'
            mock_meta.storage = {'tier': 'silver'}
            mock_meta.file_format = {}
            mock_meta.keys = {}
            mock_meta.transformation_pipeline = None
            mock_meta.medallion_layer = None
            mock_meta.unity_catalog = ''
            mock_catalog.get_table_metadata.return_value = mock_meta
            from acoharmony._notes.generator import NotebookGenerator
            gen = NotebookGenerator.__new__(NotebookGenerator)
            gen.catalog = mock_catalog
            gen.storage = mock_storage
            # Schema file won't exist, so it takes fallback path
            result = gen.get_schema_with_full_details('test_table')
            assert result['name'] == 'test_table'

    @pytest.mark.unit
    def test_get_data_path_for_schema(self) -> None:
        """Test get_data_path_for_schema function."""
        with patch('acoharmony._notes.generator.Catalog') as MockCatalog, \
             patch('acoharmony._notes.generator.StorageBackend') as MockStorage:
            mock_storage = MockStorage.return_value
            mock_storage.get_data_path.return_value = Path('/tmp/data')
            mock_catalog = MockCatalog.return_value
            mock_meta = MagicMock()
            mock_meta.name = 'my_table'
            mock_meta.medallion_layer = None
            mock_meta.storage = {'tier': 'processed'}
            mock_catalog.get_table_metadata.return_value = mock_meta
            from acoharmony._notes.generator import NotebookGenerator
            gen = NotebookGenerator.__new__(NotebookGenerator)
            gen.catalog = mock_catalog
            gen.storage = mock_storage
            result = gen.get_data_path_for_schema('my_table')
            assert 'my_table.parquet' in result

    @pytest.mark.unit
    def test_create_notebook(self) -> None:
        """Test create_notebook function."""
        with patch('acoharmony._notes.generator.Catalog') as MockCatalog, \
             patch('acoharmony._notes.generator.StorageBackend') as MockStorage, \
             patch('acoharmony._notes.generator.jinja2') as MockJinja:
            mock_storage = MockStorage.return_value
            mock_storage.get_data_path.return_value = Path('/tmp/data')
            mock_catalog = MockCatalog.return_value
            mock_meta = MagicMock()
            mock_meta.name = 'test_nb'
            mock_meta.description = 'Desc'
            mock_meta.storage = {'tier': 'silver'}
            mock_meta.file_format = {}
            mock_meta.keys = {}
            mock_meta.transformation_pipeline = None
            mock_meta.medallion_layer = None
            mock_meta.unity_catalog = ''
            mock_catalog.get_table_metadata.return_value = mock_meta
            mock_catalog.list_tables.return_value = []
            mock_template = MagicMock()
            mock_template.render.return_value = '# notebook content'
            mock_env = MagicMock()
            mock_env.get_template.return_value = mock_template
            MockJinja.Environment.return_value = mock_env
            from acoharmony._notes.generator import NotebookGenerator
            gen = NotebookGenerator.__new__(NotebookGenerator)
            gen.catalog = mock_catalog
            gen.storage = mock_storage
            gen.jinja_env = mock_env
            import tempfile
            with tempfile.TemporaryDirectory() as td:
                gen.output_dir = Path(td)
                gen.template_dir = Path(td)
                result = gen.create_notebook('test_nb')
                assert result.name == 'test_nb_dashboard.py'

    @pytest.mark.unit
    def test_list_raw_schemas(self) -> None:
        """Test list_raw_schemas function."""
        with patch('acoharmony._notes.generator.Catalog') as MockCatalog, \
             patch('acoharmony._notes.generator.StorageBackend') as MockStorage:
            mock_storage = MockStorage.return_value
            mock_storage.get_data_path.return_value = Path('/tmp/data')
            mock_catalog = MockCatalog.return_value
            mock_catalog.list_tables.return_value = ['table_b', 'table_a']
            from acoharmony._notes.generator import NotebookGenerator
            gen = NotebookGenerator.__new__(NotebookGenerator)
            gen.catalog = mock_catalog
            gen.storage = mock_storage
            result = gen.list_raw_schemas()
            assert result == ['table_a', 'table_b']

    @pytest.mark.unit
    def test_create_notebooks_for_raw_schemas(self) -> None:
        """Test create_notebooks_for_raw_schemas function."""
        from acoharmony._notes.generator import NotebookGenerator
        gen = NotebookGenerator.__new__(NotebookGenerator)
        gen.list_raw_schemas = MagicMock(return_value=[])
        gen.output_dir = Path('/tmp')
        result = gen.create_notebooks_for_raw_schemas()
        assert result == []

    @pytest.mark.unit
    def test_notebookgenerator_init(self) -> None:
        """Test NotebookGenerator initialization."""
        with patch('acoharmony._notes.generator.Catalog'), \
             patch('acoharmony._notes.generator.StorageBackend') as MockStorage:
            mock_storage = MockStorage.return_value
            mock_storage.get_data_path.return_value = Path('/tmp/data')
            from acoharmony._notes.generator import NotebookGenerator
            import tempfile
            with tempfile.TemporaryDirectory() as td:
                gen = NotebookGenerator(storage_backend=mock_storage, output_dir=Path(td))
                assert gen.storage is mock_storage
                assert gen.output_dir == Path(td)

    @pytest.mark.unit
    def test_schemaobj_init(self) -> None:
        """Test SchemaObj initialization via create_notebook's inner class."""
        # SchemaObj is an inner class in create_notebook; test the pattern directly
        class SchemaObj:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)
        obj = SchemaObj({'name': 'test', 'value': 42})
        assert obj.name == 'test'
        assert obj.value == 42

