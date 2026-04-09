# © 2025 HarmonyCares
"""Tests for acoharmony._notes.config module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
from pathlib import Path
from unittest.mock import MagicMock


class TestConfig:
    """Test suite for config."""

    @pytest.mark.unit
    def test_from_schema(self) -> None:
        """Test from_schema function."""
        from acoharmony._notes.config import NotebookConfig

        schema = MagicMock()
        schema.name = "test_schema"
        schema.description = "A test schema"
        schema.storage = {"tier": "silver"}
        schema.keys = {"primary_key": ["id_col"]}
        schema.columns = [{"name": "total_spend"}, {"name": "date_col"}]

        storage_config = MagicMock()
        storage_config.get_path.return_value = Path("/data/silver")

        result = NotebookConfig.from_schema(schema, storage_config=storage_config)
        assert result.schema_name == "test_schema"
        assert result.schema_description == "A test schema"
        assert result.storage_tier == "silver"
        assert result.primary_key == "id_col"
        assert "test_schema.parquet" in result.data_path

    @pytest.mark.unit
    def test_notebookconfig_init(self) -> None:
        """Test NotebookConfig initialization."""
        from acoharmony._notes.config import NotebookConfig

        config = NotebookConfig(
            schema_name="test",
            schema_description="desc",
            storage_tier="silver",
            data_path="/data/test.parquet",
        )
        assert config.schema_name == "test"
        assert config.schema_description == "desc"
        assert config.app_width == "medium"
        assert config.hide_code is True
        assert config.primary_key is None
        assert config.max_display_rows == 100


class TestNotebookConfigFromSchemaNoKeys:
    """Cover branch 87->92: schema.keys dict exists but has no 'primary_key'."""

    @pytest.mark.unit
    def test_keys_without_primary_key(self):
        """When schema.keys has no 'primary_key', primary_key should be None."""
        from acoharmony._notes.config import NotebookConfig

        schema = MagicMock()
        schema.name = "test_schema"
        schema.description = "A test schema"
        schema.storage = {"tier": "silver"}
        schema.keys = {"some_other_key": "value"}  # has keys but no primary_key
        # No columns attribute to keep it simple
        del schema.columns

        storage_config = MagicMock()
        storage_config.get_path.return_value = Path("/data/silver")

        result = NotebookConfig.from_schema(schema, storage_config=storage_config)
        assert result.primary_key is None
        assert result.schema_name == "test_schema"

