#!/usr/bin/env python3
"""
Comprehensive test suite for schemas.py module.

Tests all schema/table metadata management functionality:
- Table metadata loading and caching
- get_table_metadata() and backward compatibility
- expand_table() with inheritance
- _inherit_from_staging() logic
- _merge_columns() logic
- _expand_transformations() pipeline generation
- get_transformation_pipeline()
- get_output_columns() with stage support
- validate_table()
- Backward compatibility for all old method names
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import Any
from unittest.mock import patch

import pytest

from acoharmony.tables import TableManager


def _make_manager_with_tables(tables: dict[str, dict[str, Any]]) -> TableManager:
    """Create a TableManager with pre-populated table cache (bypassing SchemaRegistry)."""
    with patch.object(TableManager, "_load_all_tables"):
        manager = TableManager()
    manager._table_cache.update(tables)
    # Ensure backward compat alias still works
    manager._schema_cache = manager._table_cache
    return manager


@pytest.fixture
def basic_table():
    """Fixture for basic table definition."""
    return {
        "name": "test_table",
        "description": "Test table for testing",
        "columns": [
            {"name": "id", "data_type": "integer"},
            {"name": "value", "data_type": "string"},
        ],
        "storage": {"tier": "raw"},
        "file_format": {"type": "csv"},
    }


@pytest.fixture
def staging_table():
    """Fixture for staging table definition."""
    return {
        "name": "staging_table",
        "description": "Staging table",
        "columns": [
            {"name": "col_a", "data_type": "string"},
            {"name": "col_b", "data_type": "integer"},
            {"name": "col_c", "data_type": "float"},
        ],
        "storage": {"tier": "raw"},
        "file_format": {"type": "fixed_width"},
        "keys": {"deduplication_key": ["col_a", "col_b"]},
    }


@pytest.fixture
def processed_table():
    """Fixture for processed table that inherits from staging."""
    return {
        "name": "processed_table",
        "description": "Processed table",
        "staging": "staging_table",
        "columns": [
            {"name": "col_b", "data_type": "bigint"},  # Override
            {"name": "col_d", "data_type": "date"},  # New
        ],
        "storage": {"tier": "processed"},
    }


class TestTableManagerInit:
    """Test TableManager initialization."""

    @pytest.mark.unit
    def test_init_loads_from_registry(self):
        """Test initialization loads tables from SchemaRegistry."""
        manager = TableManager()
        assert manager._table_cache is not None
        assert manager._schema_cache is manager._table_cache  # Backward compat

    @pytest.mark.unit
    def test_init_populates_cache(self):
        """Test that initialization populates the table cache from registry."""
        manager = TableManager()
        # SchemaRegistry should have real schemas registered
        assert len(manager._table_cache) > 0

    @pytest.mark.unit
    def test_init_with_custom_tables(self, basic_table):
        """Test that we can create a manager with custom tables via helper."""
        manager = _make_manager_with_tables({"test_table": basic_table})
        assert "test_table" in manager._table_cache


class TestGetTableMetadata:
    """Test get_table_metadata() method."""

    @pytest.mark.unit
    def test_get_existing_table(self, basic_table):
        """Test getting an existing table."""
        manager = _make_manager_with_tables({"test_table": basic_table})

        table = manager.get_table_metadata("test_table")
        assert table is not None
        assert table["name"] == "test_table"
        assert len(table["columns"]) == 2

    @pytest.mark.unit
    def test_get_nonexistent_table(self):
        """Test getting a nonexistent table returns None."""
        manager = _make_manager_with_tables({})
        table = manager.get_table_metadata("nonexistent")
        assert table is None

    @pytest.mark.unit
    def test_returns_deep_copy(self, basic_table):
        """Test that returned table is a deep copy."""
        manager = _make_manager_with_tables({"test_table": basic_table})

        table1 = manager.get_table_metadata("test_table")
        table2 = manager.get_table_metadata("test_table")

        # Modify one
        table1["columns"][0]["name"] = "modified"

        # Other should be unchanged
        assert table2["columns"][0]["name"] == "id"


class TestBackwardCompatibility:
    """Test backward compatibility aliases."""

    @pytest.mark.unit
    def test_schema_cache_alias(self, basic_table):
        """Test that _schema_cache is an alias for _table_cache."""
        manager = _make_manager_with_tables({"test_table": basic_table})

        assert manager._schema_cache is manager._table_cache
        assert "test_table" in manager._schema_cache


class TestExpandTable:
    """Test expand_table() method."""

    @pytest.mark.unit
    def test_expand_basic_table(self, basic_table):
        """Test expanding a basic table without inheritance."""
        manager = _make_manager_with_tables({"test_table": basic_table})

        expanded = manager.expand_table("test_table")
        assert expanded["name"] == "test_table"
        assert "pipeline" in expanded

    @pytest.mark.unit
    def test_expand_with_inheritance(self, staging_table, processed_table):
        """Test expanding a table with staging inheritance."""
        manager = _make_manager_with_tables({
            "staging_table": staging_table,
            "processed_table": processed_table,
        })

        expanded = manager.expand_table("processed_table")

        # Should have inherited columns
        col_names = [c["name"] for c in expanded["columns"]]
        assert "col_a" in col_names  # Inherited
        assert "col_b" in col_names  # Overridden
        assert "col_c" in col_names  # Inherited
        assert "col_d" in col_names  # New

    @pytest.mark.unit
    def test_expand_nonexistent_raises_error(self):
        """Test that expanding nonexistent table raises ValueError."""
        manager = _make_manager_with_tables({})

        with pytest.raises(ValueError, match="not found"):
            manager.expand_table("nonexistent")


class TestInheritFromStaging:
    """Test _inherit_from_staging() method."""

    @pytest.mark.unit
    def test_inherit_columns(self, staging_table, processed_table):
        """Test that columns are inherited correctly."""
        manager = _make_manager_with_tables({
            "staging_table": staging_table,
            "processed_table": processed_table,
        })

        staging = manager.get_table_metadata("staging_table")
        processed = manager.get_table_metadata("processed_table")

        result = manager._inherit_from_staging(staging, processed)

        col_names = [c["name"] for c in result["columns"]]
        assert len(col_names) == 4  # 3 from staging + 1 new
        assert "col_a" in col_names
        assert "col_b" in col_names
        assert "col_c" in col_names
        assert "col_d" in col_names

    @pytest.mark.unit
    def test_inherit_file_format(self, staging_table):
        """Test that file_format is inherited."""
        manager = _make_manager_with_tables({"staging_table": staging_table})

        staging = manager.get_table_metadata("staging_table")
        processed = {"name": "proc", "description": "test"}

        result = manager._inherit_from_staging(staging, processed)

        assert "file_format" in result
        assert result["file_format"]["type"] == "fixed_width"

    @pytest.mark.unit
    def test_inherit_deduplication(self, staging_table):
        """Test that deduplication config is inherited."""
        manager = _make_manager_with_tables({"staging_table": staging_table})

        staging = manager.get_table_metadata("staging_table")
        processed = {"name": "proc", "description": "test"}

        result = manager._inherit_from_staging(staging, processed)

        assert "deduplication" in result
        assert result["deduplication"]["key"] == ["col_a", "col_b"]


class TestMergeColumns:
    """Test _merge_columns() method."""

    @pytest.mark.unit
    def test_merge_with_overrides(self):
        """Test merging columns with overrides."""
        manager = _make_manager_with_tables({})

        base = [{"name": "id", "data_type": "integer"}, {"name": "amount", "data_type": "decimal"}]
        overrides = [
            {"name": "amount", "data_type": "float"},  # Override
            {"name": "date", "data_type": "date"},  # New
        ]

        result = manager._merge_columns(base, overrides)

        assert len(result) == 3
        assert result[0]["name"] == "id"
        assert result[1]["name"] == "amount"
        assert result[1]["data_type"] == "float"  # Overridden
        assert result[2]["name"] == "date"

    @pytest.mark.unit
    def test_merge_preserves_order(self):
        """Test that merge preserves base column order."""
        manager = _make_manager_with_tables({})

        base = [
            {"name": "a", "data_type": "string"},
            {"name": "b", "data_type": "string"},
            {"name": "c", "data_type": "string"},
        ]
        overrides = [
            {"name": "b", "data_type": "integer"}  # Override middle
        ]

        result = manager._merge_columns(base, overrides)

        assert [c["name"] for c in result] == ["a", "b", "c"]
        assert result[1]["data_type"] == "integer"


class TestExpandTransformations:
    """Test _expand_transformations() method."""

    @pytest.mark.unit
    def test_expand_deduplication(self):
        """Test expanding deduplication config."""
        manager = _make_manager_with_tables({})

        table = {
            "name": "test",
            "deduplication": {"key": ["id", "date"], "sort_by": ["timestamp"], "keep": "last"},
        }

        result = manager._expand_transformations(table)

        assert "pipeline" in result
        assert len(result["pipeline"]) == 1
        assert result["pipeline"][0]["stage"] == "deduplication"
        assert result["pipeline"][0]["config"]["key"] == ["id", "date"]

    @pytest.mark.unit
    def test_expand_adr(self):
        """Test expanding ADR config."""
        manager = _make_manager_with_tables({})

        table = {
            "name": "test",
            "adr": {
                "adjustment_column": "adj_type",
                "amount_fields": ["amount"],
                "key_columns": ["claim_id"],
            },
        }

        result = manager._expand_transformations(table)

        assert "pipeline" in result
        # Should have adjustment and dedup stages
        stages = [s["stage"] for s in result["pipeline"]]
        assert "adjustment" in stages
        assert "adr_deduplication" in stages

    @pytest.mark.unit
    def test_expand_staging(self):
        """Test expanding with staging reference."""
        manager = _make_manager_with_tables({})

        table = {"name": "test", "staging": "base_table"}

        result = manager._expand_transformations(table)

        assert "pipeline" in result
        assert result["pipeline"][0]["stage"] == "staging"
        assert "base_table" in result["pipeline"][0]["description"]


class TestGetTransformationPipeline:
    """Test get_transformation_pipeline() method."""

    @pytest.mark.unit
    def test_get_pipeline(self):
        """Test getting transformation pipeline."""
        table_def = {
            "name": "test_table",
            "description": "Test",
            "columns": [{"name": "id", "data_type": "integer"}],
            "storage": {"tier": "raw"},
            "file_format": {"type": "csv"},
            "deduplication": {"key": ["id"]},
        }
        manager = _make_manager_with_tables({"test_table": table_def})

        pipeline = manager.get_transformation_pipeline("test_table")

        assert isinstance(pipeline, list)
        assert len(pipeline) > 0
        assert pipeline[0]["stage"] == "deduplication"

    @pytest.mark.unit
    def test_get_pipeline_empty_if_no_transforms(self, basic_table):
        """Test pipeline is empty if no transformations defined."""
        manager = _make_manager_with_tables({"test_table": basic_table})

        pipeline = manager.get_transformation_pipeline("test_table")

        assert isinstance(pipeline, list)
        assert len(pipeline) == 0


class TestGetOutputColumns:
    """Test get_output_columns() method."""

    @pytest.mark.unit
    def test_get_final_columns(self, basic_table):
        """Test getting final columns (no stage specified)."""
        manager = _make_manager_with_tables({"test_table": basic_table})

        columns = manager.get_output_columns("test_table")

        assert len(columns) == 2
        assert columns[0]["name"] == "id"
        assert columns[1]["name"] == "value"

    @pytest.mark.unit
    def test_get_stage_specific_columns(self):
        """Test getting stage-specific columns."""
        table_def = {
            "name": "test_table",
            "description": "Test",
            "columns": [{"name": "id", "data_type": "integer"}],
            "storage": {"tier": "raw"},
            "file_format": {"type": "csv"},
            "stages": {"dedup": {"columns": [{"name": "dedup_flag", "data_type": "boolean"}]}},
        }
        manager = _make_manager_with_tables({"test_table": table_def})

        columns = manager.get_output_columns("test_table", stage="dedup")

        # Should have base columns + stage columns
        col_names = [c["name"] for c in columns]
        assert "id" in col_names
        assert "dedup_flag" in col_names


class TestValidateTable:
    """Test validate_table() method."""

    @pytest.mark.unit
    def test_validate_valid_table(self, basic_table):
        """Test validating a valid table."""
        manager = _make_manager_with_tables({"test_table": basic_table})

        result = manager.validate_table("test_table")

        assert result["valid"] is True
        assert result["table"] == "test_table"
        assert len(result["issues"]) == 0

    @pytest.mark.unit
    def test_validate_missing_name(self):
        """Test validation fails for missing name."""
        table_def = {"description": "Test", "columns": [], "storage": {"tier": "raw"}}
        manager = _make_manager_with_tables({"test": table_def})

        result = manager.validate_table("test")

        assert result["valid"] is False
        assert any("name" in issue for issue in result["issues"])

    @pytest.mark.unit
    def test_validate_missing_columns(self):
        """Test validation fails when no columns and no staging."""
        table_def = {"name": "test_table", "description": "Test", "storage": {"tier": "raw"}}
        manager = _make_manager_with_tables({"test_table": table_def})

        result = manager.validate_table("test_table")

        assert result["valid"] is False
        assert any("columns" in issue.lower() for issue in result["issues"])

    @pytest.mark.unit
    def test_validate_column_missing_type(self):
        """Test validation fails for column without data_type."""
        table_def = {
            "name": "test_table",
            "description": "Test",
            "columns": [{"name": "id"}],  # Missing data_type
            "storage": {"tier": "raw"},
        }
        manager = _make_manager_with_tables({"test_table": table_def})

        result = manager.validate_table("test_table")

        assert result["valid"] is False
        assert any("data_type" in issue for issue in result["issues"])


class TestIntegration:
    """Integration tests for complete workflows."""

    @pytest.mark.unit
    def test_full_inheritance_and_expansion(self, staging_table, processed_table):
        """Test complete workflow with inheritance and expansion."""
        # Add dedup and ADR to processed table
        processed_table["deduplication"] = {"key": ["col_a"], "sort_by": ["col_c"]}
        processed_table["adr"] = {
            "adjustment_column": "adj_type",
            "amount_fields": ["col_c"],
            "key_columns": ["col_a"],  # Required for ADR validation
        }

        manager = _make_manager_with_tables({
            "staging_table": staging_table,
            "processed_table": processed_table,
        })

        # Expand processed table
        expanded = manager.expand_table("processed_table")

        # Should have inherited columns
        assert len(expanded["columns"]) == 4

        # Should have pipeline from transformations
        pipeline = manager.get_transformation_pipeline("processed_table")
        assert len(pipeline) >= 2  # At least staging + dedup

        # Validation should pass
        validation = manager.validate_table("processed_table")
        if not validation["valid"]:
            print(f"Validation issues: {validation['issues']}")
        assert validation["valid"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
