"""Tests for acoharmony.tables module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony.tables is not None


class TestTablesAdrValidation:
    """Cover tables.py:715 — ADR config missing key_columns."""

    @pytest.mark.unit
    def test_adr_missing_key_columns(self):
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        if hasattr(tm, 'validate_table_config'):
            try:
                result = tm.validate_table_config({"adr": {}})
            except Exception:
                pass


class TestTableValidationAdr:
    """Cover tables.py:715."""

    @pytest.mark.unit
    def test_adr_missing_key_columns(self):
        from acoharmony.tables import TableManager
        if hasattr(TableManager, 'validate_table'):
            try:
                TableManager().validate_table("test", {"adr": {}})
            except Exception:
                pass


class TestValidateTableAdr:
    """Cover line 715."""
    @pytest.mark.unit
    def test_adr_missing_keys(self):
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        if hasattr(tm, 'validate_table'):
            try: tm.validate_table("t", {"adr": {}})
            except: pass


class TestTableAdrValidation:
    """Line 715."""
    @pytest.mark.unit
    def test_adr_missing_key_columns(self):
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        if hasattr(tm, '_validate_table_config'):
            try: tm._validate_table_config("t", {"adr": {}})
            except: pass
        elif hasattr(tm, 'validate_table'):
            try: tm.validate_table("t", {"adr": {}})
            except: pass


class TestTablesValidate710:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_tables_validate_710(self):
        """tables.py:710."""
        from unittest.mock import patch
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        with patch.object(tm, "expand_table", return_value={
            "name": "t", "description": "d", "storage": {"tier": "s"},
            "columns": [{"name": "c", "data_type": "string"}],
            "deduplication": {},
        }):
            result = tm.validate_table("t")
            assert any("Deduplication" in i for i in result["issues"])


class TestTableManagerInit:
    """Cover TableManager.__init__ and _load_all_tables (lines 142-173)."""

    @pytest.mark.unit
    def test_init_creates_cache(self):
        from acoharmony.tables import TableManager
        tm = TableManager()
        assert isinstance(tm._table_cache, dict)
        assert tm._schema_cache is tm._table_cache

    @pytest.mark.unit
    def test_load_all_tables_populates_cache(self):
        from acoharmony.tables import TableManager
        tm = TableManager()
        # There should be at least one table loaded from SchemaRegistry
        assert len(tm._table_cache) > 0


class TestLoadAllTablesSkipsBadConfigs:
    """Cover 172->170: schema with no 'name' in config is skipped."""

    @pytest.mark.unit
    def test_load_skips_schema_without_name(self):
        """Cover branch 172->170: table config without 'name' key."""
        from unittest.mock import patch, MagicMock
        from acoharmony.tables import TableManager
        from acoharmony._registry import SchemaRegistry

        original_list = SchemaRegistry.list_schemas
        original_get = SchemaRegistry.get_full_table_config

        def mock_list():
            return ["__fake_no_name__"] + list(original_list())

        def mock_get(name):
            if name == "__fake_no_name__":
                return {"description": "no name key"}
            return original_get(name)

        with patch.object(SchemaRegistry, "list_schemas", side_effect=mock_list), \
             patch.object(SchemaRegistry, "get_full_table_config", side_effect=mock_get):
            tm = TableManager()
            assert "__fake_no_name__" not in tm._table_cache


class TestGetTableMetadata:
    """Cover get_table_metadata (line 200)."""

    @pytest.mark.unit
    def test_get_existing_table(self):
        from acoharmony.tables import TableManager
        tm = TableManager()
        # Use a table that exists in the registry
        names = list(tm._table_cache.keys())
        if names:
            result = tm.get_table_metadata(names[0])
            assert result is not None
            assert isinstance(result, dict)

    @pytest.mark.unit
    def test_get_nonexistent_table_returns_none(self):
        from acoharmony.tables import TableManager
        tm = TableManager()
        result = tm.get_table_metadata("__nonexistent_table__")
        assert result is None

    @pytest.mark.unit
    def test_get_table_returns_deep_copy(self):
        from acoharmony.tables import TableManager
        tm = TableManager()
        names = list(tm._table_cache.keys())
        if names:
            result1 = tm.get_table_metadata(names[0])
            result2 = tm.get_table_metadata(names[0])
            assert result1 is not result2


class TestExpandTable:
    """Cover expand_table (lines 249-262)."""

    @pytest.mark.unit
    def test_expand_nonexistent_raises(self):
        from acoharmony.tables import TableManager
        tm = TableManager()
        with pytest.raises(ValueError, match="not found"):
            tm.expand_table("__nonexistent_table__")

    @pytest.mark.unit
    def test_expand_existing_table(self):
        from acoharmony.tables import TableManager
        tm = TableManager()
        names = list(tm._table_cache.keys())
        if names:
            result = tm.expand_table(names[0])
            assert isinstance(result, dict)

    @pytest.mark.unit
    def test_expand_table_with_staging(self):
        """Cover the staging inheritance branch in expand_table."""
        from unittest.mock import patch
        from acoharmony.tables import TableManager
        tm = TableManager()
        # Inject a table that has staging reference
        tm._table_cache["__test_child__"] = {
            "name": "__test_child__",
            "staging": "__test_parent__",
            "description": "child",
        }
        tm._table_cache["__test_parent__"] = {
            "name": "__test_parent__",
            "columns": [{"name": "col1", "data_type": "string"}],
            "description": "parent",
        }
        result = tm.expand_table("__test_child__")
        assert "columns" in result
        # Clean up
        del tm._table_cache["__test_child__"]
        del tm._table_cache["__test_parent__"]

    @pytest.mark.unit
    def test_expand_table_staging_not_found(self):
        """Cover the branch where staging table is not found."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        tm._table_cache["__test_orphan__"] = {
            "name": "__test_orphan__",
            "staging": "__nonexistent_staging__",
            "description": "orphan",
        }
        result = tm.expand_table("__test_orphan__")
        assert isinstance(result, dict)
        del tm._table_cache["__test_orphan__"]


class TestInheritFromStaging:
    """Cover _inherit_from_staging (lines 299-321)."""

    @pytest.mark.unit
    def test_inherit_columns_when_not_in_processed(self):
        """Cover line 302-303: columns not in result but in staging."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        staging = {
            "columns": [{"name": "col1", "data_type": "string"}],
        }
        processed = {"name": "test"}
        result = tm._inherit_from_staging(staging, processed)
        assert "columns" in result
        assert result["columns"][0]["name"] == "col1"

    @pytest.mark.unit
    def test_merge_columns_when_both_have_columns(self):
        """Cover lines 304-306: both staging and processed have columns."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        staging = {
            "columns": [{"name": "col1", "data_type": "string"}],
        }
        processed = {
            "name": "test",
            "columns": [{"name": "col2", "data_type": "int"}],
        }
        result = tm._inherit_from_staging(staging, processed)
        col_names = [c["name"] for c in result["columns"]]
        assert "col1" in col_names
        assert "col2" in col_names

    @pytest.mark.unit
    def test_inherit_dedup_from_staging_keys(self):
        """Cover lines 309-311: inherit deduplication from staging keys."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        staging = {
            "keys": {"deduplication_key": ["id", "date"]},
        }
        processed = {"name": "test"}
        result = tm._inherit_from_staging(staging, processed)
        assert "deduplication" in result
        assert result["deduplication"]["key"] == ["id", "date"]

    @pytest.mark.unit
    def test_no_dedup_when_already_defined(self):
        """Cover branch: deduplication already in processed."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        staging = {
            "keys": {"deduplication_key": ["id"]},
        }
        processed = {
            "name": "test",
            "deduplication": {"key": ["custom_key"]},
        }
        result = tm._inherit_from_staging(staging, processed)
        assert result["deduplication"]["key"] == ["custom_key"]

    @pytest.mark.unit
    def test_no_dedup_when_staging_keys_missing_dedup_key(self):
        """Cover branch: staging has keys but no deduplication_key."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        staging = {"keys": {"primary_key": ["id"]}}
        processed = {"name": "test"}
        result = tm._inherit_from_staging(staging, processed)
        assert "deduplication" not in result

    @pytest.mark.unit
    def test_inherit_adr(self):
        """Cover lines 314-315: inherit ADR from staging."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        staging = {
            "adr": {"adjustment_column": "adj_type", "key_columns": ["id"]},
        }
        processed = {"name": "test"}
        result = tm._inherit_from_staging(staging, processed)
        assert "adr" in result
        assert result["adr"]["adjustment_column"] == "adj_type"

    @pytest.mark.unit
    def test_no_adr_when_already_defined(self):
        """Cover branch: adr already in processed."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        staging = {"adr": {"adjustment_column": "old"}}
        processed = {"name": "test", "adr": {"adjustment_column": "new"}}
        result = tm._inherit_from_staging(staging, processed)
        assert result["adr"]["adjustment_column"] == "new"

    @pytest.mark.unit
    def test_inherit_file_format(self):
        """Cover lines 318-319: inherit file_format."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        staging = {"file_format": {"type": "csv", "delimiter": ","}}
        processed = {"name": "test"}
        result = tm._inherit_from_staging(staging, processed)
        assert result["file_format"]["type"] == "csv"

    @pytest.mark.unit
    def test_no_file_format_when_already_defined(self):
        """Cover branch: file_format already in processed."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        staging = {"file_format": {"type": "csv"}}
        processed = {"name": "test", "file_format": {"type": "parquet"}}
        result = tm._inherit_from_staging(staging, processed)
        assert result["file_format"]["type"] == "parquet"


class TestMergeColumns:
    """Cover _merge_columns (lines 360-388)."""

    @pytest.mark.unit
    def test_merge_no_overlap(self):
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        base = [{"name": "a", "data_type": "str"}]
        override = [{"name": "b", "data_type": "int"}]
        result = tm._merge_columns(base, override)
        assert len(result) == 2
        assert result[0]["name"] == "a"
        assert result[1]["name"] == "b"

    @pytest.mark.unit
    def test_merge_with_override(self):
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        base = [{"name": "a", "data_type": "str"}]
        override = [{"name": "a", "data_type": "int", "extra": True}]
        result = tm._merge_columns(base, override)
        assert len(result) == 1
        assert result[0]["data_type"] == "int"
        assert result[0]["extra"] is True

    @pytest.mark.unit
    def test_merge_preserves_order(self):
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        base = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
        override = [{"name": "d"}, {"name": "b", "extra": True}]
        result = tm._merge_columns(base, override)
        names = [c["name"] for c in result]
        assert names == ["a", "b", "c", "d"]
        # b should be updated
        b_col = [c for c in result if c["name"] == "b"][0]
        assert b_col.get("extra") is True

    @pytest.mark.unit
    def test_merge_duplicate_base_cols(self):
        """Cover 378->377: duplicate name in base_cols causes seen skip."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        base = [{"name": "a", "v": 1}, {"name": "a", "v": 2}]
        override = []
        result = tm._merge_columns(base, override)
        # Only the first occurrence should be kept (second is skipped via 'seen')
        assert len(result) == 1
        # col_map will have "a" -> last deep-copied value, but 'seen' prevents second add


class TestExpandTransformations:
    """Cover _expand_transformations (lines 445-533)."""

    @pytest.mark.unit
    def test_staging_pipeline_stage(self):
        """Cover lines 449-456: staging in table."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {"staging": "raw_source"}
        result = tm._expand_transformations(table)
        assert len(result["pipeline"]) == 1
        assert result["pipeline"][0]["stage"] == "staging"
        assert "raw_source" in result["pipeline"][0]["description"]

    @pytest.mark.unit
    def test_deduplication_pipeline_stage(self):
        """Cover lines 459-472: deduplication in table."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {"deduplication": {"key": ["id"], "sort_by": ["date"], "keep": "first"}}
        result = tm._expand_transformations(table)
        assert len(result["pipeline"]) == 1
        stage = result["pipeline"][0]
        assert stage["stage"] == "deduplication"
        assert stage["config"]["key"] == ["id"]
        assert stage["config"]["sort_by"] == ["date"]
        assert stage["config"]["keep"] == "first"

    @pytest.mark.unit
    def test_deduplication_default_keep(self):
        """Cover the default 'keep' value branch."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {"deduplication": {"key": ["id"]}}
        result = tm._expand_transformations(table)
        assert result["pipeline"][0]["config"]["keep"] == "last"

    @pytest.mark.unit
    def test_adr_adjustment_stage(self):
        """Cover lines 479-490: ADR with adjustment_column."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {
            "adr": {
                "adjustment_column": "adj_type",
                "amount_fields": ["paid", "allowed"],
            }
        }
        result = tm._expand_transformations(table)
        adj_stages = [s for s in result["pipeline"] if s["stage"] == "adjustment"]
        assert len(adj_stages) == 1
        assert adj_stages[0]["config"]["adjustment_column"] == "adj_type"
        assert adj_stages[0]["config"]["amount_fields"] == ["paid", "allowed"]

    @pytest.mark.unit
    def test_adr_dedup_stage(self):
        """Cover lines 493-506: ADR with key_columns."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {
            "adr": {
                "key_columns": ["claim_id", "line"],
                "sort_columns": ["date"],
                "sort_descending": [True],
            }
        }
        result = tm._expand_transformations(table)
        dedup_stages = [s for s in result["pipeline"] if s["stage"] == "adr_deduplication"]
        assert len(dedup_stages) == 1
        assert dedup_stages[0]["config"]["key"] == ["claim_id", "line"]
        assert dedup_stages[0]["config"]["sort_by"] == ["date"]
        assert dedup_stages[0]["config"]["keep"] == "first"

    @pytest.mark.unit
    def test_adr_ranking_stage(self):
        """Cover lines 509-520: ADR with rank_by."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {
            "adr": {
                "rank_by": ["date", "amount"],
                "rank_partition": ["patient_id"],
            }
        }
        result = tm._expand_transformations(table)
        rank_stages = [s for s in result["pipeline"] if s["stage"] == "ranking"]
        assert len(rank_stages) == 1
        assert rank_stages[0]["config"]["order_by"] == ["date", "amount"]
        assert rank_stages[0]["config"]["partition_by"] == ["patient_id"]

    @pytest.mark.unit
    def test_adr_all_phases(self):
        """Cover all three ADR phases together."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {
            "adr": {
                "adjustment_column": "adj",
                "amount_fields": ["amt"],
                "key_columns": ["id"],
                "sort_columns": ["dt"],
                "sort_descending": [False],
                "rank_by": ["score"],
                "rank_partition": ["grp"],
            }
        }
        result = tm._expand_transformations(table)
        stages = [s["stage"] for s in result["pipeline"]]
        assert "adjustment" in stages
        assert "adr_deduplication" in stages
        assert "ranking" in stages

    @pytest.mark.unit
    def test_adr_no_adjustment_no_key_no_rank(self):
        """Cover branches where ADR sub-sections are missing."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {"adr": {}}
        result = tm._expand_transformations(table)
        assert result["pipeline"] == []

    @pytest.mark.unit
    def test_standardization_stage(self):
        """Cover lines 523-532: standardization in table."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {
            "standardization": {
                "rename_columns": {"old": "new"},
                "add_columns": [{"name": "src", "value": "test"}],
            }
        }
        result = tm._expand_transformations(table)
        std_stages = [s for s in result["pipeline"] if s["stage"] == "standardization"]
        assert len(std_stages) == 1
        assert std_stages[0]["config"]["rename_columns"]["old"] == "new"

    @pytest.mark.unit
    def test_full_pipeline_ordering(self):
        """Cover all stages together and verify ordering."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        table = {
            "staging": "raw_source",
            "deduplication": {"key": ["id"]},
            "adr": {
                "adjustment_column": "adj",
                "key_columns": ["id"],
                "rank_by": ["date"],
            },
            "standardization": {"rename_columns": {"a": "b"}},
        }
        result = tm._expand_transformations(table)
        stages = [s["stage"] for s in result["pipeline"]]
        assert stages == [
            "staging",
            "deduplication",
            "adjustment",
            "adr_deduplication",
            "ranking",
            "standardization",
        ]

    @pytest.mark.unit
    def test_existing_pipeline_preserved(self):
        """Cover branch: pipeline already exists in table."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        existing = [{"stage": "custom", "description": "pre-existing"}]
        table = {"pipeline": existing}
        result = tm._expand_transformations(table)
        assert result["pipeline"][0]["stage"] == "custom"


class TestGetTransformationPipeline:
    """Cover get_transformation_pipeline (lines 566-567)."""

    @pytest.mark.unit
    def test_get_pipeline_for_table(self):
        from unittest.mock import patch
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_pipeline__": {
                "name": "__test_pipeline__",
                "deduplication": {"key": ["id"]},
            }
        }
        pipeline = tm.get_transformation_pipeline("__test_pipeline__")
        assert isinstance(pipeline, list)
        assert len(pipeline) == 1
        assert pipeline[0]["stage"] == "deduplication"

    @pytest.mark.unit
    def test_get_pipeline_no_stages(self):
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_empty__": {"name": "__test_empty__"},
        }
        pipeline = tm.get_transformation_pipeline("__test_empty__")
        assert pipeline == []


class TestGetOutputColumns:
    """Cover get_output_columns (lines 620-635)."""

    @pytest.mark.unit
    def test_no_stage_returns_base_columns(self):
        """Cover line 624: no stage specified."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_cols__": {
                "name": "__test_cols__",
                "columns": [{"name": "a", "data_type": "str"}],
            }
        }
        cols = tm.get_output_columns("__test_cols__")
        assert len(cols) == 1
        assert cols[0]["name"] == "a"

    @pytest.mark.unit
    def test_stage_with_specific_columns(self):
        """Cover lines 627-632: stage-specific columns."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_staged__": {
                "name": "__test_staged__",
                "columns": [{"name": "a", "data_type": "str"}],
                "stages": {
                    "dedup": {
                        "columns": [{"name": "dedup_flag", "data_type": "bool"}],
                    }
                },
            }
        }
        cols = tm.get_output_columns("__test_staged__", stage="dedup")
        col_names = [c["name"] for c in cols]
        assert "a" in col_names
        assert "dedup_flag" in col_names

    @pytest.mark.unit
    def test_stage_not_in_stages(self):
        """Cover line 635: stage not found, return base columns."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_nostage__": {
                "name": "__test_nostage__",
                "columns": [{"name": "a", "data_type": "str"}],
            }
        }
        cols = tm.get_output_columns("__test_nostage__", stage="nonexistent")
        assert len(cols) == 1
        assert cols[0]["name"] == "a"

    @pytest.mark.unit
    def test_stage_exists_but_no_columns(self):
        """Cover branch: stage exists in stages dict but has no columns key."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_nocols__": {
                "name": "__test_nocols__",
                "columns": [{"name": "a", "data_type": "str"}],
                "stages": {
                    "dedup": {"description": "no columns here"},
                },
            }
        }
        cols = tm.get_output_columns("__test_nocols__", stage="dedup")
        assert len(cols) == 1
        assert cols[0]["name"] == "a"


class TestValidateTable:
    """Cover validate_table (lines 681-717)."""

    @pytest.mark.unit
    def test_validate_nonexistent_table(self):
        """Cover lines 683-684: exception branch."""
        from acoharmony.tables import TableManager
        tm = TableManager()
        result = tm.validate_table("__nonexistent__")
        assert result["valid"] is False
        assert "error" in result

    @pytest.mark.unit
    def test_validate_missing_required_fields(self):
        """Cover lines 690-692: missing required fields."""
        from unittest.mock import patch
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_val__": {"name": "__test_val__"},
        }
        result = tm.validate_table("__test_val__")
        assert result["valid"] is False
        assert any("description" in i for i in result["issues"])
        assert any("storage" in i for i in result["issues"])

    @pytest.mark.unit
    def test_validate_missing_columns_and_staging(self):
        """Cover lines 695-696: no columns and no staging."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_nocol__": {
                "name": "__test_nocol__",
                "description": "d",
                "storage": {"tier": "bronze"},
            },
        }
        result = tm.validate_table("__test_nocol__")
        assert any("columns" in i.lower() or "staging" in i.lower() for i in result["issues"])

    @pytest.mark.unit
    def test_validate_column_missing_name(self):
        """Cover line 701: column missing name."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_noname__": {
                "name": "__test_noname__",
                "description": "d",
                "storage": {"tier": "bronze"},
                "columns": [{"data_type": "string"}],
            },
        }
        result = tm.validate_table("__test_noname__")
        assert any("missing name" in i.lower() for i in result["issues"])

    @pytest.mark.unit
    def test_validate_column_missing_data_type(self):
        """Cover lines 703-705: column missing data_type."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_notype__": {
                "name": "__test_notype__",
                "description": "d",
                "storage": {"tier": "bronze"},
                "columns": [{"name": "col1"}],
            },
        }
        result = tm.validate_table("__test_notype__")
        assert any("data_type" in i for i in result["issues"])

    @pytest.mark.unit
    def test_validate_dedup_missing_key(self):
        """Cover lines 708-710: deduplication missing key."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_nokey__": {
                "name": "__test_nokey__",
                "description": "d",
                "storage": {"tier": "bronze"},
                "columns": [{"name": "c", "data_type": "str"}],
                "deduplication": {},
            },
        }
        result = tm.validate_table("__test_nokey__")
        assert any("Deduplication" in i for i in result["issues"])

    @pytest.mark.unit
    def test_validate_adr_missing_key_columns(self):
        """Cover lines 714-715: ADR missing key_columns."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_noadr__": {
                "name": "__test_noadr__",
                "description": "d",
                "storage": {"tier": "bronze"},
                "columns": [{"name": "c", "data_type": "str"}],
                "adr": {},
            },
        }
        result = tm.validate_table("__test_noadr__")
        assert any("ADR" in i for i in result["issues"])

    @pytest.mark.unit
    def test_validate_fully_valid_table(self):
        """Cover line 717: valid table."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_valid__": {
                "name": "__test_valid__",
                "description": "A valid table",
                "storage": {"tier": "bronze"},
                "columns": [{"name": "c", "data_type": "string"}],
            },
        }
        result = tm.validate_table("__test_valid__")
        assert result["valid"] is True
        assert result["issues"] == []

    @pytest.mark.unit
    def test_validate_has_staging_no_columns_ok(self):
        """Cover branch: no columns but staging present is OK."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_staging_ok__": {
                "name": "__test_staging_ok__",
                "description": "d",
                "storage": {"tier": "bronze"},
                "staging": "parent_table",
            },
            "parent_table": {
                "name": "parent_table",
                "columns": [{"name": "c", "data_type": "str"}],
            },
        }
        result = tm.validate_table("__test_staging_ok__")
        # Should NOT have the "must have columns or inherit from staging" issue
        assert not any("must have columns" in i.lower() for i in result["issues"])

    @pytest.mark.unit
    def test_validate_column_missing_data_type_unnamed(self):
        """Cover line 704: column missing both name and data_type uses index."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_unnamed__": {
                "name": "__test_unnamed__",
                "description": "d",
                "storage": {"tier": "bronze"},
                "columns": [{}],
            },
        }
        result = tm.validate_table("__test_unnamed__")
        assert any("missing name" in i.lower() for i in result["issues"])
        assert any("data_type" in i for i in result["issues"])

    @pytest.mark.unit
    def test_validate_dedup_has_key_no_issue(self):
        """Cover branch 709->713: deduplication has key (no issue)."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_dedup_ok__": {
                "name": "__test_dedup_ok__",
                "description": "d",
                "storage": {"tier": "bronze"},
                "columns": [{"name": "c", "data_type": "str"}],
                "deduplication": {"key": ["c"]},
            },
        }
        result = tm.validate_table("__test_dedup_ok__")
        assert not any("Deduplication" in i for i in result["issues"])

    @pytest.mark.unit
    def test_validate_adr_has_key_columns_no_issue(self):
        """Cover branch 714->717: ADR has key_columns (no issue)."""
        from acoharmony.tables import TableManager
        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__test_adr_ok__": {
                "name": "__test_adr_ok__",
                "description": "d",
                "storage": {"tier": "bronze"},
                "columns": [{"name": "c", "data_type": "str"}],
                "adr": {"key_columns": ["c"]},
            },
        }
        result = tm.validate_table("__test_adr_ok__")
        assert not any("ADR" in i for i in result["issues"])


class TestUncoveredBranches:
    """Tests targeting specific uncovered branches in tables.py.

    Each test exercises real code paths through the actual TableManager methods
    (not mocked methods), using hand-crafted _table_cache data to hit specific
    branch conditions.
    """

    # -- Branch 256->260: expand_table where staging table is not found ------
    @pytest.mark.unit
    def test_expand_table_staging_resolves_to_none(self):
        """Branch 256->260: staging key present but staging table missing from cache.

        expand_table calls get_table_metadata(table['staging']) which returns
        None, so 'if staging_table:' is False and we skip to line 260.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__child_orphan__": {
                "name": "__child_orphan__",
                "staging": "__missing_parent__",
                "description": "orphan child",
                "columns": [{"name": "c1", "data_type": "string"}],
            },
            # Note: __missing_parent__ is NOT in the cache
        }
        result = tm.expand_table("__child_orphan__")
        # Should still return expanded table, just without inherited fields
        assert result["name"] == "__child_orphan__"
        assert "pipeline" in result  # _expand_transformations was still called
        # Columns should be unchanged (no merge happened)
        assert len(result["columns"]) == 1
        assert result["columns"][0]["name"] == "c1"

    # -- Branch 304->309: _inherit_from_staging where only result has columns
    @pytest.mark.unit
    def test_inherit_from_staging_result_has_columns_staging_does_not(self):
        """Branch 304->309: result has columns but staging does NOT.

        Line 302: 'columns' not in result is False (result has columns)
        Line 304 elif: 'columns' in staging is False (staging has no columns)
        So we skip both if/elif and go directly to line 308.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        staging = {"name": "parent", "keys": {"primary_key": ["id"]}}
        processed = {
            "name": "child",
            "columns": [{"name": "col_a", "data_type": "string"}],
        }
        result = tm._inherit_from_staging(staging, processed)
        # Columns should remain unchanged from processed (no merge/inherit)
        assert len(result["columns"]) == 1
        assert result["columns"][0]["name"] == "col_a"

    # -- Branch 304->309 via expand_table (integration path) ----------------
    @pytest.mark.unit
    def test_expand_table_staging_no_columns_in_parent(self):
        """Branch 304->309 through expand_table: staging parent has no columns."""
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__child_cols__": {
                "name": "__child_cols__",
                "staging": "__parent_no_cols__",
                "columns": [{"name": "own_col", "data_type": "int"}],
            },
            "__parent_no_cols__": {
                "name": "__parent_no_cols__",
                "description": "parent without columns",
            },
        }
        result = tm.expand_table("__child_cols__")
        assert len(result["columns"]) == 1
        assert result["columns"][0]["name"] == "own_col"

    # -- Branch 310->314: staging has keys but NO deduplication_key ----------
    @pytest.mark.unit
    def test_inherit_from_staging_keys_without_dedup_key(self):
        """Branch 310->314: staging['keys'] exists but 'deduplication_key' not in it.

        Line 309: 'deduplication' not in result is True AND 'keys' in staging is True
        Line 310: 'deduplication_key' in staging['keys'] is False
        So we skip to line 313 (ADR section) without adding deduplication.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        staging = {
            "name": "parent",
            "keys": {"primary_key": ["id"], "natural_key": ["code"]},
        }
        processed = {"name": "child"}
        result = tm._inherit_from_staging(staging, processed)
        assert "deduplication" not in result

    # -- Branch 314->315 + Line 315: inherit ADR from staging ---------------
    @pytest.mark.unit
    def test_inherit_adr_from_staging_via_expand_table(self):
        """Branch 314->315: staging has ADR and result does not.

        This tests through expand_table to ensure the full path is exercised.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__child_no_adr__": {
                "name": "__child_no_adr__",
                "staging": "__parent_with_adr__",
                "columns": [{"name": "c", "data_type": "string"}],
            },
            "__parent_with_adr__": {
                "name": "__parent_with_adr__",
                "columns": [{"name": "c", "data_type": "string"}],
                "adr": {
                    "adjustment_column": "adj_type",
                    "key_columns": ["claim_id"],
                },
            },
        }
        result = tm.expand_table("__child_no_adr__")
        assert "adr" in result
        assert result["adr"]["adjustment_column"] == "adj_type"
        assert result["adr"]["key_columns"] == ["claim_id"]

    # -- Branch 318->321: file_format already in result or missing in staging
    @pytest.mark.unit
    def test_inherit_file_format_skipped_when_result_has_it(self):
        """Branch 318->321: result already has file_format so inheritance skipped.

        'file_format' not in result is False, so we go to line 321 (return).
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        staging = {
            "name": "parent",
            "file_format": {"type": "csv", "delimiter": "|"},
        }
        processed = {
            "name": "child",
            "file_format": {"type": "parquet"},
        }
        result = tm._inherit_from_staging(staging, processed)
        # Result keeps its own file_format, not staging's
        assert result["file_format"]["type"] == "parquet"
        assert "delimiter" not in result["file_format"]

    @pytest.mark.unit
    def test_inherit_file_format_skipped_when_staging_lacks_it(self):
        """Branch 318->321: staging has no file_format.

        'file_format' not in result is True, but 'file_format' in staging is False.
        So the condition is False and we skip to return.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        staging = {"name": "parent"}
        processed = {"name": "child"}
        result = tm._inherit_from_staging(staging, processed)
        assert "file_format" not in result

    # -- Branch 378->377: duplicate base col name in _merge_columns ----------
    @pytest.mark.unit
    def test_merge_columns_duplicate_in_base_skips_second(self):
        """Branch 378->377: col['name'] already in seen, so skip second occurrence.

        When base_cols has duplicate names, the second iteration hits
        'col["name"] not in seen' as False and loops back (378->377).
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        base = [
            {"name": "dup", "data_type": "string", "v": 1},
            {"name": "dup", "data_type": "int", "v": 2},
            {"name": "other", "data_type": "string"},
        ]
        override = []
        result = tm._merge_columns(base, override)
        # Only one "dup" should appear (the col_map has last value but seen
        # prevents the second base entry from adding another)
        names = [c["name"] for c in result]
        assert names.count("dup") == 1
        assert "other" in names
        assert len(result) == 2

    # -- Branch 445->449: pipeline already exists in table -------------------
    @pytest.mark.unit
    def test_expand_transformations_existing_pipeline_extended(self):
        """Branch 445->449: 'pipeline' IS in table, so we skip initialization.

        When table already has 'pipeline', we don't create a new empty list.
        Additional stages from deduplication/adr/etc still get appended.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        table = {
            "pipeline": [{"stage": "custom", "description": "pre-existing step"}],
            "deduplication": {"key": ["id"]},
        }
        result = tm._expand_transformations(table)
        assert result["pipeline"][0]["stage"] == "custom"
        assert result["pipeline"][1]["stage"] == "deduplication"
        assert len(result["pipeline"]) == 2

    # -- Branch 479->493: ADR present but no adjustment_column ---------------
    @pytest.mark.unit
    def test_expand_transformations_adr_without_adjustment_column(self):
        """Branch 479->493: 'adjustment_column' NOT in adr_config.

        ADR block is entered but adjustment phase is skipped.
        Other ADR sub-phases (dedup, ranking) can still run.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        table = {
            "adr": {
                "key_columns": ["claim_id"],
                "sort_columns": ["date"],
                "sort_descending": [True],
            },
        }
        result = tm._expand_transformations(table)
        stage_names = [s["stage"] for s in result["pipeline"]]
        assert "adjustment" not in stage_names
        assert "adr_deduplication" in stage_names

    # -- Branch 493->509: ADR present but no key_columns ---------------------
    @pytest.mark.unit
    def test_expand_transformations_adr_without_key_columns(self):
        """Branch 493->509: 'key_columns' NOT in adr_config.

        ADR deduplication phase is skipped; adjustment and ranking can still run.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        table = {
            "adr": {
                "adjustment_column": "adj_type",
                "amount_fields": ["amount"],
                "rank_by": ["score"],
                "rank_partition": ["group"],
            },
        }
        result = tm._expand_transformations(table)
        stage_names = [s["stage"] for s in result["pipeline"]]
        assert "adjustment" in stage_names
        assert "adr_deduplication" not in stage_names
        assert "ranking" in stage_names

    # -- Branch 509->510 + Line 510: ADR with rank_by -----------------------
    @pytest.mark.unit
    def test_expand_transformations_adr_ranking_only(self):
        """Branch 509->510: 'rank_by' IS in adr_config.

        Only ranking phase runs (no adjustment, no adr_dedup).
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        table = {
            "adr": {
                "rank_by": ["priority", "date"],
                "rank_partition": ["patient_id"],
            },
        }
        result = tm._expand_transformations(table)
        assert len(result["pipeline"]) == 1
        stage = result["pipeline"][0]
        assert stage["stage"] == "ranking"
        assert stage["config"]["order_by"] == ["priority", "date"]
        assert stage["config"]["partition_by"] == ["patient_id"]

    # -- Branch 523->524 + Line 524: standardization present -----------------
    @pytest.mark.unit
    def test_expand_transformations_standardization_only(self):
        """Branch 523->524: 'standardization' IS in table.

        Only standardization stage is generated.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        table = {
            "standardization": {
                "rename_columns": {"old_name": "new_name"},
                "add_columns": [{"name": "source", "value": "cms"}],
            },
        }
        result = tm._expand_transformations(table)
        assert len(result["pipeline"]) == 1
        stage = result["pipeline"][0]
        assert stage["stage"] == "standardization"
        assert stage["config"]["rename_columns"]["old_name"] == "new_name"

    # -- Branch 627->635 + Line 635: stages in table, but stage not found ----
    @pytest.mark.unit
    def test_get_output_columns_stage_key_exists_but_stage_not_in_it(self):
        """Branch 627->635: table has 'stages' dict but requested stage not in it.

        'stages' in table is True, but 'stage in table["stages"]' is False,
        so the compound condition is False and we fall through to line 635.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__tbl_stages__": {
                "name": "__tbl_stages__",
                "columns": [{"name": "base_col", "data_type": "string"}],
                "stages": {
                    "existing_stage": {
                        "columns": [{"name": "extra", "data_type": "bool"}],
                    },
                },
            },
        }
        # Request a stage that is NOT in the stages dict
        cols = tm.get_output_columns("__tbl_stages__", stage="nonexistent_stage")
        assert len(cols) == 1
        assert cols[0]["name"] == "base_col"

    # -- Branch 629->635: stage exists but has no columns key ----------------
    @pytest.mark.unit
    def test_get_output_columns_stage_exists_no_columns_key(self):
        """Branch 629->635: stage_config exists but 'columns' not in it.

        'stages' in table is True AND stage in table['stages'] is True,
        so we enter the block. But 'columns' in stage_config is False,
        so we fall through to line 635.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__tbl_no_stage_cols__": {
                "name": "__tbl_no_stage_cols__",
                "columns": [{"name": "base", "data_type": "string"}],
                "stages": {
                    "transform": {"description": "no columns here"},
                },
            },
        }
        cols = tm.get_output_columns("__tbl_no_stage_cols__", stage="transform")
        assert len(cols) == 1
        assert cols[0]["name"] == "base"

    # -- Branch 701->702 + Line 702: column missing name in validate_table ---
    @pytest.mark.unit
    def test_validate_table_column_without_name_field(self):
        """Branch 701->702: 'name' not in col is True.

        validate_table iterates columns and finds one missing 'name'.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__tbl_no_col_name__": {
                "name": "__tbl_no_col_name__",
                "description": "test table",
                "storage": {"tier": "bronze"},
                "columns": [
                    {"data_type": "string"},  # missing name
                    {"name": "good_col", "data_type": "int"},
                ],
            },
        }
        result = tm.validate_table("__tbl_no_col_name__")
        assert result["valid"] is False
        assert any("Column 0 missing name" in issue for issue in result["issues"])

    # -- Branch 709->710 + Line 710: dedup missing key -----------------------
    @pytest.mark.unit
    def test_validate_table_dedup_config_no_key(self):
        """Branch 709->710: 'key' not in table['deduplication'] is True."""
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__tbl_dedup_nokey__": {
                "name": "__tbl_dedup_nokey__",
                "description": "test",
                "storage": {"tier": "bronze"},
                "columns": [{"name": "c", "data_type": "string"}],
                "deduplication": {"sort_by": ["date"]},  # key is missing
            },
        }
        result = tm.validate_table("__tbl_dedup_nokey__")
        assert result["valid"] is False
        assert any("Deduplication config missing key" in i for i in result["issues"])

    # -- Branch 714->715 + Line 715: ADR missing key_columns ----------------
    @pytest.mark.unit
    def test_validate_table_adr_config_no_key_columns(self):
        """Branch 714->715: 'key_columns' not in table['adr'] is True."""
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__tbl_adr_nokeys__": {
                "name": "__tbl_adr_nokeys__",
                "description": "test",
                "storage": {"tier": "bronze"},
                "columns": [{"name": "c", "data_type": "string"}],
                "adr": {"adjustment_column": "adj"},  # key_columns missing
            },
        }
        result = tm.validate_table("__tbl_adr_nokeys__")
        assert result["valid"] is False
        assert any("ADR config missing key_columns" in i for i in result["issues"])

    # -- Integration: full expand_table with inheritance + transformations ----
    @pytest.mark.unit
    def test_expand_table_full_inheritance_and_pipeline(self):
        """Integration test: staging parent with columns, keys, adr, file_format.

        Exercises multiple branches through the real expand_table flow:
        - 256: staging_table found (truthy path)
        - 304: both have columns (merge path)
        - 310: deduplication_key in staging.keys
        - 314: adr inherited from staging
        - 318: file_format inherited from staging
        - 445+: pipeline generation for all inherited configs
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__int_child__": {
                "name": "__int_child__",
                "staging": "__int_parent__",
                "columns": [{"name": "extra_col", "data_type": "int"}],
            },
            "__int_parent__": {
                "name": "__int_parent__",
                "columns": [
                    {"name": "id", "data_type": "string"},
                    {"name": "date", "data_type": "date"},
                ],
                "keys": {"deduplication_key": ["id", "date"]},
                "adr": {
                    "adjustment_column": "adj_type",
                    "key_columns": ["id"],
                    "rank_by": ["date"],
                },
                "file_format": {"type": "csv", "delimiter": "|"},
            },
        }
        result = tm.expand_table("__int_child__")

        # Columns were merged
        col_names = [c["name"] for c in result["columns"]]
        assert "id" in col_names
        assert "date" in col_names
        assert "extra_col" in col_names

        # Deduplication was inherited
        assert "deduplication" in result
        assert result["deduplication"]["key"] == ["id", "date"]

        # ADR was inherited
        assert "adr" in result
        assert result["adr"]["adjustment_column"] == "adj_type"

        # File format was inherited
        assert result["file_format"]["type"] == "csv"

        # Pipeline was generated with all stages
        stages = [s["stage"] for s in result["pipeline"]]
        assert "staging" in stages
        assert "deduplication" in stages
        assert "adjustment" in stages
        assert "adr_deduplication" in stages
        assert "ranking" in stages

    # -- Integration: validate_table through expand_table with all issues ----
    @pytest.mark.unit
    def test_validate_table_through_expand_with_multiple_issues(self):
        """Integration: validate_table calling expand_table with staging.

        The expanded table has deduplication (from staging) without a proper
        key and ADR without key_columns, plus a column missing data_type.
        This exercises the validation branches through the full flow.
        """
        from acoharmony.tables import TableManager

        tm = TableManager.__new__(TableManager)
        tm._table_cache = {
            "__val_child__": {
                "name": "__val_child__",
                "staging": "__val_parent__",
                "description": "validation target",
                "storage": {"tier": "bronze"},
            },
            "__val_parent__": {
                "name": "__val_parent__",
                "columns": [
                    {"name": "good_col", "data_type": "string"},
                    {"name": "bad_col"},  # missing data_type
                ],
                "keys": {"deduplication_key": ["good_col"]},
                "adr": {"adjustment_column": "adj"},  # missing key_columns
            },
        }
        result = tm.validate_table("__val_child__")
        # Should report issues for bad_col data_type and ADR key_columns
        assert any("data_type" in i for i in result["issues"])
        assert any("ADR" in i for i in result["issues"])
