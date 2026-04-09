# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for registry module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import dataclasses
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from acoharmony._registry.registry import SchemaRegistry

if TYPE_CHECKING:
    pass


class TestSchemaRegistry:
    """Tests for SchemaRegistry."""

    @pytest.mark.unit
    def test_schemaregistry_initialization(self) -> None:
        """SchemaRegistry can be initialized."""
        assert hasattr(SchemaRegistry, "_schemas")
        assert hasattr(SchemaRegistry, "_metadata")
        assert hasattr(SchemaRegistry, "_parsers")
        assert hasattr(SchemaRegistry, "_transforms")

    @pytest.mark.unit
    def test_schemaregistry_basic_functionality(self) -> None:
        """SchemaRegistry basic functionality works."""
        # list_schemas returns a list
        schemas = SchemaRegistry.list_schemas()
        assert isinstance(schemas, list)
        # get_schema for non-existent returns None
        assert SchemaRegistry.get_schema("__nonexistent_test_xyz__") is None
        # get_metadata for non-existent returns empty dict
        assert SchemaRegistry.get_metadata("__nonexistent_test_xyz__") == {}


# Function tests

@pytest.mark.unit
def test_get_schema_basic() -> None:
    """get_schema basic functionality."""
    from acoharmony._registry.registry import get_schema

    # Non-existent schema returns None
    result = get_schema("__nonexistent_xyz__")
    assert result is None


@pytest.mark.unit
def test_get_parser_for_schema_basic() -> None:
    """get_parser_for_schema basic functionality."""
    from acoharmony._registry.registry import get_parser_for_schema

    # Non-existent schema returns empty dict
    result = get_parser_for_schema("__nonexistent_xyz__")
    assert result == {}


@pytest.mark.unit
def test_get_transform_for_schema_basic() -> None:
    """get_transform_for_schema basic functionality."""
    from acoharmony._registry.registry import get_transform_for_schema

    result = get_transform_for_schema("__nonexistent_xyz__")
    assert result == {}


@pytest.mark.unit
def test_get_metadata_for_schema_basic() -> None:
    """get_metadata_for_schema basic functionality."""
    from acoharmony._registry.registry import get_metadata_for_schema

    result = get_metadata_for_schema("__nonexistent_xyz__")
    assert result == {}


@pytest.mark.unit
def test_list_registered_schemas_basic() -> None:
    """list_registered_schemas basic functionality."""
    from acoharmony._registry.registry import list_registered_schemas

    result = list_registered_schemas()
    assert isinstance(result, list)


class TestGetFullTableConfigBranches:
    """Cover uncovered branches in get_full_table_config."""

    def setup_method(self):
        """Save original registry state."""
        self._orig = {
            "_schemas": SchemaRegistry._schemas.copy(),
            "_metadata": SchemaRegistry._metadata.copy(),
            "_parsers": SchemaRegistry._parsers.copy(),
            "_transforms": SchemaRegistry._transforms.copy(),
            "_lineage": SchemaRegistry._lineage.copy(),
            "_storage": SchemaRegistry._storage.copy(),
            "_deduplication": SchemaRegistry._deduplication.copy(),
            "_adr": SchemaRegistry._adr.copy(),
            "_standardization": SchemaRegistry._standardization.copy(),
            "_tuva": SchemaRegistry._tuva.copy(),
            "_xref": SchemaRegistry._xref.copy(),
            "_staging": SchemaRegistry._staging.copy(),
            "_keys": SchemaRegistry._keys.copy(),
            "_foreign_keys": SchemaRegistry._foreign_keys.copy(),
            "_sheets": SchemaRegistry._sheets.copy(),
            "_four_icli": SchemaRegistry._four_icli.copy(),
            "_polars": SchemaRegistry._polars.copy(),
            "_sources": SchemaRegistry._sources.copy(),
        }

    def teardown_method(self):
        """Restore original registry state."""
        for attr, val in self._orig.items():
            setattr(SchemaRegistry, attr, val)

    @pytest.mark.unit
    def test_parser_config_populates_file_format(self):
        """Branch 277->280: parser config is truthy, sets file_format."""
        SchemaRegistry._schemas["__test_parser"] = type("FM", (), {})
        SchemaRegistry._metadata["__test_parser"] = {"name": "__test_parser"}
        SchemaRegistry._parsers["__test_parser"] = {"type": "fixed_width", "encoding": "utf-8"}

        config = SchemaRegistry.get_full_table_config("__test_parser")
        assert "file_format" in config
        assert config["file_format"]["type"] == "fixed_width"

    @pytest.mark.unit
    def test_adr_config_populates(self):
        """Branch 301->302: adr config is truthy, sets adr."""
        SchemaRegistry._schemas["__test_adr"] = type("FM", (), {})
        SchemaRegistry._metadata["__test_adr"] = {"name": "__test_adr"}
        SchemaRegistry._adr["__test_adr"] = {"rule": "some_adr_rule"}

        config = SchemaRegistry.get_full_table_config("__test_adr")
        assert "adr" in config
        assert config["adr"]["rule"] == "some_adr_rule"

    @pytest.mark.unit
    def test_extract_columns_from_dataclass_model(self):
        """Branches 346->351, 359->360: model_cls is dataclass, columns extracted."""

        @dataclasses.dataclass
        class TestModel:
            name: str = ""
            count: int = 0

        SchemaRegistry._schemas["__test_cols"] = TestModel
        SchemaRegistry._metadata["__test_cols"] = {"name": "__test_cols"}

        config = SchemaRegistry.get_full_table_config("__test_cols")
        assert "columns" in config
        assert any(c["name"] == "name" for c in config["columns"])
        assert any(c["name"] == "count" for c in config["columns"])

    @pytest.mark.unit
    def test_extract_columns_non_dataclass(self):
        """Branch 359->360: model_cls is not a dataclass, returns empty."""
        SchemaRegistry._schemas["__test_ndc"] = type("NotDC", (), {})
        SchemaRegistry._metadata["__test_ndc"] = {"name": "__test_ndc"}

        config = SchemaRegistry.get_full_table_config("__test_ndc")
        assert "columns" not in config

    @pytest.mark.unit
    def test_extract_columns_type_map_fallthrough(self):
        """Branches 381->386: annotation doesn't match any type_map entry, defaults to 'string'."""

        @dataclasses.dataclass
        class TestModelUnknown:
            data: bytes = b""

        columns = SchemaRegistry._extract_columns(TestModelUnknown)
        assert len(columns) == 1
        assert columns[0]["data_type"] == "string"

    @pytest.mark.unit
    def test_extract_columns_with_metadata_and_description(self):
        """Branches 389->390, 391->392: dc_field.metadata truthy, pydantic_field has description."""
        mock_field_default = MagicMock()
        mock_field_default.description = "A test field"
        mock_field_default.default = dataclasses.MISSING
        mock_field_default.json_schema_extra = None

        @dataclasses.dataclass
        class TestModelMeta:
            value: str = dataclasses.field(default=mock_field_default, metadata={"info": True})

        columns = SchemaRegistry._extract_columns(TestModelMeta)
        assert len(columns) == 1
        assert columns[0].get("description") == "A test field"

    @pytest.mark.unit
    def test_extract_columns_missing_default_required(self):
        """Branch 396->397: default is MISSING, col['required'] = True."""

        @dataclasses.dataclass
        class TestModelReq:
            required_field: str = dataclasses.MISSING

        columns = SchemaRegistry._extract_columns(TestModelReq)
        assert len(columns) == 1
        assert columns[0].get("required") is True

    @pytest.mark.unit
    def test_extract_columns_with_default_value_and_description(self):
        """Branches 398->405, 391->395: default has .default and .description attrs."""
        mock_default = MagicMock()
        mock_default.default = "hello"
        mock_default.description = "A default field"
        mock_default.json_schema_extra = None
        mock_default.metadata = None

        @dataclasses.dataclass
        class TestModelDef:
            field_a: str = dataclasses.field(default=mock_default)

        columns = SchemaRegistry._extract_columns(TestModelDef)
        assert len(columns) == 1
        assert columns[0].get("default") == "hello"
        assert columns[0].get("description") == "A default field"

    @pytest.mark.unit
    def test_extract_columns_with_json_schema_extra(self):
        """Branch 398->405 final section: json_schema_extra has position keys."""
        mock_default = MagicMock()
        mock_default.default = None
        mock_default.description = None
        mock_default.json_schema_extra = {"start_pos": 0, "end_pos": 10, "length": 10}

        @dataclasses.dataclass
        class TestModelExtra:
            field_x: str = dataclasses.field(default=mock_default)

        columns = SchemaRegistry._extract_columns(TestModelExtra)
        assert len(columns) == 1
        assert columns[0].get("start_pos") == 0
        assert columns[0].get("end_pos") == 10
        assert columns[0].get("length") == 10

    @pytest.mark.unit
    def test_extract_columns_with_no_metadata_no_description(self):
        """Branch 391->395: dc_field.metadata is truthy but pydantic_field has no description."""
        mock_field_default = MagicMock()
        mock_field_default.description = ""
        mock_field_default.default = "val"
        mock_field_default.json_schema_extra = None

        @dataclasses.dataclass
        class TestModelNoDesc:
            value: str = dataclasses.field(default=mock_field_default, metadata={"info": True})

        columns = SchemaRegistry._extract_columns(TestModelNoDesc)
        assert len(columns) == 1
        # description should not be set from metadata block (empty string is falsy)
        # but may be set from the default block
        assert "description" not in columns[0] or columns[0].get("description") == ""

    @pytest.mark.unit
    def test_get_full_table_config_no_metadata(self):
        """Branch 277->280: meta is empty/falsy, skips config.update(meta)."""
        # Register schema name only in _schemas, not in _metadata
        SchemaRegistry._schemas["__test_no_meta"] = type("FM", (), {})
        # _metadata for this key is empty dict (default from get())

        config = SchemaRegistry.get_full_table_config("__test_no_meta")
        # Config should still work, just won't have metadata merged in
        assert isinstance(config, dict)

    @pytest.mark.unit
    def test_get_full_table_config_columns_already_in_config(self):
        """Branch 346->351: 'columns' already in config, skips column extraction."""

        @dataclasses.dataclass
        class TestModelSkip:
            name: str = ""

        SchemaRegistry._schemas["__test_cols_skip"] = TestModelSkip
        SchemaRegistry._metadata["__test_cols_skip"] = {
            "name": "__test_cols_skip",
            "columns": [{"name": "pre_existing_col"}],
        }

        config = SchemaRegistry.get_full_table_config("__test_cols_skip")
        # Columns from metadata should be used, not extracted from model
        assert config["columns"] == [{"name": "pre_existing_col"}]

    @pytest.mark.unit
    def test_get_full_table_config_no_model(self):
        """Branch 346->351: model_cls is None, skips column extraction."""
        # Only register metadata, no model class
        SchemaRegistry._metadata["__test_no_model"] = {
            "name": "__test_no_model",
        }
        # Don't register in _schemas

        config = SchemaRegistry.get_full_table_config("__test_no_model")
        assert "columns" not in config


class TestExtractColumns:
    """Cover _extract_columns lines 370-430."""

    @pytest.mark.unit
    def test_extract_columns_from_dataclass(self):
        """Extracts column definitions from a dataclass model."""
        import dataclasses

        @dataclasses.dataclass
        class MockModel:
            name: str = ""
            age: int = 0
            active: bool = False
            score: float = 0.0

        columns = SchemaRegistry._extract_columns(MockModel)
        assert len(columns) == 4
        names = [c["name"] for c in columns]
        assert "name" in names
        assert "age" in names
        # Check type mapping
        name_col = next(c for c in columns if c["name"] == "name")
        assert name_col["data_type"] == "string"
        age_col = next(c for c in columns if c["name"] == "age")
        assert age_col["data_type"] == "integer"

    @pytest.mark.unit
    def test_extract_columns_non_dataclass(self):
        """Non-dataclass returns empty list."""

        class NotADataclass:
            pass

        columns = SchemaRegistry._extract_columns(NotADataclass)
        assert columns == []

    @pytest.mark.unit
    def test_extract_columns_with_date_field(self):
        """Date fields get date_format default."""
        import dataclasses
        from datetime import date

        @dataclasses.dataclass
        class DateModel:
            created: date = None

        columns = SchemaRegistry._extract_columns(DateModel)
        assert len(columns) == 1
        assert columns[0]["data_type"] == "date"
        assert columns[0]["date_format"] == "%Y-%m-%d"

    @pytest.mark.unit
    def test_extract_columns_with_json_schema_extra(self):
        """json_schema_extra position metadata is extracted."""
        import dataclasses
        from pydantic import Field

        @dataclasses.dataclass
        class PosModel:
            col_a: str = Field(
                default="",
                json_schema_extra={"start_pos": 1, "end_pos": 10, "length": 10},
            )

        columns = SchemaRegistry._extract_columns(PosModel)
        assert len(columns) == 1
        assert columns[0]["start_pos"] == 1
        assert columns[0]["end_pos"] == 10


class TestFullTableConfigBranches:
    """Cover remaining branches in get_full_table_config."""

    @pytest.mark.unit
    def test_four_icli_branch(self):
        """Cover line 344-346: four_icli config is included."""
        SchemaRegistry._metadata["__test_4i"] = {"name": "__test_4i"}
        SchemaRegistry._four_icli["__test_4i"] = {"category": "Claims"}

        config = SchemaRegistry.get_full_table_config("__test_4i")
        assert config["fourIcli"]["category"] == "Claims"
        # Cleanup
        SchemaRegistry._metadata.pop("__test_4i", None)
        SchemaRegistry._four_icli.pop("__test_4i", None)

    @pytest.mark.unit
    def test_polars_and_sources_branches(self):
        """Cover lines 349-354: polars and sources config included."""
        SchemaRegistry._metadata["__test_ps"] = {"name": "__test_ps"}
        SchemaRegistry._polars["__test_ps"] = {"dtypes": {"col": "str"}}
        SchemaRegistry._sources["__test_ps"] = {"main": "file.parquet"}

        config = SchemaRegistry.get_full_table_config("__test_ps")
        assert "polars" in config
        assert "sources" in config
        # Cleanup
        SchemaRegistry._metadata.pop("__test_ps", None)
        SchemaRegistry._polars.pop("__test_ps", None)
        SchemaRegistry._sources.pop("__test_ps", None)


class TestConvenienceFunctions:
    """Cover convenience functions at module level lines 524-539."""

    @pytest.mark.unit
    def test_get_record_types_for_schema(self):
        from acoharmony._registry.registry import get_record_types_for_schema
        result = get_record_types_for_schema("tparc")
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_get_sheets_for_schema(self):
        from acoharmony._registry.registry import get_sheets_for_schema
        result = get_sheets_for_schema("reach_bnmr")
        assert isinstance(result, (dict, type(None)))

    @pytest.mark.unit
    def test_get_four_icli_for_schema(self):
        from acoharmony._registry.registry import get_four_icli_for_schema
        result = get_four_icli_for_schema("cclf1")
        assert isinstance(result, (dict, type(None)))

    @pytest.mark.unit
    def test_get_full_table_config(self):
        from acoharmony._registry.registry import get_full_table_config
        result = get_full_table_config("cclf1")
        assert isinstance(result, dict)


class TestRegistryListMethods:
    """Cover list_schemas_by_tier (185) and list_schemas_by_parser (198)."""

    @pytest.mark.unit
    def test_list_schemas_by_tier(self):
        result = SchemaRegistry.list_schemas_by_tier("bronze")
        assert isinstance(result, list)
        # Also test with a tier that exists in the metadata
        all_tiers = {m.get("tier") for m in SchemaRegistry._metadata.values() if m.get("tier")}
        if all_tiers:
            result2 = SchemaRegistry.list_schemas_by_tier(next(iter(all_tiers)))
            assert len(result2) > 0

    @pytest.mark.unit
    def test_list_schemas_by_parser(self):
        all_types = {c.get("type") for c in SchemaRegistry._parsers.values() if c.get("type")}
        if all_types:
            result = SchemaRegistry.list_schemas_by_parser(next(iter(all_types)))
            assert isinstance(result, list)
            assert len(result) > 0
        else:
            result = SchemaRegistry.list_schemas_by_parser("nonexistent")
            assert result == []


class TestRegistryClear:
    """Cover clear() lines 435-453."""

    @pytest.mark.unit
    def test_clear_and_restore(self):
        import copy

        # Save state
        saved = {
            attr: copy.deepcopy(getattr(SchemaRegistry, attr))
            for attr in [
                "_schemas", "_metadata", "_parsers", "_transforms", "_lineage",
                "_storage", "_deduplication", "_adr", "_standardization", "_tuva",
                "_xref", "_staging", "_keys", "_foreign_keys", "_record_types",
                "_sheets", "_four_icli", "_polars", "_sources",
            ]
        }

        SchemaRegistry.clear()
        assert len(SchemaRegistry._schemas) == 0
        assert len(SchemaRegistry._metadata) == 0
        assert len(SchemaRegistry._parsers) == 0
        assert len(SchemaRegistry._four_icli) == 0
        assert len(SchemaRegistry._sources) == 0

        # Restore
        for attr, data in saved.items():
            setattr(SchemaRegistry, attr, data)


class TestRemainingConvenienceFunctions:
    """Cover convenience functions lines 484-519."""

    @pytest.mark.unit
    def test_get_storage_for_schema(self):
        from acoharmony._registry.registry import get_storage_for_schema
        result = get_storage_for_schema("cclf1")
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_get_deduplication_for_schema(self):
        from acoharmony._registry.registry import get_deduplication_for_schema
        result = get_deduplication_for_schema("cclf1")
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_get_adr_for_schema(self):
        from acoharmony._registry.registry import get_adr_for_schema
        result = get_adr_for_schema("cclf1")
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_get_standardization_for_schema(self):
        from acoharmony._registry.registry import get_standardization_for_schema
        result = get_standardization_for_schema("cclf1")
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_get_tuva_for_schema(self):
        from acoharmony._registry.registry import get_tuva_for_schema
        result = get_tuva_for_schema("enrollment")
        assert isinstance(result, (dict, type(None)))

    @pytest.mark.unit
    def test_get_xref_for_schema(self):
        from acoharmony._registry.registry import get_xref_for_schema
        result = get_xref_for_schema("enrollment")
        assert isinstance(result, (dict, type(None)))

    @pytest.mark.unit
    def test_get_staging_for_schema(self):
        from acoharmony._registry.registry import get_staging_for_schema
        result = get_staging_for_schema("nonexistent")
        assert result is None

    @pytest.mark.unit
    def test_get_keys_for_schema(self):
        from acoharmony._registry.registry import get_keys_for_schema
        result = get_keys_for_schema("cclf1")
        assert isinstance(result, (dict, type(None)))
