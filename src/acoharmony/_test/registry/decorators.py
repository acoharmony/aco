# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for decorators module."""

# TODO: Implement comprehensive tests for this module
# TODO: Add property-based tests using hypothesis where appropriate
# TODO: Ensure all edge cases are covered
# TODO: Add integration tests if needed

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    pass



# Function tests

@pytest.mark.unit
def test_register_schema_basic() -> None:
    """register_schema basic functionality."""
    from acoharmony._registry.decorators import register_schema
    from acoharmony._registry.registry import SchemaRegistry

    @register_schema(name="__test_reg_schema__", version=1, tier="bronze", description="test")
    class TestModel:
        pass

    assert SchemaRegistry.get_schema("__test_reg_schema__") is TestModel
    meta = SchemaRegistry.get_metadata("__test_reg_schema__")
    assert meta["name"] == "__test_reg_schema__"
    assert meta["version"] == 1
    assert meta["tier"] == "bronze"
    assert TestModel.schema_name() == "__test_reg_schema__"


@pytest.mark.unit
def test_with_parser_basic() -> None:
    """with_parser basic functionality."""
    from acoharmony._registry.decorators import register_schema, with_parser
    from acoharmony._registry.registry import SchemaRegistry

    @register_schema(name="__test_parser_dec__", version=1, tier="bronze")
    @with_parser(type="fixed_width", encoding="utf-8")
    class TestParserModel:
        pass

    config = SchemaRegistry.get_parser_config("__test_parser_dec__")
    assert config["type"] == "fixed_width"
    assert config["encoding"] == "utf-8"
    assert TestParserModel.parser_config()["type"] == "fixed_width"


@pytest.mark.unit
def test_with_metadata_basic() -> None:
    """with_metadata basic functionality."""
    from acoharmony._registry.decorators import register_schema, with_metadata
    from acoharmony._registry.registry import SchemaRegistry

    @with_metadata(custom_field="custom_value")
    @register_schema(name="__test_metadata_dec__", version=1, tier="bronze")
    class TestMetaModel:
        pass

    meta = SchemaRegistry.get_metadata("__test_metadata_dec__")
    assert meta.get("custom_field") == "custom_value"


class TestSchemaClassMethods:
    """Cover classmethod bodies injected by register_schema (lines 112-137)."""

    @pytest.mark.unit
    def test_schema_name_method(self):
        from acoharmony._tables.cclf1 import Cclf1
        assert Cclf1.schema_name() == "cclf1"

    @pytest.mark.unit
    def test_schema_metadata_method(self):
        from acoharmony._tables.cclf1 import Cclf1
        meta = Cclf1.schema_metadata()
        assert isinstance(meta, dict)
        assert "name" in meta

    @pytest.mark.unit
    def test_schema_version_method(self):
        from acoharmony._tables.cclf1 import Cclf1
        v = Cclf1.schema_version()
        assert v is not None

    @pytest.mark.unit
    def test_schema_tier_method(self):
        from acoharmony._tables.cclf1 import Cclf1
        assert Cclf1.schema_tier() in ("bronze", "silver", "gold")

    @pytest.mark.unit
    def test_schema_description_method(self):
        from acoharmony._tables.cclf1 import Cclf1
        desc = Cclf1.schema_description()
        assert isinstance(desc, str)

    @pytest.mark.unit
    def test_get_file_patterns_method(self):
        from acoharmony._tables.cclf1 import Cclf1
        patterns = Cclf1.get_file_patterns()
        assert isinstance(patterns, dict)


class TestParserConfigMethod:
    """Cover parser_config classmethod body (line 201)."""

    @pytest.mark.unit
    def test_parser_config_method(self):
        from acoharmony._tables.cclf1 import Cclf1
        cfg = Cclf1.parser_config()
        assert isinstance(cfg, dict)


class TestWithParserBranches:
    """Cover with_parser update-registry branch (line 208)."""

    @pytest.mark.unit
    def test_with_parser_updates_registry(self):
        from acoharmony._registry.decorators import register_schema, with_parser
        from acoharmony._registry.registry import SchemaRegistry

        @with_parser(type="csv", encoding="utf-8", has_header=True)
        @register_schema(name="__test_parser_dec__", version=1, tier="bronze")
        class TestParserModel:
            pass

        parser_cfg = SchemaRegistry.get_parser_config("__test_parser_dec__")
        assert parser_cfg["type"] == "csv"


class TestExistingModelDecoratorMethods:
    """Call decorator classmethods on existing registered models to cover
    the classmethod bodies for models that were registered at import time."""

    @pytest.mark.unit
    def test_cclf1_lineage_config(self):
        from acoharmony._tables.cclf1 import Cclf1
        if hasattr(Cclf1, "lineage_config"):
            cfg = Cclf1.lineage_config()
            assert isinstance(cfg, dict)

    @pytest.mark.unit
    def test_cclf1_storage_config(self):
        from acoharmony._tables.cclf1 import Cclf1
        if hasattr(Cclf1, "storage_config"):
            cfg = Cclf1.storage_config()
            assert isinstance(cfg, dict)

    @pytest.mark.unit
    def test_enrollment_deduplication_config(self):
        from acoharmony._tables.enrollment import Enrollment
        if hasattr(Enrollment, "deduplication_config"):
            cfg = Enrollment.deduplication_config()
            assert isinstance(cfg, dict)

    @pytest.mark.unit
    def test_enrollment_xref_config(self):
        from acoharmony._tables.enrollment import Enrollment
        if hasattr(Enrollment, "xref_config"):
            cfg = Enrollment.xref_config()
            assert isinstance(cfg, dict)


class TestRemainingDecoratorClassmethods:
    """Cover all remaining decorator classmethod bodies and registry-update lines."""

    @pytest.mark.unit
    def test_with_record_types(self):
        """Cover with_record_types lines 816, 822."""
        from acoharmony._registry.decorators import register_schema, with_record_types

        @with_record_types(record_types={"CLMH": {"columns": []}})
        @register_schema(name="__test_rt__", version=1, tier="bronze")
        class TestRtModel:
            pass

        cfg = TestRtModel.record_types_config()
        assert "CLMH" in cfg["record_types"]

    @pytest.mark.unit
    def test_with_sheets(self):
        """Cover with_sheets lines 862, 868."""
        from acoharmony._registry.decorators import register_schema, with_sheets

        @with_sheets(sheets=[{"sheet_index": 0, "sheet_type": "data"}])
        @register_schema(name="__test_sheets__", version=1, tier="bronze")
        class TestSheetsModel:
            pass

        cfg = TestSheetsModel.sheets_config()
        assert len(cfg["sheets"]) == 1

    @pytest.mark.unit
    def test_with_four_icli(self):
        """Cover with_four_icli lines 922, 928."""
        from acoharmony._registry.decorators import register_schema, with_four_icli

        @with_four_icli(category="Claims", file_type_code=101)
        @register_schema(name="__test_4i__", version=1, tier="bronze")
        class TestFourIcliModel:
            pass

        cfg = TestFourIcliModel.four_icli_config()
        assert cfg["category"] == "Claims"

    @pytest.mark.unit
    def test_with_polars(self):
        """Cover with_polars lines 975, 981."""
        from acoharmony._registry.decorators import register_schema, with_polars

        @with_polars(dtypes={"col1": "str"}, infer_schema_length=1000)
        @register_schema(name="__test_polars__", version=1, tier="bronze")
        class TestPolarsModel:
            pass

        cfg = TestPolarsModel.polars_config()
        assert cfg["dtypes"]["col1"] == "str"

    @pytest.mark.unit
    def test_with_metadata_no_prior_schema(self):
        """Cover with_metadata line 1039: no existing _schema_metadata."""
        from acoharmony._registry.decorators import with_metadata

        @with_metadata(custom_key="value")
        class PlainModel:
            pass

        assert PlainModel._schema_metadata["custom_key"] == "value"

    @pytest.mark.unit
    def test_existing_model_classmethods(self):
        """Call classmethods on models that use these decorators."""
        from acoharmony._tables.tparc import Tparc
        if hasattr(Tparc, "record_types_config"):
            cfg = Tparc.record_types_config()
            assert isinstance(cfg, dict)

        from acoharmony._tables.cclf1 import Cclf1
        if hasattr(Cclf1, "four_icli_config"):
            cfg = Cclf1.four_icli_config()
            assert isinstance(cfg, dict)
        if hasattr(Cclf1, "storage_config"):
            cfg = Cclf1.storage_config()
            assert isinstance(cfg, dict)


class TestDecoratorsSheetsIsNone:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_decorators_sheets_is_none(self):
        """850->852: sheets is None."""
        from acoharmony._registry.decorators import register_schema, with_sheets
        @with_sheets()
        @register_schema(name="__test_no_sheets", version=1, tier="bronze")
        class NoSheetsModel:
            pass
        from acoharmony._registry.registry import SchemaRegistry
        SchemaRegistry._metadata.pop("__test_no_sheets", None)


class TestDecoratorNoneBranches:
    """Cover all branches where optional decorator parameters are None/falsy.

    Each test exercises the path where a parameter is NOT provided (None),
    causing the decorator to skip adding that key to the config dict.
    """

    @pytest.mark.unit
    def test_with_record_types_none(self):
        """Cover 806->809: record_types is None."""
        from acoharmony._registry.decorators import register_schema, with_record_types

        @with_record_types()
        @register_schema(name="__test_rt_none__", version=1, tier="bronze")
        class Model:
            pass

        cfg = Model.record_types_config()
        assert "record_types" not in cfg


class TestWithStorageRegistryUpdate:
    """Cover with_storage line 249: SchemaRegistry._storage[schema_name] = cfg."""

    @pytest.mark.unit
    def test_with_storage_registers_in_schema_registry(self):
        from acoharmony._registry.decorators import register_schema, with_storage
        from acoharmony._registry.registry import SchemaRegistry

        @with_storage(tier="bronze", file_patterns={"a": "*.csv"})
        @register_schema(name="__test_with_storage__", version=1, tier="bronze")
        class Model:
            pass

        try:
            assert "__test_with_storage__" in SchemaRegistry._storage
            assert SchemaRegistry._storage["__test_with_storage__"]["tier"] == "bronze"
        finally:
            SchemaRegistry._storage.pop("__test_with_storage__", None)
            SchemaRegistry._schemas.pop("__test_with_storage__", None)
            SchemaRegistry._metadata.pop("__test_with_storage__", None)


class TestWithStagingClassmethodAndRegistry:
    """Cover with_staging lines 274 and 280."""

    @pytest.mark.unit
    def test_with_staging_classmethod_returns_source(self):
        """Line 274: cls.staging_source() returns the configured source."""
        from acoharmony._registry.decorators import register_schema, with_staging

        @with_staging(source="parent_table")
        @register_schema(name="__test_with_staging_cm__", version=1, tier="silver")
        class Model:
            pass

        from acoharmony._registry.registry import SchemaRegistry

        try:
            assert Model.staging_source() == "parent_table"
        finally:
            SchemaRegistry._staging.pop("__test_with_staging_cm__", None)
            SchemaRegistry._schemas.pop("__test_with_staging_cm__", None)
            SchemaRegistry._metadata.pop("__test_with_staging_cm__", None)

    @pytest.mark.unit
    def test_with_staging_registers_source_in_schema_registry(self):
        """Line 280: SchemaRegistry._staging[name] = source."""
        from acoharmony._registry.decorators import register_schema, with_staging
        from acoharmony._registry.registry import SchemaRegistry

        @with_staging(source="upstream")
        @register_schema(name="__test_with_staging_reg__", version=1, tier="silver")
        class Model:
            pass

        try:
            assert SchemaRegistry._staging.get("__test_with_staging_reg__") == "upstream"
        finally:
            SchemaRegistry._staging.pop("__test_with_staging_reg__", None)
            SchemaRegistry._schemas.pop("__test_with_staging_reg__", None)
            SchemaRegistry._metadata.pop("__test_with_staging_reg__", None)
