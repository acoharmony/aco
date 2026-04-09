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
def test_with_transform_basic() -> None:
    """with_transform basic functionality."""
    from acoharmony._registry.decorators import register_schema, with_transform
    from acoharmony._registry.registry import SchemaRegistry

    @register_schema(name="__test_transform_dec__", version=1, tier="silver")
    @with_transform(name="my_transform", depends_on=["cclf1"])
    class TestTransformModel:
        pass

    config = SchemaRegistry.get_transform_config("__test_transform_dec__")
    assert config["name"] == "my_transform"
    assert config["depends_on"] == ["cclf1"]


@pytest.mark.unit
def test_with_lineage_basic() -> None:
    """with_lineage basic functionality."""
    from acoharmony._registry.decorators import register_schema, with_lineage
    from acoharmony._registry.registry import SchemaRegistry

    @register_schema(name="__test_lineage_dec__", version=1, tier="silver")
    @with_lineage(depends_on=["cclf1"], produces=["gold_output"])
    class TestLineageModel:
        pass

    config = SchemaRegistry.get_lineage_config("__test_lineage_dec__")
    assert config["depends_on"] == ["cclf1"]
    assert config["produces"] == ["gold_output"]


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


class TestTransformConfigMethod:
    """Cover transform_config classmethod body (line 261)."""

    @pytest.mark.unit
    def test_transform_config_method(self):
        from acoharmony._tables.cclf1 import Cclf1
        cfg = Cclf1.transform_config()
        assert isinstance(cfg, dict)


class TestWithTransformBranches:
    """Cover with_transform branches (lines 243, 249, 268)."""

    @pytest.mark.unit
    def test_with_transform_type_param(self):
        from acoharmony._registry.decorators import register_schema, with_transform

        @with_transform(type="custom", name="my_transform", depends_on=["dep1"])
        @register_schema(name="__test_xform_dec__", version=1, tier="bronze")
        class TestXformModel:
            pass

        cfg = TestXformModel.transform_config()
        assert cfg["type"] == "custom"
        assert cfg["name"] == "my_transform"
        assert cfg["depends_on"] == ["dep1"]


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


class TestAllDecoratorBranches:
    """Cover all decorator classmethod bodies and conditional branches.

    Creates a single test model with all decorators applied, then calls each
    classmethod to exercise the injected method bodies.
    """

    @pytest.mark.unit
    def test_all_decorator_classmethods(self):
        from acoharmony._registry.decorators import (
            register_schema,
            with_deduplication,
            with_keys,
            with_lineage,
            with_parser,
            with_staging,
            with_standardization,
            with_storage,
            with_transform,
            with_tuva,
            with_xref,
        )

        @with_xref(
            table="xref_table",
            join_key="mbi",
            xref_key="prvs",
            current_column="crnt",
            output_column="resolved_mbi",
            description="MBI crosswalk",
        )
        @with_staging(source="raw_claims")
        @with_keys(
            primary_key=["id"],
            natural_key=["mbi", "date"],
            deduplication_key=["mbi"],
            foreign_keys=[{"column": "mbi", "references": "bene.mbi"}],
        )
        @with_tuva(
            models={"intermediate": ["int_enrollment"]},
            inject=["eligibility"],
        )
        @with_standardization(
            rename_columns={"old_col": "new_col"},
            add_columns=[{"name": "src", "value": "test"}],
            add_computed={"year": "extract_year(date)"},
        )
        @with_deduplication(
            key=["mbi", "date"],
            sort_by=["date"],
            keep="last",
        )
        @with_storage(
            tier="bronze",
            file_patterns={"main": "*.csv"},
            medallion_layer="bronze",
        )
        @with_lineage(
            depends_on=["source_a"],
            produces=["output_b"],
        )
        @with_transform(type="custom", name="test_xf", depends_on=["dep"])
        @with_parser(type="csv", encoding="utf-8", has_header=True)
        @register_schema(
            name="__test_all_decs__",
            version=1,
            tier="bronze",
            description="Test model with all decorators",
            file_patterns={"main": "test*.csv"},
        )
        class FullyDecoratedModel:
            pass

        # Exercise ALL injected classmethods (covers lines 112-137, 201, 261,
        # 302, 314, 321, 369, 375, 418, 424, 459-494, 534, 540, 581, 587,
        # 643, 649, 674, 680, 728, 734, 774, 780)
        assert FullyDecoratedModel.schema_name() == "__test_all_decs__"
        assert isinstance(FullyDecoratedModel.schema_metadata(), dict)
        assert FullyDecoratedModel.schema_version() == 1
        assert FullyDecoratedModel.schema_tier() == "bronze"
        assert FullyDecoratedModel.schema_description() == "Test model with all decorators"
        assert isinstance(FullyDecoratedModel.get_file_patterns(), dict)
        assert FullyDecoratedModel.parser_config()["type"] == "csv"
        assert FullyDecoratedModel.transform_config()["name"] == "test_xf"
        assert FullyDecoratedModel.lineage_config()["depends_on"] == ["source_a"]
        assert FullyDecoratedModel.storage_config()["tier"] == "bronze"
        assert FullyDecoratedModel.deduplication_config()["key"] == ["mbi", "date"]
        assert FullyDecoratedModel.standardization_config()["rename_columns"]["old_col"] == "new_col"
        assert FullyDecoratedModel.tuva_config()["inject"] == ["eligibility"]
        assert FullyDecoratedModel.xref_config()["table"] == "xref_table"
        assert FullyDecoratedModel.staging_source() == "raw_claims"
        assert FullyDecoratedModel.keys_config()["primary_key"] == ["id"]


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
    def test_with_adr_full(self):
        """Cover with_adr lines 459-494."""
        from acoharmony._registry.decorators import register_schema, with_adr

        @with_adr(
            adjustment_column="adj_type",
            amount_fields=["paid", "allowed"],
            key_columns=["claim_id"],
            sort_columns=["date"],
            sort_descending=[True],
            rank_by=["date"],
            rank_partition=["patient_id"],
        )
        @register_schema(name="__test_adr__", version=1, tier="bronze")
        class TestAdrModel:
            pass

        cfg = TestAdrModel.adr_config()
        assert cfg["adjustment_column"] == "adj_type"
        assert cfg["amount_fields"] == ["paid", "allowed"]

    @pytest.mark.unit
    def test_with_foreign_keys(self):
        """Cover with_foreign_keys lines 774, 780."""
        from acoharmony._registry.decorators import register_schema, with_foreign_keys

        @with_foreign_keys(references=[{"column": "patient_id", "table": "patients", "key": "id"}])
        @register_schema(name="__test_fk__", version=1, tier="bronze")
        class TestFkModel:
            pass

        cfg = TestFkModel.foreign_keys_config()
        assert len(cfg["references"]) == 1

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
    def test_with_sources(self):
        """Cover with_sources lines 1008, 1014."""
        from acoharmony._registry.decorators import register_schema, with_sources

        @with_sources("cclf5", "provider_list")
        @register_schema(name="__test_sources__", version=1, tier="silver")
        class TestSourcesModel:
            pass

        cfg = TestSourcesModel.sources_config()
        assert "cclf5" in cfg
        assert "provider_list" in cfg

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
    def test_with_lineage_no_depends_no_produces(self):
        """Cover 298->301: depends_on is None, produces is None."""
        from acoharmony._registry.decorators import register_schema, with_lineage

        @with_lineage()
        @register_schema(name="__test_lineage_none__", version=1, tier="bronze")
        class Model:
            pass

        cfg = Model.lineage_config()
        assert "depends_on" not in cfg
        assert "produces" not in cfg

    @pytest.mark.unit
    def test_with_deduplication_no_key_no_sort(self):
        """Cover 405->408 (key is None) and 408->411 (sort_by is None)."""
        from acoharmony._registry.decorators import register_schema, with_deduplication

        @with_deduplication()
        @register_schema(name="__test_dedup_none__", version=1, tier="bronze")
        class Model:
            pass

        cfg = Model.deduplication_config()
        assert "key" not in cfg
        assert "sort_by" not in cfg
        assert cfg["keep"] == "last"

    @pytest.mark.unit
    def test_with_adr_all_none(self):
        """Cover 462->464 through 474->477: all ADR params are None."""
        from acoharmony._registry.decorators import register_schema, with_adr

        @with_adr()
        @register_schema(name="__test_adr_none__", version=1, tier="bronze")
        class Model:
            pass

        cfg = Model.adr_config()
        assert "adjustment_column" not in cfg
        assert "amount_fields" not in cfg
        assert "key_columns" not in cfg
        assert "sort_columns" not in cfg
        assert "sort_descending" not in cfg
        assert "rank_by" not in cfg
        assert "rank_partition" not in cfg

    @pytest.mark.unit
    def test_with_adr_updates_registry_when_already_registered(self):
        """Cover 489->492: schema already registered, with_adr updates registry."""
        from acoharmony._registry.decorators import register_schema, with_adr
        from acoharmony._registry.registry import SchemaRegistry

        @register_schema(name="__test_adr_post__", version=1, tier="bronze")
        @with_adr(adjustment_column="adj")
        class Model:
            pass

        cfg = SchemaRegistry._adr.get("__test_adr_post__")
        assert cfg is not None
        assert cfg["adjustment_column"] == "adj"

    @pytest.mark.unit
    def test_with_tuva_no_models_no_inject(self):
        """Cover 569->571 (models is None) and 571->574 (inject is None)."""
        from acoharmony._registry.decorators import register_schema, with_tuva

        @with_tuva()
        @register_schema(name="__test_tuva_none__", version=1, tier="bronze")
        class Model:
            pass

        cfg = Model.tuva_config()
        assert "models" not in cfg
        assert "inject" not in cfg

    @pytest.mark.unit
    def test_with_xref_all_none(self):
        """Cover 623->625 through 633->636: all xref params are None/empty."""
        from acoharmony._registry.decorators import register_schema, with_xref

        @with_xref()
        @register_schema(name="__test_xref_none__", version=1, tier="bronze")
        class Model:
            pass

        cfg = Model.xref_config()
        assert "description" not in cfg
        assert "table" not in cfg
        assert "join_key" not in cfg
        assert "xref_key" not in cfg
        assert "current_column" not in cfg
        assert "output_column" not in cfg

    @pytest.mark.unit
    def test_with_keys_all_none(self):
        """Cover 712->714, 714->716, 718->721: all keys params are None."""
        from acoharmony._registry.decorators import register_schema, with_keys

        @with_keys()
        @register_schema(name="__test_keys_none__", version=1, tier="bronze")
        class Model:
            pass

        cfg = Model.keys_config()
        assert "primary_key" not in cfg
        assert "natural_key" not in cfg
        assert "deduplication_key" not in cfg
        assert "foreign_keys" not in cfg

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
