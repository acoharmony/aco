"""Tests for medallion.py — MedallionLayer and UnityCatalogNamespace."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestMedallionLayer:
    """Tests for MedallionLayer enum."""

    @pytest.mark.unit
    def test_enum_values(self):

        assert MedallionLayer.BRONZE.value == "bronze"
        assert MedallionLayer.SILVER.value == "silver"
        assert MedallionLayer.GOLD.value == "gold"

    @pytest.mark.unit
    def test_unity_schema_property(self):

        assert MedallionLayer.BRONZE.unity_schema == "bronze"
        assert MedallionLayer.SILVER.unity_schema == "silver"
        assert MedallionLayer.GOLD.unity_schema == "gold"

    @pytest.mark.unit
    def test_data_tier_property(self):

        assert MedallionLayer.BRONZE.data_tier == "bronze"
        assert MedallionLayer.SILVER.data_tier == "silver"
        assert MedallionLayer.GOLD.data_tier == "gold"

    @pytest.mark.unit
    def test_from_tier_lowercase(self):

        assert MedallionLayer.from_tier("bronze") == MedallionLayer.BRONZE
        assert MedallionLayer.from_tier("silver") == MedallionLayer.SILVER
        assert MedallionLayer.from_tier("gold") == MedallionLayer.GOLD

    @pytest.mark.unit
    def test_from_tier_uppercase(self):

        assert MedallionLayer.from_tier("BRONZE") == MedallionLayer.BRONZE
        assert MedallionLayer.from_tier("Silver") == MedallionLayer.SILVER
        assert MedallionLayer.from_tier("GOLD") == MedallionLayer.GOLD

    @pytest.mark.unit
    def test_from_tier_invalid(self):

        with pytest.raises(ValueError, match=r".*"):
            MedallionLayer.from_tier("platinum")

    @pytest.mark.unit
    def test_from_unity_schema(self):

        assert MedallionLayer.from_unity_schema("bronze") == MedallionLayer.BRONZE
        assert MedallionLayer.from_unity_schema("SILVER") == MedallionLayer.SILVER
        assert MedallionLayer.from_unity_schema("Gold") == MedallionLayer.GOLD

    @pytest.mark.unit
    def test_from_unity_schema_invalid(self):

        with pytest.raises(ValueError, match=r".*"):
            MedallionLayer.from_unity_schema("raw")


class TestUnityCatalogNamespace:
    """Tests for UnityCatalogNamespace."""

    @pytest.mark.unit
    def test_init_and_attributes(self):

        ns = UnityCatalogNamespace("main", "bronze", "cclf1")
        assert ns.catalog == "main"
        assert ns.schema == "bronze"
        assert ns.table == "cclf1"

    @pytest.mark.unit
    def test_full_name(self):

        ns = UnityCatalogNamespace("prod", "silver", "institutional_claim")
        assert ns.full_name == "prod.silver.institutional_claim"

    @pytest.mark.unit
    def test_medallion_layer(self):

        ns = UnityCatalogNamespace("main", "gold", "summary")
        assert ns.medallion_layer == MedallionLayer.GOLD

    @pytest.mark.unit
    def test_str_representation(self):

        ns = UnityCatalogNamespace("dev", "bronze", "cclf5")
        assert str(ns) == "dev.bronze.cclf5"

    @pytest.mark.unit
    def test_repr_representation(self):

        ns = UnityCatalogNamespace("dev", "bronze", "cclf5")
        expected = "UnityCatalogNamespace(catalog='dev', schema='bronze', table='cclf5')"
        assert repr(ns) == expected


class TestUnityCatalogNamespaceDeeper:
    """Cover UnityCatalogNamespace methods."""

    @pytest.mark.unit
    def test_medallion_layer_property(self):

        ns = UnityCatalogNamespace("main", "silver", "claims")
        assert ns.medallion_layer == MedallionLayer.SILVER

    @pytest.mark.unit
    def test_str(self):

        ns = UnityCatalogNamespace("main", "gold", "agg")
        assert str(ns) == "main.gold.agg"

    @pytest.mark.unit
    def test_repr(self):

        ns = UnityCatalogNamespace("main", "bronze", "raw")
        r = repr(ns)
        assert "main" in r
        assert "bronze" in r
        assert "raw" in r
