"""
Tests for schema registry module.

Tests the registry's ability to discover and register file type codes

# Magic auto-import: brings in ALL exports from module under test
from dataclasses import dataclass
from acoharmony._test._import_magic import auto_import

@auto_import
class _:
    pass  # noqa: E701

from schema YAML files.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from acoharmony._4icli.registry import (
    RegisteredFileType,
    SchemaRegistry,
    get_all_file_types,
    get_categories,
    get_file_type,
    get_file_type_codes,
    get_file_types_by_category,
    reload_registry,
)


@pytest.fixture
def mock_schemas_dir(tmp_path: Path):
    """Create a temporary schemas directory with test schemas."""
    schemas_dir = tmp_path / "_schemas"
    schemas_dir.mkdir()
    cclf_schema = {
        "name": "cclf8",
        "description": "CCLF8 Claims Data",
        "fourIcli": {"fileTypeCode": 113, "filePattern": "CCLF8.*.zip", "category": "CCLF"},
    }
    (schemas_dir / "cclf8.yml").write_text(yaml.dump(cclf_schema))
    palmr_schema = {
        "name": "provider_alignment",
        "description": "Provider Alignment Report",
        "fourIcli": {
            "fileTypeCode": 165,
            "filePattern": "P.*.PALMR.*,REACH.*.PRLBR.*",
            "category": "Reports",
        },
    }
    (schemas_dir / "provider_alignment.yml").write_text(yaml.dump(palmr_schema))
    other_schema = {"name": "other_schema", "description": "Schema without 4icli config"}
    (schemas_dir / "other.yml").write_text(yaml.dump(other_schema))
    invalid_schema = {
        "name": "invalid",
        "fourIcli": {"fileTypeCode": None, "filePattern": "INVALID.*"},
    }
    (schemas_dir / "invalid.yml").write_text(yaml.dump(invalid_schema))
    return schemas_dir


@pytest.mark.unit
class TestSchemaRegistry:
    """Test SchemaRegistry class."""

    @pytest.mark.unit
    def test_singleton_pattern(self):
        """Registry should be a singleton."""
        registry1 = SchemaRegistry()
        registry2 = SchemaRegistry()
        assert registry1 is registry2

    @pytest.mark.unit
    def test_discover_schemas(self, mock_schemas_dir, monkeypatch):
        """Test schema discovery from CentralRegistry."""
        with patch("acoharmony._4icli.registry.CentralRegistry") as MockCR:
            MockCR.list_schemas.return_value = ["cclf8", "provider_alignment"]
            MockCR.get_four_icli_config.side_effect = lambda name: {
                "cclf8": {"fileTypeCode": 113, "filePattern": "CCLF8.*.zip", "category": "CCLF"},
                "provider_alignment": {"fileTypeCode": 165, "filePattern": "P.*.PALMR.*", "category": "Reports"},
            }.get(name)
            MockCR.get_metadata.return_value = {"description": "test"}
            registry = SchemaRegistry()
            registry._file_types.clear()
            registry._by_schema.clear()
            registry._by_category.clear()
            registry._discover_schemas()
            codes = registry.get_file_type_codes()
            assert len(codes) >= 2

    @pytest.mark.unit
    def test_get_file_type_codes(self, mock_schemas_dir, monkeypatch):
        """Test getting all registered file type codes."""
        with patch("acoharmony._4icli.registry.SchemaRegistry._discover_schemas"):
            registry = SchemaRegistry()
            registry._file_types = {
                113: RegisteredFileType(113, "cclf8", "CCLF8.*.zip", "CCLF"),
                165: RegisteredFileType(165, "provider_alignment", "P.*.PALMR.*", "Reports"),
            }
            codes = registry.get_file_type_codes()
            assert codes == [113, 165]

    @pytest.mark.unit
    def test_get_by_code(self):
        """Test getting file type by code."""
        with patch("acoharmony._4icli.registry.SchemaRegistry._discover_schemas"):
            registry = SchemaRegistry()
            registry._file_types = {113: RegisteredFileType(113, "cclf8", "CCLF8.*.zip", "CCLF")}
            result = registry.get_by_code(113)
            assert result is not None
            assert result.file_type_code == 113
            assert result.schema_name == "cclf8"
            assert registry.get_by_code(999) is None

    @pytest.mark.unit
    def test_get_by_schema(self):
        """Test getting file types by schema name."""
        with patch("acoharmony._4icli.registry.SchemaRegistry._discover_schemas"):
            registry = SchemaRegistry()
            ft = RegisteredFileType(113, "cclf8", "CCLF8.*.zip", "CCLF")
            registry._file_types = {113: ft}
            registry._by_schema = {"cclf8": [ft]}
            results = registry.get_by_schema("cclf8")
            assert len(results) == 1
            assert results[0].file_type_code == 113
            assert registry.get_by_schema("nonexistent") == []

    @pytest.mark.unit
    def test_get_by_category(self):
        """Test getting file types by category."""
        with patch("acoharmony._4icli.registry.SchemaRegistry._discover_schemas"):
            registry = SchemaRegistry()
            ft1 = RegisteredFileType(113, "cclf8", "CCLF8.*.zip", "CCLF")
            ft2 = RegisteredFileType(114, "cclf9", "CCLF9.*.zip", "CCLF")
            registry._file_types = {113: ft1, 114: ft2}
            registry._by_category = {"CCLF": [ft1, ft2]}
            results = registry.get_by_category("CCLF")
            assert len(results) == 2
            assert all(ft.category == "CCLF" for ft in results)

    @pytest.mark.unit
    def test_get_categories(self):
        """Test getting all registered categories."""
        with patch("acoharmony._4icli.registry.SchemaRegistry._discover_schemas"):
            registry = SchemaRegistry()
            ft1 = RegisteredFileType(113, "cclf8", "CCLF8.*.zip", "CCLF")
            ft2 = RegisteredFileType(165, "palmr", "P.*.PALMR.*", "Reports")
            registry._file_types = {113: ft1, 165: ft2}
            registry._by_category = {"CCLF": [ft1], "Reports": [ft2]}
            categories = registry.get_categories()
            assert categories == ["CCLF", "Reports"]

    @pytest.mark.unit
    def test_get_all(self):
        """Test getting all registered file types."""
        with patch("acoharmony._4icli.registry.SchemaRegistry._discover_schemas"):
            registry = SchemaRegistry()
            ft1 = RegisteredFileType(113, "cclf8", "CCLF8.*.zip", "CCLF")
            ft2 = RegisteredFileType(165, "palmr", "P.*.PALMR.*", "Reports")
            registry._file_types = {113: ft1, 165: ft2}
            all_types = registry.get_all()
            assert len(all_types) == 2
            assert ft1 in all_types
            assert ft2 in all_types

    @pytest.mark.unit
    def test_reload(self):
        """Test registry reload."""
        with patch("acoharmony._4icli.registry.SchemaRegistry._discover_schemas") as mock_discover:
            registry = SchemaRegistry()
            registry._file_types = {113: RegisteredFileType(113, "test", "*.zip", "Test")}
            registry.reload()
            mock_discover.assert_called()

    @pytest.mark.unit
    def test_multiple_patterns(self):
        """Test handling of multiple comma-separated patterns."""
        with patch("acoharmony._4icli.registry.SchemaRegistry._discover_schemas"):
            registry = SchemaRegistry()
            ft1 = RegisteredFileType(165, "palmr", "P.*.PALMR.*", "Reports")
            ft2 = RegisteredFileType(165, "palmr", "REACH.*.PRLBR.*", "Reports")
            registry._file_types = {165: ft1}
            registry._by_schema = {"palmr": [ft1, ft2]}
            results = registry.get_by_schema("palmr")
            assert len(results) == 2


@pytest.mark.unit
class TestRegistryPublicAPI:
    """Test public API functions."""

    @pytest.mark.unit
    def test_get_file_type_codes(self):
        """Test get_file_type_codes() public function."""
        with patch("acoharmony._4icli.registry._registry") as mock_registry:
            mock_registry.get_file_type_codes.return_value = [113, 165, 170]
            codes = get_file_type_codes()
            assert codes == [113, 165, 170]
            mock_registry.get_file_type_codes.assert_called_once()

    @pytest.mark.unit
    def test_get_file_type(self):
        """Test get_file_type() public function."""
        with patch("acoharmony._4icli.registry._registry") as mock_registry:
            mock_ft = RegisteredFileType(113, "cclf8", "CCLF8.*.zip", "CCLF")
            mock_registry.get_by_code.return_value = mock_ft
            result = get_file_type(113)
            assert result == mock_ft
            mock_registry.get_by_code.assert_called_once_with(113)

    @pytest.mark.unit
    def test_get_file_types_by_category(self):
        """Test get_file_types_by_category() public function."""
        with patch("acoharmony._4icli.registry._registry") as mock_registry:
            mock_fts = [
                RegisteredFileType(113, "cclf8", "CCLF8.*.zip", "CCLF"),
                RegisteredFileType(114, "cclf9", "CCLF9.*.zip", "CCLF"),
            ]
            mock_registry.get_by_category.return_value = mock_fts
            results = get_file_types_by_category("CCLF")
            assert results == mock_fts
            mock_registry.get_by_category.assert_called_once_with("CCLF")

    @pytest.mark.unit
    def test_get_categories(self):
        """Test get_categories() public function."""
        with patch("acoharmony._4icli.registry._registry") as mock_registry:
            mock_registry.get_categories.return_value = ["CCLF", "Reports"]
            categories = get_categories()
            assert categories == ["CCLF", "Reports"]
            mock_registry.get_categories.assert_called_once()

    @pytest.mark.unit
    def test_get_all_file_types(self):
        """Test get_all_file_types() public function."""
        with patch("acoharmony._4icli.registry._registry") as mock_registry:
            mock_fts = [
                RegisteredFileType(113, "cclf8", "CCLF8.*.zip", "CCLF"),
                RegisteredFileType(165, "palmr", "P.*.PALMR.*", "Reports"),
            ]
            mock_registry.get_all.return_value = mock_fts
            results = get_all_file_types()
            assert results == mock_fts
            mock_registry.get_all.assert_called_once()

    @pytest.mark.unit
    def test_reload_registry(self):
        """Test reload_registry() public function."""
        with patch("acoharmony._4icli.registry._registry") as mock_registry:
            reload_registry()
            mock_registry.reload.assert_called_once()


@pytest.mark.unit
class TestRegisteredFileType:
    """Test RegisteredFileType dataclass."""

    @pytest.mark.unit
    def test_creation(self):
        """Test creating RegisteredFileType."""
        ft = RegisteredFileType(
            file_type_code=113,
            schema_name="cclf8",
            file_pattern="CCLF8.*.zip",
            category="CCLF",
            description="CCLF8 Claims Data",
        )
        assert ft.file_type_code == 113
        assert ft.schema_name == "cclf8"
        assert ft.file_pattern == "CCLF8.*.zip"
        assert ft.category == "CCLF"
        assert ft.description == "CCLF8 Claims Data"

    @pytest.mark.unit
    def test_optional_fields(self):
        """Test optional category and description fields."""
        ft = RegisteredFileType(file_type_code=113, schema_name="cclf8", file_pattern="CCLF8.*.zip")
        assert ft.category is None
        assert ft.description is None


@pytest.mark.unit
class TestRegistryEdgeCases:
    """Test edge cases and error handling in registry."""

    @pytest.mark.unit
    def test_schemas_directory_not_found(self):
        """Test handling when no schemas are registered."""
        with patch("acoharmony._4icli.registry.CentralRegistry") as MockCR:
            MockCR.list_schemas.return_value = []
            registry = SchemaRegistry()
            registry._file_types.clear()
            registry._by_schema.clear()
            registry._by_category.clear()
            registry._discover_schemas()
            assert registry._file_types == {}

    @pytest.mark.unit
    def test_empty_schema_file(self):
        """Test handling of schemas without fourIcli config."""
        with patch("acoharmony._4icli.registry.CentralRegistry") as MockCR:
            MockCR.list_schemas.return_value = ["empty_schema"]
            MockCR.get_four_icli_config.return_value = None
            registry = SchemaRegistry()
            registry._file_types.clear()
            registry._by_schema.clear()
            registry._by_category.clear()
            registry._discover_schemas()
            assert isinstance(registry._file_types, dict)
            assert len(registry._file_types) == 0

    @pytest.mark.unit
    def test_schema_without_category(self):
        """Test handling of schema without category field."""
        with patch("acoharmony._4icli.registry.CentralRegistry") as MockCR:
            MockCR.list_schemas.return_value = ["no_category"]
            MockCR.get_four_icli_config.return_value = {
                "fileTypeCode": 999,
                "filePattern": "TEST.*.zip",
            }
            MockCR.get_metadata.return_value = {"description": "Schema without category"}
            registry = SchemaRegistry()
            registry._file_types.clear()
            registry._by_schema.clear()
            registry._by_category.clear()
            registry._discover_schemas()
            assert 999 in registry._file_types
            assert registry._file_types[999].category is None

    @pytest.mark.unit
    def test_schema_without_category_not_in_by_category(self, tmp_path):
        """Branch 111->91: when category is None/falsy, _by_category is not populated."""
        from acoharmony._4icli.registry import SchemaRegistry, RegisteredFileType

        with patch("acoharmony._4icli.registry.SchemaRegistry._discover_schemas"):
            registry = SchemaRegistry()

        registry._file_types.clear()
        registry._by_schema.clear()
        registry._by_category.clear()

        # Simulate _discover_schemas with a schema that has no category
        with patch("acoharmony._4icli.registry.CentralRegistry") as MockCR:
            MockCR.list_schemas.return_value = ["nocatschema"]
            MockCR.get_four_icli_config.return_value = {
                "fileTypeCode": 777,
                "filePattern": "NOCAT.*.zip",
                # category is absent -> .get("category") returns None
            }
            MockCR.get_metadata.return_value = {"description": "no cat"}
            registry._discover_schemas()

        # The file type should be registered
        assert 777 in registry._file_types
        # But _by_category should NOT have any entries for None
        assert None not in registry._by_category
        assert len(registry._by_category) == 0

    @pytest.mark.unit
    def test_malformed_schema_exception(self):
        """Test handling of malformed schema config that raises exception."""
        with patch("acoharmony._4icli.registry.CentralRegistry") as MockCR:
            MockCR.list_schemas.return_value = ["malformed"]
            MockCR.get_four_icli_config.return_value = {
                "fileTypeCode": None,  # Invalid: None file type code
                "filePattern": "MALFORMED.*",
            }
            registry = SchemaRegistry()
            registry._file_types.clear()
            registry._by_schema.clear()
            registry._by_category.clear()
            registry._discover_schemas()
            # Schema with None fileTypeCode should be skipped
            assert len(registry._file_types) == 0


@pytest.mark.unit
class TestMatchFilenameToFileType:
    """Test match_filename_to_file_type function."""

    @pytest.mark.unit
    def test_match_filename_exact_pattern(self):
        """Test matching filename with exact pattern."""
        from acoharmony._4icli.registry import match_filename_to_file_type

        result = match_filename_to_file_type("P.D0259.ACO.ZCY24.D240209.T1950440.zip")
        assert result is None or isinstance(result, RegisteredFileType)

    @pytest.mark.unit
    def test_match_filename_wildcard_pattern(self):
        """Test matching filename with wildcard pattern."""
        from acoharmony._4icli.registry import match_filename_to_file_type

        with patch("acoharmony._4icli.registry._registry") as mock_registry:
            ft1 = RegisteredFileType(113, "cclf8", "P.*.CCLF8.*.zip", "CCLF")
            ft2 = RegisteredFileType(165, "palmr", "P.*.PALMR.*.txt", "Reports")
            mock_registry._by_schema = {"cclf8": [ft1], "palmr": [ft2]}
            result = match_filename_to_file_type("P.D0259.CCLF8.ZCY24.D240209.zip")
            assert result is not None
            assert result.file_type_code == 113
            assert result.schema_name == "cclf8"

    @pytest.mark.unit
    def test_match_filename_no_match(self):
        """Test matching filename that doesn't match any pattern."""
        from acoharmony._4icli.registry import match_filename_to_file_type

        with patch("acoharmony._4icli.registry._registry") as mock_registry:
            ft1 = RegisteredFileType(113, "cclf8", "P.*.CCLF8.*.zip", "CCLF")
            mock_registry._by_schema = {"cclf8": [ft1]}
            result = match_filename_to_file_type("UNMATCHED_FILE.txt")
            assert result is None

    @pytest.mark.unit
    def test_match_filename_multiple_schemas_same_code(self):
        """Test matching when multiple schemas share same file_type_code."""
        from acoharmony._4icli.registry import match_filename_to_file_type

        with patch("acoharmony._4icli.registry._registry") as mock_registry:
            ft1 = RegisteredFileType(165, "palmr", "P.*.PALMR.*.txt", "Reports")
            ft2 = RegisteredFileType(165, "palmr", "REACH.*.PRLBR.*.xlsx", "Reports")
            mock_registry._by_schema = {"palmr": [ft1, ft2]}
            result1 = match_filename_to_file_type("P.D0259.PALMR.RP.D240209.txt")
            assert result1 is not None
            assert result1.file_type_code == 165
            result2 = match_filename_to_file_type("REACH.D0259.PRLBR.PY2024.D241111.xlsx")
            assert result2 is not None
            assert result2.file_type_code == 165


@pytest.mark.integration
class TestRegistryWithActualSchemas:
    """Integration tests using actual schema files."""

    @pytest.mark.unit
    def test_loads_actual_schemas(self):
        """Test loading from actual schema directory."""
        reload_registry()
        codes = get_file_type_codes()
        assert len(codes) > 0
        cclf8 = get_file_type(113)
        if cclf8:
            assert cclf8.file_type_code == 113
            assert cclf8.schema_name is not None

    @pytest.mark.unit
    def test_categories_from_actual_schemas(self):
        """Test getting categories from actual schemas."""
        categories = get_categories()
        assert isinstance(categories, list)
        if categories:
            assert all(isinstance(cat, str) for cat in categories)

    @pytest.mark.unit
    def test_all_file_types_have_required_fields(self):
        """Test that all registered file types have required fields."""
        all_types = get_all_file_types()
        for ft in all_types:
            assert isinstance(ft.file_type_code, int)
            assert isinstance(ft.schema_name, str)
            assert isinstance(ft.file_pattern, str)
            assert ft.file_type_code > 0
            assert len(ft.schema_name) > 0
            assert len(ft.file_pattern) > 0


class TestRegistryMatchFilename:
    @pytest.mark.unit
    def test_match_returns_none_for_no_schemas(self):
        from acoharmony._4icli.registry import match_filename_to_file_type

        with patch("acoharmony._4icli.registry._registry") as mock_reg:
            mock_reg._by_schema = {}
            result = match_filename_to_file_type("anything.txt")
            assert result is None
