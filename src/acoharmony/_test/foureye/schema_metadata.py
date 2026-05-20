# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for 4icli schema-aware metadata loading."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._4icli.inventory import _load_schema_patterns, _match_file_type_code


class TestSchemaPatternLoading:
    """Tests for loading patterns from schema files."""

    @pytest.mark.unit
    def test_load_schema_patterns(self) -> None:
        """_load_schema_patterns loads patterns from schema files."""
        patterns = _load_schema_patterns()

        # Should have patterns loaded
        assert len(patterns) > 0

        # Each pattern should have required fields
        for pattern in patterns:
            assert "pattern" in pattern
            assert "file_type_code" in pattern
            assert "schema_name" in pattern
            assert isinstance(pattern["file_type_code"], int)
            assert isinstance(pattern["pattern"], str)
            assert isinstance(pattern["schema_name"], str)

    @pytest.mark.unit
    def test_patterns_include_known_schemas(self) -> None:
        """Patterns include known file types."""
        patterns = _load_schema_patterns()

        # Extract file type codes
        type_codes = {p["file_type_code"] for p in patterns}

        # Should include CCLF (113)
        assert 113 in type_codes

        # Should include some report types
        assert 159 in type_codes or 165 in type_codes or 175 in type_codes

    @pytest.mark.unit
    def test_schemas_with_fouricli_block_are_valid(self) -> None:
        """Schemas registered with @with_four_icli have valid metadata."""
        from acoharmony import _tables as _  # noqa: F401  - populate registry
        from acoharmony._registry import SchemaRegistry

        allowed_categories = {
            "Beneficiary List",
            "CCLF",
            "Claim and Claim Line Feed (CCLF) Files",
            "Reports",
        }

        schemas_with_fouricli = []
        for name in SchemaRegistry.list_schemas():
            cfg = SchemaRegistry.get_four_icli_config(name)
            if not cfg:
                continue
            schemas_with_fouricli.append(name)
            assert "category" in cfg, f"{name} missing category"
            assert "fileTypeCode" in cfg, f"{name} missing fileTypeCode"
            assert cfg["category"] in allowed_categories, (
                f"{name} has invalid category: {cfg['category']}"
            )

        # Should have at least some schemas with fourIcli blocks
        assert len(schemas_with_fouricli) > 5


class TestFileTypeMatching:
    """Tests for matching files to type codes."""

    @pytest.mark.unit
    def test_match_cclf_file(self) -> None:
        """_match_file_type_code matches CCLF files."""
        patterns = _load_schema_patterns()

        # Test CCLF filename (actual filename from inventory)
        type_code = _match_file_type_code("P.D0259.ACO.ZCY24.D240209.T1950440.zip", patterns)

        assert type_code == 113

    @pytest.mark.unit
    def test_match_bar_file(self) -> None:
        """_match_file_type_code matches BAR files."""
        patterns = _load_schema_patterns()

        # Test BAR filename (actual filename from inventory)
        type_code = _match_file_type_code("P.D0259.ALGC24.RP.D240119.T1735222.xlsx", patterns)

        assert type_code == 159

    @pytest.mark.unit
    def test_match_palmr_file(self) -> None:
        """_match_file_type_code matches PALMR files."""
        patterns = _load_schema_patterns()

        # Test PALMR filename (actual filename from inventory)
        type_code = _match_file_type_code("P.D0259.PALMR.D240116.T1743260.csv", patterns)

        assert type_code == 165

    @pytest.mark.unit
    def test_wildcard_patterns_match_anything(self) -> None:
        """_match_file_type_code may match generic files with wildcard patterns."""
        patterns = _load_schema_patterns()

        # Many stub schemas have '*' pattern, so even generic files may match
        type_code = _match_file_type_code("random_file.txt", patterns)

        # May match a wildcard pattern or None
        assert type_code is None or isinstance(type_code, int)
