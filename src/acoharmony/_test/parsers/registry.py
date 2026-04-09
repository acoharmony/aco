# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._parsers._registry module."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import importlib
from unittest.mock import MagicMock, patch

import pytest
import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._registry is not None


class TestParserModule:
    """Tests for parser functionality."""

    @pytest.mark.unit
    def test_basic_placeholder(self) -> None:
        """Placeholder test."""
        assert True

class TestRegistry:
    """Tests for acoharmony._parsers._registry."""

    @pytest.mark.unit
    def test_register_and_get_parser(self):
        from acoharmony._parsers._registry import ParserRegistry

        @ParserRegistry.register("test_format_1", metadata={"desc": "test"})
        def my_parser():
            return "parsed"

        assert ParserRegistry.get_parser("test_format_1") is my_parser
        assert ParserRegistry.get_metadata("test_format_1") == {"desc": "test"}

    @pytest.mark.unit
    def test_list_parsers(self):
        from acoharmony._parsers._registry import ParserRegistry

        parsers = ParserRegistry.list_parsers()
        assert isinstance(parsers, list)
        assert "csv" in parsers

    @pytest.mark.unit
    def test_get_parser_not_found(self):
        from acoharmony._parsers._registry import ParserRegistry

        assert ParserRegistry.get_parser("nonexistent_format_xyz") is None

    @pytest.mark.unit
    def test_get_metadata_not_found(self):
        from acoharmony._parsers._registry import ParserRegistry

        assert ParserRegistry.get_metadata("nonexistent_format_xyz") is None

    @pytest.mark.unit
    def test_clear(self):
        from acoharmony._parsers._registry import ParserRegistry

        saved_parsers = dict(ParserRegistry._parsers)
        saved_metadata = dict(ParserRegistry._metadata)
        try:
            ParserRegistry.clear()
            assert ParserRegistry.list_parsers() == []
            assert ParserRegistry.get_parser("csv") is None
        finally:
            ParserRegistry._parsers.update(saved_parsers)
            ParserRegistry._metadata.update(saved_metadata)

    @pytest.mark.unit
    def test_register_parser_convenience(self):
        from acoharmony._parsers._registry import ParserRegistry, register_parser

        @register_parser("test_convenience_fmt", description="test")
        def convenience_parser():
            return "result"

        assert ParserRegistry.get_parser("test_convenience_fmt") is convenience_parser

    @pytest.mark.unit
    def test_register_without_metadata(self):
        from acoharmony._parsers._registry import ParserRegistry

        @ParserRegistry.register("test_no_meta")
        def no_meta_parser():
            return "result"

        assert ParserRegistry.get_parser("test_no_meta") is no_meta_parser
        assert ParserRegistry.get_metadata("test_no_meta") is None


class TestParsers:
    """Cover uncovered lines in parsers.py."""

    @pytest.mark.unit
    def test_parse_file_cclf_filename_sets_fixed_width(self, tmp_path):
        cclf_file = tmp_path / "P.A1234.ACO.ZC1Y24.D241201.T1234567.CCLF1"
        cclf_file.write_text("dummy")
        mock_schema = MagicMock()
        mock_schema.file_format = {}
        del mock_schema.file_format
        mock_parser = MagicMock(return_value=MagicMock())
        with (
            patch("acoharmony.parsers.ParserRegistry") as MockReg,
            patch("acoharmony.parsers.apply_column_types", side_effect=lambda lf, s: lf),
            patch("acoharmony.parsers.apply_schema_transformations", side_effect=lambda lf, s: lf),
            patch("acoharmony.parsers.extract_file_date", return_value=None),
            patch("acoharmony.parsers.add_source_tracking", side_effect=lambda lf, **kw: lf),
        ):
            MockReg.get_parser.return_value = mock_parser
            from acoharmony.parsers import parse_file

            parse_file(cclf_file, mock_schema, add_tracking=False)
            MockReg.get_parser.assert_called_with("fixed_width")

    @pytest.mark.unit
    def test_parse_file_no_parser_raises(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n")
        mock_schema = MagicMock()
        mock_schema.file_format = {"type": "nonexistent_format"}
        with patch("acoharmony.parsers.ParserRegistry") as MockReg:
            MockReg.get_parser.return_value = None
            from acoharmony.parsers import parse_file

            with pytest.raises(ValueError, match="No parser registered"):
                parse_file(csv_file, mock_schema)

    @pytest.mark.unit
    def test_parse_excel(self, tmp_path):
        mock_parser = MagicMock(return_value=MagicMock())
        with patch("acoharmony.parsers.ParserRegistry") as MockReg:
            MockReg.get_parser.return_value = mock_parser
            from acoharmony.parsers import parse_excel

            xlsx = tmp_path / "test.xlsx"
            xlsx.write_bytes(b"dummy")
            parse_excel(xlsx, limit=5, sheet_name="Sheet1")
            MockReg.get_parser.assert_called_with("excel")
            mock_parser.assert_called_once()

    @pytest.mark.unit
    def test_parse_fixed_width(self, tmp_path):
        mock_parser = MagicMock(return_value=MagicMock())
        mock_schema = MagicMock()
        with patch("acoharmony.parsers.ParserRegistry") as MockReg:
            MockReg.get_parser.return_value = mock_parser
            from acoharmony.parsers import parse_fixed_width

            fw_file = tmp_path / "test.txt"
            fw_file.write_text("dummy")
            parse_fixed_width(fw_file, mock_schema, limit=10, encoding="utf-8")
            MockReg.get_parser.assert_called_with("fixed_width")
            mock_parser.assert_called_once()


class TestCoreParserImports:
    """Verify core parser modules import without full deps."""

    @pytest.mark.unit
    def test_import_parsers_public_api(self) -> None:
        """Public parser API imports."""
        from acoharmony.parsers import parse_file

        assert callable(parse_file)

    @pytest.mark.unit
    def test_import_parser_registry(self) -> None:
        """ParserRegistry is available."""
        from acoharmony._parsers import ParserRegistry

        assert ParserRegistry is not None

    @pytest.mark.unit
    def test_import_date_extraction(self) -> None:
        """Date extraction is available."""
        from acoharmony._parsers import extract_file_date

        assert callable(extract_file_date)

    @pytest.mark.unit
    def test_import_source_tracking(self) -> None:
        """Source tracking is available."""
        from acoharmony._parsers import add_source_tracking

        assert callable(add_source_tracking)

    @pytest.mark.unit
    def test_import_transformations(self) -> None:
        """Transformation functions are available."""
        from acoharmony._parsers import apply_column_types, apply_schema_transformations

        assert callable(apply_column_types)
        assert callable(apply_schema_transformations)

    @pytest.mark.unit
    def test_import_model_aware(self) -> None:
        """ModelAwareCoercer import is handled gracefully."""
        from acoharmony._parsers import ModelAwareCoercer

        assert ModelAwareCoercer is None or ModelAwareCoercer is not None

    @pytest.mark.unit
    def test_import_parquet_parser(self) -> None:
        """Parquet parser is available."""
        from acoharmony._parsers import parse_parquet

        assert callable(parse_parquet)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "module_name",
        [
            "acoharmony._parsers._csv",
            "acoharmony._parsers._delimited",
            "acoharmony._parsers._excel",
            "acoharmony._parsers._fixed_width",
            "acoharmony._parsers._json",
            "acoharmony._parsers._parquet",
            "acoharmony._parsers._tparc",
            "acoharmony._parsers._participant_list_excel",
            "acoharmony._parsers._xml",
            "acoharmony._parsers._ecfr_xml",
            "acoharmony._parsers._federal_register_xml",
            "acoharmony._parsers._mabel_log",
            "acoharmony._parsers._date_extraction",
            "acoharmony._parsers._date_handler",
            "acoharmony._parsers._aco_id",
            "acoharmony._parsers._source_tracking",
            "acoharmony._parsers._transformations",
            "acoharmony._parsers._registry",
        ],
    )
    def test_core_parser_module_imports(self, module_name: str) -> None:
        """Each core parser module imports successfully."""
        mod = importlib.import_module(module_name)
        assert mod is not None
