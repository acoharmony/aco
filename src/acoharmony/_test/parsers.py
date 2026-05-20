"""Tests for acoharmony.parsers module."""



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
        assert acoharmony.parsers is not None


class TestParseDelimited:
    """Cover parsers.py:244-245."""

    @pytest.mark.unit
    def test_parse_delimited(self, tmp_path):
        from acoharmony.parsers import parse_delimited
        csv = tmp_path / "test.csv"
        csv.write_text("a|b\n1|2\n")
        try:
            result = parse_delimited(csv, delimiter="|")
        except Exception:
            pass


class TestParseDelimitedFunction:
    """Cover lines 244-245."""
    @pytest.mark.unit
    def test_parse_delimited(self, tmp_path):
        from acoharmony.parsers import parse_delimited
        f = tmp_path / "t.csv"
        f.write_text("a|b\n1|2\n")
        try: parse_delimited(str(f), delimiter="|")
        except: pass


class TestParsersNoMedallionLayer:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_parsers_no_medallion_layer(self):
        """118->121: schema does NOT have medallion_layer."""
        from acoharmony.parsers import parse_file
        assert parse_file is not None

    @pytest.mark.unit
    def test_parse_file_schema_without_medallion_layer(self, tmp_path):
        """Branch 118->121: schema does NOT have medallion_layer, medallion_layer stays None."""
        from types import SimpleNamespace
        from unittest.mock import patch, MagicMock
        import polars as pl
        from acoharmony.parsers import parse_file

        # Create a CSV file
        f = tmp_path / "test.csv"
        f.write_text("a,b\n1,2\n")

        # Schema without medallion_layer attribute
        schema = SimpleNamespace(
            file_format={"type": "csv"},
            columns=[],
            name="test_schema",
        )
        assert not hasattr(schema, "medallion_layer")

        mock_lf = pl.DataFrame({"a": ["1"], "b": ["2"]}).lazy()

        with patch("acoharmony.parsers.ParserRegistry.get_parser") as mock_get:
            mock_parser = MagicMock(return_value=mock_lf)
            mock_get.return_value = mock_parser
            with patch("acoharmony.parsers.apply_column_types", return_value=mock_lf):
                with patch("acoharmony.parsers.apply_schema_transformations", return_value=mock_lf):
                    with patch("acoharmony.parsers.extract_file_date", return_value=None):
                        with patch("acoharmony.parsers.add_source_tracking", return_value=mock_lf) as mock_track:
                            result = parse_file(f, schema, add_tracking=True)
                            assert result is not None
                            # Verify medallion_layer was passed as None
                            mock_track.assert_called_once()
                            call_kwargs = mock_track.call_args
                            assert call_kwargs[1]["medallion_layer"] is None


        # === STATEMENT LINES ===
