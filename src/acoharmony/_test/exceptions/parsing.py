from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import pytest

# © 2025 HarmonyCares
"""Tests for acoharmony/_exceptions/_parsing.py."""



class TestParsing:
    """Test suite for _parsing."""

    @pytest.mark.unit
    def test_for_schema(self) -> None:
        """Test for_schema function."""
        exc = SchemaNotFoundError.for_schema("my_schema")
        assert "my_schema" in exc.message
        assert exc.context.why != ""
        assert exc.context.how != ""
        assert exc.context.metadata["schema_name"] == "my_schema"
        assert len(exc.context.causes) > 0
        assert len(exc.context.remediation_steps) > 0

    @pytest.mark.unit
    def test_for_file(self) -> None:
        """Test for_file function."""
        exc = InvalidFileFormatError.for_file("/tmp/test.csv", "CSV")
        assert "/tmp/test.csv" in exc.message
        assert "CSV" in exc.message
        assert exc.context.metadata["file_path"] == "/tmp/test.csv"
        assert exc.context.metadata["expected_format"] == "CSV"

    @pytest.mark.unit
    def test_for_columns(self) -> None:
        """Test for_columns function."""
        exc = MissingColumnError.for_columns(
            missing_columns=["col_a", "col_b"],
            file_path="/tmp/data.csv",
            schema_name="test_schema",
        )
        assert "col_a" in exc.message
        assert "col_b" in exc.message
        assert exc.context.metadata["missing_columns"] == ["col_a", "col_b"]
        assert exc.context.metadata["file_path"] == "/tmp/data.csv"
        assert exc.context.metadata["schema_name"] == "test_schema"

    @pytest.mark.unit
    def test_parseerror_init(self) -> None:
        """Test ParseError initialization."""
        exc = ParseError("parse failed", auto_log=False, auto_trace=False)
        assert exc.message == "parse failed"
        assert exc.error_code == "PARSE_001"
        assert exc.category == "parsing"
        assert isinstance(exc, ACOHarmonyException)

    @pytest.mark.unit
    def test_schemanotfounderror_init(self) -> None:
        """Test SchemaNotFoundError initialization."""
        exc = SchemaNotFoundError("schema missing", auto_log=False, auto_trace=False)
        assert exc.message == "schema missing"
        assert exc.error_code == "PARSE_002"
        assert exc.category == "parsing"
        assert isinstance(exc, ACOHarmonyException)

    @pytest.mark.unit
    def test_invalidfileformaterror_init(self) -> None:
        """Test InvalidFileFormatError initialization."""
        exc = InvalidFileFormatError("bad format", auto_log=False, auto_trace=False)
        assert exc.message == "bad format"
        assert exc.error_code == "PARSE_003"
        assert exc.category == "parsing"
        assert isinstance(exc, ACOHarmonyException)



# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for _parsing module."""

# TODO: Implement comprehensive tests for this module
# TODO: Add property-based tests using hypothesis where appropriate
# TODO: Ensure all edge cases are covered
# TODO: Add integration tests if needed




if TYPE_CHECKING:
    pass


class TestParseError:
    """Tests for ParseError."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_parseerror_initialization(self) -> None:
        """ParseError can be initialized."""
        exc = ParseError("test parse error", auto_log=False, auto_trace=False)
        assert exc.message == "test parse error"
        assert isinstance(exc, ACOHarmonyException)
        assert isinstance(exc, Exception)

    @pytest.mark.unit
    def test_parseerror_basic_functionality(self) -> None:
        """ParseError basic functionality works."""
        with pytest.raises(ParseError):
            raise ParseError("parse failed", auto_log=False, auto_trace=False)
        exc = ParseError("err", auto_log=False, auto_trace=False)
        assert "PARSE_001" in repr(exc)

class TestSchemaNotFoundError:
    """Tests for SchemaNotFoundError."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_schemanotfounderror_initialization(self) -> None:
        """SchemaNotFoundError can be initialized."""
        exc = SchemaNotFoundError("missing", auto_log=False, auto_trace=False)
        assert exc.message == "missing"
        assert exc.error_code == "PARSE_002"

    @pytest.mark.unit
    def test_schemanotfounderror_basic_functionality(self) -> None:
        """SchemaNotFoundError basic functionality works."""
        exc = SchemaNotFoundError.for_schema("test_schema")
        assert "test_schema" in exc.message
        assert isinstance(exc, SchemaNotFoundError)

class TestInvalidFileFormatError:
    """Tests for InvalidFileFormatError."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_invalidfileformaterror_initialization(self) -> None:
        """InvalidFileFormatError can be initialized."""
        exc = InvalidFileFormatError("bad", auto_log=False, auto_trace=False)
        assert exc.message == "bad"
        assert exc.error_code == "PARSE_003"

    @pytest.mark.unit
    def test_invalidfileformaterror_basic_functionality(self) -> None:
        """InvalidFileFormatError basic functionality works."""
        exc = InvalidFileFormatError.for_file("/tmp/f.dat", "parquet")
        assert "/tmp/f.dat" in exc.message
        assert "parquet" in exc.message

class TestMissingColumnError:
    """Tests for MissingColumnError."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_missingcolumnerror_initialization(self) -> None:
        """MissingColumnError can be initialized."""
        exc = MissingColumnError("cols missing", auto_log=False, auto_trace=False)
        assert exc.message == "cols missing"
        assert exc.error_code == "PARSE_004"

    @pytest.mark.unit
    def test_missingcolumnerror_basic_functionality(self) -> None:
        """MissingColumnError basic functionality works."""
        exc = MissingColumnError.for_columns(["a", "b"], "/tmp/d.csv", "schema1")
        assert "a" in exc.message
        assert "b" in exc.message
        assert isinstance(exc, MissingColumnError)

class TestDataTypeMismatchError:
    """Tests for DataTypeMismatchError."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_datatypemismatcherror_initialization(self) -> None:
        """DataTypeMismatchError can be initialized."""
        exc = DataTypeMismatchError("type mismatch", auto_log=False, auto_trace=False)
        assert exc.message == "type mismatch"
        assert exc.error_code == "PARSE_005"
        assert exc.category == "parsing"

    @pytest.mark.unit
    def test_datatypemismatcherror_basic_functionality(self) -> None:
        """DataTypeMismatchError basic functionality works."""
        with pytest.raises(DataTypeMismatchError):
            raise DataTypeMismatchError("mismatch", auto_log=False, auto_trace=False)
        exc = DataTypeMismatchError("m", auto_log=False, auto_trace=False)
        assert isinstance(exc, ACOHarmonyException)

class TestFixedWidthParseError:
    """Tests for FixedWidthParseError."""

    # TODO: Implement tests for this class

    @pytest.mark.unit
    def test_fixedwidthparseerror_initialization(self) -> None:
        """FixedWidthParseError can be initialized."""
        exc = FixedWidthParseError("fw error", auto_log=False, auto_trace=False)
        assert exc.message == "fw error"
        assert exc.error_code == "PARSE_006"
        assert exc.category == "parsing"

    @pytest.mark.unit
    def test_fixedwidthparseerror_basic_functionality(self) -> None:
        """FixedWidthParseError basic functionality works."""
        with pytest.raises(FixedWidthParseError):
            raise FixedWidthParseError("fw", auto_log=False, auto_trace=False)
        exc = FixedWidthParseError("fw", auto_log=False, auto_trace=False)
        assert isinstance(exc, ACOHarmonyException)
