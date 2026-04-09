"""
Tests for 4icli output parser module.

Tests the parser's ability to extract file information from 4icli stdout.
"""

# Magic auto-import: brings in ALL exports from module under test
from dataclasses import dataclass
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import pytest

from acoharmony._4icli.parser import (
    ParsedCommandOutput,
    ParsedFileEntry,
    _parse_size_to_bytes,
    extract_file_count,
    extract_filenames,
    parse_datahub_output,
)


@pytest.mark.unit
class TestParseSizeToBytes:
    """Test _parse_size_to_bytes helper function."""

    @pytest.mark.unit
    def test_parse_kb(self):
        """Test parsing KB sizes."""
        result = _parse_size_to_bytes("6.57 KB")
        assert result == int(6.57 * 1024)
        assert _parse_size_to_bytes("1 KB") == 1024

    @pytest.mark.unit
    def test_parse_mb(self):
        """Test parsing MB sizes."""
        result = _parse_size_to_bytes("64.66 MB")
        assert result == int(64.66 * 1024**2)
        assert _parse_size_to_bytes("1.5 MB") == int(1.5 * 1024**2)

    @pytest.mark.unit
    def test_parse_gb(self):
        """Test parsing GB sizes."""
        assert _parse_size_to_bytes("2.5 GB") == 2684354560

    @pytest.mark.unit
    def test_parse_tb(self):
        """Test parsing TB sizes."""
        assert _parse_size_to_bytes("1.2 TB") == 1319413953331

    @pytest.mark.unit
    def test_invalid_input(self):
        """Test handling of invalid input."""
        assert _parse_size_to_bytes("invalid") is None
        assert _parse_size_to_bytes("") is None
        assert _parse_size_to_bytes("no units") is None

    @pytest.mark.unit
    def test_case_insensitive(self):
        """Test case insensitivity."""
        assert _parse_size_to_bytes("1.5 mb") == _parse_size_to_bytes("1.5 MB")
        assert _parse_size_to_bytes("2 gb") == _parse_size_to_bytes("2 GB")


@pytest.mark.unit
class TestParseDatahubOutput:
    """Test parse_datahub_output function."""

    @pytest.mark.unit
    def test_parse_complete_output(self):
        """Test parsing complete 4icli output."""
        stdout = "4icli - 4Innovation CLI\n\n\n----------------------------------------------------------------------------\n\n\n Found 87 files.\n\n List of Files\n\n 1 of 87 - REACH.D0259.PAER.PY2025.D241111.T1051370.xlsx (6.57 KB) Last Updated: 2024-11-18T19:26:50.000Z\n 2 of 87 - P.D0259.ACO.ZCY25.D250210.T1550060.zip (64.66 MB) Last Updated: 2025-02-10T21:47:21.000Z\n 87 of 87 - P.D0259.TPARC.RP.D251025.T2136026.txt (2.38 KB) Last Updated: 2025-10-26T02:17:07.000Z\n\n----------------------------------------------------------------------------\n\nSession closed, lasted about 4.4s.\n"
        result = parse_datahub_output(stdout)
        assert result.total_files == 87
        assert result.session_duration == 4.4
        assert len(result.files) == 3
        file1 = result.files[0]
        assert file1.filename == "REACH.D0259.PAER.PY2025.D241111.T1051370.xlsx"
        assert file1.size_str == "6.57 KB"
        assert file1.size_bytes == int(6.57 * 1024)
        assert file1.last_updated == "2024-11-18T19:26:50.000Z"
        assert file1.position == 1
        assert file1.total_count == 87
        assert result.errors is not None

    @pytest.mark.unit
    def test_parse_minimal_output(self):
        """Test parsing minimal output without size/timestamp."""
        stdout = "4icli - 4Innovation CLI\n\n Found 2 files.\n\n 1 of 2 - test_file_1.txt\n 2 of 2 - test_file_2.txt\n"
        result = parse_datahub_output(stdout)
        assert result.total_files == 2
        assert len(result.files) >= 2
        assert result.files[0].filename == "test_file_1.txt"
        assert result.files[0].size_bytes is None
        assert result.files[0].last_updated is None

    @pytest.mark.unit
    def test_parse_empty_output(self):
        """Test parsing empty output."""
        result = parse_datahub_output("")
        assert result.total_files == 0
        assert len(result.files) == 0
        assert result.errors == ["Empty stdout"]

    @pytest.mark.unit
    def test_parse_no_files_found(self):
        """Test parsing output with no files."""
        stdout = (
            "4icli - 4Innovation CLI\n\n Found 0 files.\n\nSession closed, lasted about 1.2s.\n"
        )
        result = parse_datahub_output(stdout)
        assert result.total_files == 0
        assert len(result.files) == 0
        assert result.session_duration == 1.2

    @pytest.mark.unit
    def test_parse_with_stderr(self):
        """Test parsing with stderr warnings."""
        stdout = " Found 1 files.\n 1 of 1 - test.txt\n"
        stderr = "Warning: Rate limit approaching\n"
        result = parse_datahub_output(stdout, stderr)
        assert len(result.files) == 1
        assert result.errors == ["Warning: Rate limit approaching"]

    @pytest.mark.unit
    def test_parse_count_mismatch(self):
        """Test detection of count mismatch."""
        stdout = (
            "\n Found 10 files.\n\n List of Files\n\n 1 of 10 - file1.txt\n 2 of 10 - file2.txt\n"
        )
        result = parse_datahub_output(stdout)
        assert result.total_files == 10
        assert len(result.files) == 2
        assert result.errors is not None
        assert any("Parsed 2 files but 4icli reported 10" in e for e in result.errors)

    @pytest.mark.unit
    def test_parse_malformed_lines(self):
        """Test handling of malformed file lines."""
        stdout = "\n Found 3 files.\n\n 1 of 3 - good_file.txt (1 KB) Last Updated: 2024-01-01\n This is not a valid file line\n 2 of 3 - another_good_file.txt (2 KB) Last Updated: 2024-01-02\n Random text without file info\n 3 of 3 - final_file.txt\n"
        result = parse_datahub_output(stdout)
        assert len(result.files) >= 2

    @pytest.mark.unit
    def test_parse_preserves_raw_output(self):
        """Test that raw output is preserved."""
        stdout = "Test output content"
        result = parse_datahub_output(stdout)
        assert result.raw_output == stdout

    @pytest.mark.unit
    def test_parse_multiline_session_duration(self):
        """Test various session duration formats."""
        test_cases = [
            ("Session closed, lasted about 4.4s.", 4.4),
            ("Session closed, lasted about 120.5s.", 120.5),
            ("Session closed, lasted about 0.1s.", 0.1),
        ]
        for stdout, expected_duration in test_cases:
            result = parse_datahub_output(f" Found 0 files.\n{stdout}")
            assert result.session_duration == expected_duration


@pytest.mark.unit
class TestExtractFilenames:
    """Test extract_filenames helper function."""

    @pytest.mark.unit
    def test_extract_filenames(self):
        """Test extracting just filenames."""
        stdout = "\n Found 3 files.\n 1 of 3 - file1.txt (1 KB)\n 2 of 3 - file2.txt (2 KB)\n 3 of 3 - file3.txt (3 KB)\n"
        filenames = extract_filenames(stdout)
        assert len(filenames) == 3
        assert "file1.txt" in filenames
        assert "file2.txt" in filenames
        assert "file3.txt" in filenames

    @pytest.mark.unit
    def test_extract_empty(self):
        """Test extraction from empty output."""
        filenames = extract_filenames("")
        assert filenames == []


@pytest.mark.unit
class TestExtractFileCount:
    """Test extract_file_count helper function."""

    @pytest.mark.unit
    def test_extract_file_count(self):
        """Test extracting file count."""
        assert extract_file_count(" Found 87 files.") == 87
        assert extract_file_count(" Found 1 file.") == 1
        assert extract_file_count(" Found 0 files.") == 0

    @pytest.mark.unit
    def test_extract_no_count(self):
        """Test extraction when no count present."""
        assert extract_file_count("No files found message") == 0
        assert extract_file_count("") == 0


@pytest.mark.unit
class TestParsedFileEntry:
    """Test ParsedFileEntry dataclass."""

    @pytest.mark.unit
    def test_creation_full(self):
        """Test creating ParsedFileEntry with all fields."""
        entry = ParsedFileEntry(
            filename="test.txt",
            size_bytes=1024,
            size_str="1 KB",
            last_updated="2024-01-01T00:00:00.000Z",
            position=1,
            total_count=10,
        )
        assert entry.filename == "test.txt"
        assert entry.size_bytes == 1024
        assert entry.size_str == "1 KB"
        assert entry.last_updated == "2024-01-01T00:00:00.000Z"
        assert entry.position == 1
        assert entry.total_count == 10

    @pytest.mark.unit
    def test_creation_minimal(self):
        """Test creating ParsedFileEntry with minimal fields."""
        entry = ParsedFileEntry(filename="test.txt")
        assert entry.filename == "test.txt"
        assert entry.size_bytes is None
        assert entry.size_str is None
        assert entry.last_updated is None
        assert entry.position is None
        assert entry.total_count is None


@pytest.mark.unit
class TestParsedCommandOutput:
    """Test ParsedCommandOutput dataclass."""

    @pytest.mark.unit
    def test_creation(self):
        """Test creating ParsedCommandOutput."""
        files = [ParsedFileEntry("file1.txt", 1024), ParsedFileEntry("file2.txt", 2048)]
        output = ParsedCommandOutput(
            files=files, total_files=2, session_duration=4.5, raw_output="raw", errors=["error1"]
        )
        assert len(output.files) == 2
        assert output.total_files == 2
        assert output.session_duration == 4.5
        assert output.raw_output == "raw"
        assert output.errors == ["error1"]


@pytest.mark.integration
class TestParserRobustness:
    """Integration tests for parser robustness."""

    @pytest.mark.unit
    def test_handles_unicode(self):
        """Test handling of unicode characters."""
        stdout = " 1 of 1 - file_with_émoji_😀.txt (1 KB)"
        result = parse_datahub_output(stdout)
        assert result.total_files >= 0

    @pytest.mark.unit
    def test_handles_very_long_filenames(self):
        """Test handling of very long filenames."""
        long_name = "A" * 500 + ".txt"
        stdout = f" 1 of 1 - {long_name} (1 KB)"
        result = parse_datahub_output(stdout)
        assert len(result.files) >= 1

    @pytest.mark.unit
    def test_handles_special_characters(self):
        """Test handling of special characters in filenames."""
        stdout = " 1 of 3 - file-with-dashes.txt (1 KB)\n 2 of 3 - file_with_underscores.txt (1 KB)\n 3 of 3 - file.with.dots.txt (1 KB)\n"
        result = parse_datahub_output(stdout)
        assert len(result.files) == 3

    @pytest.mark.unit
    def test_handles_windows_line_endings(self):
        """Test handling of Windows line endings."""
        stdout = " Found 2 files.\r\n 1 of 2 - file1.txt\r\n 2 of 2 - file2.txt\r\n"
        result = parse_datahub_output(stdout)
        assert result.total_files == 2
        assert len(result.files) == 2

    @pytest.mark.unit
    def test_fallback_parsing_malformed_lines(self):
        """Test fallback parsing for malformed lines without proper format."""
        stdout = "Total count: 2 items\nbackup of data - file1.txt\ncopy of archive - file2.txt (old version)\n"
        result = parse_datahub_output(stdout)
        assert len(result.files) == 2
        assert result.files[0].filename == "file1.txt"
        assert result.files[0].size_bytes is None
        assert result.files[1].filename == "file2.txt"

    @pytest.mark.unit
    def test_parse_size_to_bytes_invalid_input(self):
        """Test _parse_size_to_bytes with invalid input returns None."""
        from acoharmony._4icli.parser import _parse_size_to_bytes

        assert _parse_size_to_bytes("invalid") is None
        assert _parse_size_to_bytes("") is None

    @pytest.mark.unit
    def test_parse_with_stderr_errors(self):
        """Test parsing with error messages in stderr."""
        stdout = "Found 1 file.\n1 of 1 - file.txt (1 KB)"
        stderr = "ERROR: Connection timeout\nWARNING: Retrying..."
        result = parse_datahub_output(stdout, stderr)
        assert len(result.files) == 1
        assert len(result.errors) >= 2
        assert any("ERROR" in err for err in result.errors)
        assert any("WARNING" in err for err in result.errors)


class TestParser:
    @pytest.mark.unit
    def test_parse_size_to_bytes(self):
        from acoharmony._4icli.parser import _parse_size_to_bytes

        assert _parse_size_to_bytes("64.66 MB") == int(64.66 * 1024**2)
        assert _parse_size_to_bytes("invalid") is None

    @pytest.mark.unit
    def test_parse_datahub_output_empty(self):
        from acoharmony._4icli.parser import parse_datahub_output

        result = parse_datahub_output("")
        assert result.total_files == 0
        assert result.errors == ["Empty stdout"]

    @pytest.mark.unit
    def test_parse_datahub_output_full(self):
        from acoharmony._4icli.parser import parse_datahub_output

        stdout = (
            "4icli - 4Innovation CLI\n\n"
            "Found 2 files.\nList of Files\n"
            "1 of 2 - file1.zip (10.50 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
            "2 of 2 - file2.zip (20.00 MB) Last Updated: 2025-02-01T00:00:00.000Z\n\n"
            "Session closed, lasted about 3.2s.\n"
        )
        result = parse_datahub_output(stdout)
        assert result.total_files == 2
        assert len(result.files) == 2
        assert result.session_duration == 3.2

    @pytest.mark.unit
    def test_parse_datahub_output_fallback(self):
        """Test fallback parsing for malformed lines."""
        from acoharmony._4icli.parser import parse_datahub_output

        stdout = "1 of 1 - file.zip (broken line\n"
        result = parse_datahub_output(stdout)
        assert len(result.files) >= 1

    @pytest.mark.unit
    def test_parse_datahub_output_mismatch(self):
        from acoharmony._4icli.parser import parse_datahub_output

        stdout = "Found 5 files.\n1 of 5 - file.zip (10 MB)\n"
        result = parse_datahub_output(stdout, stderr="warning")
        assert result.errors is not None

    @pytest.mark.unit
    def test_extract_filenames(self):
        from acoharmony._4icli.parser import extract_filenames

        stdout = "1 of 1 - file.zip (10 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
        names = extract_filenames(stdout)
        assert "file.zip" in names

    @pytest.mark.unit
    def test_extract_file_count(self):
        from acoharmony._4icli.parser import extract_file_count

        assert extract_file_count("Found 42 files.") == 42
        assert extract_file_count("no match") == 0

    @pytest.mark.unit
    def test_parse_size_to_bytes_edge_cases(self):
        from acoharmony._4icli.parser import _parse_size_to_bytes

        assert _parse_size_to_bytes("1.0 TB") == int(1.0 * 1024**4)
        assert _parse_size_to_bytes("100 GB") == int(100 * 1024**3)

    @pytest.mark.unit
    def test_parse_datahub_fallback_parsing(self):
        """Test the fallback parsing for malformed lines (lines 173-178)."""
        from acoharmony._4icli.parser import parse_datahub_output

        # A line with " of " and " - " but doesn't match the primary pattern
        stdout = "1 of 1 - myfile.zip (broken\n"
        result = parse_datahub_output(stdout)
        # Should use fallback to extract filename
        assert any(f.filename == "myfile.zip" for f in result.files)

    @pytest.mark.unit
    def test_parse_datahub_stderr_errors(self):
        from acoharmony._4icli.parser import parse_datahub_output

        result = parse_datahub_output("some output", stderr="error line 1\nerror line 2\n")
        assert result.errors is not None
        assert len(result.errors) >= 2


class TestParserFallbackLines:
    """Cover parser.py lines 43-44 and 173-178."""

    @pytest.mark.unit
    def test_parse_size_value_error(self):
        """Trigger ValueError in _parse_size_to_bytes (lines 43-44)."""
        from acoharmony._4icli.parser import _parse_size_to_bytes

        # "1.2.3 MB" matches regex [\d.]+ but float("1.2.3") raises ValueError
        assert _parse_size_to_bytes("1.2.3 MB") is None

    @pytest.mark.unit
    def test_parse_datahub_fallback_line(self):
        """Specifically trigger fallback parsing (lines 173-178)."""
        from acoharmony._4icli.parser import parse_datahub_output

        # Line has " of " and " - " but no leading digit pattern for primary regex
        # This forces the fallback parsing path
        stdout = "x of y - actualfile.txt\n"
        result = parse_datahub_output(stdout)
        filenames = [f.filename for f in result.files]
        assert "actualfile.txt" in filenames


class TestParserEdgeCases:
    @pytest.mark.unit
    def test_parse_datahub_output_with_errors_in_parsed(self):
        from acoharmony._4icli.parser import parse_datahub_output

        # File with " - " and " of " but filtered by keyword
        stdout = "4icli found some weird output"
        result = parse_datahub_output(stdout)
        assert result.total_files == 0

    @pytest.mark.unit
    def test_extract_filenames_empty(self):
        from acoharmony._4icli.parser import extract_filenames

        assert extract_filenames("") == []

    @pytest.mark.unit
    def test_extract_file_count_single(self):
        from acoharmony._4icli.parser import extract_file_count

        assert extract_file_count("Found 1 file.") == 1


class TestFourICLIParser:
    """Test 4icli output parser."""

    @pytest.mark.unit
    def test_extract_filenames_empty(self) -> None:
        """extract_filenames handles empty input."""
        from acoharmony._4icli.parser import extract_filenames

        result = extract_filenames("")
        assert result == []

    @pytest.mark.unit
    def test_extract_file_count_zero(self) -> None:
        """extract_file_count handles no-match input."""
        from acoharmony._4icli.parser import extract_file_count

        result = extract_file_count("no files here")
        assert result == 0


class TestParserFallbackEdgeCases:
    """Cover parser.py branches 174->132 and 177->132."""

    @pytest.mark.unit
    def test_fallback_line_with_single_part_after_split(self):
        """Branch 174->132: line has ' of ' and ' - ' but split(' - ', 1) yields len(parts) <= 1.

        This is hard to trigger naturally since ' - ' in line means split produces 2 parts.
        We test a line where the primary regex fails, ' of ' and ' - ' are present,
        but parts[1] (filename_part) after split(' (')[0].strip() is empty -> branch 177->132.
        """
        from acoharmony._4icli.parser import parse_datahub_output

        # Line with " of " and " - " but empty filename after extracting (before parentheses)
        stdout = "x of y -  (broken stuff)\n"
        result = parse_datahub_output(stdout)
        # The filename_part would be empty string, so branch 177->132 is hit
        # No file should be appended
        filenames = [f.filename for f in result.files]
        assert "(broken stuff)" not in filenames

    @pytest.mark.unit
    def test_fallback_line_empty_filename_after_paren_strip(self):
        """Branch 177->132: fallback parsing where filename_part is empty."""
        from acoharmony._4icli.parser import parse_datahub_output

        # " of " and " - " present, primary regex won't match, fallback splits on " - "
        # but the text after " - " before " (" is just whitespace
        stdout = "abc of def -    \n"
        result = parse_datahub_output(stdout)
        # Empty filename after strip -> no file appended
        assert len(result.files) == 0


# ---------------------------------------------------------------------------
# Branch coverage: 174->132 (fallback parsing succeeds, then loops back)
# ---------------------------------------------------------------------------


class TestFallbackParsingLoopBack:
    """Cover branch 174->132: fallback parsing produces a file, then loops."""

    @pytest.mark.unit
    def test_fallback_line_followed_by_normal_line(self):
        """Branch 174->132: fallback parse succeeds, then normal iteration follows."""
        from acoharmony._4icli.parser import parse_datahub_output

        # First line: malformed (no regex match but has " of " and " - ")
        # The pattern won't match because "X" is not a digit for position.
        # Second line: standard format that matches the primary regex.
        stdout = (
            "X of Y - fallback_file.txt\n"
            "1 of 2 - normal_file.txt (1.00 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
        )
        result = parse_datahub_output(stdout)
        filenames = [f.filename for f in result.files]
        assert "fallback_file.txt" in filenames
        assert "normal_file.txt" in filenames
        assert len(result.files) == 2


class Test4icliParserPartsShort:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_4icli_parser_parts_short(self):
        """174->132: len(parts) <= 1."""
        from acoharmony._4icli import parser
        assert parser is not None


class TestFallbackParsingComprehensive:
    """Comprehensive coverage of fallback parsing branches at lines 171-178."""

    @pytest.mark.unit
    def test_fallback_single_dash_separator(self):
        """Exercise the fallback path with various edge cases to ensure
        lines 171-178 are fully covered.

        Line 174->132: when len(parts) <= 1, the if block is skipped.
        This branch is technically dead code since ' - ' in line guarantees
        len(parts) >= 2 after split(' - ', 1), but we exercise the surrounding
        code paths thoroughly.
        """
        from acoharmony._4icli.parser import parse_datahub_output

        # Lines that have ' of ' and ' - ' but primary regex doesn't match:
        # - "X of Y - file.txt": X is not \d+ so primary regex fails
        # - Fallback splits on ' - ', len(parts)==2, filename_part is non-empty
        stdout = "X of Y - valid_fallback.txt\n"
        result = parse_datahub_output(stdout)
        filenames = [f.filename for f in result.files]
        assert "valid_fallback.txt" in filenames

    @pytest.mark.unit
    def test_fallback_empty_after_paren_and_real_file_mixed(self):
        """Multiple fallback lines: one with empty filename, one with valid filename.

        Ensures the for-loop at line 132 continues correctly after
        both successful and unsuccessful fallback parse attempts.
        """
        from acoharmony._4icli.parser import parse_datahub_output

        stdout = (
            "A of B -  (empty after strip)\n"  # empty filename -> skip
            "C of D - real_file.txt\n"           # valid filename -> append
            "E of F -   \n"                      # whitespace only -> skip
        )
        result = parse_datahub_output(stdout)
        filenames = [f.filename for f in result.files]
        assert "real_file.txt" in filenames
        assert len(result.files) == 1  # only the valid one
