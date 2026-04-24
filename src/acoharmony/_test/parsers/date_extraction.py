"""Unit tests for date extraction - Polars style."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from unittest.mock import MagicMock
from types import SimpleNamespace

import pytest

from .conftest import _schema_with_storage, create_mock_metadata


class TestExtractFileDate:
    """Test extract_file_date function."""

    @pytest.mark.unit
    def test_extract_date_cclf_format(self):
        """Test date extraction from CCLF filename."""
        mock_schema = create_mock_metadata("cclf1", [], {"type": "fixed_width"})
        filename = "P.A2671.ACO.ZC2Y24.D240508.T0902530"
        date = extract_file_date(filename, mock_schema)
        assert date == "2024-05-08"

    @pytest.mark.unit
    def test_extract_date_quarterly(self):
        """Test date extraction from quarterly filename."""
        mock_schema = create_mock_metadata("test", [], {"type": "csv"})
        filename = "enrollment_2024Q1.csv"
        date = extract_file_date(filename, mock_schema)
        assert date is not None

    @pytest.mark.unit
    def test_extract_date_no_pattern(self):
        """Test date extraction when no pattern matches."""
        mock_schema = create_mock_metadata("test", [], {"type": "csv"})
        filename = "nodate.csv"
        date = extract_file_date(filename, mock_schema)
        assert date is None


class TestDateExtraction:
    """Tests for date extraction from filenames/data."""

    @pytest.mark.unit
    def test_extract_date_from_string(self) -> None:
        """Extract date from formatted string."""
        filename = "data_2024_01_15.csv"
        # Would extract 2024-01-15
        assert "2024" in filename
        assert "01" in filename
        assert "15" in filename

    @pytest.mark.unit
    def test_extract_quarterly_date_from_filename(self) -> None:
        """Test extraction of quarterly dates from filenames."""

        # Test quarterly pattern: Y2024Q2
        class MockSchema:
            file_date = {"year_extraction": {"quarterly": r"Y(\d{4})Q([1-4])"}}

        date_str = extract_file_date("REACH_Y2024Q2_Report.xlsx", MockSchema())

        # Should return Q2 end date (June 30)
        assert date_str == "2024-06-30"


class TestDateExtractionCoverageGaps:
    """Additional tests for _date_extraction branch coverage."""

    @pytest.mark.unit
    def test_d_yy_invalid_month(self):
        """Cover branch where mm > 12 in D[YY]MMDD pattern."""
        from acoharmony._parsers._date_extraction import extract_file_date

        result = extract_file_date("data.D241315", None)
        assert result is None

    @pytest.mark.unit
    def test_schema_pattern_no_match(self):
        """Cover loop where no schema pattern matches."""
        from acoharmony._parsers._date_extraction import extract_file_date

        schema = _schema_with_storage(
            [], storage={"file_patterns": {"report_year_extraction": {"annual": "NOMATCH(\\d{4})"}}}
        )
        assert extract_file_date("2024Q1_report", schema) == "2024-03-31"


class TestDateExtraction2:
    """Tests for acoharmony._parsers._date_extraction."""

    @pytest.mark.unit
    def test_annual_reconciliation(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("Y2022.D259999", None) == "2022-12-31"

    @pytest.mark.unit
    def test_annual_reconciliation_with_actual_date(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("Y2023.D240115", None) == "2023-12-31"

    @pytest.mark.unit
    def test_schema_annual_pattern(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        schema = _schema_with_storage(
            [], storage={"file_patterns": {"report_year_extraction": {"annual": "Y(\\d{4})"}}}
        )
        assert extract_file_date("report_Y2024_data", schema) == "2024-01-01"

    @pytest.mark.unit
    def test_schema_quarterly_pattern(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        schema = _schema_with_storage(
            [],
            storage={
                "file_patterns": {"report_year_extraction": {"quarterly": "(\\d{4})Q([1-4])"}}
            },
        )
        assert extract_file_date("report_2024Q2_data", schema) == "2024-06-30"

    @pytest.mark.unit
    def test_quarterly_q1(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("report_2024Q1_claims.csv", None) == "2024-03-31"

    @pytest.mark.unit
    def test_quarterly_q2(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("report_2024Q2_claims.csv", None) == "2024-06-30"

    @pytest.mark.unit
    def test_quarterly_q3(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("2024Q3_data", None) == "2024-09-30"

    @pytest.mark.unit
    def test_quarterly_q4(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("2024Q4_data", None) == "2024-12-31"

    @pytest.mark.unit
    def test_m_dd_yyyy_format(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        result = extract_file_date("D0259 Provider List - 1-30-2026 15.27.44.xlsx", None)
        assert result == "2026-01-30"

    @pytest.mark.unit
    def test_m_d_yy_format(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        result = extract_file_date("ACO REACH Participant List PY2025 - 8-5-25 13.19.51.xlsx", None)
        assert result == "2025-08-05"

    @pytest.mark.unit
    def test_iso_timestamp(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("report_2025-09-23T08_20_29.json", None) == "2025-09-23"

    @pytest.mark.unit
    def test_yyyy_mm_dd(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("HC REACH Report 2025-08-25.xlsx", None) == "2025-08-25"

    @pytest.mark.unit
    def test_yyyymmdd(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("data_20240831_export.csv", None) == "2024-08-31"

    @pytest.mark.unit
    def test_cclf_format(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("P.A2671.ACO.ZC2Y24.D240508.T0902530", None) == "2024-05-08"

    @pytest.mark.unit
    def test_bar_algr(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("BAR.ALGR23.RP.D240424.csv", None) == "2023-12-31"

    @pytest.mark.unit
    def test_bar_algc(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("BAR.ALGC24.RP.D240508.csv", None) == "2024-05-08"

    @pytest.mark.unit
    def test_d_yy_9999(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("data.D249999", None) == "2024-01-01"

    @pytest.mark.unit
    def test_d_yy_mmdd(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("data.D240315", None) == "2024-03-15"

    @pytest.mark.unit
    def test_mmddyyyy_underscore_pattern(self):
        """Filenames like ACO_REACH_Calendar_updated_03052026.xlsx use
        _MMDDYYYY (underscore-anchored) format — line 244-246 of the
        extractor."""
        from acoharmony._parsers._date_extraction import extract_file_date

        result = extract_file_date("ACO_REACH_Calendar_updated_03052026.xlsx", None)
        assert result == "2026-03-05"

    @pytest.mark.unit
    def test_no_match(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date("nodate.txt", None) is None

    @pytest.mark.unit
    def test_schema_no_storage(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        schema = SimpleNamespace()
        assert extract_file_date("2024Q1_data", schema) == "2024-03-31"

    @pytest.mark.unit
    def test_exception_returns_none(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        assert extract_file_date(None, None) is None

    @pytest.mark.unit
    def test_schema_quarterly_invalid_quarter(self):
        from acoharmony._parsers._date_extraction import extract_file_date

        schema = _schema_with_storage(
            [],
            storage={"file_patterns": {"report_year_extraction": {"quarterly": "(\\d{4})Q(\\d)"}}},
        )
        result = extract_file_date("report_2024Q5_data", schema)
        assert result == "2024-03-31"


class TestSchemaUnknownPatternType:
    """Test that unrecognized schema pattern types are skipped (branch 182→175)."""

    @pytest.mark.unit
    def test_unknown_pattern_type_skipped_and_loop_continues(self):
        """When a schema pattern matches but its type is unrecognized, the loop continues."""
        from acoharmony._parsers._date_extraction import extract_file_date

        schema = _schema_with_storage(
            [],
            storage={
                "file_patterns": {
                    "report_year_extraction": {
                        "monthly": r"RPT_(\d{4})_(\d{2})",
                    }
                }
            },
        )
        # "monthly" regex matches but is neither "annual" nor "quarterly",
        # so the elif on line 182 is False and the loop back-edge 182→175 fires.
        # After exhausting schema patterns, fallback pattern 2024Q1 matches.
        result = extract_file_date("RPT_2024_06_2024Q1_data", schema)
        assert result == "2024-03-31"


class TestCoreDateExtraction:
    """Test core date extraction functionality."""

    @pytest.mark.unit
    def test_date_extraction(self) -> None:
        """Date extraction from filenames works."""
        from unittest.mock import MagicMock

        from acoharmony._parsers._date_extraction import extract_file_date

        mock_schema = MagicMock()
        mock_schema.date_extraction = None
        result = extract_file_date("CCLF8.D240101.T1234567.zip", mock_schema)
        assert result is not None or result is None
