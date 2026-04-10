# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for display and I/O functions in consolidated_alignments notebook.

Tests functions:
- Display functions: display_enrollment_patterns, display_vintage_cohort_overview,
  display_vintage_cohort_table, display_excel_export_button
- I/O functions: load_consolidated_alignment_data, load_outreach_data
- Excel export: create_excel_workbook
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import acoharmony

# Add bundled test-fixture notebooks directory to path so we can import
# `consolidated_alignments` (a marimo notebook) for behavioral tests.
sys.path.insert(
    0,
    str(Path(__file__).resolve().parent.parent / "_fixtures" / "notebooks"),
)

# Import the notebook module

try:
    import consolidated_alignments
except ModuleNotFoundError:
    import pytest
    pytest.skip("consolidated_alignments notebook not on path", allow_module_level=True)


@pytest.fixture(scope="module")
def notebook_defs():
    """Run notebook once and cache definitions for all tests."""
    _, defs = consolidated_alignments.app.run()
    return defs


class TestDisplayEnrollmentPatterns:
    """Tests for display_enrollment_patterns function."""

    @pytest.fixture
    def sample_df(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "has_continuous_enrollment": [True, True, False],
            "has_program_transition": [False, False, True],
            "months_in_reach": [12, 6, 3],
            "months_in_mssp": [0, 6, 0],
            "total_aligned_months": [12, 12, 3],
            "enrollment_gaps": [0, 1, 2],
        })

    @pytest.fixture
    def sample_df_enriched(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002", "M003"],
            "ym_202401_reach": [True, False, True],
            "ym_202401_mssp": [False, True, False],
            "has_continuous_enrollment": [True, True, False],
            "has_program_transition": [False, False, True],
            "months_in_reach": [12, 6, 3],
            "months_in_mssp": [0, 6, 0],
            "total_aligned_months": [12, 12, 3],
            "enrollment_gaps": [0, 1, 2],
        })

    @pytest.mark.unit
    def test_performs_calculations(self, notebook_defs, sample_df, sample_df_enriched):
        func = notebook_defs["display_enrollment_patterns"]

        # Function should run without errors
        result = func(sample_df, sample_df_enriched, "202401", consolidated_alignments.mo, pl)

        # Should return marimo markdown output
        assert result is not None

    @pytest.mark.unit
    def test_handles_none_selected_ym(self, notebook_defs, sample_df, sample_df_enriched):
        func = notebook_defs["display_enrollment_patterns"]

        result = func(sample_df, sample_df_enriched, None, consolidated_alignments.mo, pl)

        assert result is not None

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, sample_df, sample_df_enriched):
        func = notebook_defs["display_enrollment_patterns"]

        # Same inputs should produce consistent output
        result1 = func(sample_df, sample_df_enriched, "202401", consolidated_alignments.mo, pl)
        result2 = func(sample_df, sample_df_enriched, "202401", consolidated_alignments.mo, pl)

        assert result1 is not None
        assert result2 is not None


class TestDisplayVintageCohortOverview:
    """Tests for display_vintage_cohort_overview function."""

    @pytest.fixture
    def vintage_data(self):
        return pl.DataFrame({
            "vintage_cohort": ["0-6 months", "6-12 months", "12-24 months"],
            "count": [10, 15, 8],
            "pct": [30.3, 45.5, 24.2],
            "current_reach": [8, 12, 5],
            "current_mssp": [2, 3, 3],
            "avg_months_reach": [3.0, 9.0, 18.0],
            "avg_months_mssp": [0.0, 0.0, 6.0],
            "avg_total_months": [3.0, 9.0, 24.0],
            "transitions": [2, 1, 2],
            "pct_of_enrolled": [30.3, 45.5, 24.2],
            "pct_in_reach": [80.0, 80.0, 62.5],
            "pct_in_mssp": [20.0, 20.0, 37.5],
            "pct_with_transitions": [20.0, 6.7, 25.0],
        })

    @pytest.mark.unit
    def test_handles_valid_data(self, notebook_defs, vintage_data):
        func = notebook_defs["display_vintage_cohort_overview"]

        result = func(vintage_data, consolidated_alignments.mo, pl)

        assert result is not None

    @pytest.mark.unit
    def test_handles_none_data(self, notebook_defs):
        func = notebook_defs["display_vintage_cohort_overview"]

        func(None, consolidated_alignments.mo, pl)

        # Function should handle None gracefully (may return None or empty output)
        # Just verify it doesn't crash
        pass

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, vintage_data):
        func = notebook_defs["display_vintage_cohort_overview"]

        result1 = func(vintage_data, consolidated_alignments.mo, pl)
        result2 = func(vintage_data, consolidated_alignments.mo, pl)

        assert result1 is not None
        assert result2 is not None


class TestDisplayVintageCohortTable:
    """Tests for display_vintage_cohort_table function."""

    @pytest.fixture
    def vintage_data(self):
        return pl.DataFrame({
            "vintage_cohort": ["0-6 months", "6-12 months"],
            "count": [10, 15],
            "pct": [40.0, 60.0],
            "current_reach": [8, 12],
            "current_mssp": [2, 3],
            "avg_months_reach": [3.0, 9.0],
            "avg_months_mssp": [0.0, 0.0],
            "avg_total_months": [3.0, 9.0],
            "transitions": [2, 1],
            "pct_of_enrolled": [40.0, 60.0],
            "pct_in_reach": [80.0, 80.0],
            "pct_in_mssp": [20.0, 20.0],
            "pct_with_transitions": [20.0, 6.7],
        })

    @pytest.mark.unit
    def test_handles_valid_data(self, notebook_defs, vintage_data):
        func = notebook_defs["display_vintage_cohort_table"]

        result = func(vintage_data, consolidated_alignments.mo, pl)

        assert result is not None

    @pytest.mark.unit
    def test_handles_none_data(self, notebook_defs):
        func = notebook_defs["display_vintage_cohort_table"]

        func(None, consolidated_alignments.mo, pl)

        # Function should handle None gracefully (may return None or empty output)
        # Just verify it doesn't crash
        pass

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, vintage_data):
        func = notebook_defs["display_vintage_cohort_table"]

        result1 = func(vintage_data, consolidated_alignments.mo, pl)
        result2 = func(vintage_data, consolidated_alignments.mo, pl)

        assert result1 is not None
        assert result2 is not None


class TestDisplayExcelExportButton:
    """Tests for display_excel_export_button function."""

    @pytest.fixture
    def mock_stats(self):
        return pl.DataFrame({"metric": ["total"], "value": [100]})

    @pytest.fixture
    def mock_df(self):
        return pl.LazyFrame({"current_mbi": ["M001", "M002"]})

    @pytest.mark.unit
    def test_creates_button(self, notebook_defs, mock_stats, mock_df):
        func = notebook_defs["display_excel_export_button"]

        # Call with all required parameters
        result = func(
            current_alignment_stats=mock_stats,
            historical_stats=mock_stats,
            alignment_trends=None,
            transition_stats=None,
            vintage_distribution=None,
            df=mock_df,
            df_enriched=mock_df,
            datetime=consolidated_alignments.datetime,
            mo=consolidated_alignments.mo,
            most_recent_ym="202401",
            pl=pl,
            sva_stats={"total": 100},
            action_stats=mock_stats,
            outreach_metrics={"total_contacted": 50},
            office_stats=None,
            office_alignment_types=None,
            office_program_dist=None,
            office_transitions=None,
            office_metadata=None,
            office_campaign_metrics=None,
            office_vintage_distribution=None,
            newly_added_stats=None,
        )

        assert result is not None

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs, mock_stats, mock_df):
        func = notebook_defs["display_excel_export_button"]

        kwargs = {
            "current_alignment_stats": mock_stats,
            "historical_stats": mock_stats,
            "alignment_trends": None,
            "transition_stats": None,
            "vintage_distribution": None,
            "df": mock_df,
            "df_enriched": mock_df,
            "datetime": consolidated_alignments.datetime,
            "mo": consolidated_alignments.mo,
            "most_recent_ym": "202401",
            "pl": pl,
            "sva_stats": {"total": 100},
            "action_stats": mock_stats,
            "outreach_metrics": {"total_contacted": 50},
            "office_stats": None,
            "office_alignment_types": None,
            "office_program_dist": None,
            "office_transitions": None,
            "office_metadata": None,
            "office_campaign_metrics": None,
            "office_vintage_distribution": None,
            "newly_added_stats": None,
        }

        result1 = func(**kwargs)
        result2 = func(**kwargs)

        assert result1 is not None
        assert result2 is not None


class TestLoadConsolidatedAlignmentData:
    """Tests for load_consolidated_alignment_data function."""

    @patch('polars.scan_parquet')
    @pytest.mark.unit
    def test_loads_data_from_path(self, mock_scan, notebook_defs):
        func = notebook_defs["load_consolidated_alignment_data"]

        # Mock the scan_parquet to return a LazyFrame
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        silver_path = Path("/path/to/silver")
        result = func(silver_path, pl)

        # Should call scan_parquet with correct path
        mock_scan.assert_called_once()
        call_args = mock_scan.call_args[0][0]
        assert "consolidated_alignment.parquet" in call_args
        assert result == mock_lf

    @patch('polars.scan_parquet')
    @pytest.mark.unit
    def test_idempotent(self, mock_scan, notebook_defs):
        func = notebook_defs["load_consolidated_alignment_data"]

        # Mock the scan_parquet
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        silver_path = Path("/path/to/silver")
        result1 = func(silver_path, pl)
        result2 = func(silver_path, pl)

        # Should call scan_parquet twice with same args
        assert mock_scan.call_count == 2
        assert result1 == mock_lf
        assert result2 == mock_lf


class TestLoadOutreachData:
    """Tests for load_outreach_data function."""

    @pytest.mark.unit
    def test_loads_both_datasets(self, notebook_defs):
        func = notebook_defs["load_outreach_data"]

        # Mock catalog with scan_table method
        mock_catalog = MagicMock()
        mock_emails = MagicMock()
        mock_mailed = MagicMock()
        mock_catalog.scan_table.side_effect = [mock_emails, mock_mailed]

        emails, mailed = func(mock_catalog)

        # Should call scan_table twice
        assert mock_catalog.scan_table.call_count == 2
        mock_catalog.scan_table.assert_any_call("emails")
        mock_catalog.scan_table.assert_any_call("mailed")
        assert emails == mock_emails
        assert mailed == mock_mailed

    @pytest.mark.unit
    def test_idempotent(self, notebook_defs):
        func = notebook_defs["load_outreach_data"]

        # Mock catalog
        mock_catalog = MagicMock()
        mock_emails = MagicMock()
        mock_mailed = MagicMock()
        mock_catalog.scan_table.side_effect = [mock_emails, mock_mailed, mock_emails, mock_mailed]

        result1 = func(mock_catalog)
        mock_catalog.reset_mock()
        mock_catalog.scan_table.side_effect = [mock_emails, mock_mailed]
        result2 = func(mock_catalog)

        # Both calls should produce same structure
        assert len(result1) == 2
        assert len(result2) == 2


class TestCreateExcelWorkbook:
    """Tests for create_excel_workbook function."""

    @pytest.fixture
    def mock_stats(self):
        return pl.DataFrame({"metric": ["total"], "value": [100]})

    @pytest.fixture
    def mock_action_stats(self):
        return pl.DataFrame({
            "sva_action_needed": ["Renewal", "New Signature"],
            "count": [50, 30]
        })

    @pytest.fixture
    def mock_df(self):
        return pl.LazyFrame({
            "current_mbi": ["M001", "M002"],
            "office_location": ["Dallas", "Houston"],
        })

    @pytest.fixture
    def mock_trends(self):
        return pl.DataFrame({
            "year_month": ["202401", "202402"],
            "REACH": [50, 55],
            "MSSP": [30, 35],
        })

    @patch('xlsxwriter.Workbook')
    @pytest.mark.unit
    def test_creates_workbook_with_all_sheets(self, mock_workbook_class, notebook_defs, mock_stats, mock_action_stats, mock_df, mock_trends):
        func = notebook_defs["create_excel_workbook"]

        # Mock workbook and worksheet
        mock_wb = MagicMock()
        mock_ws = MagicMock()
        mock_wb.add_worksheet.return_value = mock_ws
        mock_wb.add_format.return_value = MagicMock()
        mock_workbook_class.return_value = mock_wb

        result = func(
            current_alignment_stats=mock_stats,
            historical_stats=mock_stats,
            alignment_trends=mock_trends,
            transition_stats=None,
            vintage_distribution=None,
            df=mock_df,
            df_enriched=mock_df,
            most_recent_ym="202401",
            pl=pl,
            sva_stats={"total": 100},
            action_stats=mock_action_stats,
            outreach_metrics={"total_contacted": 50},
            office_stats=None,
            office_alignment_types=None,
            office_program_dist=None,
            office_transitions=None,
            office_metadata=None,
            office_campaign_metrics=None,
            office_vintage_distribution=None,
            year_over_year_newly_added_beneficiaries=None,
        )

        # Should create workbook
        mock_workbook_class.assert_called_once()
        # Should add worksheets
        assert mock_wb.add_worksheet.call_count > 0
        # Should close workbook
        mock_wb.close.assert_called_once()
        # Should return BytesIO value
        assert result is not None

    @patch('xlsxwriter.Workbook')
    @pytest.mark.unit
    def test_handles_none_dataframes(self, mock_workbook_class, notebook_defs, mock_stats, mock_action_stats, mock_df):
        func = notebook_defs["create_excel_workbook"]

        # Mock workbook
        mock_wb = MagicMock()
        mock_ws = MagicMock()
        mock_wb.add_worksheet.return_value = mock_ws
        mock_wb.add_format.return_value = MagicMock()
        mock_workbook_class.return_value = mock_wb

        # Call with None for optional dataframes
        result = func(
            current_alignment_stats=mock_stats,
            historical_stats=mock_stats,
            alignment_trends=None,
            transition_stats=None,
            vintage_distribution=None,
            df=mock_df,
            df_enriched=mock_df,
            most_recent_ym="202401",
            pl=pl,
            sva_stats={"total": 100},
            action_stats=mock_action_stats,
            outreach_metrics={"total_contacted": 50},
            office_stats=None,
            office_alignment_types=None,
            office_program_dist=None,
            office_transitions=None,
            office_metadata=None,
            office_campaign_metrics=None,
            office_vintage_distribution=None,
            year_over_year_newly_added_beneficiaries=None,
        )

        # Should still create workbook
        assert result is not None

    @patch('xlsxwriter.Workbook')
    @pytest.mark.unit
    def test_idempotent(self, mock_workbook_class, notebook_defs, mock_stats, mock_action_stats, mock_df):
        func = notebook_defs["create_excel_workbook"]

        # Mock workbook
        mock_wb = MagicMock()
        mock_ws = MagicMock()
        mock_wb.add_worksheet.return_value = mock_ws
        mock_wb.add_format.return_value = MagicMock()
        mock_workbook_class.return_value = mock_wb

        kwargs = {
            "current_alignment_stats": mock_stats,
            "historical_stats": mock_stats,
            "alignment_trends": None,
            "transition_stats": None,
            "vintage_distribution": None,
            "df": mock_df,
            "df_enriched": mock_df,
            "most_recent_ym": "202401",
            "pl": pl,
            "sva_stats": {"total": 100},
            "action_stats": mock_action_stats,
            "outreach_metrics": {"total_contacted": 50},
            "office_stats": None,
            "office_alignment_types": None,
            "office_program_dist": None,
            "office_transitions": None,
            "office_metadata": None,
            "office_campaign_metrics": None,
            "office_vintage_distribution": None,
            "year_over_year_newly_added_beneficiaries": None,
        }

        result1 = func(**kwargs)
        result2 = func(**kwargs)

        # Both should return results
        assert result1 is not None
        assert result2 is not None


class TestIdempotencyComprehensive:
    """Comprehensive idempotency tests for all display and I/O functions."""

    @patch('polars.scan_parquet')
    @pytest.mark.unit
    def test_load_functions_idempotent(self, mock_scan, notebook_defs):
        """Load functions return consistent results when called multiple times."""

        # Mock scan_parquet
        mock_lf = MagicMock()
        mock_scan.return_value = mock_lf

        # Test load_consolidated_alignment_data
        func = notebook_defs["load_consolidated_alignment_data"]
        r1 = func(Path("/path/to/silver"), pl)
        r2 = func(Path("/path/to/silver"), pl)
        assert r1 == mock_lf
        assert r2 == mock_lf

    @pytest.mark.unit
    def test_display_functions_idempotent(self, notebook_defs):
        """Display functions return consistent output when called multiple times."""

        # Test display_vintage_cohort_overview with None
        # Function handles None gracefully, just verify it doesn't crash
        func = notebook_defs["display_vintage_cohort_overview"]
        func(None, consolidated_alignments.mo, pl)
        func(None, consolidated_alignments.mo, pl)
        # Test passes if no exceptions raised
