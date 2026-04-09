from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date
from typing import Any

import polars as pl
import pytest

from acoharmony._expressions.reach_bnmr import ReachBNMRMultiTableExpression


def _make_bnmr_df(sheet_types: list[str]) -> pl.LazyFrame:
    """Create a test DataFrame with the given sheet_types."""
    rows = []
    for st in sheet_types:
        rows.append({"sheet_type": st, "col_a": f"val_{st}", "col_b": 1.0})
    return pl.DataFrame(rows, schema={"sheet_type": pl.Utf8, "col_a": pl.Utf8, "col_b": pl.Float64}).lazy()


class TestMetadataFields:
    """Test METADATA_FIELDS constant."""

    @pytest.mark.unit
    def test_has_expected_fields(self):
        fields = ReachBNMRMultiTableExpression.METADATA_FIELDS
        assert 'performance_year' in fields
        assert 'aco_id' in fields
        assert 'aco_type' in fields
        assert 'quality_score' in fields

class TestBuild:
    """Test the build() classmethod."""

    @pytest.mark.unit
    def test_build_with_report_parameters(self):
        """Build extracts metadata from report_parameters sheet type."""
        df = _make_bnmr_df(['report_parameters', 'claims', 'risk'])
        result = ReachBNMRMultiTableExpression.build(df)
        assert 'reach_bnmr_metadata' in result
        metadata = result['reach_bnmr_metadata'].collect()
        assert metadata.height == 1

    @pytest.mark.unit
    def test_build_splits_by_sheet_type(self):
        """Build creates separate tables for each sheet type."""
        df = _make_bnmr_df(['claims', 'risk', 'county', 'uspcc'])
        result = ReachBNMRMultiTableExpression.build(df)
        assert 'reach_bnmr_claims' in result
        assert 'reach_bnmr_risk' in result
        assert 'reach_bnmr_county' in result
        assert 'reach_bnmr_uspcc' in result

    @pytest.mark.unit
    def test_build_concatenates_overlapping_types(self):
        """financial_settlement and claims both map to reach_bnmr_claims."""
        df = _make_bnmr_df(['financial_settlement', 'claims'])
        result = ReachBNMRMultiTableExpression.build(df)
        claims_df = result['reach_bnmr_claims'].collect()
        assert claims_df.height == 2

    @pytest.mark.unit
    def test_build_concatenates_risk_types(self):
        """riskscore_ad, riskscore_esrd, and risk all go to reach_bnmr_risk."""
        df = _make_bnmr_df(['riskscore_ad', 'riskscore_esrd', 'risk'])
        result = ReachBNMRMultiTableExpression.build(df)
        risk_df = result['reach_bnmr_risk'].collect()
        assert risk_df.height == 3

    @pytest.mark.unit
    def test_build_with_all_sheet_types(self):
        """Build handles all recognized sheet types."""
        all_types = list(ReachBNMRMultiTableExpression.SHEET_TO_TABLE.keys())
        df = _make_bnmr_df(all_types)
        result = ReachBNMRMultiTableExpression.build(df)
        expected_tables = set(ReachBNMRMultiTableExpression.SHEET_TO_TABLE.values())
        for table in expected_tables:
            assert table in result

    @pytest.mark.unit
    def test_build_with_empty_input(self):
        """Build handles empty DataFrame gracefully."""
        df = pl.DataFrame({'sheet_type': [], 'col_a': [], 'col_b': []}, schema={'sheet_type': pl.Utf8, 'col_a': pl.Utf8, 'col_b': pl.Float64}).lazy()
        result = ReachBNMRMultiTableExpression.build(df)
        assert 'reach_bnmr_metadata' in result

    @pytest.mark.unit
    def test_build_with_unknown_sheet_type(self):
        """Unknown sheet types don't create tables but don't error."""
        df = _make_bnmr_df(['unknown_type', 'claims'])
        result = ReachBNMRMultiTableExpression.build(df)
        assert 'reach_bnmr_claims' in result

    @pytest.mark.unit
    def test_build_stop_loss_tables(self):
        """Stop loss sheet types create separate tables."""
        df = _make_bnmr_df(['stop_loss_county', 'stop_loss_claims', 'stop_loss_payout'])
        result = ReachBNMRMultiTableExpression.build(df)
        assert 'reach_bnmr_stop_loss_county' in result
        assert 'reach_bnmr_stop_loss_claims' in result
        assert 'reach_bnmr_stop_loss_payout' in result

    @pytest.mark.unit
    def test_build_heba_and_cap(self):
        """HEBA and CAP sheet types create correct tables."""
        df = _make_bnmr_df(['heba', 'cap'])
        result = ReachBNMRMultiTableExpression.build(df)
        assert 'reach_bnmr_heba' in result
        assert 'reach_bnmr_cap' in result
        assert result['reach_bnmr_heba'].collect().height == 1
        assert result['reach_bnmr_cap'].collect().height == 1
'Additional tests for _expressions/_acr_readmission.py to cover 10 missing lines.\n\nTargets:\n- identify_planned_readmissions with CCS mapping and PAA2 value sets\n- assign_specialty_cohorts with CCS-based cohort assignment\n- calculate_acr_summary edge cases\n- load_acr_value_sets with corrupted/unreadable files\n'

def _claims_lf(**overrides) -> pl.LazyFrame:
    data = {'claim_id': ['C001'], 'person_id': ['P001'], 'admission_date': [date(2025, 3, 15)], 'discharge_date': [date(2025, 3, 20)], 'diagnosis_code_1': ['A01'], 'bill_type_code': ['111'], 'facility_npi': ['NPI001'], 'discharge_status_code': ['01']}
    data.update(overrides)
    return pl.DataFrame(data).lazy()

def _eligibility_lf(**overrides) -> pl.LazyFrame:
    data = {'person_id': ['P001'], 'birth_date': [date(1950, 1, 1)]}
    data.update(overrides)
    return pl.DataFrame(data).lazy()

def _empty_vs() -> dict[str, pl.LazyFrame]:
    return {'ccs_icd10_cm': pl.DataFrame().lazy(), 'exclusions': pl.DataFrame().lazy(), 'cohort_icd10': pl.DataFrame().lazy(), 'cohort_ccs': pl.DataFrame().lazy(), 'paa2': pl.DataFrame().lazy()}

def _config(**overrides) -> dict[str, Any]:
    cfg = {'performance_year': 2025, 'lookback_days': 30, 'min_age': 65, 'patient_id_column': 'person_id', 'admission_date_column': 'admission_date', 'discharge_date_column': 'discharge_date', 'diagnosis_column': 'diagnosis_code_1'}
    cfg.update(overrides)
    return cfg

def _index_lf():
    """Standard index admissions LazyFrame."""
    return pl.DataFrame({'claim_id': ['C001'], 'person_id': ['P001'], 'admission_date': [date(2025, 3, 1)], 'discharge_date': [date(2025, 3, 5)], 'exclusion_flag': [False], 'ccs_diagnosis_category': ['CCS100'], 'discharge_status_code': ['01'], 'facility_id': ['NPI001'], 'principal_diagnosis_code': ['A01'], 'age_at_admission': [75]}).lazy()


# ---------------------------------------------------------------------------
# Tests for MultiLevelHeaderExpression – uncovered branches
# ---------------------------------------------------------------------------


class TestSanitizeColumnNameDigitPrefix:
    """Cover branch 170->171: name starts with a digit gets 'col_' prefix."""

    @pytest.mark.unit
    def test_digit_prefix_gets_col_prefix(self):
        """A name that starts with a digit should be prefixed with 'col_'."""
        result = MultiLevelHeaderExpression.sanitize_column_name("2024_revenue")
        assert result == "col_2024_revenue"

    @pytest.mark.unit
    def test_digit_only_name(self):
        result = MultiLevelHeaderExpression.sanitize_column_name("123")
        assert result == "col_123"


class TestExtractHeadersForwardFill:
    """Cover the forward_fill=True path (lines 217-237).

    Branches covered:
        217->218  (forward_fill is True)
        218->219  (loop over header_rows)
        218->239  (exit loop -> continue to col mapping)
        220->221  (loop over columns – body)
        220->228  (exit inner loop -> filled_values)
        222->223  (value is not None -> True)
        222->225  (value is not None -> False / else)
        230->231  (loop over row_values – body)
        230->237  (exit loop -> store in header_row_data)
        231->232  (value is not None and value != "" -> True)
    """

    @pytest.mark.unit
    def test_forward_fill_basic(self):
        """Forward-fill propagates a spanning header to subsequent empty columns."""
        # Row 0: parent header with a span  (None means the parent spans)
        # Row 1: child headers
        df = pl.DataFrame({
            "col_0": ["Category A", "Metric 1"],
            "col_1": [None, "Metric 2"],
            "col_2": ["Category B", "Metric 3"],
        })
        config = MultiLevelHeaderConfig(
            header_rows=[0, 1],
            forward_fill=True,
            sanitize_names=True,
            skip_empty_parts=True,
        )
        mapping = MultiLevelHeaderExpression.extract_headers(df, config)
        # col_0 -> "category_a" + "metric_1"
        assert mapping["col_0"] == "category_a_metric_1"
        # col_1 -> forward-filled "category_a" + "metric_2"
        assert mapping["col_1"] == "category_a_metric_2"
        # col_2 -> "category_b" + "metric_3"
        assert mapping["col_2"] == "category_b_metric_3"

    @pytest.mark.unit
    def test_forward_fill_with_none_value(self):
        """Forward-fill handles None values (branch 222->225)."""
        df = pl.DataFrame({
            "col_0": [None, "child_a"],
            "col_1": ["Parent", "child_b"],
        })
        config = MultiLevelHeaderConfig(
            header_rows=[0, 1],
            forward_fill=True,
            sanitize_names=True,
            skip_empty_parts=True,
        )
        mapping = MultiLevelHeaderExpression.extract_headers(df, config)
        # col_0: row 0 is None -> forward-fill has no prior value -> ""
        # col_1: row 0 is "Parent" -> forward-filled
        assert mapping["col_0"] == "child_a"
        assert mapping["col_1"] == "parent_child_b"

    @pytest.mark.unit
    def test_forward_fill_empty_string_treated_as_blank(self):
        """Empty string in a header row triggers forward-fill (branch 231->232 false)."""
        df = pl.DataFrame({
            "col_0": ["Group X", "val1"],
            "col_1": ["", "val2"],
            "col_2": ["Group Y", "val3"],
        })
        config = MultiLevelHeaderConfig(
            header_rows=[0, 1],
            forward_fill=True,
            sanitize_names=True,
            skip_empty_parts=True,
        )
        mapping = MultiLevelHeaderExpression.extract_headers(df, config)
        # col_1 row 0 is "" -> forward-fill uses "Group X"
        assert mapping["col_1"] == "group_x_val2"

    @pytest.mark.unit
    def test_forward_fill_no_sanitize(self):
        """Forward-fill works when sanitize_names is False."""
        df = pl.DataFrame({
            "col_0": ["Base PCC", "Amount"],
            "col_1": [None, "Count"],
        })
        config = MultiLevelHeaderConfig(
            header_rows=[0, 1],
            forward_fill=True,
            sanitize_names=False,
            skip_empty_parts=True,
        )
        mapping = MultiLevelHeaderExpression.extract_headers(df, config)
        assert mapping["col_0"] == "Base PCC_Amount"
        assert mapping["col_1"] == "Base PCC_Count"


class TestExtractHeadersNoForwardFill:
    """Cover branches in the non-forward-fill path."""

    @pytest.mark.unit
    def test_no_forward_fill_none_value(self):
        """Cover 247->250: value is None in the non-forward-fill path -> value_str = ''."""
        df = pl.DataFrame({
            "col_0": [None, "child_a"],
            "col_1": ["Parent", "child_b"],
        })
        config = MultiLevelHeaderConfig(
            header_rows=[0, 1],
            forward_fill=False,
            sanitize_names=True,
            skip_empty_parts=True,
        )
        mapping = MultiLevelHeaderExpression.extract_headers(df, config)
        # col_0 row 0 is None -> value_str="" -> skipped (skip_empty_parts=True)
        assert mapping["col_0"] == "child_a"
        assert mapping["col_1"] == "parent_child_b"

    @pytest.mark.unit
    def test_no_forward_fill_all_none_fallback(self):
        """Cover 255->258: all header_parts empty -> fallback to original col_name."""
        df = pl.DataFrame({
            "col_0": [None, None],
            "col_1": ["X", "Y"],
        })
        config = MultiLevelHeaderConfig(
            header_rows=[0, 1],
            forward_fill=False,
            sanitize_names=False,
            skip_empty_parts=True,
        )
        mapping = MultiLevelHeaderExpression.extract_headers(df, config)
        # col_0: both rows are None -> header_parts is empty -> fallback
        assert mapping["col_0"] == "col_0"
        assert mapping["col_1"] == "X_Y"


class TestExtractHeadersDuplicateNames:
    """Cover branches 269->273: duplicate combined_name handling."""

    @pytest.mark.unit
    def test_duplicate_combined_names_get_suffix(self):
        """Cover 269->273: when two columns produce the same combined_name, second gets _1."""
        df = pl.DataFrame({
            "col_0": ["Amount"],
            "col_1": ["Amount"],
            "col_2": ["Amount"],
        })
        config = MultiLevelHeaderConfig(
            header_rows=[0],
            forward_fill=False,
            sanitize_names=False,
            skip_empty_parts=True,
        )
        mapping = MultiLevelHeaderExpression.extract_headers(df, config)
        assert mapping["col_0"] == "Amount"
        assert mapping["col_1"] == "Amount_1"
        assert mapping["col_2"] == "Amount_2"


class TestApplyDataStartRowNone:
    """Cover branch 337->338: data_start_row is None -> auto-computed."""

    @pytest.mark.unit
    def test_apply_auto_data_start_row(self):
        """Cover 337->338: data_start_row=None defaults to max(header_rows)+1."""
        df = pl.DataFrame({
            "col_0": ["Header A", "data_1", "data_2"],
            "col_1": ["Header B", "data_3", "data_4"],
        })
        config = MultiLevelHeaderConfig(
            header_rows=[0],
            forward_fill=False,
            sanitize_names=True,
            skip_empty_parts=True,
        )
        result = MultiLevelHeaderExpression.apply(df.lazy(), config, data_start_row=None)
        collected = result.collect()
        # data_start_row should be 1 (max([0]) + 1), so 2 data rows remain
        assert collected.height == 2
        assert "header_a" in collected.columns
        assert "header_b" in collected.columns
