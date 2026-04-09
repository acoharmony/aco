from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._acr_readmission import AcrReadmissionExpression
from acoharmony._expressions._chronic_conditions import ChronicConditionsExpression


class TestChronicConditionsExpression:

    @pytest.mark.unit
    def test_build(self):
        result = ChronicConditionsExpression.build({})
        assert 'config' in result

    @pytest.mark.unit
    def test_transform_patient_conditions_long(self):
        claims = pl.DataFrame({'patient_id': ['P1', 'P1'], 'diagnosis_code': ['E11', 'I10'], 'claim_end_date': [date(2024, 1, 1), date(2024, 2, 1)]}).lazy()
        mapping = pl.DataFrame({'code': ['E11'], 'condition_category': ['diabetes'], 'condition': ['Type 2 Diabetes']}).lazy()
        result = ChronicConditionsExpression.transform_patient_conditions_long(claims, mapping, {}).collect()
        assert 'meets_criteria' in result.columns

class TestPivotToWide:
    """Cover pivot_to_wide lines 152-168."""

    @pytest.mark.unit
    def test_pivot_creates_condition_columns(self):
        """Long format → wide with condition flag columns."""
        long_data = pl.DataFrame({
            "patient_id": ["P1", "P1", "P2"],
            "condition_category": ["diabetes", "hypertension", "diabetes"],
            "meets_criteria": [True, True, True],
        }).lazy()

        result = ChronicConditionsExpression.transform_patient_conditions_wide(
            long_data, {}
        ).collect()
        assert "patient_id" in result.columns
        assert "diabetes" in result.columns
        assert "condition_count" in result.columns
        assert result.height == 2  # P1 and P2


class TestLoadValueSetsExceptions:
    """Test exception handling in load_acr_value_sets."""

    @pytest.mark.unit
    def test_missing_files_returns_empty(self, tmp_path):
        """Missing parquet files return empty LazyFrames."""
        result = AcrReadmissionExpression.load_acr_value_sets(tmp_path)
        for key in ('ccs_icd10_cm', 'exclusions', 'cohort_icd10', 'cohort_ccs', 'paa2'):
            assert result[key].collect().height == 0
