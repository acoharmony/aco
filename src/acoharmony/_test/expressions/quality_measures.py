from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._quality_measures import QualityMeasuresExpression


class TestQualityMeasuresExpression:

    @pytest.mark.unit
    def test_build(self):
        result = QualityMeasuresExpression.build({'measurement_year': 2025})
        assert 'expressions' in result
        assert result['config']['measurement_year'] == 2025

    @pytest.mark.unit
    def test_transform_measure_summary_empty(self):
        medical = pl.DataFrame({'claim_id': []}).lazy()
        pharmacy = pl.DataFrame({'claim_id': []}).lazy()
        elig = pl.DataFrame({'patient_id': []}).lazy()
        result = QualityMeasuresExpression.transform_measure_summary(medical, pharmacy, elig, {}, {'measures': []}).collect()
        assert len(result) == 0

    @pytest.mark.unit
    def test_transform_measure_summary_diabetes(self):
        medical = pl.DataFrame({'claim_id': ['C1']}).lazy()
        pharmacy = pl.DataFrame({'claim_id': ['C2']}).lazy()
        elig = pl.DataFrame({'patient_id': ['P1']}).lazy()
        result = QualityMeasuresExpression.transform_measure_summary(medical, pharmacy, elig, {}, {'measures': ['diabetes_hba1c_control'], 'measurement_year': 2025}).collect()
        assert len(result) == 1


class TestQualityMeasuresBranches:
    """Cover branches 137->146/160, 160->161/164."""

    @pytest.mark.unit
    def test_diabetes_measure_in_list(self):
        """Branch 137->146: 'diabetes_hba1c_control' IS in measures."""
        medical = pl.DataFrame({'claim_id': ['C1']}).lazy()
        pharmacy = pl.DataFrame({'claim_id': ['C2']}).lazy()
        elig = pl.DataFrame({'patient_id': ['P1']}).lazy()
        result = QualityMeasuresExpression.transform_measure_summary(
            medical, pharmacy, elig, {},
            {'measures': ['diabetes_hba1c_control'], 'measurement_year': 2024}
        ).collect()
        assert result.height == 1
        assert result['measure_name'][0] == 'diabetes_hba1c_control'

    @pytest.mark.unit
    def test_no_measures_configured(self):
        """Branch 137->160, 160->164: no measures configured, empty result."""
        medical = pl.DataFrame({'claim_id': ['C1']}).lazy()
        pharmacy = pl.DataFrame({'claim_id': ['C2']}).lazy()
        elig = pl.DataFrame({'patient_id': ['P1']}).lazy()
        result = QualityMeasuresExpression.transform_measure_summary(
            medical, pharmacy, elig, {},
            {'measures': [], 'measurement_year': 2024}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_measure_results_concat(self):
        """Branch 160->161: measure_results is non-empty, concat called."""
        medical = pl.DataFrame({'claim_id': ['C1']}).lazy()
        pharmacy = pl.DataFrame({'claim_id': ['C2']}).lazy()
        elig = pl.DataFrame({'patient_id': ['P1']}).lazy()
        result = QualityMeasuresExpression.transform_measure_summary(
            medical, pharmacy, elig, {},
            {'measures': ['diabetes_hba1c_control'], 'measurement_year': 2024}
        ).collect()
        assert 'measure_name' in result.columns
        assert result.height > 0
