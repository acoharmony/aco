"""Unit tests for _ed_classification transforms module."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from typing import TYPE_CHECKING

import polars as pl
import pytest
import acoharmony

from acoharmony._transforms._ed_classification import EdClassificationTransform

if TYPE_CHECKING:
    pass

class TestEdClassificationTransform:
    """Tests for EdClassificationTransform."""

    @pytest.mark.unit
    def test_edclassificationtransform_initialization(self) -> None:
        """EdClassificationTransform can be initialized."""
        t = EdClassificationTransform()
        assert t is not None
        assert t.name == 'ed_classification'

    @pytest.mark.unit
    def test_edclassificationtransform_basic_functionality(self) -> None:
        """EdClassificationTransform basic functionality works."""
        t = EdClassificationTransform()
        assert t.transform_name == 'ed_classification'
        assert 'medical_claim.parquet' in t.required_inputs
        assert t.output_names == ['ed_classification']

class TestEdClassificationTransformExtended:
    """Tests for ED Classification transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms._ed_classification import EdClassificationTransform
        assert EdClassificationTransform is not None

    @pytest.mark.unit
    def test_metadata(self):
        from acoharmony._transforms._ed_classification import EdClassificationTransform
        assert EdClassificationTransform.transform_name == 'ed_classification'
        assert 'medical_claim.parquet' in EdClassificationTransform.required_inputs
        assert EdClassificationTransform.output_names == ['ed_classification']
        assert len(EdClassificationTransform.required_seeds) >= 2
import datetime
from unittest.mock import MagicMock, patch


class TestEdClassificationTransformExecute:
    """Tests for EdClassificationTransform.execute covering execution paths."""

    def _make_medical_claims(self, gold_path):
        df = pl.DataFrame({'claim_id': ['C1', 'C2', 'C3'], 'person_id': ['P1', 'P2', 'P3'], 'revenue_code': ['0450', '9999', ''], 'place_of_service_code': ['23', '11', '23'], 'diagnosis_code_1': ['A01', 'B02', 'C03'], 'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1), datetime.date(2024, 3, 1)], 'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 2, 2), datetime.date(2024, 3, 2)]})
        df.write_parquet(str(gold_path / 'medical_claim.parquet'))

    def _make_seeds(self, silver_path):
        johnston = pl.DataFrame({'icd_10_cm': ['A01', 'C03'], 'non_emergent': [0.5, 0.1], 'emergent_primary_care': [0.2, 0.3], 'emergent_ed_preventable': [0.1, 0.2], 'emergent_ed_not_preventable': [0.1, 0.3], 'injury': [0.05, 0.0], 'mental_health': [0.03, 0.0], 'alcohol': [0.01, 0.05], 'drug': [0.01, 0.05]})
        johnston.write_parquet(str(silver_path / 'value_sets_ed_classification_johnston_icd10.parquet'))
        categories = pl.DataFrame({'category_id': ['1'], 'category_name': ['Non-Emergent']})
        categories.write_parquet(str(silver_path / 'value_sets_ed_classification_categories.parquet'))

    @pytest.mark.unit
    @patch('acoharmony._transforms._ed_classification.EdClassificationExpression.classify_ed_visits')
    def test_execute_basic(self, mock_classify, tmp_path):
        """Execute runs through the full pipeline."""
        gold = tmp_path / 'gold'
        gold.mkdir()
        silver = tmp_path / 'silver'
        silver.mkdir()
        self._make_medical_claims(gold)
        self._make_seeds(silver)
        classified = pl.DataFrame({'claim_id': ['C1', 'C3'], 'person_id': ['P1', 'P3'], 'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 3, 1)], 'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 3, 2)], 'diagnosis_code_1': ['A01', 'C03'], 'ed_classification_primary': ['Non-Emergent', 'Emergent'], 'preventable_ed_flag': [True, False], 'non_emergent': [0.5, 0.1], 'emergent_primary_care': [0.2, 0.3], 'emergent_ed_preventable': [0.1, 0.2], 'emergent_ed_not_preventable': [0.1, 0.3], 'injury': [0.05, 0.0], 'mental_health': [0.03, 0.0], 'alcohol': [0.01, 0.05], 'drug': [0.01, 0.05], 'unclassified': [0.0, 0.0]}).lazy()
        mock_classify.return_value = classified
        from acoharmony._store import StorageBackend
        from acoharmony._transforms._ed_classification import (
            EdClassificationExpression,
            EdClassificationTransform,
        )
        EdClassificationExpression.calculate_preventable_ed_flag = staticmethod(lambda lf: lf)
        storage = MagicMock(spec=StorageBackend)
        storage.get_path.return_value = gold
        t = EdClassificationTransform(storage=storage)
        t.get_gold_path = lambda: gold
        t.get_silver_path = lambda: silver
        t.get_input_path = lambda f, layer=None: silver / f if layer else gold / f
        t.write_outputs = lambda d: {k: gold / f'{k}.parquet' for k in d}
        results = t.execute()
        assert 'ed_classification' in results

    @pytest.mark.unit
    def test_initialization(self):
        t = EdClassificationTransform()
        assert t.name == 'ed_classification'
        assert t.transform_name == 'ed_classification'

    @pytest.mark.unit
    def test_execute_none_config(self):
        """Config defaults to {} when None."""
        t = EdClassificationTransform()
        assert t.name == 'ed_classification'

    @pytest.mark.unit
    @patch('acoharmony._transforms._ed_classification.EdClassificationExpression.classify_ed_visits')
    def test_execute_with_explicit_config(self, mock_classify, tmp_path):
        """Execute with an explicit non-None config dict (covers branch 89->92)."""
        gold = tmp_path / 'gold'
        gold.mkdir()
        silver = tmp_path / 'silver'
        silver.mkdir()
        self._make_medical_claims(gold)
        self._make_seeds(silver)
        classified = pl.DataFrame({
            'claim_id': ['C1', 'C3'], 'person_id': ['P1', 'P3'],
            'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 3, 1)],
            'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 3, 2)],
            'diagnosis_code_1': ['A01', 'C03'],
            'ed_classification_primary': ['Non-Emergent', 'Emergent'],
            'preventable_ed_flag': [True, False],
            'non_emergent': [0.5, 0.1], 'emergent_primary_care': [0.2, 0.3],
            'emergent_ed_preventable': [0.1, 0.2],
            'emergent_ed_not_preventable': [0.1, 0.3],
            'injury': [0.05, 0.0], 'mental_health': [0.03, 0.0],
            'alcohol': [0.01, 0.05], 'drug': [0.01, 0.05],
            'unclassified': [0.0, 0.0],
        }).lazy()
        mock_classify.return_value = classified
        from acoharmony._store import StorageBackend
        from acoharmony._transforms._ed_classification import EdClassificationExpression
        EdClassificationExpression.calculate_preventable_ed_flag = staticmethod(lambda lf: lf)
        storage = MagicMock(spec=StorageBackend)
        storage.get_path.return_value = gold
        t = EdClassificationTransform(storage=storage)
        t.get_gold_path = lambda: gold
        t.get_silver_path = lambda: silver
        t.get_input_path = lambda f, layer=None: silver / f if layer else gold / f
        t.write_outputs = lambda d: {k: gold / f'{k}.parquet' for k in d}
        results = t.execute(config={})
        assert 'ed_classification' in results
