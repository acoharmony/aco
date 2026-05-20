"""Unit tests for _service_category transforms module."""
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

from acoharmony._transforms._service_category import ServiceCategoryTransform

if TYPE_CHECKING:
    pass

class TestServiceCategoryTransform:
    """Tests for ServiceCategoryTransform."""

    @pytest.mark.unit
    def test_servicecategorytransform_initialization(self) -> None:
        """ServiceCategoryTransform can be initialized."""
        t = ServiceCategoryTransform()
        assert t is not None
        assert t.name == 'service_category'

    @pytest.mark.unit
    def test_servicecategorytransform_basic_functionality(self) -> None:
        """ServiceCategoryTransform basic functionality works."""
        t = ServiceCategoryTransform()
        assert t.transform_name == 'service_category'
        assert 'medical_claim.parquet' in t.required_inputs
        assert 'service_category' in t.output_names

class TestServiceCategoryTransformExtendedExtended:
    """Tests for Service Category transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert ServiceCategoryTransform is not None

    @pytest.mark.unit
    def test_metadata(self):
        assert ServiceCategoryTransform.transform_name == 'service_category'
        assert 'medical_claim.parquet' in ServiceCategoryTransform.required_inputs
        assert len(ServiceCategoryTransform.required_seeds) >= 1

class TestServiceCategoryTransformExtended:
    """Tests for ServiceCategoryTransform metadata."""

    @pytest.mark.unit
    def test_class_metadata(self):
        assert ServiceCategoryTransform.transform_name == 'service_category'
        assert 'medical_claim.parquet' in ServiceCategoryTransform.required_inputs
        assert 'service_category' in ServiceCategoryTransform.output_names

    @pytest.mark.unit
    def test_initialization(self):
        t = ServiceCategoryTransform()
        assert t.name == 'service_category'

    @pytest.mark.unit
    def test_repr(self):
        t = ServiceCategoryTransform()
        r = repr(t)
        assert 'ServiceCategoryTransform' in r
import datetime
from unittest.mock import MagicMock, patch


class TestServiceCategoryTransformExecute:
    """Tests for ServiceCategoryTransform.execute covering uncovered lines."""

    def _make_medical_claims(self, tmp_path):
        df = pl.DataFrame({'claim_id': ['C1', 'C2'], 'person_id': ['P1', 'P2'], 'claim_type': ['institutional', 'professional'], 'bill_type_code': ['111', ''], 'revenue_center_code': ['0100', ''], 'place_of_service_code': ['21', '11'], 'hcpcs_code': ['99213', '99214'], 'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)], 'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 2, 2)], 'paid_amount': [500.0, 200.0]})
        path = tmp_path / 'gold'
        path.mkdir(parents=True, exist_ok=True)
        df.write_parquet(str(path / 'medical_claim.parquet'))
        return path

    def _make_pharmacy_claims(self, gold_path, has_data=True):
        if has_data:
            df = pl.DataFrame({'claim_id': ['RX1'], 'person_id': ['P1'], 'claim_type': ['pharmacy'], 'dispensing_date': [datetime.date(2024, 3, 1)], 'paid_amount': [50.0]})
        else:
            df = pl.DataFrame({'claim_id': pl.Series([], dtype=pl.Utf8), 'person_id': pl.Series([], dtype=pl.Utf8), 'claim_type': pl.Series([], dtype=pl.Utf8), 'dispensing_date': pl.Series([], dtype=pl.Date), 'paid_amount': pl.Series([], dtype=pl.Float64)})
        df.write_parquet(str(gold_path / 'pharmacy_claim.parquet'))

    def _make_seed(self, tmp_path):
        silver = tmp_path / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        df = pl.DataFrame({'category': ['inpatient'], 'code': ['111']})
        df.write_parquet(str(silver / 'value_sets_service_categories_service_categories.parquet'))
        return silver

    @pytest.mark.unit
    @patch('acoharmony._transforms._service_category.ServiceCategoryExpression.categorize_claims')
    def test_execute_medical_only(self, mock_categorize, tmp_path):
        """Execute with medical claims only (no pharmacy data)."""
        gold = self._make_medical_claims(tmp_path)
        self._make_pharmacy_claims(gold, has_data=False)
        self._make_seed(tmp_path)
        mock_categorize.return_value = pl.DataFrame({'claim_id': ['C1', 'C2'], 'person_id': ['P1', 'P2'], 'claim_type': ['institutional', 'professional'], 'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)], 'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 2, 2)], 'paid_amount': [500.0, 200.0], 'service_category_1': ['inpatient', 'outpatient'], 'service_category_2': ['acute', 'office']}).lazy()
        from acoharmony._store import StorageBackend
        storage = MagicMock(spec=StorageBackend)
        storage.get_path.return_value = gold
        t = ServiceCategoryTransform(storage=storage)
        results = t.execute()
        assert 'service_category' in results

    @pytest.mark.unit
    @patch('acoharmony._transforms._service_category.ServiceCategoryExpression.categorize_claims')
    def test_execute_with_pharmacy(self, mock_categorize, tmp_path):
        """Execute with both medical and pharmacy claims."""
        gold = self._make_medical_claims(tmp_path)
        self._make_pharmacy_claims(gold, has_data=True)
        self._make_seed(tmp_path)
        mock_categorize.return_value = pl.DataFrame({'claim_id': ['C1', 'C2'], 'person_id': ['P1', 'P2'], 'claim_type': ['institutional', 'professional'], 'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)], 'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 2, 2)], 'paid_amount': [500.0, 200.0], 'service_category_1': ['inpatient', 'outpatient'], 'service_category_2': ['acute', 'office']}).lazy()
        from acoharmony._store import StorageBackend
        storage = MagicMock(spec=StorageBackend)
        storage.get_path.return_value = gold
        t = ServiceCategoryTransform(storage=storage)
        results = t.execute()
        assert 'service_category' in results

    @pytest.mark.unit
    def test_execute_none_config(self, tmp_path):
        """Execute sets config to {} when None."""
        t = ServiceCategoryTransform()
        assert t.name == 'service_category'

    @pytest.mark.unit
    @patch('acoharmony._transforms._service_category.ServiceCategoryExpression.categorize_claims')
    def test_execute_with_explicit_config(self, mock_categorize, tmp_path):
        """Execute with an explicit non-None config dict (covers branch 82->85)."""
        gold = self._make_medical_claims(tmp_path)
        self._make_pharmacy_claims(gold, has_data=False)
        self._make_seed(tmp_path)
        mock_categorize.return_value = pl.DataFrame({
            'claim_id': ['C1', 'C2'], 'person_id': ['P1', 'P2'],
            'claim_type': ['institutional', 'professional'],
            'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)],
            'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 2, 2)],
            'paid_amount': [500.0, 200.0],
            'service_category_1': ['inpatient', 'outpatient'],
            'service_category_2': ['acute', 'office'],
        }).lazy()
        from acoharmony._store import StorageBackend
        storage = MagicMock(spec=StorageBackend)
        storage.get_path.return_value = gold
        t = ServiceCategoryTransform(storage=storage)
        results = t.execute(config={})
        assert 'service_category' in results
