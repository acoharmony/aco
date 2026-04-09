"""Unit tests for medical_claim transforms module."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
import acoharmony

from acoharmony._transforms.medical_claim import execute

if TYPE_CHECKING:
    pass

class _MockMedallionStorage:
    """Mock medallion storage for transform tests."""

    def __init__(self, silver_path=None, gold_path=None):
        if silver_path is None:
            silver_path = Path('.')
        self.silver_path = silver_path
        self.gold_path = gold_path or silver_path

    def get_path(self, layer: str='silver'):
        if layer == 'silver':
            return self.silver_path
        if layer == 'gold':
            return self.gold_path
        return self.silver_path

class _MockExecutor:
    """Mock executor for transform tests."""

    def __init__(self, base=None, storage_config=None):
        if storage_config is not None:
            self.storage_config = storage_config
        elif base is not None:
            self.storage_config = _MockMedallionStorage(silver_path=base)
        else:
            self.storage_config = _MockMedallionStorage()

@pytest.mark.unit
def test_execute_basic(tmp_path: Path) -> None:
    """execute basic functionality -- unions three claim type parquets."""
    claim_df = TestMedicalClaim._claim_df('DME')
    _write(claim_df, tmp_path / 'int_dme_claim_deduped.parquet')
    _write(claim_df, tmp_path / 'int_physician_claim_deduped.parquet')
    _write(claim_df, tmp_path / 'int_institutional_claim_deduped.parquet')
    result = execute(_make_executor(tmp_path))
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert 'claim_id' in df.columns
    assert df.shape[0] == 3

def _write(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame as parquet, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)

def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()

@pytest.fixture
def executor(tmp_base: Path) -> _MockExecutor:
    return _MockExecutor(tmp_base)

def _make_executor(silver_dir: Path) -> _MockExecutor:
    return _MockExecutor(storage_config=_MockMedallionStorage(silver_path=silver_dir))

def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)

class TestMedicalClaimExecutor:
    """Tests for medical_claim executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import medical_claim
        assert medical_claim is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestMedicalClaim:

    @staticmethod
    def _claim_df(prefix: str) -> pl.DataFrame:
        """Minimal medical_claim schema row."""
        data: dict[str, list] = {'claim_id': [f'{prefix}_CLM1'], 'claim_line_number': [1], 'claim_type': ['professional'], 'person_id': ['MBI1'], 'member_id': ['MBI1'], 'payer': ['medicare'], 'plan': ['medicare'], 'claim_start_date': [None], 'claim_end_date': [None], 'claim_line_start_date': [None], 'claim_line_end_date': [None], 'admission_date': [None], 'discharge_date': [None], 'admit_source_code': [None], 'admit_type_code': [None], 'discharge_disposition_code': [None], 'place_of_service_code': ['11'], 'bill_type_code': [None], 'ms_drg_code': [None], 'apr_drg_code': [None], 'revenue_center_code': [None], 'service_unit_quantity': [None], 'hcpcs_code': ['99213'], 'hcpcs_modifier_1': [None], 'hcpcs_modifier_2': [None], 'hcpcs_modifier_3': [None], 'hcpcs_modifier_4': [None], 'hcpcs_modifier_5': [None], 'rendering_npi': [None], 'rendering_tin': [None], 'billing_npi': [None], 'billing_tin': [None], 'facility_npi': [None], 'paid_date': [None], 'paid_amount': [100.0], 'allowed_amount': [150.0], 'charge_amount': [150.0], 'coinsurance_amount': [None], 'copayment_amount': [None], 'deductible_amount': [None], 'total_cost_amount': [None], 'diagnosis_code_type': [None]}
        for i in range(1, 26):
            data[f'diagnosis_code_{i}'] = [None]
            data[f'diagnosis_poa_{i}'] = [None]
        data['procedure_code_type'] = [None]
        for i in range(1, 26):
            data[f'procedure_code_{i}'] = [None]
            data[f'procedure_date_{i}'] = [None]
        data['in_network_flag'] = [1]
        data['data_source'] = ['medicare cclf']
        data['source_filename'] = ['test.csv']
        data['ingest_datetime'] = ['2023-07-01']
        return pl.DataFrame(data)

    @pytest.mark.unit
    def test_union_of_three_types(self, tmp_path: Path) -> None:
        _write(self._claim_df('DME'), tmp_path / 'int_dme_claim_deduped.parquet')
        _write(self._claim_df('PHY'), tmp_path / 'int_physician_claim_deduped.parquet')
        _write(self._claim_df('INST'), tmp_path / 'int_institutional_claim_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 3
        claim_ids = set(result['claim_id'].to_list())
        assert claim_ids == {'DME_CLM1', 'PHY_CLM1', 'INST_CLM1'}

    @pytest.mark.unit
    def test_schema_preserved(self, tmp_path: Path) -> None:
        _write(self._claim_df('DME'), tmp_path / 'int_dme_claim_deduped.parquet')
        _write(self._claim_df('PHY'), tmp_path / 'int_physician_claim_deduped.parquet')
        _write(self._claim_df('INST'), tmp_path / 'int_institutional_claim_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'claim_id' in result.columns
        assert 'person_id' in result.columns
        assert 'data_source' in result.columns
