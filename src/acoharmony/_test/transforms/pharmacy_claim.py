"""Unit tests for pharmacy_claim transforms module."""
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

from acoharmony._transforms.pharmacy_claim import execute

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
    """execute basic functionality -- casts types on pharmacy claims."""
    df = pl.DataFrame({
        'claim_id': ['CLM1'], 'claim_line_number': ['1'], 'person_id': ['MBI1'],
        'member_id': ['MBI1'], 'payer': ['medicare'], 'plan': ['medicare'],
        'prescribing_provider_npi': ['1234567890'], 'dispensing_provider_npi': ['9876543210'],
        'dispensing_date': ['2023-03-15'], 'ndc_code': ['12345678901'],
        'quantity': ['30'], 'days_supply': ['30'], 'refills': ['1'],
        'paid_date': ['2023-03-15'], 'paid_amount': ['10.50'], 'allowed_amount': [None],
        'charge_amount': [None], 'coinsurance_amount': [None], 'copayment_amount': ['10.50'],
        'deductible_amount': [None], 'in_network_flag': ['1'],
        'data_source': ['medicare cclf'], 'source_filename': ['cclf7.csv'],
        'ingest_datetime': ['2023-07-01'],
    })
    _write(df, tmp_path / 'int_pharmacy_claim_deduped.parquet')
    result = execute(_make_executor(tmp_path))
    assert isinstance(result, pl.LazyFrame)
    collected = result.collect()
    assert 'claim_id' in collected.columns
    assert collected['claim_line_number'].dtype == pl.Int64

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

class TestPharmacyClaimExecutor:
    """Tests for pharmacy_claim executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import pharmacy_claim
        assert pharmacy_claim is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestPharmacyClaim:

    @pytest.mark.unit
    def test_type_casting(self, tmp_path: Path) -> None:
        df = pl.DataFrame({'claim_id': ['CLM1'], 'claim_line_number': ['1'], 'person_id': ['MBI1'], 'member_id': ['MBI1'], 'payer': ['medicare'], 'plan': ['medicare'], 'prescribing_provider_npi': ['1234567890'], 'dispensing_provider_npi': ['9876543210'], 'dispensing_date': ['2023-03-15'], 'ndc_code': ['12345678901'], 'quantity': ['30'], 'days_supply': ['30'], 'refills': ['1'], 'paid_date': ['2023-03-15'], 'paid_amount': ['10.50'], 'allowed_amount': [None], 'charge_amount': [None], 'coinsurance_amount': [None], 'copayment_amount': ['10.50'], 'deductible_amount': [None], 'in_network_flag': ['1'], 'data_source': ['medicare cclf'], 'source_filename': ['cclf7.csv'], 'ingest_datetime': ['2023-07-01']})
        _write(df, tmp_path / 'int_pharmacy_claim_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['claim_line_number'].dtype == pl.Int64
        assert result['quantity'].dtype == pl.Int64
        assert result['claim_id'].dtype == pl.String
