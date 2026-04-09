"""Unit tests for int_diagnosis_pivot transforms module."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
import acoharmony

from acoharmony._transforms.int_diagnosis_pivot import execute

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
    """execute basic functionality -- pivot produces diagnosis_code_* columns."""
    deduped = pl.DataFrame({
        'cur_clm_uniq_id': ['CLM1'], 'current_bene_mbi_id': ['MBI1'],
        'dgns_prcdr_icd_ind': ['0'], 'clm_val_sqnc_num': ['01'],
        'clm_dgns_cd': ['J9601'], 'clm_poa_ind': ['Y'],
    })
    _write(deduped, tmp_path / 'int_diagnosis_deduped.parquet')
    result = execute(_make_executor(tmp_path))
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert 'diagnosis_code_1' in df.columns
    assert df.shape[0] >= 1

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

class TestIntDiagnosisPivot:
    """Tests for int_diagnosis_pivot executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_diagnosis_pivot
        assert int_diagnosis_pivot is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntDiagnosisPivotExtended:

    @pytest.mark.unit
    def test_pivot_creates_diagnosis_columns(self, tmp_path: Path) -> None:
        deduped = pl.DataFrame({'cur_clm_uniq_id': ['CLM1', 'CLM1', 'CLM1'], 'current_bene_mbi_id': ['MBI1', 'MBI1', 'MBI1'], 'dgns_prcdr_icd_ind': ['0', '0', '0'], 'clm_val_sqnc_num': ['01', '02', '03'], 'clm_dgns_cd': ['J9601', 'E119', 'I10'], 'clm_poa_ind': ['Y', 'N', 'Y']})
        _write(deduped, tmp_path / 'int_diagnosis_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        for i in range(1, 26):
            assert f'diagnosis_code_{i}' in result.columns
            assert f'diagnosis_poa_{i}' in result.columns
        assert result.shape[0] == 1
        assert result['diagnosis_code_1'][0] == 'J9601'
        assert result['diagnosis_code_2'][0] == 'E119'
        assert result['diagnosis_code_3'][0] == 'I10'
        assert result['diagnosis_poa_1'][0] == 'Y'
        assert result['diagnosis_poa_2'][0] == 'N'

    @pytest.mark.unit
    def test_pivot_sparse(self, tmp_path: Path) -> None:
        """Only sequence 01 present, rest should be null."""
        deduped = pl.DataFrame({'cur_clm_uniq_id': ['CLM1'], 'current_bene_mbi_id': ['MBI1'], 'dgns_prcdr_icd_ind': ['0'], 'clm_val_sqnc_num': ['01'], 'clm_dgns_cd': ['J9601'], 'clm_poa_ind': ['Y']})
        _write(deduped, tmp_path / 'int_diagnosis_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['diagnosis_code_1'][0] == 'J9601'
        assert result['diagnosis_code_2'][0] is None
