"""Unit tests for int_diagnosis_deduped transforms module."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
import acoharmony

from acoharmony._transforms.int_diagnosis_deduped import execute

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
    """execute basic functionality -- returns LazyFrame with diagnosis columns."""
    cclf4 = pl.DataFrame({
        'cur_clm_uniq_id': ['CLM1'], 'bene_mbi_id': ['MBI1'], 'clm_type_cd': ['10'],
        'clm_prod_type_cd': ['50'], 'clm_val_sqnc_num': ['01'], 'clm_dgns_cd': ['J9601'],
        'prvdr_oscar_num': ['100001'], 'clm_from_dt': ['2023-01-01'],
        'clm_thru_dt': ['2023-01-15'], 'clm_poa_ind': ['Y'], 'dgns_prcdr_icd_ind': ['0'],
        'source_filename': ['cclf4.csv'], 'file_date': ['2023-07-01'],
    })
    _write(cclf4, tmp_path / 'cclf4.parquet')
    _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
    result = execute(_make_executor(tmp_path))
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert 'cur_clm_uniq_id' in df.columns
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

def _xref_df() -> pl.DataFrame:
    """Minimal identity_timeline fixture: OLD_MBI remapped to NEW_MBI."""
    return pl.DataFrame(
        {
            'mbi': ['OLD_MBI', 'NEW_MBI'],
            'maps_to_mbi': ['NEW_MBI', None],
            'effective_date': [None, None],
            'obsolete_date': [None, None],
            'file_date': [None, None],
            'observation_type': ['cclf9_remap', 'cclf8_self'],
            'source_file': ['xref.csv', 'xref.csv'],
            'hcmpi': [None, None],
            'chain_id': ['chain_test', 'chain_test'],
            'hop_index': [1, 0],
            'is_current_as_of_file_date': [True, True],
        },
        schema={
            'mbi': pl.String, 'maps_to_mbi': pl.String,
            'effective_date': pl.Date, 'obsolete_date': pl.Date, 'file_date': pl.Date,
            'observation_type': pl.String, 'source_file': pl.String, 'hcmpi': pl.String,
            'chain_id': pl.String, 'hop_index': pl.Int64,
            'is_current_as_of_file_date': pl.Boolean,
        },
    )

def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)

class TestIntDiagnosisDedupedExtended:
    """Tests for int_diagnosis_deduped executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_diagnosis_deduped
        assert int_diagnosis_deduped is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntDiagnosisDeduped:

    @staticmethod
    def _cclf4(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'bene_mbi_id': ['MBI1'], 'clm_type_cd': ['10'], 'clm_prod_type_cd': ['50'], 'clm_val_sqnc_num': ['01'], 'clm_dgns_cd': ['J9601'], 'prvdr_oscar_num': ['100001'], 'clm_from_dt': ['2023-01-01'], 'clm_thru_dt': ['2023-01-15'], 'clm_poa_ind': ['Y'], 'dgns_prcdr_icd_ind': ['0'], 'source_filename': ['cclf4.csv'], 'file_date': ['2023-07-01']}
        base.update(overrides)
        return pl.DataFrame(base)

    @pytest.mark.unit
    def test_output_columns(self, tmp_path: Path) -> None:
        _write(self._cclf4(), tmp_path / 'cclf4.parquet')
        _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        expected = {'cur_clm_uniq_id', 'current_bene_mbi_id', 'bene_hic_num', 'clm_type_cd', 'clm_prod_type_cd', 'clm_val_sqnc_num', 'clm_dgns_cd', 'bene_eqtbl_bic_hicn_num', 'prvdr_oscar_num', 'clm_from_dt', 'clm_thru_dt', 'clm_poa_ind', 'dgns_prcdr_icd_ind', 'source_filename', 'file_date'}
        assert set(result.columns) == expected

    @pytest.mark.unit
    def test_null_mbi_filtered_out(self, tmp_path: Path) -> None:
        df = pl.concat([self._cclf4(), self._cclf4(bene_mbi_id=[None], cur_clm_uniq_id=['CLM_NULL'])])
        _write(df, tmp_path / 'cclf4.parquet')
        _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 1

    @pytest.mark.unit
    def test_dedup_by_file_date(self, tmp_path: Path) -> None:
        df = pl.concat([self._cclf4(file_date=['2023-01-01'], source_filename=['old.csv']), self._cclf4(file_date=['2023-07-01'], source_filename=['new.csv'])])
        _write(df, tmp_path / 'cclf4.parquet')
        _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 1
