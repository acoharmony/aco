"""Unit tests for int_dme_claim_adr transforms module."""
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

from acoharmony._transforms.int_dme_claim_adr import execute

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
    """execute basic functionality -- returns LazyFrame with current_bene_mbi_id."""
    cclf6 = pl.DataFrame({
        'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'bene_mbi_id': ['MBI1'],
        'clm_from_dt': ['2023-01-01'], 'clm_thru_dt': ['2023-01-15'],
        'clm_line_hcpcs_cd': ['E0601'], 'clm_line_cvrd_pd_amt': ['200.00'],
        'clm_adjsmt_type_cd': ['0'], 'clm_cntl_num': ['CTRL1'], 'clm_pos_cd': ['11'],
        'clm_line_from_dt': ['2023-01-01'], 'clm_line_thru_dt': ['2023-01-15'],
        'payto_prvdr_npi_num': ['1234567890'], 'ordrg_prvdr_npi_num': ['9876543210'],
        'clm_efctv_dt': ['2023-02-01'], 'clm_line_alowd_chrg_amt': ['250.00'],
        'source_filename': ['cclf6.csv'], 'file_date': ['2023-07-01'],
    })
    _write(cclf6, tmp_path / 'cclf6.parquet')
    _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
    result = execute(_make_executor(tmp_path))
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert 'current_bene_mbi_id' in df.columns
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
    """Minimal int_beneficiary_xref_deduped with one mapping row."""
    return pl.DataFrame({'prvs_num': ['OLD_MBI'], 'crnt_num': ['NEW_MBI'], 'prvs_id_efctv_dt': ['2023-01-01'], 'prvs_id_obslt_dt': ['2023-06-01'], 'source_filename': ['xref.csv'], 'file_date': ['2023-07-01']})

def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)

class TestIntDmeClaimAdr:
    """Tests for int_dme_claim_adr executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_dme_claim_adr
        assert int_dme_claim_adr is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntDmeClaimAdrExtended:

    @staticmethod
    def _cclf6(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'bene_mbi_id': ['MBI1'], 'clm_from_dt': ['2023-01-01'], 'clm_thru_dt': ['2023-01-15'], 'clm_line_hcpcs_cd': ['E0601'], 'clm_line_cvrd_pd_amt': ['200.00'], 'clm_adjsmt_type_cd': ['0'], 'clm_cntl_num': ['CTRL1'], 'clm_pos_cd': ['11'], 'clm_line_from_dt': ['2023-01-01'], 'clm_line_thru_dt': ['2023-01-15'], 'payto_prvdr_npi_num': ['1234567890'], 'ordrg_prvdr_npi_num': ['9876543210'], 'clm_efctv_dt': ['2023-02-01'], 'clm_line_alowd_chrg_amt': ['250.00'], 'source_filename': ['cclf6.csv'], 'file_date': ['2023-07-01']}
        base.update(overrides)
        return pl.DataFrame(base)

    @pytest.mark.unit
    def test_output_columns(self, tmp_path: Path) -> None:
        _write(self._cclf6(), tmp_path / 'cclf6.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'current_bene_mbi_id' in result.columns
        assert 'row_num' in result.columns
        assert 'clm_line_cvrd_pd_amt' in result.columns

    @pytest.mark.unit
    def test_cancellation_negates_amounts(self, tmp_path: Path) -> None:
        _write(self._cclf6(clm_adjsmt_type_cd=['1'], clm_line_cvrd_pd_amt=['200.00']), tmp_path / 'cclf6.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert float(result['clm_line_cvrd_pd_amt'][0]) == -200.0
