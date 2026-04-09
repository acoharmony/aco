"""Unit tests for int_dme_claim_deduped transforms module."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
import acoharmony

from acoharmony._transforms.int_dme_claim_deduped import execute

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
    """execute basic functionality -- returns LazyFrame with claim_id column."""
    adr = pl.DataFrame({
        'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'current_bene_mbi_id': ['MBI1'],
        'clm_from_dt': [date(2023, 1, 1)], 'clm_thru_dt': [date(2023, 1, 15)],
        'clm_pos_cd': ['11'], 'clm_line_from_dt': [date(2023, 1, 1)],
        'clm_line_thru_dt': [date(2023, 1, 15)], 'clm_line_hcpcs_cd': ['E0601'],
        'clm_line_cvrd_pd_amt': [200.0], 'payto_prvdr_npi_num': ['1234567890'],
        'ordrg_prvdr_npi_num': ['9876543210'], 'clm_adjsmt_type_cd': ['0'],
        'clm_efctv_dt': [date(2023, 2, 1)], 'clm_cntl_num': ['CTRL1'],
        'clm_line_alowd_chrg_amt': [250.0], 'source_filename': ['cclf6.csv'],
        'file_date': ['2023-07-01'], 'row_num': [1],
    })
    _write(adr, tmp_path / 'int_dme_claim_adr.parquet')
    result = execute(_make_executor(tmp_path))
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert 'claim_id' in df.columns

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

class TestIntDmeClaimDedupedExtended:
    """Tests for int_dme_claim_deduped executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_dme_claim_deduped
        assert int_dme_claim_deduped is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntDmeClaimDeduped:

    @staticmethod
    def _adr_df(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'current_bene_mbi_id': ['MBI1'], 'clm_from_dt': [date(2023, 1, 1)], 'clm_thru_dt': [date(2023, 1, 15)], 'clm_pos_cd': ['11'], 'clm_line_from_dt': [date(2023, 1, 1)], 'clm_line_thru_dt': [date(2023, 1, 15)], 'clm_line_hcpcs_cd': ['E0601'], 'clm_line_cvrd_pd_amt': [200.0], 'payto_prvdr_npi_num': ['1234567890'], 'ordrg_prvdr_npi_num': ['9876543210'], 'clm_adjsmt_type_cd': ['0'], 'clm_efctv_dt': [date(2023, 2, 1)], 'clm_cntl_num': ['CTRL1'], 'clm_line_alowd_chrg_amt': [250.0], 'source_filename': ['cclf6.csv'], 'file_date': ['2023-07-01'], 'row_num': [1]}
        base.update(overrides)
        return pl.DataFrame(base)

    @pytest.mark.unit
    def test_basic_output(self, tmp_path: Path) -> None:
        _write(self._adr_df(), tmp_path / 'int_dme_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'claim_id' in result.columns
        assert 'claim_type' in result.columns
        assert result['claim_type'][0] == 'professional'
        assert result['payer'][0] == 'medicare'
        assert result['data_source'][0] == 'medicare cclf'

    @pytest.mark.unit
    def test_canceled_excluded(self, tmp_path: Path) -> None:
        _write(self._adr_df(clm_adjsmt_type_cd=['1']), tmp_path / 'int_dme_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_non_latest_excluded(self, tmp_path: Path) -> None:
        _write(self._adr_df(row_num=[2]), tmp_path / 'int_dme_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_negative_amounts_excluded(self, tmp_path: Path) -> None:
        _write(self._adr_df(clm_line_cvrd_pd_amt=[-100.0]), tmp_path / 'int_dme_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_sentinel_dates_nullified(self, tmp_path: Path) -> None:
        _write(self._adr_df(clm_from_dt=[date(1000, 1, 1)], clm_efctv_dt=[date(9999, 12, 31)]), tmp_path / 'int_dme_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        if result.shape[0] > 0:
            assert result['claim_start_date'][0] is None
            assert result['paid_date'][0] is None

    @pytest.mark.unit
    def test_diagnosis_procedure_columns_null_for_dme(self, tmp_path: Path) -> None:
        _write(self._adr_df(), tmp_path / 'int_dme_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        for i in range(1, 26):
            assert result[f'diagnosis_code_{i}'][0] is None
            assert result[f'procedure_code_{i}'][0] is None
