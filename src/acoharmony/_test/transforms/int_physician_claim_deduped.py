"""Unit tests for int_physician_claim_deduped transforms module."""
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

from acoharmony._transforms.int_physician_claim_deduped import execute

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
    """execute basic functionality -- returns LazyFrame with claim_id."""
    adr = pl.DataFrame({
        'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'current_bene_mbi_id': ['MBI1'],
        'clm_from_dt': [date(2023, 1, 1)], 'clm_thru_dt': [date(2023, 1, 15)],
        'clm_pos_cd': ['11'], 'clm_line_from_dt': [date(2023, 1, 1)],
        'clm_line_thru_dt': [date(2023, 1, 15)], 'clm_line_hcpcs_cd': ['99213'],
        'clm_line_cvrd_pd_amt': [100.0], 'clm_rndrg_prvdr_tax_num': ['123456789'],
        'rndrg_prvdr_npi_num': ['1234567890'], 'clm_adjsmt_type_cd': ['0'],
        'clm_efctv_dt': [date(2023, 2, 1)], 'clm_cntl_num': ['CTRL1'],
        'clm_line_alowd_chrg_amt': [150.0], 'clm_line_srvc_unit_qty': ['1'],
        'hcpcs_1_mdfr_cd': [None], 'hcpcs_2_mdfr_cd': [None], 'hcpcs_3_mdfr_cd': [None],
        'hcpcs_4_mdfr_cd': [None], 'hcpcs_5_mdfr_cd': [None],
        'clm_dgns_1_cd': ['J9601'], 'clm_dgns_2_cd': [None], 'clm_dgns_3_cd': [None],
        'clm_dgns_4_cd': [None], 'clm_dgns_5_cd': [None], 'clm_dgns_6_cd': [None],
        'clm_dgns_7_cd': [None], 'clm_dgns_8_cd': [None], 'dgns_prcdr_icd_ind': ['0'],
        'clm_dgns_9_cd': [None], 'clm_dgns_10_cd': [None], 'clm_dgns_11_cd': [None],
        'clm_dgns_12_cd': [None], 'source_filename': ['cclf5.csv'],
        'file_date': ['2023-07-01'], 'row_num': [1],
    })
    _write(adr, tmp_path / 'int_physician_claim_adr.parquet')
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

class TestIntPhysicianClaimDedupedExtended:
    """Tests for int_physician_claim_deduped executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_physician_claim_deduped
        assert int_physician_claim_deduped is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntPhysicianClaimDeduped:

    @staticmethod
    def _adr_df(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'current_bene_mbi_id': ['MBI1'], 'clm_from_dt': [date(2023, 1, 1)], 'clm_thru_dt': [date(2023, 1, 15)], 'clm_pos_cd': ['11'], 'clm_line_from_dt': [date(2023, 1, 1)], 'clm_line_thru_dt': [date(2023, 1, 15)], 'clm_line_hcpcs_cd': ['99213'], 'clm_line_cvrd_pd_amt': [100.0], 'clm_rndrg_prvdr_tax_num': ['123456789'], 'rndrg_prvdr_npi_num': ['1234567890'], 'clm_adjsmt_type_cd': ['0'], 'clm_efctv_dt': [date(2023, 2, 1)], 'clm_cntl_num': ['CTRL1'], 'clm_line_alowd_chrg_amt': [150.0], 'clm_line_srvc_unit_qty': ['1'], 'hcpcs_1_mdfr_cd': [None], 'hcpcs_2_mdfr_cd': [None], 'hcpcs_3_mdfr_cd': [None], 'hcpcs_4_mdfr_cd': [None], 'hcpcs_5_mdfr_cd': [None], 'clm_dgns_1_cd': ['J9601'], 'clm_dgns_2_cd': [None], 'clm_dgns_3_cd': [None], 'clm_dgns_4_cd': [None], 'clm_dgns_5_cd': [None], 'clm_dgns_6_cd': [None], 'clm_dgns_7_cd': [None], 'clm_dgns_8_cd': [None], 'dgns_prcdr_icd_ind': ['0'], 'clm_dgns_9_cd': [None], 'clm_dgns_10_cd': [None], 'clm_dgns_11_cd': [None], 'clm_dgns_12_cd': [None], 'source_filename': ['cclf5.csv'], 'file_date': ['2023-07-01'], 'row_num': [1]}
        base.update(overrides)
        return pl.DataFrame(base)

    @pytest.mark.unit
    def test_basic_output(self, tmp_path: Path) -> None:
        _write(self._adr_df(), tmp_path / 'int_physician_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'claim_id' in result.columns
        assert result['claim_type'][0] == 'professional'
        assert result['diagnosis_code_1'][0] == 'J9601'

    @pytest.mark.unit
    def test_canceled_excluded(self, tmp_path: Path) -> None:
        _write(self._adr_df(clm_adjsmt_type_cd=['1']), tmp_path / 'int_physician_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_rendering_npi_mapped(self, tmp_path: Path) -> None:
        _write(self._adr_df(), tmp_path / 'int_physician_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['rendering_npi'][0] == '1234567890'
        assert result['rendering_tin'][0] == '123456789'
