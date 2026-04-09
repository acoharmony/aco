"""Unit tests for int_pharmacy_claim_adr transforms module."""
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

from acoharmony._transforms.int_pharmacy_claim_adr import execute

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
    cclf7 = pl.DataFrame({
        'cur_clm_uniq_id': ['CLM1'], 'bene_mbi_id': ['MBI1'], 'bene_hic_num': [None],
        'clm_line_ndc_cd': ['12345678901'], 'clm_line_from_dt': ['2023-03-15'],
        'prvdr_srvc_id_qlfyr_cd': ['01'], 'clm_srvc_prvdr_gnrc_id_num': ['1234567890'],
        'clm_dspnsng_stus_cd': ['P'], 'clm_line_srvc_unit_qty': ['30'],
        'clm_line_days_suply_qty': ['30'], 'prvdr_prsbng_id_qlfyr_cd': ['01'],
        'clm_prsbng_prvdr_gnrc_id_num': ['9876543210'], 'clm_line_bene_pmt_amt': ['10.50'],
        'clm_adjsmt_type_cd': ['0'], 'clm_line_rx_srvc_rfrnc_num': ['REF001'],
        'clm_line_rx_fill_num': ['1'], 'source_filename': ['cclf7.csv'],
        'file_date': ['2023-07-01'],
    })
    _write(cclf7, tmp_path / 'cclf7.parquet')
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

class TestIntPharmacyClaimAdrExtended:
    """Tests for int_pharmacy_claim_adr executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_pharmacy_claim_adr
        assert int_pharmacy_claim_adr is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntPharmacyClaimAdr:

    @staticmethod
    def _cclf7(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'bene_mbi_id': ['MBI1'], 'bene_hic_num': [None], 'clm_line_ndc_cd': ['12345678901'], 'clm_line_from_dt': ['2023-03-15'], 'prvdr_srvc_id_qlfyr_cd': ['01'], 'clm_srvc_prvdr_gnrc_id_num': ['1234567890'], 'clm_dspnsng_stus_cd': ['P'], 'clm_line_srvc_unit_qty': ['30'], 'clm_line_days_suply_qty': ['30'], 'prvdr_prsbng_id_qlfyr_cd': ['01'], 'clm_prsbng_prvdr_gnrc_id_num': ['9876543210'], 'clm_line_bene_pmt_amt': ['10.50'], 'clm_adjsmt_type_cd': ['0'], 'clm_line_rx_srvc_rfrnc_num': ['REF001'], 'clm_line_rx_fill_num': ['1'], 'source_filename': ['cclf7.csv'], 'file_date': ['2023-07-01']}
        base.update(overrides)
        return pl.DataFrame(base)

    @pytest.mark.unit
    def test_output_columns(self, tmp_path: Path) -> None:
        _write(self._cclf7(), tmp_path / 'cclf7.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'current_bene_mbi_id' in result.columns
        assert 'row_num' in result.columns

    @pytest.mark.unit
    def test_mbi_crosswalk(self, tmp_path: Path) -> None:
        _write(self._cclf7(bene_mbi_id=['OLD_MBI']), tmp_path / 'cclf7.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['current_bene_mbi_id'][0] == 'NEW_MBI'

    @pytest.mark.unit
    def test_dedup_by_file_date(self, tmp_path: Path) -> None:
        df = pl.concat([self._cclf7(file_date=['2023-01-01']), self._cclf7(file_date=['2023-07-01'])])
        _write(df, tmp_path / 'cclf7.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 1
