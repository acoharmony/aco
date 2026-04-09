"""Unit tests for int_physician_claim_adr transforms module."""
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

from acoharmony._transforms.int_physician_claim_adr import execute

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
    cclf5 = pl.DataFrame({
        'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'bene_mbi_id': ['MBI1'],
        'bene_hic_num': [None], 'clm_type_cd': ['10'], 'clm_from_dt': ['2023-01-01'],
        'clm_thru_dt': ['2023-01-15'], 'rndrg_prvdr_type_cd': ['01'],
        'rndrg_prvdr_fips_st_cd': ['36'], 'clm_prvdr_spclty_cd': ['01'],
        'clm_fed_type_srvc_cd': ['1'], 'clm_pos_cd': ['11'],
        'clm_line_from_dt': ['2023-01-01'], 'clm_line_thru_dt': ['2023-01-15'],
        'clm_line_hcpcs_cd': ['99213'], 'clm_line_cvrd_pd_amt': ['100.00'],
        'clm_line_prmry_pyr_cd': ['A'], 'clm_line_dgns_cd': ['J9601'],
        'clm_rndrg_prvdr_tax_num': ['123456789'], 'rndrg_prvdr_npi_num': ['1234567890'],
        'clm_carr_pmt_dnl_cd': ['1'], 'clm_prcsg_ind_cd': ['A'],
        'clm_adjsmt_type_cd': ['0'], 'clm_efctv_dt': ['2023-02-01'],
        'clm_idr_ld_dt': ['2023-03-01'], 'clm_cntl_num': ['CTRL1'],
        'bene_eqtbl_bic_hicn_num': [None], 'clm_line_alowd_chrg_amt': ['150.00'],
        'clm_line_srvc_unit_qty': ['1'], 'hcpcs_1_mdfr_cd': [None],
        'hcpcs_2_mdfr_cd': [None], 'hcpcs_3_mdfr_cd': [None], 'hcpcs_4_mdfr_cd': [None],
        'hcpcs_5_mdfr_cd': [None], 'clm_disp_cd': ['01'], 'clm_dgns_1_cd': ['J9601'],
        'clm_dgns_2_cd': [None], 'clm_dgns_3_cd': [None], 'clm_dgns_4_cd': [None],
        'clm_dgns_5_cd': [None], 'clm_dgns_6_cd': [None], 'clm_dgns_7_cd': [None],
        'clm_dgns_8_cd': [None], 'dgns_prcdr_icd_ind': ['0'], 'clm_dgns_9_cd': [None],
        'clm_dgns_10_cd': [None], 'clm_dgns_11_cd': [None], 'clm_dgns_12_cd': [None],
        'hcpcs_betos_cd': [None], 'source_filename': ['cclf5.csv'],
        'file_date': ['2023-07-01'],
    })
    _write(cclf5, tmp_path / 'cclf5.parquet')
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

class TestIntPhysicianClaimAdrExtended:
    """Tests for int_physician_claim_adr executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_physician_claim_adr
        assert int_physician_claim_adr is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntPhysicianClaimAdr:

    @staticmethod
    def _cclf5(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'bene_mbi_id': ['MBI1'], 'bene_hic_num': [None], 'clm_type_cd': ['10'], 'clm_from_dt': ['2023-01-01'], 'clm_thru_dt': ['2023-01-15'], 'rndrg_prvdr_type_cd': ['01'], 'rndrg_prvdr_fips_st_cd': ['36'], 'clm_prvdr_spclty_cd': ['01'], 'clm_fed_type_srvc_cd': ['1'], 'clm_pos_cd': ['11'], 'clm_line_from_dt': ['2023-01-01'], 'clm_line_thru_dt': ['2023-01-15'], 'clm_line_hcpcs_cd': ['99213'], 'clm_line_cvrd_pd_amt': ['100.00'], 'clm_line_prmry_pyr_cd': ['A'], 'clm_line_dgns_cd': ['J9601'], 'clm_rndrg_prvdr_tax_num': ['123456789'], 'rndrg_prvdr_npi_num': ['1234567890'], 'clm_carr_pmt_dnl_cd': ['1'], 'clm_prcsg_ind_cd': ['A'], 'clm_adjsmt_type_cd': ['0'], 'clm_efctv_dt': ['2023-02-01'], 'clm_idr_ld_dt': ['2023-03-01'], 'clm_cntl_num': ['CTRL1'], 'bene_eqtbl_bic_hicn_num': [None], 'clm_line_alowd_chrg_amt': ['150.00'], 'clm_line_srvc_unit_qty': ['1'], 'hcpcs_1_mdfr_cd': [None], 'hcpcs_2_mdfr_cd': [None], 'hcpcs_3_mdfr_cd': [None], 'hcpcs_4_mdfr_cd': [None], 'hcpcs_5_mdfr_cd': [None], 'clm_disp_cd': ['01'], 'clm_dgns_1_cd': ['J9601'], 'clm_dgns_2_cd': [None], 'clm_dgns_3_cd': [None], 'clm_dgns_4_cd': [None], 'clm_dgns_5_cd': [None], 'clm_dgns_6_cd': [None], 'clm_dgns_7_cd': [None], 'clm_dgns_8_cd': [None], 'dgns_prcdr_icd_ind': ['0'], 'clm_dgns_9_cd': [None], 'clm_dgns_10_cd': [None], 'clm_dgns_11_cd': [None], 'clm_dgns_12_cd': [None], 'hcpcs_betos_cd': [None], 'source_filename': ['cclf5.csv'], 'file_date': ['2023-07-01']}
        base.update(overrides)
        return pl.DataFrame(base)

    @pytest.mark.unit
    def test_output_columns(self, tmp_path: Path) -> None:
        _write(self._cclf5(), tmp_path / 'cclf5.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'current_bene_mbi_id' in result.columns
        assert 'row_num' in result.columns
        assert 'clm_line_cvrd_pd_amt' in result.columns

    @pytest.mark.unit
    def test_cancellation_negates_amounts(self, tmp_path: Path) -> None:
        _write(self._cclf5(clm_adjsmt_type_cd=['1'], clm_line_cvrd_pd_amt=['100.00']), tmp_path / 'cclf5.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert float(result['clm_line_cvrd_pd_amt'][0]) == -100.0

    @pytest.mark.unit
    def test_mbi_crosswalk(self, tmp_path: Path) -> None:
        _write(self._cclf5(bene_mbi_id=['OLD_MBI']), tmp_path / 'cclf5.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['current_bene_mbi_id'][0] == 'NEW_MBI'
