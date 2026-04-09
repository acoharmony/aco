"""Unit tests for int_institutional_claim_deduped transforms module."""
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

from acoharmony._transforms.int_institutional_claim_deduped import execute

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
    adr = TestIntInstitutionalClaimDeduped._adr_df()
    _write(adr, tmp_path / 'int_institutional_claim_adr.parquet')
    _write(TestIntInstitutionalClaimDeduped._revenue_center_df(), tmp_path / 'int_revenue_center_deduped.parquet')
    _write(TestIntInstitutionalClaimDeduped._diag_pivot_df(), tmp_path / 'int_diagnosis_pivot.parquet')
    _write(TestIntInstitutionalClaimDeduped._proc_pivot_df(), tmp_path / 'int_procedure_pivot.parquet')
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

class TestIntInstitutionalClaimDedupedExtended:
    """Tests for int_institutional_claim_deduped executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_institutional_claim_deduped
        assert int_institutional_claim_deduped is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntInstitutionalClaimDeduped:

    @staticmethod
    def _adr_df(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'current_bene_mbi_id': ['MBI1'], 'clm_from_dt': [date(2023, 1, 1)], 'clm_thru_dt': [date(2023, 1, 15)], 'clm_bill_fac_type_cd': ['1'], 'clm_bill_clsfctn_cd': ['1'], 'clm_pmt_amt': [5000.0], 'bene_ptnt_stus_cd': ['01'], 'dgns_drg_cd': ['470'], 'fac_prvdr_npi_num': ['1234567890'], 'atndg_prvdr_npi_num': ['1111111111'], 'clm_adjsmt_type_cd': ['0'], 'clm_efctv_dt': [date(2023, 2, 1)], 'clm_admsn_type_cd': ['1'], 'clm_admsn_src_cd': ['1'], 'clm_bill_freq_cd': ['1'], 'dgns_prcdr_icd_ind': ['0'], 'clm_mdcr_instnl_tot_chrg_amt': [6000.0], 'clm_blg_prvdr_oscar_num': ['100001'], 'source_filename': ['cclf1.csv'], 'file_date': ['2023-07-01'], 'row_num': [1]}
        base.update(overrides)
        return pl.DataFrame(base)

    @staticmethod
    def _revenue_center_df(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'current_bene_mbi_id': ['MBI1'], 'bene_hic_num': [None], 'clm_type_cd': ['10'], 'clm_line_from_dt': [date(2023, 1, 1)], 'clm_line_thru_dt': [date(2023, 1, 15)], 'clm_line_prod_rev_ctr_cd': ['0450'], 'clm_line_instnl_rev_ctr_dt': ['2023-01-01'], 'clm_line_hcpcs_cd': ['99213'], 'bene_eqtbl_bic_hicn_num': [None], 'prvdr_oscar_num': ['100001'], 'clm_from_dt': ['2023-01-01'], 'clm_thru_dt': ['2023-01-15'], 'clm_line_srvc_unit_qty': ['1'], 'clm_line_cvrd_pd_amt': [5000.0], 'hcpcs_1_mdfr_cd': [None], 'hcpcs_2_mdfr_cd': [None], 'hcpcs_3_mdfr_cd': [None], 'hcpcs_4_mdfr_cd': [None], 'hcpcs_5_mdfr_cd': [None], 'clm_rev_apc_hipps_cd': [None], 'source_filename': ['cclf2.csv'], 'file_date': ['2023-07-01']}
        base.update(overrides)
        return pl.DataFrame(base)

    @staticmethod
    def _diag_pivot_df() -> pl.DataFrame:
        data: dict[str, list] = {'cur_clm_uniq_id': ['CLM1'], 'current_bene_mbi_id': ['MBI1'], 'dgns_prcdr_icd_ind': ['0']}
        for i in range(1, 26):
            data[f'diagnosis_code_{i}'] = ['J9601'] if i == 1 else [None]
            data[f'diagnosis_poa_{i}'] = ['Y'] if i == 1 else [None]
        return pl.DataFrame(data)

    @staticmethod
    def _proc_pivot_df() -> pl.DataFrame:
        data: dict[str, list] = {'cur_clm_uniq_id': ['CLM1'], 'current_bene_mbi_id': ['MBI1'], 'dgns_prcdr_icd_ind': ['0']}
        for i in range(1, 26):
            data[f'procedure_code_{i}'] = ['99213'] if i == 1 else [None]
            data[f'procedure_date_{i}'] = ['2023-01-10'] if i == 1 else [None]
        return pl.DataFrame(data)

    @pytest.mark.unit
    def test_basic_output(self, tmp_path: Path) -> None:
        _write(self._adr_df(), tmp_path / 'int_institutional_claim_adr.parquet')
        _write(self._revenue_center_df(), tmp_path / 'int_revenue_center_deduped.parquet')
        _write(self._diag_pivot_df(), tmp_path / 'int_diagnosis_pivot.parquet')
        _write(self._proc_pivot_df(), tmp_path / 'int_procedure_pivot.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'claim_id' in result.columns
        assert 'claim_type' in result.columns
        assert result['claim_type'][0] == 'institutional'
        assert result['payer'][0] == 'medicare'

    @pytest.mark.unit
    def test_canceled_excluded(self, tmp_path: Path) -> None:
        _write(self._adr_df(clm_adjsmt_type_cd=['1']), tmp_path / 'int_institutional_claim_adr.parquet')
        _write(self._revenue_center_df(), tmp_path / 'int_revenue_center_deduped.parquet')
        _write(self._diag_pivot_df(), tmp_path / 'int_diagnosis_pivot.parquet')
        _write(self._proc_pivot_df(), tmp_path / 'int_procedure_pivot.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_bill_type_code_concatenated(self, tmp_path: Path) -> None:
        _write(self._adr_df(), tmp_path / 'int_institutional_claim_adr.parquet')
        _write(self._revenue_center_df(), tmp_path / 'int_revenue_center_deduped.parquet')
        _write(self._diag_pivot_df(), tmp_path / 'int_diagnosis_pivot.parquet')
        _write(self._proc_pivot_df(), tmp_path / 'int_procedure_pivot.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        if result.shape[0] > 0:
            assert result['bill_type_code'][0] == '111'

    @pytest.mark.unit
    def test_diagnosis_pivot_joined(self, tmp_path: Path) -> None:
        _write(self._adr_df(), tmp_path / 'int_institutional_claim_adr.parquet')
        _write(self._revenue_center_df(), tmp_path / 'int_revenue_center_deduped.parquet')
        _write(self._diag_pivot_df(), tmp_path / 'int_diagnosis_pivot.parquet')
        _write(self._proc_pivot_df(), tmp_path / 'int_procedure_pivot.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        if result.shape[0] > 0:
            assert result['diagnosis_code_1'][0] == 'J9601'
            assert result['procedure_code_1'][0] == '99213'
