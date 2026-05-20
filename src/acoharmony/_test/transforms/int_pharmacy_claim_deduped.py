"""Unit tests for int_pharmacy_claim_deduped transforms module."""
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

from acoharmony._transforms.int_pharmacy_claim_deduped import execute

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
        'cur_clm_uniq_id': ['CLM1'], 'current_bene_mbi_id': ['MBI1'], 'bene_hic_num': [None],
        'clm_line_ndc_cd': ['12345678901'], 'clm_line_from_dt': ['2023-03-15'],
        'prvdr_srvc_id_qlfyr_cd': ['01'], 'clm_srvc_prvdr_gnrc_id_num': ['1234567890'],
        'clm_dspnsng_stus_cd': ['P'], 'clm_line_srvc_unit_qty': ['30'],
        'clm_line_days_suply_qty': ['30'], 'prvdr_prsbng_id_qlfyr_cd': ['01'],
        'clm_prsbng_prvdr_gnrc_id_num': ['9876543210'], 'clm_line_bene_pmt_amt': ['10.50'],
        'clm_adjsmt_type_cd': ['0'], 'clm_line_rx_srvc_rfrnc_num': ['REF001'],
        'clm_line_rx_fill_num': ['1'], 'source_filename': ['cclf7.csv'],
        'file_date': ['2023-07-01'], 'row_num': [1],
    })
    _write(adr, tmp_path / 'int_pharmacy_claim_adr.parquet')
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

class TestIntPharmacyClaimDedupedExtended:
    """Tests for int_pharmacy_claim_deduped executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_pharmacy_claim_deduped
        assert int_pharmacy_claim_deduped is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntPharmacyClaimDeduped:

    @staticmethod
    def _adr_df(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'current_bene_mbi_id': ['MBI1'], 'bene_hic_num': [None], 'clm_line_ndc_cd': ['12345678901'], 'clm_line_from_dt': ['2023-03-15'], 'prvdr_srvc_id_qlfyr_cd': ['01'], 'clm_srvc_prvdr_gnrc_id_num': ['1234567890'], 'clm_dspnsng_stus_cd': ['P'], 'clm_line_srvc_unit_qty': ['30'], 'clm_line_days_suply_qty': ['30'], 'prvdr_prsbng_id_qlfyr_cd': ['01'], 'clm_prsbng_prvdr_gnrc_id_num': ['9876543210'], 'clm_line_bene_pmt_amt': ['10.50'], 'clm_adjsmt_type_cd': ['0'], 'clm_line_rx_srvc_rfrnc_num': ['REF001'], 'clm_line_rx_fill_num': ['1'], 'source_filename': ['cclf7.csv'], 'file_date': ['2023-07-01'], 'row_num': [1]}
        base.update(overrides)
        return pl.DataFrame(base)

    @pytest.mark.unit
    def test_basic_output(self, tmp_path: Path) -> None:
        _write(self._adr_df(), tmp_path / 'int_pharmacy_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'claim_id' in result.columns
        assert 'person_id' in result.columns
        assert 'dispensing_provider_npi' in result.columns
        assert result['payer'][0] == 'medicare'

    @pytest.mark.unit
    def test_canceled_claims_excluded(self, tmp_path: Path) -> None:
        df = pl.concat([self._adr_df(clm_adjsmt_type_cd=['0']), self._adr_df(cur_clm_uniq_id=['CLM_CANCEL'], clm_adjsmt_type_cd=['1'])])
        _write(df, tmp_path / 'int_pharmacy_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 1
        assert result['claim_id'][0] == 'CLM1'

    @pytest.mark.unit
    def test_duplicates_excluded(self, tmp_path: Path) -> None:
        """Claims with count > 1 after filtering are excluded."""
        df = pl.concat([self._adr_df(cur_clm_uniq_id=['DUP'], clm_adjsmt_type_cd=['0'], row_num=[1]), self._adr_df(cur_clm_uniq_id=['DUP'], clm_adjsmt_type_cd=['2'], row_num=[1])])
        _write(df, tmp_path / 'int_pharmacy_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_npi_qualifier_mapping(self, tmp_path: Path) -> None:
        _write(self._adr_df(prvdr_prsbng_id_qlfyr_cd=['99'], prvdr_srvc_id_qlfyr_cd=['99']), tmp_path / 'int_pharmacy_claim_adr.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['prescribing_provider_npi'][0] is None
        assert result['dispensing_provider_npi'][0] is None
