"""Unit tests for int_revenue_center_deduped transforms module."""
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

from acoharmony._transforms.int_revenue_center_deduped import execute

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
    """execute basic functionality -- returns LazyFrame with revenue center columns."""
    cclf2 = pl.DataFrame({
        'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'bene_mbi_id': ['MBI1'],
        'clm_type_cd': ['10'], 'clm_line_from_dt': ['2023-01-01'],
        'clm_line_thru_dt': ['2023-01-15'], 'clm_line_prod_rev_ctr_cd': ['0450'],
        'clm_line_instnl_rev_ctr_dt': ['2023-01-01'], 'clm_line_hcpcs_cd': ['99213'],
        'prvdr_oscar_num': ['100001'], 'clm_from_dt': ['2023-01-01'],
        'clm_thru_dt': ['2023-01-15'], 'clm_line_srvc_unit_qty': ['1'],
        'clm_line_cvrd_pd_amt': ['100.00'], 'hcpcs_1_mdfr_cd': [None],
        'hcpcs_2_mdfr_cd': [None], 'hcpcs_3_mdfr_cd': [None], 'hcpcs_4_mdfr_cd': [None],
        'hcpcs_5_mdfr_cd': [None], 'clm_rev_apc_hipps_cd': [None],
        'source_filename': ['cclf2.csv'], 'file_date': ['2023-07-01'],
    })
    _write(cclf2, tmp_path / 'cclf2.parquet')
    _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
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

class TestIntRevenueCenterDeduped:
    """Tests for int_revenue_center_deduped executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_revenue_center_deduped
        assert int_revenue_center_deduped is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntRevenueCenterDedupedExtended:

    @staticmethod
    def _cclf2(**overrides: Any) -> pl.DataFrame:
        base = {'cur_clm_uniq_id': ['CLM1'], 'clm_line_num': ['1'], 'bene_mbi_id': ['MBI1'], 'clm_type_cd': ['10'], 'clm_line_from_dt': ['2023-01-01'], 'clm_line_thru_dt': ['2023-01-15'], 'clm_line_prod_rev_ctr_cd': ['0450'], 'clm_line_instnl_rev_ctr_dt': ['2023-01-01'], 'clm_line_hcpcs_cd': ['99213'], 'prvdr_oscar_num': ['100001'], 'clm_from_dt': ['2023-01-01'], 'clm_thru_dt': ['2023-01-15'], 'clm_line_srvc_unit_qty': ['1'], 'clm_line_cvrd_pd_amt': ['100.00'], 'hcpcs_1_mdfr_cd': [None], 'hcpcs_2_mdfr_cd': [None], 'hcpcs_3_mdfr_cd': [None], 'hcpcs_4_mdfr_cd': [None], 'hcpcs_5_mdfr_cd': [None], 'clm_rev_apc_hipps_cd': [None], 'source_filename': ['cclf2.csv'], 'file_date': ['2023-07-01']}
        base.update(overrides)
        return pl.DataFrame(base)

    @pytest.mark.unit
    def test_output_columns(self, tmp_path: Path) -> None:
        _write(self._cclf2(), tmp_path / 'cclf2.parquet')
        _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'current_bene_mbi_id' in result.columns
        assert 'cur_clm_uniq_id' in result.columns
        assert 'clm_line_num' in result.columns
        assert 'row_num' not in result.columns

    @pytest.mark.unit
    def test_dedup_keeps_latest(self, tmp_path: Path) -> None:
        df = pl.concat([self._cclf2(file_date=['2023-01-01'], source_filename=['old.csv']), self._cclf2(file_date=['2023-07-01'], source_filename=['new.csv'])])
        _write(df, tmp_path / 'cclf2.parquet')
        _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 1
