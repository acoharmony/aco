"""Unit tests for int_beneficiary_xref_deduped transforms module."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
import acoharmony

from acoharmony._transforms.int_beneficiary_xref_deduped import execute

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
    """execute basic functionality -- returns LazyFrame with expected columns."""
    cclf9 = pl.DataFrame({
        'prvs_num': ['MBI_A'], 'crnt_num': ['MBI_B'],
        'prvs_id_efctv_dt': ['2023-01-01'], 'prvs_id_obslt_dt': ['2023-06-01'],
        'source_filename': ['f1.csv'], 'file_date': ['2023-07-01'],
    })
    _write(cclf9, tmp_path / 'cclf9.parquet')
    result = execute(_make_executor(tmp_path))
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert 'prvs_num' in df.columns
    assert 'crnt_num' in df.columns
    assert df.shape[0] >= 1

def _make_executor(silver_dir: Path) -> _MockExecutor:
    return _MockExecutor(storage_config=_MockMedallionStorage(silver_path=silver_dir))

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

def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)

class TestIntBeneficiaryXrefDedupedExtended:
    """Tests for int_beneficiary_xref_deduped executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_beneficiary_xref_deduped
        assert int_beneficiary_xref_deduped is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntBeneficiaryXrefDeduped:
    """Tests for int_beneficiary_xref_deduped.execute."""

    @pytest.mark.unit
    def test_basic_dedup_keeps_latest(self, tmp_path: Path) -> None:
        cclf9 = pl.DataFrame({'prvs_num': ['MBI_A', 'MBI_A'], 'crnt_num': ['MBI_B', 'MBI_C'], 'prvs_id_efctv_dt': ['2023-01-01', '2023-01-01'], 'prvs_id_obslt_dt': ['2023-06-01', '2023-06-01'], 'source_filename': ['f1.csv', 'f2.csv'], 'file_date': ['2023-01-01', '2023-06-01']})
        _write(cclf9, tmp_path / 'cclf9.parquet')
        executor = _make_executor(tmp_path)
        result = execute(executor).collect()
        assert result.shape[0] == 1
        assert result['crnt_num'][0] == 'MBI_C'

    @pytest.mark.unit
    def test_chained_mbi_resolution(self, tmp_path: Path) -> None:
        """If crnt_num itself has a newer mapping, resolve the chain."""
        cclf9 = pl.DataFrame({'prvs_num': ['MBI_A', 'MBI_B'], 'crnt_num': ['MBI_B', 'MBI_C'], 'prvs_id_efctv_dt': ['2023-01-01', '2023-01-01'], 'prvs_id_obslt_dt': ['2023-06-01', '2023-06-01'], 'source_filename': ['f1.csv', 'f2.csv'], 'file_date': ['2023-01-01', '2023-06-01']})
        _write(cclf9, tmp_path / 'cclf9.parquet')
        executor = _make_executor(tmp_path)
        result = execute(executor).collect()
        row_a = result.filter(pl.col('prvs_num') == 'MBI_A')
        assert row_a['crnt_num'][0] == 'MBI_C'

    @pytest.mark.unit
    def test_output_columns(self, tmp_path: Path) -> None:
        cclf9 = pl.DataFrame({'prvs_num': ['MBI_X'], 'crnt_num': ['MBI_Y'], 'prvs_id_efctv_dt': ['2023-01-01'], 'prvs_id_obslt_dt': ['2023-06-01'], 'source_filename': ['f.csv'], 'file_date': ['2023-07-01']})
        _write(cclf9, tmp_path / 'cclf9.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        expected_cols = {'prvs_num', 'crnt_num', 'prvs_id_efctv_dt', 'prvs_id_obslt_dt', 'source_filename', 'file_date'}
        assert set(result.columns) == expected_cols
