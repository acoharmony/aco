"""Unit tests for int_enrollment transforms module."""
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

from acoharmony._transforms.int_enrollment import execute

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
    """execute basic functionality -- returns LazyFrame with enrollment columns."""
    enrollment = pl.DataFrame({
        'current_bene_mbi_id': ['MBI1'],
        'enrollment_start_date': ['2023-01-01'],
        'enrollment_end_date': ['2023-12-31'],
    })
    _write(enrollment, tmp_path / 'enrollment.parquet')
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

class TestIntEnrollment:
    """Tests for int_enrollment executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_enrollment
        assert int_enrollment is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntEnrollmentExtended:

    @pytest.mark.unit
    def test_basic_enrollment(self, tmp_path: Path) -> None:
        enrollment = pl.DataFrame({'current_bene_mbi_id': ['MBI1'], 'enrollment_start_date': ['2023-01-01'], 'enrollment_end_date': ['2023-12-31']})
        _write(enrollment, tmp_path / 'enrollment.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert set(result.columns) == {'current_bene_mbi_id', 'enrollment_start_date', 'enrollment_end_date'}
        assert result.shape[0] == 1

    @pytest.mark.unit
    def test_mbi_crosswalk_updates_enrollment(self, tmp_path: Path) -> None:
        enrollment = pl.DataFrame({'current_bene_mbi_id': ['OLD_MBI'], 'enrollment_start_date': ['2023-01-01'], 'enrollment_end_date': ['2023-12-31']})
        _write(enrollment, tmp_path / 'enrollment.parquet')
        _write(_xref_df(), tmp_path / 'int_beneficiary_xref_deduped.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['current_bene_mbi_id'][0] == 'NEW_MBI'
