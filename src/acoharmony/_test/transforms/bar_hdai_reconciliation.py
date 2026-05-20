"""Unit tests for bar_hdai_reconciliation transforms module."""
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

from acoharmony._transforms.bar_hdai_reconciliation import execute

if TYPE_CHECKING:
    pass

@pytest.mark.unit
def test_execute_basic(tmp_path: Path) -> None:
    """execute basic functionality -- reconciles BAR and HDAI reports."""
    base = tmp_path / 'data'
    silver = base / 'silver'
    silver.mkdir(parents=True, exist_ok=True)
    bar = pl.DataFrame({
        'bene_mbi': ['MBI1'], 'bene_first_name': ['John'], 'bene_last_name': ['Doe'],
        'bene_date_of_birth': [date(1950, 1, 1)], 'bene_date_of_death': [None],
        'bene_address_line_1': ['123 Main'], 'bene_city': ['Chicago'],
        'bene_state': ['IL'], 'bene_zip_5': ['60601'],
        'start_date': [date(2024, 1, 1)], 'end_date': [date(2024, 12, 31)],
        'file_date': [date(2024, 6, 1)],
    })
    hdai = pl.DataFrame({
        'mbi': ['MBI1'], 'patient_first_name': ['John'], 'patient_last_name': ['Doe'],
        'patient_dob': [date(1950, 1, 1)], 'patient_dod': [None],
        'patient_address': ['123 Main'], 'patient_city': ['Chicago'],
        'patient_state': ['IL'], 'patient_zip': ['60601'],
        'enrollment_status': ['Active'], 'most_recent_awv_date': [date(2024, 3, 1)],
        'last_em_visit': [date(2024, 2, 1)], 'file_date': [date(2024, 6, 15)],
    })
    _write_parquet(silver / 'bar.parquet', bar)
    _write_parquet(silver / 'hdai_reach.parquet', hdai)
    executor = _MockExecutor(base)
    result = _call_execute(execute, executor)
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert 'mbi' in df.columns
    assert df.shape[0] >= 1

class _MockMedallionStorage:
    """Mock medallion storage for transform tests."""

    def __init__(self, silver_path=None, gold_path=None):
        if silver_path is None:
            silver_path = Path('.')
        self.silver_path = silver_path
        self.gold_path = gold_path or silver_path

    def get_path(self, layer='silver'):
        layer_str = layer.value if hasattr(layer, 'value') else str(layer)
        base = self.gold_path if layer_str == 'gold' else self.silver_path
        return base / layer_str

class _MockExecutor:
    """Mock executor for transform tests."""

    def __init__(self, base=None, storage_config=None):
        if storage_config is not None:
            self.storage_config = storage_config
        elif base is not None:
            self.storage_config = _MockMedallionStorage(silver_path=base)
        else:
            self.storage_config = _MockMedallionStorage()

def _write(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame as parquet, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)

def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()

def _write_parquet(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)

def _call_execute(execute_fn: Any, executor: Any) -> pl.LazyFrame:
    """Call a transform's execute function, handling decorator chains.

    Recursively searches through decorator wrappers (.func, __wrapped__,
    __closure__) to find and call the original execute function.
    """
    try:
        result = execute_fn(executor)
        if isinstance(result, pl.LazyFrame):
            return result
    except TypeError:
        pass
    visited: set[int] = set()

    def _search(obj: Any, depth: int=0) -> pl.LazyFrame | None:
        """Recursively search through decorator chain for the original function."""
        if obj is None or id(obj) in visited or depth > 10:
            return None
        visited.add(id(obj))
        if callable(obj) and hasattr(obj, '__code__'):
            co_name = obj.__code__.co_name
            if co_name == 'execute' or co_name not in ('wrapper', 'decorator'):
                try:
                    result = obj(executor)
                    if isinstance(result, pl.LazyFrame):
                        return result
                except TypeError:
                    pass
                except Exception:
                    if co_name == 'execute':
                        raise
        for attr in ('func', '__wrapped__'):
            found = _search(getattr(obj, attr, None), depth + 1)
            if found is not None:
                return found
        closure = getattr(obj, '__closure__', None)
        if closure:
            for cell in closure:
                try:
                    val = cell.cell_contents
                except ValueError:
                    continue
                if callable(val):
                    found = _search(val, depth + 1)
                    if found is not None:
                        return found
        return None
    result = _search(execute_fn)
    if result is not None:
        return result
    raise RuntimeError(f'Could not obtain a LazyFrame from {execute_fn!r} — check the decorator chain on the source transform.')

@pytest.fixture
def tmp_base(tmp_path: Path) -> Path:
    return tmp_path / 'data'

@pytest.fixture
def executor(tmp_base: Path) -> _MockExecutor:
    return _MockExecutor(tmp_base)

def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)

class TestBarHdaiReconciliationExtended:
    """Tests for bar_hdai_reconciliation executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import bar_hdai_reconciliation
        assert bar_hdai_reconciliation is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestBarHdaiReconciliation:
    """Tests for bar_hdai_reconciliation.execute."""

    @staticmethod
    def _bar_df() -> pl.DataFrame:
        return pl.DataFrame({'bene_mbi': ['MBI1', 'MBI2', 'MBI3'], 'bene_first_name': ['John', 'Jane', 'Bob'], 'bene_last_name': ['Doe', 'Smith', 'Jones'], 'bene_date_of_birth': [date(1950, 1, 1), date(1955, 6, 15), date(1960, 3, 20)], 'bene_date_of_death': [None, date(2024, 6, 1), None], 'bene_address_line_1': ['123 Main', '456 Oak', '789 Pine'], 'bene_city': ['Chicago', 'Springfield', 'Peoria'], 'bene_state': ['IL', 'IL', 'IL'], 'bene_zip_5': ['60601', '62701', '61602'], 'start_date': [date(2024, 1, 1)] * 3, 'end_date': [date(2024, 12, 31)] * 3, 'file_date': [date(2024, 6, 1)] * 3})

    @staticmethod
    def _hdai_df() -> pl.DataFrame:
        return pl.DataFrame({'mbi': ['MBI1', 'MBI2', 'MBI4'], 'patient_first_name': ['John', 'Jane', 'Alice'], 'patient_last_name': ['Doe', 'Smith', 'Wonder'], 'patient_dob': [date(1950, 1, 1), date(1955, 6, 15), date(1965, 9, 10)], 'patient_dod': [None, date(2024, 6, 1), None], 'patient_address': ['123 Main', '456 Oak', '321 Elm'], 'patient_city': ['Chicago', 'Springfield', 'Decatur'], 'patient_state': ['IL', 'IL', 'IL'], 'patient_zip': ['60601', '62701', '62521'], 'enrollment_status': ['Active', 'Deceased', 'Active'], 'most_recent_awv_date': [date(2024, 3, 1), None, date(2024, 5, 1)], 'last_em_visit': [date(2024, 2, 1), None, date(2024, 4, 1)], 'file_date': [date(2024, 6, 15)] * 3})

    @pytest.mark.unit
    def test_reconciliation_statuses(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'bar.parquet', self._bar_df())
        _write_parquet(silver / 'hdai_reach.parquet', self._hdai_df())
        result = _call_execute(execute, executor).collect()
        statuses = dict(zip(result['mbi'].to_list(), result['reconciliation_status'].to_list(), strict=False))
        assert statuses['MBI1'] == 'In Both'
        assert statuses['MBI2'] == 'In Both'
        assert statuses['MBI3'] == 'BAR Only'
        assert statuses['MBI4'] == 'HDAI Only'

    @pytest.mark.unit
    def test_is_alive_flag(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'bar.parquet', self._bar_df())
        _write_parquet(silver / 'hdai_reach.parquet', self._hdai_df())
        result = _call_execute(execute, executor).collect()
        alive_map = dict(zip(result['mbi'].to_list(), result['is_alive'].to_list(), strict=False))
        assert alive_map['MBI1'] is True
        assert alive_map['MBI2'] is False
        assert alive_map['MBI4'] is True

    @pytest.mark.unit
    def test_output_columns(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'bar.parquet', self._bar_df())
        _write_parquet(silver / 'hdai_reach.parquet', self._hdai_df())
        result = _call_execute(execute, executor).collect()
        expected_cols = {'mbi', 'first_name', 'last_name', 'date_of_birth', 'death_date', 'is_alive', 'reconciliation_status', 'in_bar', 'in_hdai', 'bar_start_date', 'bar_end_date', 'hdai_enrollment_status', 'hdai_awv_date', 'hdai_last_em', 'address_line_1', 'city', 'state', 'zip_code', 'bar_city', 'hdai_city', 'bar_state', 'hdai_state', 'bar_zip', 'hdai_zip', 'bar_report_date', 'hdai_report_date'}
        assert expected_cols.issubset(set(result.columns))

    @pytest.mark.unit
    def test_death_date_coalesced(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'bar.parquet', self._bar_df())
        _write_parquet(silver / 'hdai_reach.parquet', self._hdai_df())
        result = _call_execute(execute, executor).collect()
        death_map = dict(zip(result['mbi'].to_list(), result['death_date'].to_list(), strict=False))
        assert death_map['MBI2'] == date(2024, 6, 1)
        assert death_map['MBI1'] is None

    @pytest.mark.unit
    def test_uses_most_recent_file_dates(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        bar_with_old = pl.concat([self._bar_df(), pl.DataFrame({'bene_mbi': ['MBI_OLD'], 'bene_first_name': ['Old'], 'bene_last_name': ['Record'], 'bene_date_of_birth': [date(1940, 1, 1)], 'bene_date_of_death': [None], 'bene_address_line_1': ['Old Address'], 'bene_city': ['Old City'], 'bene_state': ['XX'], 'bene_zip_5': ['00000'], 'start_date': [date(2023, 1, 1)], 'end_date': [date(2023, 12, 31)], 'file_date': [date(2023, 1, 1)]})])
        _write_parquet(silver / 'bar.parquet', bar_with_old)
        _write_parquet(silver / 'hdai_reach.parquet', self._hdai_df())
        result = _call_execute(execute, executor).collect()
        mbis = result['mbi'].to_list()
        assert 'MBI_OLD' not in mbis
