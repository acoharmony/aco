"""Unit tests for gift_card_distro transforms module."""
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

from acoharmony._transforms.gift_card_distro import execute

if TYPE_CHECKING:
    pass

@pytest.mark.unit
def test_execute_basic(tmp_path: Path) -> None:
    """execute basic functionality -- processes GCM, BAR, and HDAI data."""
    base = tmp_path / 'data'
    silver = base / 'silver'
    silver.mkdir(parents=True, exist_ok=True)
    gcm = pl.DataFrame({
        'hcmpi': ['H1'], 'mbi': ['MBI1'], 'awv_date': [date(2024, 3, 1)],
        'awv_status': ['Complete'], 'gift_card_status': ['Sent'],
        'lc_status_current': ['Active'], 'roll12_awv_enc': [1], 'roll12_em': [2],
        'payer': ['Medicare'], 'payer_current': ['MCR'], 'total_count': [1],
        'patientaddress': ['123 Main St'], 'patientcity': ['Chicago'],
        'patientstate': ['IL'], 'patientzip': ['60601'],
    })
    bar = pl.DataFrame({
        'bene_mbi': ['MBI1'], 'bene_first_name': ['John'], 'bene_last_name': ['Doe'],
        'bene_date_of_death': [None], 'bene_address_line_1': ['123 Main St'],
        'bene_city': ['Chicago'], 'bene_state': ['IL'], 'bene_zip_5': ['60601'],
        'start_date': [date(2024, 1, 1)], 'end_date': [date(2024, 12, 31)],
        'file_date': [date(2024, 6, 1)],
    })
    hdai = pl.DataFrame({
        'mbi': ['MBI1'], 'patient_first_name': ['John'], 'patient_last_name': ['Doe'],
        'patient_dod': [None], 'enrollment_status': ['Active'],
        'most_recent_awv_date': [date(2024, 3, 1)], 'last_em_visit': [date(2024, 2, 1)],
        'patient_address': ['123 Main St'], 'patient_city': ['Chicago'],
        'patient_state': ['IL'], 'patient_zip': ['60601'],
        'file_date': [date(2024, 6, 15)],
    })
    _write_parquet(silver / 'gcm.parquet', gcm)
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

class TestGiftCardDistroExtended:
    """Tests for gift_card_distro executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import gift_card_distro
        assert gift_card_distro is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestGiftCardDistro:
    """Tests for gift_card_distro.execute."""

    @staticmethod
    def _gcm_df() -> pl.DataFrame:
        return pl.DataFrame({'hcmpi': ['H1', 'H2'], 'mbi': ['MBI1', 'MBI2'], 'awv_date': [date(2024, 3, 1), None], 'awv_status': ['Complete', 'Pending'], 'gift_card_status': ['Sent', 'Pending'], 'lc_status_current': ['Active', 'Inactive'], 'roll12_awv_enc': [1, 0], 'roll12_em': [2, 1], 'payer': ['Medicare', 'Medicare'], 'payer_current': ['MCR', 'MCR'], 'total_count': [1, 2], 'patientaddress': ['123 Main St', None], 'patientcity': ['Chicago', None], 'patientstate': ['IL', None], 'patientzip': ['60601', None]})

    @staticmethod
    def _bar_df() -> pl.DataFrame:
        return pl.DataFrame({'bene_mbi': ['MBI1'], 'bene_first_name': ['John'], 'bene_last_name': ['Doe'], 'bene_date_of_death': [None], 'bene_address_line_1': ['123 Main St'], 'bene_city': ['Chicago'], 'bene_state': ['IL'], 'bene_zip_5': ['60601'], 'start_date': [date(2024, 1, 1)], 'end_date': [date(2024, 12, 31)], 'file_date': [date(2024, 6, 1)]})

    @staticmethod
    def _hdai_df() -> pl.DataFrame:
        return pl.DataFrame({'mbi': ['MBI1'], 'patient_first_name': ['John'], 'patient_last_name': ['Doe'], 'patient_dod': [None], 'enrollment_status': ['Active'], 'most_recent_awv_date': [date(2024, 3, 1)], 'last_em_visit': [date(2024, 2, 1)], 'patient_address': ['123 Main St'], 'patient_city': ['Chicago'], 'patient_state': ['IL'], 'patient_zip': ['60601'], 'file_date': [date(2024, 6, 15)]})

    @pytest.mark.unit
    def test_basic_execution(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'gcm.parquet', self._gcm_df())
        _write_parquet(silver / 'bar.parquet', self._bar_df())
        _write_parquet(silver / 'hdai_reach.parquet', self._hdai_df())
        result = _call_execute(execute, executor).collect()
        assert len(result) == 2

    @pytest.mark.unit
    def test_data_source_status(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'gcm.parquet', self._gcm_df())
        _write_parquet(silver / 'bar.parquet', self._bar_df())
        _write_parquet(silver / 'hdai_reach.parquet', self._hdai_df())
        result = _call_execute(execute, executor).collect()
        status_map = dict(zip(result['mbi'].to_list(), result['data_source_status'].to_list(), strict=False))
        assert status_map['MBI1'] == 'GCM+BAR+HDAI'
        assert status_map['MBI2'] == 'GCM Only'

    @pytest.mark.unit
    def test_output_columns(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'gcm.parquet', self._gcm_df())
        _write_parquet(silver / 'bar.parquet', self._bar_df())
        _write_parquet(silver / 'hdai_reach.parquet', self._hdai_df())
        result = _call_execute(execute, executor).collect()
        expected_cols = {'hcmpi', 'mbi', 'first_name', 'last_name', 'is_alive', 'in_gcm', 'in_bar', 'in_hdai', 'data_source_status', 'gift_card_status', 'gcm_lifecycle_status', 'bar_report_date', 'hdai_report_date', 'payer', 'payer_current', 'total_count'}
        assert expected_cols.issubset(set(result.columns))

    @pytest.mark.unit
    def test_is_alive_with_death_date(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'gcm.parquet', self._gcm_df())
        bar_with_death = self._bar_df().with_columns(pl.lit(date(2024, 5, 1)).alias('bene_date_of_death'))
        _write_parquet(silver / 'bar.parquet', bar_with_death)
        _write_parquet(silver / 'hdai_reach.parquet', self._hdai_df())
        result = _call_execute(execute, executor).collect()
        alive_map = dict(zip(result['mbi'].to_list(), result['is_alive'].to_list(), strict=False))
        assert alive_map['MBI1'] is False
