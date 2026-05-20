"""Unit tests for gcm_address_enriched transforms module."""
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

from acoharmony._transforms.gcm_address_enriched import execute

if TYPE_CHECKING:
    pass

@pytest.mark.unit
def test_execute_basic(tmp_path: Path) -> None:
    """execute basic functionality -- enriches GCM addresses from CCLF8."""
    base = tmp_path / 'data'
    silver = base / 'silver'
    silver.mkdir(parents=True, exist_ok=True)
    gcm = pl.DataFrame({
        'total_count': [1], 'hcmpi': ['H1'], 'payer_current': ['MCR'],
        'payer': ['Medicare'], 'roll12_awv_enc': [1], 'awv_status': ['Complete'],
        'roll12_em': [2], 'lc_status_current': ['Active'],
        'awv_date': [date(2024, 3, 1)], 'mbi': ['MBI1'],
        'patientaddress': ['123 Main St'], 'patientaddress2': pl.Series([None], dtype=pl.Utf8),
        'patientcity': ['Chicago'], 'patientstate': ['IL'], 'patientzip': ['60601'],
        'gift_card_status': ['Sent'],
        'processed_at': [datetime(2024, 1, 1)], 'source_file': ['gcm.xlsx'],
        'source_filename': ['gcm.xlsx'], 'file_date': [date(2024, 1, 1)],
        'medallion_layer': ['silver'],
    })
    cclf8 = pl.DataFrame({
        'bene_mbi_id': ['MBI1'], 'bene_line_1_adr': ['123 Main St'],
        'bene_line_2_adr': ['Apt 1'], 'bene_city': ['Chicago'],
        'bene_state': ['IL'], 'bene_zip': ['60601'],
        'file_date': [date(2024, 1, 1)],
    })
    _write_parquet(silver / 'gcm.parquet', gcm)
    _write_parquet(silver / 'cclf8.parquet', cclf8)
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

class TestGcmAddressEnrichedExtended:
    """Tests for gcm_address_enriched executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import gcm_address_enriched
        assert gcm_address_enriched is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestGcmAddressEnriched:
    """Tests for gcm_address_enriched.execute."""

    @staticmethod
    def _gcm_df() -> pl.DataFrame:
        return pl.DataFrame({'total_count': [1, 2], 'hcmpi': ['H1', 'H2'], 'payer_current': ['MCR', 'MCR'], 'payer': ['Medicare', 'Medicare'], 'roll12_awv_enc': [1, 0], 'awv_status': ['Complete', 'Pending'], 'roll12_em': [2, 1], 'lc_status_current': ['Active', 'Inactive'], 'awv_date': [date(2024, 3, 1), None], 'mbi': ['MBI1', 'MBI2'], 'patientaddress': ['123 Main St', None], 'patientaddress2': pl.Series([None, None], dtype=pl.Utf8), 'patientcity': ['Chicago', 'null'], 'patientstate': ['IL', ''], 'patientzip': ['60601', None], 'gift_card_status': ['Sent', 'Pending'], 'processed_at': [datetime(2024, 1, 1), datetime(2024, 1, 1)], 'source_file': ['gcm.xlsx', 'gcm.xlsx'], 'source_filename': ['gcm.xlsx', 'gcm.xlsx'], 'file_date': [date(2024, 1, 1), date(2024, 1, 1)], 'medallion_layer': ['silver', 'silver']})

    @staticmethod
    def _cclf8_df() -> pl.DataFrame:
        return pl.DataFrame({'bene_mbi_id': ['MBI1', 'MBI2', 'MBI2'], 'bene_line_1_adr': ['123 Main St', '456 Oak Ave', '456 Oak Ave'], 'bene_line_2_adr': ['Apt 1', 'Suite 200', 'Suite 200'], 'bene_city': ['Chicago', 'Springfield', 'Springfield'], 'bene_state': ['IL', 'IL', 'IL'], 'bene_zip': ['60601', '62701', '62701'], 'file_date': [date(2024, 1, 1), date(2024, 1, 1), date(2023, 6, 1)]})

    @pytest.mark.unit
    def test_enriches_missing_addresses(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'gcm.parquet', self._gcm_df())
        _write_parquet(silver / 'cclf8.parquet', self._cclf8_df())
        result = _call_execute(execute, executor).collect()
        assert len(result) == 2
        mbi2_row = result.filter(pl.col('mbi') == 'MBI2')
        assert mbi2_row['patientaddress'][0] == '456 Oak Ave'
        assert mbi2_row['patientcity'][0] == 'Springfield'
        assert mbi2_row['patientstate'][0] == 'IL'
        mbi1_row = result.filter(pl.col('mbi') == 'MBI1')
        assert mbi1_row['patientaddress'][0] == '123 Main St'

    @pytest.mark.unit
    def test_address_was_missing_flag(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'gcm.parquet', self._gcm_df())
        _write_parquet(silver / 'cclf8.parquet', self._cclf8_df())
        result = _call_execute(execute, executor).collect()
        mbi1_row = result.filter(pl.col('mbi') == 'MBI1')
        mbi2_row = result.filter(pl.col('mbi') == 'MBI2')
        assert mbi1_row['address_was_missing'][0] is False
        assert mbi2_row['address_was_missing'][0] is True

    @pytest.mark.unit
    def test_output_columns(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'gcm.parquet', self._gcm_df())
        _write_parquet(silver / 'cclf8.parquet', self._cclf8_df())
        result = _call_execute(execute, executor).collect()
        expected_cols = {'total_count', 'hcmpi', 'mbi', 'patientaddress', 'patientaddress_original', 'patientcity', 'patientcity_original', 'patientstate', 'patientstate_original', 'patientzip', 'patientzip_original', 'address_was_missing', 'gift_card_status'}
        assert expected_cols.issubset(set(result.columns))

    @pytest.mark.unit
    def test_preserves_original_addresses(self, executor: _MockExecutor, tmp_base: Path) -> None:
        silver = tmp_base / 'silver'
        silver.mkdir(parents=True, exist_ok=True)
        _write_parquet(silver / 'gcm.parquet', self._gcm_df())
        _write_parquet(silver / 'cclf8.parquet', self._cclf8_df())
        result = _call_execute(execute, executor).collect()
        mbi2_row = result.filter(pl.col('mbi') == 'MBI2')
        assert mbi2_row['patientaddress_original'][0] is None
