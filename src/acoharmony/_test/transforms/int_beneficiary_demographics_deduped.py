"""Unit tests for int_beneficiary_demographics_deduped transforms module."""
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

from acoharmony._transforms.int_beneficiary_demographics_deduped import execute

if TYPE_CHECKING:
    pass

@pytest.mark.unit
def test_execute_basic(tmp_path: Path) -> None:
    """execute basic functionality -- returns a LazyFrame with expected columns."""
    cclf8 = pl.DataFrame({
        'bene_mbi_id': ['MBI1'], 'bene_fips_state_cd': ['OH'], 'bene_fips_cnty_cd': ['001'],
        'bene_zip_cd': ['45373'], 'bene_dob': ['19500501'], 'bene_sex_cd': ['F'],
        'bene_race_cd': ['1'], 'bene_mdcr_stus_cd': ['10'], 'bene_dual_stus_cd': ['00'],
        'bene_death_dt': [None], 'bene_rng_bgn_dt': ['20250101'], 'bene_rng_end_dt': ['20251231'],
        'bene_fst_name': ['ALICE'], 'bene_mdl_name': ['M'], 'bene_lst_name': ['SMITH'],
        'bene_orgnl_entlmt_rsn_cd': ['0'], 'bene_entlmt_buyin_ind': ['3'],
        'bene_part_a_enrlmt_bgn_dt': ['20200101'], 'bene_part_b_enrlmt_bgn_dt': ['20200101'],
        'bene_line_1_adr': ['123 MAIN ST'], 'bene_line_2_adr': [None], 'bene_line_3_adr': [None],
        'bene_line_4_adr': [None], 'bene_line_5_adr': [None], 'bene_line_6_adr': [None],
        'bene_city': ['TROY'], 'bene_state': ['OH'], 'bene_zip': ['45373'],
        'bene_zip_ext': ['1234'], 'source_filename': ['cclf8.csv'], 'file_date': ['2025-01'],
    })
    _write(cclf8, tmp_path / 'cclf8.parquet')
    _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
    result = execute(_make_executor(tmp_path))
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert 'current_bene_mbi_id' in df.columns
    assert df.shape[0] >= 1

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

def _write(df: pl.DataFrame, path: Path) -> None:
    """Write a DataFrame as parquet, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)

def _get_inner_fn(decorated):
    """Walk decorator chain to find original function named 'execute'."""
    visited = set()

    def _search(obj):
        if obj is None or id(obj) in visited:
            return None
        visited.add(id(obj))
        if callable(obj) and hasattr(obj, '__code__') and (obj.__code__.co_name == 'execute'):
            return obj
        for attr in ('func', '__wrapped__'):
            found = _search(getattr(obj, attr, None))
            if found:
                return found
        if hasattr(obj, '__closure__') and obj.__closure__:
            for cell in obj.__closure__:
                try:
                    found = _search(cell.cell_contents)
                    if found:
                        return found
                except ValueError:
                    pass
        return None
    return _search(decorated)

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

class TestIntBeneficiaryDemographicsDedupedExtended:
    """Tests for int_beneficiary_demographics_deduped executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import int_beneficiary_demographics_deduped
        assert int_beneficiary_demographics_deduped is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestIntBeneficiaryDemographicsDedupedExtended2:
    """Tests for int_beneficiary_demographics_deduped transform."""

    @staticmethod
    def _base_cclf8_columns():
        return {'bene_mbi_id': ['MBI001', 'MBI001', 'MBI002'], 'bene_fips_state_cd': ['OH', 'OH', 'OH'], 'bene_fips_cnty_cd': ['001', '001', '002'], 'bene_zip_cd': ['45373', '45373', '45402'], 'bene_dob': ['19500501', '19500501', '19450820'], 'bene_sex_cd': ['F', 'F', 'M'], 'bene_race_cd': ['1', '1', '2'], 'bene_mdcr_stus_cd': ['10', '10', '10'], 'bene_dual_stus_cd': ['00', '00', '01'], 'bene_death_dt': [None, None, None], 'bene_rng_bgn_dt': ['20250101', '20250101', '20250101'], 'bene_rng_end_dt': ['20251231', '20251231', '20251231'], 'bene_fst_name': ['ALICE', 'ALICE', 'BOB'], 'bene_mdl_name': ['M', 'M', 'J'], 'bene_lst_name': ['SMITH', 'SMITH', 'JONES'], 'bene_orgnl_entlmt_rsn_cd': ['0', '0', '0'], 'bene_entlmt_buyin_ind': ['3', '3', '3'], 'bene_part_a_enrlmt_bgn_dt': ['20200101', '20200101', '20180101'], 'bene_part_b_enrlmt_bgn_dt': ['20200101', '20200101', '20180101'], 'bene_line_1_adr': ['123 MAIN ST', '123 MAIN ST', '456 OAK AVE'], 'bene_line_2_adr': [None, None, None], 'bene_line_3_adr': [None, None, None], 'bene_line_4_adr': [None, None, None], 'bene_line_5_adr': [None, None, None], 'bene_line_6_adr': [None, None, None], 'bene_city': ['TROY', 'TROY', 'DAYTON'], 'bene_state': ['OH', 'OH', 'OH'], 'bene_zip': ['45373', '45373', '45402'], 'bene_zip_ext': ['1234', '1234', '5678'], 'source_filename': ['cclf8_jan.csv', 'cclf8_feb.csv', 'cclf8_jan.csv'], 'file_date': ['2025-01', '2025-02', '2025-01']}

    @pytest.mark.unit
    def test_execute_xref_fails_fallback(self, tmp_path):
        """Cover lines 91-93: xref scan raises -> fallback to original MBI.

        The try/except at lines 82-93 catches xref failures. We monkeypatch
        to force the xref scan_parquet to raise.
        """
        from acoharmony._transforms import int_beneficiary_demographics_deduped as mod
        silver = tmp_path
        _write(pl.DataFrame(self._base_cclf8_columns()), silver / 'cclf8.parquet')
        executor = _MockExecutor(silver)
        inner = _get_inner_fn(mod.execute)
        assert inner is not None, 'Could not find inner execute function'
        _real_scan = pl.scan_parquet
        _call_count = [0]

        def _patched(path, *a, **kw):
            _call_count[0] += 1
            if _call_count[0] > 1:
                raise OSError('mocked xref failure')
            return _real_scan(path, *a, **kw)
        original_fn = mod.pl.scan_parquet
        mod.pl.scan_parquet = _patched
        try:
            result = inner(executor)
            df = result.collect()
        finally:
            mod.pl.scan_parquet = original_fn
        assert 'current_bene_mbi_id' in df.columns
        assert set(df['current_bene_mbi_id'].to_list()) == {'MBI001', 'MBI002'}

    @pytest.mark.unit
    def test_execute_xref_success(self, tmp_path):
        """Cover the success path for comparison."""
        from acoharmony._transforms import int_beneficiary_demographics_deduped as mod
        silver = tmp_path
        _write(pl.DataFrame(self._base_cclf8_columns()), silver / 'cclf8.parquet')
        # identity_timeline fixture: MBI001 remaps to MBI999; MBI999 is the leaf.
        xref = pl.DataFrame(
            {
                'mbi': ['MBI001', 'MBI999'],
                'maps_to_mbi': ['MBI999', None],
                'effective_date': [None, None],
                'obsolete_date': [None, None],
                'file_date': [None, None],
                'observation_type': ['cclf9_remap', 'cclf8_self'],
                'source_file': ['t', 't'],
                'hcmpi': [None, None],
                'chain_id': ['c', 'c'],
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
        _write(xref, silver / 'identity_timeline.parquet')
        executor = _MockExecutor(silver)
        inner = _get_inner_fn(mod.execute)
        result = inner(executor)
        df = result.collect()
        assert 'current_bene_mbi_id' in df.columns
        mbis = set(df['current_bene_mbi_id'].to_list())
        assert 'MBI999' in mbis
        assert 'MBI002' in mbis

class TestIntBeneficiaryDemographicsDeduped:

    @staticmethod
    def _cclf8(**overrides: Any) -> pl.DataFrame:
        base = {'bene_mbi_id': ['MBI1'], 'bene_hic_num': [None], 'bene_fips_state_cd': ['36'], 'bene_fips_cnty_cd': ['061'], 'bene_zip_cd': ['10001'], 'bene_dob': ['1950-01-15'], 'bene_sex_cd': ['1'], 'bene_race_cd': ['1'], 'bene_mdcr_stus_cd': ['10'], 'bene_dual_stus_cd': ['00'], 'bene_death_dt': [None], 'bene_rng_bgn_dt': ['2023-01-01'], 'bene_rng_end_dt': ['2023-12-31'], 'bene_fst_name': ['JOHN'], 'bene_mdl_name': ['A'], 'bene_lst_name': ['DOE'], 'bene_orgnl_entlmt_rsn_cd': ['0'], 'bene_entlmt_buyin_ind': ['3'], 'bene_part_a_enrlmt_bgn_dt': ['2015-01-01'], 'bene_part_b_enrlmt_bgn_dt': ['2015-01-01'], 'bene_line_1_adr': ['123 Main St'], 'bene_line_2_adr': [None], 'bene_line_3_adr': [None], 'bene_line_4_adr': [None], 'bene_line_5_adr': [None], 'bene_line_6_adr': [None], 'bene_city': ['NEW YORK'], 'bene_state': ['NY'], 'bene_zip': ['10001'], 'bene_zip_ext': ['1234'], 'source_filename': ['cclf8.csv'], 'file_date': ['2023-07-01']}
        base.update(overrides)
        return pl.DataFrame(base)

    @pytest.mark.unit
    def test_basic_output_columns(self, tmp_path: Path) -> None:
        _write(self._cclf8(), tmp_path / 'cclf8.parquet')
        _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert 'current_bene_mbi_id' in result.columns
        assert 'geo_zip_plc_name' in result.columns
        assert 'geo_usps_state_cd' in result.columns
        assert 'geo_zip5_cd' in result.columns
        assert 'geo_zip4_cd' in result.columns
        assert result.shape[0] == 1

    @pytest.mark.unit
    def test_dedup_keeps_latest_file_date(self, tmp_path: Path) -> None:
        df = pl.concat([self._cclf8(file_date=['2023-01-01'], bene_fst_name=['OLD']), self._cclf8(file_date=['2023-07-01'], bene_fst_name=['NEW'])])
        _write(df, tmp_path / 'cclf8.parquet')
        _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result.shape[0] == 1
        assert result['bene_fst_name'][0] == 'NEW'

    @pytest.mark.unit
    def test_mbi_crosswalk_applied(self, tmp_path: Path) -> None:
        _write(self._cclf8(bene_mbi_id=['OLD_MBI']), tmp_path / 'cclf8.parquet')
        _write(_xref_df(), tmp_path / 'identity_timeline.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['current_bene_mbi_id'][0] == 'NEW_MBI'
