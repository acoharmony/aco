"""Unit tests for eligibility transforms module."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
import acoharmony

from acoharmony._transforms.eligibility import execute

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
    """execute basic functionality -- joins demographics with enrollment."""
    demographics = TestEligibility._demographics_df()
    enrollment = TestEligibility._enrollment_df()
    _write(demographics, tmp_path / 'int_beneficiary_demographics_deduped.parquet')
    _write(enrollment, tmp_path / 'int_enrollment.parquet')
    result = execute(_make_executor(tmp_path))
    assert isinstance(result, pl.LazyFrame)
    df = result.collect()
    assert 'person_id' in df.columns
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

def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)

class TestEligibilityExecutor:
    """Tests for eligibility executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import eligibility
        assert eligibility is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        assert callable(execute)

class TestEligibility:

    @staticmethod
    def _demographics_df() -> pl.DataFrame:
        return pl.DataFrame({'current_bene_mbi_id': ['MBI1'], 'bene_sex_cd': ['1'], 'bene_race_cd': ['1'], 'bene_dob': [date(1950, 1, 15)], 'bene_death_dt': [None], 'bene_orgnl_entlmt_rsn_cd': ['0'], 'bene_dual_stus_cd': ['00'], 'bene_mdcr_stus_cd': ['10'], 'bene_fst_name': ['JOHN'], 'bene_lst_name': ['DOE'], 'bene_line_1_adr': ['123 Main St'], 'bene_line_2_adr': [None], 'bene_line_3_adr': [None], 'bene_line_4_adr': [None], 'bene_line_5_adr': [None], 'bene_line_6_adr': [None], 'geo_zip_plc_name': ['NEW YORK'], 'geo_usps_state_cd': ['NY'], 'geo_zip5_cd': ['10001'], 'geo_zip4_cd': [None]}, schema_overrides={'bene_death_dt': pl.Date})

    @staticmethod
    def _enrollment_df() -> pl.DataFrame:
        return pl.DataFrame({'current_bene_mbi_id': ['MBI1'], 'enrollment_start_date': [date(2023, 1, 1)], 'enrollment_end_date': [date(2023, 12, 31)]})

    @pytest.mark.unit
    def test_basic_output_columns(self, tmp_path: Path) -> None:
        _write(self._demographics_df(), tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        expected_cols = {'person_id', 'member_id', 'subscriber_id', 'gender', 'race', 'birth_date', 'death_date', 'death_flag', 'enrollment_start_date', 'enrollment_end_date', 'payer', 'payer_type', 'plan', 'original_reason_entitlement_code', 'dual_status_code', 'medicare_status_code', 'first_name', 'last_name', 'social_security_number', 'subscriber_relation', 'address', 'city', 'state', 'zip_code', 'phone', 'data_source'}
        assert set(result.columns) == expected_cols

    @pytest.mark.unit
    def test_gender_mapping(self, tmp_path: Path) -> None:
        _write(self._demographics_df(), tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['gender'][0] == 'male'

    @pytest.mark.unit
    def test_gender_female(self, tmp_path: Path) -> None:
        demo = self._demographics_df().with_columns(pl.lit('2').alias('bene_sex_cd'))
        _write(demo, tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['gender'][0] == 'female'

    @pytest.mark.unit
    def test_gender_unknown(self, tmp_path: Path) -> None:
        demo = self._demographics_df().with_columns(pl.lit('0').alias('bene_sex_cd'))
        _write(demo, tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['gender'][0] == 'unknown'

    @pytest.mark.unit
    def test_race_mapping(self, tmp_path: Path) -> None:
        _write(self._demographics_df(), tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['race'][0] == 'white'

    @pytest.mark.unit
    def test_race_hispanic(self, tmp_path: Path) -> None:
        demo = self._demographics_df().with_columns(pl.lit('5').alias('bene_race_cd'))
        _write(demo, tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['race'][0] == 'hispanic'

    @pytest.mark.unit
    def test_death_flag(self, tmp_path: Path) -> None:
        demo = self._demographics_df().with_columns(pl.lit(date(2023, 6, 1)).alias('bene_death_dt'))
        _write(demo, tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['death_flag'][0] == 1

    @pytest.mark.unit
    def test_death_flag_zero_when_alive(self, tmp_path: Path) -> None:
        _write(self._demographics_df(), tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['death_flag'][0] == 0

    @pytest.mark.unit
    def test_address_concatenation(self, tmp_path: Path) -> None:
        demo = self._demographics_df().with_columns(pl.lit('Apt 4B').alias('bene_line_2_adr'))
        _write(demo, tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert '123 Main St' in result['address'][0]
        assert 'Apt 4B' in result['address'][0]

    @pytest.mark.unit
    def test_zip_code_with_extension(self, tmp_path: Path) -> None:
        demo = self._demographics_df().with_columns(pl.lit('1234').alias('geo_zip4_cd'))
        _write(demo, tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['zip_code'][0] == '10001-1234'

    @pytest.mark.unit
    def test_payer_fields(self, tmp_path: Path) -> None:
        _write(self._demographics_df(), tmp_path / 'int_beneficiary_demographics_deduped.parquet')
        _write(self._enrollment_df(), tmp_path / 'int_enrollment.parquet')
        result = execute(_make_executor(tmp_path)).collect()
        assert result['payer'][0] == 'medicare'
        assert result['payer_type'][0] == 'medicare'
        assert result['plan'][0] == 'medicare'
        assert result['data_source'][0] == 'medicare cclf'
