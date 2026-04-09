"""Tests for _transforms.aco_alignment_demographics module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch
import datetime
from pathlib import Path  # noqa: E402
from unittest.mock import MagicMock  # noqa: E402

import polars as pl  # noqa: E402
import pytest
import acoharmony


class TestAcoAlignmentDemographics:
    """Tests for ACO alignment demographics transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._aco_alignment_demographics is not None

    @pytest.mark.unit
    def test_apply_transform_exists(self):
        assert callable(apply_transform)





def _make_base_df():
    """Minimal alignment LazyFrame with current_mbi."""
    return pl.DataFrame({
        "current_mbi": ["MBI001", "MBI002", "MBI003"],
        "current_program": ["MSSP", "REACH", "MSSP"],
    }).lazy()


def _make_demographics_parquet(path: Path):
    """Write a minimal deduplicated demographics parquet file."""
    demo = pl.DataFrame({
        "current_bene_mbi_id": ["MBI001", "MBI002"],
        "bene_fst_name": ["Alice", "Bob"],
        "bene_lst_name": ["Smith", "Jones"],
        "bene_mdl_name": ["A", "B"],
        "bene_line_1_adr": ["123 Main St", "456 Oak Ave"],
        "geo_zip_plc_name": ["CityA", "CityB"],
        "geo_usps_state_cd": ["CA", "NY"],
        "geo_zip5_cd": ["90210", "10001"],
        "bene_fips_cnty_cd": ["06037", "36061"],
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    demo.write_parquet(path)


def _make_demographics_parquet_no_county(path: Path):
    """Write demographics parquet without county column."""
    demo = pl.DataFrame({
        "current_bene_mbi_id": ["MBI001", "MBI002"],
        "bene_fst_name": ["Alice", "Bob"],
        "bene_lst_name": ["Smith", "Jones"],
        "bene_mdl_name": ["A", "B"],
        "bene_line_1_adr": ["123 Main St", "456 Oak Ave"],
        "geo_zip_plc_name": ["CityA", "CityB"],
        "geo_usps_state_cd": ["CA", "NY"],
        "geo_zip5_cd": ["90210", "10001"],
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    demo.write_parquet(path)


def _make_catalog(silver_path: Path, bar_df=None):
    """Create mock catalog with storage_config."""
    catalog = MagicMock()
    catalog.storage_config.get_path.return_value = silver_path
    catalog.scan_table.return_value = bar_df
    return catalog


def _make_logger():
    return MagicMock()


class TestDemographicsTransformIdempotency:
    """Test idempotency guard."""

    @pytest.mark.unit
    def test_skip_when_already_joined(self):

        df = pl.DataFrame({
            "current_mbi": ["MBI001"],
            "current_program": ["MSSP"],
            "_demographics_joined": [True],
        }).lazy()
        logger = _make_logger()
        catalog = MagicMock()
        result = apply_transform(df, {}, catalog, logger, force=False)
        collected = result.collect()
        assert "_demographics_joined" in collected.columns
        logger.info.assert_any_call("Demographics already joined, skipping")

    @pytest.mark.unit
    def test_force_bypasses_idempotency(self, tmp_path):

        silver = tmp_path / "silver"
        _make_demographics_parquet(silver / "int_beneficiary_demographics_deduped.parquet")

        df = pl.DataFrame({
            "current_mbi": ["MBI001"],
            "current_program": ["MSSP"],
            "_demographics_joined": [True],
        }).lazy()
        logger = _make_logger()
        catalog = _make_catalog(silver, bar_df=None)
        result = apply_transform(df, {}, catalog, logger, force=True)
        collected = result.collect()
        assert "bene_first_name" in collected.columns


class TestDemographicsTransformJoin:
    """Test the main demographics join logic."""

    @pytest.mark.unit
    def test_join_adds_demographic_columns(self, tmp_path):

        silver = tmp_path / "silver"
        _make_demographics_parquet(silver / "int_beneficiary_demographics_deduped.parquet")

        df = _make_base_df()
        logger = _make_logger()
        catalog = _make_catalog(silver, bar_df=None)
        result = apply_transform(df, {}, catalog, logger).collect()

        assert "bene_first_name" in result.columns
        assert "bene_last_name" in result.columns
        assert "bene_zip_5" in result.columns
        assert "bene_county" in result.columns
        assert "_demographics_joined" in result.columns
        assert result.height == 3

    @pytest.mark.unit
    def test_join_without_county_in_source(self, tmp_path):

        silver = tmp_path / "silver"
        _make_demographics_parquet_no_county(silver / "int_beneficiary_demographics_deduped.parquet")

        df = _make_base_df()
        logger = _make_logger()
        catalog = _make_catalog(silver, bar_df=None)
        result = apply_transform(df, {}, catalog, logger).collect()

        assert "bene_first_name" in result.columns
        assert "bene_county" not in result.columns
        assert "_demographics_joined" in result.columns

    @pytest.mark.unit
    def test_missing_parquet_raises_error(self, tmp_path):

        silver = tmp_path / "silver"
        silver.mkdir(parents=True, exist_ok=True)

        df = _make_base_df()
        logger = _make_logger()
        catalog = _make_catalog(silver, bar_df=None)
        with pytest.raises((ValueError, FileNotFoundError)):
            apply_transform(df, {}, catalog, logger)


class TestDemographicsBarFallback:
    """Test BAR data filling logic."""

    @pytest.mark.unit
    def test_bar_fills_missing_zip(self, tmp_path):

        silver = tmp_path / "silver"
        _make_demographics_parquet(silver / "int_beneficiary_demographics_deduped.parquet")

        # MBI003 is NOT in demographics, but IS in BAR
        bar_df = pl.DataFrame({
            "bene_mbi": ["MBI003"],
            "file_date": [datetime.date(2024, 6, 1)],
            "bene_zip_5": ["30301"],
            "bene_state": ["GA"],
            "bene_county_fips": ["13121"],
        }).lazy()

        df = _make_base_df()
        logger = _make_logger()
        catalog = _make_catalog(silver, bar_df=bar_df)
        result = apply_transform(df, {}, catalog, logger).collect()

        mbi003 = result.filter(pl.col("current_mbi") == "MBI003")
        assert mbi003["bene_zip"][0] == "30301"
        assert mbi003["bene_state"][0] == "GA"
        assert "_demographics_joined" in result.columns
        logger.info.assert_any_call("Filled missing demographics from BAR data")

    @pytest.mark.unit
    def test_bar_coalesces_with_existing_demographics(self, tmp_path):

        silver = tmp_path / "silver"
        _make_demographics_parquet(silver / "int_beneficiary_demographics_deduped.parquet")

        # MBI001 is in both demo and BAR -- demo should take precedence
        bar_df = pl.DataFrame({
            "bene_mbi": ["MBI001"],
            "file_date": [datetime.date(2024, 6, 1)],
            "bene_zip_5": ["99999"],
            "bene_state": ["TX"],
            "bene_county_fips": ["48001"],
        }).lazy()

        df = _make_base_df()
        logger = _make_logger()
        catalog = _make_catalog(silver, bar_df=bar_df)
        result = apply_transform(df, {}, catalog, logger).collect()

        mbi001 = result.filter(pl.col("current_mbi") == "MBI001")
        # Demographics value should win via coalesce
        assert mbi001["bene_zip"][0] == "90210"
        assert mbi001["bene_state"][0] == "CA"

    @pytest.mark.unit
    def test_bar_with_no_county_in_result(self, tmp_path):

        silver = tmp_path / "silver"
        _make_demographics_parquet_no_county(silver / "int_beneficiary_demographics_deduped.parquet")

        bar_df = pl.DataFrame({
            "bene_mbi": ["MBI003"],
            "file_date": [datetime.date(2024, 6, 1)],
            "bene_zip_5": ["30301"],
            "bene_state": ["GA"],
            "bene_county_fips": ["13121"],
        }).lazy()

        df = _make_base_df()
        logger = _make_logger()
        catalog = _make_catalog(silver, bar_df=bar_df)
        result = apply_transform(df, {}, catalog, logger).collect()
        # bene_county column should NOT be present (not in demographics source)
        assert "bene_county" not in result.columns


# ---------------------------------------------------------------------------
# Coverage gap tests: _aco_alignment_demographics.py lines 64-65
# ---------------------------------------------------------------------------


class TestDemographicsSourceNotFound:
    """Cover error when demographics source is missing."""

    @pytest.mark.unit
    def test_missing_demographics_raises(self):
        """Lines 64-65: raise ValueError when demographics parquet not found."""


        df = _make_base_df()
        logger = _make_logger()
        catalog = MagicMock()
        catalog.storage_config.get_path.return_value = Path("/tmp/nonexistent")

        with patch("acoharmony._transforms._aco_alignment_demographics.pl.scan_parquet",
                    side_effect=FileNotFoundError("not found")):
            with pytest.raises(ValueError, match="int_beneficiary_demographics_deduped source not found"):
                apply_transform(df, {}, catalog, logger)
