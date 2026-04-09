"""Tests for _transforms._aco_alignment_temporal module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import inspect
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import acoharmony


class TestAcoAlignmentTemporal:
    """Tests for ACO alignment temporal transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._aco_alignment_temporal is not None

    @pytest.mark.unit
    def test_apply_transform_exists(self):
        assert callable(apply_transform)


class TestCalculateFirstProgramDate:
    """Tests for _calculate_first_program_date."""

    @pytest.mark.unit
    def test_basic(self):

        year_months = ["202401", "202402", "202403"]
        expr = _calculate_first_program_date(year_months, "reach")

        df = pl.DataFrame({
            "ym_202401_reach": [False, True],
            "ym_202402_reach": [True, True],
            "ym_202403_reach": [True, False],
        })

        result = df.with_columns(expr.alias("first_date"))
        assert result["first_date"][0] == date(2024, 2, 1)
        assert result["first_date"][1] == date(2024, 1, 1)

    @pytest.mark.unit
    def test_all_false(self):

        year_months = ["202401", "202402"]
        expr = _calculate_first_program_date(year_months, "mssp")

        df = pl.DataFrame({
            "ym_202401_mssp": [False],
            "ym_202402_mssp": [False],
        })

        result = df.with_columns(expr.alias("first_date"))
        assert result["first_date"][0] is None

    @pytest.mark.unit
    def test_single_month(self):

        year_months = ["202406"]
        expr = _calculate_first_program_date(year_months, "reach")

        df = pl.DataFrame({"ym_202406_reach": [True, False]})
        result = df.with_columns(expr.alias("first_date"))
        assert result["first_date"][0] == date(2024, 6, 1)
        assert result["first_date"][1] is None


class TestCalculateLastProgramDate:
    """Tests for _calculate_last_program_date."""

    @pytest.mark.unit
    def test_basic(self):

        year_months = ["202401", "202402", "202403"]
        expr = _calculate_last_program_date(year_months, "reach")

        df = pl.DataFrame({
            "ym_202401_reach": [True, False],
            "ym_202402_reach": [True, False],
            "ym_202403_reach": [False, True],
        })

        result = df.with_columns(expr.alias("last_date"))
        assert result["last_date"][0] == date(2024, 2, 1)
        assert result["last_date"][1] == date(2024, 3, 1)

    @pytest.mark.unit
    def test_all_false(self):

        year_months = ["202401", "202402"]
        expr = _calculate_last_program_date(year_months, "mssp")

        df = pl.DataFrame({
            "ym_202401_mssp": [False],
            "ym_202402_mssp": [False],
        })

        result = df.with_columns(expr.alias("last_date"))
        assert result["last_date"][0] is None

    @pytest.mark.unit
    def test_single_month(self):

        year_months = ["202412"]
        expr = _calculate_last_program_date(year_months, "mssp")

        df = pl.DataFrame({"ym_202412_mssp": [True]})
        result = df.with_columns(expr.alias("last_date"))
        assert result["last_date"][0] == date(2024, 12, 1)

    @pytest.mark.unit
    def test_all_true(self):

        year_months = ["202401", "202402", "202403"]
        expr = _calculate_last_program_date(year_months, "reach")

        df = pl.DataFrame({
            "ym_202401_reach": [True],
            "ym_202402_reach": [True],
            "ym_202403_reach": [True],
        })

        result = df.with_columns(expr.alias("last_date"))
        assert result["last_date"][0] == date(2024, 3, 1)


class TestBuildMbiMap:
    """Tests for _build_mbi_map."""

    @pytest.mark.unit
    def test_basic(self):

        crosswalk_df = pl.DataFrame({
            "prvs_num": ["MBI_OLD1", "MBI_OLD2", "MBI_SAME"],
            "crnt_num": ["MBI_NEW1", "MBI_NEW2", "MBI_SAME"],
        }).lazy()

        logger = MagicMock()
        result = _build_mbi_map(crosswalk_df, logger)
        assert result["MBI_OLD1"] == "MBI_NEW1"
        assert result["MBI_OLD2"] == "MBI_NEW2"
        assert "MBI_SAME" not in result

    @pytest.mark.unit
    def test_empty(self):

        crosswalk_df = pl.DataFrame({
            "prvs_num": pl.Series([], dtype=pl.Utf8),
            "crnt_num": pl.Series([], dtype=pl.Utf8),
        }).lazy()

        logger = MagicMock()
        result = _build_mbi_map(crosswalk_df, logger)
        assert result == {}

    @pytest.mark.unit
    def test_null_values(self):

        crosswalk_df = pl.DataFrame({
            "prvs_num": ["MBI1", None, "MBI3"],
            "crnt_num": ["MBI2", "MBI4", None],
        }).lazy()

        logger = MagicMock()
        result = _build_mbi_map(crosswalk_df, logger)
        assert result.get("MBI1") == "MBI2"
        assert None not in result


class TestDetermineObservableRange:
    """Tests for _determine_observable_range."""

    @pytest.mark.unit
    def test_basic(self):

        sources = {
            "bar": pl.DataFrame({"file_date": ["2024-01-15", "2024-06-15"]}).lazy(),
            "alr": pl.DataFrame({"file_date": ["2024-03-01", "2024-09-01"]}).lazy(),
        }
        logger = MagicMock()
        start, end = _determine_observable_range(sources, logger)
        assert start == date(2024, 1, 15)
        assert end == date(2024, 9, 1)

    @pytest.mark.unit
    def test_single_date_each(self):

        sources = {
            "bar": pl.DataFrame({"file_date": ["2024-05-01"]}).lazy(),
            "alr": pl.DataFrame({"file_date": ["2024-05-01"]}).lazy(),
        }
        logger = MagicMock()
        start, end = _determine_observable_range(sources, logger)
        assert start == date(2024, 5, 1)
        assert end == date(2024, 5, 1)


class TestPrepareBarData:
    """Tests for _prepare_bar_data."""

    @pytest.mark.unit
    def test_basic(self):

        bar_df = pl.DataFrame({
            "bene_mbi": ["MBI1", "MBI2"],
            "source_filename": ["D0259_BAR_202401.csv", "D0259_BAR_202401.csv"],
            "file_date": ["2024-01-15", "2024-01-15"],
            "bene_date_of_death": [None, None],
        }).lazy()

        mbi_map = {"MBI1": "MBI1_NEW"}
        logger = MagicMock()
        result = _prepare_bar_data(bar_df, mbi_map, logger)
        collected = result.collect()

        assert "current_mbi" in collected.columns
        assert "program" in collected.columns
        assert "file_date_parsed" in collected.columns
        assert collected["current_mbi"][0] == "MBI1_NEW"
        assert collected["current_mbi"][1] == "MBI2"


class TestPrepareAlrData:
    """Tests for _prepare_alr_data."""

    @pytest.mark.unit
    def test_basic(self):

        alr_df = pl.DataFrame({
            "bene_mbi": ["MBI_A", "MBI_B"],
            "source_filename": ["A1234_ALR_202403.csv", "A1234_ALR_202403.csv"],
            "file_date": ["2024-03-01", "2024-03-01"],
        }).lazy()

        mbi_map = {}
        logger = MagicMock()
        result = _prepare_alr_data(alr_df, mbi_map, logger)
        collected = result.collect()

        assert "current_mbi" in collected.columns
        assert "program" in collected.columns
        assert collected["program"][0] == "MSSP"


class TestPrepareFfsData:
    """Tests for _prepare_ffs_data."""

    @pytest.mark.unit
    def test_basic(self):

        ffs_df = pl.DataFrame({
            "bene_mbi": ["MBI_X"],
            "ffs_first_date": [date(2024, 1, 15)],
            "claim_count": [5],
        }).lazy()

        mbi_map = {"MBI_X": "MBI_Y"}
        logger = MagicMock()
        result = _prepare_ffs_data(ffs_df, mbi_map, logger)
        collected = result.collect()

        assert "current_mbi" in collected.columns
        assert collected["current_mbi"][0] == "MBI_Y"
        assert "ffs_first_date" in collected.columns


class TestPrepareDemographics:
    """Tests for _prepare_demographics."""

    @pytest.mark.unit
    def test_basic(self):

        demo_df = pl.DataFrame({
            "current_bene_mbi_id": ["MBI1"],
            "bene_dob": [date(1950, 1, 1)],
            "bene_death_dt": [None],
            "bene_sex_cd": ["1"],
            "bene_race_cd": ["1"],
            "bene_fips_state_cd": ["36"],
            "bene_fips_cnty_cd": ["061"],
            "bene_zip_cd": ["10001"],
        }).lazy()

        mbi_map = {}
        logger = MagicMock()
        result = _prepare_demographics(demo_df, mbi_map, logger)
        collected = result.collect()

        assert "current_mbi" in collected.columns
        assert "birth_date" in collected.columns
        assert "death_date" in collected.columns
        assert collected["current_mbi"][0] == "MBI1"


class TestBuildTemporalMatrixVectorized:
    """Tests for _build_temporal_matrix_vectorized."""

    @pytest.mark.unit
    def test_basic_reach_only(self):

        bar_data = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 1, 15)],
            "bene_mbi": ["MBI1"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "birth_date": [date(1950, 1, 1)],
            "death_date": [None],
            "sex": ["1"],
            "race": ["1"],
            "ethnicity": [None],
            "state": ["36"],
            "county": ["061"],
            "zip_code": ["10001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 1, 1), date(2024, 1, 31),
            None, demographics, logger,
        )

        collected = result.collect()
        assert collected.height == 1
        assert "ym_202401_reach" in collected.columns
        assert collected["ym_202401_reach"][0] is True
        assert collected["ym_202401_ffs"][0] is False

    @pytest.mark.unit
    def test_reach_and_mssp_exclusive(self):

        bar_data = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 1, 15)],
            "bene_mbi": ["MBI1"],
            "bene_date_of_death": [None],
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "aco_id": ["A1234"],
            "program": ["MSSP"],
            "file_date_parsed": [date(2024, 1, 15)],
            "bene_mbi": ["MBI1"],
            "bene_date_of_death": [None],
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "birth_date": [date(1950, 1, 1)],
            "death_date": [None],
            "sex": ["1"],
            "race": ["1"],
            "ethnicity": [None],
            "state": ["36"],
            "county": ["061"],
            "zip_code": ["10001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 1, 1), date(2024, 1, 31),
            None, demographics, logger,
        )

        collected = result.collect()
        # REACH takes precedence, MSSP should be False
        assert collected["ym_202401_reach"][0] is True
        assert collected["ym_202401_mssp"][0] is False

    @pytest.mark.unit
    def test_ffs_data_tracked(self):

        bar_data = pl.DataFrame({
            "current_mbi": ["MBI_OTHER"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 1, 15)],
            "bene_mbi": ["MBI_OTHER"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        ffs_data = pl.DataFrame({
            "current_mbi": ["MBI_FFS"],
            "has_ffs_service": [True],
            "ffs_first_date": [date(2023, 6, 1)],
            "ffs_claim_count": [10],
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI_FFS", "MBI_OTHER"],
            "birth_date": [date(1960, 3, 1), date(1955, 5, 1)],
            "death_date": pl.Series([None, None], dtype=pl.Date),
            "sex": ["2", "1"],
            "race": ["2", "1"],
            "ethnicity": [None, None],
            "state": ["06", "36"],
            "county": ["037", "061"],
            "zip_code": ["90001", "10001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 1, 1), date(2024, 1, 31),
            ffs_data, demographics, logger,
        )

        collected = result.collect()
        assert collected.height >= 1
        # FFS MBI should be present
        ffs_row = collected.filter(pl.col("current_mbi") == "MBI_FFS")
        assert ffs_row.height == 1
        # Since not in any ACO, FFS should be true
        assert ffs_row["ym_202401_ffs"][0] is True
        # first_claim should be True (ffs_first_date < month)
        assert ffs_row["ym_202401_first_claim"][0] is True

    @pytest.mark.unit
    def test_death_date_exclusion(self):

        bar_data = pl.DataFrame({
            "current_mbi": ["MBI_DEAD"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 1, 15)],
            "bene_mbi": ["MBI_DEAD"],
            "bene_date_of_death": [date(2023, 6, 1)],  # Died before 2024
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI_DEAD"],
            "birth_date": [date(1940, 1, 1)],
            "death_date": [date(2023, 6, 1)],
            "sex": ["1"],
            "race": ["1"],
            "ethnicity": [None],
            "state": ["36"],
            "county": ["061"],
            "zip_code": ["10001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 1, 1), date(2024, 1, 31),
            None, demographics, logger,
        )

        collected = result.collect()
        dead_row = collected.filter(pl.col("current_mbi") == "MBI_DEAD")
        # Beneficiary died before month, so should NOT be in REACH
        assert dead_row["ym_202401_reach"][0] is False

    @pytest.mark.unit
    def test_multi_month_range(self):

        bar_data = pl.DataFrame({
            "current_mbi": ["MBI1", "MBI1"],
            "aco_id": ["D0259", "D0259"],
            "program": ["REACH", "REACH"],
            "file_date_parsed": [date(2024, 1, 15), date(2024, 2, 15)],
            "bene_mbi": ["MBI1", "MBI1"],
            "bene_date_of_death": pl.Series([None, None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "birth_date": [date(1950, 1, 1)],
            "death_date": [None],
            "sex": ["1"],
            "race": ["1"],
            "ethnicity": [None],
            "state": ["36"],
            "county": ["061"],
            "zip_code": ["10001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 1, 1), date(2024, 2, 28),
            None, demographics, logger,
        )

        collected = result.collect()
        assert "ym_202401_reach" in collected.columns
        assert "ym_202402_reach" in collected.columns
        assert collected["months_in_reach"][0] == 2


class TestCollectRequiredSources:
    """Tests for _collect_required_sources."""

    @pytest.mark.unit
    def test_missing_source_raises(self):

        catalog = MagicMock()
        catalog.scan_table.return_value = None
        logger = MagicMock()

        with pytest.raises(ValueError, match="source not found"):
            _collect_required_sources(catalog, logger)

    @patch("acoharmony.config.get_config")
    @pytest.mark.unit
    def test_missing_crosswalk_raises(self, mock_config, tmp_path):

        catalog = MagicMock()
        catalog.scan_table.return_value = pl.DataFrame({"col": ["val"]}).lazy()

        mock_cfg = MagicMock()
        mock_cfg.storage.base_path = tmp_path
        mock_cfg.storage.silver_dir = "silver"
        mock_config.return_value = mock_cfg

        logger = MagicMock()

        with pytest.raises(ValueError, match="enterprise_crosswalk not found"):
            _collect_required_sources(catalog, logger)


class TestApplyTransformIdempotency:
    """Tests for apply_transform caching behavior."""

    @pytest.mark.unit
    def test_apply_transform_has_force_parameter(self):


        inner = getattr(apply_transform, "func", apply_transform)
        sig = inspect.signature(inner)
        assert "force" in sig.parameters


class TestApplyTransformCaching:
    """Tests for apply_transform idempotency / caching branches."""

    def _make_sources(self):
        """Helper to build minimal sources dict used by _collect_required_sources."""
        bar = pl.DataFrame({
            "file_date": ["2024-01-15"],
            "processed_at": ["2024-01-15T00:00:00"],
            "bene_mbi": ["MBI1"],
            "source_filename": ["BAR_202401.csv"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()
        alr = pl.DataFrame({
            "file_date": ["2024-01-15"],
            "processed_at": ["2024-01-15T00:00:00"],
            "bene_mbi": ["MBI2"],
            "source_filename": ["ALR_202401.csv"],
        }).lazy()
        ffs = pl.DataFrame({
            "bene_mbi": ["MBI3"],
            "ffs_first_date": [date(2024, 1, 1)],
            "claim_count": [1],
        }).lazy()
        demo = pl.DataFrame({
            "current_bene_mbi_id": ["MBI1"],
            "bene_dob": [date(1950, 1, 1)],
            "bene_death_dt": pl.Series([None], dtype=pl.Date),
            "bene_sex_cd": ["1"],
            "bene_race_cd": ["1"],
            "bene_fips_state_cd": ["36"],
            "bene_fips_cnty_cd": ["061"],
            "bene_zip_cd": ["10001"],
        }).lazy()
        xwalk = pl.DataFrame({
            "prvs_num": ["MBI1"],
            "crnt_num": ["MBI1"],
        }).lazy()
        return {"bar": bar, "alr": alr, "ffs_first_dates": ffs,
                "beneficiary_demographics": demo, "enterprise_crosswalk": xwalk}

    @patch("acoharmony._transforms._aco_alignment_temporal._collect_required_sources")
    @patch("acoharmony._transforms._aco_alignment_temporal._determine_observable_range")
    @pytest.mark.unit
    def test_cached_matrix_returned_when_current(self, mock_range, mock_sources):
        """When existing matrix is up-to-date, it should be returned directly."""

        sources = self._make_sources()
        mock_sources.return_value = sources
        mock_range.return_value = (date(2024, 1, 1), date(2024, 1, 15))

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"exists": True}

        existing = pl.DataFrame({
            "observable_end": ["2024-01-15"],
            "processed_at": ["2025-01-01T00:00:00"],
        }).lazy()
        catalog.scan_table.return_value = existing

        # The sources' processed_at are before the matrix processed_at
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        result = inner(None, {}, catalog, logger, force=False)
        # Should return the existing matrix (no rebuild)
        assert result is existing

    @patch("acoharmony._transforms._aco_alignment_temporal._collect_required_sources")
    @patch("acoharmony._transforms._aco_alignment_temporal._determine_observable_range")
    @pytest.mark.unit
    def test_rebuild_when_observable_end_increased(self, mock_range, mock_sources, tmp_path):
        """When observable_end is newer than existing, should rebuild."""

        sources = self._make_sources()
        mock_sources.return_value = sources
        # observable_end is beyond existing matrix's end
        mock_range.return_value = (date(2024, 1, 1), date(2024, 6, 15))

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"exists": True}

        existing = pl.DataFrame({
            "observable_end": ["2024-01-15"],
            "processed_at": ["2025-01-01T00:00:00"],
        }).lazy()
        catalog.scan_table.return_value = existing

        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        # The rebuild path will fail trying to access config/storage, which is fine:
        # we just need to verify the caching logic was bypassed
        with patch("acoharmony._transforms._aco_alignment_temporal._build_mbi_map", return_value={}), \
             patch("acoharmony._transforms._aco_alignment_temporal._prepare_bar_data") as mock_bar, \
             patch("acoharmony._transforms._aco_alignment_temporal._prepare_alr_data") as mock_alr, \
             patch("acoharmony._transforms._aco_alignment_temporal._prepare_ffs_data") as mock_ffs, \
             patch("acoharmony._transforms._aco_alignment_temporal._prepare_demographics") as mock_demo, \
             patch("acoharmony._transforms._aco_alignment_temporal._build_temporal_matrix_vectorized") as mock_build, \
             patch("acoharmony.config.get_config") as mock_config:

            mock_bar.return_value = pl.DataFrame({"current_mbi": ["MBI1"]}).lazy()
            mock_alr.return_value = pl.DataFrame({"current_mbi": ["MBI1"]}).lazy()
            mock_ffs.return_value = pl.DataFrame({"current_mbi": ["MBI1"]}).lazy()
            mock_demo.return_value = pl.DataFrame({"current_mbi": ["MBI1"]}).lazy()
            # Return a minimal matrix from the build function
            matrix = pl.DataFrame({"current_mbi": ["MBI1"]}).lazy()
            mock_build.return_value = matrix

            cfg = MagicMock()
            cfg.storage.base_path = tmp_path
            cfg.storage.silver_dir = "silver"
            cfg.transform.compression = "zstd"
            cfg.transform.row_group_size = 100
            mock_config.return_value = cfg
            (tmp_path / "silver").mkdir(parents=True, exist_ok=True)

            try:
                inner(None, {}, catalog, logger, force=False)
            except Exception:
                pass  # Expected - the key point is the rebuild was triggered
            # Verify that the rebuild path was entered (observable end log message)
            rebuild_logged = any(
                "Source data updated" in str(c) and "rebuilding" in str(c)
                for c in logger.info.call_args_list
            )
            assert rebuild_logged

    @patch("acoharmony._transforms._aco_alignment_temporal._collect_required_sources")
    @patch("acoharmony._transforms._aco_alignment_temporal._determine_observable_range")
    @pytest.mark.unit
    def test_rebuild_when_source_modified_after_matrix(self, mock_range, mock_sources):
        """When source data is modified after matrix was built, should rebuild."""

        bar = pl.DataFrame({
            "file_date": ["2024-01-15"],
            "processed_at": ["2025-06-01T00:00:00"],  # Newer than matrix
            "bene_mbi": ["MBI1"],
            "source_filename": ["BAR.csv"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()
        alr = pl.DataFrame({
            "file_date": ["2024-01-15"],
            "processed_at": ["2025-06-01T00:00:00"],  # Newer than matrix
            "bene_mbi": ["MBI2"],
            "source_filename": ["ALR.csv"],
        }).lazy()
        sources = {
            "bar": bar, "alr": alr,
            "ffs_first_dates": pl.DataFrame({"bene_mbi": ["X"], "ffs_first_date": [date(2024, 1, 1)], "claim_count": [1]}).lazy(),
            "beneficiary_demographics": pl.DataFrame({
                "current_bene_mbi_id": ["MBI1"], "bene_dob": [date(1950, 1, 1)],
                "bene_death_dt": pl.Series([None], dtype=pl.Date), "bene_sex_cd": ["1"],
                "bene_race_cd": ["1"], "bene_fips_state_cd": ["36"],
                "bene_fips_cnty_cd": ["061"], "bene_zip_cd": ["10001"],
            }).lazy(),
            "enterprise_crosswalk": pl.DataFrame({"prvs_num": ["MBI1"], "crnt_num": ["MBI1"]}).lazy(),
        }
        mock_sources.return_value = sources
        mock_range.return_value = (date(2024, 1, 15), date(2024, 1, 15))

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"exists": True}

        existing = pl.DataFrame({
            "observable_end": ["2024-01-15"],
            "processed_at": ["2025-01-01T00:00:00"],  # Older than source
        }).lazy()
        catalog.scan_table.return_value = existing

        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        # The rebuild path calls further functions that will fail without full config,
        # but we verify the log message indicates rebuilding
        try:
            inner(None, {}, catalog, logger, force=False)
        except Exception:
            pass
        rebuild_logged = any(
            "Source data modified after matrix build" in str(c)
            for c in logger.info.call_args_list
        )
        assert rebuild_logged

    @patch("acoharmony._transforms._aco_alignment_temporal._collect_required_sources")
    @patch("acoharmony._transforms._aco_alignment_temporal._determine_observable_range")
    @pytest.mark.unit
    def test_exception_during_cache_check_triggers_rebuild(self, mock_range, mock_sources):
        """When cache check raises an exception, should fall through to rebuild."""

        sources = self._make_sources()
        mock_sources.return_value = sources
        mock_range.return_value = (date(2024, 1, 1), date(2024, 1, 15))

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"exists": True}
        catalog.scan_table.side_effect = Exception("Table corrupted")

        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        try:
            inner(None, {}, catalog, logger, force=False)
        except Exception:
            pass  # Expected to fail during rebuild
        rebuild_logged = any(
            "needs rebuild" in str(c) for c in logger.info.call_args_list
        )
        assert rebuild_logged

    @patch("acoharmony._transforms._aco_alignment_temporal._collect_required_sources")
    @patch("acoharmony._transforms._aco_alignment_temporal._determine_observable_range")
    @pytest.mark.unit
    def test_force_skips_cache_check(self, mock_range, mock_sources):
        """When force=True, should skip cache check entirely."""

        sources = self._make_sources()
        mock_sources.return_value = sources
        mock_range.return_value = (date(2024, 1, 1), date(2024, 1, 15))

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"exists": True}

        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        try:
            inner(None, {}, catalog, logger, force=True)
        except Exception:
            pass
        # When force=True, the catalog.scan_table for "aco_alignment" should NOT be called
        # (scan_table may be called for sources, but not for the cache)
        build_logged = any(
            "Building temporal matrix from source data" in str(c)
            for c in logger.info.call_args_list
        )
        assert build_logged

    @patch("acoharmony._transforms._aco_alignment_temporal._collect_required_sources")
    @patch("acoharmony._transforms._aco_alignment_temporal._determine_observable_range")
    @pytest.mark.unit
    def test_no_existing_metadata_rebuilds(self, mock_range, mock_sources):
        """When no existing metadata, should proceed to rebuild."""

        sources = self._make_sources()
        mock_sources.return_value = sources
        mock_range.return_value = (date(2024, 1, 1), date(2024, 1, 15))

        catalog = MagicMock()
        catalog.get_table_metadata.return_value = None  # No existing metadata

        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        try:
            inner(None, {}, catalog, logger, force=False)
        except Exception:
            pass
        build_logged = any(
            "Building temporal matrix from source data" in str(c)
            for c in logger.info.call_args_list
        )
        assert build_logged


class TestCollectRequiredSourcesSuccess:
    """Test successful source collection."""

    @patch("acoharmony.config.get_config")
    @pytest.mark.unit
    def test_all_sources_collected(self, mock_config, tmp_path):

        mock_cfg = MagicMock()
        mock_cfg.storage.base_path = tmp_path
        mock_cfg.storage.silver_dir = "silver"
        mock_config.return_value = mock_cfg

        silver = tmp_path / "silver"
        silver.mkdir(parents=True, exist_ok=True)
        # Write crosswalk parquet
        pl.DataFrame({"prvs_num": ["A"], "crnt_num": ["B"]}).write_parquet(
            silver / "enterprise_crosswalk.parquet"
        )

        catalog = MagicMock()
        catalog.scan_table.return_value = pl.DataFrame({"col": ["val"]}).lazy()
        logger = MagicMock()

        sources = _collect_required_sources(catalog, logger)
        assert "bar" in sources
        assert "alr" in sources
        assert "ffs_first_dates" in sources
        assert "beneficiary_demographics" in sources
        assert "enterprise_crosswalk" in sources
        assert len(sources) == 5


class TestBuildTemporalMatrixEdgeCases:
    """Additional edge case tests for temporal matrix building."""

    @pytest.mark.unit
    def test_empty_data_as_of_month_produces_false_columns(self):
        """When no data exists for a given month, all columns should be False."""

        # Bar data is from Feb, but we ask for Jan too
        bar_data = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 2, 15)],
            "bene_mbi": ["MBI1"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "birth_date": [date(1950, 1, 1)],
            "death_date": pl.Series([None], dtype=pl.Date),
            "sex": ["1"], "race": ["1"], "ethnicity": [None],
            "state": ["36"], "county": ["061"], "zip_code": ["10001"],
        }).lazy()

        logger = MagicMock()
        # Start date is Jan but bar data starts Feb - Jan should have all False
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 1, 1), date(2024, 2, 28),
            None, demographics, logger,
        )
        collected = result.collect()
        # Jan should be all false since no data as of Jan (empty path sets all to False)
        mbi1 = collected.filter(pl.col("current_mbi") == "MBI1")
        assert mbi1["ym_202401_reach"][0] is False
        assert mbi1["ym_202401_mssp"][0] is False
        assert mbi1["ym_202401_ffs"][0] is False  # empty data path sets all to False
        # Feb should have REACH
        assert mbi1["ym_202402_reach"][0] is True

    @pytest.mark.unit
    def test_mssp_only_no_reach(self):
        """MSSP-only beneficiary with no REACH data."""

        bar_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": ["MBI_MSSP"],
            "aco_id": ["A1234"],
            "program": ["MSSP"],
            "file_date_parsed": [date(2024, 3, 1)],
            "bene_mbi": ["MBI_MSSP"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI_MSSP"],
            "birth_date": [date(1955, 6, 1)],
            "death_date": pl.Series([None], dtype=pl.Date),
            "sex": ["2"], "race": ["2"], "ethnicity": [None],
            "state": ["06"], "county": ["037"], "zip_code": ["90001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 3, 1), date(2024, 3, 31),
            None, demographics, logger,
        )
        collected = result.collect()
        assert collected["ym_202403_mssp"][0] is True
        assert collected["ym_202403_reach"][0] is False
        assert collected["current_program"][0] == "MSSP"

    @pytest.mark.unit
    def test_december_month_boundary(self):
        """Test that December to January boundary is handled correctly."""

        bar_data = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 12, 15)],
            "bene_mbi": ["MBI1"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "birth_date": [date(1950, 1, 1)],
            "death_date": pl.Series([None], dtype=pl.Date),
            "sex": ["1"], "race": ["1"], "ethnicity": [None],
            "state": ["36"], "county": ["061"], "zip_code": ["10001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 12, 1), date(2024, 12, 31),
            None, demographics, logger,
        )
        collected = result.collect()
        assert "ym_202412_reach" in collected.columns
        assert collected["ym_202412_reach"][0] is True

    @pytest.mark.unit
    def test_summary_columns_calculated(self):
        """Test that summary columns (ever_reach, continuous_enrollment, etc.) are computed."""

        bar_data = pl.DataFrame({
            "current_mbi": ["MBI1", "MBI1"],
            "aco_id": ["D0259", "D0259"],
            "program": ["REACH", "REACH"],
            "file_date_parsed": [date(2024, 1, 15), date(2024, 2, 15)],
            "bene_mbi": ["MBI1", "MBI1"],
            "bene_date_of_death": pl.Series([None, None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "birth_date": [date(1950, 1, 1)],
            "death_date": pl.Series([None], dtype=pl.Date),
            "sex": ["1"], "race": ["1"], "ethnicity": [None],
            "state": ["36"], "county": ["061"], "zip_code": ["10001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 1, 1), date(2024, 2, 28),
            None, demographics, logger,
        )
        collected = result.collect()
        assert collected["ever_reach"][0] is True
        assert collected["ever_mssp"][0] is False
        assert collected["months_in_reach"][0] == 2
        assert collected["months_in_mssp"][0] == 0
        assert collected["current_program"][0] == "REACH"
        assert collected["bene_mbi"][0] == "MBI1"
        assert "first_reach_date" in collected.columns
        assert "last_reach_date" in collected.columns
        assert "enrollment_gaps" in collected.columns
        assert "continuous_enrollment" in collected.columns

    @pytest.mark.unit
    def test_existing_end_raw_as_datetime(self):
        """Test _determine_observable_range conversion when existing_end_raw is datetime."""

        inner = getattr(apply_transform, "func", apply_transform)
        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"exists": True}

        existing = pl.DataFrame({
            "observable_end": [datetime(2024, 1, 15)],
            "processed_at": [datetime(2025, 1, 1)],
        }).lazy()
        catalog.scan_table.return_value = existing

        sources = {
            "bar": pl.DataFrame({"file_date": ["2024-01-15"], "processed_at": [datetime(2024, 1, 1)]}).lazy(),
            "alr": pl.DataFrame({"file_date": ["2024-01-15"], "processed_at": [datetime(2024, 1, 1)]}).lazy(),
        }

        with patch("acoharmony._transforms._aco_alignment_temporal._collect_required_sources", return_value={**sources,
            "ffs_first_dates": pl.DataFrame({"bene_mbi": ["X"], "ffs_first_date": [date(2024, 1, 1)], "claim_count": [1]}).lazy(),
            "beneficiary_demographics": pl.DataFrame({
                "current_bene_mbi_id": ["MBI1"], "bene_dob": [date(1950, 1, 1)],
                "bene_death_dt": pl.Series([None], dtype=pl.Date), "bene_sex_cd": ["1"],
                "bene_race_cd": ["1"], "bene_fips_state_cd": ["36"],
                "bene_fips_cnty_cd": ["061"], "bene_zip_cd": ["10001"],
            }).lazy(),
            "enterprise_crosswalk": pl.DataFrame({"prvs_num": ["MBI1"], "crnt_num": ["MBI1"]}).lazy(),
        }), \
            patch("acoharmony._transforms._aco_alignment_temporal._determine_observable_range",
                  return_value=(date(2024, 1, 1), date(2024, 1, 15))):
            logger = MagicMock()
            result = inner(None, {}, catalog, logger, force=False)
            # With datetime observable_end and matching dates, should return cached
            assert result is existing


# ---------------------------------------------------------------------------
# Coverage gap tests: _aco_alignment_temporal.py lines 89, 103, 472, 550
# ---------------------------------------------------------------------------


class TestAcoAlignmentTemporalGaps:
    """Cover missing branches in temporal alignment."""

    @patch("acoharmony._transforms._aco_alignment_temporal._collect_required_sources")
    @patch("acoharmony._transforms._aco_alignment_temporal._determine_observable_range")
    @pytest.mark.unit
    def test_existing_end_raw_as_date_object(self, mock_range, mock_sources):
        """Branch 86->89: existing_end_raw is a date (not str or datetime), used as-is."""
        inner = getattr(apply_transform, "func", apply_transform)
        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"exists": True}

        # Use a plain date object for observable_end so it falls through both
        # isinstance(str) and isinstance(datetime) checks into the else branch.
        existing = pl.DataFrame({
            "observable_end": pl.Series([date(2024, 6, 15)], dtype=pl.Date),
            "processed_at": [datetime(2025, 6, 1)],
        }).lazy()
        catalog.scan_table.return_value = existing

        sources = {
            "bar": pl.DataFrame({"file_date": ["2024-01-15"], "processed_at": [datetime(2024, 1, 1)]}).lazy(),
            "alr": pl.DataFrame({"file_date": ["2024-01-15"], "processed_at": [datetime(2024, 1, 1)]}).lazy(),
        }
        mock_sources.return_value = {
            **sources,
            "ffs_first_dates": pl.DataFrame({"bene_mbi": ["X"], "ffs_first_date": [date(2024, 1, 1)], "claim_count": [1]}).lazy(),
            "beneficiary_demographics": pl.DataFrame({
                "current_bene_mbi_id": ["MBI1"], "bene_dob": [date(1950, 1, 1)],
                "bene_death_dt": pl.Series([None], dtype=pl.Date), "bene_sex_cd": ["1"],
                "bene_race_cd": ["1"], "bene_fips_state_cd": ["36"],
                "bene_fips_cnty_cd": ["061"], "bene_zip_cd": ["10001"],
            }).lazy(),
            "enterprise_crosswalk": pl.DataFrame({"prvs_num": ["MBI1"], "crnt_num": ["MBI1"]}).lazy(),
        }
        # observable_end <= existing_end so we go into the source-modified check
        mock_range.return_value = (date(2024, 1, 1), date(2024, 6, 15))

        logger = MagicMock()
        result = inner(None, {}, catalog, logger, force=False)
        # Source processed_at (2024-01-01) < matrix processed_at (2025-06-01)
        # so the matrix is current and should be returned as-is.
        assert result is existing

    @patch("acoharmony._transforms._aco_alignment_temporal._collect_required_sources")
    @patch("acoharmony._transforms._aco_alignment_temporal._determine_observable_range")
    @pytest.mark.unit
    def test_to_datetime_helper_with_none_processed_at(self, mock_range, mock_sources):
        """Branch 102->103: to_datetime receives None, returns datetime.min."""
        inner = getattr(apply_transform, "func", apply_transform)
        catalog = MagicMock()
        catalog.get_table_metadata.return_value = {"exists": True}

        existing = pl.DataFrame({
            "observable_end": ["2024-06-15"],
            "processed_at": [datetime(2025, 6, 1)],
        }).lazy()
        catalog.scan_table.return_value = existing

        # Both sources have None as processed_at to trigger `to_datetime(None)`
        sources = {
            "bar": pl.DataFrame({"file_date": ["2024-01-15"], "processed_at": pl.Series([None], dtype=pl.Datetime)}).lazy(),
            "alr": pl.DataFrame({"file_date": ["2024-01-15"], "processed_at": pl.Series([None], dtype=pl.Datetime)}).lazy(),
        }
        mock_sources.return_value = {
            **sources,
            "ffs_first_dates": pl.DataFrame({"bene_mbi": ["X"], "ffs_first_date": [date(2024, 1, 1)], "claim_count": [1]}).lazy(),
            "beneficiary_demographics": pl.DataFrame({
                "current_bene_mbi_id": ["MBI1"], "bene_dob": [date(1950, 1, 1)],
                "bene_death_dt": pl.Series([None], dtype=pl.Date), "bene_sex_cd": ["1"],
                "bene_race_cd": ["1"], "bene_fips_state_cd": ["36"],
                "bene_fips_cnty_cd": ["061"], "bene_zip_cd": ["10001"],
            }).lazy(),
            "enterprise_crosswalk": pl.DataFrame({"prvs_num": ["MBI1"], "crnt_num": ["MBI1"]}).lazy(),
        }
        # observable_end <= existing_end so we proceed to source-modified check
        mock_range.return_value = (date(2024, 1, 1), date(2024, 6, 15))

        logger = MagicMock()
        result = inner(None, {}, catalog, logger, force=False)
        # to_datetime(None) returns datetime.min, which < matrix processed_at
        # so the matrix is current and should be returned.
        assert result is existing

    @pytest.mark.unit
    def test_ffs_data_with_null_first_date_skipped(self):
        """Branch 453->452: ffs row with None ffs_first_date is skipped in dict build."""
        bar_data = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 1, 15)],
            "bene_mbi": ["MBI1"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        # ffs_data has a row with None ffs_first_date which should be skipped
        ffs_data = pl.DataFrame({
            "current_mbi": ["MBI_FFS_NULL"],
            "has_ffs_service": [True],
            "ffs_first_date": pl.Series([None], dtype=pl.Date),
            "ffs_claim_count": [0],
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI1", "MBI_FFS_NULL"],
            "birth_date": [date(1950, 1, 1), date(1955, 1, 1)],
            "death_date": pl.Series([None, None], dtype=pl.Date),
            "sex": ["1", "1"], "race": ["1", "1"], "ethnicity": [None, None],
            "state": ["36", "36"], "county": ["061", "061"], "zip_code": ["10001", "10001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 1, 1), date(2024, 1, 31),
            ffs_data, demographics, logger,
        )
        collected = result.collect()
        # The FFS-null MBI should still be in the result (from ffs_data's unique MBIs)
        ffs_null_row = collected.filter(pl.col("current_mbi") == "MBI_FFS_NULL")
        assert ffs_null_row.height == 1
        # Since ffs_first_date was None, it should NOT be in ffs_dict,
        # so first_claim should be False
        assert ffs_null_row["ym_202401_first_claim"][0] is False

    @pytest.mark.unit
    def test_end_date_month_appended_when_start_after_end(self):
        """Branch 470->471: end_date.day > 1 and its month not yet in year_months."""
        # Use start_date > end_date so the while loop produces an empty list,
        # but end_date.day > 1 triggers the safety-net append.
        bar_data = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 2, 15)],
            "bene_mbi": ["MBI1"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "birth_date": [date(1950, 1, 1)],
            "death_date": pl.Series([None], dtype=pl.Date),
            "sex": ["1"], "race": ["1"], "ethnicity": [None],
            "state": ["36"], "county": ["061"], "zip_code": ["10001"],
        }).lazy()

        logger = MagicMock()
        # start_date (March) > end_date (Feb 15) — while loop empty,
        # but end_date.day=15 > 1 and "202402" not in [], so it gets appended.
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 3, 1), date(2024, 2, 15),
            None, demographics, logger,
        )
        collected = result.collect()
        assert "ym_202402_reach" in collected.columns

    @pytest.mark.unit
    def test_reach_mbis_filters_mssp_candidates(self):
        """Branch 548->549: when reach_mbis is non-empty, MSSP candidates are filtered."""
        # MBI1 is in both REACH and MSSP; MBI2 is MSSP only.
        # REACH should take precedence for MBI1; MBI2 stays MSSP.
        bar_data = pl.DataFrame({
            "current_mbi": ["MBI1"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 3, 15)],
            "bene_mbi": ["MBI1"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": ["MBI1", "MBI2"],
            "aco_id": ["A1234", "A1234"],
            "program": ["MSSP", "MSSP"],
            "file_date_parsed": [date(2024, 3, 15), date(2024, 3, 15)],
            "bene_mbi": ["MBI1", "MBI2"],
            "bene_date_of_death": pl.Series([None, None], dtype=pl.Date),
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI1", "MBI2"],
            "birth_date": [date(1950, 1, 1), date(1955, 1, 1)],
            "death_date": pl.Series([None, None], dtype=pl.Date),
            "sex": ["1", "2"], "race": ["1", "2"], "ethnicity": [None, None],
            "state": ["36", "06"], "county": ["061", "037"], "zip_code": ["10001", "90001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 3, 1), date(2024, 3, 31),
            None, demographics, logger,
        )
        collected = result.collect()
        mbi1 = collected.filter(pl.col("current_mbi") == "MBI1")
        mbi2 = collected.filter(pl.col("current_mbi") == "MBI2")
        # MBI1 is REACH, should NOT be MSSP (filtered out by reach_mbis)
        assert mbi1["ym_202403_reach"][0] is True
        assert mbi1["ym_202403_mssp"][0] is False
        # MBI2 is MSSP only
        assert mbi2["ym_202403_reach"][0] is False
        assert mbi2["ym_202403_mssp"][0] is True

    @pytest.mark.unit
    def test_ffs_first_date_after_month_not_eligible(self):
        """Branch 579->581: ffs_dict[mbi] > month_date, so is_in_ffs stays False."""
        # Need at least one record in combined data so the month loop doesn't
        # hit the early-continue (data_as_of_month.height == 0) branch.
        bar_data = pl.DataFrame({
            "current_mbi": ["MBI_OTHER"],
            "aco_id": ["D0259"],
            "program": ["REACH"],
            "file_date_parsed": [date(2024, 1, 15)],
            "bene_mbi": ["MBI_OTHER"],
            "bene_date_of_death": pl.Series([None], dtype=pl.Date),
        }).lazy()

        alr_data = pl.DataFrame({
            "current_mbi": pl.Series([], dtype=pl.Utf8),
            "aco_id": pl.Series([], dtype=pl.Utf8),
            "program": pl.Series([], dtype=pl.Utf8),
            "file_date_parsed": pl.Series([], dtype=pl.Date),
            "bene_mbi": pl.Series([], dtype=pl.Utf8),
            "bene_date_of_death": pl.Series([], dtype=pl.Date),
        }).lazy()

        # FFS first date is in the future relative to the month being processed
        ffs_data = pl.DataFrame({
            "current_mbi": ["MBI_FFS"],
            "has_ffs_service": [True],
            "ffs_first_date": [date(2024, 6, 1)],  # After Jan 2024
            "ffs_claim_count": [5],
        }).lazy()

        demographics = pl.DataFrame({
            "current_mbi": ["MBI_FFS", "MBI_OTHER"],
            "birth_date": [date(1960, 3, 1), date(1955, 5, 1)],
            "death_date": pl.Series([None, None], dtype=pl.Date),
            "sex": ["1", "1"], "race": ["1", "1"], "ethnicity": [None, None],
            "state": ["36", "36"], "county": ["061", "061"], "zip_code": ["10001", "10001"],
        }).lazy()

        logger = MagicMock()
        result = _build_temporal_matrix_vectorized(
            bar_data, alr_data,
            date(2024, 1, 1), date(2024, 1, 31),
            ffs_data, demographics, logger,
        )
        collected = result.collect()
        ffs_row = collected.filter(pl.col("current_mbi") == "MBI_FFS")
        assert ffs_row.height == 1
        # ffs_first_date (June 2024) > month_date (Jan 2024), so first_claim is False
        assert ffs_row["ym_202401_first_claim"][0] is False
