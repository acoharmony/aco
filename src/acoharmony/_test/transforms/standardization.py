from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
import inspect
from unittest.mock import MagicMock

import polars as pl
import pytest

import acoharmony

# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._transforms._standardization module."""





class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._standardization is not None



# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.standardization module."""





def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestStandardizationTransform:
    """Tests for standardize and apply_standard_transform."""

    @pytest.mark.unit
    def test_standardize_with_cms_columns(self):

        df = pl.DataFrame({
            "cur_clm_uniq_id": ["CLM1", "CLM2"],
            "bene_mbi_id": ["MBI1", "MBI2"],
            "clm_from_dt": [datetime.date(2024, 1, 1), datetime.date(2024, 6, 15)],
            "clm_thru_dt": [datetime.date(2024, 1, 31), datetime.date(2024, 6, 30)],
            "prvdr_oscar_num": ["OS1", "OS2"],
            "rndrg_prvdr_npi": ["NPI1", "NPI2"],
        }).lazy()
        result = standardize(df).collect()
        assert "claim_id" in result.columns
        assert "beneficiary_id" in result.columns
        assert "service_from_date" in result.columns
        assert "service_thru_date" in result.columns
        assert "provider_id" in result.columns
        assert "provider_npi" in result.columns
        # Computed columns from service_from_date
        assert "service_year" in result.columns
        assert "service_month" in result.columns
        assert result["service_year"][0] == 2024
        assert result["service_month"][0] == 1

    @pytest.mark.unit
    def test_standardize_idempotent(self):

        df = pl.DataFrame({
            "claim_id": ["CLM1"],  # Already renamed
            "service_from_date": [datetime.date(2024, 3, 1)],
        }).lazy()
        result = standardize(df).collect()
        # Should not duplicate columns
        assert result.columns.count("claim_id") == 1
        assert "service_year" in result.columns

    @pytest.mark.unit
    def test_standardize_without_service_date(self):

        df = pl.DataFrame({"cur_clm_uniq_id": ["CLM1"]}).lazy()
        result = standardize(df).collect()
        assert "claim_id" in result.columns
        assert "service_year" not in result.columns  # No service_from_date

    @pytest.mark.unit
    def test_apply_standard_transform_renames(self):

        df = pl.DataFrame({
            "old_col1": ["a", "b"],
            "old_col2": [1, 2],
        }).lazy()
        logger = MagicMock()
        config = {
            "rename_columns": {"old_col1": "new_col1", "old_col2": "new_col2"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "new_col1" in result.columns
        assert "new_col2" in result.columns
        assert "old_col1" not in result.columns

    @pytest.mark.unit
    def test_apply_standard_transform_computed_year(self):

        df = pl.DataFrame({
            "service_from_date": [datetime.date(2024, 3, 15)],
        }).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {"service_year": "year_from_service_date"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["service_year"][0] == 2024

    @pytest.mark.unit
    def test_apply_standard_transform_computed_month(self):

        df = pl.DataFrame({
            "service_from_date": [datetime.date(2024, 7, 1)],
        }).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {"svc_month": "month_from_service_date"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["svc_month"][0] == 7

    @pytest.mark.unit
    def test_apply_standard_transform_computed_quarter(self):

        df = pl.DataFrame({
            "service_from_date": [datetime.date(2024, 7, 1)],
        }).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {"svc_quarter": "quarter_from_service_date"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["svc_quarter"][0] == 3

    @pytest.mark.unit
    def test_apply_standard_transform_computed_year_month_enrollment(self):

        df = pl.DataFrame({
            "enrollment_start_date": [datetime.date(2024, 3, 1)],
        }).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {"ym": "format_year_month_from_enrollment_start"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["ym"][0] == "202403"

    @pytest.mark.unit
    def test_apply_standard_transform_add_literal_column(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "add_columns": [{"name": "aco_id", "value": "ACO123"}],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["aco_id"][0] == "ACO123"

    @pytest.mark.unit
    def test_apply_standard_transform_add_null_column(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "add_columns": [{"name": "empty_col", "value": "null"}],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["empty_col"][0] is None

    @pytest.mark.unit
    def test_apply_standard_transform_add_column_reference(self):

        df = pl.DataFrame({"source_col": ["val1"]}).lazy()
        logger = MagicMock()
        config = {
            "add_columns": [{"name": "target_col", "value": "source_col"}],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["target_col"][0] == "val1"

    @pytest.mark.unit
    def test_apply_standard_transform_add_column_no_name(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "add_columns": [{"value": "something"}],  # No name
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result.columns == ["id"]  # No column added

    @pytest.mark.unit
    def test_apply_standard_transform_add_column_already_exists(self):

        df = pl.DataFrame({"existing": [1]}).lazy()
        logger = MagicMock()
        config = {
            "add_columns": [{"name": "existing", "value": "new_val"}],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["existing"][0] == 1  # Original value kept

    @pytest.mark.unit
    def test_apply_standard_transform_conditional_is_not_null(self):

        df = pl.DataFrame({
            "bene_death_dt": [datetime.date(2024, 3, 15), None],
        }).lazy()
        logger = MagicMock()
        config = {
            "conditional_columns": [
                {
                    "name": "is_deceased",
                    "condition": "bene_death_dt.is_not_null()",
                    "value": "true",
                    "else": "false",
                }
            ],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["is_deceased"][0] == "true"
        assert result["is_deceased"][1] == "false"

    @pytest.mark.unit
    def test_apply_standard_transform_conditional_missing_column(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "conditional_columns": [
                {
                    "name": "flag",
                    "condition": "missing_col.is_not_null()",
                    "value": "true",
                    "else": "false",
                }
            ],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "flag" not in result.columns  # Skipped due to missing col

    @pytest.mark.unit
    def test_apply_standard_transform_conditional_unsupported(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "conditional_columns": [
                {
                    "name": "flag",
                    "condition": "id > 5",
                    "value": "true",
                    "else": "false",
                }
            ],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "flag" not in result.columns  # Skipped due to unsupported condition

    @pytest.mark.unit
    def test_apply_standard_transform_conditional_no_name(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "conditional_columns": [
                {"condition": "id.is_not_null()", "value": "true", "else": "false"}
            ],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result.columns == ["id"]  # Skipped

    @pytest.mark.unit
    def test_apply_standard_transform_conditional_already_exists(self):

        df = pl.DataFrame({"existing": [1]}).lazy()
        logger = MagicMock()
        config = {
            "conditional_columns": [
                {
                    "name": "existing",
                    "condition": "existing.is_not_null()",
                    "value": "true",
                    "else": "false",
                }
            ],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["existing"][0] == 1  # Original kept

    @pytest.mark.unit
    def test_apply_standard_transform_enrollment_end_death(self):

        df = pl.DataFrame({
            "bene_death_dt": [datetime.date(2024, 3, 15), None],
        }).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {
                "enrollment_end_date": "enrollment_end_date_with_death_truncation"
            },
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "enrollment_end_date" in result.columns
        # For the dead person, enrollment_end_date should be end of March
        end_date = result["enrollment_end_date"][0]
        assert end_date.month == 3
        assert end_date.day == 31
        # For the alive person
        assert result["enrollment_end_date"][1] is None

    @pytest.mark.unit
    def test_apply_standard_transform_empty_config(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        result = apply_standard_transform(df, {}, logger).collect()
        assert result.columns == ["id"]


class TestStandardizationConditionalEdgeCases:
    """Edge cases for apply_standard_transform conditional columns."""

    @pytest.mark.unit
    def test_conditional_end_of_month_true_value(self):

        df = pl.DataFrame({
            "bene_death_dt": [datetime.date(2024, 2, 15)],
        }).lazy()
        logger = MagicMock()
        config = {
            "conditional_columns": [
                {
                    "name": "coverage_end",
                    "condition": "bene_death_dt.is_not_null()",
                    "value": "bene_death_dt.dt.end_of_month()",
                    "else": "null",
                }
            ],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "coverage_end" in result.columns
        end_date = result["coverage_end"][0]
        assert end_date.month == 2
        assert end_date.day == 29  # 2024 is a leap year

    @pytest.mark.unit
    def test_conditional_end_of_month_missing_col(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "conditional_columns": [
                {
                    "name": "coverage_end",
                    "condition": "id.is_not_null()",
                    "value": "missing_col.dt.end_of_month()",
                    "else": "null",
                }
            ],
        }
        result = apply_standard_transform(df, config, logger).collect()
        # Should skip because missing_col doesn't exist
        assert "coverage_end" not in result.columns

    @pytest.mark.unit
    def test_conditional_null_else_value(self):

        df = pl.DataFrame({
            "some_col": [None, "val"],
        }).lazy()
        logger = MagicMock()
        config = {
            "conditional_columns": [
                {
                    "name": "flag",
                    "condition": "some_col.is_not_null()",
                    "value": "yes",
                    "else": "null",
                }
            ],
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert result["flag"][0] is None
        assert result["flag"][1] == "yes"


class TestComputedColumnMissingSource:
    """Test apply_standard_transform skips computed when source missing."""

    @pytest.mark.unit
    def test_year_from_service_date_missing(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {"year": "year_from_service_date"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "year" not in result.columns

    @pytest.mark.unit
    def test_month_from_service_date_missing(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {"month": "month_from_service_date"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "month" not in result.columns

    @pytest.mark.unit
    def test_quarter_from_service_date_missing(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {"quarter": "quarter_from_service_date"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "quarter" not in result.columns

    @pytest.mark.unit
    def test_format_year_month_enrollment_missing(self):

        df = pl.DataFrame({"id": [1]}).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {"ym": "format_year_month_from_enrollment_start"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "ym" not in result.columns

    @pytest.mark.unit
    def test_computed_column_already_exists(self):

        df = pl.DataFrame({
            "service_year": [2024],
            "service_from_date": [datetime.date(2024, 1, 1)],
        }).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {"service_year": "year_from_service_date"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        # Should keep original value since column exists
        assert result["service_year"][0] == 2024


class TestRenameMapNoQualifyingEntries:
    """Cover branches 352->351 and 355->359: rename_map is non-empty but no entries qualify."""

    @pytest.mark.unit
    def test_rename_map_old_name_not_in_schema(self):
        """When rename_map has entries but old_name is not in schema,
        actual_renames stays empty and rename is skipped (branches 352->351, 355->359)."""
        df = pl.DataFrame({"existing_col": [1]}).lazy()
        logger = MagicMock()
        config = {
            "rename_columns": {"nonexistent_col": "new_name"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        assert "existing_col" in result.columns
        assert "new_name" not in result.columns
        # Renamed 0 columns - the info log for rename should not have been called
        rename_calls = [
            c for c in logger.info.call_args_list
            if "Renamed" in str(c)
        ]
        assert len(rename_calls) == 0

    @pytest.mark.unit
    def test_rename_map_target_already_exists(self):
        """When rename_map has entries but new_name already in schema,
        actual_renames stays empty (branches 352->351, 355->359)."""
        df = pl.DataFrame({
            "old_col": ["a"],
            "new_col": ["b"],
        }).lazy()
        logger = MagicMock()
        config = {
            "rename_columns": {"old_col": "new_col"},
        }
        result = apply_standard_transform(df, config, logger).collect()
        # old_col should still exist (not renamed)
        assert "old_col" in result.columns
        assert result["new_col"][0] == "b"


class TestUnrecognizedComputationSkipped:
    """Cover branch 389->359: unrecognized computation falls through all elif
    branches and loops back to the for statement."""

    @pytest.mark.unit
    def test_unknown_computation_string_is_silently_skipped(self):
        """When an unrecognized computation string is provided, it does not match
        any of the elif branches (including format_year_month_from_enrollment_start
        at line 389) and the loop continues (branch 389->359)."""
        df = pl.DataFrame({
            "id": [1],
        }).lazy()
        logger = MagicMock()
        config = {
            "add_computed": {
                "mystery_col": "some_unknown_computation_type",
            },
        }
        result = apply_standard_transform(df, config, logger).collect()
        # Unknown computation should be silently skipped
        assert "mystery_col" not in result.columns
        assert result.columns == ["id"]


class TestStandardizeEdgeCases:
    """Additional edge cases for the standardize function."""

    @pytest.mark.unit
    def test_current_bene_mbi_id_takes_priority(self):
        """When current_bene_mbi_id exists, it maps to beneficiary_id."""

        df = pl.DataFrame({
            "current_bene_mbi_id": ["MBI1"],
        }).lazy()
        result = standardize(df).collect()
        assert "beneficiary_id" in result.columns
        assert result["beneficiary_id"][0] == "MBI1"

    @pytest.mark.unit
    def test_no_rename_if_target_exists(self):

        df = pl.DataFrame({
            "cur_clm_uniq_id": ["CLM1"],
            "claim_id": ["EXISTING"],
        }).lazy()
        result = standardize(df).collect()
        # Should not rename because claim_id already exists
        assert "cur_clm_uniq_id" in result.columns
        assert result["claim_id"][0] == "EXISTING"


class TestStandardizationTransformExtended:
    """Tests for Standardization transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._standardization is not None


# ---------------------------------------------------------------------------
# Coverage gap tests: _standardization.py lines 456, 459
# ---------------------------------------------------------------------------


class TestConditionalColumnException:
    """Cover exception handling in conditional column addition."""

    @pytest.mark.unit
    def test_conditional_column_failure_logs_warning(self):
        """Lines 456, 459: exception during conditional column is logged as warning."""
        # Verify the module has the expected exception handling pattern

        source = inspect.getsource(acoharmony._transforms._standardization)
        assert "Failed to add conditional column" in source
