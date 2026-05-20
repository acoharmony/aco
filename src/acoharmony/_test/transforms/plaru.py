





# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch
import pytest
import acoharmony

from acoharmony._transforms._plaru import process_plaru_sheet, transform_plaru_workbook

# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._transforms._plaru module."""








class TestModuleStructure:
    """Basic module structure tests."""


    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""


        assert acoharmony._transforms._plaru is not None



# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms._plaru module."""







class TestPLARUSheetConfig:
    """Tests for PLARUSheetConfig."""


    @pytest.mark.unit
    def test_default_values(self):
        config = PLARUSheetConfig(sheet_index=0, sheet_type="test")
        assert config.sheet_index == 0
        assert config.sheet_type == "test"
        assert config.transform_type == "standard"
        assert config.auto_detect is False
        assert config.pivot_config is None
        assert config.header_config is None
        assert config.meta_config is None
        assert config.append_config is None
        assert config.matrix_config is None
        assert config.data_start_row is None
        assert config.silver_columns is None
        assert config.drop_columns is None

    @pytest.mark.unit
    def test_with_pivot_config(self):
        config = PLARUSheetConfig(
            sheet_index=0,
            sheet_type="report_parameters",
            transform_type="key_value_pivot",
            pivot_config=KeyValuePivotConfig(
                key_column="column_1",
                value_column="column_2",
            ),
        )
        assert config.transform_type == "key_value_pivot"
        assert config.pivot_config is not None

    @pytest.mark.unit
    def test_with_header_config(self):
        config = PLARUSheetConfig(
            sheet_index=2,
            sheet_type="base_pcc_pmt_detailed",
            transform_type="multi_level_header",
            header_config=MultiLevelHeaderConfig(
                header_rows=[4, 5],
                separator="_",
            ),
        )
        assert config.transform_type == "multi_level_header"
        assert config.header_config is not None


class TestPLARUSheetConfigs:
    """Tests for PLARU_SHEET_CONFIGS constant."""


    @pytest.mark.unit
    def test_has_all_sheets(self):
        assert len(PLARU_SHEET_CONFIGS) == 9
        for i in range(9):
            assert i in PLARU_SHEET_CONFIGS

    @pytest.mark.unit
    def test_sheet_0_is_key_value_pivot(self):
        config = PLARU_SHEET_CONFIGS[0]
        assert config.sheet_type == "report_parameters"
        assert config.transform_type == "key_value_pivot"
        assert config.pivot_config is not None

    @pytest.mark.unit
    def test_sheet_1_is_standard(self):
        config = PLARU_SHEET_CONFIGS[1]
        assert config.sheet_type == "payment_history"
        assert config.transform_type == "standard"

    @pytest.mark.unit
    def test_sheet_2_is_multi_level(self):
        config = PLARU_SHEET_CONFIGS[2]
        assert config.sheet_type == "base_pcc_pmt_detailed"
        assert config.transform_type == "multi_level_header"
        assert config.header_config is not None
        assert config.header_config.header_rows == [4, 5]

    @pytest.mark.unit
    def test_sheet_8_is_standard(self):
        config = PLARU_SHEET_CONFIGS[8]
        assert config.sheet_type == "data_claims_prvdr"
        assert config.transform_type == "standard"


class TestCleanPlaruSheet:
    """Tests for clean_plaru_sheet."""


    @pytest.mark.unit
    def test_basic_passthrough(self):
        df = pl.DataFrame({"col1": ["a", "b"], "col2": [1, 2]}).lazy()
        result = clean_plaru_sheet(df, "test_sheet", {})
        collected = result.collect()
        assert collected.height == 2
        assert collected.columns == ["col1", "col2"]

    @pytest.mark.unit
    def test_drop_columns(self):
        df = pl.DataFrame({
            "keep_col": [1, 2],
            "drop_me": ["x", "y"],
            "also_drop": [True, False],
        }).lazy()

        sheet_config = {"drop_columns": ["drop_me", "also_drop"]}
        result = clean_plaru_sheet(df, "test", sheet_config)
        collected = result.collect()
        assert "keep_col" in collected.columns
        assert "drop_me" not in collected.columns
        assert "also_drop" not in collected.columns

    @pytest.mark.unit
    def test_drop_columns_nonexistent_ignored(self):
        df = pl.DataFrame({"col1": [1]}).lazy()
        sheet_config = {"drop_columns": ["nonexistent_col"]}
        result = clean_plaru_sheet(df, "test", sheet_config)
        collected = result.collect()
        assert collected.height == 1

    @pytest.mark.unit
    def test_silver_columns_rename(self):
        df = pl.DataFrame({
            "old_name": ["val1", "val2"],
        }).lazy()

        sheet_config = {
            "silver_columns": [
                {"name": "new_name", "source_name": "old_name", "data_type": "string"},
            ],
        }
        result = clean_plaru_sheet(df, "test", sheet_config)
        collected = result.collect()
        assert "new_name" in collected.columns

    @pytest.mark.unit
    def test_silver_columns_cast_float(self):
        df = pl.DataFrame({
            "amount": ["100.5", "200.0"],
        }).lazy()

        sheet_config = {
            "silver_columns": [
                {"name": "amount", "source_name": "amount", "data_type": "float"},
            ],
        }
        result = clean_plaru_sheet(df, "test", sheet_config)
        collected = result.collect()
        assert collected["amount"].dtype == pl.Float64

    @pytest.mark.unit
    def test_silver_columns_cast_integer(self):
        df = pl.DataFrame({
            "count": ["10", "20"],
        }).lazy()

        sheet_config = {
            "silver_columns": [
                {"name": "count", "source_name": "count", "data_type": "integer"},
            ],
        }
        result = clean_plaru_sheet(df, "test", sheet_config)
        collected = result.collect()
        assert collected["count"].dtype == pl.Int64

    @pytest.mark.unit
    def test_metric_column_filtering(self):
        df = pl.DataFrame({
            "metric": ["Revenue", "", None, "Cost"],
            "value": [100, 0, 0, 200],
        }).lazy()

        result = clean_plaru_sheet(df, "test", {})
        collected = result.collect()
        assert collected.height == 2
        assert "Revenue" in collected["metric"].to_list()
        assert "Cost" in collected["metric"].to_list()

    @pytest.mark.unit
    def test_no_silver_columns(self):
        df = pl.DataFrame({"col": [1, 2]}).lazy()
        sheet_config = {"silver_columns": []}
        result = clean_plaru_sheet(df, "test", sheet_config)
        collected = result.collect()
        assert collected.height == 2


class TestProcessPlaruSheet:
    """Tests for process_plaru_sheet."""


    @pytest.mark.unit
    def test_standard_type(self):
        df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]}).lazy()
        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test", transform_type="standard"
        )
        result = process_plaru_sheet(df, config)
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().height == 2

    @pytest.mark.unit
    def test_key_value_pivot_type(self):
        df = pl.DataFrame({
            "column_1": ["Key1", "Key2", "Key3"],
            "column_2": ["Value1", "Value2", "Value3"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0,
            sheet_type="report_parameters",
            transform_type="key_value_pivot",
            pivot_config=KeyValuePivotConfig(
                key_column="column_1",
                value_column="column_2",
                skip_empty_values=True,
                sanitize_keys=True,
            ),
        )
        result = process_plaru_sheet(df, config)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_key_value_pivot_missing_config_raises(self):
        df = pl.DataFrame({"col": [1]}).lazy()
        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="key_value_pivot",
            pivot_config=None,
        )
        with pytest.raises(ValueError, match="pivot_config required"):
            process_plaru_sheet(df, config)

    @pytest.mark.unit
    def test_multi_level_header_missing_config_raises(self):
        df = pl.DataFrame({"col": [1]}).lazy()
        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="multi_level_header",
            header_config=None,
        )
        with pytest.raises(ValueError, match="header_config required"):
            process_plaru_sheet(df, config)

    @pytest.mark.unit
    def test_matrix_extractor_missing_config_raises(self):
        df = pl.DataFrame({"col": [1]}).lazy()
        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="matrix_extractor",
            matrix_config=None,
        )
        with pytest.raises(ValueError, match="matrix_config required"):
            process_plaru_sheet(df, config)

    @pytest.mark.unit
    def test_dynamic_meta_detect_missing_config_raises(self):
        df = pl.DataFrame({"col": [1]}).lazy()
        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="dynamic_meta_detect",
            meta_config=None,
        )
        with pytest.raises(ValueError, match="meta_config required"):
            process_plaru_sheet(df, config)

    @pytest.mark.unit
    def test_append_detect_missing_config_raises(self):
        df = pl.DataFrame({"col": [1]}).lazy()
        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="append_detect",
            append_config=None,
        )
        with pytest.raises(ValueError, match="append_config required"):
            process_plaru_sheet(df, config)

    @pytest.mark.unit
    def test_unknown_transform_type_raises(self):
        df = pl.DataFrame({"col": [1]}).lazy()
        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="nonexistent_type",
        )
        with pytest.raises(ValueError, match="Unknown transform_type"):
            process_plaru_sheet(df, config)

    @pytest.mark.unit
    def test_standard_with_silver_columns(self):
        df = pl.DataFrame({"old_col": ["abc"]}).lazy()
        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="standard",
            silver_columns=[
                {"name": "new_col", "source_name": "old_col", "data_type": "string"},
            ],
        )
        result = process_plaru_sheet(df, config)
        collected = result.collect()
        assert "new_col" in collected.columns


class TestTransformPlaruWorkbook:
    """Tests for transform_plaru_workbook."""

    # The @transform decorator validates first arg is pl.LazyFrame,
    # but transform_plaru_workbook takes a dict. Use .func to bypass.


    _func = staticmethod(transform_plaru_workbook.func)

    @pytest.mark.unit
    def test_empty_sheets(self):
        result = self._func({})
        assert result == {}

    @pytest.mark.unit
    def test_unconfigured_sheet_skipped(self):
        sheets = {
            99: pl.DataFrame({"col": [1]}).lazy(),
        }
        result = self._func(sheets)
        assert len(result) == 0

    @pytest.mark.unit
    def test_standard_sheet_processed(self):
        sheets = {
            1: pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]}).lazy(),
        }
        configs = {
            1: PLARUSheetConfig(
                sheet_index=1, sheet_type="payment_history",
                transform_type="standard",
            ),
        }
        result = self._func(sheets, configs)
        assert "payment_history" in result
        df = result["payment_history"].collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_multiple_sheets(self):
        sheets = {
            1: pl.DataFrame({"a": [1]}).lazy(),
            8: pl.DataFrame({"b": [2]}).lazy(),
        }
        configs = {
            1: PLARUSheetConfig(
                sheet_index=1, sheet_type="payment_history",
                transform_type="standard",
            ),
            8: PLARUSheetConfig(
                sheet_index=8, sheet_type="data_claims_prvdr",
                transform_type="standard",
            ),
        }
        result = self._func(sheets, configs)
        assert "payment_history" in result
        assert "data_claims_prvdr" in result

    @pytest.mark.unit
    def test_error_in_sheet_fallback_to_original(self, capsys):
        # Use a config that will error (missing pivot_config)
        sheets = {
            0: pl.DataFrame({"col": [1]}).lazy(),
        }
        configs = {
            0: PLARUSheetConfig(
                sheet_index=0, sheet_type="report_parameters",
                transform_type="key_value_pivot",
                pivot_config=None,  # Will cause ValueError
            ),
        }
        result = self._func(sheets, configs)
        # Should fall back to original df
        assert "report_parameters" in result

    @pytest.mark.unit
    def test_default_configs_used(self):
        sheets = {
            1: pl.DataFrame({"col": [1]}).lazy(),
        }
        # Pass None for configs - should use PLARU_SHEET_CONFIGS
        result = self._func(sheets, None)
        assert "payment_history" in result


class TestAutoDetect:
    """Tests for auto-detect layout functionality."""


    @pytest.mark.unit
    def test_auto_detect_with_standard_keeps_standard(self):
        df = pl.DataFrame({
            "col1": ["header1", "data1", "data3"],
            "col2": ["header2", "data2", "data4"],
            "col3": ["header3", "data5", "data6"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="standard",
            auto_detect=True,
        )
        result = process_plaru_sheet(df, config)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_auto_detect_long_key_value_layout(self):
        """Auto-detect should recognise a key-value layout and switch transform_type."""
        # Two-column DataFrame with distinct keys -> should be detected as LONG_KEY_VALUE


        df = pl.DataFrame({
            "column_1": [f"Key{i}" for i in range(15)],
            "column_2": [f"Value{i}" for i in range(15)],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test_auto",
            transform_type="standard",
            auto_detect=True,
        )
        result = process_plaru_sheet(df, config)
        assert isinstance(result, pl.LazyFrame)
        # After auto-detect, config should have been switched
        # The key-value pivot should produce a single row
        collected = result.collect()
        assert collected.height <= 1 or config.transform_type == "key_value_pivot"


class TestCleanPlaruSheetExtended:
    """Additional edge case tests for clean_plaru_sheet."""


    @pytest.mark.unit
    def test_silver_columns_no_source_name_match(self):
        """When source_name doesn't match any column, should not rename."""


        df = pl.DataFrame({"existing_col": ["val"]}).lazy()
        sheet_config = {
            "silver_columns": [
                {"name": "new_name", "source_name": "nonexistent", "data_type": "string"},
            ],
        }
        result = clean_plaru_sheet(df, "test", sheet_config)
        collected = result.collect()
        assert "existing_col" in collected.columns
        assert "new_name" not in collected.columns

    @pytest.mark.unit
    def test_silver_columns_no_data_type(self):
        """When no data_type specified, column should pass through unchanged."""


        df = pl.DataFrame({"col": ["abc"]}).lazy()
        sheet_config = {
            "silver_columns": [
                {"name": "col", "source_name": "col"},
            ],
        }
        result = clean_plaru_sheet(df, "test", sheet_config)
        collected = result.collect()
        assert collected["col"][0] == "abc"

    @pytest.mark.unit
    def test_drop_columns_empty_list(self):
        """Empty drop_columns list should not drop anything."""


        df = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        result = clean_plaru_sheet(df, "test", {"drop_columns": []})
        collected = result.collect()
        assert "a" in collected.columns
        assert "b" in collected.columns

    @pytest.mark.unit
    def test_silver_columns_none(self):
        """None silver_columns should pass through."""


        df = pl.DataFrame({"x": [1]}).lazy()
        result = clean_plaru_sheet(df, "test", {"silver_columns": None})
        collected = result.collect()
        assert collected.height == 1

    @pytest.mark.unit
    def test_cast_string_type(self):
        """Test cast to Utf8 string type."""


        df = pl.DataFrame({"num_col": [123, 456]}).lazy()
        sheet_config = {
            "silver_columns": [
                {"name": "num_col", "source_name": "num_col", "data_type": "string"},
            ],
        }
        result = clean_plaru_sheet(df, "test", sheet_config)
        collected = result.collect()
        assert collected["num_col"].dtype == pl.Utf8


class TestProcessPlaruSheetExtended:
    """Extended tests for process_plaru_sheet covering more branches."""


    @pytest.mark.unit
    def test_multi_level_header_with_tracking_columns(self):
        """Multi-level header with tracking columns should preserve them."""


        df = pl.DataFrame({
            "column_1": ["Parent", "Value1", "Value3"],
            "column_2": ["Child", "Value2", "Value4"],
            "processed_at": ["2024-01-01", "2024-01-01", "2024-01-01"],
            "source_file": ["file.xlsx", "file.xlsx", "file.xlsx"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=2, sheet_type="base_pcc_pmt_detailed",
            transform_type="multi_level_header",
            header_config=MultiLevelHeaderConfig(
                header_rows=[0],
                separator="_",
                skip_empty_parts=True,
                sanitize_names=True,
            ),
            data_start_row=1,
        )
        result = process_plaru_sheet(df, config)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_multi_level_header_without_tracking_columns(self):
        """Multi-level header without any tracking columns."""


        df = pl.DataFrame({
            "column_1": ["Header1", "Data1", "Data3"],
            "column_2": ["Header2", "Data2", "Data4"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=2, sheet_type="test_sheet",
            transform_type="multi_level_header",
            header_config=MultiLevelHeaderConfig(
                header_rows=[0],
                separator="_",
                skip_empty_parts=True,
                sanitize_names=True,
            ),
            data_start_row=1,
        )
        result = process_plaru_sheet(df, config)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_standard_with_drop_columns(self):
        """Standard transform with drop_columns config."""


        df = pl.DataFrame({
            "keep": [1, 2],
            "drop_me": ["a", "b"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="standard",
            drop_columns=["drop_me"],
        )
        result = process_plaru_sheet(df, config)
        collected = result.collect()
        assert "drop_me" not in collected.columns
        assert "keep" in collected.columns


class TestTransformPlaruWorkbookExtended:
    """Extended tests for transform_plaru_workbook."""


    _func = staticmethod(transform_plaru_workbook.func)

    @pytest.mark.unit
    def test_sheet_error_prints_and_falls_back(self, capsys):
        """When a sheet transform errors, it should print error and use original."""
        # Use an invalid config that will cause an error


        sheets = {
            0: pl.DataFrame({"col": [1]}).lazy(),
        }
        configs = {
            0: PLARUSheetConfig(
                sheet_index=0, sheet_type="failed_sheet",
                transform_type="key_value_pivot",
                pivot_config=None,  # Will cause ValueError
            ),
        }
        result = self._func(sheets, configs)
        assert "failed_sheet" in result
        captured = capsys.readouterr()
        assert "Error processing sheet 0" in captured.out

    @pytest.mark.unit
    def test_mixed_success_and_failure(self, capsys):
        """Mix of successful and failing sheet transforms."""


        sheets = {
            0: pl.DataFrame({"col": [1]}).lazy(),
            1: pl.DataFrame({"a": [1, 2]}).lazy(),
        }
        configs = {
            0: PLARUSheetConfig(
                sheet_index=0, sheet_type="will_fail",
                transform_type="key_value_pivot",
                pivot_config=None,
            ),
            1: PLARUSheetConfig(
                sheet_index=1, sheet_type="will_succeed",
                transform_type="standard",
            ),
        }
        result = self._func(sheets, configs)
        assert "will_fail" in result
        assert "will_succeed" in result
        # will_succeed should have been properly transformed
        assert result["will_succeed"].collect().height == 2


class TestProcessPlaruSheetMatrixExtractor:
    """Tests for matrix_extractor transform type (lines 327-363)."""


    _func = staticmethod(process_plaru_sheet.func)

    @pytest.mark.unit
    def test_matrix_extractor_missing_config_raises(self):
        """matrix_extractor without matrix_config raises ValueError."""


        df = pl.DataFrame({"col": [1]}).lazy()
        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test",
            transform_type="matrix_extractor",
            matrix_config=None,
        )
        with pytest.raises(ValueError, match="matrix_config required"):
            self._func(df, config)

    @pytest.mark.unit
    def test_matrix_extractor_with_results(self):
        """matrix_extractor processes data and returns results."""


        df = pl.DataFrame({
            "column_1": ["Header", "A", "B"],
            "column_2": ["Value", "1", "2"],
            "processed_at": ["2024-01-01", "2024-01-01", "2024-01-01"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test_matrix",
            transform_type="matrix_extractor",
            matrix_config=MatrixExtractorConfig(),
        )

        with patch("acoharmony._transforms._plaru.MatrixExtractor") as mock_extractor_cls:
            mock_instance = MagicMock()
            mock_extractor_cls.return_value = mock_instance
            mock_instance.extract.return_value = [
                pl.DataFrame({"field": ["a"], "value": ["1"]}),
            ]

            result = self._func(df, config)
            assert isinstance(result, pl.LazyFrame)
            collected = result.collect()
            assert "field" in collected.columns

    @pytest.mark.unit
    def test_matrix_extractor_empty_results_with_tracking(self):
        """matrix_extractor returns tracking columns when extract returns empty."""


        df = pl.DataFrame({
            "column_1": ["A"],
            "processed_at": ["2024-01-01"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test_matrix",
            transform_type="matrix_extractor",
            matrix_config=MatrixExtractorConfig(),
        )

        with patch("acoharmony._transforms._plaru.MatrixExtractor") as mock_cls:
            mock_cls.return_value.extract.return_value = []

            result = self._func(df, config)
            assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_matrix_extractor_empty_results_no_tracking(self):
        """matrix_extractor returns empty DataFrame when no tracking and no results."""


        df = pl.DataFrame({
            "column_1": ["A"],
            "column_2": ["B"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test_matrix",
            transform_type="matrix_extractor",
            matrix_config=MatrixExtractorConfig(),
        )

        with patch("acoharmony._transforms._plaru.MatrixExtractor") as mock_cls:
            mock_cls.return_value.extract.return_value = []

            result = self._func(df, config)
            assert isinstance(result, pl.LazyFrame)
            assert result.collect().height == 0

    @pytest.mark.unit
    def test_matrix_extractor_with_tracking_no_results(self):
        """matrix_extractor with tracking columns but no data columns returns tracking."""


        df = pl.DataFrame({
            "data_col": ["X"],
            "source_file": ["/path/file.xlsx"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test_matrix",
            transform_type="matrix_extractor",
            matrix_config=MatrixExtractorConfig(),
        )

        with patch("acoharmony._transforms._plaru.MatrixExtractor") as mock_cls:
            mock_cls.return_value.extract.return_value = []

            result = self._func(df, config)
            assert isinstance(result, pl.LazyFrame)


class TestProcessPlaruSheetDynamicMetaDetect:
    """Tests for dynamic_meta_detect transform type (lines 369-370)."""


    _func = staticmethod(process_plaru_sheet.func)

    @pytest.mark.unit
    def test_dynamic_meta_detect_success(self):
        """dynamic_meta_detect processes data and returns result."""


        df = pl.DataFrame({
            "column_1": ["Header", "Data1"],
            "column_2": ["Value", "Data2"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test_dmd",
            transform_type="dynamic_meta_detect",
            meta_config=DynamicMetaConfig(header_rows=[0]),
        )

        result_lf = pl.DataFrame({"parsed": ["data"]}).lazy()

        with patch("acoharmony._transforms._plaru.DynamicMetaDetectExpression") as mock_dmd:
            mock_dmd.apply.return_value = (result_lf, {})

            result = self._func(df, config)
            assert isinstance(result, pl.LazyFrame)
            assert result.collect().height == 1


class TestProcessPlaruSheetAppendDetect:
    """Tests for append_detect transform type (lines 375-377)."""


    _func = staticmethod(process_plaru_sheet.func)

    @pytest.mark.unit
    def test_append_detect_success(self):
        """append_detect processes data and returns result."""


        df = pl.DataFrame({
            "column_1": ["A", "B"],
            "column_2": ["1", "2"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test_append",
            transform_type="append_detect",
            append_config=AppendDetectConfig(),
        )

        with patch("acoharmony._transforms._plaru.AppendDetectExpression") as mock_ade:
            mock_ade.apply.return_value = {}

            result = self._func(df, config)
            assert isinstance(result, pl.LazyFrame)
            assert result.collect().height == 2


class TestAutoDetectMultiLevelHeader:
    """Tests for auto_detect detecting multi_level_header layout (line 266)."""


    _func = staticmethod(process_plaru_sheet.func)

    @pytest.mark.unit
    def test_auto_detect_multi_level_header(self):
        """auto_detect changes transform_type to multi_level_header when detected."""

        # Create a DataFrame that looks like it has multi-level headers


        df = pl.DataFrame({
            "column_1": ["Parent", "Child1", "Data1", "Data2"],
            "column_2": ["Parent", "Child2", "Data3", "Data4"],
            "column_3": ["Parent", "Child3", "Data5", "Data6"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0, sheet_type="test_auto",
            transform_type="standard",
            auto_detect=True,
        )

        with patch("acoharmony._transforms._plaru.TableLayoutDetector") as mock_detector:
            mock_detector.detect.return_value = TableLayout.MULTI_LEVEL_HEADER

            # multi_level_header needs header_config, which is None, so it should raise
            with pytest.raises(ValueError, match="header_config required"):
                self._func(df, config)


class TestCleanPlaruSheetEmptyCastExpressions:
    """Cover branch 163->168: cast_expressions is empty."""

    @pytest.mark.unit
    def test_empty_cast_expressions_skips_select(self):
        """When silver_columns is non-empty but DataFrame has no columns left,
        cast_expressions is empty and the select is skipped (branch 163->168)."""
        # DataFrame with all columns dropped, but silver_columns is still non-empty
        df = pl.DataFrame().lazy()
        sheet_config = {
            "silver_columns": [
                {"name": "target", "source_name": "nonexistent", "data_type": "string"},
            ],
        }
        result = clean_plaru_sheet(df, "test_sheet", sheet_config)
        collected = result.collect()
        assert collected.height == 0
        assert len(collected.columns) == 0


def _unwrap_plaru_func(fn):
    """Unwrap all decorators to get the original function from _plaru.py."""
    while hasattr(fn, '__wrapped__'):
        fn = fn.__wrapped__
    return fn


class TestMultiLevelHeaderTrackingLengthEqual:
    """Cover branch 303->305: tracking length == transformed length (false branch)."""

    _orig = staticmethod(_unwrap_plaru_func(process_plaru_sheet))

    @pytest.mark.unit
    def test_tracking_not_sliced_when_lengths_equal(self):
        """When MultiLevelHeaderExpression.apply returns same number of rows as tracking,
        the slice is skipped and tracking is used as-is (branch 303->305 false)."""
        df = pl.DataFrame({
            "column_1": ["Header", "Data1", "Data2"],
            "column_2": ["SubHeader", "Val1", "Val2"],
            "processed_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=2,
            sheet_type="test_mlh",
            transform_type="multi_level_header",
            header_config=MultiLevelHeaderConfig(
                header_rows=[0],
                separator="_",
                skip_empty_parts=True,
                sanitize_names=True,
            ),
            data_start_row=1,
        )

        # Mock returns same number of rows (3) as tracking
        transformed_df = pl.DataFrame({
            "header_data1": ["v1", "v2", "v3"]
        }).lazy()

        with patch("acoharmony._transforms._plaru.MultiLevelHeaderExpression") as mock_mlh:
            mock_mlh.apply.return_value = transformed_df
            result = self._orig(df, config)
            assert isinstance(result, pl.LazyFrame)
            collected = result.collect()
            # All 3 rows preserved since lengths match
            assert collected.height == 3
            assert "processed_at" in collected.columns
            assert "header_data1" in collected.columns


class TestMatrixExtractorResultsNoTracking:
    """Cover branch 352->358: matrix_extractor has results but NO tracking columns."""

    _orig = staticmethod(_unwrap_plaru_func(process_plaru_sheet))

    @pytest.mark.unit
    def test_matrix_results_without_tracking_columns(self):
        """When matrix extractor returns results but no tracking columns exist,
        the tracking merge is skipped (branch 352->358 false)."""
        df = pl.DataFrame({
            "column_1": ["A", "B", "C"],
            "column_2": ["1", "2", "3"],
        }).lazy()

        config = PLARUSheetConfig(
            sheet_index=0,
            sheet_type="test_matrix_no_track",
            transform_type="matrix_extractor",
            matrix_config=MatrixExtractorConfig(),
        )

        with patch("acoharmony._transforms._plaru.MatrixExtractor") as mock_cls:
            mock_instance = MagicMock()
            mock_cls.return_value = mock_instance
            mock_instance.extract.return_value = [
                pl.DataFrame({"metric": ["m1"], "value": ["v1"]}),
                pl.DataFrame({"metric": ["m2"], "value": ["v2"]}),
            ]

            result = self._orig(df, config)
            assert isinstance(result, pl.LazyFrame)
            collected = result.collect()
            # 2 result rows, no tracking columns added
            assert collected.height == 2
            assert "metric" in collected.columns
            assert "processed_at" not in collected.columns
            assert "source_file" not in collected.columns
