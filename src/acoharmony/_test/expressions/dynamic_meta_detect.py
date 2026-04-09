# © 2025 HarmonyCares
# All rights reserved.





# =============================================================================
# Tests for dynamic_meta_detect
# =============================================================================













# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest


class TestDynamicMetaDetect:
    """Test cases for expression builders."""

    @pytest.mark.unit
    def test_pattern_detection_disabled(self):
        """When detect_patterns is False, uses UNKNOWN pattern (line 401)."""
        config = DynamicMetaConfig(header_rows=[0], detect_patterns=False)
        df = pl.DataFrame({"col_a": ["val1"], "col_b": ["val2"]})

        result = DynamicMetaDetectExpression.extract_dynamic_headers(df, config)
        assert isinstance(result, dict)

    # ------------------------------------------------------------------
    # detect_pattern: custom_patterns branch (227->228, 228->229, 229->228, 229->230)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_detect_pattern_custom_match(self):
        """Custom pattern matched returns CUSTOM."""
        customs = {"region": ["North", "South"]}
        assert DynamicMetaDetectExpression.detect_pattern("North", customs) == HeaderPattern.CUSTOM

    @pytest.mark.unit
    def test_detect_pattern_custom_no_match(self):
        """Custom patterns provided but value not in any list falls through (227->228, 228->233)."""
        customs = {"region": ["North", "South"]}
        # "Jan" is not in custom patterns, falls through to MONTH_ABBR
        assert DynamicMetaDetectExpression.detect_pattern("Jan", customs) == HeaderPattern.MONTH_ABBR

    @pytest.mark.unit
    def test_detect_pattern_custom_multiple_patterns_no_match(self):
        """Multiple custom pattern lists, none match, iterates all (229->228)."""
        customs = {"region": ["North"], "color": ["Red"]}
        result = DynamicMetaDetectExpression.detect_pattern("Blue", customs)
        assert result == HeaderPattern.UNKNOWN

    # ------------------------------------------------------------------
    # detect_pattern: no custom_patterns (227->233)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_detect_pattern_no_custom_patterns(self):
        """No custom patterns, goes directly to month check."""
        assert DynamicMetaDetectExpression.detect_pattern("Jan", None) == HeaderPattern.MONTH_ABBR

    # ------------------------------------------------------------------
    # detect_pattern: MONTH_ABBR (233->234)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_detect_pattern_month_abbr(self):
        assert DynamicMetaDetectExpression.detect_pattern("Feb") == HeaderPattern.MONTH_ABBR

    # ------------------------------------------------------------------
    # detect_pattern: MONTH_FULL (237->238)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_detect_pattern_month_full(self):
        assert DynamicMetaDetectExpression.detect_pattern("January") == HeaderPattern.MONTH_FULL

    # ------------------------------------------------------------------
    # detect_pattern: MONTH_NUM (241->242)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_detect_pattern_month_num(self):
        assert DynamicMetaDetectExpression.detect_pattern("03") == HeaderPattern.MONTH_NUM

    @pytest.mark.unit
    def test_detect_pattern_month_num_single_digit(self):
        assert DynamicMetaDetectExpression.detect_pattern("7") == HeaderPattern.MONTH_NUM

    # ------------------------------------------------------------------
    # detect_pattern: QUARTER (245->251)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_detect_pattern_quarter_upper(self):
        assert DynamicMetaDetectExpression.detect_pattern("Q1") == HeaderPattern.QUARTER

    @pytest.mark.unit
    def test_detect_pattern_quarter_lowercase(self):
        """Lowercase q1 triggers .upper() match (245->251 second condition)."""
        assert DynamicMetaDetectExpression.detect_pattern("q2") == HeaderPattern.QUARTER

    @pytest.mark.unit
    def test_detect_pattern_quarter_full(self):
        assert DynamicMetaDetectExpression.detect_pattern("Quarter 3") == HeaderPattern.QUARTER

    # ------------------------------------------------------------------
    # detect_pattern: YEAR_FULL (254->255)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_detect_pattern_year_full(self):
        assert DynamicMetaDetectExpression.detect_pattern("2024") == HeaderPattern.YEAR_FULL

    # ------------------------------------------------------------------
    # detect_pattern: YEAR_SHORT (258->259) and UNKNOWN (258->261)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_detect_pattern_year_short(self):
        assert DynamicMetaDetectExpression.detect_pattern("24") == HeaderPattern.YEAR_SHORT

    @pytest.mark.unit
    def test_detect_pattern_unknown(self):
        """Non-matching string returns UNKNOWN (258->261)."""
        assert DynamicMetaDetectExpression.detect_pattern("SomeRandomText") == HeaderPattern.UNKNOWN

    # ------------------------------------------------------------------
    # extract_metadata: MONTH_ABBR (284->285)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_metadata_month_abbr(self):
        meta = DynamicMetaDetectExpression.extract_metadata("Mar", HeaderPattern.MONTH_ABBR)
        assert meta["month"] == 3
        assert meta["month_name"] == "March"
        assert meta["month_abbr"] == "Mar"

    # ------------------------------------------------------------------
    # extract_metadata: MONTH_FULL (290->291)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_metadata_month_full(self):
        meta = DynamicMetaDetectExpression.extract_metadata("February", HeaderPattern.MONTH_FULL)
        assert meta["month"] == 2
        assert meta["month_name"] == "February"
        assert meta["month_abbr"] == "Feb"

    # ------------------------------------------------------------------
    # extract_metadata: MONTH_NUM (296->297)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_metadata_month_num(self):
        meta = DynamicMetaDetectExpression.extract_metadata("05", HeaderPattern.MONTH_NUM)
        assert meta["month"] == 5
        assert meta["month_name"] == "May"
        assert meta["month_abbr"] == "May"

    # ------------------------------------------------------------------
    # extract_metadata: QUARTER Q-format (303->304)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_metadata_quarter_q_format(self):
        meta = DynamicMetaDetectExpression.extract_metadata("Q3", HeaderPattern.QUARTER)
        assert meta["quarter"] == 3

    # ------------------------------------------------------------------
    # extract_metadata: QUARTER "Quarter N" format (303->307)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_metadata_quarter_full_format(self):
        meta = DynamicMetaDetectExpression.extract_metadata("Quarter 2", HeaderPattern.QUARTER)
        assert meta["quarter"] == 2

    # ------------------------------------------------------------------
    # extract_metadata: YEAR_FULL (310->311)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_metadata_year_full(self):
        meta = DynamicMetaDetectExpression.extract_metadata("2025", HeaderPattern.YEAR_FULL)
        assert meta["year"] == 2025

    # ------------------------------------------------------------------
    # extract_metadata: YEAR_SHORT <=50 (316->317)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_metadata_year_short_low(self):
        meta = DynamicMetaDetectExpression.extract_metadata("25", HeaderPattern.YEAR_SHORT)
        assert meta["year"] == 2025

    # ------------------------------------------------------------------
    # extract_metadata: YEAR_SHORT >50 (316->319)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_metadata_year_short_high(self):
        meta = DynamicMetaDetectExpression.extract_metadata("99", HeaderPattern.YEAR_SHORT)
        assert meta["year"] == 1999

    # ------------------------------------------------------------------
    # extract_dynamic_headers: forward_fill + detect_patterns + sanitize (377->380, 396->397, 418->423)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_dynamic_headers_full_pipeline(self):
        """Multi-level headers with forward fill, detection, and sanitize enabled."""
        df = pl.DataFrame({
            "c0": ["2024", "Q1", "Amount", "100"],
            "c1": ["", "Q2", "Amount", "200"],
            "c2": ["2025", "Q1", "Count", "50"],
        })
        config = DynamicMetaConfig(
            header_rows=[0, 1, 2],
            forward_fill_sparse=True,
            detect_patterns=True,
            sanitize_names=True,
        )
        result = DynamicMetaDetectExpression.extract_dynamic_headers(df, config)
        # c0 -> 2024_q1_amount (sanitized; leading digit gets col_ prefix)
        assert result["c0"].column_name == "col_2024_q1_amount"
        assert result["c0"].metadata.get("year") == 2024
        assert result["c0"].metadata.get("quarter") == 1
        # c1 -> forward-filled "2024" + "Q2" + "Amount"
        assert result["c1"].column_name == "col_2024_q2_amount"
        # c2 -> 2025_q1_count
        assert result["c2"].column_name == "col_2025_q1_count"

    # ------------------------------------------------------------------
    # apply: data_start_row=None (466->467) and data_start_row provided (466->469)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_apply_data_start_row_none(self):
        """data_start_row defaults to max(header_rows) + 1 (466->467)."""
        df = pl.DataFrame({
            "c0": ["Jan", "Amount", "10", "20"],
            "c1": ["Feb", "Amount", "30", "40"],
        })
        config = DynamicMetaConfig(header_rows=[0, 1])
        result_df, metadata = DynamicMetaDetectExpression.apply(df.lazy(), config)
        collected = result_df.collect()
        # Should start from row 2 (max([0,1]) + 1 = 2)
        assert collected.shape[0] == 2
        assert "jan_amount" in collected.columns

    @pytest.mark.unit
    def test_apply_data_start_row_provided(self):
        """data_start_row explicitly set (466->469)."""
        df = pl.DataFrame({
            "c0": ["Jan", "Amount", "skip", "10", "20"],
            "c1": ["Feb", "Amount", "skip", "30", "40"],
        })
        config = DynamicMetaConfig(header_rows=[0, 1])
        result_df, metadata = DynamicMetaDetectExpression.apply(df.lazy(), config, data_start_row=3)
        collected = result_df.collect()
        assert collected.shape[0] == 2

    # ------------------------------------------------------------------
    # detect_pattern: 13-digit number not matching month_num (241->245)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_detect_pattern_large_digit_not_month(self):
        """A digit value >12 but <=2 chars should not be month_num. 13 is 2 digits but >12."""
        result = DynamicMetaDetectExpression.detect_pattern("13")
        # 13 is digit, len==2, but 13>12 so not MONTH_NUM; goes to YEAR_SHORT
        assert result == HeaderPattern.YEAR_SHORT







    # ------------------------------------------------------------------
    # extract_dynamic_headers: forward_fill_sparse=False (377->380 false branch)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_dynamic_headers_no_forward_fill(self):
        """forward_fill_sparse=False skips forward-fill (377->380 false branch)."""
        df = pl.DataFrame({
            "c0": ["2024", "Q1", "100"],
            "c1": ["", "Q2", "200"],
        })
        config = DynamicMetaConfig(
            header_rows=[0, 1],
            forward_fill_sparse=False,
            detect_patterns=True,
            sanitize_names=True,
        )
        result = DynamicMetaDetectExpression.extract_dynamic_headers(df, config)
        # c1 should NOT have forward-filled "2024"; empty string means only Q2 used
        assert "q2" in result["c1"].column_name

    # ------------------------------------------------------------------
    # extract_dynamic_headers: sanitize_names=False (418->423 false branch)
    # ------------------------------------------------------------------
    @pytest.mark.unit
    def test_extract_dynamic_headers_no_sanitize(self):
        """sanitize_names=False skips sanitization (418->423 false branch)."""
        df = pl.DataFrame({
            "c0": ["Jan", "Amount", "100"],
        })
        config = DynamicMetaConfig(
            header_rows=[0, 1],
            forward_fill_sparse=True,
            detect_patterns=True,
            sanitize_names=False,
        )
        result = DynamicMetaDetectExpression.extract_dynamic_headers(df, config)
        # Without sanitization, the name keeps original casing
        assert result["c0"].column_name == "Jan_Amount"

    @pytest.mark.unit
    def test_empty_header_uses_col_name_fallback(self):
        """When header parts are empty, uses column name as fallback (line 416)."""
        config = DynamicMetaConfig(header_rows=[0])
        df = pl.DataFrame({"col_a": [None], "col_b": [None]})

        result = DynamicMetaDetectExpression.extract_dynamic_headers(df, config)
        assert isinstance(result, dict)


        # ===================== Coverage gap: _append_detect.py line 280 =====================



















