from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._expressions.reach_bnmr import ReachBNMRMultiTableExpression


class TestSheetToTable:
    """Test SHEET_TO_TABLE mapping."""

    @pytest.mark.unit
    def test_has_expected_keys(self):
        mapping = ReachBNMRMultiTableExpression.SHEET_TO_TABLE
        assert 'financial_settlement' in mapping
        assert 'claims' in mapping
        assert 'risk' in mapping
        assert 'county' in mapping
        assert 'uspcc' in mapping
        assert 'heba' in mapping
        assert 'cap' in mapping
        assert 'stop_loss_county' in mapping
        assert 'stop_loss_claims' in mapping
        assert 'stop_loss_payout' in mapping

    @pytest.mark.unit
    def test_multiple_types_map_to_same_table(self):
        """financial_settlement and claims map to same table."""
        mapping = ReachBNMRMultiTableExpression.SHEET_TO_TABLE
        assert mapping['financial_settlement'] == mapping['claims']
        assert mapping['riskscore_ad'] == mapping['risk']


import polars as pl

from acoharmony._expressions._matrix_extractor import (
    DetectionConfig,
    LabelOffset,
    LabelOffsetsConfig,
    MatrixExtractor,
    MatrixExtractorConfig,
    MatrixRegion,
    NamingConfig,
    SectionContext,
    SectionDetectionConfig,
)


class TestMatrixExtractorInit:
    """Cover __init__ and __post_init__ lines 354-387."""

    @pytest.mark.unit
    def test_init_with_config(self):
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        assert extractor.config is config

    @pytest.mark.unit
    def test_matrix_region_default_section_contexts(self):
        region = MatrixRegion(
            start_row=0, end_row=5, start_col=0, end_col=3,
            data=pl.DataFrame({"a": [1, 2], "b": [3, 4]}),
        )
        assert region.section_contexts == {}


class TestDetectSectionHierarchy:
    """Cover detect_section_hierarchy lines 418-472."""

    @pytest.mark.unit
    def test_detects_parent_and_child_sections(self):
        config = MatrixExtractorConfig(
            detection=DetectionConfig(
                section_detection=SectionDetectionConfig(
                    section_column=0,
                    section_markers=["Calculation"],
                    aggregation_markers=["Total"],
                ),
            ),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "label": [
                "Prospective Payment Calculation",  # parent
                "Eligible Months",                    # section header (after parent)
                "AD Claims Aligned",                  # data row
                "ESRD Claims Aligned",                # data row
                "Total Claims",                       # aggregation
                None,                                 # break
                "Benchmark PBPM",                     # new section (after empty)
                "AD Benchmark",                       # data row
            ],
            "val": [None, None, 100, 200, 300, None, None, 50],
        })

        contexts = extractor.detect_section_hierarchy(
            df, config.detection.section_detection
        )

        assert len(contexts) > 0
        # Row 0 should be parent section header
        assert contexts[0].is_section_header is True
        assert contexts[0].parent_section == "Prospective Payment Calculation"
        # Row 2 should be data
        assert 2 in contexts
        assert contexts[2].is_section_header is False
        assert contexts[2].row_label == "AD Claims Aligned"


class TestIsSectionHeader:
    """Cover _is_section_header lines 487-522."""

    @pytest.mark.unit
    def test_subcategory_prefix_not_header(self):
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        # "AD " prefix → not a section header
        result = extractor._is_section_header(
            "AD Claims Aligned", 2, [None, "Header", "AD Claims Aligned"], cfg
        )
        assert result is False

    @pytest.mark.unit
    def test_empty_prev_row_is_header(self):
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        # Previous row empty → section header
        result = extractor._is_section_header(
            "Benchmark PBPM", 2, ["data", None, "Benchmark PBPM"], cfg
        )
        assert result is True

    @pytest.mark.unit
    def test_section_marker_is_header(self):
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0, section_markers=["Benchmark"])

        result = extractor._is_section_header("Benchmark PBPM", 0, ["Benchmark PBPM"], cfg)
        assert result is True


class TestIsParentSection:
    """Cover _is_parent_section lines 537-538."""

    @pytest.mark.unit
    def test_parent_keywords(self):
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        assert extractor._is_parent_section("Prospective Payment Calculation", cfg) is True
        assert extractor._is_parent_section("Eligible Months", cfg) is False


class TestDetectMatrices:
    """Cover detect_matrices lines 562-647."""

    @pytest.mark.unit
    def test_detects_single_matrix(self):
        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=2, min_cols=2),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "label": ["Header", "Row1", "Row2"],
            "jan": [None, 100, 200],
            "feb": [None, 150, 250],
        })

        matrices = extractor.detect_matrices(df)
        assert len(matrices) >= 1
        assert matrices[0].data.height >= 2

    @pytest.mark.unit
    def test_skips_metadata_columns(self):
        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=1),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "val": [1, 2],
            "processed_at": ["2024-01-01", "2024-01-02"],
            "source_file": ["/path/a", "/path/b"],
        })

        matrices = extractor.detect_matrices(df)
        # Only "val" column should be detected, metadata skipped
        for m in matrices:
            assert "processed_at" not in m.data.columns


class TestExtractLabels:
    """Cover extract_labels lines 677-732."""

    @pytest.mark.unit
    def test_column_labels(self):
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "c0": ["Header A", "data1"],
            "c1": ["Header B", "data2"],
        })

        region = MatrixRegion(
            start_row=1, end_row=1, start_col=0, end_col=1,
            data=df.slice(1, 1),
        )

        offsets = [LabelOffset(offset=-1)]
        labels = extractor.extract_labels(df, region, offsets, axis="col")

        assert 0 in labels
        assert "Header A" in labels[0]

    @pytest.mark.unit
    def test_row_labels_from_section_context(self):
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({"c0": ["val1"], "c1": ["val2"]})

        region = MatrixRegion(
            start_row=5, end_row=5, start_col=0, end_col=1,
            data=df,
            section_contexts={
                0: SectionContext(
                    parent_section="Payment",
                    current_section="Monthly",
                    row_label="Jan",
                    is_section_header=False,
                    is_aggregation=False,
                ),
            },
        )

        labels = extractor.extract_labels(df, region, [], axis="row")
        assert 5 in labels
        assert "Payment" in labels[5]
        assert "Monthly" in labels[5]
        assert "Jan" in labels[5]


class TestForwardFillLabels:
    """Cover _forward_fill_labels lines 748-759."""

    @pytest.mark.unit
    def test_fills_empty_from_previous(self):
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        labels = {0: ["A"], 1: [], 2: ["B"], 3: []}
        extractor._forward_fill_labels(labels, axis="col")
        assert labels[1] == ["A"]
        assert labels[3] == ["B"]


class TestCombineLabels:
    """Cover combine_labels lines 779-806."""

    @pytest.mark.unit
    def test_combines_row_and_col_labels(self):
        config = MatrixExtractorConfig(
            naming=NamingConfig(separator="_", order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        row_labels = {10: ["Payment", "Monthly"]}
        col_labels = {3: ["Jan"]}

        result = extractor.combine_labels(row_labels, col_labels)
        assert (10, 3) in result
        assert "payment" in result[(10, 3)]
        assert "jan" in result[(10, 3)]


class TestSanitizeFieldName:
    """Cover _sanitize_field_name lines 822-843."""

    @pytest.mark.unit
    def test_sanitizes_name(self):
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        assert extractor._sanitize_field_name("Hello World!") == "hello_world"
        assert extractor._sanitize_field_name("  spaces  ") == "spaces"
        assert extractor._sanitize_field_name("123start") == "col_123start"
        assert extractor._sanitize_field_name("a--b__c") == "a_b_c"


class TestExtractFull:
    """Cover extract lines 859-913."""

    @pytest.mark.unit
    def test_extract_returns_dataframes(self):
        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
            label_offsets=LabelOffsetsConfig(
                column_labels=[LabelOffset(offset=-1)],
                row_labels=[],
            ),
            naming=NamingConfig(separator="_", order=["col"]),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "label": ["", "Row1", "Row2"],
            "jan": ["Jan", "100", "200"],
            "feb": ["Feb", "150", "250"],
        })

        results = extractor.extract(df)
        assert isinstance(results, list)
        # Should have at least one result matrix
        if results:
            assert "field_name" in results[0].columns
            assert "value" in results[0].columns


class TestMatrixExtractorNoRecords:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_matrix_extractor_no_records(self):
        """909->865: records is empty."""
        from acoharmony._expressions._matrix_extractor import MatrixExtractor, MatrixExtractorConfig, DetectionConfig
        config = MatrixExtractorConfig(detection=DetectionConfig(min_rows=100, min_cols=100))
        ext = MatrixExtractor(config)
        df = pl.DataFrame({"a": [None], "b": [None]})
        results = ext.extract(df)
        assert results == []


class TestIsSectionHeaderTotalWithSubcategoryNext:
    """Cover branches 501->507 and 503->506: 'Total X' followed by subcategory prefix."""

    @pytest.mark.unit
    def test_total_row_with_ad_next_is_header(self):
        """501->507, 503->506: 'Total Foo' where next row starts with 'AD ' => True."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Something", "Total Eligible", "AD Claims Aligned"]
        result = extractor._is_section_header("Total Eligible", 1, all_values, cfg)
        assert result is True

    @pytest.mark.unit
    def test_total_row_with_esrd_next_is_header(self):
        """503->506: 'Total X' where next row starts with 'ESRD ' => True."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Total Claims", "ESRD Data"]
        result = extractor._is_section_header("Total Claims", 0, all_values, cfg)
        assert result is True

    @pytest.mark.unit
    def test_total_row_with_part_next_is_header(self):
        """503->506: 'Total X' where next row starts with 'Part ' => True."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Total Sections", "Part A Data"]
        result = extractor._is_section_header("Total Sections", 0, all_values, cfg)
        assert result is True

    @pytest.mark.unit
    def test_total_row_without_subcategory_next_is_not_header(self):
        """501->507: 'Total X' where next row is not subcategory => False (falls to return False)."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Total Claims", "Regular Row"]
        result = extractor._is_section_header("Total Claims", 0, all_values, cfg)
        assert result is False

    @pytest.mark.unit
    def test_total_row_at_end_of_values(self):
        """501->507: 'Total X' at end of list (no next row) => False."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Total Claims"]
        result = extractor._is_section_header("Total Claims", 0, all_values, cfg)
        assert result is False


class TestIsSectionHeaderPrevRowNotEmpty:
    """Cover branches 510->516, 516->522, 518->522: prev row NOT empty, various next-row cases."""

    @pytest.mark.unit
    def test_prev_not_empty_no_next_row(self):
        """510->516, 516->522: prev row has data, no next row => default True (line 522)."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        # No subcategory prefix, prev row is NOT empty, no next row
        all_values = ["Some Data", "Benchmark PBPM"]
        result = extractor._is_section_header("Benchmark PBPM", 1, all_values, cfg)
        assert result is True

    @pytest.mark.unit
    def test_prev_not_empty_next_not_subcategory(self):
        """510->516, 518->522: prev row has data, next row not subcategory => default True."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Some Data", "Benchmark PBPM", "Regular Row"]
        result = extractor._is_section_header("Benchmark PBPM", 1, all_values, cfg)
        assert result is True

    @pytest.mark.unit
    def test_prev_not_empty_next_is_subcategory(self):
        """510->516, 516->519: prev row has data, next row is subcategory => True."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Some Data", "Benchmark PBPM", "AD Claims"]
        result = extractor._is_section_header("Benchmark PBPM", 1, all_values, cfg)
        assert result is True

    @pytest.mark.unit
    def test_prev_not_empty_next_is_none(self):
        """518->522: prev row has data, next row is None => default True."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Some Data", "Benchmark PBPM", None]
        result = extractor._is_section_header("Benchmark PBPM", 1, all_values, cfg)
        assert result is True


class TestDetectMatricesWithSectionDetection:
    """Cover branch 568->569: section detection enabled in detect_matrices."""

    @pytest.mark.unit
    def test_detect_matrices_with_section_detection(self):
        """568->569: section_detection is not None => call detect_section_hierarchy."""
        config = MatrixExtractorConfig(
            detection=DetectionConfig(
                min_rows=1,
                min_cols=2,
                section_detection=SectionDetectionConfig(
                    section_column=0,
                    section_markers=["Header"],
                    aggregation_markers=["Total"],
                ),
            ),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "label": ["Header Section", "AD Data Row", "Regular Row"],
            "jan": [None, 100, 200],
            "feb": [None, 150, 250],
        })

        matrices = extractor.detect_matrices(df)
        # Should still detect matrices; section contexts should be populated
        assert len(matrices) >= 1
        # The section hierarchy should have been detected
        assert isinstance(matrices[0].section_contexts, dict)


class TestDetectMatricesEmptyColumnBreak:
    """Cover branch 597->598: empty column triggers group flush when group >= min_cols."""

    @pytest.mark.unit
    def test_empty_column_separates_groups(self):
        """597->598: empty column where current_group >= min_cols => col_groups.append."""
        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
        )
        extractor = MatrixExtractor(config)

        # Two data columns, then an empty column, then two more data columns
        df = pl.DataFrame({
            "a": [1, 2],
            "b": [3, 4],
            "empty": [None, None],
            "c": [5, 6],
            "d": [7, 8],
        })

        matrices = extractor.detect_matrices(df)
        # Should detect two separate matrix groups
        assert len(matrices) == 2


class TestDetectMatricesTooFewRows:
    """Cover branch 621->622: data_rows < min_rows => skip col group."""

    @pytest.mark.unit
    def test_col_group_too_few_rows_skipped(self):
        """621->622: a col group has fewer data rows than min_rows => continue."""
        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=5, min_cols=2),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "a": [1, 2],
            "b": [3, 4],
        })

        matrices = extractor.detect_matrices(df)
        assert len(matrices) == 0


class TestExtractLabelsColValueEdgeCases:
    """Cover branches 695->685 and 697->685: value not None but empty after strip, and value_str truthy."""

    @pytest.mark.unit
    def test_col_label_value_whitespace_only(self):
        """695->685: value is not None but strip() is empty string => don't append."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "c0": ["   ", "data1"],
            "c1": ["Header B", "data2"],
        })

        region = MatrixRegion(
            start_row=1, end_row=1, start_col=0, end_col=1,
            data=df.slice(1, 1),
        )

        offsets = [LabelOffset(offset=-1)]
        labels = extractor.extract_labels(df, region, offsets, axis="col")

        # c0 at row 0 is whitespace-only => no label appended
        assert labels[0] == []
        # c1 at row 0 has a real value
        assert labels[1] == ["Header B"]

    @pytest.mark.unit
    def test_col_label_value_present(self):
        """697->685: value is not None and not empty => append and continue loop."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "c0": ["Parent", "Child", "data1"],
            "c1": ["ParentB", "ChildB", "data2"],
        })

        region = MatrixRegion(
            start_row=2, end_row=2, start_col=0, end_col=1,
            data=df.slice(2, 1),
        )

        offsets = [LabelOffset(offset=-2), LabelOffset(offset=-1)]
        labels = extractor.extract_labels(df, region, offsets, axis="col")

        assert labels[0] == ["Parent", "Child"]
        assert labels[1] == ["ParentB", "ChildB"]


class TestExtractLabelsColForwardFill:
    """Cover branch 704->705: forward_fill is True => call _forward_fill_labels."""

    @pytest.mark.unit
    def test_col_labels_forward_fill_applied(self):
        """704->705: offset_cfg.forward_fill is True => _forward_fill_labels called."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "c0": ["Spanning", "data1"],
            "c1": [None, "data2"],
            "c2": [None, "data3"],
        })

        region = MatrixRegion(
            start_row=1, end_row=1, start_col=0, end_col=2,
            data=df.slice(1, 1),
        )

        offsets = [LabelOffset(offset=-1, forward_fill=True)]
        labels = extractor.extract_labels(df, region, offsets, axis="col")

        # c0 has "Spanning", c1 and c2 are empty => forward-filled
        assert labels[0] == ["Spanning"]
        assert labels[1] == ["Spanning"]
        assert labels[2] == ["Spanning"]


class TestExtractLabelsRowSectionHeaderSkipped:
    """Cover branch 719->720: section header row is skipped in row label extraction."""

    @pytest.mark.unit
    def test_row_labels_skip_section_header(self):
        """719->720: context.is_section_header => continue (skip this row)."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({"c0": ["hdr", "val1"], "c1": ["hdr2", "val2"]})

        region = MatrixRegion(
            start_row=0, end_row=1, start_col=0, end_col=1,
            data=df,
            section_contexts={
                0: SectionContext(
                    parent_section="Parent",
                    current_section="Section",
                    row_label="Header Row",
                    is_section_header=True,
                    is_aggregation=False,
                ),
                1: SectionContext(
                    parent_section="Parent",
                    current_section="Section",
                    row_label="Data Row",
                    is_section_header=False,
                    is_aggregation=False,
                ),
            },
        )

        labels = extractor.extract_labels(df, region, [], axis="row")

        # Row 0 (section header) should be skipped entirely
        assert 0 not in labels
        # Row 1 (data row) should be present
        assert 1 in labels
        assert "Data Row" in labels[1]


class TestExtractLabelsRowPartialContext:
    """Cover branches 723->725, 725->727, 727->730: missing parent/section/row_label."""

    @pytest.mark.unit
    def test_row_labels_no_parent_section(self):
        """723->725: parent_section is None => skip parent label."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({"c0": ["val"]})

        region = MatrixRegion(
            start_row=0, end_row=0, start_col=0, end_col=0,
            data=df,
            section_contexts={
                0: SectionContext(
                    parent_section=None,
                    current_section="Monthly",
                    row_label="Jan",
                    is_section_header=False,
                    is_aggregation=False,
                ),
            },
        )

        labels = extractor.extract_labels(df, region, [], axis="row")
        assert 0 in labels
        assert labels[0] == ["Monthly", "Jan"]

    @pytest.mark.unit
    def test_row_labels_no_current_section(self):
        """725->727: current_section is None => skip current section label."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({"c0": ["val"]})

        region = MatrixRegion(
            start_row=0, end_row=0, start_col=0, end_col=0,
            data=df,
            section_contexts={
                0: SectionContext(
                    parent_section="Payment",
                    current_section=None,
                    row_label="Jan",
                    is_section_header=False,
                    is_aggregation=False,
                ),
            },
        )

        labels = extractor.extract_labels(df, region, [], axis="row")
        assert 0 in labels
        assert labels[0] == ["Payment", "Jan"]

    @pytest.mark.unit
    def test_row_labels_no_row_label(self):
        """727->730: row_label is empty string => skip row_label."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({"c0": ["val"]})

        region = MatrixRegion(
            start_row=0, end_row=0, start_col=0, end_col=0,
            data=df,
            section_contexts={
                0: SectionContext(
                    parent_section="Payment",
                    current_section="Monthly",
                    row_label="",
                    is_section_header=False,
                    is_aggregation=False,
                ),
            },
        )

        labels = extractor.extract_labels(df, region, [], axis="row")
        assert 0 in labels
        assert labels[0] == ["Payment", "Monthly"]

    @pytest.mark.unit
    def test_row_labels_all_none(self):
        """723->725, 725->727, 727->730: all context fields are None/empty."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({"c0": ["val"]})

        region = MatrixRegion(
            start_row=0, end_row=0, start_col=0, end_col=0,
            data=df,
            section_contexts={
                0: SectionContext(
                    parent_section=None,
                    current_section=None,
                    row_label="",
                    is_section_header=False,
                    is_aggregation=False,
                ),
            },
        )

        labels = extractor.extract_labels(df, region, [], axis="row")
        assert 0 in labels
        assert labels[0] == []


class TestForwardFillLabelsMultipleIndices:
    """Cover branch 757->751: multiple indices in forward fill loop."""

    @pytest.mark.unit
    def test_forward_fill_multiple_indices(self):
        """757->751: iterate through multiple indices (loop back)."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)

        labels = {0: ["A"], 1: [], 2: [], 3: ["B"], 4: [], 5: []}
        extractor._forward_fill_labels(labels, axis="col")
        assert labels[0] == ["A"]
        assert labels[1] == ["A"]
        assert labels[2] == ["A"]
        assert labels[3] == ["B"]
        assert labels[4] == ["B"]
        assert labels[5] == ["B"]


class TestCombineLabelsColOrder:
    """Cover branch 790->787: axis == 'col' in naming order."""

    @pytest.mark.unit
    def test_combine_labels_col_order(self):
        """790->787: order includes 'col' => extend parts with col_label_list."""
        config = MatrixExtractorConfig(
            naming=NamingConfig(separator="_", order=["col", "row"]),
        )
        extractor = MatrixExtractor(config)

        row_labels = {0: ["RowPart"]}
        col_labels = {0: ["ColPart"]}

        result = extractor.combine_labels(row_labels, col_labels)
        assert (0, 0) in result
        # col comes first in the order
        assert result[(0, 0)] == "colpart_rowpart"


class TestCombineLabelsSkipEmptyFalse:
    """Cover branch 794->798: skip_empty is False."""

    @pytest.mark.unit
    def test_combine_labels_no_skip_empty(self):
        """794->798: skip_empty is False => don't filter empty parts."""
        config = MatrixExtractorConfig(
            naming=NamingConfig(separator="_", skip_empty=False, sanitize=False, order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        row_labels = {0: ["RowPart", ""]}
        col_labels = {0: ["ColPart"]}

        result = extractor.combine_labels(row_labels, col_labels)
        # Empty string should be preserved (not filtered), and no sanitize to preserve it
        assert (0, 0) in result
        assert result[(0, 0)] == "RowPart__ColPart"


class TestCombineLabelsNoSanitize:
    """Cover branch 801->804: sanitize is False."""

    @pytest.mark.unit
    def test_combine_labels_no_sanitize(self):
        """801->804: sanitize is False => don't call _sanitize_field_name."""
        config = MatrixExtractorConfig(
            naming=NamingConfig(separator="_", sanitize=False, order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        row_labels = {0: ["Hello World"]}
        col_labels = {0: ["Jan"]}

        result = extractor.combine_labels(row_labels, col_labels)
        # Without sanitize, original case and spaces preserved
        assert result[(0, 0)] == "Hello World_Jan"


class TestExtractOutOfBoundsSkip:
    """Cover branch 887->888: relative_row or relative_col out of bounds."""

    @pytest.mark.unit
    def test_extract_skips_out_of_bounds_row(self):
        """887->888: relative_row >= len(region.data) => continue."""
        from unittest.mock import patch

        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
            label_offsets=LabelOffsetsConfig(
                column_labels=[],
                row_labels=[],
            ),
            naming=NamingConfig(separator="_", order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "a": [1, 2],
            "b": [3, 4],
        })

        # Patch combine_labels to inject an out-of-bounds field name
        original_combine = extractor.combine_labels

        def patched_combine(row_labels, col_labels):
            result = original_combine(row_labels, col_labels)
            # Add an out-of-bounds entry: row 99 doesn't exist in a 2-row matrix
            result[(99, 0)] = "out_of_bounds_row"
            return result

        with patch.object(extractor, "combine_labels", side_effect=patched_combine):
            results = extractor.extract(df)

        # Should still produce results, just skipping the out-of-bounds entry
        assert isinstance(results, list)

    @pytest.mark.unit
    def test_extract_skips_out_of_bounds_col(self):
        """887->888: relative_col >= len(region.data.columns) => continue."""
        from unittest.mock import patch

        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
            label_offsets=LabelOffsetsConfig(
                column_labels=[],
                row_labels=[],
            ),
            naming=NamingConfig(separator="_", order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "a": [1, 2],
            "b": [3, 4],
        })

        original_combine = extractor.combine_labels

        def patched_combine(row_labels, col_labels):
            result = original_combine(row_labels, col_labels)
            # Add an out-of-bounds entry: col 99 doesn't exist
            result[(0, 99)] = "out_of_bounds_col"
            return result

        with patch.object(extractor, "combine_labels", side_effect=patched_combine):
            results = extractor.extract(df)

        assert isinstance(results, list)


class TestExtractWithSectionContexts:
    """Cover branches 893->894, 895->896, 895->899, 909->865: extract with section contexts."""

    @pytest.mark.unit
    def test_extract_skips_section_headers_in_records(self):
        """893->894, 895->896: section header rows are skipped during record creation."""
        from unittest.mock import patch

        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
            label_offsets=LabelOffsetsConfig(
                column_labels=[],
                row_labels=[],
            ),
            naming=NamingConfig(separator="_", order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "a": [10, 20, 30],
            "b": [40, 50, 60],
        })

        # Inject section contexts into detected regions so the extract loop
        # encounters a section header (is_section_header=True) and a data row
        original_detect = extractor.detect_matrices

        def patched_detect(input_df):
            regions = original_detect(input_df)
            for region in regions:
                region.section_contexts = {
                    0: SectionContext(
                        parent_section="Parent",
                        current_section="Section",
                        row_label="Header Row",
                        is_section_header=True,  # This row should be SKIPPED
                        is_aggregation=False,
                    ),
                    1: SectionContext(
                        parent_section="Parent",
                        current_section="Section",
                        row_label="Data Row",
                        is_section_header=False,  # This row should create records
                        is_aggregation=False,
                    ),
                }
            return regions

        with patch.object(extractor, "detect_matrices", side_effect=patched_detect):
            results = extractor.extract(df)

        assert isinstance(results, list)
        assert len(results) >= 1
        # Section header row (row 0) values should not appear
        # Data row (row 1) should appear
        all_values = results[0]["value"].to_list()
        # Row 0 values are 10, 40; row 1 values are 20, 50
        # 10 and 40 should NOT be in results (section header skipped)
        assert 10 not in all_values
        assert 40 not in all_values

    @pytest.mark.unit
    def test_extract_non_header_section_context_creates_records(self):
        """895->899, 909->865: non-header section context rows create records successfully."""
        from unittest.mock import patch

        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
            label_offsets=LabelOffsetsConfig(
                column_labels=[],
                row_labels=[],
            ),
            naming=NamingConfig(separator="_", order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "a": [100, 200],
            "b": [300, 400],
        })

        original_detect = extractor.detect_matrices

        def patched_detect(input_df):
            regions = original_detect(input_df)
            for region in regions:
                region.section_contexts = {
                    0: SectionContext(
                        parent_section="Parent",
                        current_section="Section",
                        row_label="Data A",
                        is_section_header=False,
                        is_aggregation=False,
                    ),
                    1: SectionContext(
                        parent_section="Parent",
                        current_section="Section",
                        row_label="Data B",
                        is_section_header=False,
                        is_aggregation=True,
                    ),
                }
            return regions

        with patch.object(extractor, "detect_matrices", side_effect=patched_detect):
            results = extractor.extract(df)

        assert isinstance(results, list)
        assert len(results) >= 1
        for r in results:
            assert "field_name" in r.columns
            assert "value" in r.columns
        # Both rows should have records since neither is a section header
        all_values = results[0]["value"].to_list()
        assert 100 in all_values
        assert 200 in all_values


class TestExtractFullPipelineWithLabels:
    """Cover 909->865 and multiple label/extract branches in a realistic scenario."""

    @pytest.mark.unit
    def test_full_pipeline_produces_records(self):
        """909->865: records list is non-empty => result_df appended to results."""
        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
            label_offsets=LabelOffsetsConfig(
                column_labels=[LabelOffset(offset=-1, forward_fill=True)],
                row_labels=[],
            ),
            naming=NamingConfig(separator="_", order=["col"]),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "label": ["", "Row1", "Row2"],
            "jan": ["January", "100", "200"],
            "feb": ["February", "150", "250"],
        })

        results = extractor.extract(df)
        assert len(results) >= 1
        assert results[0].height > 0
        assert "field_name" in results[0].columns

    @pytest.mark.unit
    def test_extract_with_col_label_out_of_range(self):
        """689: label_row < 0 => skip offset (continue)."""
        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
            label_offsets=LabelOffsetsConfig(
                column_labels=[LabelOffset(offset=-5)],  # way above
                row_labels=[],
            ),
            naming=NamingConfig(separator="_", order=["col"]),
        )
        extractor = MatrixExtractor(config)

        # Matrix starts at row 0, offset -5 => label_row = -5, which is < 0
        df = pl.DataFrame({
            "a": [1, 2],
            "b": [3, 4],
        })

        results = extractor.extract(df)
        assert isinstance(results, list)


class TestIsSectionHeaderFirstRowSubcategoryNext:
    """Cover branch 510->516: row_idx == 0 (no prev row), next row has subcategory prefix."""

    @pytest.mark.unit
    def test_first_row_next_has_subcategory(self):
        """510->516: row_idx == 0 skips prev-row check, next row starts with 'AD ' => True."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Benchmark PBPM", "AD Claims"]
        result = extractor._is_section_header("Benchmark PBPM", 0, all_values, cfg)
        assert result is True

    @pytest.mark.unit
    def test_first_row_next_no_subcategory(self):
        """510->516: row_idx == 0 skips prev-row check, next row not subcategory => default True."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Benchmark PBPM", "Regular Row"]
        result = extractor._is_section_header("Benchmark PBPM", 0, all_values, cfg)
        assert result is True

    @pytest.mark.unit
    def test_first_row_no_next_row(self):
        """510->516->522: row_idx == 0, only one value => default True."""
        config = MatrixExtractorConfig()
        extractor = MatrixExtractor(config)
        cfg = SectionDetectionConfig(section_column=0)

        all_values = ["Benchmark PBPM"]
        result = extractor._is_section_header("Benchmark PBPM", 0, all_values, cfg)
        assert result is True


class TestCombineLabelsColOnlyOrder:
    """Cover branch 790->787: 'col' axis in naming order extends col_label_list."""

    @pytest.mark.unit
    def test_col_only_order(self):
        """790->787: order=['col'] only extends col labels, no row labels."""
        config = MatrixExtractorConfig(
            naming=NamingConfig(separator="_", order=["col"]),
        )
        extractor = MatrixExtractor(config)

        row_labels = {0: ["RowPart"]}
        col_labels = {0: ["ColPart"]}

        result = extractor.combine_labels(row_labels, col_labels)
        assert (0, 0) in result
        # Only col part should appear
        assert "colpart" in result[(0, 0)]
        assert "rowpart" not in result[(0, 0)]


class TestExtractSectionHeaderSkipAndNonHeaderRecord:
    """Cover branches 895->896 (section header skip) and 909->865 (non-header record)."""

    @pytest.mark.unit
    def test_extract_mixed_header_and_data_rows(self):
        """895->896: section header row skipped; 909->865: data row creates record."""
        from unittest.mock import patch

        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
            label_offsets=LabelOffsetsConfig(
                column_labels=[],
                row_labels=[],
            ),
            naming=NamingConfig(separator="_", order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "a": [10, 20, 30],
            "b": [40, 50, 60],
        })

        original_detect = extractor.detect_matrices

        def patched_detect(input_df):
            regions = original_detect(input_df)
            for region in regions:
                region.section_contexts = {
                    0: SectionContext(
                        parent_section="P",
                        current_section="S",
                        row_label="Header",
                        is_section_header=True,
                        is_aggregation=False,
                    ),
                    2: SectionContext(
                        parent_section="P",
                        current_section="S",
                        row_label="Data",
                        is_section_header=False,
                        is_aggregation=False,
                    ),
                }
            return regions

        with patch.object(extractor, "detect_matrices", side_effect=patched_detect):
            results = extractor.extract(df)

        assert isinstance(results, list)
        assert len(results) >= 1
        all_values = results[0]["value"].to_list()
        # Row 0 (header) values 10, 40 should be skipped
        assert 10 not in all_values
        assert 40 not in all_values
        # Row 2 (data) values 30, 60 should be present
        assert 30 in all_values
        assert 60 in all_values


class TestExtractSectionHeaderSkipInExtractLoop:
    """Cover branch 895->896: section header row is skipped during extract record creation.

    The key insight: extract_labels already filters section headers from row_labels,
    so combine_labels won't include them. To hit branch 895->896 in the extract loop,
    we must inject the section_header row back into field_names via patching combine_labels.
    """

    @pytest.mark.unit
    def test_section_header_row_skipped_in_extract_records(self):
        """Branch 895->896: relative_row is in section_contexts and is_section_header=True -> continue."""
        from unittest.mock import patch as mock_patch

        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=2),
            label_offsets=LabelOffsetsConfig(column_labels=[], row_labels=[]),
            naming=NamingConfig(separator="_", order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "a": [10, 20],
            "b": [40, 50],
        })

        original_detect = extractor.detect_matrices
        original_combine = extractor.combine_labels

        def patched_detect(input_df):
            regions = original_detect(input_df)
            for region in regions:
                # Mark row 0 (relative) as section header
                region.section_contexts = {
                    0: SectionContext(
                        parent_section="P",
                        current_section="S",
                        row_label="Header",
                        is_section_header=True,
                        is_aggregation=False,
                    ),
                }
            return regions

        def patched_combine(row_labels, col_labels):
            result = original_combine(row_labels, col_labels)
            # Force-add entries for the section header row (row_idx=0)
            # so the extract loop encounters it and must skip via 895->896
            for col_idx in col_labels:
                result[(0, col_idx)] = "forced_header_field"
            return result

        with mock_patch.object(extractor, "detect_matrices", side_effect=patched_detect), \
             mock_patch.object(extractor, "combine_labels", side_effect=patched_combine):
            results = extractor.extract(df)

        assert isinstance(results, list)
        if results:
            all_field_names = results[0]["field_name"].to_list()
            # The forced_header_field should NOT appear (it was skipped)
            assert "forced_header_field" not in all_field_names


class TestExtractEmptyRecordsLoopBack:
    """Cover branch 909->865: records list empty for a region -> loop continues."""

    @pytest.mark.unit
    def test_all_rows_are_section_headers_no_records(self):
        """Branch 909->865: all field_name entries map to section headers,
        so records stays empty, and the if at 909 is False."""
        from unittest.mock import patch as mock_patch

        config = MatrixExtractorConfig(
            detection=DetectionConfig(min_rows=1, min_cols=1),
            label_offsets=LabelOffsetsConfig(column_labels=[], row_labels=[]),
            naming=NamingConfig(separator="_", order=["row", "col"]),
        )
        extractor = MatrixExtractor(config)

        df = pl.DataFrame({
            "a": [10, 20],
            "b": [40, 50],
        })

        original_detect = extractor.detect_matrices

        def patched_detect(input_df):
            regions = original_detect(input_df)
            for region in regions:
                # Mark ALL rows as section headers
                region.section_contexts = {
                    i: SectionContext(
                        parent_section="P",
                        current_section="S",
                        row_label=f"Header{i}",
                        is_section_header=True,
                        is_aggregation=False,
                    )
                    for i in range(region.end_row - region.start_row + 1)
                }
            return regions

        def patched_combine(row_labels, col_labels):
            # Force field_names for all rows (including headers)
            result = {}
            for row_idx in range(2):  # rows 0, 1
                for col_idx in range(2):  # cols 0, 1
                    result[(row_idx, col_idx)] = f"field_{row_idx}_{col_idx}"
            return result

        with mock_patch.object(extractor, "detect_matrices", side_effect=patched_detect), \
             mock_patch.object(extractor, "combine_labels", side_effect=patched_combine):
            results = extractor.extract(df)

        # All rows were section headers -> no records -> no DataFrames
        assert results == []
