# © 2025 HarmonyCares
# All rights reserved.





# =============================================================================
# Tests for append_detect
# =============================================================================













# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestAppendDetect:
    """Test cases for expression builders."""

    @pytest.mark.unit
    def test_combine_values_hierarchical(self):
        """HIERARCHICAL strategy concatenates like concatenate."""

        result = AppendDetectExpression.combine_values(
            ["level1", "level2", "level3"],
            strategy=AppendStrategy.HIERARCHICAL,
            separator=" > "
        )
        assert result == "level1 > level2 > level3"










    @pytest.mark.unit
    def test_combine_values_concatenate(self):
        """CONCATENATE strategy joins values with separator."""
        result = AppendDetectExpression.combine_values(
            ["a", "b"],
            strategy=AppendStrategy.CONCATENATE,
            separator=", ",
        )
        assert result == "a, b"


class TestGetCellValue:
    """Cover get_cell_value lines 226-238."""

    @pytest.mark.unit
    def test_valid_cell(self):
        import polars as pl
        df = pl.DataFrame({"A": ["hello", "world"], "B": [1, 2]})
        assert AppendDetectExpression.get_cell_value(df, 0, 0) == "hello"
        assert AppendDetectExpression.get_cell_value(df, 1, 0) == "world"

    @pytest.mark.unit
    def test_out_of_bounds_row(self):
        import polars as pl
        df = pl.DataFrame({"A": ["x"]})
        assert AppendDetectExpression.get_cell_value(df, -1, 0) is None
        assert AppendDetectExpression.get_cell_value(df, 5, 0) is None

    @pytest.mark.unit
    def test_out_of_bounds_col(self):
        import polars as pl
        df = pl.DataFrame({"A": ["x"]})
        assert AppendDetectExpression.get_cell_value(df, 0, -1) is None
        assert AppendDetectExpression.get_cell_value(df, 0, 5) is None

    @pytest.mark.unit
    def test_none_value(self):
        import polars as pl
        df = pl.DataFrame({"A": [None]})
        assert AppendDetectExpression.get_cell_value(df, 0, 0) is None

    @pytest.mark.unit
    def test_empty_string(self):
        import polars as pl
        df = pl.DataFrame({"A": ["  "]})
        assert AppendDetectExpression.get_cell_value(df, 0, 0) is None


class TestCombineValuesExtended:
    """Cover combine_values branches lines 264-280."""

    @pytest.mark.unit
    def test_first_non_empty(self):
        result = AppendDetectExpression.combine_values(
            ["", "first", "second"],
            strategy=AppendStrategy.FIRST_NON_EMPTY,
            separator="_",
        )
        assert result == "first"

    @pytest.mark.unit
    def test_first_non_empty_all_empty(self):
        result = AppendDetectExpression.combine_values(
            ["", ""],
            strategy=AppendStrategy.FIRST_NON_EMPTY,
            separator="_",
        )
        assert result == ""

    @pytest.mark.unit
    def test_last_non_empty(self):
        result = AppendDetectExpression.combine_values(
            ["first", "second", ""],
            strategy=AppendStrategy.LAST_NON_EMPTY,
            separator="_",
        )
        assert result == "second"

    @pytest.mark.unit
    def test_last_non_empty_all_empty(self):
        result = AppendDetectExpression.combine_values(
            ["", ""],
            strategy=AppendStrategy.LAST_NON_EMPTY,
            separator="_",
        )
        assert result == ""


class TestGenerateCellNamesCross:
    """Cover generate_cell_names_cross lines 302-349."""

    @pytest.mark.unit
    def test_cross_strategy(self):
        import polars as pl
        # Simple pivot: row 0 = col headers, col 0 = row headers, data at (1,1)
        df = pl.DataFrame({
            "c0": ["", "Sales"],
            "c1": ["2024", "100"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.CROSS,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=1,
            col_header_rows=[0],
            row_header_cols=[0],
        )
        result = AppendDetectExpression.generate_cell_names_cross(df, config)
        assert (1, 1) in result
        assert "2024" in result[(1, 1)].cell_name
        assert "sales" in result[(1, 1)].cell_name.lower()

    @pytest.mark.unit
    def test_cross_no_headers(self):
        import polars as pl
        df = pl.DataFrame({"A": [1, 2], "B": [3, 4]})
        config = AppendDetectConfig(
            direction=AppendDirection.CROSS,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=0,
            data_start_col=0,
            row_header_cols=None,
            col_header_rows=None,
        )
        result = AppendDetectExpression.generate_cell_names_cross(df, config)
        assert len(result) == 0


class TestGenerateCellNamesReferences:
    """Cover generate_cell_names_references lines 371-411."""

    @pytest.mark.unit
    def test_references_with_offsets(self):
        import polars as pl
        df = pl.DataFrame({
            "c0": ["header_a", "header_b", "val1"],
            "c1": ["header_c", "header_d", "val2"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.ROW,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=2,
            data_start_col=0,
            references=[CellReference(row_offset=-2, col_offset=0)],
        )
        result = AppendDetectExpression.generate_cell_names_references(df, config)
        assert (2, 0) in result
        assert "header_a" in result[(2, 0)].cell_name.lower()


class TestApplyMethod:
    """Cover apply method lines 440-456."""

    @pytest.mark.unit
    def test_apply_cross_direction(self):
        import polars as pl
        df = pl.DataFrame({
            "c0": ["", "Row1"],
            "c1": ["Col1", "100"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.CROSS,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=1,
            col_header_rows=[0],
            row_header_cols=[0],
        )
        result = AppendDetectExpression.apply(df, config)
        assert (1, 1) in result

    @pytest.mark.unit
    def test_apply_row_direction_auto_references(self):
        """ROW direction auto-generates col_offset=-1 reference."""
        import polars as pl
        df = pl.DataFrame({
            "c0": ["label_a", "label_b"],
            "c1": ["100", "200"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.ROW,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=0,
            data_start_col=1,
        )
        result = AppendDetectExpression.apply(df, config)
        assert (0, 1) in result
        assert "label_a" in result[(0, 1)].cell_name.lower()

    @pytest.mark.unit
    def test_apply_column_direction_auto_references(self):
        """COLUMN direction auto-generates row_offset=-1 reference."""
        import polars as pl
        df = pl.DataFrame({
            "c0": ["header", "data1"],
            "c1": ["header2", "data2"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.COLUMN,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=0,
        )
        result = AppendDetectExpression.apply(df, config)
        assert (1, 0) in result

    @pytest.mark.unit
    def test_apply_diagonal_direction(self):
        """DIAGONAL direction auto-generates row_offset=-1, col_offset=-1."""
        import polars as pl
        df = pl.DataFrame({
            "c0": ["corner", "left"],
            "c1": ["top", "data"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.DIAGONAL,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=1,
        )
        result = AppendDetectExpression.apply(df, config)
        assert (1, 1) in result
        # diagonal offset (-1, -1) → cell (0,0) = "corner"
        assert "corner" in result[(1, 1)].cell_name.lower()




















class TestAppendDetectNotDiagonal:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_append_detect_not_diagonal(self):
        """452->456: direction is not DIAGONAL."""
        from acoharmony._expressions._append_detect import AppendDetectExpression, AppendDetectConfig, AppendDirection
        df = pl.DataFrame({"c0": ["h", "d"], "c1": ["h2", "d2"]})
        config = AppendDetectConfig(direction=AppendDirection.ROW, strategy="concatenate", separator="_", data_start_row=1, data_start_col=1)
        result = AppendDetectExpression.apply(df, config)
        assert isinstance(result, dict)


class TestCombineValuesHierarchicalFallback:
    """Cover branch 276->280: strategy not matching any known enum value."""

    @pytest.mark.unit
    def test_combine_values_unknown_strategy_fallback(self):
        """When strategy doesn't match any known value, fall through to default join."""
        # Bypass StrEnum by passing a raw string that doesn't match any branch
        result = AppendDetectExpression.combine_values(
            ["a", "b", "c"],
            strategy="unknown_strategy",
            separator="-",
        )
        assert result == "a-b-c"


class TestCrossSkipEmptyAndFalsyHeaders:
    """Cover branches 316->314 and 323->321: loop continues without appending."""

    @pytest.mark.unit
    def test_cross_with_null_column_header(self):
        """When a col header cell is None, the loop continues (316->314)."""
        import polars as pl

        # Row 0 is the column header row. col 1 has None header.
        df = pl.DataFrame({
            "c0": [None, "RowLabel"],
            "c1": [None, "data"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.CROSS,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=1,
            col_header_rows=[0],
            row_header_cols=[0],
            sanitize_names=False,
        )
        result = AppendDetectExpression.generate_cell_names_cross(df, config)
        # col header at (0,1) is None -> skipped; row header at (1,0)="RowLabel"
        assert (1, 1) in result
        assert result[(1, 1)].cell_name == "RowLabel"
        assert result[(1, 1)].components == ["RowLabel"]

    @pytest.mark.unit
    def test_cross_with_null_row_header(self):
        """When a row header cell is None, the loop continues (323->321)."""
        import polars as pl

        df = pl.DataFrame({
            "c0": ["ColHead", None],
            "c1": ["ColHead2", "data"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.CROSS,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=1,
            col_header_rows=[0],
            row_header_cols=[0],
            sanitize_names=False,
        )
        result = AppendDetectExpression.generate_cell_names_cross(df, config)
        # col header at (0,1)="ColHead2", row header at (1,0)=None -> skipped
        assert (1, 1) in result
        assert result[(1, 1)].cell_name == "ColHead2"
        assert result[(1, 1)].components == ["ColHead2"]


class TestCrossEmptyComponentsNoEntry:
    """Cover branch 328->309: all headers are empty so components stays empty."""

    @pytest.mark.unit
    def test_cross_all_headers_null(self):
        """When all header cells are None, no entry is generated for the data cell."""
        import polars as pl

        df = pl.DataFrame({
            "c0": [None, None],
            "c1": [None, "data"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.CROSS,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=1,
            col_header_rows=[0],
            row_header_cols=[0],
            sanitize_names=False,
        )
        result = AppendDetectExpression.generate_cell_names_cross(df, config)
        # Both headers are None -> components is empty -> cell not in result
        assert (1, 1) not in result
        assert len(result) == 0


class TestCrossSanitizeNames:
    """Cover branch 334->341: sanitize_names=True in cross generation."""

    @pytest.mark.unit
    def test_cross_sanitize_names_true(self):
        """sanitize_names=True causes names to be lowercased and cleaned."""
        import polars as pl

        df = pl.DataFrame({
            "c0": ["", "Row Label!"],
            "c1": ["Col-Header 2024", "100"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.CROSS,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=1,
            col_header_rows=[0],
            row_header_cols=[0],
            sanitize_names=True,
        )
        result = AppendDetectExpression.generate_cell_names_cross(df, config)
        assert (1, 1) in result
        name = result[(1, 1)].cell_name
        # Sanitized: lowercase, no special chars, underscores for spaces/hyphens
        assert name == name.lower()
        assert "!" not in name
        assert " " not in name


class TestReferencesLoopContinueOnFalsy:
    """Cover branches 385->380 and 390->375: loop continues when value is falsy."""

    @pytest.mark.unit
    def test_references_with_null_reference_value(self):
        """When a referenced cell is None, the loop continues (385->380)."""
        import polars as pl

        # Reference points to row above, but row 0 col 0 is None
        df = pl.DataFrame({
            "c0": [None, "data"],
            "c1": ["header_present", "val"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.COLUMN,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=0,
            references=[CellReference(row_offset=-1, col_offset=0)],
            sanitize_names=False,
        )
        result = AppendDetectExpression.generate_cell_names_references(df, config)
        # (1,0) references (0,0) which is None -> no components -> no entry
        assert (1, 0) not in result
        # (1,1) references (0,1) = "header_present" -> has entry
        assert (1, 1) in result
        assert result[(1, 1)].cell_name == "header_present"

    @pytest.mark.unit
    def test_references_all_null_no_entry(self):
        """When all references resolve to None, cell has no entry (390->375)."""
        import polars as pl

        df = pl.DataFrame({
            "c0": [None, "data"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.COLUMN,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=0,
            references=[CellReference(row_offset=-1, col_offset=0)],
            sanitize_names=False,
        )
        result = AppendDetectExpression.generate_cell_names_references(df, config)
        # Only data cell is (1,0) referencing (0,0) which is None
        assert (1, 0) not in result
        assert len(result) == 0


class TestReferencesSanitizeNames:
    """Cover branch 396->403: sanitize_names=True in references generation."""

    @pytest.mark.unit
    def test_references_sanitize_names_true(self):
        """sanitize_names=True causes referenced names to be cleaned."""
        import polars as pl

        df = pl.DataFrame({
            "c0": ["My Header!!", "data_val"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.COLUMN,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=0,
            references=[CellReference(row_offset=-1, col_offset=0)],
            sanitize_names=True,
        )
        result = AppendDetectExpression.generate_cell_names_references(df, config)
        assert (1, 0) in result
        name = result[(1, 0)].cell_name
        # Sanitized: lowercase, no special chars
        assert name == "my_header"
        assert "!" not in name


class TestApplyFallthroughNoMatchingDirection:
    """Cover branches 444->456 and 452->456: direction doesn't match ROW/COLUMN/DIAGONAL."""

    @pytest.mark.unit
    def test_apply_unknown_direction_empty_references(self):
        """Direction that isn't ROW/COLUMN/DIAGONAL with empty references falls through."""
        import polars as pl
        from unittest.mock import patch

        df = pl.DataFrame({
            "c0": ["header", "data"],
            "c1": ["header2", "data2"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.ROW,  # Will be overridden
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=1,
            data_start_col=0,
            references=[],
        )
        # Patch the direction to a value that won't match ROW/COLUMN/DIAGONAL
        # but also won't match CROSS (so the else branch is entered)
        config.direction = "other"  # type: ignore[assignment]

        result = AppendDetectExpression.apply(df, config)
        # references stays empty list, generate_cell_names_references called with no refs
        assert isinstance(result, dict)
        # With no references, no cell names are generated
        assert len(result) == 0


class TestApplyWithExplicitReferencesSkipsAutoGeneration:
    """Cover branch 444->456: config.references is non-empty, skip auto-generation."""

    @pytest.mark.unit
    def test_apply_non_cross_with_explicit_references(self):
        """444->456: direction is not CROSS, references already populated => skip auto-gen."""
        import polars as pl

        df = pl.DataFrame({
            "c0": ["label_a", "label_b"],
            "c1": ["100", "200"],
        })
        config = AppendDetectConfig(
            direction=AppendDirection.ROW,
            strategy=AppendStrategy.CONCATENATE,
            separator="_",
            data_start_row=0,
            data_start_col=1,
            references=[CellReference(row_offset=0, col_offset=-1)],
        )
        result = AppendDetectExpression.apply(df, config)
        assert isinstance(result, dict)
        assert (0, 1) in result
        assert "label_a" in result[(0, 1)].cell_name.lower()
