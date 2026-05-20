# © 2025 HarmonyCares
# All rights reserved.





# =============================================================================
# Tests for reach_hedr_eligible
# =============================================================================













# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest


class TestReachHedrEligible:
    """Test cases for expression builders."""

    @pytest.mark.unit
    def test_calculate_reach_hedr_rate(self):

        assert calculate_reach_hedr_rate(1000, 850) == 85.0
        assert calculate_reach_hedr_rate(0, 0) == 0.0










    @pytest.mark.unit
    def test_build_reach_hedr_denominator_expr_with_months_col(self):

        schema = ["months_in_reach", "death_date"]
        df = pl.DataFrame({
            "months_in_reach": [6, 3, 12],
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
        })
        result = df.select(build_reach_hedr_denominator_expr(2024, df_schema=schema))
        vals = result.to_series().to_list()
        assert vals == [True, False, True]










    @pytest.mark.unit
    def test_build_reach_hedr_rate_expr(self):

        schema = ["months_in_reach", "death_date", "race"]
        exprs = build_reach_hedr_rate_expr(
            performance_year=2024,
            required_data_columns=["race"],
            df_schema=schema,
        )
        assert "hedr_denominator" in exprs
        assert "hedr_numerator" in exprs
        assert "hedr_eligible" in exprs
        assert "hedr_complete" in exprs

    @pytest.mark.unit
    def test_denominator_df_schema_none(self):
        """Branch 71->72: df_schema defaults to [] when None."""
        expr = build_reach_hedr_denominator_expr(2024, df_schema=None)
        df = pl.DataFrame({"x": [1]})
        result = df.select(expr)
        # With empty schema, only living_expr (pl.lit(True)) applies
        assert result.to_series().to_list() == [True]

    @pytest.mark.unit
    def test_numerator_df_schema_none(self):
        """Branch 176->177: df_schema defaults to [] when None."""
        expr = build_reach_hedr_numerator_expr(2024, df_schema=None)
        df = pl.DataFrame({"x": [1]})
        result = df.select(expr)
        assert result.to_series().to_list() == [True]

    @pytest.mark.unit
    def test_numerator_required_col_not_in_schema(self):
        """Branch 192->191: required_data_columns col not in df_schema is skipped."""
        schema = ["months_in_reach", "death_date"]
        df = pl.DataFrame({
            "months_in_reach": [6, 3],
            "death_date": pl.Series([None, None], dtype=pl.Date),
        })
        # "race" is in required_data_columns but NOT in df_schema
        expr = build_reach_hedr_numerator_expr(
            2024,
            required_data_columns=["race"],
            df_schema=schema,
        )
        result = df.select(expr)
        # race not in schema so skipped; only denominator criteria apply
        assert result.to_series().to_list() == [True, False]

    @pytest.mark.unit
    def test_numerator_common_hedr_columns_in_schema(self):
        """Branch 217->220: common HEDR columns present in schema, no required_data_columns."""
        schema = ["months_in_reach", "death_date", "race", "ethnicity"]
        df = pl.DataFrame({
            "months_in_reach": [6, 6, 6],
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
            "race": ["White", "", "Hispanic"],
            "ethnicity": ["Non-Hispanic", "Hispanic", None],
        })
        expr = build_reach_hedr_numerator_expr(
            2024,
            required_data_columns=None,
            df_schema=schema,
        )
        result = df.select(expr)
        vals = result.to_series().to_list()
        # Row 0: race="White" ok, ethnicity="Non-Hispanic" ok => True
        # Row 1: race="" empty after strip => False
        # Row 2: ethnicity=None => False
        assert vals == [True, False, False]

    @pytest.mark.unit
    def test_numerator_hedr_data_complete_flag(self):
        """Branch 229->230: hedr_data_complete column in schema is checked."""
        schema = ["months_in_reach", "death_date", "hedr_data_complete"]
        df = pl.DataFrame({
            "months_in_reach": [6, 6, 6],
            "death_date": pl.Series([None, None, None], dtype=pl.Date),
            "hedr_data_complete": [True, False, True],
        })
        expr = build_reach_hedr_numerator_expr(
            2024,
            required_data_columns=None,
            df_schema=schema,
        )
        result = df.select(expr)
        vals = result.to_series().to_list()
        # hedr_data_complete gates: True, False, True
        assert vals == [True, False, True]


















