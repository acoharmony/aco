# © 2025 HarmonyCares
# All rights reserved.





# =============================================================================
# Tests for cclf_claim_filters
# =============================================================================

# The implementation uses `"col_name" in pl.col("*")` which raises
# TypeError because pl.Expr is not iterable — this is a known issue
# in the source code.









# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._cclf_claim_filters import CclfClaimFilterExpression


class TestCclfClaimFilters:
    """Test cases for expression builders."""

    @pytest.mark.unit
    def test_positive_amounts_sum_clm_line_cvrd(self):
        """Test with sum_clm_line_cvrd_pd_amt column (line 164)."""

        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["sum_clm_line_cvrd_pd_amt"])
        assert expr is not None










    @pytest.mark.unit
    def test_positive_amounts_clm_pmt_amt(self):
        """Test with clm_pmt_amt column (line 166)."""

        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["clm_pmt_amt"])
        assert expr is not None










    @pytest.mark.unit
    def test_positive_amounts_clm_line_cvrd(self):
        """Test with clm_line_cvrd_pd_amt column (line 168)."""

        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["clm_line_cvrd_pd_amt"])
        assert expr is not None










    @pytest.mark.unit
    def test_positive_amounts_no_payment_column(self):
        """Test with no payment column - returns lit(True) (line 172)."""

        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["other_column"])
        assert expr is not None










    @pytest.mark.unit
    def test_positive_amounts_none_columns(self):
        """Test with None columns."""

        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=None)
        assert expr is not None










    @pytest.mark.unit
    def test_invalid_date_detection_is_callable(self):
        """invalid_date_detection is a static method that exists."""
        assert hasattr(CclfClaimFilterExpression, 'invalid_date_detection')
        assert callable(CclfClaimFilterExpression.invalid_date_detection)










    @pytest.mark.unit
    def test_positive_amounts_sum_clm_pmt_amt(self):
        """Line 163: returns expr for sum_clm_pmt_amt."""

        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["sum_clm_pmt_amt"])
        assert expr is not None


class TestCclfClaimFiltersWithDataFrames:
    """Evaluate expressions against real DataFrames to cover all branches."""

    @pytest.mark.unit
    def test_sum_clm_pmt_amt_filter(self):
        """Branch 162→163: sum_clm_pmt_amt column present and used."""
        df = pl.DataFrame({"sum_clm_pmt_amt": [100.0, -50.0, 0.0]})
        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["sum_clm_pmt_amt"])
        result = df.filter(expr)
        assert result.height == 1
        assert result["sum_clm_pmt_amt"][0] == 100.0

    @pytest.mark.unit
    def test_sum_clm_line_cvrd_pd_amt_filter(self):
        """Branch 162→164→165: sum_clm_line_cvrd_pd_amt column used."""
        df = pl.DataFrame({"sum_clm_line_cvrd_pd_amt": [200.0, -30.0]})
        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["sum_clm_line_cvrd_pd_amt"])
        result = df.filter(expr)
        assert result.height == 1

    @pytest.mark.unit
    def test_clm_pmt_amt_filter(self):
        """Branch 164→166→167: clm_pmt_amt column used."""
        df = pl.DataFrame({"clm_pmt_amt": [50.0, 0.0, 75.0]})
        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["clm_pmt_amt"])
        result = df.filter(expr)
        assert result.height == 2

    @pytest.mark.unit
    def test_clm_line_cvrd_pd_amt_filter(self):
        """Branch 166→168→169: clm_line_cvrd_pd_amt column used."""
        df = pl.DataFrame({"clm_line_cvrd_pd_amt": [10.0, -5.0]})
        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["clm_line_cvrd_pd_amt"])
        result = df.filter(expr)
        assert result.height == 1

    @pytest.mark.unit
    def test_no_payment_column_fallback(self):
        """Branch 168→172: no payment column, falls through to lit(True)."""
        df = pl.DataFrame({"some_other_col": [1, 2, 3]})
        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=["some_other_col"])
        result = df.filter(expr)
        # lit(True) means all rows pass
        assert result.height == 3

    @pytest.mark.unit
    def test_latest_non_canceled_filter(self):
        """Test latest_non_canceled filter evaluates correctly."""
        df = pl.DataFrame({
            "row_num": [1, 2, 1, 1],
            "clm_adjsmt_type_cd": ["0", "0", "1", "2"],
        })
        expr = CclfClaimFilterExpression.latest_non_canceled_filter()
        result = df.filter(expr)
        # row_num==1 AND clm_adjsmt_type_cd != '1'
        # Row 0: 1, '0' -> pass
        # Row 1: 2, '0' -> fail (row_num != 1)
        # Row 2: 1, '1' -> fail (canceled)
        # Row 3: 1, '2' -> pass
        assert result.height == 2


class TestSentinelDateExpressions:
    """Cover build_sentinel_date_expressions lines 122-131."""

    @pytest.mark.unit
    def test_sentinel_dates_detected(self):
        df = pl.DataFrame({
            "clm_from_dt": ["2024-01-15", "1000-01-01", "9999-12-31"],
            "clm_thru_dt": ["2024-02-15", "2024-03-01", "9999-12-31"],
            "clm_efctv_dt": ["2024-01-01", "1000-01-01", "2024-06-01"],
        })
        exprs = CclfClaimFilterExpression.invalid_date_detection()
        result = df.with_columns(exprs)
        assert result["is_sentinel_from_dt"][0] is False
        assert result["is_sentinel_from_dt"][1] is True
        assert result["is_sentinel_from_dt"][2] is True
        assert result["is_sentinel_thru_dt"][2] is True


class TestNonCanceledClaimBranches:
    """Cover branches 157-167 in non_canceled_claim_filter."""

    @pytest.mark.unit
    def test_sum_clm_pmt_amt_column(self):
        """Branch 157->158: 'sum_clm_pmt_amt' in cols."""
        from acoharmony._expressions._cclf_claim_filters import CclfClaimFilterExpression

        expr = CclfClaimFilterExpression.positive_amounts_filter(
            columns=["sum_clm_pmt_amt", "other_col"]
        )
        df = pl.DataFrame({"sum_clm_pmt_amt": [100.0, -50.0, 0.0]})
        result = df.select(expr.alias("keep"))
        assert result["keep"].to_list() == [True, False, False]

    @pytest.mark.unit
    def test_sum_clm_line_cvrd_pd_amt_column(self):
        """Branch 159->160: 'sum_clm_line_cvrd_pd_amt' in cols."""
        from acoharmony._expressions._cclf_claim_filters import CclfClaimFilterExpression

        expr = CclfClaimFilterExpression.positive_amounts_filter(
            columns=["sum_clm_line_cvrd_pd_amt"]
        )
        df = pl.DataFrame({"sum_clm_line_cvrd_pd_amt": [200.0, 0.0]})
        result = df.select(expr.alias("keep"))
        assert result["keep"].to_list() == [True, False]

    @pytest.mark.unit
    def test_clm_pmt_amt_column(self):
        """Branch 161->162: 'clm_pmt_amt' in cols."""
        from acoharmony._expressions._cclf_claim_filters import CclfClaimFilterExpression

        expr = CclfClaimFilterExpression.positive_amounts_filter(
            columns=["clm_pmt_amt"]
        )
        df = pl.DataFrame({"clm_pmt_amt": [50.0, -10.0]})
        result = df.select(expr.alias("keep"))
        assert result["keep"].to_list() == [True, False]

    @pytest.mark.unit
    def test_clm_line_cvrd_pd_amt_column(self):
        """Branch 163->164: 'clm_line_cvrd_pd_amt' in cols."""
        from acoharmony._expressions._cclf_claim_filters import CclfClaimFilterExpression

        expr = CclfClaimFilterExpression.positive_amounts_filter(
            columns=["clm_line_cvrd_pd_amt"]
        )
        df = pl.DataFrame({"clm_line_cvrd_pd_amt": [10.0, 0.0]})
        result = df.select(expr.alias("keep"))
        assert result["keep"].to_list() == [True, False]

    @pytest.mark.unit
    def test_no_payment_column_fallback(self):
        """Branch 163->167: no recognized payment column, returns pl.lit(True)."""
        from acoharmony._expressions._cclf_claim_filters import CclfClaimFilterExpression

        expr = CclfClaimFilterExpression.positive_amounts_filter(
            columns=["unrelated_col"]
        )
        df = pl.DataFrame({"unrelated_col": ["a", "b"]})
        result = df.filter(expr)
        # pl.lit(True) keeps all rows
        assert result.height == 2

    @pytest.mark.unit
    def test_none_columns_default(self):
        """Branch 157->159 when columns=None (defaults to empty set)."""
        from acoharmony._expressions._cclf_claim_filters import CclfClaimFilterExpression

        expr = CclfClaimFilterExpression.positive_amounts_filter(columns=None)
        df = pl.DataFrame({"x": [1]})
        result = df.filter(expr)
        assert result.height == 1


















