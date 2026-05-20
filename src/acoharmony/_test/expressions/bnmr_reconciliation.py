# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._bnmr_reconciliation."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._bnmr_reconciliation import BnmrReconciliationExpression


class TestBnmrNetExpenditureExpr:
    """Computes net_expenditure from BNMR's component reduction columns.

    BNMR does NOT ship a pre-netted ``total_exp_amt_agg`` like MER does.
    Instead we compute the net as:

        net = clm_pmt_amt_agg
              - sqstr_amt_agg
              - apa_rdctn_amt_agg
              - ucc_amt_agg
              - op_dsh_amt_agg
              - cp_dsh_amt_agg
              - op_ime_amt_agg
              - cp_ime_amt_agg
              - nonpbp_rdct_amt_agg
              + aco_amt_agg_apa

    aco_amt_agg_apa is ADDED (not subtracted) because it represents an
    ACO-specific payment adjustment that flows back to the ACO's net.
    """

    @pytest.mark.unit
    def test_all_zero_reductions_equals_gross(self):
        """With every reduction column = 0, net equals clm_pmt_amt_agg."""
        df = pl.DataFrame(
            {
                "clm_pmt_amt_agg": [1000.00],
                "sqstr_amt_agg": [0.0],
                "apa_rdctn_amt_agg": [0.0],
                "ucc_amt_agg": [0.0],
                "op_dsh_amt_agg": [0.0],
                "cp_dsh_amt_agg": [0.0],
                "op_ime_amt_agg": [0.0],
                "cp_ime_amt_agg": [0.0],
                "nonpbp_rdct_amt_agg": [0.0],
                "aco_amt_agg_apa": [0.0],
            }
        )
        result = df.with_columns(
            BnmrReconciliationExpression.net_expenditure_expr()
        )
        assert float(result["net_expenditure"][0]) == pytest.approx(1000.00)

    @pytest.mark.unit
    def test_reductions_subtract_correctly(self):
        """Each reduction column subtracts from the gross by exactly its value."""
        df = pl.DataFrame(
            {
                "clm_pmt_amt_agg": [1000.00],
                "sqstr_amt_agg": [20.00],
                "apa_rdctn_amt_agg": [30.00],
                "ucc_amt_agg": [10.00],
                "op_dsh_amt_agg": [5.00],
                "cp_dsh_amt_agg": [5.00],
                "op_ime_amt_agg": [5.00],
                "cp_ime_amt_agg": [5.00],
                "nonpbp_rdct_amt_agg": [20.00],
                "aco_amt_agg_apa": [0.0],
            }
        )
        result = df.with_columns(
            BnmrReconciliationExpression.net_expenditure_expr()
        )
        # 1000 - 20 - 30 - 10 - 5 - 5 - 5 - 5 - 20 = 900
        assert float(result["net_expenditure"][0]) == pytest.approx(900.00)

    @pytest.mark.unit
    def test_aco_amt_agg_apa_is_added_not_subtracted(self):
        """aco_amt_agg_apa is an ACO-specific payment ADJUSTMENT that increases net."""
        df = pl.DataFrame(
            {
                "clm_pmt_amt_agg": [1000.00],
                "sqstr_amt_agg": [0.0],
                "apa_rdctn_amt_agg": [0.0],
                "ucc_amt_agg": [0.0],
                "op_dsh_amt_agg": [0.0],
                "cp_dsh_amt_agg": [0.0],
                "op_ime_amt_agg": [0.0],
                "cp_ime_amt_agg": [0.0],
                "nonpbp_rdct_amt_agg": [0.0],
                "aco_amt_agg_apa": [150.00],  # should ADD to gross
            }
        )
        result = df.with_columns(
            BnmrReconciliationExpression.net_expenditure_expr()
        )
        assert float(result["net_expenditure"][0]) == pytest.approx(1150.00)

    @pytest.mark.unit
    def test_nulls_propagate(self):
        """A null in any reduction column propagates to net_expenditure."""
        df = pl.DataFrame(
            {
                "clm_pmt_amt_agg": [1000.00],
                "sqstr_amt_agg": [None],
                "apa_rdctn_amt_agg": [0.0],
                "ucc_amt_agg": [0.0],
                "op_dsh_amt_agg": [0.0],
                "cp_dsh_amt_agg": [0.0],
                "op_ime_amt_agg": [0.0],
                "cp_ime_amt_agg": [0.0],
                "nonpbp_rdct_amt_agg": [0.0],
                "aco_amt_agg_apa": [0.0],
            },
            schema={
                "clm_pmt_amt_agg": pl.Float64,
                "sqstr_amt_agg": pl.Float64,
                "apa_rdctn_amt_agg": pl.Float64,
                "ucc_amt_agg": pl.Float64,
                "op_dsh_amt_agg": pl.Float64,
                "cp_dsh_amt_agg": pl.Float64,
                "op_ime_amt_agg": pl.Float64,
                "cp_ime_amt_agg": pl.Float64,
                "nonpbp_rdct_amt_agg": pl.Float64,
                "aco_amt_agg_apa": pl.Float64,
            },
        )
        result = df.with_columns(
            BnmrReconciliationExpression.net_expenditure_expr()
        )
        assert result["net_expenditure"][0] is None
