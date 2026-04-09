from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._cclf_adr import CclfAdrExpression


class TestCclfAdrExpression:

    @pytest.mark.unit
    def test_negate_cancellations_header(self):
        df = pl.DataFrame({'clm_adjsmt_type_cd': ['0', '1', '2'], 'clm_pmt_amt': [100.0, 50.0, 200.0], 'clm_mdcr_instnl_tot_chrg_amt': [500.0, 300.0, 600.0]})
        exprs = CclfAdrExpression.negate_cancellations_header()
        result = df.select(exprs)
        assert result['clm_pmt_amt'][0] == pytest.approx(100.0)
        assert result['clm_pmt_amt'][1] == pytest.approx(-50.0)
        assert result['clm_pmt_amt'][2] == pytest.approx(200.0)
        assert result['clm_mdcr_instnl_tot_chrg_amt'][1] == pytest.approx(-300.0)

    @pytest.mark.unit
    def test_negate_cancellations_line(self):
        df = pl.DataFrame({'clm_adjsmt_type_cd': ['0', '1'], 'clm_line_cvrd_pd_amt': [100.0, 50.0], 'clm_line_alowd_chrg_amt': [200.0, 100.0]})
        exprs = CclfAdrExpression.negate_cancellations_line()
        result = df.select(exprs)
        assert result['clm_line_cvrd_pd_amt'][1] == pytest.approx(-50.0)
        assert result['clm_line_alowd_chrg_amt'][1] == pytest.approx(-100.0)

    @pytest.mark.unit
    def test_rank_by_effective_date(self):
        df = pl.DataFrame({'clm_blg_prvdr_oscar_num': ['A', 'A', 'B'], 'clm_efctv_dt': ['2024-01-01', '2024-02-01', '2024-01-01']})
        expr = CclfAdrExpression.rank_by_effective_date(['clm_blg_prvdr_oscar_num'])
        result = df.with_columns(expr)
        assert 'row_num' in result.columns

    @pytest.mark.unit
    def test_sum_by_natural_key(self):
        df = pl.DataFrame({'clm_pmt_amt': [100.0, -100.0, 200.0], 'group': ['A', 'A', 'B']})
        expr = CclfAdrExpression.sum_by_natural_key('clm_pmt_amt', ['group'])
        result = df.with_columns(expr)
        assert result['sum_clm_pmt_amt'][0] == pytest.approx(0.0)
        assert result['sum_clm_pmt_amt'][2] == pytest.approx(200.0)
