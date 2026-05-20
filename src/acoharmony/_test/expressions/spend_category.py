from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._spend_category import SpendCategoryExpression


class TestSpendCategoryExpression:

    @pytest.mark.unit
    def test_is_inpatient_spend(self):
        df = pl.DataFrame({'bill_type_code': ['111', '131', '121'], 'paid_amount': [100.0, 200.0, 300.0]})
        result = df.select(SpendCategoryExpression.is_inpatient_spend().alias('ip'))
        assert result['ip'].to_list() == [100.0, 0.0, 300.0]

    @pytest.mark.unit
    def test_is_outpatient_spend(self):
        df = pl.DataFrame({'bill_type_code': ['131', '111'], 'paid_amount': [100.0, 200.0]})
        result = df.select(SpendCategoryExpression.is_outpatient_spend().alias('op'))
        assert result['op'][0] == 100.0
        assert result['op'][1] == 0.0

    @pytest.mark.unit
    def test_is_snf_spend(self):
        df = pl.DataFrame({'bill_type_code': ['211', '111'], 'paid_amount': [100.0, 200.0]})
        result = df.select(SpendCategoryExpression.is_snf_spend().alias('snf'))
        assert result['snf'][0] == 100.0

    @pytest.mark.unit
    def test_is_hospice_spend(self):
        df = pl.DataFrame({'bill_type_code': ['811', '111'], 'paid_amount': [100.0, 200.0]})
        result = df.select(SpendCategoryExpression.is_hospice_spend().alias('hospice'))
        assert result['hospice'][0] == 100.0

    @pytest.mark.unit
    def test_is_home_health_spend(self):
        df = pl.DataFrame({'bill_type_code': ['321', '111'], 'paid_amount': [100.0, 200.0]})
        result = df.select(SpendCategoryExpression.is_home_health_spend().alias('hh'))
        assert result['hh'][0] == 100.0

    @pytest.mark.unit
    def test_is_part_b_carrier_spend(self):
        df = pl.DataFrame({'bill_type_code': [None, '111', ''], 'paid_amount': [100.0, 200.0, 50.0]})
        result = df.select(SpendCategoryExpression.is_part_b_carrier_spend().alias('b'))
        assert result['b'][0] == 100.0
        assert result['b'][1] == 0.0
        assert result['b'][2] == 50.0
