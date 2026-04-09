from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._pfs_rate_calc import PFSRateCalcConfig, PFSRateCalcExpression


class TestPFSRateCalcExpression:
    """Tests for _pfs_rate_calc expression builders."""

    @pytest.mark.unit
    def test_calculate_work_payment(self):
        df = pl.DataFrame({'work_rvu': [1.92], 'pw_gpci': [1.088]})
        result = df.select(PFSRateCalcExpression.calculate_work_payment())
        assert abs(result[0, 0] - 1.92 * 1.088) < 0.001

    @pytest.mark.unit
    def test_calculate_pe_payment(self):
        df = pl.DataFrame({'nf_pe_rvu': [1.31], 'pe_gpci': [1.459]})
        result = df.select(PFSRateCalcExpression.calculate_pe_payment())
        assert abs(result[0, 0] - 1.31 * 1.459) < 0.001

    @pytest.mark.unit
    def test_calculate_mp_payment(self):
        df = pl.DataFrame({'mp_rvu': [0.15], 'mp_gpci': [1.494]})
        result = df.select(PFSRateCalcExpression.calculate_mp_payment())
        assert abs(result[0, 0] - 0.15 * 1.494) < 0.001

    @pytest.mark.unit
    def test_calculate_total_rvu_adjusted(self):
        df = pl.DataFrame({'work_payment': [2.089], 'pe_payment': [1.911], 'mp_payment': [0.224]})
        result = df.select(PFSRateCalcExpression.calculate_total_rvu_adjusted())
        assert abs(result[0, 0] - 4.224) < 0.001

    @pytest.mark.unit
    def test_calculate_payment_rate(self):
        df = pl.DataFrame({'total_rvu_adjusted': [4.224], 'conversion_factor': [34.6062]})
        result = df.select(PFSRateCalcExpression.calculate_payment_rate())
        assert abs(result[0, 0] - 4.224 * 34.6062) < 0.01

    @pytest.mark.unit
    def test_calculate_rate_change_dollars(self):
        df = pl.DataFrame({'payment_rate': [146.17], 'prior_payment_rate': [106.31]})
        result = df.select(PFSRateCalcExpression.calculate_rate_change_dollars())
        assert abs(result[0, 0] - 39.86) < 0.01

    @pytest.mark.unit
    def test_calculate_rate_change_percent(self):
        df = pl.DataFrame({'payment_rate': [146.17], 'prior_payment_rate': [106.31]})
        result = df.select(PFSRateCalcExpression.calculate_rate_change_percent())
        expected = (146.17 - 106.31) / 106.31 * 100
        assert abs(result[0, 0] - expected) < 0.01

    @pytest.mark.unit
    def test_select_pe_rvu_column(self):
        func = getattr(PFSRateCalcExpression.select_pe_rvu_column, '__wrapped__', None)
        if func is not None:
            assert func('non_facility') == 'nf_pe_rvu'
            assert func('facility') == 'f_pe_rvu'
        else:
            assert hasattr(PFSRateCalcExpression, 'select_pe_rvu_column')

    @pytest.mark.unit
    def test_validate_gpci(self):
        df = pl.DataFrame({'pw_gpci': [1.088, None, 0.0]})
        result = df.select(PFSRateCalcExpression.validate_gpci('pw_gpci').alias('pw_gpci'))
        vals = result['pw_gpci'].to_list()
        assert vals[0] == 1.088
        assert vals[1] == 1.0
        assert vals[2] == 1.0

    @pytest.mark.unit
    def test_build_payment_calculation(self):
        calcs = PFSRateCalcExpression.build_payment_calculation()
        assert 'work_payment' in calcs
        assert 'pe_payment' in calcs
        assert 'mp_payment' in calcs
        assert 'total_rvu_adjusted' in calcs
        assert 'payment_rate' in calcs
        df = pl.DataFrame({'work_rvu': [1.92], 'nf_pe_rvu': [1.31], 'mp_rvu': [0.15], 'pw_gpci': [1.088], 'pe_gpci': [1.459], 'mp_gpci': [1.494], 'conversion_factor': [34.6062]})
        result = df.with_columns(**calcs)
        assert abs(result['payment_rate'][0] - 146.17) < 1.0

    @pytest.mark.unit
    def test_pfs_rate_calc_config(self):
        config = PFSRateCalcConfig()
        assert config.hcpcs_codes == []
        assert config.facility_type == 'non_facility'
        assert config.include_comparison is True
        assert config.use_home_visit_codes is False
        config2 = PFSRateCalcConfig(hcpcs_codes=['99347'], year=2026, facility_type='facility')
        assert config2.hcpcs_codes == ['99347']
        assert config2.year == 2026
