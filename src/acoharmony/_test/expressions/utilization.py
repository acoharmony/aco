from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._utilization import UtilizationExpression


class TestUtilizationExpression:

    @pytest.mark.unit
    def test_is_inpatient_admission(self):
        df = pl.DataFrame({'bill_type_code': ['111', '131', '121']})
        result = df.select(UtilizationExpression.is_inpatient_admission().alias('ip'))
        assert result['ip'].to_list() == [1, 0, 1]

    @pytest.mark.unit
    def test_is_er_visit(self):
        df = pl.DataFrame({'bill_type_code': ['131', '131'], 'revenue_center_code': ['0450', '0100'], 'place_of_service_code': ['22', '23']})
        result = df.select(UtilizationExpression.is_er_visit().alias('er'))
        assert result['er'][0] == 1
        assert result['er'][1] == 1

    @pytest.mark.unit
    def test_is_em_visit(self):
        df = pl.DataFrame({'hcpcs_code': ['99213', '12345', '99341']})
        result = df.select(UtilizationExpression.is_em_visit().alias('em'))
        assert result['em'][0] == 1
        assert result['em'][1] == 0
        assert result['em'][2] == 1

    @pytest.mark.unit
    def test_is_awv(self):
        df = pl.DataFrame({'hcpcs_code': ['G0438', 'G0439', '99213']})
        result = df.select(UtilizationExpression.is_awv().alias('awv'))
        assert result['awv'].to_list() == [1, 1, 0]

class TestUtilizationExpressionMetrics:
    """Test UtilizationExpression.calculate_utilization_metrics."""

    @pytest.mark.unit
    def test_calculate_utilization_metrics_basic(self):
        """Test basic utilization metrics calculation."""
        claims = pl.DataFrame({'service_category_2': ['IP', 'ER', 'ER', 'EM'], 'person_id': ['A', 'A', 'B', 'C']}).lazy()
        eligibility = pl.DataFrame({'person_id': ['A', 'B', 'C']}).lazy()
        config = {'measurement_year': 2024}
        visit_util, admission_rates, bed_days, high_util, service_mix = UtilizationExpression.calculate_utilization_metrics(claims, eligibility, admissions=None, config=config)
        visit_collected = visit_util.collect()
        assert 'total_visits' in visit_collected.columns
        assert 'unique_members' in visit_collected.columns
        adm_collected = admission_rates.collect()
        assert adm_collected.height == 0

    @pytest.mark.unit
    def test_calculate_utilization_metrics_with_admissions(self):
        """Test utilization metrics with admissions data."""
        claims = pl.DataFrame({'service_category_2': ['IP', 'ER'], 'person_id': ['A', 'B']}).lazy()
        eligibility = pl.DataFrame({'person_id': ['A', 'B']}).lazy()
        config = {'measurement_year': 2024}
        admissions = pl.DataFrame({'encounter_type': ['IP', 'IP', 'ER'], 'person_id': ['A', 'A', 'B']}).lazy()
        visit_util, admission_rates, bed_days, high_util, service_mix = UtilizationExpression.calculate_utilization_metrics(claims, eligibility, admissions=admissions, config=config)
        adm_collected = admission_rates.collect()
        assert adm_collected.height > 0
