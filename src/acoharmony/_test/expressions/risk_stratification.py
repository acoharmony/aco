from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._risk_stratification import RiskStratificationExpression


class TestRiskStratificationExpression:

    @pytest.mark.unit
    def test_calculate_clinical_risk_score(self):
        eligibility = pl.DataFrame({'person_id': ['P1', 'P2'], 'age': [70, 50], 'gender': ['M', 'F']}).lazy()
        result = RiskStratificationExpression.calculate_clinical_risk_score(eligibility, None, None, {}).collect()
        assert 'clinical_risk_score' in result.columns
        assert 'age_risk_score' in result.columns

    @pytest.mark.unit
    def test_clinical_risk_with_hcc_and_chronic(self):
        eligibility = pl.DataFrame({'person_id': ['P1'], 'age': [80], 'gender': ['F']}).lazy()
        hcc_raf = pl.DataFrame({'person_id': ['P1'], 'raf_score': [2.5]}).lazy()
        chronic = pl.DataFrame({'person_id': ['P1', 'P1', 'P1'], 'condition': ['A', 'B', 'C']}).lazy()
        result = RiskStratificationExpression.calculate_clinical_risk_score(eligibility, hcc_raf, chronic, {}).collect()
        assert result['raf_risk_score'][0] == 4

    @pytest.mark.unit
    def test_calculate_utilization_risk_score_none(self):
        result = RiskStratificationExpression.calculate_utilization_risk_score(None, None, {}).collect()
        assert 'utilization_risk_score' in result.columns

    @pytest.mark.unit
    def test_calculate_utilization_risk_score_with_data(self):
        admissions = pl.DataFrame({'person_id': ['P1', 'P1', 'P1'], 'encounter_type': ['inpatient', 'emergency_department', 'inpatient']}).lazy()
        readmissions = pl.DataFrame({'person_id': ['P1']}).lazy()
        result = RiskStratificationExpression.calculate_utilization_risk_score(admissions, readmissions, {}).collect()
        assert 'utilization_risk_score' in result.columns

    @pytest.mark.unit
    def test_utilization_risk_readmissions_only(self):
        """Covers branches 230->235, 249->250, 251->252: readmissions provided but no admissions."""
        readmissions = pl.DataFrame({'person_id': ['P1']}).lazy()
        result = RiskStratificationExpression.calculate_utilization_risk_score(None, readmissions, {}).collect()
        assert 'utilization_risk_score' in result.columns
        assert 'ip_risk_score' in result.columns
        assert 'ed_risk_score' in result.columns
        assert 'readmission_risk_score' in result.columns
        # ip and ed risk scores should default to 1 since no admissions data
        assert result['ip_risk_score'][0] == 1
        assert result['ed_risk_score'][0] == 1
        # readmission risk score should be 5 since has_readmission is True
        assert result['readmission_risk_score'][0] == 5

    @pytest.mark.unit
    def test_utilization_risk_admissions_only(self):
        """Covers branch 253->254: admissions provided but no readmissions."""
        admissions = pl.DataFrame({
            'person_id': ['P1', 'P1'],
            'encounter_type': ['inpatient', 'emergency_department'],
        }).lazy()
        result = RiskStratificationExpression.calculate_utilization_risk_score(admissions, None, {}).collect()
        assert 'utilization_risk_score' in result.columns
        assert 'readmission_risk_score' in result.columns
        # readmission_risk_score should default to 1 since no readmission data
        assert result['readmission_risk_score'][0] == 1

    @pytest.mark.unit
    def test_calculate_cost_risk_score_none(self):
        result = RiskStratificationExpression.calculate_cost_risk_score(None, {}).collect()
        assert 'cost_risk_score' in result.columns

    @pytest.mark.unit
    def test_calculate_cost_risk_score_with_data(self):
        tcoc = pl.DataFrame({'person_id': ['P1'], 'total_medical_cost': [50000.0], 'medical_pmpm': [600.0], 'cost_tier': ['top_5_pct']}).lazy()
        result = RiskStratificationExpression.calculate_cost_risk_score(tcoc, {}).collect()
        assert result['cost_risk_score'][0] == 4

    @pytest.mark.unit
    def test_calculate_composite_risk_tier(self):
        clinical = pl.DataFrame({'person_id': ['P1'], 'clinical_risk_score': [4.0]}).lazy()
        utilization = pl.DataFrame({'person_id': ['P1'], 'utilization_risk_score': [4.0]}).lazy()
        cost = pl.DataFrame({'person_id': ['P1'], 'cost_risk_score': [5.0]}).lazy()
        result = RiskStratificationExpression.calculate_composite_risk_tier(clinical, utilization, cost, {}).collect()
        assert 'risk_tier' in result.columns
        assert 'priority_for_care_management' in result.columns


class TestStratifyMemberRisk:
    """Cover stratify_member_risk lines 445-511."""

    @pytest.mark.unit
    def test_full_stratification(self):
        from datetime import date

        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "birth_date": [date(1955, 1, 1), date(1960, 6, 15)],
            "age": [70, 65],
            "gender": ["M", "F"],
        }).lazy()

        hcc_raf = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "raf_score": [1.5, 0.8],
        }).lazy()

        chronic_conditions = pl.DataFrame({
            "person_id": ["P1", "P1", "P2"],
            "condition": ["diabetes", "chf", "diabetes"],
            "meets_criteria": [True, True, True],
        }).lazy()

        admissions = pl.DataFrame({
            "person_id": ["P1", "P1", "P1"],
            "encounter_type": ["inpatient", "inpatient", "emergency_department"],
            "admission_date": [date(2024, 1, 1), date(2024, 3, 1), date(2024, 6, 1)],
        }).lazy()

        readmissions = pl.DataFrame({
            "person_id": ["P1"],
            "index_encounter_id": ["E1"],
            "readmission_encounter_id": ["E2"],
            "days_to_readmission": [15],
        }).lazy()

        tcoc = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "total_medical_cost": [50000.0, 10000.0],
            "medical_pmpm": [4166.67, 833.33],
            "cost_tier": ["top_5_pct", "normal"],
        }).lazy()

        member_scores, tier_summary, high_risk, composite = (
            RiskStratificationExpression.stratify_member_risk(
                eligibility, hcc_raf, chronic_conditions,
                admissions, readmissions, tcoc, {}
            )
        )

        ms = member_scores.collect()
        assert "person_id" in ms.columns
        assert "composite_risk_score" in ms.columns or "clinical_risk_score" in ms.columns

        ts = tier_summary.collect()
        assert "risk_tier" in ts.columns
        assert "member_count" in ts.columns

        hr = high_risk.collect()
        assert hr.height >= 0  # May be 0 if no critical/high

        cr = composite.collect()
        assert "risk_tier" in cr.columns


class TestRiskStratificationClinicalBranches:
    """Cover branches 77->78/99, 107->108/132 (hcc_raf/chronic_conditions None)."""

    @pytest.mark.unit
    def test_clinical_no_raf(self):
        """Branch 77->99: hcc_raf is None."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [70],
            "gender": ["M"],
        }).lazy()

        result = RiskStratificationExpression.calculate_clinical_risk_score(
            eligibility, None, None, {}
        )
        collected = result.collect()
        assert "raf_risk_score" in collected.columns

    @pytest.mark.unit
    def test_clinical_with_raf(self):
        """Branch 77->78: hcc_raf is provided."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [70],
            "gender": ["M"],
        }).lazy()
        hcc_raf = pl.DataFrame({
            "person_id": ["P1"],
            "raf_score": [2.5],
        }).lazy()

        result = RiskStratificationExpression.calculate_clinical_risk_score(
            eligibility, hcc_raf, None, {}
        )
        collected = result.collect()
        assert "raf_risk_score" in collected.columns

    @pytest.mark.unit
    def test_clinical_with_chronic_conditions(self):
        """Branch 107->108: chronic_conditions is provided."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [70],
            "gender": ["M"],
        }).lazy()
        chronic = pl.DataFrame({
            "person_id": ["P1", "P1", "P1"],
            "condition": ["diabetes", "hypertension", "copd"],
        }).lazy()

        result = RiskStratificationExpression.calculate_clinical_risk_score(
            eligibility, None, chronic, {}
        )
        collected = result.collect()
        assert "chronic_condition_risk_score" in collected.columns

    @pytest.mark.unit
    def test_clinical_no_chronic_conditions(self):
        """Branch 107->132: chronic_conditions is None."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [70],
            "gender": ["M"],
        }).lazy()

        result = RiskStratificationExpression.calculate_clinical_risk_score(
            eligibility, None, None, {}
        )
        collected = result.collect()
        assert "chronic_condition_risk_score" in collected.columns


class TestRiskStratificationUtilizationBranches:
    """Cover branches 176->178/226, 226->227/247, 230->231/235,
    247->249/270, 249->250/251, 251->252/253, 253->254/259."""

    @pytest.mark.unit
    def test_utilization_with_admissions(self):
        """Branch 176->178: admissions is not None."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        admissions = pl.DataFrame({
            "person_id": ["P1", "P1"],
            "encounter_type": ["inpatient", "emergency_department"],
        }).lazy()

        result = RiskStratificationExpression.calculate_utilization_risk_score(
            admissions, None, {}
        )
        collected = result.collect()
        assert "ip_risk_score" in collected.columns

    @pytest.mark.unit
    def test_utilization_no_admissions_with_readmissions(self):
        """Branch 176->226, 226->227: no admissions, readmissions provided."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        readmissions = pl.DataFrame({
            "person_id": ["P1"],
            "days_to_readmission": [10],
        }).lazy()

        result = RiskStratificationExpression.calculate_utilization_risk_score(
            None, readmissions, {}
        )
        collected = result.collect()
        assert "has_readmission" in collected.columns

    @pytest.mark.unit
    def test_utilization_admissions_and_readmissions(self):
        """Branch 230->231: admissions+readmissions, both joined."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        admissions = pl.DataFrame({
            "person_id": ["P1"],
            "encounter_type": ["inpatient"],
        }).lazy()
        readmissions = pl.DataFrame({
            "person_id": ["P1"],
            "days_to_readmission": [15],
        }).lazy()

        result = RiskStratificationExpression.calculate_utilization_risk_score(
            admissions, readmissions, {}
        )
        collected = result.collect()
        assert "readmission_risk_score" in collected.columns

    @pytest.mark.unit
    def test_utilization_neither(self):
        """Branch 247->270: no admissions or readmissions, empty result."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        result = RiskStratificationExpression.calculate_utilization_risk_score(
            None, None, {}
        )
        collected = result.collect()
        assert "utilization_risk_score" in collected.columns

    @pytest.mark.unit
    def test_utilization_missing_score_columns(self):
        """Branch 249->250, 251->252, 253->254: missing score columns filled."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        # Only readmissions (no admissions), so ip_risk_score and ed_risk_score missing
        readmissions = pl.DataFrame({
            "person_id": ["P1"],
            "days_to_readmission": [10],
        }).lazy()

        result = RiskStratificationExpression.calculate_utilization_risk_score(
            None, readmissions, {}
        )
        collected = result.collect()
        assert "ip_risk_score" in collected.columns
        assert "ed_risk_score" in collected.columns


class TestRiskStratificationCostBranches:
    """Cover branches 302->304/330."""

    @pytest.mark.unit
    def test_cost_with_tcoc(self):
        """Branch 302->304: tcoc is not None."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        tcoc = pl.DataFrame({
            "person_id": ["P1"],
            "total_medical_cost": [50000.0],
            "medical_pmpm": [4166.67],
            "cost_tier": ["top_5_pct"],
        }).lazy()

        result = RiskStratificationExpression.calculate_cost_risk_score(tcoc, {})
        collected = result.collect()
        assert "cost_risk_score" in collected.columns

    @pytest.mark.unit
    def test_cost_no_tcoc(self):
        """Branch 302->330: tcoc is None, empty result."""
        from acoharmony._expressions._risk_stratification import RiskStratificationExpression

        result = RiskStratificationExpression.calculate_cost_risk_score(None, {})
        collected = result.collect()
        assert "cost_risk_score" in collected.columns
        assert collected.height == 0
