# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.lab_results module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date, datetime  # noqa: F811

import polars as pl
import pytest
import acoharmony

DEFAULT_CONFIG = {"measurement_year": 2024}




def _make_lab_results(rows: list[dict] | None = None) -> pl.LazyFrame:
    if rows is None:
        rows = [
            {"person_id": "P001", "loinc_code": "4548-4", "result_value": "7.2",
             "result_date": date(2024, 6, 1)},
            {"person_id": "P001", "loinc_code": "13457-7", "result_value": "120",
             "result_date": date(2024, 6, 1)},
            {"person_id": "P002", "loinc_code": "4548-4", "result_value": "10.1",
             "result_date": date(2024, 7, 1)},
            {"person_id": "P002", "loinc_code": "2339-0", "result_value": "130",
             "result_date": date(2024, 7, 1)},
        ]
    return pl.DataFrame(rows).lazy()




def _make_eligibility(rows: list[dict] | None = None) -> pl.LazyFrame:
    if rows is None:
        rows = [
            {
                "person_id": "P001",
                "enrollment_start_date": date(2024, 1, 1),
                "enrollment_end_date": date(2024, 12, 31),
            },
            {
                "person_id": "P002",
                "enrollment_start_date": date(2024, 1, 1),
                "enrollment_end_date": date(2024, 12, 31),
            },
        ]
    return pl.DataFrame(rows).lazy()


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestLabResultsIdentify:

    @pytest.mark.unit
    def test_identify_hba1c(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "4548-4", "result_value": "6.5",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "hba1c"

    @pytest.mark.unit
    def test_identify_ldl(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "13457-7", "result_value": "100",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "ldl"

    @pytest.mark.unit
    def test_identify_glucose(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "2339-0", "result_value": "95",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "glucose"

    @pytest.mark.unit
    def test_identify_bp_systolic(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "8480-6", "result_value": "120",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "bp_systolic"

    @pytest.mark.unit
    def test_identify_bp_diastolic(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "8462-4", "result_value": "80",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "bp_diastolic"

    @pytest.mark.unit
    def test_identify_creatinine(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "2160-0", "result_value": "1.0",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "creatinine"

    @pytest.mark.unit
    def test_identify_egfr(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "33914-3", "result_value": "85",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "egfr"

    @pytest.mark.unit
    def test_identify_total_cholesterol(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "2093-3", "result_value": "200",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "total_cholesterol"

    @pytest.mark.unit
    def test_identify_hdl(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "2085-9", "result_value": "55",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "hdl"

    @pytest.mark.unit
    def test_identify_triglycerides(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "2571-8", "result_value": "150",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "triglycerides"

    @pytest.mark.unit
    def test_unknown_loinc_code(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results([
            {"person_id": "P001", "loinc_code": "99999-9", "result_value": "42",
             "result_date": date(2024, 6, 1)},
        ])
        result = LabResultsTransform.identify_lab_tests(labs, DEFAULT_CONFIG).collect()
        assert result["lab_test_type"][0] == "other"


class TestLabResultsAbnormalFlags:

    def _flagged(self, lab_test_type: str, result_value: str) -> pl.DataFrame:
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001"],
            "loinc_code": ["test"],
            "result_value": [result_value],
            "result_date": [date(2024, 6, 1)],
            "lab_test_type": [lab_test_type],
        }).lazy()
        return LabResultsTransform.flag_abnormal_results(labs, DEFAULT_CONFIG).collect()

    @pytest.mark.unit
    def test_hba1c_optimal(self):
        df = self._flagged("hba1c", "5.0")
        assert df["hba1c_control_status"][0] == "optimal"
        assert df["is_abnormal"][0] is False

    @pytest.mark.unit
    def test_hba1c_controlled(self):
        df = self._flagged("hba1c", "6.5")
        assert df["hba1c_control_status"][0] == "controlled"

    @pytest.mark.unit
    def test_hba1c_uncontrolled(self):
        df = self._flagged("hba1c", "8.0")
        assert df["hba1c_control_status"][0] == "uncontrolled"

    @pytest.mark.unit
    def test_hba1c_poor_control(self):
        df = self._flagged("hba1c", "10.0")
        assert df["hba1c_control_status"][0] == "poor_control"
        assert df["is_abnormal"][0] is True

    @pytest.mark.unit
    def test_ldl_optimal(self):
        df = self._flagged("ldl", "90")
        assert df["ldl_control_status"][0] == "optimal"
        assert df["is_abnormal"][0] is False

    @pytest.mark.unit
    def test_ldl_near_optimal(self):
        df = self._flagged("ldl", "115")
        assert df["ldl_control_status"][0] == "near_optimal"

    @pytest.mark.unit
    def test_ldl_borderline_high(self):
        df = self._flagged("ldl", "150")
        assert df["ldl_control_status"][0] == "borderline_high"

    @pytest.mark.unit
    def test_ldl_high(self):
        df = self._flagged("ldl", "175")
        assert df["ldl_control_status"][0] == "high"
        assert df["is_abnormal"][0] is True

    @pytest.mark.unit
    def test_ldl_very_high(self):
        df = self._flagged("ldl", "200")
        assert df["ldl_control_status"][0] == "very_high"
        assert df["is_abnormal"][0] is True

    @pytest.mark.unit
    def test_glucose_abnormal(self):
        df = self._flagged("glucose", "130")
        assert df["is_abnormal"][0] is True

    @pytest.mark.unit
    def test_glucose_normal(self):
        df = self._flagged("glucose", "90")
        assert df["is_abnormal"][0] is False

    @pytest.mark.unit
    def test_bp_systolic_abnormal(self):
        df = self._flagged("bp_systolic", "145")
        assert df["is_abnormal"][0] is True

    @pytest.mark.unit
    def test_bp_systolic_normal(self):
        df = self._flagged("bp_systolic", "115")
        assert df["is_abnormal"][0] is False

    @pytest.mark.unit
    def test_bp_diastolic_abnormal(self):
        df = self._flagged("bp_diastolic", "95")
        assert df["is_abnormal"][0] is True

    @pytest.mark.unit
    def test_bp_diastolic_normal(self):
        df = self._flagged("bp_diastolic", "75")
        assert df["is_abnormal"][0] is False

    @pytest.mark.unit
    def test_egfr_abnormal_low(self):
        df = self._flagged("egfr", "45")
        assert df["is_abnormal"][0] is True

    @pytest.mark.unit
    def test_egfr_normal(self):
        df = self._flagged("egfr", "95")
        assert df["is_abnormal"][0] is False

    @pytest.mark.unit
    def test_non_flagged_test_type(self):
        """Test types without specific abnormal rules are not flagged."""
        df = self._flagged("triglycerides", "500")
        assert df["is_abnormal"][0] is False


class TestLabResultsDiabeticControl:

    @pytest.mark.unit
    def test_diabetic_control_output_columns(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001", "P001"],
            "loinc_code": ["4548-4", "4548-4"],
            "result_value": ["7.5", "6.8"],
            "result_date": [date(2024, 3, 1), date(2024, 9, 1)],
            "lab_test_type": ["hba1c", "hba1c"],
            "hba1c_control_status": ["uncontrolled", "controlled"],
            "ldl_control_status": [None, None],
            "is_abnormal": [False, False],
        }).lazy()
        diabetic_members = pl.DataFrame({"person_id": ["P001"]}).lazy()
        result = LabResultsTransform.calculate_diabetic_control(
            labs, diabetic_members, DEFAULT_CONFIG,
        ).collect()
        expected = {
            "person_id", "most_recent_hba1c", "most_recent_date", "control_status",
            "hba1c_test_count", "is_controlled_lt_7", "is_controlled_lt_8",
            "is_poor_control", "has_adequate_testing",
        }
        assert expected == set(result.columns)

    @pytest.mark.unit
    def test_diabetic_control_most_recent(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001", "P001"],
            "loinc_code": ["4548-4", "4548-4"],
            "result_value": ["9.5", "6.8"],
            "result_date": [date(2024, 3, 1), date(2024, 9, 1)],
            "lab_test_type": ["hba1c", "hba1c"],
            "hba1c_control_status": ["poor_control", "controlled"],
            "ldl_control_status": [None, None],
            "is_abnormal": [True, False],
        }).lazy()
        diabetic_members = pl.DataFrame({"person_id": ["P001"]}).lazy()
        result = LabResultsTransform.calculate_diabetic_control(
            labs, diabetic_members, DEFAULT_CONFIG,
        ).collect()
        # Most recent (Sept) should be 6.8
        assert result["most_recent_hba1c"][0] == "6.8"
        assert result["is_controlled_lt_7"][0] is True

    @pytest.mark.unit
    def test_diabetic_control_non_diabetic_excluded(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001"],
            "loinc_code": ["4548-4"],
            "result_value": ["7.0"],
            "result_date": [date(2024, 6, 1)],
            "lab_test_type": ["hba1c"],
            "hba1c_control_status": ["controlled"],
            "ldl_control_status": [None],
            "is_abnormal": [False],
        }).lazy()
        diabetic_members = pl.DataFrame({"person_id": ["P099"]}).lazy()
        result = LabResultsTransform.calculate_diabetic_control(
            labs, diabetic_members, DEFAULT_CONFIG,
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_adequate_testing_flag(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001", "P001", "P002"],
            "loinc_code": ["4548-4"] * 3,
            "result_value": ["7.0", "6.5", "8.0"],
            "result_date": [date(2024, 3, 1), date(2024, 9, 1), date(2024, 6, 1)],
            "lab_test_type": ["hba1c"] * 3,
            "hba1c_control_status": ["controlled", "controlled", "uncontrolled"],
            "ldl_control_status": [None] * 3,
            "is_abnormal": [False, False, False],
        }).lazy()
        diabetic_members = pl.DataFrame({"person_id": ["P001", "P002"]}).lazy()
        result = LabResultsTransform.calculate_diabetic_control(
            labs, diabetic_members, DEFAULT_CONFIG,
        ).collect()
        p001 = result.filter(pl.col("person_id") == "P001")
        p002 = result.filter(pl.col("person_id") == "P002")
        assert p001["has_adequate_testing"][0] is True   # 2 tests
        assert p002["has_adequate_testing"][0] is False   # 1 test


class TestLabResultsCvdControl:

    @pytest.mark.unit
    def test_cvd_control_output_columns(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001"],
            "loinc_code": ["13457-7"],
            "result_value": ["90"],
            "result_date": [date(2024, 6, 1)],
            "lab_test_type": ["ldl"],
            "hba1c_control_status": [None],
            "ldl_control_status": ["optimal"],
            "is_abnormal": [False],
        }).lazy()
        cvd_members = pl.DataFrame({"person_id": ["P001"]}).lazy()
        result = LabResultsTransform.calculate_cvd_control(
            labs, cvd_members, DEFAULT_CONFIG,
        ).collect()
        expected = {
            "person_id", "most_recent_ldl", "most_recent_date", "control_status",
            "ldl_test_count", "is_controlled_lt_100", "is_optimal_lt_70",
            "is_high_risk", "has_testing",
        }
        assert expected == set(result.columns)

    @pytest.mark.unit
    def test_cvd_control_values(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001"],
            "loinc_code": ["13457-7"],
            "result_value": ["65"],
            "result_date": [date(2024, 6, 1)],
            "lab_test_type": ["ldl"],
            "hba1c_control_status": [None],
            "ldl_control_status": ["optimal"],
            "is_abnormal": [False],
        }).lazy()
        cvd_members = pl.DataFrame({"person_id": ["P001"]}).lazy()
        result = LabResultsTransform.calculate_cvd_control(
            labs, cvd_members, DEFAULT_CONFIG,
        ).collect()
        assert result["is_controlled_lt_100"][0] is True
        assert result["is_optimal_lt_70"][0] is True
        assert result["is_high_risk"][0] is False

    @pytest.mark.unit
    def test_cvd_high_risk(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001"],
            "loinc_code": ["13457-7"],
            "result_value": ["180"],
            "result_date": [date(2024, 6, 1)],
            "lab_test_type": ["ldl"],
            "hba1c_control_status": [None],
            "ldl_control_status": ["high"],
            "is_abnormal": [True],
        }).lazy()
        cvd_members = pl.DataFrame({"person_id": ["P001"]}).lazy()
        result = LabResultsTransform.calculate_cvd_control(
            labs, cvd_members, DEFAULT_CONFIG,
        ).collect()
        assert result["is_high_risk"][0] is True
        assert result["is_controlled_lt_100"][0] is False


class TestLabResultsTestingCompliance:

    @pytest.mark.unit
    def test_testing_compliance_output(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001", "P001", "P002"],
            "loinc_code": ["4548-4", "13457-7", "4548-4"],
            "result_value": ["7.0", "100", "8.0"],
            "result_date": [date(2024, 6, 1), date(2024, 6, 1), date(2024, 7, 1)],
            "lab_test_type": ["hba1c", "ldl", "hba1c"],
        }).lazy()
        eligibility = _make_eligibility()
        result = LabResultsTransform.calculate_testing_compliance(
            labs, eligibility, DEFAULT_CONFIG,
        ).collect()
        assert "person_id" in result.columns
        # Should have rows for both P001 and P002
        assert result.height == 2

    @pytest.mark.unit
    def test_testing_compliance_fills_nulls(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = pl.DataFrame({
            "person_id": ["P001"],
            "loinc_code": ["4548-4"],
            "result_value": ["7.0"],
            "result_date": [date(2024, 6, 1)],
            "lab_test_type": ["hba1c"],
        }).lazy()
        eligibility = _make_eligibility()
        result = LabResultsTransform.calculate_testing_compliance(
            labs, eligibility, DEFAULT_CONFIG,
        ).collect()
        # P002 has no labs, their hba1c count should be 0
        p002 = result.filter(pl.col("person_id") == "P002")
        if "hba1c" in p002.columns:
            assert p002["hba1c"][0] == 0


class TestLabResultsComprehensive:

    @pytest.mark.unit
    def test_analyze_lab_results_returns_five_tuple(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results()
        eligibility = _make_eligibility()
        diabetic = pl.DataFrame({"person_id": ["P001"]}).lazy()
        cvd = pl.DataFrame({"person_id": ["P001"]}).lazy()
        result = LabResultsTransform.analyze_lab_results(
            labs, eligibility, diabetic, cvd, DEFAULT_CONFIG,
        )
        assert isinstance(result, tuple)
        assert len(result) == 5
        for item in result:
            assert isinstance(item, pl.LazyFrame)

    @pytest.mark.unit
    def test_analyze_lab_results_none_diabetic_members(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        labs = _make_lab_results()
        eligibility = _make_eligibility()
        result = LabResultsTransform.analyze_lab_results(
            labs, eligibility, None, None, DEFAULT_CONFIG,
        )
        assert len(result) == 5
        # diabetic_control and cvd_control should be empty DataFrames
        diabetic_control = result[2].collect()
        cvd_control = result[3].collect()
        assert diabetic_control.height == 0
        assert cvd_control.height == 0

    @pytest.mark.unit
    def test_analyze_lab_results_abnormal_filter(self):
        from acoharmony._transforms.lab_results import LabResultsTransform

        # P002 has hba1c of 10.1 (poor control, abnormal)
        labs = _make_lab_results()
        eligibility = _make_eligibility()
        _, abnormal, _, _, _ = LabResultsTransform.analyze_lab_results(
            labs, eligibility, None, None, DEFAULT_CONFIG,
        )
        abnormal_df = abnormal.collect()
        # At least the hba1c=10.1 and glucose=130 should be abnormal
        assert abnormal_df.height >= 1
