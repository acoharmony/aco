"""Tests for acoharmony._transforms._quality_diabetes module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

import acoharmony


def _make_eligibility(person_ids, year=2024):
    """Build a minimal eligibility LazyFrame for the given person IDs."""
    return pl.DataFrame(
        {
            "person_id": person_ids,
            "enrollment_start_date": [date(year, 1, 1)] * len(person_ids),
            "enrollment_end_date": [date(year, 12, 31)] * len(person_ids),
            "age": [50] * len(person_ids),
        }
    ).lazy()


def _make_claims(person_ids, diagnosis_codes=None, procedure_codes=None, year=2024):
    """Build a minimal claims LazyFrame."""
    n = len(person_ids)
    diag = diagnosis_codes if diagnosis_codes else ["E11.9"] * n
    proc = procedure_codes if procedure_codes else ["99999"] * n
    return pl.DataFrame(
        {
            "person_id": person_ids,
            "diagnosis_code_1": diag,
            "procedure_code": proc,
            "claim_end_date": [date(year, 6, 15)] * n,
        }
    ).lazy()


def _diabetes_value_sets():
    """Return a value_sets dict that includes a Diabetes code set."""
    return {
        "Diabetes": pl.DataFrame({"code": ["E11.9"]}).lazy(),
    }


def _hba1c_value_sets():
    """Return value_sets with Diabetes and HbA1c Lab Test."""
    return {
        "Diabetes": pl.DataFrame({"code": ["E11.9"]}).lazy(),
        "HbA1c Lab Test": pl.DataFrame({"code": ["83036"]}).lazy(),
    }


def _eye_exam_value_sets():
    """Return value_sets with Diabetes and Diabetic Retinal Screening."""
    return {
        "Diabetes": pl.DataFrame({"code": ["E11.9"]}).lazy(),
        "Diabetic Retinal Screening": pl.DataFrame({"code": ["92250"]}).lazy(),
    }


def _pregnancy_value_sets():
    """Return value_sets with Diabetes and Pregnancy."""
    return {
        "Diabetes": pl.DataFrame({"code": ["E11.9"]}).lazy(),
        "Pregnancy": pl.DataFrame({"code": ["O09.90"]}).lazy(),
    }


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._quality_diabetes is not None


# ---------------------------------------------------------------------------
# DiabetesHbA1cPoorControl  (branches 85->86/94, 145->146/157)
# ---------------------------------------------------------------------------

class TestDiabetesHbA1cPoorControlDenominator:
    """Tests for DiabetesHbA1cPoorControl.calculate_denominator."""

    @pytest.mark.unit
    def test_denominator_no_diabetes_value_set(self):
        """Branch 85->86: diabetes_codes is None returns empty denominator."""
        measure = DiabetesHbA1cPoorControl({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        eligibility = _make_eligibility(["P1"])
        value_sets: dict[str, pl.LazyFrame] = {}  # No "Diabetes" key

        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 0
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_denominator_with_diabetes_value_set(self):
        """Branch 85->94: diabetes_codes present, normal path."""
        measure = DiabetesHbA1cPoorControl({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        eligibility = _make_eligibility(["P1"])
        value_sets = _diabetes_value_sets()

        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 1
        assert result["denominator_flag"][0] is True


class TestDiabetesHbA1cPoorControlNumerator:
    """Tests for DiabetesHbA1cPoorControl.calculate_numerator."""

    @pytest.mark.unit
    def test_numerator_empty_hba1c_value_set(self):
        """Branch 145->146: empty HbA1c codes marks all as numerator."""
        measure = DiabetesHbA1cPoorControl({"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = _make_claims(["P1"])
        value_sets: dict[str, pl.LazyFrame] = {}  # No HbA1c keys

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.height == 1
        assert result["numerator_flag"][0] is True

    @pytest.mark.unit
    def test_numerator_with_hba1c_value_set(self):
        """Branch 145->157: HbA1c codes present, placeholder logic."""
        measure = DiabetesHbA1cPoorControl({"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = _make_claims(["P1"])
        value_sets = _hba1c_value_sets()

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.height == 1
        assert "numerator_flag" in result.columns


# ---------------------------------------------------------------------------
# DiabetesHbA1cTesting  (branches 237->238/245, 284->285/289)
# ---------------------------------------------------------------------------

class TestDiabetesHbA1cTestingDenominator:
    """Tests for DiabetesHbA1cTesting.calculate_denominator."""

    @pytest.mark.unit
    def test_denominator_no_diabetes_value_set(self):
        """Branch 237->238: diabetes_codes is None returns empty denominator."""
        measure = DiabetesHbA1cTesting({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        eligibility = _make_eligibility(["P1"])
        value_sets: dict[str, pl.LazyFrame] = {}

        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 0
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_denominator_with_diabetes_value_set(self):
        """Branch 237->245: diabetes_codes present, normal path."""
        measure = DiabetesHbA1cTesting({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        eligibility = _make_eligibility(["P1"])
        value_sets = _diabetes_value_sets()

        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 1
        assert result["denominator_flag"][0] is True


class TestDiabetesHbA1cTestingNumerator:
    """Tests for DiabetesHbA1cTesting.calculate_numerator."""

    @pytest.mark.unit
    def test_numerator_empty_hba1c_value_set(self):
        """Branch 284->285: empty HbA1c codes marks none as tested."""
        measure = DiabetesHbA1cTesting({"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = _make_claims(["P1"])
        value_sets: dict[str, pl.LazyFrame] = {}

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.height == 1
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_numerator_with_hba1c_value_set(self):
        """Branch 284->289: HbA1c codes present, joins on procedure_code."""
        measure = DiabetesHbA1cTesting({"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = _make_claims(["P1"], procedure_codes=["83036"])
        value_sets = _hba1c_value_sets()

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.height == 1
        assert "numerator_flag" in result.columns


# ---------------------------------------------------------------------------
# DiabetesBPControl  (branches 373->374/381, 441->442/447)
# ---------------------------------------------------------------------------

class TestDiabetesBPControlDenominator:
    """Tests for DiabetesBPControl.calculate_denominator."""

    @pytest.mark.unit
    def test_denominator_no_diabetes_value_set(self):
        """Branch 373->374: diabetes_codes is None returns empty denominator."""
        measure = DiabetesBPControl({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        eligibility = _make_eligibility(["P1"])
        value_sets: dict[str, pl.LazyFrame] = {}

        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 0
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_denominator_with_diabetes_value_set(self):
        """Branch 373->381: diabetes_codes present, normal path."""
        measure = DiabetesBPControl({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        eligibility = _make_eligibility(["P1"])
        value_sets = _diabetes_value_sets()

        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 1
        assert result["denominator_flag"][0] is True


class TestDiabetesBPControlExclusions:
    """Tests for DiabetesBPControl.calculate_exclusions."""

    @pytest.mark.unit
    def test_exclusions_no_pregnancy_value_set(self):
        """Branch 441->442: pregnancy_codes is None, no exclusions."""
        measure = DiabetesBPControl({"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = _make_claims(["P1"])
        value_sets: dict[str, pl.LazyFrame] = {}  # No "Pregnancy" key

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert result.height == 1
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_exclusions_with_pregnancy_value_set(self):
        """Branch 441->447: pregnancy_codes present, joins claims."""
        measure = DiabetesBPControl({"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = _make_claims(["P1"], diagnosis_codes=["O09.90"])
        value_sets = _pregnancy_value_sets()

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert result.height == 1
        assert "exclusion_flag" in result.columns


# ---------------------------------------------------------------------------
# DiabetesEyeExam  (branches 522->523/530, 568->569/573)
# ---------------------------------------------------------------------------

class TestDiabetesEyeExamDenominator:
    """Tests for DiabetesEyeExam.calculate_denominator."""

    @pytest.mark.unit
    def test_denominator_no_diabetes_value_set(self):
        """Branch 522->523: diabetes_codes is None returns empty denominator."""
        measure = DiabetesEyeExam({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        eligibility = _make_eligibility(["P1"])
        value_sets: dict[str, pl.LazyFrame] = {}

        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 0
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_denominator_with_diabetes_value_set(self):
        """Branch 522->530: diabetes_codes present, normal path."""
        measure = DiabetesEyeExam({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        eligibility = _make_eligibility(["P1"])
        value_sets = _diabetes_value_sets()

        result = measure.calculate_denominator(claims, eligibility, value_sets).collect()
        assert result.height == 1
        assert result["denominator_flag"][0] is True


class TestDiabetesEyeExamNumerator:
    """Tests for DiabetesEyeExam.calculate_numerator."""

    @pytest.mark.unit
    def test_numerator_empty_eye_exam_value_set(self):
        """Branch 568->569: empty eye exam codes marks none as having exam."""
        measure = DiabetesEyeExam({"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = _make_claims(["P1"])
        value_sets: dict[str, pl.LazyFrame] = {}

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.height == 1
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_numerator_with_eye_exam_value_set(self):
        """Branch 568->573: eye exam codes present, joins on procedure_code."""
        measure = DiabetesEyeExam({"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = _make_claims(["P1"], procedure_codes=["92250"])
        value_sets = _eye_exam_value_sets()

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.height == 1
        assert "numerator_flag" in result.columns


class TestDiabetesHbA1cTestingExclusions:
    """Cover DiabetesHbA1cTesting.calculate_exclusions line 324."""

    @pytest.mark.unit
    def test_no_exclusions(self):
        measure = DiabetesHbA1cTesting({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1", "P2"]}).lazy()
        claims = _make_claims(["P1"])
        result = measure.calculate_exclusions(denominator, claims, {}).collect()
        assert "exclusion_flag" in result.columns
        assert result["exclusion_flag"].to_list() == [False, False]


class TestDiabetesBPControlNumerator:
    """Cover DiabetesBPControl.calculate_numerator lines 417-429."""

    @pytest.mark.unit
    def test_numerator_placeholder(self):
        measure = DiabetesBPControl({"measurement_year": 2024})
        denominator = pl.DataFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        ).lazy()
        claims = _make_claims(["P1"])
        result = measure.calculate_numerator(denominator, claims, {}).collect()
        assert "numerator_flag" in result.columns
        assert result["numerator_flag"][0] is False


class TestDiabetesEyeExamExclusions:
    """Cover DiabetesEyeExam.calculate_exclusions line 608."""

    @pytest.mark.unit
    def test_no_exclusions(self):
        measure = DiabetesEyeExam({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1"]}).lazy()
        claims = _make_claims(["P1"])
        result = measure.calculate_exclusions(denominator, claims, {}).collect()
        assert "exclusion_flag" in result.columns
        assert result["exclusion_flag"][0] is False


class TestDiabetesDenominatorNoDiabetesCodes:
    """Cover branches where diabetes_codes is None (85->86, 237->238, 373->374, 522->523)."""

    @pytest.mark.unit
    def test_hba1c_poor_control_no_diabetes_codes(self):
        """Branch 85->86: Diabetes value set not found."""
        measure = DiabetesHbA1cPoorControl({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        elig = _make_eligibility(["P1"])
        result = measure.calculate_denominator(claims, elig, {}).collect()
        assert result.height == 0
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_hba1c_testing_no_diabetes_codes(self):
        """Branch 237->238: Diabetes value set not found for HbA1c Testing."""
        measure = DiabetesHbA1cTesting({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        elig = _make_eligibility(["P1"])
        result = measure.calculate_denominator(claims, elig, {}).collect()
        assert result.height == 0
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_bp_control_no_diabetes_codes(self):
        """Branch 373->374: Diabetes value set not found for BP Control."""
        measure = DiabetesBPControl({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        elig = _make_eligibility(["P1"])
        result = measure.calculate_denominator(claims, elig, {}).collect()
        assert result.height == 0
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_eye_exam_no_diabetes_codes(self):
        """Branch 522->523: Diabetes value set not found for Eye Exam."""
        measure = DiabetesEyeExam({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        elig = _make_eligibility(["P1"])
        result = measure.calculate_denominator(claims, elig, {}).collect()
        assert result.height == 0
        assert "denominator_flag" in result.columns


class TestDiabetesWithDiabetesCodes:
    """Cover branches where diabetes codes exist (85->94, 237->245, 373->381, 522->530)."""

    @pytest.mark.unit
    def test_hba1c_poor_control_with_diabetes(self):
        """Branch 85->94: Diabetes found, joins claims."""
        measure = DiabetesHbA1cPoorControl({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        elig = _make_eligibility(["P1"])
        result = measure.calculate_denominator(claims, elig, _diabetes_value_sets()).collect()
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_hba1c_testing_with_diabetes(self):
        """Branch 237->245: Diabetes found for HbA1c Testing."""
        measure = DiabetesHbA1cTesting({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        elig = _make_eligibility(["P1"])
        result = measure.calculate_denominator(claims, elig, _diabetes_value_sets()).collect()
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_bp_control_with_diabetes(self):
        """Branch 373->381: Diabetes found for BP Control."""
        measure = DiabetesBPControl({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        elig = _make_eligibility(["P1"])
        result = measure.calculate_denominator(claims, elig, _diabetes_value_sets()).collect()
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_eye_exam_with_diabetes(self):
        """Branch 522->530: Diabetes found for Eye Exam."""
        measure = DiabetesEyeExam({"measurement_year": 2024})
        claims = _make_claims(["P1"])
        elig = _make_eligibility(["P1"])
        result = measure.calculate_denominator(claims, elig, _diabetes_value_sets()).collect()
        assert "denominator_flag" in result.columns


class TestDiabetesNumeratorBranches:
    """Cover numerator branches: 145->146/157, 284->285/289, 441->442/447, 568->569/573."""

    @pytest.mark.unit
    def test_hba1c_poor_control_no_hba1c_codes(self):
        """Branch 145->146: HbA1c codes empty, marks all as numerator."""
        measure = DiabetesHbA1cPoorControl({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1"], "denominator_flag": [True]}).lazy()
        claims = _make_claims(["P1"])
        result = measure.calculate_numerator(denominator, claims, {}).collect()
        assert "numerator_flag" in result.columns

    @pytest.mark.unit
    def test_hba1c_poor_control_with_hba1c_codes(self):
        """Branch 145->157: HbA1c codes found."""
        measure = DiabetesHbA1cPoorControl({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1"], "denominator_flag": [True]}).lazy()
        claims = _make_claims(["P1"])
        result = measure.calculate_numerator(
            denominator, claims, _hba1c_value_sets()
        ).collect()
        assert "numerator_flag" in result.columns

    @pytest.mark.unit
    def test_hba1c_testing_no_hba1c_codes(self):
        """Branch 284->285: HbA1c empty for Testing measure."""
        measure = DiabetesHbA1cTesting({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1"], "denominator_flag": [True]}).lazy()
        claims = _make_claims(["P1"])
        result = measure.calculate_numerator(denominator, claims, {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_hba1c_testing_with_hba1c_codes(self):
        """Branch 284->289: HbA1c found for Testing."""
        measure = DiabetesHbA1cTesting({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1"], "denominator_flag": [True]}).lazy()
        claims = _make_claims(["P1"], procedure_codes=["83036"])
        result = measure.calculate_numerator(
            denominator, claims, _hba1c_value_sets()
        ).collect()
        assert "numerator_flag" in result.columns

    @pytest.mark.unit
    def test_bp_control_no_pregnancy_codes(self):
        """Branch 441->442: no Pregnancy value set."""
        measure = DiabetesBPControl({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1"]}).lazy()
        claims = _make_claims(["P1"])
        result = measure.calculate_exclusions(denominator, claims, {}).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_bp_control_with_pregnancy_codes(self):
        """Branch 441->447: Pregnancy codes present."""
        measure = DiabetesBPControl({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1"]}).lazy()
        claims = _make_claims(["P1"], diagnosis_codes=["O09.00"])
        vs = {"Pregnancy": pl.DataFrame({"code": ["O09.00"]}).lazy()}
        result = measure.calculate_exclusions(denominator, claims, vs).collect()
        assert "exclusion_flag" in result.columns

    @pytest.mark.unit
    def test_eye_exam_no_codes(self):
        """Branch 568->569: Eye Exam value set empty."""
        measure = DiabetesEyeExam({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1"], "denominator_flag": [True]}).lazy()
        claims = _make_claims(["P1"])
        result = measure.calculate_numerator(denominator, claims, {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_eye_exam_with_codes(self):
        """Branch 568->573: Eye Exam codes found."""
        measure = DiabetesEyeExam({"measurement_year": 2024})
        denominator = pl.DataFrame({"person_id": ["P1"], "denominator_flag": [True]}).lazy()
        claims = _make_claims(["P1"], procedure_codes=["67028"])
        vs = {
            "Diabetic Retinal Screening": pl.DataFrame({"code": ["67028"]}).lazy(),
        }
        result = measure.calculate_numerator(denominator, claims, vs).collect()
        assert "numerator_flag" in result.columns
