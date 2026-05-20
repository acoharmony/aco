# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.quality_measure_base module."""

from __future__ import annotations
from dataclasses import dataclass

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date

import polars as pl
import pytest
import acoharmony


def _make_claims(
    rows: list[dict],
    *,
    extra_schema: dict | None = None,
) -> pl.LazyFrame:
    """Build a LazyFrame with standard claims columns, filling missing cols with defaults."""
    defaults = {
        "person_id": "P001",
        "claim_id": "C001",
        "claim_type": "institutional",
        "bill_type_code": "110",
        "admission_date": date(2024, 3, 1),
        "discharge_date": date(2024, 3, 5),
        "diagnosis_code_1": "J18.9",
        "diagnosis_code_2": None,
        "diagnosis_code_3": None,
        "procedure_code_1": "99213",
        "facility_npi": "1234567890",
        "paid_amount": 1000.0,
        "allowed_amount": 1200.0,
        "claim_start_date": date(2024, 3, 1),
        "claim_end_date": date(2024, 3, 5),
        "revenue_code": "0100",
        "place_of_service_code": "21",
    }
    filled = []
    for row in rows:
        merged = {**defaults, **row}
        filled.append(merged)
    schema = {
        "person_id": pl.Utf8,
        "claim_id": pl.Utf8,
        "claim_type": pl.Utf8,
        "bill_type_code": pl.Utf8,
        "admission_date": pl.Date,
        "discharge_date": pl.Date,
        "diagnosis_code_1": pl.Utf8,
        "diagnosis_code_2": pl.Utf8,
        "diagnosis_code_3": pl.Utf8,
        "procedure_code_1": pl.Utf8,
        "facility_npi": pl.Utf8,
        "paid_amount": pl.Float64,
        "allowed_amount": pl.Float64,
        "claim_start_date": pl.Date,
        "claim_end_date": pl.Date,
        "revenue_code": pl.Utf8,
        "place_of_service_code": pl.Utf8,
    }
    if extra_schema:
        schema.update(extra_schema)
    return pl.DataFrame(filled, schema=schema).lazy()


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
class TestQualityMeasureBase:
    """Tests for quality measure base classes."""

    @pytest.mark.unit
    def test_import_quality_measure_base(self):
        from acoharmony._transforms._quality_measure_base import (
            MeasureFactory,
            MeasureMetadata,
            QualityMeasureBase,
        )
        assert MeasureMetadata is not None
        assert QualityMeasureBase is not None
        assert MeasureFactory is not None

    @pytest.mark.unit
    def test_measure_metadata_creation(self):
        from acoharmony._transforms._quality_measure_base import MeasureMetadata
        m = MeasureMetadata(
            measure_id="TEST001",
            measure_name="Test Measure",
            measure_steward="TEST",
            measure_version="2024",
            description="A test measure",
            numerator_description="test numerator",
            denominator_description="test denominator",
            exclusions_description="test exclusions",
        )
        assert m.measure_id == "TEST001"
        assert m.measure_name == "Test Measure"
        assert m.measure_steward == "TEST"
        assert m.exclusions_description == "test exclusions"

    @pytest.mark.unit
    def test_measure_metadata_optional_exclusions(self):
        from acoharmony._transforms._quality_measure_base import MeasureMetadata
        m = MeasureMetadata(
            measure_id="TEST002",
            measure_name="No Exclusions",
            measure_steward="TEST",
            measure_version="2024",
            description="Desc",
            numerator_description="num",
            denominator_description="den",
        )
        assert m.exclusions_description is None


class TestQualityPreventive:
    """Tests for preventive quality measures."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import _quality_preventive
        assert acoharmony._transforms._quality_preventive is not None

    @pytest.mark.unit
    def test_breast_cancer_screening_metadata(self):
        from acoharmony._transforms._quality_preventive import BreastCancerScreening
        m = BreastCancerScreening()
        meta = m.get_metadata()
        assert meta.measure_id == "NQF2372"
        assert "Breast Cancer" in meta.measure_name
        assert meta.measure_steward == "NCQA"

    @pytest.mark.unit
    def test_breast_cancer_screening_is_quality_measure(self):
        from acoharmony._transforms._quality_measure_base import QualityMeasureBase
        from acoharmony._transforms._quality_preventive import BreastCancerScreening
        assert issubclass(BreastCancerScreening, QualityMeasureBase)


class TestMeasureMetadata:
    """Tests for the MeasureMetadata dataclass."""

    @pytest.mark.unit
    def test_create_with_required_fields(self):
        from acoharmony._transforms._quality_measure_base import MeasureMetadata

        meta = MeasureMetadata(
            measure_id="TEST001",
            measure_name="Test Measure",
            measure_steward="NQF",
            measure_version="2024",
            description="A test measure",
            numerator_description="People who pass",
            denominator_description="All eligible people",
        )
        assert meta.measure_id == "TEST001"
        assert meta.exclusions_description is None

    @pytest.mark.unit
    def test_create_with_exclusions(self):
        from acoharmony._transforms._quality_measure_base import MeasureMetadata

        meta = MeasureMetadata(
            measure_id="TEST002",
            measure_name="Test Measure 2",
            measure_steward="NCQA",
            measure_version="2024",
            description="Another test",
            numerator_description="Num",
            denominator_description="Den",
            exclusions_description="Hospice",
        )
        assert meta.exclusions_description == "Hospice"


class TestQualityMeasureBaseV2:
    """Tests for QualityMeasureBase abstract class and calculate flow."""

    def _create_concrete_measure(self, has_exclusions=False):
        from acoharmony._transforms._quality_measure_base import MeasureMetadata, QualityMeasureBase

        class TestMeasure(QualityMeasureBase):
            def get_metadata(self):
                return MeasureMetadata(
                    measure_id="TEST001",
                    measure_name="Test Measure",
                    measure_steward="NQF",
                    measure_version="2024",
                    description="Test",
                    numerator_description="Num",
                    denominator_description="Den",
                )

            def calculate_denominator(self, claims, eligibility, value_sets):
                return (
                    eligibility.select("person_id")
                    .unique()
                    .with_columns([pl.lit(True).alias("denominator_flag")])
                )

            def calculate_numerator(self, denominator, claims, value_sets):
                return denominator.with_columns([pl.lit(True).alias("numerator_flag")])

            if has_exclusions:
                def calculate_exclusions(self, denominator, claims, value_sets):
                    return denominator.select("person_id").with_columns(
                        [pl.lit(True).alias("exclusion_flag")]
                    )

        return TestMeasure

    @pytest.mark.unit
    def test_init_sets_metadata(self):
        cls = self._create_concrete_measure()
        measure = cls()
        assert measure.metadata.measure_id == "TEST001"
        assert measure.config == {}

    @pytest.mark.unit
    def test_init_with_config(self):
        cls = self._create_concrete_measure()
        measure = cls(config={"measurement_year": 2025})
        assert measure.config["measurement_year"] == 2025

    @pytest.mark.unit
    def test_calculate_full_flow(self):
        cls = self._create_concrete_measure()
        measure = cls()

        claims = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["E11", "E12"],
        }).lazy()
        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2"],
        }).lazy()

        result = measure.calculate(claims, eligibility, {})
        df = result.collect()
        assert "measure_id" in df.columns
        assert "measure_name" in df.columns
        assert "numerator_flag" in df.columns
        assert "exclusion_flag" in df.columns
        assert "performance_met" in df.columns
        assert df["measure_id"][0] == "TEST001"

    @pytest.mark.unit
    def test_default_exclusions_false(self):
        cls = self._create_concrete_measure()
        measure = cls()

        eligibility = pl.DataFrame({"person_id": ["P1", "P2"]}).lazy()
        claims = pl.DataFrame({"person_id": ["P1"]}).lazy()
        result = measure.calculate(claims, eligibility, {}).collect()
        # Default exclusions should all be False
        assert result["exclusion_flag"].to_list() == [False, False]

    @pytest.mark.unit
    def test_performance_met_logic(self):
        """performance_met = numerator_flag AND NOT exclusion_flag."""
        cls = self._create_concrete_measure()
        measure = cls()

        eligibility = pl.DataFrame({"person_id": ["P1"]}).lazy()
        claims = pl.DataFrame({"person_id": ["P1"]}).lazy()
        result = measure.calculate(claims, eligibility, {}).collect()
        # numerator=True, exclusion=False => performance_met=True
        assert result["performance_met"][0] is True

    @pytest.mark.unit
    def test_calculate_summary(self):
        from acoharmony._transforms._quality_measure_base import QualityMeasureBase

        measure_results = pl.DataFrame({
            "person_id": ["P1", "P2", "P3", "P4"],
            "measure_id": ["M1", "M1", "M1", "M1"],
            "measure_name": ["Test", "Test", "Test", "Test"],
            "denominator_flag": [True, True, True, True],
            "numerator_flag": [True, True, False, False],
            "exclusion_flag": [False, True, False, False],
            "performance_met": [True, False, False, False],
        }).lazy()

        summary = QualityMeasureBase.calculate_summary(measure_results).collect()
        assert summary.height == 1
        assert summary["denominator_count"][0] == 4
        assert summary["numerator_count"][0] == 2
        assert summary["exclusion_count"][0] == 1
        assert summary["performance_count"][0] == 1
        # rate = 1 / (4 - 1) = 0.333...
        assert abs(summary["performance_rate"][0] - 1 / 3) < 0.01


class TestMeasureFactory:
    """Tests for MeasureFactory registration and creation."""

    def setup_method(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        self._orig = MeasureFactory._registry.copy()

    def teardown_method(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        MeasureFactory._registry = self._orig

    @pytest.mark.unit
    def test_register_and_create(self):
        from acoharmony._transforms._quality_measure_base import (
            MeasureFactory,
            MeasureMetadata,
            QualityMeasureBase,
        )

        class FakeMeasure(QualityMeasureBase):
            def get_metadata(self):
                return MeasureMetadata(
                    measure_id="FAKE01",
                    measure_name="Fake",
                    measure_steward="Test",
                    measure_version="1.0",
                    description="Fake",
                    numerator_description="N",
                    denominator_description="D",
                )

            def calculate_denominator(self, claims, eligibility, value_sets):
                return eligibility.select("person_id").with_columns([pl.lit(True).alias("denominator_flag")])

            def calculate_numerator(self, denominator, claims, value_sets):
                return denominator.with_columns([pl.lit(False).alias("numerator_flag")])

        MeasureFactory.register("FAKE01", FakeMeasure)
        instance = MeasureFactory.create("FAKE01", config={"year": 2024})
        assert instance.metadata.measure_id == "FAKE01"
        assert instance.config == {"year": 2024}

    @pytest.mark.unit
    def test_create_nonexistent_raises(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        MeasureFactory._registry = {}
        with pytest.raises(KeyError, match="not registered"):
            MeasureFactory.create("NONEXISTENT")

    @pytest.mark.unit
    def test_list_measures(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        # After importing quality_diabetes, measures should be registered
        measures = MeasureFactory.list_measures()
        assert isinstance(measures, list)


class TestPreventiveQualityMeasures:
    """Tests for preventive care quality measures."""

    def _make_claims(self):
        return pl.DataFrame({
            "person_id": ["P1", "P2"],
            "diagnosis_code_1": ["Z12.31", "C50"],
            "procedure_code": ["77067", "45378"],
            "claim_end_date": [datetime.date(2024, 6, 1), datetime.date(2024, 9, 1)],
        }).lazy()

    def _make_eligibility(self):
        return pl.DataFrame({
            "person_id": ["P1", "P2"],
            "age": [55, 65],
            "gender": ["female", "female"],
            "enrollment_start_date": [datetime.date(2023, 1, 1), datetime.date(2023, 1, 1)],
            "enrollment_end_date": [datetime.date(2024, 12, 31), datetime.date(2024, 12, 31)],
        }).lazy()

    @pytest.mark.unit
    def test_breast_cancer_screening_metadata(self):
        from acoharmony._transforms._quality_preventive import BreastCancerScreening

        m = BreastCancerScreening(config={"measurement_year": 2024})
        assert m.get_metadata().measure_id == "NQF2372"

    @pytest.mark.unit
    def test_breast_cancer_screening_denominator(self):
        from acoharmony._transforms._quality_preventive import BreastCancerScreening

        m = BreastCancerScreening(config={"measurement_year": 2024})
        result = m.calculate_denominator(
            self._make_claims(), self._make_eligibility(), {}
        ).collect()
        # Both women aged 50-74
        assert result.height == 2

    @pytest.mark.unit
    def test_breast_cancer_screening_numerator_no_codes(self):
        from acoharmony._transforms._quality_preventive import BreastCancerScreening

        m = BreastCancerScreening(config={"measurement_year": 2024})
        denom = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        result = m.calculate_numerator(denom, self._make_claims(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_breast_cancer_screening_numerator_with_codes(self):
        from acoharmony._transforms._quality_preventive import BreastCancerScreening

        m = BreastCancerScreening(config={"measurement_year": 2024})
        denom = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        value_sets = {"Mammography": pl.DataFrame({"code": ["77067"]}).lazy()}
        result = m.calculate_numerator(denom, self._make_claims(), value_sets).collect()
        assert result["numerator_flag"][0] is True

    @pytest.mark.unit
    def test_breast_cancer_exclusions_no_codes(self):
        from acoharmony._transforms._quality_preventive import BreastCancerScreening

        m = BreastCancerScreening(config={"measurement_year": 2024})
        denom = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        result = m.calculate_exclusions(denom, self._make_claims(), {}).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_breast_cancer_exclusions_with_codes(self):
        from acoharmony._transforms._quality_preventive import BreastCancerScreening

        m = BreastCancerScreening(config={"measurement_year": 2024})
        denom = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "denominator_flag": [True, True],
        }).lazy()
        claims = pl.DataFrame({
            "person_id": ["P2"],
            "diagnosis_code_1": ["C50"],
            "procedure_code": ["99213"],
            "claim_end_date": [datetime.date(2024, 3, 1)],
        }).lazy()
        value_sets = {"Breast Cancer": pl.DataFrame({"code": ["C50"]}).lazy()}
        result = m.calculate_exclusions(denom, claims, value_sets).collect()
        # P2 has breast cancer diagnosis
        p2_row = result.filter(pl.col("person_id") == "P2")
        assert p2_row["exclusion_flag"][0] is True

    @pytest.mark.unit
    def test_colorectal_screening_metadata(self):
        from acoharmony._transforms._quality_preventive import ColorectalCancerScreening

        m = ColorectalCancerScreening(config={"measurement_year": 2024})
        assert m.get_metadata().measure_id == "NQF0034"

    @pytest.mark.unit
    def test_colorectal_screening_denominator(self):
        from acoharmony._transforms._quality_preventive import ColorectalCancerScreening

        m = ColorectalCancerScreening(config={"measurement_year": 2024})
        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2", "P3"],
            "age": [50, 76, 45],
            "enrollment_start_date": [datetime.date(2023, 1, 1)] * 3,
            "enrollment_end_date": [datetime.date(2024, 12, 31)] * 3,
        }).lazy()
        result = m.calculate_denominator(self._make_claims(), eligibility, {}).collect()
        # P1 (50) and P3 (45) in range 45-75
        assert result.height == 2

    @pytest.mark.unit
    def test_colorectal_screening_numerator_with_codes(self):
        from acoharmony._transforms._quality_preventive import ColorectalCancerScreening

        m = ColorectalCancerScreening(config={"measurement_year": 2024})
        denom = pl.DataFrame({
            "person_id": ["P2"],
            "denominator_flag": [True],
        }).lazy()
        value_sets = {"Colonoscopy": pl.DataFrame({"code": ["45378"]}).lazy()}
        result = m.calculate_numerator(denom, self._make_claims(), value_sets).collect()
        assert result["numerator_flag"][0] is True

    @pytest.mark.unit
    def test_colorectal_screening_numerator_no_codes(self):
        from acoharmony._transforms._quality_preventive import ColorectalCancerScreening

        m = ColorectalCancerScreening(config={"measurement_year": 2024})
        denom = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        result = m.calculate_numerator(denom, self._make_claims(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_annual_wellness_visit_metadata(self):
        from acoharmony._transforms._quality_preventive import AnnualWellnessVisit

        m = AnnualWellnessVisit(config={"measurement_year": 2024})
        assert m.get_metadata().measure_id == "AWV"

    @pytest.mark.unit
    def test_annual_wellness_visit_denominator(self):
        from acoharmony._transforms._quality_preventive import AnnualWellnessVisit

        m = AnnualWellnessVisit(config={"measurement_year": 2024})
        result = m.calculate_denominator(
            self._make_claims(), self._make_eligibility(), {}
        ).collect()
        assert result.height == 2  # All enrolled members

    @pytest.mark.unit
    def test_annual_wellness_numerator_no_codes(self):
        from acoharmony._transforms._quality_preventive import AnnualWellnessVisit

        m = AnnualWellnessVisit(config={"measurement_year": 2024})
        denom = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        result = m.calculate_numerator(denom, self._make_claims(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_annual_wellness_exclusions_no_hospice(self):
        from acoharmony._transforms._quality_preventive import AnnualWellnessVisit

        m = AnnualWellnessVisit(config={"measurement_year": 2024})
        denom = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        result = m.calculate_exclusions(denom, self._make_claims(), {}).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_annual_wellness_exclusions_with_hospice(self):
        from acoharmony._transforms._quality_preventive import AnnualWellnessVisit

        m = AnnualWellnessVisit(config={"measurement_year": 2024})
        denom = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["Z51.5"],
            "procedure_code": ["HOSP01"],
            "claim_end_date": [datetime.date(2024, 6, 1)],
        }).lazy()
        value_sets = {"Hospice Encounter": pl.DataFrame({"code": ["HOSP01"]}).lazy()}
        result = m.calculate_exclusions(denom, claims, value_sets).collect()
        assert result["exclusion_flag"][0] is True

    @pytest.mark.unit
    def test_preventive_measures_registered_in_factory(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        measures = MeasureFactory.list_measures()
        assert "NQF2372" in measures
        assert "NQF0034" in measures
        assert "AWV" in measures


class TestQualityMeasureCalculateIntegration:
    """Integration tests for the full calculate() flow."""

    @pytest.mark.unit
    def test_diabetes_hba1c_full_calculate(self):
        from acoharmony._transforms._quality_diabetes import DiabetesHbA1cPoorControl

        m = DiabetesHbA1cPoorControl(config={"measurement_year": 2024})

        claims = pl.DataFrame({
            "person_id": ["P1", "P2", "P1"],
            "diagnosis_code_1": ["E11.9", "E11.9", "I10"],
            "procedure_code": ["83036", "99213", "99213"],
            "claim_end_date": [
                datetime.date(2024, 3, 1),
                datetime.date(2024, 6, 1),
                datetime.date(2024, 9, 1),
            ],
        }).lazy()
        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "age": [55, 70],
            "enrollment_start_date": [datetime.date(2023, 1, 1)] * 2,
            "enrollment_end_date": [datetime.date(2024, 12, 31)] * 2,
        }).lazy()
        value_sets = {
            "Diabetes": pl.DataFrame({"code": ["E11.9"]}).lazy(),
        }

        result = m.calculate(claims, eligibility, value_sets).collect()
        assert result.height == 2
        assert "measure_id" in result.columns
        assert "performance_met" in result.columns

    @pytest.mark.unit
    def test_calculate_summary_integration(self):
        from acoharmony._transforms._quality_diabetes import DiabetesHbA1cPoorControl
        from acoharmony._transforms._quality_measure_base import QualityMeasureBase

        m = DiabetesHbA1cPoorControl(config={"measurement_year": 2024})

        claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["E11.9"],
            "procedure_code": ["83036"],
            "claim_end_date": [datetime.date(2024, 3, 1)],
        }).lazy()
        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [55],
            "enrollment_start_date": [datetime.date(2023, 1, 1)],
            "enrollment_end_date": [datetime.date(2024, 12, 31)],
        }).lazy()
        value_sets = {"Diabetes": pl.DataFrame({"code": ["E11.9"]}).lazy()}

        measure_results = m.calculate(claims, eligibility, value_sets)
        summary = QualityMeasureBase.calculate_summary(measure_results).collect()
        assert summary.height == 1
        assert "performance_rate" in summary.columns


class TestQualityModulesImport:
    """Test that quality modules import and register measures."""

    @pytest.mark.unit
    def test_quality_modules_register_measures(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        # After all quality modules are imported, we should have many measures
        measures = MeasureFactory.list_measures()
        assert len(measures) >= 7  # At least diabetes(4) + preventive(3) + ACR(1)

    @pytest.mark.unit
    def test_measure_factory_create_diabetes(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        m = MeasureFactory.create("NQF0059")
        assert m.metadata.measure_id == "NQF0059"

    @pytest.mark.unit
    def test_measure_factory_create_preventive(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        m = MeasureFactory.create("NQF2372")
        assert m.metadata.measure_id == "NQF2372"

    @pytest.mark.unit
    def test_measure_factory_create_acr(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        m = MeasureFactory.create("NQF1789")
        assert m.metadata.measure_id == "NQF1789"


class TestCalculateSummaryZeroDenominator:
    """Test calculate_summary when denominator minus exclusions is zero."""

    @pytest.mark.unit
    def test_zero_denominator_rate(self):
        from acoharmony._transforms._quality_measure_base import QualityMeasureBase

        measure_results = pl.DataFrame({
            "person_id": ["P1"],
            "measure_id": ["M1"],
            "measure_name": ["Test"],
            "denominator_flag": [True],
            "numerator_flag": [False],
            "exclusion_flag": [True],  # All excluded
            "performance_met": [False],
        }).lazy()

        summary = QualityMeasureBase.calculate_summary(measure_results).collect()
        # denominator - exclusions = 0 -> NaN -> filled to 0.0
        assert summary["performance_rate"][0] == 0.0


class TestMedicationAdherenceMeasures:
    """Tests for medication adherence quality measures."""

    def _make_claims(self):
        return pl.DataFrame({
            "person_id": ["P1", "P1", "P2", "P3"],
            "diagnosis_code_1": ["I25.10", "E11.9", "I25.10", "Z99"],
            "procedure_code": ["STATIN1", "ACE1", "STATIN1", "OTHER"],
            "claim_end_date": [
                date(2024, 3, 1),
                date(2024, 6, 1),
                date(2024, 4, 1),
                date(2024, 5, 1),
            ],
        }).lazy()

    def _make_eligibility(self):
        return pl.DataFrame({
            "person_id": ["P1", "P2", "P3"],
            "enrollment_start_date": [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 1, 1),
            ],
            "enrollment_end_date": [
                date(2024, 12, 31),
                date(2024, 12, 31),
                date(2024, 12, 31),
            ],
            "age": [70, 65, 50],
        }).lazy()

    @pytest.mark.unit
    def test_statin_adherence_metadata(self):
        from acoharmony._transforms._quality_medication_adherence import (
            StatinAdherencePQA,
        )

        measure = StatinAdherencePQA({"measurement_year": 2024})
        meta = measure.get_metadata()
        assert meta.measure_id == "PQA_STATIN"
        assert "statin" in meta.measure_name.lower()

    @pytest.mark.unit
    def test_statin_adherence_denominator_no_value_sets(self):
        from acoharmony._transforms._quality_medication_adherence import (
            StatinAdherencePQA,
        )

        measure = StatinAdherencePQA({"measurement_year": 2024})
        result = measure.calculate_denominator(
            self._make_claims(), self._make_eligibility(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_statin_adherence_denominator_with_cvd_no_statin(self):
        from acoharmony._transforms._quality_medication_adherence import (
            StatinAdherencePQA,
        )

        measure = StatinAdherencePQA({"measurement_year": 2024})
        value_sets = {
            "Myocardial Infarction": pl.DataFrame({
                "code": ["I25.10"],
            }).lazy(),
        }
        result = measure.calculate_denominator(
            self._make_claims(), self._make_eligibility(), value_sets
        ).collect()
        # No statin value set -> empty
        assert result.height == 0

    @pytest.mark.unit
    def test_statin_adherence_denominator_full(self):
        from acoharmony._transforms._quality_medication_adherence import (
            StatinAdherencePQA,
        )

        measure = StatinAdherencePQA({"measurement_year": 2024})
        value_sets = {
            "Myocardial Infarction": pl.DataFrame({"code": ["I25.10"]}).lazy(),
            "Statin Medications": pl.DataFrame({"code": ["STATIN1"]}).lazy(),
        }
        result = measure.calculate_denominator(
            self._make_claims(), self._make_eligibility(), value_sets
        ).collect()
        assert result.height > 0
        assert "denominator_flag" in result.columns

    @pytest.mark.unit
    def test_statin_numerator_placeholder(self):
        from acoharmony._transforms._quality_medication_adherence import (
            StatinAdherencePQA,
        )

        measure = StatinAdherencePQA({"measurement_year": 2024})
        denominator = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        result = measure.calculate_numerator(denominator, self._make_claims(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_statin_exclusions_no_value_sets(self):
        from acoharmony._transforms._quality_medication_adherence import (
            StatinAdherencePQA,
        )

        measure = StatinAdherencePQA({"measurement_year": 2024})
        denominator = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        result = measure.calculate_exclusions(denominator, self._make_claims(), {}).collect()
        assert result["exclusion_flag"][0] is False

    @pytest.mark.unit
    def test_statin_exclusions_with_esrd(self):
        from acoharmony._transforms._quality_medication_adherence import (
            StatinAdherencePQA,
        )

        measure = StatinAdherencePQA({"measurement_year": 2024})
        claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["ESRD1"],
            "claim_end_date": [date(2024, 3, 1)],
        }).lazy()
        value_sets = {
            "ESRD": pl.DataFrame({"code": ["ESRD1"]}).lazy(),
        }
        denominator = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        # Should have exclusion_flag = True for P1
        assert result.height > 0

    @pytest.mark.unit
    def test_ace_arb_metadata(self):
        from acoharmony._transforms._quality_medication_adherence import (
            ACEARBAdherenceDiabetes,
        )

        measure = ACEARBAdherenceDiabetes({"measurement_year": 2024})
        meta = measure.get_metadata()
        assert meta.measure_id == "PQA_ACEARB"

    @pytest.mark.unit
    def test_ace_arb_denominator_no_diabetes(self):
        from acoharmony._transforms._quality_medication_adherence import (
            ACEARBAdherenceDiabetes,
        )

        measure = ACEARBAdherenceDiabetes({"measurement_year": 2024})
        result = measure.calculate_denominator(
            self._make_claims(), self._make_eligibility(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_ace_arb_denominator_no_meds(self):
        from acoharmony._transforms._quality_medication_adherence import (
            ACEARBAdherenceDiabetes,
        )

        measure = ACEARBAdherenceDiabetes({"measurement_year": 2024})
        value_sets = {
            "Diabetes": pl.DataFrame({"code": ["E11.9"]}).lazy(),
        }
        result = measure.calculate_denominator(
            self._make_claims(), self._make_eligibility(), value_sets
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_ace_arb_numerator_placeholder(self):
        from acoharmony._transforms._quality_medication_adherence import (
            ACEARBAdherenceDiabetes,
        )

        measure = ACEARBAdherenceDiabetes({"measurement_year": 2024})
        denominator = pl.DataFrame({
            "person_id": ["P1"],
            "denominator_flag": [True],
        }).lazy()
        result = measure.calculate_numerator(denominator, self._make_claims(), {}).collect()
        assert result["numerator_flag"][0] is False

    @pytest.mark.unit
    def test_oral_diabetes_metadata(self):
        from acoharmony._transforms._quality_medication_adherence import (
            OralDiabetesMedicationAdherence,
        )

        measure = OralDiabetesMedicationAdherence({"measurement_year": 2024})
        meta = measure.get_metadata()
        assert meta.measure_id == "PQA_DIABETES"

    @pytest.mark.unit
    def test_oral_diabetes_denominator_no_value_sets(self):
        from acoharmony._transforms._quality_medication_adherence import (
            OralDiabetesMedicationAdherence,
        )

        measure = OralDiabetesMedicationAdherence({"measurement_year": 2024})
        result = measure.calculate_denominator(
            self._make_claims(), self._make_eligibility(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_oral_diabetes_no_med_value_set(self):
        from acoharmony._transforms._quality_medication_adherence import (
            OralDiabetesMedicationAdherence,
        )

        measure = OralDiabetesMedicationAdherence({"measurement_year": 2024})
        value_sets = {
            "Diabetes": pl.DataFrame({"code": ["E11.9"]}).lazy(),
        }
        result = measure.calculate_denominator(
            self._make_claims(), self._make_eligibility(), value_sets
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_hypertension_metadata(self):
        from acoharmony._transforms._quality_medication_adherence import (
            HypertensionMedicationAdherence,
        )

        measure = HypertensionMedicationAdherence({"measurement_year": 2024})
        meta = measure.get_metadata()
        assert meta.measure_id == "PQA_HYPERTENSION"

    @pytest.mark.unit
    def test_hypertension_denominator_no_value_sets(self):
        from acoharmony._transforms._quality_medication_adherence import (
            HypertensionMedicationAdherence,
        )

        measure = HypertensionMedicationAdherence({"measurement_year": 2024})
        result = measure.calculate_denominator(
            self._make_claims(), self._make_eligibility(), {}
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_hypertension_no_med_value_sets(self):
        from acoharmony._transforms._quality_medication_adherence import (
            HypertensionMedicationAdherence,
        )

        measure = HypertensionMedicationAdherence({"measurement_year": 2024})
        value_sets = {
            "Essential Hypertension": pl.DataFrame({"code": ["I10"]}).lazy(),
        }
        result = measure.calculate_denominator(
            self._make_claims(), self._make_eligibility(), value_sets
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_measure_factory(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        # These should be registered at import time
        measures = MeasureFactory.list_measures()
        assert "PQA_STATIN" in measures
        assert "PQA_ACEARB" in measures
        assert "PQA_DIABETES" in measures
        assert "PQA_HYPERTENSION" in measures

    @pytest.mark.unit
    def test_measure_factory_create(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        measure = MeasureFactory.create("PQA_STATIN", {"measurement_year": 2024})
        assert measure.metadata.measure_id == "PQA_STATIN"

    @pytest.mark.unit
    def test_measure_factory_unknown(self):
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        with pytest.raises(KeyError):
            MeasureFactory.create("NONEXISTENT_MEASURE")


class TestQualityMeasureBaseV3:
    """Tests for QualityMeasureBase calculate and summary methods."""

    @pytest.mark.unit
    def test_calculate_summary(self):
        from acoharmony._transforms._quality_measure_base import QualityMeasureBase

        results = pl.DataFrame({
            "person_id": ["P1", "P2", "P3", "P4"],
            "measure_id": ["M1", "M1", "M1", "M1"],
            "measure_name": ["Test", "Test", "Test", "Test"],
            "denominator_flag": [True, True, True, True],
            "numerator_flag": [True, True, False, False],
            "exclusion_flag": [False, False, True, False],
            "performance_met": [True, True, False, False],
        }).lazy()

        summary = QualityMeasureBase.calculate_summary(results).collect()
        assert summary["denominator_count"][0] == 4
        assert summary["numerator_count"][0] == 2
        assert summary["exclusion_count"][0] == 1
        assert summary["performance_count"][0] == 2

from acoharmony._transforms._quality_measure_base import (  # noqa: E402
    MeasureFactory,
    MeasureMetadata,
    QualityMeasureBase,
)


class ConcreteMeasure(QualityMeasureBase):
    """Concrete implementation for testing."""

    def get_metadata(self) -> MeasureMetadata:
        return MeasureMetadata(
            measure_id="TEST001",
            measure_name="Test Measure",
            measure_steward="TEST",
            measure_version="2024",
            description="Test measure for unit testing.",
            numerator_description="Patients meeting criteria",
            denominator_description="All eligible patients",
        )

    def calculate_denominator(self, claims, eligibility, value_sets):
        return eligibility.select("person_id").unique().with_columns(
            [pl.lit(True).alias("denominator_flag")]
        )

    def calculate_numerator(self, denominator, claims, value_sets):
        return denominator.with_columns(
            [pl.lit(True).alias("numerator_flag")]
        )


class TestMeasureMetadataV2:
    """Tests for MeasureMetadata."""

    @pytest.mark.unit
    def test_fields(self):
        metadata = MeasureMetadata(
            measure_id="NQF0001",
            measure_name="Test",
            measure_steward="NCQA",
            measure_version="2024",
            description="desc",
            numerator_description="num desc",
            denominator_description="denom desc",
            exclusions_description="excl desc",
        )
        assert metadata.measure_id == "NQF0001"
        assert metadata.exclusions_description == "excl desc"

    @pytest.mark.unit
    def test_optional_exclusions_default_none(self):
        metadata = MeasureMetadata(
            measure_id="NQF0002",
            measure_name="Test",
            measure_steward="NCQA",
            measure_version="2024",
            description="desc",
            numerator_description="num",
            denominator_description="denom",
        )
        assert metadata.exclusions_description is None


class TestMeasureFactoryV2:
    """Tests for MeasureFactory."""

    @pytest.mark.unit
    def test_register_and_create(self):
        """Register and create a measure."""
        MeasureFactory.register("TEST001", ConcreteMeasure)
        measure = MeasureFactory.create("TEST001")
        assert isinstance(measure, ConcreteMeasure)

    @pytest.mark.unit
    def test_create_with_config(self):
        """Create measure with configuration."""
        MeasureFactory.register("TEST002", ConcreteMeasure)
        measure = MeasureFactory.create("TEST002", config={"measurement_year": 2024})
        assert measure.config["measurement_year"] == 2024


class TestQualityMeasureBaseCalculate:
    """Tests for QualityMeasureBase.calculate."""

    @pytest.mark.unit
    def test_full_calculation(self):
        """Run full measure calculation pipeline."""
        measure = ConcreteMeasure()
        claims = pl.DataFrame(
            {
                "person_id": ["P1", "P2", "P3"],
                "claim_end_date": [date(2024, 3, 1)] * 3,
                "diagnosis_code_1": ["I10", "E11", "I10"],
            }
        ).lazy()

        eligibility = pl.DataFrame(
            {
                "person_id": ["P1", "P2", "P3"],
                "age": [45, 55, 35],
            }
        ).lazy()

        result = measure.calculate(claims, eligibility, {}).collect()
        assert "measure_id" in result.columns
        assert "measure_name" in result.columns
        assert "denominator_flag" in result.columns
        assert "numerator_flag" in result.columns
        assert "exclusion_flag" in result.columns
        assert "performance_met" in result.columns

        # All should be in denominator and numerator (ConcreteMeasure always returns True)
        assert result["denominator_flag"].sum() == 3
        assert result["numerator_flag"].sum() == 3
        assert result["exclusion_flag"].sum() == 0
        assert result["performance_met"].sum() == 3

    @pytest.mark.unit
    def test_calculate_summary(self):
        """Calculate summary statistics from measure results."""
        results = pl.DataFrame(
            {
                "person_id": ["P1", "P2", "P3", "P4"],
                "measure_id": ["TEST001"] * 4,
                "measure_name": ["Test Measure"] * 4,
                "denominator_flag": [True, True, True, True],
                "numerator_flag": [True, True, False, False],
                "exclusion_flag": [False, False, False, True],
                "performance_met": [True, True, False, False],
            }
        ).lazy()

        summary = QualityMeasureBase.calculate_summary(results).collect()
        assert summary["denominator_count"][0] == 4
        assert summary["numerator_count"][0] == 2
        assert summary["exclusion_count"][0] == 1
        assert summary["performance_count"][0] == 2


class TestMeasureFactoryCreateBranches:
    """Cover branches 302->303 (measure_id not in registry) and 302->306 (found)."""

    @pytest.mark.unit
    def test_create_unregistered_measure_raises_key_error(self):
        """Branch 302->303: measure_id not registered raises KeyError."""
        from acoharmony._transforms._quality_measure_base import MeasureFactory

        with pytest.raises(KeyError, match="not registered"):
            MeasureFactory.create("totally_nonexistent_measure_xyz", {})

    @pytest.mark.unit
    def test_create_registered_measure(self):
        """Branch 302->306: measure_id is in registry, creates instance."""
        from acoharmony._transforms._quality_measure_base import MeasureFactory, MeasureMetadata

        class _TestMeasure(QualityMeasureBase):
            def calculate_denominator(self, *a, **kw):
                pass
            def calculate_numerator(self, *a, **kw):
                pass
            @classmethod
            def get_metadata(cls):
                return MeasureMetadata(
                    measure_id="_branch_test_create",
                    measure_name="Test",
                    measure_steward="NQF",
                    measure_version="1.0",
                    description="Test measure",
                    numerator_description="Test numerator",
                    denominator_description="Test denominator",
                )

        MeasureFactory._registry["_branch_test_create"] = _TestMeasure
        try:
            instance = MeasureFactory.create("_branch_test_create", {"year": 2025})
            assert isinstance(instance, _TestMeasure)
        finally:
            del MeasureFactory._registry["_branch_test_create"]
