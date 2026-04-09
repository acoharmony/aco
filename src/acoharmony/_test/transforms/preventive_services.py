# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.preventive_services module."""

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


def _lazy(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestPreventiveServicesPublic:
    """Tests for preventive_services public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import preventive_services
        assert preventive_services is not None

    @pytest.mark.unit
    def test_preventive_services_transform_class(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform
        assert PreventiveServicesTransform is not None


class TestPreventiveIdentifyServices:
    """Tests for PreventiveServicesTransform.identify_preventive_services."""

    def _make_claims(self) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1", "P2", "P3", "P4"],
            "claim_id": ["C1", "C2", "C3", "C4"],
            "claim_type": ["professional", "professional", "professional", "institutional"],
            "claim_end_date": [
                date(2024, 3, 1), date(2024, 6, 1),
                date(2024, 9, 1), date(2024, 1, 1),
            ],
            "procedure_code": ["G0438", "77067", "99213", "G0438"],
        })

    @pytest.mark.unit
    def test_identifies_preventive_codes(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_claims()
        result = collect(
            PreventiveServicesTransform.identify_preventive_services(claims, DEFAULT_CONFIG)
        )
        # C1 (G0438=AWV, professional, 2024) and C2 (77067=mammogram, professional, 2024)
        # C3 (99213 is E&M, not in preventive codes)
        # C4 (institutional, excluded)
        assert result.shape[0] == 2
        codes = result["procedure_code"].to_list()
        assert "G0438" in codes
        assert "77067" in codes

    @pytest.mark.unit
    def test_excludes_non_measurement_year(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = pl.LazyFrame({
            "person_id": ["P1"],
            "claim_id": ["C1"],
            "claim_type": ["professional"],
            "claim_end_date": [date(2023, 3, 1)],
            "procedure_code": ["G0438"],
        })
        result = collect(
            PreventiveServicesTransform.identify_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_excludes_non_professional(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = pl.LazyFrame({
            "person_id": ["P1"],
            "claim_id": ["C1"],
            "claim_type": ["institutional"],
            "claim_end_date": [date(2024, 3, 1)],
            "procedure_code": ["G0438"],
        })
        result = collect(
            PreventiveServicesTransform.identify_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result.shape[0] == 0


class TestPreventiveCategorizeServices:
    """Tests for PreventiveServicesTransform.categorize_preventive_services."""

    def _make_prev_claims(self, procedure_codes: list[str]) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": [f"P{i}" for i in range(len(procedure_codes))],
            "claim_id": [f"C{i}" for i in range(len(procedure_codes))],
            "procedure_code": procedure_codes,
            "claim_end_date": [date(2024, 6, 1)] * len(procedure_codes),
        })

    @pytest.mark.unit
    def test_awv_categorization(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_prev_claims(["G0438"])
        result = collect(
            PreventiveServicesTransform.categorize_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result["preventive_service_type"][0] == "annual_wellness_visit"
        assert result["preventive_service_category"][0] == "wellness_visit"

    @pytest.mark.unit
    def test_mammogram_categorization(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_prev_claims(["77067"])
        result = collect(
            PreventiveServicesTransform.categorize_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result["preventive_service_type"][0] == "mammogram"
        assert result["preventive_service_category"][0] == "cancer_screening"

    @pytest.mark.unit
    def test_flu_vaccine_categorization(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_prev_claims(["90656"])
        result = collect(
            PreventiveServicesTransform.categorize_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result["preventive_service_type"][0] == "flu_vaccine"
        assert result["preventive_service_category"][0] == "immunization"

    @pytest.mark.unit
    def test_colonoscopy_categorization(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_prev_claims(["45378"])
        result = collect(
            PreventiveServicesTransform.categorize_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result["preventive_service_type"][0] == "colorectal_screening_colonoscopy"
        assert result["preventive_service_category"][0] == "cancer_screening"

    @pytest.mark.unit
    def test_depression_screening_categorization(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_prev_claims(["G0444"])
        result = collect(
            PreventiveServicesTransform.categorize_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result["preventive_service_type"][0] == "depression_screening"
        assert result["preventive_service_category"][0] == "other_screening"

    @pytest.mark.unit
    def test_covid_vaccine_categorization(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_prev_claims(["91300"])
        result = collect(
            PreventiveServicesTransform.categorize_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result["preventive_service_type"][0] == "covid_vaccine"
        assert result["preventive_service_category"][0] == "immunization"

    @pytest.mark.unit
    def test_multiple_service_types(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_prev_claims(["G0438", "77067", "90656", "G0444"])
        result = collect(
            PreventiveServicesTransform.categorize_preventive_services(claims, DEFAULT_CONFIG)
        )
        types = set(result["preventive_service_type"].to_list())
        assert types == {
            "annual_wellness_visit", "mammogram", "flu_vaccine", "depression_screening"
        }

    @pytest.mark.unit
    def test_annual_physical_is_wellness(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_prev_claims(["99395"])
        result = collect(
            PreventiveServicesTransform.categorize_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result["preventive_service_type"][0] == "annual_physical"
        assert result["preventive_service_category"][0] == "wellness_visit"


class TestPreventiveMemberProfile:
    """Tests for PreventiveServicesTransform.calculate_member_preventive_profile."""

    def _make_categorized_claims(self) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1", "P1", "P1", "P2"],
            "claim_id": ["C1", "C2", "C3", "C4"],
            "procedure_code": ["G0438", "77067", "90656", "90656"],
            "preventive_service_type": [
                "annual_wellness_visit", "mammogram", "flu_vaccine", "flu_vaccine",
            ],
            "preventive_service_category": [
                "wellness_visit", "cancer_screening", "immunization", "immunization",
            ],
        })

    @pytest.mark.unit
    def test_member_profile_basic(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = self._make_categorized_claims()
        result = collect(
            PreventiveServicesTransform.calculate_member_preventive_profile(
                claims, DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 2

        p1 = result.filter(pl.col("person_id") == "P1")
        assert p1["unique_preventive_services"][0] == 3
        assert p1["total_preventive_services"][0] == 3
        assert p1["has_wellness_visit"][0] is True
        assert p1["has_cancer_screening"][0] is True
        assert p1["has_immunization"][0] is True
        assert p1["has_flu_vaccine"][0] is True
        assert p1["fully_engaged"][0] is True

    @pytest.mark.unit
    def test_not_fully_engaged(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        # P2 only has flu vaccine
        claims = self._make_categorized_claims()
        result = collect(
            PreventiveServicesTransform.calculate_member_preventive_profile(
                claims, DEFAULT_CONFIG
            )
        )
        p2 = result.filter(pl.col("person_id") == "P2")
        assert p2["fully_engaged"][0] is False
        assert p2["has_wellness_visit"][0] is False
        assert p2["has_cancer_screening"][0] is False

    @pytest.mark.unit
    def test_engagement_levels(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = pl.LazyFrame({
            "person_id": ["P1", "P1", "P1", "P1", "P1", "P2", "P2", "P2", "P3"],
            "claim_id": [f"C{i}" for i in range(9)],
            "procedure_code": ["G0438", "77067", "90656", "G0444", "90670",
                               "G0438", "77067", "90656", "90656"],
            "preventive_service_type": [
                "annual_wellness_visit", "mammogram", "flu_vaccine",
                "depression_screening", "pneumonia_vaccine",
                "annual_wellness_visit", "mammogram", "flu_vaccine",
                "flu_vaccine",
            ],
            "preventive_service_category": [
                "wellness_visit", "cancer_screening", "immunization",
                "other_screening", "immunization",
                "wellness_visit", "cancer_screening", "immunization",
                "immunization",
            ],
        })
        result = collect(
            PreventiveServicesTransform.calculate_member_preventive_profile(
                claims, DEFAULT_CONFIG
            )
        )
        p1 = result.filter(pl.col("person_id") == "P1")
        assert p1["preventive_care_engagement"][0] == "high"  # 5 unique

        p2 = result.filter(pl.col("person_id") == "P2")
        assert p2["preventive_care_engagement"][0] == "moderate"  # 3 unique

        p3 = result.filter(pl.col("person_id") == "P3")
        assert p3["preventive_care_engagement"][0] == "low"  # 1 unique


class TestPreventiveScreeningCompliance:
    """Tests for PreventiveServicesTransform.calculate_screening_compliance."""

    def _make_eligibility(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "person_id": ["P1", "P2", "P3", "P4"],
                "enrollment_start_date": [date(2024, 1, 1)] * 4,
                "enrollment_end_date": [date(2024, 12, 31)] * 4,
                "age": [55, 60, 55, 30],
                "gender": ["F", "M", "F", "M"],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "age": pl.Int64,
                "gender": pl.Utf8,
            },
        )

    def _make_prev_claims(self) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1", "P1", "P2"],
            "claim_id": ["C1", "C2", "C3"],
            "preventive_service_type": ["mammogram", "flu_vaccine", "colorectal_screening_colonoscopy"],
            "preventive_service_category": ["cancer_screening", "immunization", "cancer_screening"],
        })

    @pytest.mark.unit
    def test_compliance_output_columns(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        result = collect(
            PreventiveServicesTransform.calculate_screening_compliance(
                self._make_eligibility(), self._make_prev_claims(), DEFAULT_CONFIG
            )
        )
        assert "mammogram_compliance_pct" in result.columns
        assert "colorectal_compliance_pct" in result.columns
        assert "flu_vaccine_rate_pct" in result.columns

    @pytest.mark.unit
    def test_compliance_values(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        result = collect(
            PreventiveServicesTransform.calculate_screening_compliance(
                self._make_eligibility(), self._make_prev_claims(), DEFAULT_CONFIG
            )
        )
        # 2 females aged 50-74 (P1, P3), 1 received mammogram (P1) => 50%
        assert abs(result["mammogram_compliance_pct"][0] - 50.0) < 0.01
        # 1 flu vaccine out of 4 total => 25%
        assert abs(result["flu_vaccine_rate_pct"][0] - 25.0) < 0.01

    @pytest.mark.unit
    def test_single_row_output(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        result = collect(
            PreventiveServicesTransform.calculate_screening_compliance(
                self._make_eligibility(), self._make_prev_claims(), DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 1


class TestPreventiveServicesOrchestrator:
    """Tests for PreventiveServicesTransform.analyze_preventive_services."""

    def _make_claims(self) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1", "P1", "P2", "P3"],
            "claim_id": ["C1", "C2", "C3", "C4"],
            "claim_type": ["professional", "professional", "professional", "professional"],
            "claim_end_date": [
                date(2024, 3, 1), date(2024, 6, 1),
                date(2024, 9, 1), date(2024, 1, 15),
            ],
            "procedure_code": ["G0438", "77067", "90656", "G0444"],
        })

    def _make_eligibility(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "person_id": ["P1", "P2", "P3"],
                "enrollment_start_date": [date(2024, 1, 1)] * 3,
                "enrollment_end_date": [date(2024, 12, 31)] * 3,
                "age": [55, 70, 45],
                "gender": ["F", "M", "F"],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "age": pl.Int64,
                "gender": pl.Utf8,
            },
        )

    @pytest.mark.unit
    def test_returns_four_items(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        result = PreventiveServicesTransform.analyze_preventive_services(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        assert len(result) == 4

    @pytest.mark.unit
    def test_member_profile_output(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        member_profile, _, _, _ = PreventiveServicesTransform.analyze_preventive_services(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        df = collect(member_profile)
        assert "fully_engaged" in df.columns
        assert "preventive_care_engagement" in df.columns

    @pytest.mark.unit
    def test_service_utilization(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        _, service_util, _, _ = PreventiveServicesTransform.analyze_preventive_services(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        df = collect(service_util)
        assert "preventive_service_type" in df.columns
        assert "member_count" in df.columns
        assert "service_count" in df.columns

    @pytest.mark.unit
    def test_compliance_rates(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        _, _, compliance, _ = PreventiveServicesTransform.analyze_preventive_services(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        df = collect(compliance)
        assert "mammogram_compliance_pct" in df.columns

    @pytest.mark.unit
    def test_categorized_claims_returned(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        _, _, _, prev_categorized = PreventiveServicesTransform.analyze_preventive_services(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        df = collect(prev_categorized)
        assert "preventive_service_type" in df.columns
        assert "preventive_service_category" in df.columns


class TestEdgeCases:
    """Edge cases shared across transforms."""

    @pytest.mark.unit
    def test_financial_empty_claims(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        claims = pl.LazyFrame(
            schema={
                "person_id": pl.Utf8,
                "claim_id": pl.Utf8,
                "claim_end_date": pl.Date,
                "paid_amount": pl.Float64,
                "allowed_amount": pl.Float64,
            }
        )
        cats = pl.LazyFrame(
            schema={
                "claim_id": pl.Utf8,
                "service_category_1": pl.Utf8,
                "service_category_2": pl.Utf8,
            }
        )
        result = collect(
            FinancialTotalCostTransform.aggregate_costs_by_member(claims, cats, DEFAULT_CONFIG)
        )
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_pharmacy_empty_claims(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        claims = pl.LazyFrame(
            schema={
                "person_id": pl.Utf8,
                "claim_id": pl.Utf8,
                "claim_type": pl.Utf8,
                "claim_start_date": pl.Date,
                "claim_end_date": pl.Date,
                "ndc_code": pl.Utf8,
                "paid_amount": pl.Float64,
                "allowed_amount": pl.Float64,
                "quantity": pl.Float64,
                "days_supply": pl.Int64,
                "drug_name": pl.Utf8,
                "prescribing_provider_npi": pl.Utf8,
            }
        )
        result = collect(
            PharmacyAnalysisTransform.identify_pharmacy_claims(claims, DEFAULT_CONFIG)
        )
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_preventive_empty_claims(self):
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        claims = pl.LazyFrame(
            schema={
                "person_id": pl.Utf8,
                "claim_id": pl.Utf8,
                "claim_type": pl.Utf8,
                "claim_end_date": pl.Date,
                "procedure_code": pl.Utf8,
            }
        )
        result = collect(
            PreventiveServicesTransform.identify_preventive_services(claims, DEFAULT_CONFIG)
        )
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_provider_empty_claims(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = pl.LazyFrame(
            schema={
                "person_id": pl.Utf8,
                "claim_id": pl.Utf8,
                "claim_type": pl.Utf8,
                "claim_end_date": pl.Date,
                "rendering_provider_npi": pl.Utf8,
                "rendering_provider_specialty": pl.Utf8,
                "procedure_code": pl.Utf8,
                "paid_amount": pl.Float64,
            }
        )
        result = collect(
            ProviderAttributionEnhancedTransform.identify_primary_care_visits(
                claims, DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_polypharmacy_boundary_at_5(self):
        """Test that exactly 5 unique medications triggers moderate."""
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        member_costs = pl.LazyFrame({
            "person_id": ["P1"],
            "total_fills": [5],
            "total_pharmacy_cost": [500.0],
            "total_days_supply": [150],
            "unique_medications": [5],
        })
        result = collect(
            PharmacyAnalysisTransform.detect_polypharmacy(member_costs, DEFAULT_CONFIG)
        )
        assert result["moderate_polypharmacy"][0] is True
        assert result["polypharmacy_risk"][0] == "moderate"

    @pytest.mark.unit
    def test_polypharmacy_boundary_at_10(self):
        """Test that exactly 10 unique medications triggers high."""
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        member_costs = pl.LazyFrame({
            "person_id": ["P1"],
            "total_fills": [10],
            "total_pharmacy_cost": [2000.0],
            "total_days_supply": [300],
            "unique_medications": [10],
        })
        result = collect(
            PharmacyAnalysisTransform.detect_polypharmacy(member_costs, DEFAULT_CONFIG)
        )
        assert result["high_polypharmacy"][0] is True
        assert result["polypharmacy_risk"][0] == "high"

    @pytest.mark.unit
    def test_risk_adjustment_female_lowercase(self):
        """Test that 'female' (lowercase) is recognized as female gender."""
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        lf = pl.LazyFrame({
            "person_id": ["P1"],
            "member_months": [12.0],
            "age": [30],
            "gender": ["female"],
            "total_medical_cost": [1200.0],
            "total_claims": [5],
            "pmpm_medical": [100.0],
        })
        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(lf, DEFAULT_CONFIG)
        )
        assert result["gender_factor"][0] == 1.1

    @pytest.mark.unit
    def test_preventive_service_codes_dict_populated(self):
        """Verify the class-level constant has expected keys."""
        from acoharmony._transforms.preventive_services import PreventiveServicesTransform

        codes = PreventiveServicesTransform.PREVENTIVE_SERVICE_CODES
        expected_keys = [
            "annual_wellness_visit", "annual_physical", "mammogram",
            "colorectal_screening_colonoscopy", "flu_vaccine",
            "pneumonia_vaccine", "shingles_vaccine", "covid_vaccine",
            "depression_screening", "obesity_screening", "tobacco_cessation",
        ]
        for key in expected_keys:
            assert key in codes
            assert len(codes[key]) > 0

    @pytest.mark.unit
    def test_lazyframe_return_types(self):
        """Verify that individual methods return LazyFrame (not collected)."""
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        elig = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "enrollment_start_date": [date(2024, 1, 1)],
                "enrollment_end_date": [date(2024, 12, 31)],
                "age": [50],
                "gender": ["M"],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "age": pl.Int64,
                "gender": pl.Utf8,
            },
        )
        result = FinancialTotalCostTransform.calculate_member_months(elig, DEFAULT_CONFIG)
        assert isinstance(result, pl.LazyFrame)
