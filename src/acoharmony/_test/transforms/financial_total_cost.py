# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.financial_total_cost module."""

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


def _lazy(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestFinancialTotalCostPublic:
    """Tests for financial_total_cost public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import financial_total_cost
        assert financial_total_cost is not None

    @pytest.mark.unit
    def test_financial_total_cost_transform_class(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform
        assert FinancialTotalCostTransform is not None


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


class TestFinancialTotalCostCalculateMemberMonths:
    """Tests for FinancialTotalCostTransform.calculate_member_months."""

    def _make_eligibility(
        self,
        rows: list[dict],
    ) -> pl.LazyFrame:
        return pl.LazyFrame(rows, schema={
            "person_id": pl.Utf8,
            "enrollment_start_date": pl.Date,
            "enrollment_end_date": pl.Date,
            "age": pl.Int64,
            "gender": pl.Utf8,
        })

    @pytest.mark.unit
    def test_full_year_enrollment(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        elig = self._make_eligibility([
            {
                "person_id": "P1",
                "enrollment_start_date": date(2024, 1, 1),
                "enrollment_end_date": date(2024, 12, 31),
                "age": 55,
                "gender": "M",
            }
        ])
        result = collect(FinancialTotalCostTransform.calculate_member_months(elig, DEFAULT_CONFIG))
        assert result.shape[0] == 1
        assert "member_months" in result.columns
        assert "person_id" in result.columns
        # Full year should be approximately 12 months
        mm = result["member_months"][0]
        assert 11.5 < mm < 12.5

    @pytest.mark.unit
    def test_partial_year_enrollment(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        elig = self._make_eligibility([
            {
                "person_id": "P2",
                "enrollment_start_date": date(2024, 7, 1),
                "enrollment_end_date": date(2024, 12, 31),
                "age": 30,
                "gender": "F",
            }
        ])
        result = collect(FinancialTotalCostTransform.calculate_member_months(elig, DEFAULT_CONFIG))
        mm = result["member_months"][0]
        # About 6 months
        assert 5.5 < mm < 6.5

    @pytest.mark.unit
    def test_enrollment_before_measurement_year(self):
        """Member enrolled before measurement year - effective_start clamped to Jan 1."""
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        elig = self._make_eligibility([
            {
                "person_id": "P3",
                "enrollment_start_date": date(2023, 6, 1),
                "enrollment_end_date": date(2024, 6, 30),
                "age": 40,
                "gender": "M",
            }
        ])
        result = collect(FinancialTotalCostTransform.calculate_member_months(elig, DEFAULT_CONFIG))
        assert result.shape[0] == 1
        mm = result["member_months"][0]
        # Jan 1 to Jun 30 ~ 6 months
        assert 5.5 < mm < 6.5

    @pytest.mark.unit
    def test_enrollment_outside_measurement_year_excluded(self):
        """Member enrolled entirely outside measurement year is excluded."""
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        elig = self._make_eligibility([
            {
                "person_id": "P4",
                "enrollment_start_date": date(2023, 1, 1),
                "enrollment_end_date": date(2023, 12, 31),
                "age": 60,
                "gender": "F",
            }
        ])
        result = collect(FinancialTotalCostTransform.calculate_member_months(elig, DEFAULT_CONFIG))
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_output_columns(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        elig = self._make_eligibility([
            {
                "person_id": "P1",
                "enrollment_start_date": date(2024, 1, 1),
                "enrollment_end_date": date(2024, 12, 31),
                "age": 45,
                "gender": "M",
            }
        ])
        result = collect(FinancialTotalCostTransform.calculate_member_months(elig, DEFAULT_CONFIG))
        assert set(result.columns) == {"person_id", "member_months", "age", "gender"}


class TestFinancialTotalCostAggregateCosts:
    """Tests for FinancialTotalCostTransform.aggregate_costs_by_member."""

    def _make_claims_and_categories(self):
        claims = pl.LazyFrame({
            "person_id": ["P1", "P1", "P2"],
            "claim_id": ["C1", "C2", "C3"],
            "claim_end_date": [date(2024, 3, 15), date(2024, 6, 20), date(2024, 9, 10)],
            "paid_amount": [1000.0, 2000.0, 1500.0],
            "allowed_amount": [1200.0, 2400.0, 1800.0],
        })
        categories = pl.LazyFrame({
            "claim_id": ["C1", "C2", "C3"],
            "service_category_1": ["inpatient", "outpatient", "inpatient"],
            "service_category_2": ["medical", "surgical", "medical"],
        })
        return claims, categories

    @pytest.mark.unit
    def test_basic_aggregation(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        claims, cats = self._make_claims_and_categories()
        result = collect(
            FinancialTotalCostTransform.aggregate_costs_by_member(claims, cats, DEFAULT_CONFIG)
        )
        assert result.shape[0] > 0
        assert "total_paid" in result.columns
        assert "total_allowed" in result.columns
        assert "claim_count" in result.columns
        assert "service_category_1" in result.columns

    @pytest.mark.unit
    def test_filters_by_measurement_year(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        claims = pl.LazyFrame({
            "person_id": ["P1", "P1"],
            "claim_id": ["C1", "C2"],
            "claim_end_date": [date(2024, 3, 15), date(2023, 6, 20)],
            "paid_amount": [1000.0, 2000.0],
            "allowed_amount": [1200.0, 2400.0],
        })
        categories = pl.LazyFrame({
            "claim_id": ["C1", "C2"],
            "service_category_1": ["inpatient", "outpatient"],
            "service_category_2": ["medical", "surgical"],
        })
        result = collect(
            FinancialTotalCostTransform.aggregate_costs_by_member(claims, categories, DEFAULT_CONFIG)
        )
        # Only the 2024 claim should remain
        assert result["total_paid"].sum() == 1000.0

    @pytest.mark.unit
    def test_multiple_members(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        claims, cats = self._make_claims_and_categories()
        result = collect(
            FinancialTotalCostTransform.aggregate_costs_by_member(claims, cats, DEFAULT_CONFIG)
        )
        person_ids = result["person_id"].unique().to_list()
        assert "P1" in person_ids
        assert "P2" in person_ids


class TestFinancialTotalCostCalculatePMPM:
    """Tests for FinancialTotalCostTransform.calculate_pmpm."""

    @pytest.mark.unit
    def test_basic_pmpm(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        member_costs = pl.LazyFrame({
            "person_id": ["P1", "P1", "P2"],
            "total_paid": [5000.0, 3000.0, 2000.0],
            "claim_count": [3, 2, 1],
        })
        member_months = pl.LazyFrame({
            "person_id": ["P1", "P2"],
            "member_months": [12.0, 6.0],
            "age": [55, 30],
            "gender": ["M", "F"],
        })
        result = collect(
            FinancialTotalCostTransform.calculate_pmpm(member_costs, member_months, DEFAULT_CONFIG)
        )
        assert "pmpm_medical" in result.columns
        assert "total_medical_cost" in result.columns
        # P1: total_medical_cost = 8000, member_months = 12 => pmpm = 666.67
        p1 = result.filter(pl.col("person_id") == "P1")
        assert abs(p1["total_medical_cost"][0] - 8000.0) < 0.01
        assert abs(p1["pmpm_medical"][0] - 8000.0 / 12.0) < 0.01

    @pytest.mark.unit
    def test_member_with_no_costs(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        member_costs = pl.LazyFrame({
            "person_id": ["P1"],
            "total_paid": [1000.0],
            "claim_count": [1],
        })
        member_months = pl.LazyFrame({
            "person_id": ["P1", "P2"],
            "member_months": [12.0, 12.0],
            "age": [45, 50],
            "gender": ["M", "F"],
        })
        result = collect(
            FinancialTotalCostTransform.calculate_pmpm(member_costs, member_months, DEFAULT_CONFIG)
        )
        p2 = result.filter(pl.col("person_id") == "P2")
        assert p2["total_medical_cost"][0] == 0
        assert p2["total_claims"][0] == 0


class TestFinancialTotalCostRiskAdjustment:
    """Tests for FinancialTotalCostTransform.calculate_risk_adjustment."""

    def _make_pmpm(self, age: int, gender: str, pmpm: float) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1"],
            "member_months": [12.0],
            "age": [age],
            "gender": [gender],
            "total_medical_cost": [pmpm * 12],
            "total_claims": [5],
            "pmpm_medical": [pmpm],
        })

    @pytest.mark.unit
    def test_child_age_factor(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(
                self._make_pmpm(10, "M", 100.0), DEFAULT_CONFIG
            )
        )
        assert result["age_factor"][0] == 0.5

    @pytest.mark.unit
    def test_young_adult_age_factor(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(
                self._make_pmpm(30, "M", 100.0), DEFAULT_CONFIG
            )
        )
        assert result["age_factor"][0] == 1.0

    @pytest.mark.unit
    def test_middle_age_factor(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(
                self._make_pmpm(55, "M", 100.0), DEFAULT_CONFIG
            )
        )
        assert result["age_factor"][0] == 1.5

    @pytest.mark.unit
    def test_senior_age_factor(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(
                self._make_pmpm(70, "M", 100.0), DEFAULT_CONFIG
            )
        )
        assert result["age_factor"][0] == 2.5

    @pytest.mark.unit
    def test_female_gender_factor(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(
                self._make_pmpm(30, "F", 100.0), DEFAULT_CONFIG
            )
        )
        assert result["gender_factor"][0] == 1.1

    @pytest.mark.unit
    def test_male_gender_factor(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(
                self._make_pmpm(30, "M", 100.0), DEFAULT_CONFIG
            )
        )
        assert result["gender_factor"][0] == 1.0

    @pytest.mark.unit
    def test_risk_score_is_product(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(
                self._make_pmpm(55, "F", 200.0), DEFAULT_CONFIG
            )
        )
        # age_factor=1.5, gender_factor=1.1, risk_score=1.65
        assert abs(result["risk_score"][0] - 1.65) < 0.01

    @pytest.mark.unit
    def test_risk_adjusted_pmpm(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(
                self._make_pmpm(55, "F", 200.0), DEFAULT_CONFIG
            )
        )
        expected = 200.0 / 1.65
        assert abs(result["risk_adjusted_pmpm"][0] - expected) < 0.01

    @pytest.mark.unit
    def test_output_has_required_columns(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        result = collect(
            FinancialTotalCostTransform.calculate_risk_adjustment(
                self._make_pmpm(50, "M", 100.0), DEFAULT_CONFIG
            )
        )
        for col in ["age_factor", "gender_factor", "risk_score", "risk_adjusted_pmpm"]:
            assert col in result.columns


class TestFinancialTotalCostHighCostMembers:
    """Tests for FinancialTotalCostTransform.identify_high_cost_members."""

    @pytest.mark.unit
    def test_cost_tiers_assigned(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        # Create 20 members with ascending costs
        data = {
            "person_id": [f"P{i}" for i in range(20)],
            "member_months": [12.0] * 20,
            "age": [50] * 20,
            "gender": ["M"] * 20,
            "total_medical_cost": [float(i * 1000) for i in range(20)],
            "total_claims": [5] * 20,
            "pmpm_medical": [float(i * 1000) / 12 for i in range(20)],
        }
        lf = pl.LazyFrame(data)
        result = collect(
            FinancialTotalCostTransform.identify_high_cost_members(lf, DEFAULT_CONFIG)
        )
        assert "cost_tier" in result.columns
        assert "top_10_pct" in result.columns
        assert "top_5_pct" in result.columns
        assert "top_1_pct" in result.columns
        tiers = result["cost_tier"].unique().to_list()
        assert "other" in tiers

    @pytest.mark.unit
    def test_top_10_pct_flag(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        data = {
            "person_id": [f"P{i}" for i in range(100)],
            "member_months": [12.0] * 100,
            "age": [50] * 100,
            "gender": ["M"] * 100,
            "total_medical_cost": [float(i * 100) for i in range(100)],
            "total_claims": [5] * 100,
            "pmpm_medical": [float(i * 100) / 12 for i in range(100)],
        }
        lf = pl.LazyFrame(data)
        result = collect(
            FinancialTotalCostTransform.identify_high_cost_members(lf, DEFAULT_CONFIG)
        )
        top10_count = result.filter(pl.col("top_10_pct")).shape[0]
        # Approximately 10% should be in top 10
        assert top10_count > 0
        assert top10_count <= 15  # some tolerance


class TestFinancialTotalCostCalculateTCOC:
    """Tests for FinancialTotalCostTransform.calculate_total_cost_of_care."""

    def _make_test_data(self):
        eligibility = pl.LazyFrame(
            {
                "person_id": ["P1", "P2", "P3"],
                "enrollment_start_date": [date(2024, 1, 1)] * 3,
                "enrollment_end_date": [date(2024, 12, 31)] * 3,
                "age": [55, 30, 70],
                "gender": ["M", "F", "M"],
            },
            schema={
                "person_id": pl.Utf8,
                "enrollment_start_date": pl.Date,
                "enrollment_end_date": pl.Date,
                "age": pl.Int64,
                "gender": pl.Utf8,
            },
        )
        medical_claims = pl.LazyFrame({
            "person_id": ["P1", "P1", "P2", "P3"],
            "claim_id": ["C1", "C2", "C3", "C4"],
            "claim_end_date": [
                date(2024, 3, 15),
                date(2024, 6, 20),
                date(2024, 9, 10),
                date(2024, 11, 5),
            ],
            "paid_amount": [5000.0, 3000.0, 2000.0, 15000.0],
            "allowed_amount": [6000.0, 3600.0, 2400.0, 18000.0],
        })
        service_categories = pl.LazyFrame({
            "claim_id": ["C1", "C2", "C3", "C4"],
            "service_category_1": ["inpatient", "outpatient", "outpatient", "inpatient"],
            "service_category_2": ["medical", "surgical", "medical", "medical"],
        })
        return medical_claims, eligibility, service_categories

    @pytest.mark.unit
    def test_returns_four_lazyframes(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        claims, elig, cats = self._make_test_data()
        result = FinancialTotalCostTransform.calculate_total_cost_of_care(
            claims, elig, cats, DEFAULT_CONFIG
        )
        assert len(result) == 4
        for item in result:
            assert isinstance(item, pl.LazyFrame)

    @pytest.mark.unit
    def test_member_level_tcoc(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        claims, elig, cats = self._make_test_data()
        member_pmpm, _, _, _ = FinancialTotalCostTransform.calculate_total_cost_of_care(
            claims, elig, cats, DEFAULT_CONFIG
        )
        df = collect(member_pmpm)
        assert "pmpm_medical" in df.columns
        assert "risk_adjusted_pmpm" in df.columns
        assert "cost_tier" in df.columns
        assert df.shape[0] == 3

    @pytest.mark.unit
    def test_overall_summary(self):
        from acoharmony._transforms.financial_total_cost import FinancialTotalCostTransform

        claims, elig, cats = self._make_test_data()
        _, _, _, overall = FinancialTotalCostTransform.calculate_total_cost_of_care(
            claims, elig, cats, DEFAULT_CONFIG
        )
        df = collect(overall)
        assert "total_members" in df.columns
        assert "total_cost" in df.columns
        assert "avg_pmpm" in df.columns
        assert df["total_members"][0] == 3
