# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.pharmacy_analysis module."""

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


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestPharmacyAnalysisPublic:
    """Tests for pharmacy_analysis public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import pharmacy_analysis
        assert pharmacy_analysis is not None

    @pytest.mark.unit
    def test_pharmacy_analysis_transform_class(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform
        assert PharmacyAnalysisTransform is not None


class TestPharmacyIdentifyClaims:
    """Tests for PharmacyAnalysisTransform.identify_pharmacy_claims."""

    def _make_claims(self, rows: list[dict] | None = None) -> pl.LazyFrame:
        if rows is None:
            rows = [
                {
                    "person_id": "P1",
                    "claim_id": "RX1",
                    "claim_type": "pharmacy",
                    "claim_start_date": date(2024, 2, 1),
                    "claim_end_date": date(2024, 2, 1),
                    "ndc_code": "12345678901",
                    "paid_amount": 150.0,
                    "allowed_amount": 200.0,
                    "quantity": 30.0,
                    "days_supply": 30,
                    "drug_name": "Atorvastatin",
                    "prescribing_provider_npi": "NPI001",
                },
                {
                    "person_id": "P2",
                    "claim_id": "RX2",
                    "claim_type": "pharmacy",
                    "claim_start_date": date(2024, 5, 10),
                    "claim_end_date": date(2024, 5, 10),
                    "ndc_code": "98765432109",
                    "paid_amount": 1500.0,
                    "allowed_amount": 2000.0,
                    "quantity": 1.0,
                    "days_supply": 30,
                    "drug_name": "Humira",
                    "prescribing_provider_npi": "NPI002",
                },
                {
                    "person_id": "P1",
                    "claim_id": "MC1",
                    "claim_type": "professional",
                    "claim_start_date": date(2024, 3, 1),
                    "claim_end_date": date(2024, 3, 1),
                    "ndc_code": None,
                    "paid_amount": 500.0,
                    "allowed_amount": 600.0,
                    "quantity": None,
                    "days_supply": None,
                    "drug_name": None,
                    "prescribing_provider_npi": None,
                },
            ]
        return pl.LazyFrame(rows)

    @pytest.mark.unit
    def test_filters_pharmacy_claims_only(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        claims = self._make_claims()
        result = collect(PharmacyAnalysisTransform.identify_pharmacy_claims(claims, DEFAULT_CONFIG))
        assert result.shape[0] == 2
        # claim_type is not in the output columns (filtered out by select);
        # verify by claim_id instead
        assert set(result["claim_id"].to_list()) == {"RX1", "RX2"}

    @pytest.mark.unit
    def test_filters_by_measurement_year(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        claims = self._make_claims([
            {
                "person_id": "P1",
                "claim_id": "RX1",
                "claim_type": "pharmacy",
                "claim_start_date": date(2024, 2, 1),
                "claim_end_date": date(2024, 2, 1),
                "ndc_code": "111",
                "paid_amount": 100.0,
                "allowed_amount": 120.0,
                "quantity": 30.0,
                "days_supply": 30,
                "drug_name": "DrugA",
                "prescribing_provider_npi": "NPI1",
            },
            {
                "person_id": "P1",
                "claim_id": "RX2",
                "claim_type": "pharmacy",
                "claim_start_date": date(2023, 2, 1),
                "claim_end_date": date(2023, 2, 1),
                "ndc_code": "222",
                "paid_amount": 200.0,
                "allowed_amount": 240.0,
                "quantity": 30.0,
                "days_supply": 30,
                "drug_name": "DrugB",
                "prescribing_provider_npi": "NPI2",
            },
        ])
        result = collect(PharmacyAnalysisTransform.identify_pharmacy_claims(claims, DEFAULT_CONFIG))
        assert result.shape[0] == 1
        assert result["claim_id"][0] == "RX1"

    @pytest.mark.unit
    def test_output_columns(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        claims = self._make_claims()
        result = collect(PharmacyAnalysisTransform.identify_pharmacy_claims(claims, DEFAULT_CONFIG))
        expected_cols = {
            "person_id", "claim_id", "claim_start_date", "claim_end_date",
            "ndc_code", "paid_amount", "allowed_amount", "quantity",
            "days_supply", "drug_name", "prescribing_provider_npi",
        }
        assert set(result.columns) == expected_cols


class TestPharmacyMemberDrugCosts:
    """Tests for PharmacyAnalysisTransform.calculate_member_drug_costs."""

    def _make_pharmacy_claims(self) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1", "P1", "P2"],
            "claim_id": ["RX1", "RX2", "RX3"],
            "ndc_code": ["111", "222", "333"],
            "paid_amount": [100.0, 200.0, 300.0],
            "days_supply": [30, 60, 90],
        })

    @pytest.mark.unit
    def test_basic_aggregation(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        claims = self._make_pharmacy_claims()
        result = collect(
            PharmacyAnalysisTransform.calculate_member_drug_costs(claims, DEFAULT_CONFIG)
        )
        assert result.shape[0] == 2
        assert "total_fills" in result.columns
        assert "total_pharmacy_cost" in result.columns
        assert "total_days_supply" in result.columns
        assert "unique_medications" in result.columns

    @pytest.mark.unit
    def test_member_totals(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        claims = self._make_pharmacy_claims()
        result = collect(
            PharmacyAnalysisTransform.calculate_member_drug_costs(claims, DEFAULT_CONFIG)
        )
        p1 = result.filter(pl.col("person_id") == "P1")
        assert p1["total_fills"][0] == 2
        assert abs(p1["total_pharmacy_cost"][0] - 300.0) < 0.01
        assert p1["total_days_supply"][0] == 90
        assert p1["unique_medications"][0] == 2


class TestPharmacyHighCostMedications:
    """Tests for PharmacyAnalysisTransform.identify_high_cost_medications."""

    @pytest.mark.unit
    def test_identifies_high_cost(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        claims = pl.LazyFrame({
            "person_id": ["P1", "P2", "P3"],
            "ndc_code": ["EXPENSIVE", "EXPENSIVE", "CHEAP"],
            "drug_name": ["ExpensiveDrug", "ExpensiveDrug", "CheapDrug"],
            "paid_amount": [2000.0, 3000.0, 50.0],
        })
        result = collect(
            PharmacyAnalysisTransform.identify_high_cost_medications(claims, DEFAULT_CONFIG)
        )
        # Only the drug with avg >= 1000 should appear
        assert result.shape[0] == 1
        assert result["drug_name"][0] == "ExpensiveDrug"
        assert result["fill_count"][0] == 2
        assert result["unique_members"][0] == 2

    @pytest.mark.unit
    def test_no_high_cost_medications(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        claims = pl.LazyFrame({
            "person_id": ["P1", "P2"],
            "ndc_code": ["A", "B"],
            "drug_name": ["DrugA", "DrugB"],
            "paid_amount": [50.0, 100.0],
        })
        result = collect(
            PharmacyAnalysisTransform.identify_high_cost_medications(claims, DEFAULT_CONFIG)
        )
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_output_columns(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        claims = pl.LazyFrame({
            "person_id": ["P1"],
            "ndc_code": ["X"],
            "drug_name": ["DrugX"],
            "paid_amount": [5000.0],
        })
        result = collect(
            PharmacyAnalysisTransform.identify_high_cost_medications(claims, DEFAULT_CONFIG)
        )
        expected = {"ndc_code", "drug_name", "fill_count", "total_cost", "avg_cost_per_fill", "unique_members"}
        assert set(result.columns) == expected


class TestPharmacyPolypharmacy:
    """Tests for PharmacyAnalysisTransform.detect_polypharmacy."""

    @pytest.mark.unit
    def test_high_polypharmacy(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        member_costs = pl.LazyFrame({
            "person_id": ["P1"],
            "total_fills": [15],
            "total_pharmacy_cost": [5000.0],
            "total_days_supply": [450],
            "unique_medications": [12],
        })
        result = collect(
            PharmacyAnalysisTransform.detect_polypharmacy(member_costs, DEFAULT_CONFIG)
        )
        assert result["high_polypharmacy"][0] is True
        assert result["polypharmacy_risk"][0] == "high"

    @pytest.mark.unit
    def test_moderate_polypharmacy(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        member_costs = pl.LazyFrame({
            "person_id": ["P1"],
            "total_fills": [7],
            "total_pharmacy_cost": [2000.0],
            "total_days_supply": [210],
            "unique_medications": [7],
        })
        result = collect(
            PharmacyAnalysisTransform.detect_polypharmacy(member_costs, DEFAULT_CONFIG)
        )
        assert result["moderate_polypharmacy"][0] is True
        assert result["polypharmacy_risk"][0] == "moderate"

    @pytest.mark.unit
    def test_low_polypharmacy(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        member_costs = pl.LazyFrame({
            "person_id": ["P1"],
            "total_fills": [3],
            "total_pharmacy_cost": [500.0],
            "total_days_supply": [90],
            "unique_medications": [3],
        })
        result = collect(
            PharmacyAnalysisTransform.detect_polypharmacy(member_costs, DEFAULT_CONFIG)
        )
        assert result["high_polypharmacy"][0] is False
        assert result["moderate_polypharmacy"][0] is False
        assert result["polypharmacy_risk"][0] == "low"


class TestPharmacyPMPM:
    """Tests for PharmacyAnalysisTransform.calculate_pharmacy_pmpm."""

    @pytest.mark.unit
    def test_basic_pmpm(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        member_costs = pl.LazyFrame({
            "person_id": ["P1", "P2"],
            "total_fills": [10, 5],
            "total_pharmacy_cost": [1200.0, 600.0],
            "total_days_supply": [300, 150],
            "unique_medications": [4, 2],
        })
        member_months = pl.LazyFrame({
            "person_id": ["P1", "P2"],
            "member_months": [12.0, 6.0],
        })
        result = collect(
            PharmacyAnalysisTransform.calculate_pharmacy_pmpm(
                member_costs, member_months, DEFAULT_CONFIG
            )
        )
        assert "pharmacy_pmpm" in result.columns
        p1 = result.filter(pl.col("person_id") == "P1")
        assert abs(p1["pharmacy_pmpm"][0] - 100.0) < 0.01
        p2 = result.filter(pl.col("person_id") == "P2")
        assert abs(p2["pharmacy_pmpm"][0] - 100.0) < 0.01


class TestPharmacyAnalyticsOrchestrator:
    """Tests for PharmacyAnalysisTransform.calculate_pharmacy_analytics."""

    def _make_claims(self) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1", "P1", "P2", "P3"],
            "claim_id": ["RX1", "RX2", "RX3", "MC1"],
            "claim_type": ["pharmacy", "pharmacy", "pharmacy", "professional"],
            "claim_start_date": [
                date(2024, 1, 15), date(2024, 4, 15),
                date(2024, 6, 1), date(2024, 3, 1),
            ],
            "claim_end_date": [
                date(2024, 1, 15), date(2024, 4, 15),
                date(2024, 6, 1), date(2024, 3, 1),
            ],
            "ndc_code": ["111", "222", "111", None],
            "paid_amount": [100.0, 2000.0, 150.0, 500.0],
            "allowed_amount": [120.0, 2400.0, 180.0, 600.0],
            "quantity": [30.0, 1.0, 30.0, None],
            "days_supply": [30, 30, 30, None],
            "drug_name": ["Lipitor", "Humira", "Lipitor", None],
            "prescribing_provider_npi": ["NPI1", "NPI2", "NPI1", None],
        })

    def _make_eligibility(self) -> pl.LazyFrame:
        return pl.LazyFrame(
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

    @pytest.mark.unit
    def test_returns_four_items(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        result = PharmacyAnalysisTransform.calculate_pharmacy_analytics(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        assert len(result) == 4

    @pytest.mark.unit
    def test_pharmacy_claims_filtered(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        pharmacy_claims, _, _, _ = PharmacyAnalysisTransform.calculate_pharmacy_analytics(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        df = collect(pharmacy_claims)
        assert df.shape[0] == 3  # Only 3 pharmacy claims

    @pytest.mark.unit
    def test_summary_shape(self):
        from acoharmony._transforms.pharmacy_analysis import PharmacyAnalysisTransform

        _, _, _, summary = PharmacyAnalysisTransform.calculate_pharmacy_analytics(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        df = collect(summary)
        assert df.shape[0] == 1
        assert "total_fills" in df.columns
        assert "total_pharmacy_spend" in df.columns
