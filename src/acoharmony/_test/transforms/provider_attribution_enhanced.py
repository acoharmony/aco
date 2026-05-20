# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.provider_attribution_enhanced module."""

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
class TestProviderAttributionEnhancedPublic:
    """Tests for provider_attribution_enhanced public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import provider_attribution_enhanced
        assert provider_attribution_enhanced is not None

    @pytest.mark.unit
    def test_provider_attribution_enhanced_transform_class(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )
        assert ProviderAttributionEnhancedTransform is not None


class TestProviderIdentifyPCPVisits:
    """Tests for ProviderAttributionEnhancedTransform.identify_primary_care_visits."""

    def _make_claims(self) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1", "P1", "P2", "P3"],
            "claim_id": ["C1", "C2", "C3", "C4"],
            "claim_type": ["professional", "professional", "professional", "institutional"],
            "claim_end_date": [
                date(2024, 3, 1), date(2024, 6, 1),
                date(2024, 9, 1), date(2024, 1, 1),
            ],
            "rendering_provider_npi": ["NPI1", "NPI2", "NPI1", "NPI3"],
            "rendering_provider_specialty": ["08", "11", "08", "20"],
            "procedure_code": ["99213", "99214", "99213", "99222"],
            "paid_amount": [150.0, 200.0, 150.0, 5000.0],
        })

    @pytest.mark.unit
    def test_filters_pcp_visits(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = self._make_claims()
        result = collect(
            ProviderAttributionEnhancedTransform.identify_primary_care_visits(
                claims, DEFAULT_CONFIG
            )
        )
        # C1, C2, C3 are professional with PCP specialty/CPT; C4 is institutional
        assert result.shape[0] == 3
        assert "C4" not in result["claim_id"].to_list()

    @pytest.mark.unit
    def test_excludes_non_measurement_year(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = pl.LazyFrame({
            "person_id": ["P1"],
            "claim_id": ["C1"],
            "claim_type": ["professional"],
            "claim_end_date": [date(2023, 3, 1)],
            "rendering_provider_npi": ["NPI1"],
            "rendering_provider_specialty": ["08"],
            "procedure_code": ["99213"],
            "paid_amount": [150.0],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.identify_primary_care_visits(
                claims, DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_excludes_null_npi(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = pl.LazyFrame({
            "person_id": ["P1"],
            "claim_id": ["C1"],
            "claim_type": ["professional"],
            "claim_end_date": [date(2024, 3, 1)],
            "rendering_provider_npi": [None],
            "rendering_provider_specialty": ["08"],
            "procedure_code": ["99213"],
            "paid_amount": [150.0],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.identify_primary_care_visits(
                claims, DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_output_columns(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = self._make_claims()
        result = collect(
            ProviderAttributionEnhancedTransform.identify_primary_care_visits(
                claims, DEFAULT_CONFIG
            )
        )
        expected = {
            "person_id", "claim_id", "claim_end_date",
            "rendering_provider_npi", "rendering_provider_specialty",
            "procedure_code", "paid_amount",
        }
        assert set(result.columns) == expected


class TestProviderAttributePCPPlurality:
    """Tests for ProviderAttributionEnhancedTransform.attribute_pcp_plurality."""

    @pytest.mark.unit
    def test_attributes_to_most_visited_provider(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        pcp_visits = pl.LazyFrame({
            "person_id": ["P1", "P1", "P1", "P1"],
            "claim_id": ["C1", "C2", "C3", "C4"],
            "claim_end_date": [
                date(2024, 1, 1), date(2024, 3, 1),
                date(2024, 6, 1), date(2024, 9, 1),
            ],
            "rendering_provider_npi": ["NPI1", "NPI1", "NPI1", "NPI2"],
            "paid_amount": [100.0, 100.0, 100.0, 200.0],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.attribute_pcp_plurality(
                pcp_visits, DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 1
        assert result["attributed_pcp_npi"][0] == "NPI1"
        assert result["pcp_visit_count"][0] == 3

    @pytest.mark.unit
    def test_output_columns(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        pcp_visits = pl.LazyFrame({
            "person_id": ["P1"],
            "claim_id": ["C1"],
            "claim_end_date": [date(2024, 5, 1)],
            "rendering_provider_npi": ["NPI1"],
            "paid_amount": [100.0],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.attribute_pcp_plurality(
                pcp_visits, DEFAULT_CONFIG
            )
        )
        expected = {
            "person_id", "attributed_pcp_npi", "pcp_visit_count",
            "pcp_total_paid", "first_visit_date", "last_visit_date",
        }
        assert set(result.columns) == expected

    @pytest.mark.unit
    def test_multiple_members(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        pcp_visits = pl.LazyFrame({
            "person_id": ["P1", "P1", "P2", "P2"],
            "claim_id": ["C1", "C2", "C3", "C4"],
            "claim_end_date": [
                date(2024, 1, 1), date(2024, 3, 1),
                date(2024, 5, 1), date(2024, 7, 1),
            ],
            "rendering_provider_npi": ["NPI1", "NPI1", "NPI2", "NPI2"],
            "paid_amount": [100.0, 150.0, 200.0, 250.0],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.attribute_pcp_plurality(
                pcp_visits, DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 2


class TestProviderContinuityOfCare:
    """Tests for ProviderAttributionEnhancedTransform.calculate_continuity_of_care."""

    @pytest.mark.unit
    def test_perfect_continuity(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        # All visits to same provider => UPC = 1.0
        pcp_visits = pl.LazyFrame({
            "person_id": ["P1", "P1", "P1"],
            "rendering_provider_npi": ["NPI1", "NPI1", "NPI1"],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.calculate_continuity_of_care(
                pcp_visits, DEFAULT_CONFIG
            )
        )
        assert abs(result["upc_index"][0] - 1.0) < 0.01
        assert result["continuity_category"][0] == "adequate"

    @pytest.mark.unit
    def test_split_continuity(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        # 2 visits to NPI1, 2 visits to NPI2 => UPC = 0.5
        pcp_visits = pl.LazyFrame({
            "person_id": ["P1", "P1", "P1", "P1"],
            "rendering_provider_npi": ["NPI1", "NPI1", "NPI2", "NPI2"],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.calculate_continuity_of_care(
                pcp_visits, DEFAULT_CONFIG
            )
        )
        assert abs(result["upc_index"][0] - 0.5) < 0.01

    @pytest.mark.unit
    def test_minimal_continuity_category(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        # 1-2 visits => minimal
        pcp_visits = pl.LazyFrame({
            "person_id": ["P1", "P1"],
            "rendering_provider_npi": ["NPI1", "NPI2"],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.calculate_continuity_of_care(
                pcp_visits, DEFAULT_CONFIG
            )
        )
        assert result["continuity_category"][0] == "minimal"

    @pytest.mark.unit
    def test_output_columns(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        pcp_visits = pl.LazyFrame({
            "person_id": ["P1"],
            "rendering_provider_npi": ["NPI1"],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.calculate_continuity_of_care(
                pcp_visits, DEFAULT_CONFIG
            )
        )
        expected = {
            "person_id", "total_pcp_visits", "visits_to_most_common_provider",
            "upc_index", "continuity_category",
        }
        assert set(result.columns) == expected


class TestProviderSpecialistUtilization:
    """Tests for ProviderAttributionEnhancedTransform.analyze_specialist_utilization."""

    @pytest.mark.unit
    def test_excludes_pcp_specialties(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = pl.LazyFrame({
            "person_id": ["P1", "P1", "P1"],
            "claim_id": ["C1", "C2", "C3"],
            "claim_type": ["professional", "professional", "professional"],
            "claim_end_date": [date(2024, 3, 1), date(2024, 6, 1), date(2024, 9, 1)],
            "rendering_provider_npi": ["NPI1", "NPI2", "NPI3"],
            "rendering_provider_specialty": ["08", "20", "30"],  # 08 is PCP
            "paid_amount": [100.0, 500.0, 300.0],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.analyze_specialist_utilization(
                claims, DEFAULT_CONFIG
            )
        )
        p1 = result.filter(pl.col("person_id") == "P1")
        # Only specialties 20 and 30 should count
        assert p1["total_specialist_visits"][0] == 2
        assert p1["unique_specialists"][0] == 2

    @pytest.mark.unit
    def test_excludes_institutional_claims(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = pl.LazyFrame({
            "person_id": ["P1", "P1"],
            "claim_id": ["C1", "C2"],
            "claim_type": ["professional", "institutional"],
            "claim_end_date": [date(2024, 3, 1), date(2024, 6, 1)],
            "rendering_provider_npi": ["NPI1", "NPI2"],
            "rendering_provider_specialty": ["20", "20"],
            "paid_amount": [500.0, 1000.0],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.analyze_specialist_utilization(
                claims, DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 1
        assert result["total_specialist_visits"][0] == 1


class TestProviderRankByQuality:
    """Tests for ProviderAttributionEnhancedTransform.rank_providers_by_quality."""

    def _make_claims(self) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1", "P2", "P3"],
            "claim_id": ["C1", "C2", "C3"],
            "claim_type": ["institutional", "institutional", "institutional"],
            "bill_type_code": ["111", "111", "111"],
            "claim_end_date": [date(2024, 3, 1), date(2024, 6, 1), date(2024, 9, 1)],
            "admitting_provider_npi": ["NPIADM1", "NPIADM1", "NPIADM2"],
            "paid_amount": [10000.0, 15000.0, 8000.0],
        })

    @pytest.mark.unit
    def test_without_readmissions(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = self._make_claims()
        result = collect(
            ProviderAttributionEnhancedTransform.rank_providers_by_quality(
                claims, None, DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 2
        assert "total_admissions" in result.columns
        assert "avg_admission_cost" in result.columns
        assert "cost_rank" in result.columns

    @pytest.mark.unit
    def test_with_readmissions(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = self._make_claims()
        readmission_pairs = pl.LazyFrame({
            "index_admission_id": ["C1"],
            "readmission_id": ["C2"],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.rank_providers_by_quality(
                claims, readmission_pairs, DEFAULT_CONFIG
            )
        )
        assert "readmissions" in result.columns
        assert "readmission_rate_pct" in result.columns
        adm1 = result.filter(pl.col("admitting_provider_npi") == "NPIADM1")
        assert adm1["readmissions"][0] == 1

    @pytest.mark.unit
    def test_filters_institutional_only(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        claims = pl.LazyFrame({
            "person_id": ["P1", "P2"],
            "claim_id": ["C1", "C2"],
            "claim_type": ["institutional", "professional"],
            "bill_type_code": ["111", "111"],
            "claim_end_date": [date(2024, 3, 1), date(2024, 6, 1)],
            "admitting_provider_npi": ["NPI1", "NPI2"],
            "paid_amount": [10000.0, 200.0],
        })
        result = collect(
            ProviderAttributionEnhancedTransform.rank_providers_by_quality(
                claims, None, DEFAULT_CONFIG
            )
        )
        assert result.shape[0] == 1


class TestProviderAttributionOrchestrator:
    """Tests for ProviderAttributionEnhancedTransform.calculate_provider_attribution."""

    def _make_claims(self) -> pl.LazyFrame:
        return pl.LazyFrame({
            "person_id": ["P1", "P1", "P1", "P2", "P2"],
            "claim_id": ["C1", "C2", "C3", "C4", "C5"],
            "claim_type": ["professional"] * 5,
            "claim_end_date": [
                date(2024, 1, 15), date(2024, 4, 15), date(2024, 7, 15),
                date(2024, 2, 10), date(2024, 8, 10),
            ],
            "rendering_provider_npi": ["NPI1", "NPI1", "NPI2", "NPI3", "NPI3"],
            "rendering_provider_specialty": ["08", "08", "20", "11", "11"],
            "procedure_code": ["99213", "99214", "99244", "99213", "99215"],
            "paid_amount": [150.0, 200.0, 500.0, 180.0, 220.0],
        })

    def _make_eligibility(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "person_id": ["P1", "P2"],
                "enrollment_start_date": [date(2024, 1, 1)] * 2,
                "enrollment_end_date": [date(2024, 12, 31)] * 2,
                "age": [55, 40],
                "gender": ["M", "F"],
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
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        result = ProviderAttributionEnhancedTransform.calculate_provider_attribution(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        assert len(result) == 4

    @pytest.mark.unit
    def test_pcp_attribution_present(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        member_attr, _, _, _ = ProviderAttributionEnhancedTransform.calculate_provider_attribution(
            self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
        )
        df = collect(member_attr)
        assert "attributed_pcp_npi" in df.columns
        assert df.shape[0] > 0

    @pytest.mark.unit
    def test_provider_summary(self):
        from acoharmony._transforms.provider_attribution_enhanced import (
            ProviderAttributionEnhancedTransform,
        )

        _, _, _, provider_summary = (
            ProviderAttributionEnhancedTransform.calculate_provider_attribution(
                self._make_claims(), self._make_eligibility(), DEFAULT_CONFIG
            )
        )
        df = collect(provider_summary)
        assert "attributed_members" in df.columns
        assert "total_visits" in df.columns
        assert "total_revenue" in df.columns
