# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.sdoh module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date
from typing import Any

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


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _lazy(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestSdohTransformPublic:
    """Tests for sdoh public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import sdoh
        assert sdoh is not None

    @pytest.mark.unit
    def test_sdoh_transform_class(self):
        from acoharmony._transforms.sdoh import SdohTransform
        assert SdohTransform is not None

    @pytest.mark.unit
    def test_z_code_categories(self):
        from acoharmony._transforms.sdoh import SdohTransform
        assert hasattr(SdohTransform, "Z_CODE_CATEGORIES")
        cats = SdohTransform.Z_CODE_CATEGORIES
        assert "housing_instability" in cats
        assert "food_insecurity" in cats
        assert "transportation_barriers" in cats
        assert "financial_insecurity" in cats
        assert "education_literacy" in cats
        assert "employment" in cats
        assert "social_isolation" in cats


class TestSdohZCodeCategories:
    """Tests for SDOH Z-code categories completeness."""

    @pytest.mark.unit
    def test_housing_instability_codes(self):
        from acoharmony._transforms.sdoh import SdohTransform
        codes = SdohTransform.Z_CODE_CATEGORIES["housing_instability"]
        assert "Z59.0" in codes
        assert len(codes) >= 4

    @pytest.mark.unit
    def test_food_insecurity_codes(self):
        from acoharmony._transforms.sdoh import SdohTransform
        codes = SdohTransform.Z_CODE_CATEGORIES["food_insecurity"]
        assert "Z59.4" in codes

    @pytest.mark.unit
    def test_transportation_codes(self):
        from acoharmony._transforms.sdoh import SdohTransform
        codes = SdohTransform.Z_CODE_CATEGORIES["transportation_barriers"]
        assert len(codes) >= 2

    @pytest.mark.unit
    def test_financial_codes(self):
        from acoharmony._transforms.sdoh import SdohTransform
        codes = SdohTransform.Z_CODE_CATEGORIES["financial_insecurity"]
        assert len(codes) >= 3

    @pytest.mark.unit
    def test_education_codes(self):
        from acoharmony._transforms.sdoh import SdohTransform
        codes = SdohTransform.Z_CODE_CATEGORIES["education_literacy"]
        assert "Z55.0" in codes

    @pytest.mark.unit
    def test_employment_codes(self):
        from acoharmony._transforms.sdoh import SdohTransform
        codes = SdohTransform.Z_CODE_CATEGORIES["employment"]
        assert "Z56.0" in codes

    @pytest.mark.unit
    def test_social_isolation_codes(self):
        from acoharmony._transforms.sdoh import SdohTransform
        codes = SdohTransform.Z_CODE_CATEGORIES["social_isolation"]
        assert "Z60.2" in codes


class TestSdohIdentifyZCodes:
    """Tests for SdohTransform.identify_z_codes."""

    def _make_claims(self) -> pl.LazyFrame:
        return _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2", "P3", "P4"],
                    "diagnosis_code_1": ["Z59.0", "E11.9", "Z59.4", "J44.1"],
                    "diagnosis_code_2": [None, "Z56.0", None, None],
                    "diagnosis_code_3": [None, None, None, "Z60.2"],
                    "claim_end_date": [
                        date(2024, 1, 1),
                        date(2024, 2, 1),
                        date(2024, 3, 1),
                        date(2024, 4, 1),
                    ],
                }
            )
        )

    @pytest.mark.unit
    def test_identifies_z_codes(self):
        from acoharmony._transforms.sdoh import SdohTransform

        result = SdohTransform.identify_z_codes(
            self._make_claims(), {"measurement_year": 2024}
        ).collect()

        assert "z_code" in result.columns
        # P1 has Z59.0 in dx1, P2 has Z56.0 in dx2, P3 has Z59.4 in dx1, P4 has Z60.2 in dx3
        assert result.height == 4

    @pytest.mark.unit
    def test_filters_by_measurement_year(self):
        from acoharmony._transforms.sdoh import SdohTransform

        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2"],
                    "diagnosis_code_1": ["Z59.0", "Z59.0"],
                    "diagnosis_code_2": [None, None],
                    "diagnosis_code_3": [None, None],
                    "claim_end_date": [date(2024, 1, 1), date(2023, 6, 1)],
                }
            )
        )

        result = SdohTransform.identify_z_codes(
            claims, {"measurement_year": 2024}
        ).collect()

        # Only 2024 claims
        assert result.height == 1
        assert result["person_id"][0] == "P1"

    @pytest.mark.unit
    def test_no_z_codes(self):
        from acoharmony._transforms.sdoh import SdohTransform

        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "diagnosis_code_1": ["E11.9"],
                    "diagnosis_code_2": pl.Series([None], dtype=pl.Utf8),
                    "diagnosis_code_3": pl.Series([None], dtype=pl.Utf8),
                    "claim_end_date": [date(2024, 1, 1)],
                }
            )
        )
        result = SdohTransform.identify_z_codes(
            claims, {"measurement_year": 2024}
        ).collect()

        assert result.height == 0


class TestSdohCategorizeRiskFactors:
    """Tests for SdohTransform.categorize_sdoh_risk_factors."""

    @pytest.mark.unit
    def test_categorization(self):
        from acoharmony._transforms.sdoh import SdohTransform

        z_claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2", "P3", "P4"],
                    "z_code": ["Z59.0", "Z59.4", "Z56.0", "Z71.41"],
                    "claim_end_date": [date(2024, 1, 1)] * 4,
                }
            )
        )

        result = SdohTransform.categorize_sdoh_risk_factors(z_claims, {}).collect()

        assert "sdoh_category" in result.columns
        categories = dict(
            zip(result["person_id"].to_list(), result["sdoh_category"].to_list(), strict=False)
        )
        assert categories["P1"] == "housing_instability"
        assert categories["P2"] == "food_insecurity"
        assert categories["P3"] == "employment"
        assert categories["P4"] == "substance_use"

    @pytest.mark.unit
    def test_other_z_code_fallback(self):
        from acoharmony._transforms.sdoh import SdohTransform

        z_claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "z_code": ["Z99.99"],  # Not in any SDOH category
                    "claim_end_date": [date(2024, 1, 1)],
                }
            )
        )
        result = SdohTransform.categorize_sdoh_risk_factors(z_claims, {}).collect()

        assert result["sdoh_category"][0] == "other_z_code"


class TestSdohMemberProfile:
    """Tests for SdohTransform.calculate_member_sdoh_profile."""

    @pytest.mark.unit
    def test_profile_aggregation(self):
        from acoharmony._transforms.sdoh import SdohTransform

        sdoh_claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1", "P1", "P2"],
                    "sdoh_category": [
                        "housing_instability",
                        "food_insecurity",
                        "transportation_barriers",
                        "housing_instability",
                    ],
                    "claim_end_date": [
                        date(2024, 1, 1),
                        date(2024, 2, 1),
                        date(2024, 3, 1),
                        date(2024, 4, 1),
                    ],
                }
            )
        )
        result = SdohTransform.calculate_member_sdoh_profile(
            sdoh_claims, {}
        ).collect()

        assert result.height == 2

        p1 = result.filter(pl.col("person_id") == "P1")
        assert p1["unique_sdoh_factors"][0] == 3
        assert p1["is_high_sdoh_risk"][0] is True
        assert p1["sdoh_risk_level"][0] == "high"

        p2 = result.filter(pl.col("person_id") == "P2")
        assert p2["unique_sdoh_factors"][0] == 1
        assert p2["is_high_sdoh_risk"][0] is False
        assert p2["sdoh_risk_level"][0] == "identified"

    @pytest.mark.unit
    def test_moderate_risk_level(self):
        from acoharmony._transforms.sdoh import SdohTransform

        sdoh_claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1"],
                    "sdoh_category": ["housing_instability", "food_insecurity"],
                    "claim_end_date": [date(2024, 1, 1), date(2024, 2, 1)],
                }
            )
        )
        result = SdohTransform.calculate_member_sdoh_profile(
            sdoh_claims, {}
        ).collect()

        assert result["sdoh_risk_level"][0] == "moderate"


class TestSdohScreeningRates:
    """Tests for SdohTransform.calculate_screening_rates."""

    @pytest.mark.unit
    def test_screening_rate_calculation(self):
        from acoharmony._transforms.sdoh import SdohTransform

        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2", "P3", "P4", "P5"],
                    "enrollment_start_date": [date(2024, 1, 1)] * 5,
                }
            )
        )
        sdoh_claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2", "P3"],
                    "sdoh_category": [
                        "sdoh_screening",
                        "mental_health_screening",
                        "housing_instability",
                    ],
                    "claim_end_date": [date(2024, 1, 1)] * 3,
                }
            )
        )

        result = SdohTransform.calculate_screening_rates(
            eligibility, sdoh_claims, {"measurement_year": 2024}
        ).collect()

        assert "screening_rate_pct" in result.columns
        assert "identification_rate_pct" in result.columns
        assert "positive_screen_rate_pct" in result.columns
        assert result["total_members"][0] == 5
        assert result["screened_members"][0] == 2  # P1, P2
        assert result["identified_members"][0] == 1  # P3


class TestSdohRiskFactorPrevalence:
    """Tests for SdohTransform.calculate_risk_factor_prevalence."""

    @pytest.mark.unit
    def test_prevalence_counts(self):
        from acoharmony._transforms.sdoh import SdohTransform

        sdoh_claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2", "P1"],
                    "sdoh_category": [
                        "housing_instability",
                        "housing_instability",
                        "food_insecurity",
                    ],
                }
            )
        )

        result = SdohTransform.calculate_risk_factor_prevalence(
            sdoh_claims, {}
        ).collect()

        assert "member_count" in result.columns
        assert "total_claims" in result.columns

        housing = result.filter(pl.col("sdoh_category") == "housing_instability")
        assert housing["member_count"][0] == 2
        assert housing["total_claims"][0] == 2


class TestSdohAnalyze:
    """Tests for SdohTransform.analyze_sdoh (integration)."""

    @pytest.mark.unit
    def test_end_to_end(self):
        from acoharmony._transforms.sdoh import SdohTransform

        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2"],
                    "diagnosis_code_1": ["Z59.0", "Z59.4"],
                    "diagnosis_code_2": [None, None],
                    "diagnosis_code_3": [None, None],
                    "claim_end_date": [date(2024, 1, 1), date(2024, 2, 1)],
                }
            )
        )
        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2", "P3"],
                    "enrollment_start_date": [date(2024, 1, 1)] * 3,
                }
            )
        )

        profile, prevalence, rates, z_claims = SdohTransform.analyze_sdoh(
            claims, eligibility, {"measurement_year": 2024}
        )

        assert isinstance(profile, pl.LazyFrame)
        assert isinstance(prevalence, pl.LazyFrame)
        assert isinstance(rates, pl.LazyFrame)
        assert isinstance(z_claims, pl.LazyFrame)

        profile_df = profile.collect()
        assert profile_df.height == 2


class TestIdentifyZCodes:
    """Tests for SdohTransform.identify_z_codes."""

    def _call(self, claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        from acoharmony._transforms.sdoh import SdohTransform

        return SdohTransform.identify_z_codes(claims, config)

    @pytest.mark.unit
    def test_identifies_z_codes_in_diag1(self):
        claims = pl.LazyFrame(
            {
                "claim_end_date": [_date(2024, 5, 1)],
                "diagnosis_code_1": ["Z59.0"],
                "diagnosis_code_2": ["E11.9"],
                "diagnosis_code_3": ["I10"],
            }
        )
        result = self._call(claims, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 1
        assert result["z_code"][0] == "Z59.0"

    @pytest.mark.unit
    def test_identifies_z_codes_in_diag2(self):
        claims = pl.LazyFrame(
            {
                "claim_end_date": [_date(2024, 5, 1)],
                "diagnosis_code_1": ["E11.9"],
                "diagnosis_code_2": ["Z59.4"],
                "diagnosis_code_3": ["I10"],
            }
        )
        result = self._call(claims, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 1
        assert result["z_code"][0] == "Z59.4"

    @pytest.mark.unit
    def test_identifies_z_codes_in_diag3(self):
        claims = pl.LazyFrame(
            {
                "claim_end_date": [_date(2024, 5, 1)],
                "diagnosis_code_1": ["E11.9"],
                "diagnosis_code_2": ["I10"],
                "diagnosis_code_3": ["Z60.2"],
            }
        )
        result = self._call(claims, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 1
        assert result["z_code"][0] == "Z60.2"

    @pytest.mark.unit
    def test_filters_by_measurement_year(self):
        claims = pl.LazyFrame(
            {
                "claim_end_date": [_date(2023, 5, 1), _date(2024, 5, 1)],
                "diagnosis_code_1": ["Z59.0", "Z59.0"],
                "diagnosis_code_2": ["E11.9", "E11.9"],
                "diagnosis_code_3": ["I10", "I10"],
            }
        )
        result = self._call(claims, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 1

    @pytest.mark.unit
    def test_no_z_codes(self):
        claims = pl.LazyFrame(
            {
                "claim_end_date": [_date(2024, 5, 1)],
                "diagnosis_code_1": ["E11.9"],
                "diagnosis_code_2": ["I10"],
                "diagnosis_code_3": ["J44.1"],
            }
        )
        result = self._call(claims, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 0

    @pytest.mark.unit
    def test_default_measurement_year(self):
        claims = pl.LazyFrame(
            {
                "claim_end_date": [_date(2024, 5, 1)],
                "diagnosis_code_1": ["Z59.0"],
                "diagnosis_code_2": ["E11.9"],
                "diagnosis_code_3": ["I10"],
            }
        )
        result = self._call(claims, {}).collect()
        assert result.shape[0] == 1


class TestCategorizeSDOHRiskFactors:
    """Tests for SdohTransform.categorize_sdoh_risk_factors."""

    def _call(self, z_code_claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        from acoharmony._transforms.sdoh import SdohTransform

        return SdohTransform.categorize_sdoh_risk_factors(z_code_claims, config)

    def _make_claims(self, z_codes: list[str]) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "person_id": [f"P{i}" for i in range(len(z_codes))],
                "z_code": z_codes,
                "claim_end_date": [_date(2024, 5, 1)] * len(z_codes),
            }
        )

    @pytest.mark.unit
    def test_housing_instability(self):
        result = self._call(self._make_claims(["Z59.0"]), {}).collect()
        assert result["sdoh_category"][0] == "housing_instability"

    @pytest.mark.unit
    def test_food_insecurity(self):
        result = self._call(self._make_claims(["Z59.4"]), {}).collect()
        assert result["sdoh_category"][0] == "food_insecurity"

    @pytest.mark.unit
    def test_transportation_barriers(self):
        result = self._call(self._make_claims(["Z59.82"]), {}).collect()
        assert result["sdoh_category"][0] == "transportation_barriers"

    @pytest.mark.unit
    def test_financial_insecurity(self):
        result = self._call(self._make_claims(["Z59.5"]), {}).collect()
        assert result["sdoh_category"][0] == "financial_insecurity"

    @pytest.mark.unit
    def test_education_literacy(self):
        result = self._call(self._make_claims(["Z55.0"]), {}).collect()
        assert result["sdoh_category"][0] == "education_literacy"

    @pytest.mark.unit
    def test_employment(self):
        result = self._call(self._make_claims(["Z56.0"]), {}).collect()
        assert result["sdoh_category"][0] == "employment"

    @pytest.mark.unit
    def test_social_isolation(self):
        result = self._call(self._make_claims(["Z60.2"]), {}).collect()
        assert result["sdoh_category"][0] == "social_isolation"

    @pytest.mark.unit
    def test_interpersonal_violence(self):
        result = self._call(self._make_claims(["Z69.0"]), {}).collect()
        assert result["sdoh_category"][0] == "interpersonal_violence"

    @pytest.mark.unit
    def test_inadequate_support(self):
        result = self._call(self._make_claims(["Z63.0"]), {}).collect()
        assert result["sdoh_category"][0] == "inadequate_support"

    @pytest.mark.unit
    def test_legal_problems(self):
        result = self._call(self._make_claims(["Z65.0"]), {}).collect()
        assert result["sdoh_category"][0] == "legal_problems"

    @pytest.mark.unit
    def test_substance_use(self):
        result = self._call(self._make_claims(["Z71.41"]), {}).collect()
        assert result["sdoh_category"][0] == "substance_use"

    @pytest.mark.unit
    def test_mental_health_screening(self):
        result = self._call(self._make_claims(["Z13.31"]), {}).collect()
        assert result["sdoh_category"][0] == "mental_health_screening"

    @pytest.mark.unit
    def test_sdoh_screening_z5_prefix(self):
        """Z5x codes not in specific lists fall into sdoh_screening."""
        result = self._call(self._make_claims(["Z58.0"]), {}).collect()
        assert result["sdoh_category"][0] == "sdoh_screening"

    @pytest.mark.unit
    def test_sdoh_screening_z6_prefix(self):
        """Z6x codes not in specific lists fall into sdoh_screening."""
        result = self._call(self._make_claims(["Z61.0"]), {}).collect()
        assert result["sdoh_category"][0] == "sdoh_screening"

    @pytest.mark.unit
    def test_other_z_code(self):
        """Z-codes outside Z5x/Z6x ranges go to other_z_code."""
        result = self._call(self._make_claims(["Z00.0"]), {}).collect()
        assert result["sdoh_category"][0] == "other_z_code"


class TestCalculateMemberSdohProfile:
    """Tests for SdohTransform.calculate_member_sdoh_profile."""

    def _call(self, sdoh_claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        from acoharmony._transforms.sdoh import SdohTransform

        return SdohTransform.calculate_member_sdoh_profile(sdoh_claims, config)

    @pytest.mark.unit
    def test_high_sdoh_risk(self):
        sdoh_claims = pl.LazyFrame(
            {
                "person_id": ["P1"] * 4,
                "sdoh_category": ["housing_instability", "food_insecurity", "employment", "housing_instability"],
                "claim_end_date": [
                    _date(2024, 1, 1),
                    _date(2024, 3, 1),
                    _date(2024, 5, 1),
                    _date(2024, 7, 1),
                ],
            }
        )
        result = self._call(sdoh_claims, {}).collect()
        assert result.shape[0] == 1
        assert result["unique_sdoh_factors"][0] == 3
        assert result["is_high_sdoh_risk"][0] is True
        assert result["sdoh_risk_level"][0] == "high"
        assert result["sdoh_claim_count"][0] == 4

    @pytest.mark.unit
    def test_moderate_sdoh_risk(self):
        sdoh_claims = pl.LazyFrame(
            {
                "person_id": ["P1", "P1"],
                "sdoh_category": ["housing_instability", "food_insecurity"],
                "claim_end_date": [_date(2024, 1, 1), _date(2024, 3, 1)],
            }
        )
        result = self._call(sdoh_claims, {}).collect()
        assert result["sdoh_risk_level"][0] == "moderate"
        assert result["is_high_sdoh_risk"][0] is False

    @pytest.mark.unit
    def test_identified_sdoh_risk(self):
        sdoh_claims = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "sdoh_category": ["housing_instability"],
                "claim_end_date": [_date(2024, 1, 1)],
            }
        )
        result = self._call(sdoh_claims, {}).collect()
        assert result["sdoh_risk_level"][0] == "identified"

    @pytest.mark.unit
    def test_first_and_last_dates(self):
        sdoh_claims = pl.LazyFrame(
            {
                "person_id": ["P1", "P1"],
                "sdoh_category": ["housing_instability", "food_insecurity"],
                "claim_end_date": [_date(2024, 2, 15), _date(2024, 9, 20)],
            }
        )
        result = self._call(sdoh_claims, {}).collect()
        assert result["first_sdoh_date"][0] == _date(2024, 2, 15)
        assert result["last_sdoh_date"][0] == _date(2024, 9, 20)


class TestCalculateScreeningRates:
    """Tests for SdohTransform.calculate_screening_rates."""

    def _call(
        self,
        eligibility: pl.LazyFrame,
        sdoh_claims: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        from acoharmony._transforms.sdoh import SdohTransform

        return SdohTransform.calculate_screening_rates(eligibility, sdoh_claims, config)

    @pytest.mark.unit
    def test_screening_rates(self):
        eligibility = pl.LazyFrame(
            {
                "person_id": ["P1", "P2", "P3", "P4"],
                "enrollment_start_date": [_date(2024, 1, 1)] * 4,
            }
        )
        sdoh_claims = pl.LazyFrame(
            {
                "person_id": ["P1", "P2", "P3"],
                "sdoh_category": ["sdoh_screening", "mental_health_screening", "housing_instability"],
            }
        )
        result = self._call(eligibility, sdoh_claims, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 1
        assert "screening_rate_pct" in result.columns
        assert "identification_rate_pct" in result.columns
        assert "positive_screen_rate_pct" in result.columns
        # 2 screened out of 4 = 50%
        assert result["screening_rate_pct"][0] == pytest.approx(50.0)
        # 1 identified out of 4 = 25%
        assert result["identification_rate_pct"][0] == pytest.approx(25.0)


class TestCalculateRiskFactorPrevalence:
    """Tests for SdohTransform.calculate_risk_factor_prevalence."""

    def _call(self, sdoh_claims: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:
        from acoharmony._transforms.sdoh import SdohTransform

        return SdohTransform.calculate_risk_factor_prevalence(sdoh_claims, config)

    @pytest.mark.unit
    def test_prevalence_calculation(self):
        sdoh_claims = pl.LazyFrame(
            {
                "person_id": ["P1", "P1", "P2", "P3"],
                "sdoh_category": [
                    "housing_instability",
                    "housing_instability",
                    "housing_instability",
                    "food_insecurity",
                ],
            }
        )
        result = self._call(sdoh_claims, {}).collect()
        assert result.shape[0] == 2
        # housing_instability should be first (more members)
        assert result["sdoh_category"][0] == "housing_instability"
        assert result["member_count"][0] == 2
        assert result["total_claims"][0] == 3

    @pytest.mark.unit
    def test_sorted_descending(self):
        sdoh_claims = pl.LazyFrame(
            {
                "person_id": ["P1", "P2", "P2", "P3", "P3", "P3"],
                "sdoh_category": ["A", "B", "B", "B", "B", "B"],
            }
        )
        result = self._call(sdoh_claims, {}).collect()
        counts = result["member_count"].to_list()
        assert counts == sorted(counts, reverse=True)


class TestAnalyzeSdoh:
    """Tests for SdohTransform.analyze_sdoh (orchestrator)."""

    def _call(self, claims, eligibility, config):
        from acoharmony._transforms.sdoh import SdohTransform

        return SdohTransform.analyze_sdoh(claims, eligibility, config)

    @pytest.mark.unit
    def test_full_pipeline(self):
        claims = pl.LazyFrame(
            {
                "claim_end_date": [_date(2024, 5, 1), _date(2024, 6, 1), _date(2024, 7, 1)],
                "diagnosis_code_1": ["Z59.0", "Z59.4", "E11.9"],
                "diagnosis_code_2": ["E11.9", "E11.9", "Z55.0"],
                "diagnosis_code_3": ["I10", "I10", "I10"],
                "person_id": ["P1", "P2", "P3"],
            }
        )
        eligibility = pl.LazyFrame(
            {
                "person_id": ["P1", "P2", "P3"],
                "enrollment_start_date": [_date(2024, 1, 1)] * 3,
            }
        )
        member_profile, prevalence, screening_rates, sdoh_claims = self._call(
            claims, eligibility, {"measurement_year": 2024}
        )
        assert member_profile.collect().shape[0] >= 1
        assert prevalence.collect().shape[0] >= 1
        assert screening_rates.collect().shape[0] == 1
        assert sdoh_claims.collect().shape[0] >= 1
