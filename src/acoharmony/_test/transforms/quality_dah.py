# © 2025 HarmonyCares — tests for acoharmony._transforms._quality_dah
"""Unit tests for the DAH (Days at Home) measure transform."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from acoharmony._transforms._quality_dah import DaysAtHome
from acoharmony._transforms._quality_measure_base import MeasureFactory


@pytest.mark.unit
class TestDaysAtHomeRegistration:
    def test_registered_with_factory(self):
        assert "REACH_DAH" in MeasureFactory.list_measures()
        instance = MeasureFactory.create("REACH_DAH", config={"performance_year": 2024})
        assert isinstance(instance, DaysAtHome)


@pytest.mark.unit
class TestDaysAtHomeMetadata:
    def test_metadata_fields(self):
        meta = DaysAtHome().get_metadata()
        assert meta.measure_id == "REACH_DAH"
        assert "Days at Home" in meta.measure_name
        assert meta.exclusions_description is None


@pytest.mark.unit
class TestDaysAtHomeDenominator:
    def test_includes_overlapping_enrollment(self):
        py = 2024
        elig = pl.LazyFrame(
            {
                "person_id": ["A", "B", "C", "D"],
                "enrollment_start_date": [
                    date(2024, 1, 1),  # whole year — in
                    date(2024, 6, 1),  # mid-year start — in
                    date(2025, 1, 1),  # next year — out
                    date(2023, 1, 1),  # ended in 2023 — out
                ],
                "enrollment_end_date": [
                    date(2024, 12, 31),
                    None,
                    date(2025, 12, 31),
                    date(2023, 12, 31),
                ],
            }
        )
        m = DaysAtHome(config={"performance_year": py})
        denom = m.calculate_denominator(pl.LazyFrame(), elig, {}).collect()
        assert sorted(denom["person_id"].to_list()) == ["A", "B"]


@pytest.mark.unit
class TestDaysAtHomeNumerator:
    @pytest.fixture
    def elig(self):
        return pl.LazyFrame(
            {
                "person_id": ["A", "B"],
                "birth_date": [date(1950, 1, 1), date(1950, 1, 1)],
                "death_date": [None, date(2024, 7, 1)],
                "enrollment_start_date": [date(2024, 1, 1), date(2024, 1, 1)],
                "enrollment_end_date": [date(2024, 12, 31), date(2024, 12, 31)],
            }
        )

    def test_no_claims_no_death_full_dah(self, elig):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame(
            {"person_id": ["A"], "denominator_flag": [True]}
        )
        claims = pl.LazyFrame(
            {
                "person_id": [],
                "bill_type_code": [],
                "admission_date": [],
                "discharge_date": [],
            },
            schema={
                "person_id": pl.Utf8,
                "bill_type_code": pl.Utf8,
                "admission_date": pl.Date,
                "discharge_date": pl.Date,
            },
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        # ACO REACH spec uses a 365-day window even in leap years.
        assert result["survival_days"].to_list() == [365]
        assert result["observed_dic"].to_list() == [0]
        assert result["observed_dah"].to_list() == [365]

    def test_inpatient_stay_reduces_dah(self, elig):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame(
            {"person_id": ["A"], "denominator_flag": [True]}
        )
        claims = pl.LazyFrame(
            {
                "person_id": ["A"],
                "bill_type_code": ["111"],  # inpatient
                "admission_date": [date(2024, 3, 1)],
                "discharge_date": [date(2024, 3, 10)],  # 10 days inclusive
            }
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [10]
        assert result["observed_dah"].to_list() == [365 - 10]

    def test_death_truncates_survival(self, elig):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame(
            {"person_id": ["B"], "denominator_flag": [True]}
        )
        claims = pl.LazyFrame(
            {
                "person_id": [],
                "bill_type_code": [],
                "admission_date": [],
                "discharge_date": [],
            },
            schema={
                "person_id": pl.Utf8,
                "bill_type_code": pl.Utf8,
                "admission_date": pl.Date,
                "discharge_date": pl.Date,
            },
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        # B died 2024-07-01 → days from Jan 1 to Jul 1 inclusive = 183
        assert result["survival_days"].to_list() == [183]
        assert result["observed_dah"].to_list() == [183]

    def test_non_institutional_bill_type_ignored(self, elig):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["A"], "denominator_flag": [True]})
        claims = pl.LazyFrame(
            {
                "person_id": ["A"],
                "bill_type_code": ["131"],  # outpatient — not in our prefix set
                "admission_date": [date(2024, 3, 1)],
                "discharge_date": [date(2024, 3, 10)],
            }
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [0]
        assert result["observed_dah"].to_list() == [365]

    def test_missing_eligibility_raises(self):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["A"], "denominator_flag": [True]})
        claims = pl.LazyFrame(
            {"person_id": [], "bill_type_code": [], "admission_date": [],
             "discharge_date": []},
            schema={"person_id": pl.Utf8, "bill_type_code": pl.Utf8,
                    "admission_date": pl.Date, "discharge_date": pl.Date},
        )
        with pytest.raises(ValueError, match="value_sets\\['eligibility'\\]"):
            m.calculate_numerator(denom, claims, {}).collect()


@pytest.mark.unit
class TestDaysAtHomeExclusions:
    def test_exclusions_always_false(self):
        m = DaysAtHome()
        denom = pl.LazyFrame({"person_id": ["A", "B"], "denominator_flag": [True, True]})
        excl = m.calculate_exclusions(denom, pl.LazyFrame(), {}).collect()
        assert excl["exclusion_flag"].to_list() == [False, False]
