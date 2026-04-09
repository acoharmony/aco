"""Tests for _transforms.utilization module."""

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


def collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame into a DataFrame."""
    return lf.collect()


def _lazy(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()
class TestUtilizationPublic:
    """Tests for utilization public transform module."""

    @pytest.mark.unit
    def test_import_module(self):
        assert utilization is not None

    @pytest.mark.unit
    def test_utilization_transform_class(self):
        assert UtilizationTransform is not None


class TestUtilizationMemberYears:
    """Tests for UtilizationTransform.calculate_member_years."""

    @pytest.mark.unit
    def test_full_year_enrollment(self):

        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "enrollment_start_date": [date(2024, 1, 1)],
                    "enrollment_end_date": [date(2024, 12, 31)],
                }
            )
        )

        result = UtilizationTransform.calculate_member_years(
            eligibility, {"measurement_year": 2024}
        ).collect()

        assert result.height == 1
        # Full year = 365 days
        assert result["total_days_enrolled"][0] == 365
        assert result["member_years"][0] == pytest.approx(365 / 365.25, rel=0.01)

    @pytest.mark.unit
    def test_partial_year(self):

        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "enrollment_start_date": [date(2024, 7, 1)],
                    "enrollment_end_date": [date(2024, 12, 31)],
                }
            )
        )

        result = UtilizationTransform.calculate_member_years(
            eligibility, {"measurement_year": 2024}
        ).collect()

        assert result.height == 1
        # July 1 to Dec 31 = 183 days
        assert result["total_days_enrolled"][0] == 183

    @pytest.mark.unit
    def test_enrollment_spanning_years(self):

        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "enrollment_start_date": [date(2023, 6, 1)],
                    "enrollment_end_date": [date(2025, 6, 30)],
                }
            )
        )

        result = UtilizationTransform.calculate_member_years(
            eligibility, {"measurement_year": 2024}
        ).collect()

        # Clipped to 2024-01-01 to 2024-12-31
        assert result.height == 1
        assert result["total_days_enrolled"][0] == 365

    @pytest.mark.unit
    def test_no_eligible_members(self):

        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "enrollment_start_date": [date(2023, 1, 1)],
                    "enrollment_end_date": [date(2023, 12, 31)],
                }
            )
        )

        result = UtilizationTransform.calculate_member_years(
            eligibility, {"measurement_year": 2024}
        ).collect()

        # No overlap with 2024
        assert result.height == 0


class TestUtilizationVisitUtilization:
    """Tests for UtilizationTransform.calculate_visit_utilization."""

    @pytest.mark.unit
    def test_visits_per_member_year(self):

        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1", "P1", "P2"],
                    "service_category_2": ["office_visit", "office_visit", "er", "office_visit"],
                    "claim_end_date": [date(2024, 1, 1)] * 4,
                }
            )
        )
        member_years = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2"],
                    "member_years": [1.0, 0.5],
                }
            )
        )

        result = UtilizationTransform.calculate_visit_utilization(
            claims, member_years, {"measurement_year": 2024}
        ).collect()

        assert "visit_count" in result.columns
        assert "visits_per_member_year" in result.columns
        assert result.height > 0


class TestUtilizationBedDays:
    """Tests for UtilizationTransform.calculate_bed_days."""

    @pytest.mark.unit
    def test_bed_day_calculation(self):

        admissions = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1"],
                    "length_of_stay": [5, 3],
                }
            )
        )
        member_years = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "member_years": [1.0],
                }
            )
        )

        result = UtilizationTransform.calculate_bed_days(
            admissions, member_years, {}
        ).collect()

        assert result["total_bed_days"][0] == 8
        assert result["bed_days_per_1000"][0] == 8000.0


class TestUtilizationAdmissionRates:
    """Tests for UtilizationTransform.calculate_admission_rates."""

    @pytest.mark.unit
    def test_admission_rates(self):

        admissions = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1", "P2"],
                    "encounter_type": ["inpatient", "er", "inpatient"],
                }
            )
        )
        member_years = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2"],
                    "member_years": [1.0, 1.0],
                }
            )
        )

        result = UtilizationTransform.calculate_admission_rates(
            admissions, member_years, {}
        ).collect()

        assert "admission_count" in result.columns
        assert "admissions_per_1000" in result.columns
        assert result.height > 0


class TestUtilizationHighUtilizers:
    """Tests for UtilizationTransform.identify_high_utilizers."""

    @pytest.mark.unit
    def test_high_utilizer_identification(self):

        visit_util = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P2", "P3"] * 5,
                    "service_category_2": ["office_visit"] * 15,
                    "visit_count": [20, 2, 1] * 5,
                    "visits_per_member_year": [20.0, 2.0, 1.0] * 5,
                }
            )
        )

        result = UtilizationTransform.identify_high_utilizers(
            visit_util, {}
        ).collect()

        assert "is_high_utilizer" in result.columns
        assert "utilization_tier" in result.columns


class TestUtilizationServiceMix:
    """Tests for UtilizationTransform.calculate_service_mix."""

    @pytest.mark.unit
    def test_service_mix_percentages(self):

        visit_util = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1", "P2"],
                    "service_category_2": ["office_visit", "er", "office_visit"],
                    "visit_count": [10, 5, 8],
                    "visits_per_member_year": [10.0, 5.0, 8.0],
                }
            )
        )

        result = UtilizationTransform.calculate_service_mix(visit_util, {}).collect()

        assert "percentage_of_visits" in result.columns
        assert "total_visits" in result.columns
        # Should sum to ~100
        total_pct = result["percentage_of_visits"].sum()
        assert total_pct == pytest.approx(100.0, rel=0.01)


class TestUtilizationCalculateMetrics:
    """Tests for UtilizationTransform.calculate_utilization_metrics (integration)."""

    @pytest.mark.unit
    def test_end_to_end_with_admissions(self):

        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1", "P1"],
                    "service_category_2": ["office_visit", "er"],
                    "claim_end_date": [date(2024, 3, 1), date(2024, 6, 1)],
                }
            )
        )
        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "enrollment_start_date": [date(2024, 1, 1)],
                    "enrollment_end_date": [date(2024, 12, 31)],
                }
            )
        )
        admissions = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "encounter_type": ["inpatient"],
                    "length_of_stay": [5],
                }
            )
        )

        visit_util, adm_rates, bed_days, high_util, svc_mix = (
            UtilizationTransform.calculate_utilization_metrics(
                claims, eligibility, admissions, {"measurement_year": 2024}
            )
        )

        assert isinstance(visit_util, pl.LazyFrame)
        assert isinstance(adm_rates, pl.LazyFrame)
        assert isinstance(bed_days, pl.LazyFrame)
        assert isinstance(high_util, pl.LazyFrame)
        assert isinstance(svc_mix, pl.LazyFrame)

    @pytest.mark.unit
    def test_end_to_end_without_admissions(self):

        claims = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "service_category_2": ["office_visit"],
                    "claim_end_date": [date(2024, 3, 1)],
                }
            )
        )
        eligibility = _lazy(
            pl.DataFrame(
                {
                    "person_id": ["P1"],
                    "enrollment_start_date": [date(2024, 1, 1)],
                    "enrollment_end_date": [date(2024, 12, 31)],
                }
            )
        )

        visit_util, adm_rates, bed_days, high_util, svc_mix = (
            UtilizationTransform.calculate_utilization_metrics(
                claims, eligibility, None, {"measurement_year": 2024}
            )
        )

        # Should still work with None admissions
        adm_df = adm_rates.collect()
        assert adm_df.height == 0


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)


class TestCalculateMemberYears:
    """Tests for UtilizationTransform.calculate_member_years."""

    def _call(self, eligibility: pl.LazyFrame, config: dict[str, Any]) -> pl.LazyFrame:

        return UtilizationTransform.calculate_member_years(eligibility, config)

    @pytest.mark.unit
    def test_full_year_enrollment(self):
        elig = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "enrollment_start_date": [_date(2024, 1, 1)],
                "enrollment_end_date": [_date(2024, 12, 31)],
            }
        )
        result = self._call(elig, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 1
        assert result["total_days_enrolled"][0] == 365
        assert result["member_years"][0] == pytest.approx(365 / 365.25, rel=1e-3)

    @pytest.mark.unit
    def test_partial_year_enrollment(self):
        elig = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "enrollment_start_date": [_date(2024, 7, 1)],
                "enrollment_end_date": [_date(2024, 12, 31)],
            }
        )
        result = self._call(elig, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 1
        expected_days = (_date(2024, 12, 31) - _date(2024, 7, 1)).days
        assert result["total_days_enrolled"][0] == expected_days

    @pytest.mark.unit
    def test_enrollment_spanning_years_clamped(self):
        """Enrollment that starts before and ends after the measurement year is clamped."""
        elig = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "enrollment_start_date": [_date(2023, 6, 1)],
                "enrollment_end_date": [_date(2025, 6, 1)],
            }
        )
        result = self._call(elig, {"measurement_year": 2024}).collect()
        expected_days = (_date(2024, 12, 31) - _date(2024, 1, 1)).days
        assert result["total_days_enrolled"][0] == expected_days

    @pytest.mark.unit
    def test_default_measurement_year(self):
        elig = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "enrollment_start_date": [_date(2024, 1, 1)],
                "enrollment_end_date": [_date(2024, 12, 31)],
            }
        )
        result = self._call(elig, {}).collect()
        assert result.shape[0] == 1

    @pytest.mark.unit
    def test_multiple_members(self):
        elig = pl.LazyFrame(
            {
                "person_id": ["P1", "P2"],
                "enrollment_start_date": [_date(2024, 1, 1), _date(2024, 6, 1)],
                "enrollment_end_date": [_date(2024, 12, 31), _date(2024, 12, 31)],
            }
        )
        result = self._call(elig, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 2

    @pytest.mark.unit
    def test_no_eligible_members(self):
        elig = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "enrollment_start_date": [_date(2022, 1, 1)],
                "enrollment_end_date": [_date(2022, 12, 31)],
            }
        )
        result = self._call(elig, {"measurement_year": 2024}).collect()
        assert result.shape[0] == 0


class TestCalculateVisitUtilization:
    """Tests for UtilizationTransform.calculate_visit_utilization."""

    def _call(
        self,
        claims: pl.LazyFrame,
        member_years: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:

        return UtilizationTransform.calculate_visit_utilization(claims, member_years, config)

    def _member_years(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {"person_id": ["P1", "P2"], "member_years": [1.0, 0.5]}
        )

    @pytest.mark.unit
    def test_basic_visit_utilization(self):
        claims = pl.LazyFrame(
            {
                "person_id": ["P1", "P1", "P2"],
                "claim_end_date": [_date(2024, 3, 1), _date(2024, 6, 1), _date(2024, 9, 1)],
                "service_category_2": ["ED", "ED", "office_visit"],
            }
        )
        result = self._call(claims, self._member_years(), {"measurement_year": 2024}).collect()
        assert result.shape[0] >= 2
        assert "visits_per_member_year" in result.columns

    @pytest.mark.unit
    def test_filters_to_measurement_year(self):
        claims = pl.LazyFrame(
            {
                "person_id": ["P1", "P1"],
                "claim_end_date": [_date(2024, 3, 1), _date(2023, 3, 1)],
                "service_category_2": ["ED", "ED"],
            }
        )
        result = self._call(claims, self._member_years(), {"measurement_year": 2024}).collect()
        # Only the 2024 claim should be counted
        assert result.shape[0] == 1
        assert result["visit_count"][0] == 1

    @pytest.mark.unit
    def test_default_measurement_year(self):
        claims = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "claim_end_date": [_date(2024, 3, 1)],
                "service_category_2": ["ED"],
            }
        )
        result = self._call(claims, self._member_years(), {}).collect()
        assert result.shape[0] == 1


class TestCalculateBedDays:
    """Tests for UtilizationTransform.calculate_bed_days."""

    def _call(
        self,
        admissions: pl.LazyFrame,
        member_years: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:

        return UtilizationTransform.calculate_bed_days(admissions, member_years, config)

    @pytest.mark.unit
    def test_bed_days_calculation(self):
        admissions = pl.LazyFrame(
            {"person_id": ["P1", "P1"], "length_of_stay": [5, 3]}
        )
        member_years = pl.LazyFrame(
            {"person_id": ["P1"], "member_years": [1.0]}
        )
        result = self._call(admissions, member_years, {}).collect()
        assert result.shape[0] == 1
        assert result["total_bed_days"][0] == 8
        assert result["bed_days_per_member_year"][0] == pytest.approx(8.0)
        assert result["bed_days_per_1000"][0] == pytest.approx(8000.0)

    @pytest.mark.unit
    def test_multiple_members(self):
        admissions = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "length_of_stay": [5, 10]}
        )
        member_years = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "member_years": [1.0, 0.5]}
        )
        result = self._call(admissions, member_years, {}).collect()
        assert result.shape[0] == 2


class TestCalculateAdmissionRates:
    """Tests for UtilizationTransform.calculate_admission_rates."""

    def _call(
        self,
        admissions: pl.LazyFrame,
        member_years: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:

        return UtilizationTransform.calculate_admission_rates(admissions, member_years, config)

    @pytest.mark.unit
    def test_admission_rates(self):
        admissions = pl.LazyFrame(
            {
                "person_id": ["P1", "P1", "P2"],
                "encounter_type": ["inpatient", "inpatient", "ED"],
            }
        )
        member_years = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "member_years": [1.0, 1.0]}
        )
        result = self._call(admissions, member_years, {}).collect()
        assert "admissions_per_member_year" in result.columns
        assert "admissions_per_1000" in result.columns
        # P1 inpatient: 2 admissions / 1.0 member year = 2.0
        p1_ip = result.filter(
            (pl.col("person_id") == "P1") & (pl.col("encounter_type") == "inpatient")
        )
        assert p1_ip["admission_count"][0] == 2
        assert p1_ip["admissions_per_member_year"][0] == pytest.approx(2.0)
        assert p1_ip["admissions_per_1000"][0] == pytest.approx(2000.0)


class TestIdentifyHighUtilizers:
    """Tests for UtilizationTransform.identify_high_utilizers."""

    def _call(
        self, visit_utilization: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:

        return UtilizationTransform.identify_high_utilizers(visit_utilization, config)

    @pytest.mark.unit
    def test_high_utilizer_flags(self):
        # Create enough members so quantile logic works
        visit_util = pl.LazyFrame(
            {
                "person_id": [f"P{i}" for i in range(20)],
                "visit_count": [1] * 18 + [50, 100],
                "visits_per_member_year": [1.0] * 18 + [50.0, 100.0],
            }
        )
        result = self._call(visit_util, {}).collect()
        assert "is_high_utilizer" in result.columns
        assert "utilization_tier" in result.columns
        # The top utilizers should be flagged
        high = result.filter(pl.col("is_high_utilizer"))
        assert high.shape[0] >= 1

    @pytest.mark.unit
    def test_moderate_utilizer(self):
        # Create data where someone has 12+ visits PMPY but below p90
        visit_util = pl.LazyFrame(
            {
                "person_id": [f"P{i}" for i in range(10)],
                "visit_count": [15] * 10,
                "visits_per_member_year": [15.0] * 10,
            }
        )
        result = self._call(visit_util, {}).collect()
        # All should be high since they're all at p90
        assert result.shape[0] == 10


class TestCalculateServiceMix:
    """Tests for UtilizationTransform.calculate_service_mix."""

    def _call(
        self, visit_utilization: pl.LazyFrame, config: dict[str, Any]
    ) -> pl.LazyFrame:

        return UtilizationTransform.calculate_service_mix(visit_utilization, config)

    @pytest.mark.unit
    def test_service_mix_percentages(self):
        visit_util = pl.LazyFrame(
            {
                "service_category_2": ["ED", "ED", "office_visit"],
                "visit_count": [10, 5, 5],
            }
        )
        result = self._call(visit_util, {}).collect()
        assert "percentage_of_visits" in result.columns
        assert "grand_total" in result.columns
        total_pct = result["percentage_of_visits"].sum()
        assert total_pct == pytest.approx(100.0, rel=1e-3)

    @pytest.mark.unit
    def test_sorted_descending(self):
        visit_util = pl.LazyFrame(
            {
                "service_category_2": ["A", "B", "C"],
                "visit_count": [5, 20, 10],
            }
        )
        result = self._call(visit_util, {}).collect()
        totals = result["total_visits"].to_list()
        assert totals == sorted(totals, reverse=True)


class TestCalculateUtilizationMetrics:
    """Tests for the orchestrator method UtilizationTransform.calculate_utilization_metrics."""

    def _call(self, claims, eligibility, admissions, config):

        return UtilizationTransform.calculate_utilization_metrics(
            claims, eligibility, admissions, config
        )

    def _base_eligibility(self):
        return pl.LazyFrame(
            {
                "person_id": ["P1", "P2"],
                "enrollment_start_date": [_date(2024, 1, 1), _date(2024, 1, 1)],
                "enrollment_end_date": [_date(2024, 12, 31), _date(2024, 12, 31)],
            }
        )

    def _base_claims(self):
        return pl.LazyFrame(
            {
                "person_id": ["P1", "P2"],
                "claim_end_date": [_date(2024, 3, 1), _date(2024, 6, 1)],
                "service_category_2": ["ED", "office_visit"],
            }
        )

    @pytest.mark.unit
    def test_with_admissions(self):
        admissions = pl.LazyFrame(
            {
                "person_id": ["P1"],
                "encounter_type": ["inpatient"],
                "length_of_stay": [5],
            }
        )
        visit_util, adm_rates, bed_days, high_util, svc_mix = self._call(
            self._base_claims(), self._base_eligibility(), admissions, {"measurement_year": 2024}
        )
        # All should be LazyFrames that can be collected
        assert visit_util.collect().shape[0] >= 1
        assert adm_rates.collect().shape[0] >= 1
        assert bed_days.collect().shape[0] >= 1
        assert high_util.collect().shape[0] >= 1
        assert svc_mix.collect().shape[0] >= 1

    @pytest.mark.unit
    def test_without_admissions(self):
        visit_util, adm_rates, bed_days, high_util, svc_mix = self._call(
            self._base_claims(), self._base_eligibility(), None, {"measurement_year": 2024}
        )
        # admission_rates and bed_days should be empty
        assert adm_rates.collect().shape[0] == 0
        assert bed_days.collect().shape[0] == 0
        # Others should have data
        assert visit_util.collect().shape[0] >= 1
