# © 2025 HarmonyCares — tests for acoharmony._transforms._quality_dah
"""
Unit tests for the DAH transform — verifies CMS PY2025 QMMR §3.3.2 (p15)
spec rules: adult ≥18, year-before HCC ≥ 2.0, 12-month FFS A+B lookback,
hospice carve-out, obstetric carve-out, ED + observation DIC, eligible
days = days alive in PY (no 365 cap).

Spec source:
https://www.cms.gov/files/document/py25-reach-qual-meas-meth-report.pdf
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from acoharmony._transforms._quality_dah import DaysAtHome
from acoharmony._transforms._quality_measure_base import MeasureFactory


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def claims_schema() -> dict:
    """Minimal schema covering every column the DAH transform may reference."""
    return {
        "person_id": pl.Utf8,
        "bill_type_code": pl.Utf8,
        "admission_date": pl.Date,
        "discharge_date": pl.Date,
        "claim_start_date": pl.Date,
        "claim_end_date": pl.Date,
        "claim_line_start_date": pl.Date,
        "revenue_center_code": pl.Utf8,
        "hcpcs_code": pl.Utf8,
        "diagnosis_code_1": pl.Utf8,
    }


def _empty_claims(schema: dict) -> pl.LazyFrame:
    return pl.LazyFrame({k: [] for k in schema}, schema=schema)


def _claims(rows: list[dict], schema: dict) -> pl.LazyFrame:
    """Build a claims LazyFrame, filling unspecified columns with None."""
    cols = {k: [r.get(k) for r in rows] for k in schema}
    return pl.LazyFrame(cols, schema=schema)


def _elig(rows: list[dict]) -> pl.LazyFrame:
    schema = {
        "person_id": pl.Utf8,
        "birth_date": pl.Date,
        "death_date": pl.Date,
        "enrollment_start_date": pl.Date,
        "enrollment_end_date": pl.Date,
    }
    cols = {k: [r.get(k) for r in rows] for k in schema}
    return pl.LazyFrame(cols, schema=schema)


def _hcc(rows: list[dict]) -> pl.LazyFrame:
    schema = {
        "mbi": pl.Utf8,
        "performance_year": pl.Int64,
        "model_version": pl.Utf8,
        "total_risk_score": pl.Float64,
    }
    return pl.LazyFrame(
        {k: [r.get(k) for r in rows] for k in schema}, schema=schema
    )


# ---------------------------------------------------------------------------
# Registration & metadata
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDaysAtHomeRegistration:
    def test_registered_with_factory(self):
        assert "REACH_DAH" in MeasureFactory.list_measures()
        instance = MeasureFactory.create("REACH_DAH", config={"performance_year": 2024})
        assert isinstance(instance, DaysAtHome)


@pytest.mark.unit
class TestDaysAtHomeMetadata:
    def test_metadata_cites_spec(self):
        meta = DaysAtHome().get_metadata()
        assert meta.measure_id == "REACH_DAH"
        assert "Complex" in meta.measure_name
        # Must reference §3.3.2 p15 / spec lineage
        assert "§3.3.2" in meta.numerator_description
        assert "§3.3.2" in meta.denominator_description


# ---------------------------------------------------------------------------
# Denominator — §3.3.2 p15
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDaysAtHomeDenominator:
    def test_age_18_threshold_not_65(self):
        """Spec says adult (≥18), not Medicare-65. A 30yo qualifies."""
        elig = _elig(
            [
                {
                    "person_id": "ADULT30",
                    "birth_date": date(1994, 1, 1),  # 30 yo on 2024-01-01
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                },
                {
                    "person_id": "MINOR",
                    "birth_date": date(2010, 1, 1),  # 14 yo
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                },
            ]
        )
        m = DaysAtHome(config={"performance_year": 2024})
        denom = m.calculate_denominator(pl.LazyFrame(), elig, {}).collect()
        assert denom["person_id"].to_list() == ["ADULT30"]

    def test_requires_alive_on_py_start(self):
        elig = _elig(
            [
                {
                    "person_id": "ALIVE",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                },
                {
                    "person_id": "DEAD",
                    "birth_date": date(1950, 1, 1),
                    "death_date": date(2023, 12, 31),  # died before PY
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2023, 12, 31),
                },
            ]
        )
        m = DaysAtHome(config={"performance_year": 2024})
        denom = m.calculate_denominator(pl.LazyFrame(), elig, {}).collect()
        assert denom["person_id"].to_list() == ["ALIVE"]

    def test_requires_12mo_prior_lookback(self):
        """enrollment_start must be ≤ Jan 1 of (PY-1)."""
        elig = _elig(
            [
                {
                    "person_id": "FULL_LOOKBACK",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                },
                {
                    "person_id": "SHORT_LOOKBACK",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2023, 6, 1),  # < 12 mo prior
                    "enrollment_end_date": date(2024, 12, 31),
                },
            ]
        )
        m = DaysAtHome(config={"performance_year": 2024})
        denom = m.calculate_denominator(pl.LazyFrame(), elig, {}).collect()
        assert denom["person_id"].to_list() == ["FULL_LOOKBACK"]

    def test_requires_continuous_through_py_end(self):
        elig = _elig(
            [
                {
                    "person_id": "FULL_PY",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                },
                {
                    "person_id": "DROPPED_MID_PY",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 6, 30),  # left mid-PY
                },
            ]
        )
        m = DaysAtHome(config={"performance_year": 2024})
        denom = m.calculate_denominator(pl.LazyFrame(), elig, {}).collect()
        assert denom["person_id"].to_list() == ["FULL_PY"]

    def test_died_during_py_keeps_continuous_until_dod(self):
        """Per spec: continuous A+B during full PY 'up to date of death'."""
        elig = _elig(
            [
                {
                    "person_id": "DIED_MID_PY",
                    "birth_date": date(1950, 1, 1),
                    "death_date": date(2024, 7, 1),
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 7, 1),  # = dod, OK
                }
            ]
        )
        m = DaysAtHome(config={"performance_year": 2024})
        denom = m.calculate_denominator(pl.LazyFrame(), elig, {}).collect()
        assert denom["person_id"].to_list() == ["DIED_MID_PY"]

    def test_hcc_threshold_filter_when_scores_provided(self):
        """Spec criterion 4 (§3.3.2 p15): avg HCC ≥ 2.0 in year before PY."""
        elig = _elig(
            [
                {
                    "person_id": "HIGH_HCC",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                },
                {
                    "person_id": "LOW_HCC",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                },
            ]
        )
        # The pipeline filters hcc to (py-1) before passing it; here we
        # already pre-filter, simulating that contract.
        hcc = _hcc(
            [
                {"mbi": "HIGH_HCC", "performance_year": 2023,
                 "model_version": "v28", "total_risk_score": 2.5},
                {"mbi": "HIGH_HCC", "performance_year": 2023,
                 "model_version": "v24", "total_risk_score": 2.1},
                {"mbi": "LOW_HCC", "performance_year": 2023,
                 "model_version": "v28", "total_risk_score": 1.5},
            ]
        )
        m = DaysAtHome(config={"performance_year": 2024})
        denom = m.calculate_denominator(
            pl.LazyFrame(), elig, {"hcc_scores": hcc}
        ).collect()
        assert sorted(denom["person_id"].to_list()) == ["HIGH_HCC"]

    def test_no_hcc_scores_logs_warning_keeps_all(self, caplog):
        """Without HCC scores we keep all benes meeting other criteria but log."""
        import logging
        caplog.set_level(logging.WARNING, logger="acoharmony.transforms.quality_dah")
        elig = _elig(
            [
                {
                    "person_id": "B1",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                }
            ]
        )
        m = DaysAtHome(config={"performance_year": 2024})
        denom = m.calculate_denominator(pl.LazyFrame(), elig, {}).collect()
        assert denom["person_id"].to_list() == ["B1"]
        # Warning was emitted
        assert any("HCC" in r.message for r in caplog.records)

    def test_hcc_with_mbi_to_person_crosswalk(self):
        """HCC keyed by mbi joinable via mbi_to_person value-set."""
        elig = _elig(
            [
                {
                    "person_id": "PERSON_X",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                }
            ]
        )
        hcc = _hcc(
            [
                {"mbi": "MBI_X", "performance_year": 2023,
                 "model_version": "v28", "total_risk_score": 3.0}
            ]
        )
        xwalk = pl.LazyFrame(
            {"mbi": ["MBI_X"], "person_id": ["PERSON_X"]},
            schema={"mbi": pl.Utf8, "person_id": pl.Utf8},
        )
        m = DaysAtHome(config={"performance_year": 2024})
        denom = m.calculate_denominator(
            pl.LazyFrame(),
            elig,
            {"hcc_scores": hcc, "mbi_to_person": xwalk},
        ).collect()
        assert denom["person_id"].to_list() == ["PERSON_X"]


# ---------------------------------------------------------------------------
# Numerator — §3.3.2 p15
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDaysAtHomeNumerator:
    @pytest.fixture
    def elig(self):
        return _elig(
            [
                {
                    "person_id": "B1",
                    "birth_date": date(1950, 1, 1),
                    "death_date": None,
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 12, 31),
                }
            ]
        )

    def test_no_claims_no_death_full_year_at_home(self, elig, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        result = m.calculate_numerator(
            denom, _empty_claims(claims_schema), {"eligibility": elig}
        ).collect()
        # 2024 is a leap year — eligible days = 366 (no 365 cap per spec).
        assert result["survival_days"].to_list() == [366]
        assert result["observed_dic"].to_list() == [0]
        assert result["observed_dah"].to_list() == [366]

    def test_acute_inpatient_stay_counts_as_dic(self, elig, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "111",  # acute IP
                    "admission_date": date(2024, 3, 1),
                    "discharge_date": date(2024, 3, 10),  # 10 days inclusive
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [10]
        assert result["observed_dah"].to_list() == [366 - 10]

    def test_snf_stay_counts_as_dic(self, elig, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "211",  # SNF inpatient
                    "admission_date": date(2024, 5, 1),
                    "discharge_date": date(2024, 5, 5),  # 5 days
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [5]

    def test_outpatient_does_not_count_as_dic(self, elig, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "131",  # outpatient — NOT DIC per spec
                    "admission_date": date(2024, 3, 1),
                    "discharge_date": date(2024, 3, 10),
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [0]

    def test_home_health_does_not_count_as_dic(self, elig, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "329",  # home health — NOT DIC per spec
                    "admission_date": date(2024, 3, 1),
                    "discharge_date": date(2024, 3, 10),
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [0]

    def test_hospice_overrides_dic(self, elig, claims_schema):
        """§3.3.2 p15 carve-out 1: hospice always counts as at home."""
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        # Bene has a 5-day acute IP stay AND an overlapping hospice claim.
        # Per spec: hospice wins → 0 DIC days for that overlap window.
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "111",
                    "admission_date": date(2024, 6, 1),
                    "discharge_date": date(2024, 6, 5),
                },
                {
                    "person_id": "B1",
                    "bill_type_code": "813",  # hospice
                    "claim_start_date": date(2024, 5, 25),
                    "claim_end_date": date(2024, 6, 30),
                },
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [0]

    def test_obstetric_admission_excluded_from_dic(self, elig, claims_schema):
        """§3.3.2 p15 carve-out 2: childbirth/miscarriage/termination not DIC."""
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "111",
                    "admission_date": date(2024, 4, 1),
                    "discharge_date": date(2024, 4, 3),
                    "diagnosis_code_1": "O80",  # encounter for delivery
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [0]

    def test_ed_visit_counts_as_dic_via_revenue_code(self, elig, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "131",  # outpatient
                    "revenue_center_code": "0450",  # ED revenue code
                    "claim_start_date": date(2024, 7, 4),
                    "claim_line_start_date": date(2024, 7, 4),
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [1]

    def test_observation_stay_counts_as_dic_via_revenue_code(self, elig, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "131",
                    "revenue_center_code": "0762",  # observation
                    "claim_start_date": date(2024, 8, 15),
                    "claim_line_start_date": date(2024, 8, 15),
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [1]

    def test_observation_stay_via_hcpcs(self, elig, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "131",
                    "hcpcs_code": "G0378",  # obs per hour
                    "claim_start_date": date(2024, 9, 1),
                    "claim_line_start_date": date(2024, 9, 1),
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [1]

    def test_overlapping_stays_dont_double_count_days(self, elig, claims_schema):
        """Two stays that overlap should count distinct calendar days only."""
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "111",
                    "admission_date": date(2024, 3, 1),
                    "discharge_date": date(2024, 3, 10),  # 10 days
                },
                {
                    "person_id": "B1",
                    "bill_type_code": "211",  # SNF transfer
                    "admission_date": date(2024, 3, 8),
                    "discharge_date": date(2024, 3, 12),  # overlaps days 8-10
                },
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        # Distinct days: Mar 1-12 = 12, NOT 10 + 5 = 15
        assert result["observed_dic"].to_list() == [12]

    def test_death_truncates_eligible_days(self, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        elig = _elig(
            [
                {
                    "person_id": "DIED",
                    "birth_date": date(1950, 1, 1),
                    "death_date": date(2024, 7, 1),  # 183 days into year
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 7, 1),
                }
            ]
        )
        denom = pl.LazyFrame({"person_id": ["DIED"], "denominator_flag": [True]})
        result = m.calculate_numerator(
            denom, _empty_claims(claims_schema), {"eligibility": elig}
        ).collect()
        # 2024-01-01 → 2024-07-01 inclusive = 183 days
        assert result["survival_days"].to_list() == [183]
        assert result["observed_dah"].to_list() == [183]

    def test_dic_clipped_to_period_window(self, elig, claims_schema):
        """A stay that overlaps the PY boundary only counts in-PY days."""
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = _claims(
            [
                {
                    "person_id": "B1",
                    "bill_type_code": "111",
                    "admission_date": date(2023, 12, 28),
                    "discharge_date": date(2024, 1, 4),  # 4 days inside 2024
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [4]

    def test_missing_eligibility_raises(self, claims_schema):
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["A"], "denominator_flag": [True]})
        with pytest.raises(ValueError, match=r"value_sets\['eligibility'\]"):
            m.calculate_numerator(denom, _empty_claims(claims_schema), {}).collect()

    def test_dah_never_negative(self, elig, claims_schema):
        """If DIC somehow exceeds survival_days, observed_dah floors at 0."""
        m = DaysAtHome(config={"performance_year": 2024})
        elig = _elig(
            [
                {
                    "person_id": "BRIEF",
                    "birth_date": date(1950, 1, 1),
                    "death_date": date(2024, 1, 5),
                    "enrollment_start_date": date(2022, 1, 1),
                    "enrollment_end_date": date(2024, 1, 5),
                }
            ]
        )
        denom = pl.LazyFrame({"person_id": ["BRIEF"], "denominator_flag": [True]})
        # Inpatient stay claim spans well beyond death
        claims = _claims(
            [
                {
                    "person_id": "BRIEF",
                    "bill_type_code": "111",
                    "admission_date": date(2024, 1, 1),
                    "discharge_date": date(2024, 1, 31),
                }
            ],
            claims_schema,
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        # survival = 5 days (Jan 1-5), DIC clipped to PY = 31, but DAH ≥ 0
        assert result["survival_days"].to_list() == [5]
        assert result["observed_dah"].to_list() == [0]

    def test_only_revenue_column_present(self, elig):
        """Branch coverage: rev_present=True, hcpcs_present=False."""
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        # Schema has revenue_center_code but no hcpcs_code.
        claims = pl.LazyFrame(
            {
                "person_id": ["B1"],
                "bill_type_code": ["131"],
                "admission_date": [None],
                "discharge_date": [None],
                "claim_start_date": [date(2024, 7, 4)],
                "claim_end_date": [date(2024, 7, 4)],
                "claim_line_start_date": [date(2024, 7, 4)],
                "revenue_center_code": ["0450"],  # ED
            },
            schema={
                "person_id": pl.Utf8,
                "bill_type_code": pl.Utf8,
                "admission_date": pl.Date,
                "discharge_date": pl.Date,
                "claim_start_date": pl.Date,
                "claim_end_date": pl.Date,
                "claim_line_start_date": pl.Date,
                "revenue_center_code": pl.Utf8,
            },
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [1]

    def test_only_hcpcs_column_present(self, elig):
        """Branch coverage: rev_present=False, hcpcs_present=True."""
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        claims = pl.LazyFrame(
            {
                "person_id": ["B1"],
                "bill_type_code": ["131"],
                "admission_date": [None],
                "discharge_date": [None],
                "claim_start_date": [date(2024, 9, 1)],
                "claim_end_date": [date(2024, 9, 1)],
                "claim_line_start_date": [date(2024, 9, 1)],
                "hcpcs_code": ["G0378"],  # observation
            },
            schema={
                "person_id": pl.Utf8,
                "bill_type_code": pl.Utf8,
                "admission_date": pl.Date,
                "discharge_date": pl.Date,
                "claim_start_date": pl.Date,
                "claim_end_date": pl.Date,
                "claim_line_start_date": pl.Date,
                "hcpcs_code": pl.Utf8,
            },
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [1]

    def test_no_revenue_or_hcpcs_columns_skips_ed_obs_silently(
        self, elig
    ):
        """If claim source lacks rev/hcpcs columns, ED+obs detection no-ops."""
        m = DaysAtHome(config={"performance_year": 2024})
        denom = pl.LazyFrame({"person_id": ["B1"], "denominator_flag": [True]})
        # Schema has no revenue_center_code or hcpcs_code
        claims = pl.LazyFrame(
            {
                "person_id": [],
                "bill_type_code": [],
                "admission_date": [],
                "discharge_date": [],
                "claim_start_date": [],
                "claim_end_date": [],
            },
            schema={
                "person_id": pl.Utf8,
                "bill_type_code": pl.Utf8,
                "admission_date": pl.Date,
                "discharge_date": pl.Date,
                "claim_start_date": pl.Date,
                "claim_end_date": pl.Date,
            },
        )
        result = m.calculate_numerator(denom, claims, {"eligibility": elig}).collect()
        assert result["observed_dic"].to_list() == [0]
        assert result["observed_dah"].to_list() == [366]


@pytest.mark.unit
class TestDaysAtHomeExclusions:
    def test_exclusions_always_false(self):
        """Per spec, DAH numerator exclusions are setting-based, not bene-based."""
        m = DaysAtHome()
        denom = pl.LazyFrame({"person_id": ["A", "B"], "denominator_flag": [True, True]})
        excl = m.calculate_exclusions(denom, pl.LazyFrame(), {}).collect()
        assert excl["exclusion_flag"].to_list() == [False, False]
