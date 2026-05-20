# © 2025 HarmonyCares
# All rights reserved.

"""Tests for expressions._hcc_snf_home_health_days module."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import date

import polars as pl
import pytest


@pytest.fixture
def sample_claims() -> pl.LazyFrame:
    """``gold/medical_claim``-shaped frame with SNF, HH, and other
    facility types.

    Tuva schema: ``bill_type_code`` 3-digit UB code; first two digits
    encode facility — ``21`` SNF, ``32`` Home Health, ``81`` hospice.
    """
    return pl.LazyFrame(
        {
            "person_id": [
                "A", "A",           # A: 30 days SNF + 60 days HH
                "B",                # B: 50 days SNF (exceeds 45-day threshold)
                "C", "C",           # C: duplicate SNF claims (adjustment)
                "D",                # D: 95 days HH (exceeds 90)
                "E",                # E: hospice — doesn't count
            ],
            "claim_type": [
                "institutional", "institutional",
                "institutional",
                "institutional", "institutional",
                "institutional",
                "institutional",
            ],
            "bill_type_code": [
                "211", "322",       # A: SNF, HH
                "211",              # B: SNF
                "211", "211",       # C: SNF dupes
                "322",              # D: HH
                "811",              # E hospice
            ],
            "claim_line_start_date": [
                date(2024, 1, 1), date(2024, 3, 1),
                date(2024, 5, 1),
                date(2024, 2, 1), date(2024, 2, 1),
                date(2024, 6, 1),
                date(2024, 7, 1),
            ],
            "claim_line_end_date": [
                date(2024, 1, 30), date(2024, 4, 29),
                date(2024, 6, 19),
                date(2024, 2, 28), date(2024, 2, 28),
                date(2024, 9, 3),
                date(2024, 7, 31),
            ],
        }
    )


class TestBuildSnfHhDaysInWindow:
    @pytest.mark.unit
    def test_sums_days_per_beneficiary_per_setting(self, sample_claims):
        result = build_snf_hh_days_in_window(
            sample_claims,
            window_begin=date(2024, 1, 1),
            window_end=date(2024, 12, 31),
        ).collect()
        rows = {row["person_id"]: row for row in result.to_dicts()}

        # A: SNF Jan 1-30 = 30 days; HH Mar 1 - Apr 29 = 60 days
        assert rows["A"]["snf_days"] == 30
        assert rows["A"]["home_health_days"] == 60

        # B: SNF May 1 - Jun 19 = 50 days
        assert rows["B"]["snf_days"] == 50
        assert rows["B"]["home_health_days"] == 0

        # C: duplicate SNF 2/1 - 2/28 = 28 days, deduped
        assert rows["C"]["snf_days"] == 28

        # D: HH Jun 1 - Sep 3 = 95 days
        assert rows["D"]["home_health_days"] == 95

        # E: hospice claim — no SNF/HH output row at all
        assert "E" not in rows

    @pytest.mark.unit
    def test_window_clips_spans(self, sample_claims):
        """A window ending mid-claim should count only the days INSIDE
        the window."""
        result = build_snf_hh_days_in_window(
            sample_claims,
            window_begin=date(2024, 1, 1),
            window_end=date(2024, 6, 15),
        ).collect()
        # D's HH spans Jun 1 - Sep 3, window ends Jun 15 → 15 days only
        d_row = [r for r in result.to_dicts() if r["person_id"] == "D"][0]
        assert d_row["home_health_days"] == 15


class TestBuildCriterionEMetExpr:
    @pytest.mark.unit
    @pytest.mark.parametrize("snf,hh,expected", [
        (0, 0, False),
        (44, 89, False),          # just below both thresholds
        (45, 89, True),           # SNF at threshold
        (44, 90, True),           # HH at threshold
        (44, 200, True),          # HH way above
        (100, 0, True),
        (45, 90, True),           # both meet
    ])
    def test_threshold_checks(self, snf, hh, expected):
        lf = pl.LazyFrame({"snf_days": [snf], "home_health_days": [hh]})
        result = lf.with_columns(
            build_criterion_e_met_expr().alias("met")
        ).collect()
        assert result["met"][0] == expected


class TestThresholdConstants:
    @pytest.mark.unit
    def test_snf_threshold_is_45(self):
        assert SNF_DAYS_THRESHOLD == 45

    @pytest.mark.unit
    def test_hh_threshold_is_90(self):
        assert HOME_HEALTH_DAYS_THRESHOLD == 90
