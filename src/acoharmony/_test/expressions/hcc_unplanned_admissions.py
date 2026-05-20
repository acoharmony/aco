# © 2025 HarmonyCares
# All rights reserved.

"""Tests for expressions._hcc_unplanned_admissions module."""

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
    """A ``gold/medical_claim``-shaped frame with a mix of
    institutional-inpatient / outpatient / elective / non-elective
    rows, plus duplicate admissions to exercise dedupe.

    Tuva schema:
        - ``person_id`` (crosswalked MBI)
        - ``claim_type``: ``"institutional"`` | ``"DME"`` | ``"professional"``
        - ``bill_type_code``: 3-digit UB (first 2 = facility). 11x =
          hospital inpatient, 13x = hospital outpatient.
        - ``admit_type_code``: CMS admission-type (1 Emergency, 2 Urgent,
          3 Elective, etc.).
        - ``admission_date``: for institutional inpatient.
    """
    return pl.LazyFrame(
        {
            "person_id": [
                "A", "A", "A", "A",      # A: 2 unplanned admits, 1 elective, 1 outpatient
                "B", "B",                # B: 2 unplanned admits
                "C",                     # C: 1 outpatient only — no qualifying admits
                "D", "D", "D",           # D: 3 lines same date (interim billing)
            ],
            "claim_type": [
                "institutional", "institutional", "institutional", "institutional",  # A
                "institutional", "institutional",                                    # B
                "institutional",                                                     # C
                "institutional", "institutional", "institutional",                   # D
            ],
            "bill_type_code": [
                "111", "111", "111", "131",  # A: 3 inpatient + 1 outpatient
                "111", "111",                 # B: 2 inpatient
                "131",                        # C: outpatient
                "111", "111", "111",          # D: 3 inpatient, same date
            ],
            "admit_type_code": [
                "1", "2", "3", "1",      # A: Emergency, Urgent, Elective, Emergency (non-inpatient)
                "2", "1",                # B
                "1",                     # C
                "1", "1", "1",           # D — same date, 3 lines
            ],
            "admission_date": [
                date(2024, 3, 15), date(2024, 7, 22), date(2024, 8, 1), date(2024, 9, 1),
                date(2024, 5, 1), date(2024, 11, 10),
                date(2024, 6, 1),
                date(2024, 4, 10), date(2024, 4, 10), date(2024, 4, 10),
            ],
        }
    )


class TestBuildUnplannedAdmissionFilter:
    @pytest.mark.unit
    def test_inpatient_nonelective_passes(self, sample_claims):
        df = sample_claims.filter(build_unplanned_admission_filter()).collect()
        # A: 2 unplanned (admit_type 1, 2); B: 2 unplanned; D: 3 (same date).
        # A's elective and outpatient rows are excluded, C's outpatient is excluded.
        assert df.height == 7

    @pytest.mark.unit
    def test_elective_excluded(self):
        lf = pl.LazyFrame({
            "claim_type": ["institutional"],
            "bill_type_code": ["111"],
            "admit_type_code": ["3"],
        })
        assert lf.filter(build_unplanned_admission_filter()).collect().height == 0

    @pytest.mark.unit
    def test_non_inpatient_excluded(self):
        lf = pl.LazyFrame({
            "claim_type": ["institutional"],
            "bill_type_code": ["131"],  # outpatient
            "admit_type_code": ["1"],
        })
        assert lf.filter(build_unplanned_admission_filter()).collect().height == 0

    @pytest.mark.unit
    def test_null_admission_type_counts_as_unplanned(self):
        """FOG footnote 5 says 'is not 3'; null isn't 3, so it counts."""
        lf = pl.LazyFrame({
            "claim_type": ["institutional"],
            "bill_type_code": ["111"],
            "admit_type_code": [None],
        })
        assert lf.filter(build_unplanned_admission_filter()).collect().height == 1


class TestCountUnplannedAdmissionsInWindow:
    @pytest.mark.unit
    def test_dedupe_per_admission_date(self, sample_claims):
        """D's three adjusted claims on 2024-04-10 should count ONCE."""
        result = count_unplanned_admissions_in_window(
            sample_claims,
            window_begin=date(2024, 1, 1),
            window_end=date(2024, 12, 31),
        ).collect()
        d_row = result.filter(pl.col("person_id") == "D").row(0, named=True)
        assert d_row["unplanned_admission_count"] == 1

    @pytest.mark.unit
    def test_multiple_distinct_dates_sum(self, sample_claims):
        """A has admits on 3/15 and 7/22 (also 8/1 elective, 9/1
        outpatient). Count = 2."""
        result = count_unplanned_admissions_in_window(
            sample_claims,
            window_begin=date(2024, 1, 1),
            window_end=date(2024, 12, 31),
        ).collect()
        a_row = result.filter(pl.col("person_id") == "A").row(0, named=True)
        assert a_row["unplanned_admission_count"] == 2

    @pytest.mark.unit
    def test_window_clips_out_of_range_admits(self, sample_claims):
        """A narrow April–June window keeps only the Apr 10 admit for D
        and the May 1 admit for B."""
        result = count_unplanned_admissions_in_window(
            sample_claims,
            window_begin=date(2024, 4, 1),
            window_end=date(2024, 6, 30),
        ).collect()
        mbis = sorted(result["person_id"].to_list())
        assert "A" not in mbis     # A's admits are 3/15 and 7/22 — both outside
        assert "B" in mbis
        assert "D" in mbis

    @pytest.mark.unit
    def test_beneficiary_with_no_unplanned_admits_absent(self, sample_claims):
        """C has only outpatient claims — no row in the output."""
        result = count_unplanned_admissions_in_window(
            sample_claims,
            window_begin=date(2024, 1, 1),
            window_end=date(2024, 12, 31),
        ).collect()
        assert "C" not in result["person_id"].to_list()
