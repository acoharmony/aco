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
    """A CCLF1-shaped frame with a mix of inpatient/non-inpatient,
    elective/non-elective, duplicate admissions."""
    return pl.LazyFrame(
        {
            "bene_mbi_id": [
                "A", "A", "A", "A",      # A: 2 unplanned inpatient admits, 1 elective, 1 outpatient
                "B", "B",                # B: 1 unplanned inpatient
                "C",                     # C: 1 outpatient only — no qualifying admits
                "D", "D", "D",           # D: 2 unplanned admits but on same date (interim billing)
            ],
            "clm_type_cd": [
                "60", "60", "60", "40",  # A
                "60", "60",              # B
                "40",                    # C
                "60", "60", "60",        # D
            ],
            "clm_admsn_type_cd": [
                "1", "2", "3", "1",      # A: Emergency, Urgent, Elective, Emergency (non-inpatient)
                "2", "1",                # B
                "1",                     # C
                "1", "1", "1",           # D — same date, 3 lines
            ],
            "clm_from_dt": [
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
        # A: 2 unplanned (admsn_type 1, 2); B: 2 unplanned; D: 3 (same date)
        assert df.height == 7

    @pytest.mark.unit
    def test_elective_excluded(self):
        lf = pl.LazyFrame({
            "clm_type_cd": ["60"],
            "clm_admsn_type_cd": ["3"],
        })
        assert lf.filter(build_unplanned_admission_filter()).collect().height == 0

    @pytest.mark.unit
    def test_non_inpatient_excluded(self):
        lf = pl.LazyFrame({
            "clm_type_cd": ["40"],  # outpatient
            "clm_admsn_type_cd": ["1"],
        })
        assert lf.filter(build_unplanned_admission_filter()).collect().height == 0

    @pytest.mark.unit
    def test_null_admission_type_counts_as_unplanned(self):
        """FOG footnote 5 says 'is not 3'; null isn't 3, so it counts."""
        lf = pl.LazyFrame({
            "clm_type_cd": ["60"],
            "clm_admsn_type_cd": [None],
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
        d_row = result.filter(pl.col("bene_mbi_id") == "D").row(0, named=True)
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
        a_row = result.filter(pl.col("bene_mbi_id") == "A").row(0, named=True)
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
        mbis = sorted(result["bene_mbi_id"].to_list())
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
        assert "C" not in result["bene_mbi_id"].to_list()
