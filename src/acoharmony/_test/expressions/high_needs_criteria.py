# © 2025 HarmonyCares
# All rights reserved.

"""Tests for the five per-criterion expression modules plus the rollup.

Grouped into one file so the join-and-flag plumbing can be exercised
across the full set in one place rather than duplicating fixtures in
five files.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._high_needs_criterion_a import (
    build_criterion_a_met_expr,
    build_criterion_a_qualifying_claims,
    parse_icd10_codes_from_table_b61,
)
from acoharmony._expressions._high_needs_criterion_b import (
    AD_RISK_SCORE_THRESHOLD_B,
    ESRD_RISK_SCORE_THRESHOLD_B,
    build_criterion_b_met_expr,
)
from acoharmony._expressions._high_needs_criterion_c import (
    AD_RISK_SCORE_LOWER_C,
    AD_RISK_SCORE_UPPER_C_EXCLUSIVE,
    CRITERION_C_MIN_UNPLANNED_ADMITS,
    ESRD_RISK_SCORE_LOWER_C,
    ESRD_RISK_SCORE_UPPER_C_EXCLUSIVE,
    build_criterion_c_met_expr,
)
from acoharmony._expressions._high_needs_criterion_d import (
    CRITERION_D_MIN_DISTINCT_SERVICE_DATES,
    build_criterion_d_met_expr,
    build_criterion_d_qualifying_claims,
    parse_hcpcs_codes_from_table_b62,
)
from acoharmony._expressions._high_needs_criterion_e import (
    CRITERION_E_FIRST_APPLICABLE_PY,
    build_criterion_e_applicable,
    build_criterion_e_met_expr,
)
from acoharmony._expressions._high_needs_eligibility import (
    apply_sticky_alignment,
    build_criteria_any_met_expr,
    join_criteria_to_eligibility,
)
from acoharmony._expressions._high_needs_lookback import LookbackWindow


class TestCriterionA:
    @pytest.fixture
    def b61_codes(self) -> pl.LazyFrame:
        """Two B.6.1 categories, exploded."""
        return pl.LazyFrame(
            {
                "category": ["Cerebral Palsy", "Multiple Sclerosis", "x"],
                "icd10_codes": ["G80.0, G80.1, G80.9", "G35, G36.0", "x"],
            }
        )

    @pytest.mark.unit
    def test_parse_icd10_explodes_csv_and_drops_trash(self, b61_codes):
        result = parse_icd10_codes_from_table_b61(b61_codes).collect()
        codes = sorted(result["icd10_code"].to_list())
        assert codes == ["G35", "G36.0", "G80.0", "G80.1", "G80.9"]

    @pytest.mark.unit
    def test_single_inpatient_match_qualifies(self, b61_codes):
        cclf1 = pl.LazyFrame(
            {
                "bene_mbi_id": ["A"],
                "clm_type_cd": ["60"],
                "clm_from_dt": [date(2024, 6, 15)],
                "prncpl_dgns_cd": ["G35"],
                "admtg_dgns_cd": [None],
            }
        )
        codes_lf = parse_icd10_codes_from_table_b61(b61_codes)
        window = LookbackWindow(begin=date(2024, 1, 1), end=date(2024, 12, 31))

        qualifying = build_criterion_a_qualifying_claims(cclf1, codes_lf, window=window)
        met = build_criterion_a_met_expr(qualifying).collect()

        assert met.height == 1
        row = met.row(0, named=True)
        assert row["criterion_a_met"] is True
        assert row["qualifying_claim_count"] == 1

    @pytest.mark.unit
    def test_admitting_diagnosis_also_qualifies(self, b61_codes):
        """A B.6.1 code on admtg_dgns_cd should qualify just like
        prncpl_dgns_cd."""
        cclf1 = pl.LazyFrame(
            {
                "bene_mbi_id": ["A"],
                "clm_type_cd": ["60"],
                "clm_from_dt": [date(2024, 6, 15)],
                "prncpl_dgns_cd": ["Z99.0"],  # not a B.6.1 code
                "admtg_dgns_cd": ["G80.1"],   # is a B.6.1 code
            }
        )
        codes_lf = parse_icd10_codes_from_table_b61(b61_codes)
        window = LookbackWindow(begin=date(2024, 1, 1), end=date(2024, 12, 31))

        qualifying = build_criterion_a_qualifying_claims(cclf1, codes_lf, window=window)
        met = build_criterion_a_met_expr(qualifying).collect()
        assert met.height == 1
        assert met.row(0, named=True)["criterion_a_met"] is True

    @pytest.mark.unit
    def test_outpatient_claim_does_not_qualify(self, b61_codes):
        """Only claim-type 60 (inpatient) counts for criterion (a) per
        FOG line 1503."""
        cclf1 = pl.LazyFrame(
            {
                "bene_mbi_id": ["A"],
                "clm_type_cd": ["40"],
                "clm_from_dt": [date(2024, 6, 15)],
                "prncpl_dgns_cd": ["G35"],
                "admtg_dgns_cd": [None],
            }
        )
        codes_lf = parse_icd10_codes_from_table_b61(b61_codes)
        window = LookbackWindow(begin=date(2024, 1, 1), end=date(2024, 12, 31))
        qualifying = build_criterion_a_qualifying_claims(cclf1, codes_lf, window=window)
        met = build_criterion_a_met_expr(qualifying).collect()
        assert met.height == 0

    @pytest.mark.unit
    def test_out_of_window_excluded(self, b61_codes):
        """An inpatient claim before the window boundary doesn't count."""
        cclf1 = pl.LazyFrame(
            {
                "bene_mbi_id": ["A"],
                "clm_type_cd": ["60"],
                "clm_from_dt": [date(2023, 6, 15)],
                "prncpl_dgns_cd": ["G35"],
                "admtg_dgns_cd": [None],
            }
        )
        codes_lf = parse_icd10_codes_from_table_b61(b61_codes)
        window = LookbackWindow(begin=date(2024, 1, 1), end=date(2024, 12, 31))
        qualifying = build_criterion_a_qualifying_claims(cclf1, codes_lf, window=window)
        met = build_criterion_a_met_expr(qualifying).collect()
        assert met.height == 0


class TestCriterionB:
    @pytest.mark.unit
    def test_thresholds_match_pa(self):
        # PA IV.B.1(b) line 3765.
        assert AD_RISK_SCORE_THRESHOLD_B == 3.0
        assert ESRD_RISK_SCORE_THRESHOLD_B == 0.35

    @pytest.mark.unit
    def test_ad_bene_above_threshold_qualifies(self):
        scores = pl.LazyFrame(
            {
                "mbi": ["A", "A"],
                "cohort": ["AD", "AD"],
                "model_version": ["cms_hcc_v24", "cmmi_concurrent"],
                "total_risk_score": [2.8, 3.2],
            }
        )
        result = build_criterion_b_met_expr(scores).collect()
        row = result.row(0, named=True)
        assert row["max_risk_score"] == pytest.approx(3.2)
        assert row["max_risk_score_model"] == "cmmi_concurrent"
        assert row["criterion_b_met"] is True

    @pytest.mark.unit
    def test_ad_bene_below_threshold_fails(self):
        scores = pl.LazyFrame(
            {
                "mbi": ["A"],
                "cohort": ["AD"],
                "model_version": ["cms_hcc_v24"],
                "total_risk_score": [2.5],
            }
        )
        result = build_criterion_b_met_expr(scores).collect()
        assert result.row(0, named=True)["criterion_b_met"] is False

    @pytest.mark.unit
    def test_esrd_uses_esrd_threshold(self):
        """An ESRD beneficiary with score 0.4 qualifies (above 0.35)
        even though 0.4 would not qualify under the AD threshold (3.0)."""
        scores = pl.LazyFrame(
            {
                "mbi": ["E"],
                "cohort": ["ESRD"],
                "model_version": ["cms_hcc_esrd_v24"],
                "total_risk_score": [0.4],
            }
        )
        result = build_criterion_b_met_expr(scores).collect()
        assert result.row(0, named=True)["criterion_b_met"] is True
        assert result.row(0, named=True)["criterion_b_threshold"] == 0.35


class TestCriterionC:
    @pytest.mark.unit
    def test_thresholds_match_pa(self):
        # PA IV.B.1(c) line 3779 — half-open at the top (== criterion-b
        # threshold).
        assert AD_RISK_SCORE_LOWER_C == 2.0
        assert AD_RISK_SCORE_UPPER_C_EXCLUSIVE == 3.0
        assert ESRD_RISK_SCORE_LOWER_C == 0.24
        assert ESRD_RISK_SCORE_UPPER_C_EXCLUSIVE == 0.35
        assert CRITERION_C_MIN_UNPLANNED_ADMITS == 2

    @pytest.mark.unit
    def test_band_and_admits_both_required(self):
        """Score 2.5 (in AD band) AND 2 admits — met."""
        scores = pl.LazyFrame(
            {"mbi": ["A"], "cohort": ["AD"],
             "model_version": ["cms_hcc_v24"], "total_risk_score": [2.5]}
        )
        admits = pl.LazyFrame({"mbi": ["A"], "unplanned_admission_count": [2]})
        result = build_criterion_c_met_expr(scores, admits).collect()
        row = result.row(0, named=True)
        assert row["criterion_c_score_band_met"] is True
        assert row["criterion_c_admission_count_met"] is True
        assert row["criterion_c_met"] is True

    @pytest.mark.unit
    def test_band_met_but_admits_below_threshold(self):
        scores = pl.LazyFrame(
            {"mbi": ["A"], "cohort": ["AD"],
             "model_version": ["cms_hcc_v24"], "total_risk_score": [2.5]}
        )
        admits = pl.LazyFrame({"mbi": ["A"], "unplanned_admission_count": [1]})
        result = build_criterion_c_met_expr(scores, admits).collect()
        row = result.row(0, named=True)
        assert row["criterion_c_score_band_met"] is True
        assert row["criterion_c_admission_count_met"] is False
        assert row["criterion_c_met"] is False

    @pytest.mark.unit
    def test_score_at_upper_bound_is_excluded_from_c(self):
        """Score == 3.0 is the criterion-b threshold; it must NOT also
        qualify under criterion-c's band. Half-open upper bound."""
        scores = pl.LazyFrame(
            {"mbi": ["A"], "cohort": ["AD"],
             "model_version": ["cms_hcc_v24"], "total_risk_score": [3.0]}
        )
        admits = pl.LazyFrame({"mbi": ["A"], "unplanned_admission_count": [5]})
        result = build_criterion_c_met_expr(scores, admits).collect()
        assert result.row(0, named=True)["criterion_c_score_band_met"] is False

    @pytest.mark.unit
    def test_bene_with_no_admits_gets_zero(self):
        """Left-join: beneficiary not in admissions table → count = 0."""
        scores = pl.LazyFrame(
            {"mbi": ["A"], "cohort": ["AD"],
             "model_version": ["cms_hcc_v24"], "total_risk_score": [2.5]}
        )
        admits = pl.LazyFrame(
            schema={"mbi": pl.String, "unplanned_admission_count": pl.Int64}
        )
        result = build_criterion_c_met_expr(scores, admits).collect()
        row = result.row(0, named=True)
        assert row["unplanned_admission_count"] == 0
        assert row["criterion_c_met"] is False


class TestCriterionD:
    @pytest.fixture
    def b62_codes(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "category": ["Transfer Equipment", "Hospital Bed", "x"],
                "hcpcs_code": ["E0621", "E0290", "x"],
                "long_descriptor": ["Sling", "Bed", "x"],
            }
        )

    @pytest.mark.unit
    def test_two_distinct_dates_required(self):
        assert CRITERION_D_MIN_DISTINCT_SERVICE_DATES == 2

    @pytest.mark.unit
    def test_two_claims_on_different_dates_qualifies(self, b62_codes):
        cclf6 = pl.LazyFrame(
            {
                "bene_mbi_id": ["A", "A"],
                "clm_line_hcpcs_cd": ["E0621", "E0290"],
                "clm_line_from_dt": [date(2024, 3, 1), date(2024, 8, 15)],
            }
        )
        codes_lf = parse_hcpcs_codes_from_table_b62(b62_codes)
        window = LookbackWindow(begin=date(2020, 1, 1), end=date(2024, 12, 31))
        qualifying = build_criterion_d_qualifying_claims(cclf6, codes_lf, window=window)
        met = build_criterion_d_met_expr(qualifying).collect()
        assert met.row(0, named=True)["criterion_d_met"] is True

    @pytest.mark.unit
    def test_two_claims_same_date_do_not_qualify(self, b62_codes):
        """Distinct service DATES are required per FOG line 1503."""
        cclf6 = pl.LazyFrame(
            {
                "bene_mbi_id": ["A", "A"],
                "clm_line_hcpcs_cd": ["E0621", "E0290"],
                "clm_line_from_dt": [date(2024, 3, 1), date(2024, 3, 1)],
            }
        )
        codes_lf = parse_hcpcs_codes_from_table_b62(b62_codes)
        window = LookbackWindow(begin=date(2020, 1, 1), end=date(2024, 12, 31))
        qualifying = build_criterion_d_qualifying_claims(cclf6, codes_lf, window=window)
        met = build_criterion_d_met_expr(qualifying).collect()
        assert met.row(0, named=True)["criterion_d_met"] is False

    @pytest.mark.unit
    def test_single_claim_does_not_qualify(self, b62_codes):
        cclf6 = pl.LazyFrame(
            {
                "bene_mbi_id": ["A"],
                "clm_line_hcpcs_cd": ["E0621"],
                "clm_line_from_dt": [date(2024, 3, 1)],
            }
        )
        codes_lf = parse_hcpcs_codes_from_table_b62(b62_codes)
        window = LookbackWindow(begin=date(2020, 1, 1), end=date(2024, 12, 31))
        qualifying = build_criterion_d_qualifying_claims(cclf6, codes_lf, window=window)
        met = build_criterion_d_met_expr(qualifying).collect()
        assert met.row(0, named=True)["criterion_d_met"] is False


class TestCriterionE:
    @pytest.mark.unit
    def test_applicable_only_for_py2024_onward(self):
        assert CRITERION_E_FIRST_APPLICABLE_PY == 2024
        assert build_criterion_e_applicable(2022) is False
        assert build_criterion_e_applicable(2023) is False
        assert build_criterion_e_applicable(2024) is True
        assert build_criterion_e_applicable(2025) is True

    @pytest.mark.unit
    def test_pre_py2024_forces_false_regardless_of_days(self):
        """A beneficiary with 200 SNF days in PY2023 still scores
        criterion_e_met=False because the criterion does not apply."""
        days = pl.LazyFrame(
            {"bene_mbi_id": ["A"], "snf_days": [200], "home_health_days": [200]}
        )
        result = build_criterion_e_met_expr(days, performance_year=2023).collect()
        assert result.row(0, named=True)["criterion_e_met"] is False

    @pytest.mark.unit
    def test_py2024_above_snf_threshold_qualifies(self):
        days = pl.LazyFrame(
            {"bene_mbi_id": ["A"], "snf_days": [50], "home_health_days": [0]}
        )
        result = build_criterion_e_met_expr(days, performance_year=2024).collect()
        assert result.row(0, named=True)["criterion_e_met"] is True

    @pytest.mark.unit
    def test_py2024_above_hh_threshold_qualifies(self):
        days = pl.LazyFrame(
            {"bene_mbi_id": ["A"], "snf_days": [0], "home_health_days": [100]}
        )
        result = build_criterion_e_met_expr(days, performance_year=2024).collect()
        assert result.row(0, named=True)["criterion_e_met"] is True

    @pytest.mark.unit
    def test_py2024_below_both_thresholds(self):
        days = pl.LazyFrame(
            {"bene_mbi_id": ["A"], "snf_days": [30], "home_health_days": [60]}
        )
        result = build_criterion_e_met_expr(days, performance_year=2024).collect()
        assert result.row(0, named=True)["criterion_e_met"] is False


class TestEligibilityRollup:
    @pytest.mark.unit
    def test_any_criterion_meets_causes_eligible(self):
        """If at least one per-criterion flag is True, criteria_any_met
        is True even when the other four are False."""
        lf = pl.LazyFrame(
            {
                "criterion_a_met": [False, True, False],
                "criterion_b_met": [False, False, False],
                "criterion_c_met": [False, False, False],
                "criterion_d_met": [False, False, True],
                "criterion_e_met": [False, False, False],
            }
        )
        result = lf.with_columns(
            build_criteria_any_met_expr().alias("any_met")
        ).collect()
        assert result["any_met"].to_list() == [False, True, True]

    @pytest.mark.unit
    def test_null_flags_fill_to_false(self):
        lf = pl.LazyFrame(
            {
                "criterion_a_met": [None, True],
                "criterion_b_met": [None, False],
                "criterion_c_met": [None, False],
                "criterion_d_met": [None, False],
                "criterion_e_met": [None, False],
            },
            schema={
                "criterion_a_met": pl.Boolean,
                "criterion_b_met": pl.Boolean,
                "criterion_c_met": pl.Boolean,
                "criterion_d_met": pl.Boolean,
                "criterion_e_met": pl.Boolean,
            },
        )
        result = lf.with_columns(
            build_criteria_any_met_expr().alias("any_met")
        ).collect()
        assert result["any_met"].to_list() == [False, True]

    @pytest.mark.unit
    def test_sticky_alignment_keeps_eligible_after_a_yes(self):
        """MBI X meets at Apr 1 only — should remain eligible through
        Jul 1 and Oct 1 per PA Section IV.B.3."""
        lf = pl.LazyFrame(
            {
                "mbi": ["X"] * 4,
                "check_date": [date(2025, 1, 1), date(2025, 4, 1),
                               date(2025, 7, 1), date(2025, 10, 1)],
                "criteria_any_met": [False, True, False, False],
            }
        )
        result = apply_sticky_alignment(lf).collect().sort("check_date")
        assert result["eligible_as_of_check_date"].to_list() == [False, True, True, True]
        first_dates = result["first_eligible_check_date"].to_list()
        assert all(d == date(2025, 4, 1) for d in first_dates)

    @pytest.mark.unit
    def test_sticky_alignment_never_eligible(self):
        """MBI never meets → always False, first_eligible null."""
        lf = pl.LazyFrame(
            {
                "mbi": ["Z"] * 4,
                "check_date": [date(2025, 1, 1), date(2025, 4, 1),
                               date(2025, 7, 1), date(2025, 10, 1)],
                "criteria_any_met": [False] * 4,
            }
        )
        result = apply_sticky_alignment(lf).collect().sort("check_date")
        assert all(v is False for v in result["eligible_as_of_check_date"])
        assert all(v is None for v in result["first_eligible_check_date"])

    @pytest.mark.unit
    def test_join_criteria_outer_with_defaults(self):
        """Missing per-criterion rows fill to False after join."""
        frames = {
            letter: pl.LazyFrame(
                {"mbi": ["X"], "check_date": [date(2025, 1, 1)],
                 f"criterion_{letter}_met": [True]}
            )
            for letter in ("a", "b", "c", "d", "e")
        }
        # Remove one beneficiary from criterion b so the join hits a null
        frames["b"] = pl.LazyFrame(
            {"mbi": ["Y"], "check_date": [date(2025, 1, 1)],
             "criterion_b_met": [True]}
        )
        result = join_criteria_to_eligibility(frames).collect()
        # X has criterion_b_met = null → filled to False
        x_row = result.filter(pl.col("mbi") == "X").row(0, named=True)
        assert x_row["criterion_b_met"] is False
        y_row = result.filter(pl.col("mbi") == "Y").row(0, named=True)
        assert y_row["criterion_b_met"] is True
        # Y shouldn't have the other criteria met since it wasn't in those frames
        assert y_row["criterion_a_met"] is False

    @pytest.mark.unit
    def test_join_raises_if_missing_criterion(self):
        frames = {
            letter: pl.LazyFrame({"mbi": [], "check_date": [],
                                  f"criterion_{letter}_met": []})
            for letter in ("a", "b", "c", "d")  # missing 'e'
        }
        with pytest.raises(ValueError, match="Missing per-criterion frames"):
            join_criteria_to_eligibility(frames)
