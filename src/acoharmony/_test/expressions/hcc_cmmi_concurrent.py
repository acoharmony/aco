# © 2025 HarmonyCares
# All rights reserved.

"""Tests for expressions._hcc_cmmi_concurrent module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import pytest


class TestAgeSexCell:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "age,sex,expected",
        [
            (25, "F", "F0_34"),
            (34, "F", "F0_34"),
            (35, "F", "F35_44"),
            (69, "F", "F65_69"),
            (70, "F", "F70_74"),
            (95, "F", "F95_GT"),
            (110, "F", "F95_GT"),
            (25, "M", "M0_34"),
            (70, "M", "M70_74"),
            (95, "M", "M95_GT"),
        ],
    )
    def test_boundaries(self, age, sex, expected):
        assert age_sex_cell(age, sex) == expected

    @pytest.mark.unit
    def test_lowercase_sex_normalised(self):
        assert age_sex_cell(70, "f") == "F70_74"
        assert age_sex_cell(70, "m") == "M70_74"


class TestPaymentHccCountKey:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "n,expected",
        [
            (0, None),
            (4, None),
            (5, "=5"),
            (10, "=10"),
            (14, "=14"),
            (15, ">=15"),
            (100, ">=15"),
        ],
    )
    def test_buckets(self, n, expected):
        assert payment_hcc_count_key(n) == expected


class TestApplyCmmiHierarchy:
    @pytest.mark.unit
    def test_cancer_hierarchy_drops_subordinates(self):
        # HCC 8 (Metastatic) drops 9, 10, 11, 12 per Table A-1.
        assert apply_cmmi_hierarchy(["8", "9", "10", "11", "12"]) == {"8"}

    @pytest.mark.unit
    def test_diabetes_hierarchy(self):
        # HCC 17 drops 18 and 19.
        assert apply_cmmi_hierarchy(["17", "18", "19"]) == {"17"}

    @pytest.mark.unit
    def test_unrelated_hccs_all_retained(self):
        assert apply_cmmi_hierarchy(["1", "2", "6"]) == {"1", "2", "6"}

    @pytest.mark.unit
    def test_subordinate_alone_is_retained(self):
        # If only 19 is present (no 17 or 18), 19 stays.
        assert apply_cmmi_hierarchy(["19"]) == {"19"}

    @pytest.mark.unit
    def test_unknown_hcc_is_silently_dropped(self):
        # An HCC code not in the coefficient dict (e.g. "999") is not a
        # payment HCC — the model has no coefficient for it. Silent drop
        # is the correct behaviour: the caller's dx→HCC mapping should
        # not emit codes outside the model's universe.
        assert apply_cmmi_hierarchy(["999", "1"]) == {"1"}

    @pytest.mark.unit
    def test_kidney_hierarchy_modifications(self):
        # Per the Appendix B Modified Hierarchies note, HCC 135 (Acute
        # Renal Failure) does NOT suppress HCCs 136-138 in this model
        # (unlike the V24 standard hierarchy). Verified by absence of
        # "135" as a dominant key in the hierarchy dict.
        assert apply_cmmi_hierarchy(["135", "136"]) == {"135", "136"}
        # HCC 136 does still drop 137, 138.
        assert apply_cmmi_hierarchy(["136", "137", "138"]) == {"136"}


class TestScoreCmmiConcurrent:
    @pytest.mark.unit
    def test_zero_hccs_is_just_age_sex(self):
        """A beneficiary with no HCCs scores only their age/sex cell."""
        bene = CmmiConcurrentInput(mbi="X", age=70, sex="F", hccs=())
        s = score_cmmi_concurrent(bene)
        # F70_74 = 0.1949
        assert s.total_risk_score == pytest.approx(0.1949)
        assert s.hcc_score == 0.0
        assert s.hcc_count_score == 0.0
        assert s.hcc_age_lt_65_score == 0.0
        assert s.post_transplant_score == 0.0

    @pytest.mark.unit
    def test_simple_hcc_sum(self):
        """HCCs sum as long as none are in hierarchy with each other."""
        bene = CmmiConcurrentInput(mbi="X", age=70, sex="F", hccs=("1", "2", "6"))
        s = score_cmmi_concurrent(bene)
        # F70_74 + HCC1 + HCC2 + HCC6
        expected = 0.1949 + 0.2847 + 1.1030 + 0.9210
        assert s.total_risk_score == pytest.approx(expected)

    @pytest.mark.unit
    def test_hierarchy_reduces_payment_hccs_before_summing(self):
        """With HCCs 17, 18, 19 present, only 17 contributes."""
        bene = CmmiConcurrentInput(mbi="X", age=70, sex="F", hccs=("17", "18", "19"))
        s = score_cmmi_concurrent(bene)
        expected = 0.1949 + 0.4229  # F70_74 + HCC17
        assert s.total_risk_score == pytest.approx(expected)
        assert s.payment_hccs_after_hierarchy == ("17",)

    @pytest.mark.unit
    def test_five_hccs_engages_count_coefficient(self):
        """Five or more payment HCCs adds the count-interaction term."""
        # Pick five HCCs with no hierarchy relationships between them.
        bene = CmmiConcurrentInput(
            mbi="X", age=70, sex="F",
            hccs=("1", "2", "6", "17", "21"),
        )
        s = score_cmmi_concurrent(bene)
        # age_sex + 5 HCC coefficients + count=5 coefficient (0.0433)
        hcc_sum = 0.2847 + 1.1030 + 0.9210 + 0.4229 + 1.5099
        expected = 0.1949 + hcc_sum + 0.0433
        assert s.total_risk_score == pytest.approx(expected)
        assert s.hcc_count_score == pytest.approx(0.0433)

    @pytest.mark.unit
    def test_age_lt_65_adds_hcc_interaction(self):
        """Under-65 beneficiary with HCC 110 (Cystic Fibrosis) gets the
        1.2052 age×HCC interaction."""
        bene = CmmiConcurrentInput(mbi="X", age=50, sex="F", hccs=("110",))
        s = score_cmmi_concurrent(bene)
        # F45_54 + HCC110 + HCC110×age<65
        expected = 0.1559 + 0.5460 + 1.2052
        assert s.total_risk_score == pytest.approx(expected)
        assert s.hcc_age_lt_65_score == pytest.approx(1.2052)

    @pytest.mark.unit
    def test_age_gte_65_no_hcc_interaction(self):
        """Same HCC for a 70-year-old does NOT pick up the age<65 bonus."""
        bene = CmmiConcurrentInput(mbi="X", age=70, sex="F", hccs=("110",))
        s = score_cmmi_concurrent(bene)
        expected = 0.1949 + 0.5460
        assert s.total_risk_score == pytest.approx(expected)
        assert s.hcc_age_lt_65_score == 0.0

    @pytest.mark.unit
    def test_post_transplant_indicator_added(self):
        """Post-kidney-transplant category adds the matching coefficient."""
        bene = CmmiConcurrentInput(
            mbi="X", age=70, sex="F", hccs=(),
            post_kidney_transplant_category="Age >=65 and 4-9 months post-graft",
        )
        s = score_cmmi_concurrent(bene)
        expected = 0.1949 + 2.3938  # F70_74 + post-transplant
        assert s.total_risk_score == pytest.approx(expected)
        assert s.post_transplant_score == pytest.approx(2.3938)

    @pytest.mark.unit
    def test_breakdown_columns_sum_to_total(self):
        """The per-component scores must add up to total_risk_score."""
        bene = CmmiConcurrentInput(
            mbi="X", age=50, sex="F",
            hccs=("1", "2", "6", "17", "21", "110"),
            post_kidney_transplant_category="Age <65 and 4-9 months post-graft",
        )
        s = score_cmmi_concurrent(bene)
        parts_sum = (
            s.age_sex_score
            + s.hcc_score
            + s.hcc_count_score
            + s.hcc_age_lt_65_score
            + s.post_transplant_score
        )
        assert s.total_risk_score == pytest.approx(parts_sum)
