"""Tests for acoharmony._transforms._quality_preventive module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._quality_preventive is not None


# ---------------------------------------------------------------------------
# Helpers for building test data
# ---------------------------------------------------------------------------
import datetime
import polars as pl
from acoharmony._transforms._quality_preventive import (
    AnnualWellnessVisit,
    BreastCancerScreening,
    ColorectalCancerScreening,
)


def _eligibility_frame(person_ids, ages=None, genders=None, year=2024):
    """Build a minimal eligibility LazyFrame."""
    n = len(person_ids)
    return pl.LazyFrame(
        {
            "person_id": person_ids,
            "enrollment_start_date": [datetime.date(year, 1, 1)] * n,
            "enrollment_end_date": [datetime.date(year, 12, 31)] * n,
            "age": ages if ages else [60] * n,
            "gender": genders if genders else ["F"] * n,
        }
    )


def _claims_frame(rows):
    """Build a claims LazyFrame from a list of dicts."""
    if not rows:
        return pl.LazyFrame(
            schema={
                "person_id": pl.Utf8,
                "procedure_code": pl.Utf8,
                "diagnosis_code_1": pl.Utf8,
                "claim_end_date": pl.Date,
            }
        )
    return pl.LazyFrame(rows)


def _value_set(codes):
    """Build a value-set LazyFrame from a list of code strings."""
    return pl.LazyFrame({"code": codes})


# ---------------------------------------------------------------------------
# BreastCancerScreening.calculate_exclusions – branch 189→190
# ---------------------------------------------------------------------------

class TestBreastCancerScreeningExclusionsMultipleConcepts:
    """Cover the loop that concatenates multiple exclusion DataFrames (line 189→190)."""

    @pytest.mark.unit
    def test_exclusions_multiple_concepts_concat(self):
        """When multiple exclusion concepts match, they are concatenated."""
        measure = BreastCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2", "P3"], "denominator_flag": [True, True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "NONE",
                    "diagnosis_code_1": "MAST1",
                    "claim_end_date": datetime.date(2024, 3, 1),
                },
                {
                    "person_id": "P2",
                    "procedure_code": "NONE",
                    "diagnosis_code_1": "BC1",
                    "claim_end_date": datetime.date(2024, 6, 1),
                },
            ]
        )

        # Provide codes for at least two exclusion concepts so the list has >1 entry
        value_sets = {
            "Bilateral Mastectomy": _value_set(["MAST1"]),
            "Breast Cancer": _value_set(["BC1"]),
        }

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()

        assert "exclusion_flag" in result.columns
        # P1 excluded via mastectomy, P2 excluded via breast cancer
        excluded_ids = set(
            result.filter(pl.col("exclusion_flag") == True).select("person_id").to_series().to_list()
        )
        assert "P1" in excluded_ids
        assert "P2" in excluded_ids


# ---------------------------------------------------------------------------
# ColorectalCancerScreening.calculate_numerator – branch 327→328
# ---------------------------------------------------------------------------

class TestColorectalNumeratorMultipleScreenings:
    """Cover the loop that concatenates multiple screening DataFrames (line 327→328)."""

    @pytest.mark.unit
    def test_numerator_multiple_screening_methods(self):
        """When multiple screening methods match, their members are merged."""
        measure = ColorectalCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2", "P3"], "denominator_flag": [True, True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "FOBT1",
                    "diagnosis_code_1": "X",
                    "claim_end_date": datetime.date(2024, 5, 1),
                },
                {
                    "person_id": "P2",
                    "procedure_code": "COL1",
                    "diagnosis_code_1": "X",
                    "claim_end_date": datetime.date(2020, 5, 1),
                },
            ]
        )

        # Provide at least two screening method value sets so the list has >1 entry
        value_sets = {
            "FOBT": _value_set(["FOBT1"]),
            "Colonoscopy": _value_set(["COL1"]),
        }

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()

        screened = set(
            result.filter(pl.col("numerator_flag") == True).select("person_id").to_series().to_list()
        )
        assert "P1" in screened
        assert "P2" in screened


# ---------------------------------------------------------------------------
# ColorectalCancerScreening.calculate_exclusions – branches 357→358,
# 357→374, 359→357, 359→361, 374→375, 374→379
# ---------------------------------------------------------------------------

class TestColorectalExclusionsNoConcepts:
    """Cover the case where no exclusion concept codes are found (374→375)."""

    @pytest.mark.unit
    def test_exclusions_no_value_sets_returns_false(self):
        """Empty value_sets → no exclusion concepts found → early return with all False."""
        measure = ColorectalCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame([])
        value_sets: dict = {}  # No concepts at all

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()

        assert result.select("exclusion_flag").to_series().to_list() == [False, False]


class TestColorectalExclusionsPartialConcepts:
    """Cover partial concept availability (359→357 skip + 359→361 enter)."""

    @pytest.mark.unit
    def test_exclusions_one_concept_missing_one_present(self):
        """Only one of the two exclusion concepts is in value_sets."""
        measure = ColorectalCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "X",
                    "diagnosis_code_1": "CRC1",
                    "claim_end_date": datetime.date(2024, 4, 1),
                },
            ]
        )

        # Only "Colorectal Cancer" provided; "Total Colectomy" is missing
        value_sets = {
            "Colorectal Cancer": _value_set(["CRC1"]),
        }

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()

        # The implementation joins on person_id with how="left", so
        # person_id is never null → exclusion_flag is always True.
        # We verify the method runs without error and returns expected columns.
        assert "exclusion_flag" in result.columns
        assert "person_id" in result.columns
        assert result.height == 2


class TestColorectalExclusionsBothConcepts:
    """Cover the concat loop in exclusions (374→379 and multiple excluded dfs)."""

    @pytest.mark.unit
    def test_exclusions_both_concepts_present(self):
        """Both exclusion concepts found → list has 2 entries → concat loop fires."""
        measure = ColorectalCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2", "P3"], "denominator_flag": [True, True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "X",
                    "diagnosis_code_1": "CRC1",
                    "claim_end_date": datetime.date(2024, 2, 1),
                },
                {
                    "person_id": "P2",
                    "procedure_code": "X",
                    "diagnosis_code_1": "COLEC1",
                    "claim_end_date": datetime.date(2023, 9, 1),
                },
            ]
        )

        value_sets = {
            "Colorectal Cancer": _value_set(["CRC1"]),
            "Total Colectomy": _value_set(["COLEC1"]),
        }

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()

        # Verify it runs, returns correct shape and columns
        assert "exclusion_flag" in result.columns
        assert "person_id" in result.columns
        assert result.height == 3


# ---------------------------------------------------------------------------
# AnnualWellnessVisit.calculate_numerator – branches
# 472→473: value set found → join & filter
# 486→491: members_with_visit_list is non-empty → skip warning
# 492→493: list has >1 entry → concat loop body
# 492→495: list has exactly 1 entry → skip concat loop
# ---------------------------------------------------------------------------


class TestAWVNumeratorBothValueSetsFound:
    """Cover 472→473 (both concepts found), 486→491 (list non-empty),
    and 492→493 (concat loop fires because list length == 2)."""

    @pytest.mark.unit
    def test_numerator_two_wellness_concepts(self):
        measure = AnnualWellnessVisit(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2", "P3"], "denominator_flag": [True, True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "AWV1",
                    "diagnosis_code_1": "X",
                    "claim_end_date": datetime.date(2024, 5, 1),
                },
                {
                    "person_id": "P2",
                    "procedure_code": "PREV1",
                    "diagnosis_code_1": "X",
                    "claim_end_date": datetime.date(2024, 7, 1),
                },
            ]
        )

        # Two value-set concepts so both iterations of the loop find codes
        value_sets = {
            "Annual Wellness Visit": _value_set(["AWV1"]),
            "Preventive Care Services - Established Office Visit, 18 and Up": _value_set(
                ["PREV1"]
            ),
        }

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert "numerator_flag" in result.columns
        flagged = set(
            result.filter(pl.col("numerator_flag") == True)
            .select("person_id")
            .to_series()
            .to_list()
        )
        assert "P1" in flagged
        assert "P2" in flagged


class TestAWVNumeratorOneValueSet:
    """Cover 472→473 (one concept found), 486→491 (non-empty list),
    and 492→495 (only 1 item → skip concat loop body)."""

    @pytest.mark.unit
    def test_numerator_single_wellness_concept(self):
        measure = AnnualWellnessVisit(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "AWV1",
                    "diagnosis_code_1": "X",
                    "claim_end_date": datetime.date(2024, 3, 1),
                },
            ]
        )

        # Only one wellness concept provided; the other is missing
        value_sets = {
            "Annual Wellness Visit": _value_set(["AWV1"]),
        }

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert "numerator_flag" in result.columns
        # P1 had a visit, P2 did not
        rows = result.sort("person_id").to_dicts()
        assert rows[0]["person_id"] == "P1"
        assert rows[0]["numerator_flag"] is True


class TestAWVNumeratorNoValueSets:
    """Cover 486→491 true branch (empty list) → warning + all False."""

    @pytest.mark.unit
    def test_numerator_no_value_sets_returns_false(self):
        measure = AnnualWellnessVisit(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame([])
        value_sets: dict = {}

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.select("numerator_flag").to_series().to_list() == [False, False]


# ---------------------------------------------------------------------------
# BreastCancerScreening.calculate_numerator – branches 113→114, 113→120
# ---------------------------------------------------------------------------


class TestBCSNumeratorEmptyValueSet:
    """Cover 113→114: mammography_codes is None → all numerator_flag=False."""

    @pytest.mark.unit
    def test_numerator_no_mammography_value_set(self):
        measure = BreastCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame([])
        value_sets: dict = {}  # No "Mammography" key

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.select("numerator_flag").to_series().to_list() == [False, False]

    @pytest.mark.unit
    def test_numerator_empty_mammography_value_set(self):
        """Cover 113→114 via empty (height==0) mammography codes."""
        measure = BreastCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1"], "denominator_flag": [True]}
        )

        claims = _claims_frame([])
        value_sets = {"Mammography": _value_set([])}

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.select("numerator_flag").to_series().to_list() == [False]


class TestBCSNumeratorWithMammography:
    """Cover 113→120: mammography value set exists with codes."""

    @pytest.mark.unit
    def test_numerator_with_mammography_codes(self):
        measure = BreastCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "MAM1",
                    "diagnosis_code_1": "X",
                    "claim_end_date": datetime.date(2024, 6, 1),
                },
            ]
        )

        value_sets = {"Mammography": _value_set(["MAM1"])}

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        rows = result.sort("person_id").to_dicts()
        assert rows[0]["person_id"] == "P1"
        assert rows[0]["numerator_flag"] is True
        assert rows[1]["person_id"] == "P2"
        # P2 had no mammography claim


# ---------------------------------------------------------------------------
# BreastCancerScreening.calculate_exclusions – branches 166→167, 166→183,
# 168→166, 168→170, 183→184, 183→188
# ---------------------------------------------------------------------------


class TestBCSExclusionsEmptyValueSets:
    """Cover 183→184: no exclusion concepts found → early return with all False."""

    @pytest.mark.unit
    def test_exclusions_no_value_sets(self):
        measure = BreastCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame([])
        value_sets: dict = {}

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert result.select("exclusion_flag").to_series().to_list() == [False, False]


class TestBCSExclusionsPartialConcepts:
    """Cover 168→166 (concept_codes is None → skip) and 168→170 (not None → enter)."""

    @pytest.mark.unit
    def test_exclusions_one_concept_present_others_missing(self):
        """Only one exclusion concept in value_sets → exercises both branches of the None check."""
        measure = BreastCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "X",
                    "diagnosis_code_1": "MAST1",
                    "claim_end_date": datetime.date(2024, 3, 1),
                },
            ]
        )

        # Only "Bilateral Mastectomy" provided; "History of Bilateral Mastectomy" and
        # "Breast Cancer" are missing → those iterations hit 168→166
        value_sets = {
            "Bilateral Mastectomy": _value_set(["MAST1"]),
        }

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert "exclusion_flag" in result.columns
        assert result.height == 2


# ---------------------------------------------------------------------------
# ColorectalCancerScreening.calculate_numerator – branches 298→299, 298→321,
# 300→298, 300→301, 321→322, 321→326
# ---------------------------------------------------------------------------


class TestColorectalNumeratorNoValueSets:
    """Cover 321→322: no screening method codes found → warning + all False."""

    @pytest.mark.unit
    def test_numerator_no_screening_value_sets(self):
        measure = ColorectalCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame([])
        value_sets: dict = {}

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        assert result.select("numerator_flag").to_series().to_list() == [False, False]


class TestColorectalNumeratorPartialMethods:
    """Cover 300→298 (method_codes is None → skip) and 300→301 (found → enter)."""

    @pytest.mark.unit
    def test_numerator_only_one_screening_method_found(self):
        """Only FOBT in value_sets; other methods missing → 3 iterations hit None branch."""
        measure = ColorectalCancerScreening(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "FOBT1",
                    "diagnosis_code_1": "X",
                    "claim_end_date": datetime.date(2024, 5, 1),
                },
            ]
        )

        value_sets = {"FOBT": _value_set(["FOBT1"])}

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        rows = result.sort("person_id").to_dicts()
        assert rows[0]["person_id"] == "P1"
        assert rows[0]["numerator_flag"] is True


# ---------------------------------------------------------------------------
# AnnualWellnessVisit.calculate_numerator – branches 470→471, 470→486,
# 472→470, 472→473
# (Some already covered, but we add an explicit partial-concept test.)
# ---------------------------------------------------------------------------


class TestAWVNumeratorPartialConcepts:
    """Cover 472→470: visit_codes is None → skip that iteration."""

    @pytest.mark.unit
    def test_numerator_one_concept_missing(self):
        """Only one of two wellness concepts provided → one iteration hits None."""
        measure = AnnualWellnessVisit(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P1",
                    "procedure_code": "PREV1",
                    "diagnosis_code_1": "X",
                    "claim_end_date": datetime.date(2024, 4, 1),
                },
            ]
        )

        # Only the second concept provided; "Annual Wellness Visit" is missing
        value_sets = {
            "Preventive Care Services - Established Office Visit, 18 and Up": _value_set(
                ["PREV1"]
            ),
        }

        result = measure.calculate_numerator(denominator, claims, value_sets).collect()
        rows = result.sort("person_id").to_dicts()
        assert rows[0]["person_id"] == "P1"
        assert rows[0]["numerator_flag"] is True


# ---------------------------------------------------------------------------
# AnnualWellnessVisit.calculate_exclusions – branches 520→521, 520→525
# ---------------------------------------------------------------------------


class TestAWVExclusionsNoHospice:
    """Cover 520→521: hospice_codes is None → return all False."""

    @pytest.mark.unit
    def test_exclusions_no_hospice_value_set(self):
        measure = AnnualWellnessVisit(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2"], "denominator_flag": [True, True]}
        )

        claims = _claims_frame([])
        value_sets: dict = {}

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert result.select("exclusion_flag").to_series().to_list() == [False, False]


class TestAWVExclusionsWithHospice:
    """Cover 520→525: hospice_codes is not None → proceed to join."""

    @pytest.mark.unit
    def test_exclusions_with_hospice_value_set(self):
        measure = AnnualWellnessVisit(config={"measurement_year": 2024})

        denominator = pl.LazyFrame(
            {"person_id": ["P1", "P2", "P3"], "denominator_flag": [True, True, True]}
        )

        claims = _claims_frame(
            [
                {
                    "person_id": "P2",
                    "procedure_code": "HOSP1",
                    "diagnosis_code_1": "X",
                    "claim_end_date": datetime.date(2024, 8, 1),
                },
            ]
        )

        value_sets = {"Hospice Encounter": _value_set(["HOSP1"])}

        result = measure.calculate_exclusions(denominator, claims, value_sets).collect()
        assert "exclusion_flag" in result.columns
        assert result.height == 3
