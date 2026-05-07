from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path

import pytest

from acoharmony._expressions._uamcc import MCC_GROUPS, UamccExpression


class TestUamccExpression:

    @pytest.mark.unit
    def test_mcc_groups(self):
        assert 'DIABETES' in MCC_GROUPS
        assert len(MCC_GROUPS) == 9

    @pytest.mark.unit
    def test_load_uamcc_value_sets_missing(self):
        result = UamccExpression.load_uamcc_value_sets(Path('/nonexistent'))
        assert 'cohort' in result


class TestUamccIdentifyMccCohort:
    """Cover identify_mcc_cohort lines 106-166."""

    @pytest.mark.unit
    def test_identify_mcc_cohort_basic(self):
        from datetime import date

        import polars as pl

        # claim_start_date must be a Date (polars infers from date()
        # values) to match gold/medical_claim — identify_mcc_cohort
        # compares the column directly against a parsed Date literal.
        claims = pl.DataFrame({
            "claim_id": ["C1", "C2", "C3"],
            "person_id": ["P1", "P1", "P2"],
            "claim_start_date": [date(2024, 3, 15), date(2024, 6, 20), date(2024, 9, 1)],
            "admission_date": ["2024-03-15", "2024-06-20", "2024-09-01"],
            "diagnosis_code_1": ["E119", "I509", "E119"],
            "diagnosis_code_2": ["J449", None, None],
        }).lazy()
        cohort_vs = pl.DataFrame({
            "icd_10_cm": ["E119", "I509", "J449"],
            "chronic_condition_group": ["DIABETES", "HEART_FAILURE", "COPD_ASTHMA"],
        }).lazy()

        result = UamccExpression.identify_mcc_cohort(
            claims, cohort_vs, {"performance_year": 2025}
        ).collect()

        assert "person_id" in result.columns
        assert "chronic_condition_group" in result.columns
        # P1 should have multiple condition groups
        p1 = result.filter(pl.col("person_id") == "P1")
        groups = set(p1["chronic_condition_group"].to_list())
        assert "DIABETES" in groups

    @pytest.mark.unit
    def test_identify_mcc_cohort_no_dx_columns(self):
        """No diagnosis_code_ columns → empty result."""
        import polars as pl

        claims = pl.DataFrame({
            "claim_id": ["C1"],
            "person_id": ["P1"],
            "admission_date": ["2024-03-15"],
            "other_col": ["X"],
        }).lazy()
        cohort_vs = pl.DataFrame({
            "icd_10_cm": ["E119"],
            "chronic_condition_group": ["DIABETES"],
        }).lazy()

        result = UamccExpression.identify_mcc_cohort(
            claims, cohort_vs, {"performance_year": 2025}
        ).collect()

        assert result.height == 0
        assert "person_id" in result.columns


class TestUamccBuildDenominator:
    """Cover build_denominator lines 177-212."""

    @pytest.mark.unit
    def test_build_denominator_filters_by_age_and_mcc(self):
        import polars as pl
        from datetime import date

        mcc_cohort = pl.DataFrame({
            "person_id": ["P1", "P1", "P1", "P2"],
            "chronic_condition_group": ["DIABETES", "HEART_FAILURE", "COPD_ASTHMA", "DIABETES"],
        }).lazy()
        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "birth_date": [date(1955, 6, 15), date(1990, 1, 1)],
        }).lazy()

        result = UamccExpression.build_denominator(
            mcc_cohort, eligibility, {"performance_year": 2025}
        ).collect()

        # P1: age ~69 at 2025-01-01, 3 MCC groups → included
        # P2: age ~34, only 1 MCC group → excluded
        assert result.height == 1
        assert result["person_id"][0] == "P1"


class TestClassifyPlannedAdmissions:
    """Cover classify_planned_admissions lines 233-344."""

    @pytest.mark.unit
    def test_paa_rules(self):
        """PAA rules 1-3 correctly classify planned admissions."""
        import polars as pl

        claims = pl.DataFrame({
            "claim_id": ["C1", "C2", "C3"],
            "person_id": ["P1", "P1", "P1"],
            "admission_date": ["2025-03-01", "2025-06-01", "2025-09-01"],
            # discharge_date / disposition / admit_source feed the
            # spell-linking transform downstream (UAMCC MIF v4.0 §3.5).
            # All three claims here are independent stays — no
            # transfers — so dispositions are home (01) and admit
            # sources are non-transfer (1).
            "discharge_date": ["2025-03-05", "2025-06-08", "2025-09-04"],
            "discharge_disposition_code": ["01", "01", "01"],
            "admit_source_code": ["1", "1", "1"],
            "bill_type_code": ["111", "111", "111"],
            "diagnosis_code_1": ["A01", "B02", "C03"],
            # PAA1/PAA3 procedure flags now read from procedure_code_*
            # (ICD-10-PCS columns on inpatient claims), not hcpcs_code
            # — the latter is only populated on outpatient/Part B rows.
            "procedure_code_1": ["PX100", "PX200", "PX300"],
        }).lazy()

        value_sets = {
            "ccs_icd10_cm": pl.DataFrame({
                "icd_10_cm": ["A01", "B02", "C03"],
                "ccs_category": ["CCS_DX1", "CCS_DX2", "CCS_DX3"],
            }).lazy(),
            "ccs_icd10_pcs": pl.DataFrame({
                "icd_10_pcs": ["PX100", "PX200", "PX300"],
                "ccs_category": ["CCS_PX1", "CCS_PX2", "CCS_PX3"],
            }).lazy(),
            "paa1": pl.DataFrame({
                "ccs_procedure_category": ["CCS_PX1"],  # C1 = Rule 1 planned
            }).lazy(),
            "paa2": pl.DataFrame({
                "ccs_diagnosis_category": ["CCS_DX2"],  # C2 = Rule 2 planned
            }).lazy(),
            "paa3": pl.DataFrame({
                "category_or_code": ["CCS_PX3"],  # C3 potentially planned
                "code_type": ["CCS"],
            }).lazy(),
            "paa4": pl.DataFrame({
                "category_or_code": ["OTHER"],  # C3 dx NOT acute → Rule 3
                "code_type": ["CCS"],
            }).lazy(),
        }

        result = UamccExpression.classify_planned_admissions(
            claims, value_sets, {"performance_year": 2025}
        ).collect()

        assert "is_planned" in result.columns
        assert "planned_rule" in result.columns
        assert result.height == 3
        rules = dict(zip(result["claim_id"].to_list(), result["planned_rule"].to_list(), strict=False))
        assert rules["C1"] == "RULE1"
        assert rules["C2"] == "RULE2"
        assert rules["C3"] == "RULE3"


class TestApplyOutcomeExclusions:
    """Verify exclusion flags set correctly across all three categories."""

    @pytest.mark.unit
    def test_exclusion_flags(self):
        import polars as pl

        planned = pl.DataFrame({
            "claim_id": ["C1", "C2", "C3", "C4", "C5"],
            "person_id": ["P1"] * 5,
            "admission_date": [
                "2025-03-01", "2025-06-01", "2025-09-01",
                "2025-10-01", "2025-11-01",
            ],
            "diagnosis_code_1": ["A01", "B02", "C03", "U07.1", "Z99"],
            "dx_ccs_category": ["CCS100", "CCS145", "CCS2601", "CCS999", "CCS999"],
            "is_planned": [True, False, False, False, False],
            "planned_rule": ["RULE1", None, None, None, None],
        }).lazy()

        # Mirrors the real value-set schema: per-PY rows, code_type splits CCS
        # categories from ICD-10 codes (e.g. COVID U07.1).
        value_sets = {
            "exclusions": pl.DataFrame({
                "exclusion_category": [
                    "Complications of procedures or surgeries",
                    "Accidents or injuries",
                    "COVID-19 diagnosis",
                    "Complications of procedures or surgeries",  # off-year, must be ignored
                ],
                "code_type": ["CCS", "CCS", "ICD-10-CM", "CCS"],
                "category_or_code": ["CCS145", "CCS2601", "U07.1", "CCS999"],
                "performance_year": [2025, 2025, 2025, 2024],
            }).lazy(),
        }
        config = {"performance_year": 2025}

        result = UamccExpression.apply_outcome_exclusions(
            planned, value_sets, config
        ).collect()
        assert "is_excluded" in result.columns
        # C1: planned, C2: complication CCS, C3: injury CCS, C4: COVID ICD-10
        assert result.filter(pl.col("claim_id") == "C1")["is_excluded"][0] is True
        assert result.filter(pl.col("claim_id") == "C2")["is_excluded"][0] is True
        assert result.filter(pl.col("claim_id") == "C3")["is_excluded"][0] is True
        assert result.filter(pl.col("claim_id") == "C4")["is_excluded"][0] is True
        # C5: nothing matches, off-year exclusion ignored, dx_ccs_category=CCS999
        # only appears in 2024 row which the PY filter drops
        assert result.filter(pl.col("claim_id") == "C5")["is_excluded"][0] is False


class TestCalculatePersonTime:
    """Cover calculate_person_time lines 414-468."""

    @pytest.mark.unit
    def test_person_time_calculation(self):
        import polars as pl

        denominator = pl.DataFrame({
            "person_id": ["P1", "P2"],
        }).lazy()

        claims = pl.DataFrame({
            "claim_id": ["C1", "C2"],
            "person_id": ["P1", "P1"],
            "admission_date": ["2025-01-10", "2025-03-01"],
            "discharge_date": ["2025-01-15", "2025-03-05"],
            "bill_type_code": ["111", "111"],
        }).lazy()

        result = UamccExpression.calculate_person_time(
            denominator, claims, {"performance_year": 2025}
        ).collect()

        assert "person_years" in result.columns
        assert "at_risk_days" in result.columns
        assert result.height >= 1
        # P1: 5+4=9 hospital days, at_risk = 365-9=356
        p1 = result.filter(pl.col("person_id") == "P1")
        assert p1["at_risk_days"][0] == 356


class TestCalculateUamccMeasure:
    """Cover calculate_uamcc_measure lines 503-581."""

    @pytest.mark.unit
    def test_full_uamcc_pipeline(self):
        from datetime import date

        import polars as pl

        claims = pl.DataFrame({
            "claim_id": ["C1", "C2", "C3"],
            "person_id": ["P1", "P1", "P2"],
            "claim_start_date": [date(2024, 3, 15), date(2024, 6, 20), date(2024, 9, 1)],
            "admission_date": ["2024-03-15", "2024-06-20", "2024-09-01"],
            "discharge_date": ["2024-03-20", "2024-06-25", "2024-09-05"],
            "discharge_disposition_code": ["01", "01", "01"],
            "admit_source_code": ["1", "1", "1"],
            "diagnosis_code_1": ["E119", "I509", "E119"],
            "diagnosis_code_2": ["J449", None, None],
            "hcpcs_code": ["PX100", "PX200", "PX100"],
            "bill_type_code": ["111", "111", "111"],
        }).lazy()

        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "birth_date": [date(1955, 6, 15), date(1950, 1, 1)],
        }).lazy()

        value_sets = {
            "cohort": pl.DataFrame({
                "icd_10_cm": ["E119", "I509", "J449"],
                "chronic_condition_group": ["DIABETES", "HEART_FAILURE", "COPD_ASTHMA"],
            }).lazy(),
            "ccs_icd10_cm": pl.DataFrame({
                "icd_10_cm": ["E119", "I509"],
                "ccs_category": ["CCS_DX1", "CCS_DX2"],
            }).lazy(),
            "ccs_icd10_pcs": pl.DataFrame({
                "icd_10_pcs": ["PX100", "PX200"],
                "ccs_category": ["CCS_PX1", "CCS_PX2"],
            }).lazy(),
            "paa1": pl.DataFrame({"ccs_procedure_category": ["CCS_PX1"]}).lazy(),
            "paa2": pl.DataFrame({"ccs_diagnosis_category": pl.Series([], dtype=pl.Utf8)}).lazy(),
            "paa3": pl.DataFrame({"category_or_code": pl.Series([], dtype=pl.Utf8), "code_type": pl.Series([], dtype=pl.Utf8)}).lazy(),
            "paa4": pl.DataFrame({"category_or_code": pl.Series([], dtype=pl.Utf8), "code_type": pl.Series([], dtype=pl.Utf8)}).lazy(),
            "exclusions": pl.DataFrame({
                "exclusion_category": ["Complications of procedures or surgeries"],
                "code_type": ["CCS"],
                "category_or_code": ["CCS_EXCL"],
                "performance_year": [2025],
            }).lazy(),
        }

        config = {"performance_year": 2025}

        result = UamccExpression.calculate_uamcc_measure(
            claims, eligibility, value_sets, config
        )

        assert len(result) == 6
        mcc_cohort, denominator, planned, exclusions, person_time, summary = result

        # Each should be a LazyFrame
        assert isinstance(mcc_cohort, pl.LazyFrame)
        assert isinstance(summary, pl.LazyFrame)

        # Summary should have measure metadata
        s = summary.collect()
        assert "measure_id" in s.columns
        assert s["measure_id"][0] == "UAMCC"


class TestUamccLoadValueSetsBranches:
    """Cover branches 82->83/89 (file exists) and 84->85/87 (file not exists)."""

    @pytest.mark.unit
    def test_load_value_sets_files_exist(self, tmp_path):
        """Branch 84->85: file exists, loads parquet."""
        from acoharmony._expressions._uamcc import UamccExpression

        # Create a fake parquet file for one key
        pl.DataFrame({"col": ["a"]}).write_parquet(
            tmp_path / "value_sets_uamcc_value_set_cohort.parquet"
        )

        value_sets = UamccExpression.load_uamcc_value_sets(tmp_path)
        assert "cohort" in value_sets
        assert isinstance(value_sets["cohort"], pl.LazyFrame)
        # Files that don't exist should get empty lazy frames
        assert value_sets["paa1"].collect().height == 0

    @pytest.mark.unit
    def test_load_value_sets_no_files(self, tmp_path):
        """Branch 84->87: file does NOT exist, creates empty LazyFrame."""
        from acoharmony._expressions._uamcc import UamccExpression

        value_sets = UamccExpression.load_uamcc_value_sets(tmp_path)
        for key in ["cohort", "ccs_icd10_cm", "exclusions", "paa1", "paa2"]:
            assert key in value_sets
            assert value_sets[key].collect().height == 0


class TestUamccIdentifyMccCohortBranches:
    """Cover branches 115->116 (frames not empty) and 127->128 (no frames)."""

    @pytest.mark.unit
    def test_no_diagnosis_columns(self):
        """Branch 127->128: no dx columns found, returns empty schema."""
        from acoharmony._expressions._uamcc import UamccExpression

        claims = pl.DataFrame({
            "claim_id": ["C1"],
            "person_id": ["P1"],
            "admission_date": ["2024-06-01"],
        }).lazy()

        cohort_vs = pl.DataFrame({
            "icd_10_cm": ["E11"],
            "chronic_condition_group": ["Diabetes"],
        }).lazy()

        config = {"performance_year": 2025}
        result = UamccExpression.identify_mcc_cohort(claims, cohort_vs, config).collect()
        assert "person_id" in result.columns
        assert result.height == 0

    @pytest.mark.unit
    def test_with_diagnosis_columns(self):
        """Branch 115->116 and 127->138: dx columns exist, frames built."""
        from datetime import date

        from acoharmony._expressions._uamcc import UamccExpression

        claims = pl.DataFrame({
            "claim_id": ["C1", "C2"],
            "person_id": ["P1", "P2"],
            "claim_start_date": [date(2024, 6, 1), date(2024, 7, 15)],
            "admission_date": ["2024-06-01", "2024-07-15"],
            "diagnosis_code_1": ["E11", "E10"],
        }).lazy()

        cohort_vs = pl.DataFrame({
            "icd_10_cm": ["E11"],
            "chronic_condition_group": ["Diabetes"],
        }).lazy()

        config = {"performance_year": 2025}
        result = UamccExpression.identify_mcc_cohort(claims, cohort_vs, config).collect()
        assert "person_id" in result.columns


class TestLinkAdmissionSpells:
    """Collapsing contiguous inpatient stays into single spells per
    UAMCC MIF v4.0 §3.5. Pre-link, the per-claim count over-reports the
    UAMCC numerator vs CMS — counting transfers, same-day re-admits,
    and chained acute stays as separate admissions.

    Each test exercises one branch of the link rule plus the per-spell
    aggregation invariants (admission_date from first stay, discharge
    from last, planned/excluded any-True).
    """

    def _per_claim(self, rows: list[dict]) -> pl.LazyFrame:
        from datetime import date as _date

        defaults = {
            "claim_id": [r["claim_id"] for r in rows],
            "person_id": [r.get("person_id", "P1") for r in rows],
            "admission_date": [
                r["admission_date"]
                if isinstance(r["admission_date"], _date)
                else _date.fromisoformat(r["admission_date"])
                for r in rows
            ],
            "discharge_date": [
                r["discharge_date"]
                if isinstance(r["discharge_date"], _date)
                else _date.fromisoformat(r["discharge_date"])
                for r in rows
            ],
            "discharge_disposition_code": [
                r.get("discharge_disposition_code", "01") for r in rows
            ],
            "admit_source_code": [r.get("admit_source_code", "1") for r in rows],
            "is_planned": [r.get("is_planned", False) for r in rows],
            "is_excluded": [r.get("is_excluded", False) for r in rows],
        }
        return pl.DataFrame(defaults).lazy()

    @pytest.mark.unit
    def test_independent_stays_not_linked(self):
        """Two stays a month apart, home discharge in between, normal
        admit source — must remain two separate spells."""
        per_claim = self._per_claim([
            {"claim_id": "C1", "admission_date": "2025-02-01",
             "discharge_date": "2025-02-05"},
            {"claim_id": "C2", "admission_date": "2025-04-01",
             "discharge_date": "2025-04-04"},
        ])
        spells = UamccExpression.link_admission_spells(per_claim).collect()
        assert spells.height == 2
        assert spells.sort("admission_date")["spell_id"].to_list() == ["C1", "C2"]
        assert (spells["linked_claim_count"] == 1).all()

    @pytest.mark.unit
    def test_link_via_transfer_discharge_disposition(self):
        """Prior discharge code 02 (transfer to short-term hospital)
        links the next stay even when the gap is > 1 day."""
        per_claim = self._per_claim([
            {"claim_id": "C1", "admission_date": "2025-02-01",
             "discharge_date": "2025-02-05",
             "discharge_disposition_code": "02"},
            {"claim_id": "C2", "admission_date": "2025-02-08",
             "discharge_date": "2025-02-12",
             "discharge_disposition_code": "01"},
        ])
        spells = UamccExpression.link_admission_spells(per_claim).collect()
        from datetime import date as _date
        assert spells.height == 1
        row = spells.row(0, named=True)
        assert row["spell_id"] == "C1"
        assert row["admission_date"] == _date(2025, 2, 1)
        assert row["discharge_date"] == _date(2025, 2, 12)
        assert row["discharge_disposition_code"] == "01"
        assert row["linked_claim_count"] == 2

    @pytest.mark.unit
    def test_link_via_admit_source_code(self):
        """Next stay's admit_source_code = '4' (transfer-from-hospital)
        links the chain regardless of prior disposition."""
        per_claim = self._per_claim([
            {"claim_id": "C1", "admission_date": "2025-02-01",
             "discharge_date": "2025-02-05",
             "discharge_disposition_code": "01"},
            {"claim_id": "C2", "admission_date": "2025-02-10",
             "discharge_date": "2025-02-12",
             "admit_source_code": "4"},
        ])
        spells = UamccExpression.link_admission_spells(per_claim).collect()
        assert spells.height == 1
        assert spells.row(0, named=True)["spell_id"] == "C1"

    @pytest.mark.unit
    def test_link_via_overlap_or_same_day(self):
        """Same-day re-admission (admission ≤ prior discharge) links the
        chain even when both disposition and admit_source look benign."""
        per_claim = self._per_claim([
            {"claim_id": "C1", "admission_date": "2025-02-01",
             "discharge_date": "2025-02-05"},
            {"claim_id": "C2", "admission_date": "2025-02-05",
             "discharge_date": "2025-02-08"},
        ])
        spells = UamccExpression.link_admission_spells(per_claim).collect()
        assert spells.height == 1
        assert spells.row(0, named=True)["linked_claim_count"] == 2

    @pytest.mark.unit
    def test_three_stay_chain_collapses_to_one_spell(self):
        """A → B (transfer disposition) → C (admit-from-hospital) — all
        three collapse to one spell. Confirms the per-person spell index
        increments only on chain starts."""
        per_claim = self._per_claim([
            {"claim_id": "C1", "admission_date": "2025-02-01",
             "discharge_date": "2025-02-04",
             "discharge_disposition_code": "02"},
            {"claim_id": "C2", "admission_date": "2025-02-04",
             "discharge_date": "2025-02-07",
             "admit_source_code": "4"},
            {"claim_id": "C3", "admission_date": "2025-02-07",
             "discharge_date": "2025-02-10",
             "admit_source_code": "6"},
        ])
        spells = UamccExpression.link_admission_spells(per_claim).collect()
        from datetime import date as _date
        assert spells.height == 1
        row = spells.row(0, named=True)
        assert row["linked_claim_count"] == 3
        assert row["admission_date"] == _date(2025, 2, 1)
        assert row["discharge_date"] == _date(2025, 2, 10)

    @pytest.mark.unit
    def test_planned_and_excluded_propagate_any(self):
        """``is_planned`` and ``is_excluded`` aggregate via OR across the
        chain — a chain that contains a planned or excluded stay is
        treated as planned/excluded so the numerator drops it."""
        per_claim = self._per_claim([
            {"claim_id": "C1", "admission_date": "2025-02-01",
             "discharge_date": "2025-02-05",
             "discharge_disposition_code": "02",
             "is_planned": True, "is_excluded": False},
            {"claim_id": "C2", "admission_date": "2025-02-05",
             "discharge_date": "2025-02-08",
             "is_planned": False, "is_excluded": True},
        ])
        spells = UamccExpression.link_admission_spells(per_claim).collect()
        assert spells.height == 1
        row = spells.row(0, named=True)
        assert row["is_planned"] is True
        assert row["is_excluded"] is True

    @pytest.mark.unit
    def test_per_person_spells_are_isolated(self):
        """Two persons with overlapping admission windows but distinct
        person_ids must produce independent spells. Earlier versions of
        the rule risked cross-person leakage if the sort or .over()
        partitioning was wrong."""
        per_claim = self._per_claim([
            {"claim_id": "A1", "person_id": "P1",
             "admission_date": "2025-02-01", "discharge_date": "2025-02-05"},
            {"claim_id": "B1", "person_id": "P2",
             "admission_date": "2025-02-04", "discharge_date": "2025-02-08"},
        ])
        spells = UamccExpression.link_admission_spells(per_claim).collect()
        assert spells.height == 2
        by_person = dict(zip(
            spells["person_id"].to_list(), spells["linked_claim_count"].to_list()
        ))
        assert by_person == {"P1": 1, "P2": 1}

    @pytest.mark.unit
    def test_first_row_has_no_prior_so_chain_starts(self):
        """``shift(1).over(person_id)`` returns null for the first row.
        The fill_null(False) on each link clause must keep that row as a
        chain start rather than NULL-propagating into the OR."""
        per_claim = self._per_claim([
            {"claim_id": "C1", "admission_date": "2025-02-01",
             "discharge_date": "2025-02-05"},
        ])
        spells = UamccExpression.link_admission_spells(per_claim).collect()
        assert spells.height == 1
        assert spells.row(0, named=True)["spell_id"] == "C1"
