# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.high_needs_eligibility module."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest


class _MockStorage:
    def __init__(self, root: Path):
        self._root = root

    def get_path(self, tier):
        from acoharmony.medallion import MedallionLayer
        tier_name = tier.data_tier if isinstance(tier, MedallionLayer) else str(tier).lower()
        p = self._root / tier_name
        p.mkdir(parents=True, exist_ok=True)
        return p


@pytest.fixture
def seeded(tmp_path: Path):
    """Write the minimal set of silver + gold parquets needed for the
    eligibility transform to run."""
    root = tmp_path / "workspace"
    storage = _MockStorage(root)
    silver = storage.get_path("silver")
    gold = storage.get_path("gold")

    # gold/medical_claim — one unplanned inpatient claim for person A
    # with a B.6.1 dx (G35) that qualifies criterion (a) at Jan 1 2026.
    # Window for PY2026 Jan 1 check is 2024-11-01 through 2025-10-31.
    # bill_type 111 = hospital inpatient (qualifies a/c); 211 = SNF
    # (feeds criterion e). diagnosis_code_1..25 positions are the Tuva-
    # normalised dx columns — code lands in position 1 (principal).
    _dx_row_inpatient = {f"diagnosis_code_{i}": None for i in range(1, 26)}
    _dx_row_inpatient["diagnosis_code_1"] = "G35"
    _dx_row_snf = {f"diagnosis_code_{i}": None for i in range(1, 26)}
    _dx_row_snf["diagnosis_code_1"] = "J44"

    _mc_rows = [
        {
            "person_id": "A",
            "claim_type": "institutional",
            "bill_type_code": "111",
            "admit_type_code": "1",   # Emergency (unplanned)
            "admission_date": date(2025, 6, 1),
            "claim_start_date": date(2025, 6, 1),
            "claim_line_start_date": date(2025, 6, 1),
            "claim_line_end_date": date(2025, 6, 10),
            "hcpcs_code": None,
            **_dx_row_inpatient,
        },
        {
            "person_id": "A",
            "claim_type": "institutional",
            "bill_type_code": "211",  # SNF
            "admit_type_code": None,
            "admission_date": date(2025, 1, 1),
            "claim_start_date": date(2025, 1, 1),
            "claim_line_start_date": date(2025, 1, 1),
            "claim_line_end_date": date(2025, 2, 28),
            "hcpcs_code": None,
            **_dx_row_snf,
        },
    ]
    pl.DataFrame(_mc_rows).write_parquet(gold / "medical_claim.parquet")

    # B.6.1 silver: one row, comma-separated codes
    pl.DataFrame(
        {
            "category": ["Multiple Sclerosis"],
            "icd10_codes": ["G35"],
        }
    ).write_parquet(
        silver / "reach_appendix_tables_mobility_impairment_icd10.parquet"
    )

    # B.6.2 silver: empty (no frailty codes needed for this fixture)
    pl.DataFrame(
        schema={
            "category": pl.String,
            "hcpcs_code": pl.String,
            "long_descriptor": pl.String,
        }
    ).write_parquet(silver / "reach_appendix_tables_frailty_hcpcs.parquet")

    # gold/hcc_risk_scores — give A a high score so criterion (b) also
    # passes; this helps surface the any_met = a OR b OR ... logic.
    # Scores are keyed per (mbi, model_version, PY, check_date) since
    # FOG line 1406 pins the dx window to the check date — write one
    # row per check so (b)/(c) find a score at every evaluation point.
    _check_dates = [date(2026, m, 1) for m in (1, 4, 7, 10)]
    pl.DataFrame(
        {
            "mbi": ["A"] * 4,
            "cohort": ["AD"] * 4,
            "model_version": ["cms_hcc_v24"] * 4,
            "total_risk_score": [3.5] * 4,
            "score_as_of_date": _check_dates,
            "performance_year": [2026] * 4,
            "check_date": _check_dates,
        }
    ).write_parquet(gold / "hcc_risk_scores.parquet")

    # gold/eligibility — minimal cohort data (not used by the transform
    # directly but present for completeness).
    pl.DataFrame(
        {
            "member_id": ["A"],
            "birth_date": [date(1950, 1, 1)],
            "gender": ["F"],
            "original_reason_entitlement_code": ["0"],
            "medicare_status_code": [None],
            "dual_status_code": ["NA"],
        }
    ).write_parquet(gold / "eligibility.parquet")

    return SimpleNamespace(storage_config=storage, performance_year=2026)


class TestExecute:
    @pytest.mark.unit
    def test_produces_one_row_per_check_date(self, seeded):
        """PY2026 has four check dates (Jan/Apr/Jul/Oct 1). One MBI →
        four rows."""
        df = execute(seeded).collect()
        a_rows = df.filter(pl.col("mbi") == "A")
        assert a_rows.height == 4
        dates = sorted(a_rows["check_date"].to_list())
        assert dates == [
            date(2026, 1, 1),
            date(2026, 4, 1),
            date(2026, 7, 1),
            date(2026, 10, 1),
        ]

    @pytest.mark.unit
    def test_criteria_any_met_true_when_any_criterion_met(self, seeded):
        """A has both criterion (a) and criterion (b) met at every
        check date — any_met should be True throughout."""
        df = execute(seeded).collect()
        a_rows = df.filter(pl.col("mbi") == "A")
        assert all(a_rows["criteria_any_met"].to_list())

    @pytest.mark.unit
    def test_sticky_alignment_flag_present(self, seeded):
        df = execute(seeded).collect()
        assert "eligible_as_of_check_date" in df.columns
        assert "first_eligible_check_date" in df.columns
        assert "previously_eligible" in df.columns

    @pytest.mark.unit
    def test_performance_year_stamped_on_rows(self, seeded):
        df = execute(seeded).collect()
        assert df["performance_year"].unique().to_list() == [2026]

    @pytest.mark.unit
    def test_criterion_e_false_for_py2023(self, tmp_path: Path):
        """PY2022/2023: build_criterion_e_applicable returns False so
        _criterion_e_for_check short-circuits at line 171 returning an
        empty frame, which coalesces to criterion_e_met=False everywhere."""
        root = tmp_path / "workspace_2023"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        gold = storage.get_path("gold")

        # PY2023 check dates are Jan/Apr/Jul/Oct 2023.
        # Window C for Jan 1 2023 is 2021-11-01..2022-10-31.
        _dx_row = {f"diagnosis_code_{i}": None for i in range(1, 26)}
        _dx_row["diagnosis_code_1"] = "G35"

        pl.DataFrame(
            [
                {
                    "person_id": "B",
                    "claim_type": "institutional",
                    "bill_type_code": "111",
                    "admit_type_code": "1",
                    "admission_date": date(2022, 6, 1),
                    "claim_start_date": date(2022, 6, 1),
                    "claim_line_start_date": date(2022, 6, 1),
                    "claim_line_end_date": date(2022, 6, 10),
                    "hcpcs_code": None,
                    **_dx_row,
                }
            ]
        ).write_parquet(gold / "medical_claim.parquet")

        pl.DataFrame(
            {"category": ["Multiple Sclerosis"], "icd10_codes": ["G35"]}
        ).write_parquet(
            silver / "reach_appendix_tables_mobility_impairment_icd10.parquet"
        )
        pl.DataFrame(
            schema={"category": pl.String, "hcpcs_code": pl.String, "long_descriptor": pl.String}
        ).write_parquet(silver / "reach_appendix_tables_frailty_hcpcs.parquet")

        _check_dates_2023 = [date(2023, m, 1) for m in (1, 4, 7, 10)]
        pl.DataFrame(
            {
                "mbi": ["B"] * 4,
                "cohort": ["AD"] * 4,
                "model_version": ["cms_hcc_v24"] * 4,
                "total_risk_score": [3.5] * 4,
                "score_as_of_date": _check_dates_2023,
                "performance_year": [2023] * 4,
                "check_date": _check_dates_2023,
            }
        ).write_parquet(gold / "hcc_risk_scores.parquet")

        pl.DataFrame(
            {
                "member_id": ["B"],
                "birth_date": [date(1950, 1, 1)],
                "gender": ["F"],
                "original_reason_entitlement_code": ["0"],
                "medicare_status_code": [None],
                "dual_status_code": ["NA"],
            }
        ).write_parquet(gold / "eligibility.parquet")

        executor = SimpleNamespace(storage_config=storage, performance_year=2023)
        df = execute(executor).collect()
        b_rows = df.filter(pl.col("mbi") == "B")
        assert b_rows.height == 4
        # criterion_e is not applicable for PY2023 — must be False for all rows.
        assert b_rows["criterion_e_met"].to_list() == [False, False, False, False]
