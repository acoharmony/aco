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

    # silver/cclf1 — one unplanned inpatient claim for MBI=A with a
    # B.6.1 dx (G35) that qualifies criterion (a) at Jan 1 2026.
    # Window for PY2026 Jan 1 check is 2024-11-01 through 2025-10-31.
    pl.DataFrame(
        {
            "bene_mbi_id": ["A", "A"],
            "clm_type_cd": ["60", "20"],
            "clm_admsn_type_cd": ["1", None],  # A: emergency (unplanned)
            "clm_from_dt": [date(2025, 6, 1), date(2025, 1, 1)],  # in window
            "clm_thru_dt": [date(2025, 6, 10), date(2025, 2, 28)],
            "prncpl_dgns_cd": ["G35", "J44"],
            "admtg_dgns_cd": [None, None],
        }
    ).write_parquet(silver / "cclf1.parquet")

    # silver/cclf6 — no DME claims.
    pl.DataFrame(
        schema={
            "bene_mbi_id": pl.String,
            "clm_line_hcpcs_cd": pl.String,
            "clm_line_from_dt": pl.Date,
        }
    ).write_parquet(silver / "cclf6.parquet")

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
    pl.DataFrame(
        {
            "mbi": ["A"],
            "cohort": ["AD"],
            "model_version": ["cms_hcc_v24"],
            "total_risk_score": [3.5],
            "score_as_of_date": [date(2026, 12, 31)],
            "performance_year": [2026],
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
