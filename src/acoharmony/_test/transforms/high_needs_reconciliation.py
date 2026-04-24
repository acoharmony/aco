# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.high_needs_reconciliation module."""

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


def _write_ours(gold: Path, rows: list[dict]):
    pl.DataFrame(rows).write_parquet(gold / "high_needs_eligibility.parquet")


def _write_bar(silver: Path, rows: list[dict]):
    pl.DataFrame(rows).write_parquet(silver / "bar.parquet")


def _write_pbvar(silver: Path, rows: list[dict]):
    pl.DataFrame(rows).write_parquet(silver / "pbvar.parquet")


@pytest.fixture
def seeded(tmp_path: Path):
    """Universe of 3 benes we've evaluated.

    - X: our pipeline qualified them via criterion (a). Appears on BAR
      (PY2026 ALGC) with flags roughly matching ours.
    - Y: our pipeline did NOT qualify them (never eligible). Absent
      from BAR.
    - Z: our pipeline qualified them via criterion (b). Absent from
      BAR but has a PBVAR A2 tie-out — CMS said "not eligible".
    """
    root = tmp_path / "workspace"
    storage = _MockStorage(root)
    silver = storage.get_path("silver")
    gold = storage.get_path("gold")

    _write_ours(gold, [
        {
            "mbi": "X",
            "check_date": date(2026, 1, 1),
            "performance_year": 2026,
            "criterion_a_met": True,
            "criterion_b_met": False,
            "criterion_c_met": False,
            "criterion_d_met": False,
            "criterion_e_met": False,
            "criteria_any_met": True,
            "previously_eligible": False,
            "eligible_as_of_check_date": True,
            "first_eligible_check_date": date(2026, 1, 1),
            "eligible_sticky_across_pys": True,
            "first_ever_eligible_py": 2026,
            "first_ever_eligible_check_date": date(2026, 1, 1),
        },
        {
            "mbi": "Y",
            "check_date": date(2026, 10, 1),
            "performance_year": 2026,
            "criterion_a_met": False,
            "criterion_b_met": False,
            "criterion_c_met": False,
            "criterion_d_met": False,
            "criterion_e_met": False,
            "criteria_any_met": False,
            "previously_eligible": False,
            "eligible_as_of_check_date": False,
            "first_eligible_check_date": None,
            "eligible_sticky_across_pys": False,
            "first_ever_eligible_py": None,
            "first_ever_eligible_check_date": None,
        },
        {
            "mbi": "Z",
            "check_date": date(2026, 10, 1),
            "performance_year": 2026,
            "criterion_a_met": False,
            "criterion_b_met": True,
            "criterion_c_met": False,
            "criterion_d_met": False,
            "criterion_e_met": False,
            "criteria_any_met": True,
            "previously_eligible": False,
            "eligible_as_of_check_date": True,
            "first_eligible_check_date": date(2026, 10, 1),
            "eligible_sticky_across_pys": True,
            "first_ever_eligible_py": 2026,
            "first_ever_eligible_check_date": date(2026, 10, 1),
        },
    ])

    _write_bar(silver, [
        {
            "bene_mbi": "X",
            "source_filename": "P.D0259.ALGC26.RP.D261101.T1.xlsx",
            "file_date": "2026-11-01",
            "mobility_impairment_flag": True,
            "high_risk_flag": False,
            "medium_risk_unplanned_flag": False,
            "frailty_flag": False,
            "claims_based_flag": True,
        },
    ])

    _write_pbvar(silver, [
        {
            "bene_mbi": "Z",
            "sva_response_code_list": "A2, E3",
            "file_date": "2026-03-15",
        },
    ])

    return SimpleNamespace(storage_config=storage, performance_year=2026)


class TestUniverseFromOurEvaluation:
    """The reconciliation universe is our evaluated population, NOT BAR
    and NOT PBVAR. BAR and PBVAR are optional tie-out joins that fill
    nulls when the bene is absent from those feeds."""

    @pytest.mark.unit
    def test_emits_row_per_evaluated_bene(self, seeded):
        df = execute(seeded).collect()
        assert sorted(df["mbi"].to_list()) == ["X", "Y", "Z"]

    @pytest.mark.unit
    def test_bene_without_bar_keeps_null_bar_columns(self, seeded):
        """Y is in our eligibility but not in BAR — all bar_* columns
        should be null for that row."""
        df = execute(seeded).collect()
        y = df.filter(pl.col("mbi") == "Y").row(0, named=True)
        assert y["bar_file_date"] is None
        assert y["bar_mobility_impairment_flag"] is None
        assert y["bar_claims_based_flag"] is None

    @pytest.mark.unit
    def test_bene_on_bar_gets_bar_columns_populated(self, seeded):
        df = execute(seeded).collect()
        x = df.filter(pl.col("mbi") == "X").row(0, named=True)
        assert x["bar_file_date"] == "2026-11-01"
        assert x["bar_mobility_impairment_flag"] is True
        assert x["bar_claims_based_flag"] is True


class TestPbvarA2TieOut:
    @pytest.mark.unit
    def test_bene_with_a2_flagged(self, seeded):
        """Z has PBVAR A2 — pbvar_a2_present True and response codes
        preserved."""
        df = execute(seeded).collect()
        z = df.filter(pl.col("mbi") == "Z").row(0, named=True)
        assert z["pbvar_a2_present"] is True
        assert "A2" in z["pbvar_response_codes"]

    @pytest.mark.unit
    def test_bene_without_pbvar_entry_pbvar_a2_absent(self, seeded):
        """X and Y have no PBVAR A2 row. pbvar_a2_present fills to
        False; pbvar_response_codes stays null."""
        df = execute(seeded).collect()
        for mbi in ("X", "Y"):
            row = df.filter(pl.col("mbi") == mbi).row(0, named=True)
            assert row["pbvar_a2_present"] is False
            assert row["pbvar_response_codes"] is None

    @pytest.mark.unit
    def test_missing_pbvar_file_does_not_crash(self, tmp_path):
        """PBVAR silver absent — transform still runs, pbvar_a2_present
        column present but all False."""
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        gold = storage.get_path("gold")

        pl.DataFrame([
            {
                "mbi": "A",
                "check_date": date(2026, 1, 1),
                "performance_year": 2026,
                "criterion_a_met": True,
                "criterion_b_met": False,
                "criterion_c_met": False,
                "criterion_d_met": False,
                "criterion_e_met": False,
                "criteria_any_met": True,
                "previously_eligible": False,
                "eligible_as_of_check_date": True,
                "first_eligible_check_date": date(2026, 1, 1),
                "eligible_sticky_across_pys": True,
                "first_ever_eligible_py": 2026,
                "first_ever_eligible_check_date": date(2026, 1, 1),
            },
        ]).write_parquet(gold / "high_needs_eligibility.parquet")
        # Write an empty BAR so the scan succeeds but no matches.
        pl.DataFrame(
            schema={
                "bene_mbi": pl.String,
                "source_filename": pl.String,
                "file_date": pl.String,
                "mobility_impairment_flag": pl.Boolean,
                "high_risk_flag": pl.Boolean,
                "medium_risk_unplanned_flag": pl.Boolean,
                "frailty_flag": pl.Boolean,
                "claims_based_flag": pl.Boolean,
            }
        ).write_parquet(silver / "bar.parquet")
        # Deliberately do NOT write pbvar.parquet

        executor = SimpleNamespace(storage_config=storage, performance_year=2026)
        df = execute(executor).collect()
        assert df.height == 1
        row = df.row(0, named=True)
        assert row["pbvar_a2_present"] is False

    @pytest.mark.unit
    def test_bar_string_flags_normalized_to_bool(self, tmp_path):
        """BAR Y/N string flags still normalise to booleans via the
        _to_bool helper."""
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        silver = storage.get_path("silver")
        gold = storage.get_path("gold")

        pl.DataFrame([
            {
                "mbi": "Z",
                "check_date": date(2026, 1, 1),
                "performance_year": 2026,
                "criterion_a_met": True,
                "criterion_b_met": False,
                "criterion_c_met": False,
                "criterion_d_met": False,
                "criterion_e_met": False,
                "criteria_any_met": True,
                "previously_eligible": False,
                "eligible_as_of_check_date": True,
                "first_eligible_check_date": date(2026, 1, 1),
                "eligible_sticky_across_pys": True,
                "first_ever_eligible_py": 2026,
                "first_ever_eligible_check_date": date(2026, 1, 1),
            },
        ]).write_parquet(gold / "high_needs_eligibility.parquet")

        pl.DataFrame([
            {
                "bene_mbi": "Z",
                "source_filename": "P.D0259.ALGC26.RP.D261101.T1.xlsx",
                "file_date": "2026-11-01",
                "mobility_impairment_flag": "Y",
                "high_risk_flag": "N",
                "medium_risk_unplanned_flag": "N",
                "frailty_flag": "N",
                "claims_based_flag": "Y",
            },
        ]).write_parquet(silver / "bar.parquet")

        executor = SimpleNamespace(storage_config=storage, performance_year=2026)
        df = execute(executor).collect()
        row = df.row(0, named=True)
        assert row["bar_mobility_impairment_flag"] is True
        assert row["bar_high_risk_flag"] is False
        assert row["bar_claims_based_flag"] is True


class TestReconciliationOutputColumns:
    """The reconciliation output no longer contains ``disagreement_*``
    columns — that framing implied BAR-as-truth. All comparison logic
    is the consumer's job against the raw per-criterion and tie-out
    signals this transform emits."""

    @pytest.mark.unit
    def test_output_has_no_disagreement_columns(self, seeded):
        df = execute(seeded).collect()
        for col in df.columns:
            assert not col.startswith("disagreement"), (
                f"Output still contains a 'disagreement_*' column: {col!r}"
            )

    @pytest.mark.unit
    def test_reconciliation_columns_and_tie_outs_present(self, seeded):
        df = execute(seeded).collect()
        required = {
            "mbi",
            "performance_year",
            "check_date",
            "criterion_a_met",
            "criterion_b_met",
            "criterion_c_met",
            "criterion_d_met",
            "criterion_e_met",
            "criterion_a_met_ever",
            "criterion_b_met_ever",
            "criterion_c_met_ever",
            "criterion_d_met_ever",
            "criterion_e_met_ever",
            "high_needs_eligible_sticky",
            "high_needs_eligible_this_py",
            "first_eligible_py",
            "first_eligible_check_date",
            "bar_file_date",
            "bar_mobility_impairment_flag",
            "bar_high_risk_flag",
            "bar_medium_risk_unplanned_flag",
            "bar_frailty_flag",
            "bar_claims_based_flag",
            "pbvar_a2_present",
            "pbvar_response_codes",
            "pbvar_a2_file_date",
        }
        missing = required - set(df.columns)
        assert not missing, f"Missing columns: {sorted(missing)}"
