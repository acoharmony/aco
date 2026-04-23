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


@pytest.fixture
def seeded(tmp_path: Path):
    root = tmp_path / "workspace"
    storage = _MockStorage(root)
    silver = storage.get_path("silver")
    gold = storage.get_path("gold")

    # Our determination: AGREE — MBI X meets criterion (a) per our calc
    # and BAR agrees.  DISAGREE — MBI Y meets (b) per us, BAR says no.
    _write_ours(gold, [
        # MBI X — our side says yes to (a), no to the rest
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
        },
        # MBI Y — our side says yes to (b), no to the rest
        {
            "mbi": "Y",
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
        },
    ])

    # BAR: X agrees on mobility_impairment; Y disagrees on high_risk.
    # source_filename must carry the ALG{C,R}{YY} suffix so the PY-scope
    # filter in the transform picks these rows up. PY2026 → "ALGC26".
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
        {
            "bene_mbi": "Y",
            "source_filename": "P.D0259.ALGC26.RP.D261101.T1.xlsx",
            "file_date": "2026-11-01",
            "mobility_impairment_flag": False,
            "high_risk_flag": False,     # disagrees with our True
            "medium_risk_unplanned_flag": False,
            "frailty_flag": False,
            "claims_based_flag": False,  # disagrees with our True composite
        },
    ])

    return SimpleNamespace(storage_config=storage, performance_year=2026)


class TestExecute:
    @pytest.mark.unit
    def test_emits_row_per_mbi(self, seeded):
        df = execute(seeded).collect()
        assert sorted(df["mbi"].to_list()) == ["X", "Y"]

    @pytest.mark.unit
    def test_agreement_case_has_no_disagreement_flags(self, seeded):
        """X agrees with BAR on criterion (a) and the composite."""
        df = execute(seeded).collect()
        x = df.filter(pl.col("mbi") == "X").row(0, named=True)
        assert x["our_criterion_a"] is True
        assert x["bar_mobility_impairment_flag"] is True
        assert x["disagreement_a"] is False
        assert x["disagreement_composite"] is False

    @pytest.mark.unit
    def test_disagreement_flagged_on_criterion_and_composite(self, seeded):
        """Y has our_criterion_b=True but bar_high_risk_flag=False and
        bar_claims_based_flag=False — both flags should flip."""
        df = execute(seeded).collect()
        y = df.filter(pl.col("mbi") == "Y").row(0, named=True)
        assert y["our_criterion_b"] is True
        assert y["bar_high_risk_flag"] is False
        assert y["disagreement_b"] is True
        assert y["disagreement_composite"] is True

    @pytest.mark.unit
    def test_disagreement_composite_column_present(self, seeded):
        df = execute(seeded).collect()
        assert "disagreement_composite" in df.columns
        # And every per-criterion disagreement column
        assert all(
            f"disagreement_{letter}" in df.columns
            for letter in ("a", "b", "c", "d")
        )

    @pytest.mark.unit
    def test_string_bar_flags_coerced_to_bool(self, tmp_path):
        """BAR on some feeds stores flags as 'Y'/'N' strings. The
        reconciliation transform must normalize them to bool before
        comparing."""
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
            },
        ]).write_parquet(gold / "high_needs_eligibility.parquet")

        pl.DataFrame([
            {
                "bene_mbi": "Z",
                "source_filename": "P.D0259.ALGC26.RP.D261101.T1.xlsx",
                "file_date": "2026-11-01",
                "mobility_impairment_flag": "Y",     # string, not bool
                "high_risk_flag": "N",
                "medium_risk_unplanned_flag": "N",
                "frailty_flag": "N",
                "claims_based_flag": "Y",
            },
        ]).write_parquet(silver / "bar.parquet")

        executor = SimpleNamespace(storage_config=storage, performance_year=2026)
        df = execute(executor).collect()
        z = df.row(0, named=True)
        # 'Y' string in BAR is normalized to True, so no disagreement
        assert z["bar_mobility_impairment_flag"] is True
        assert z["disagreement_a"] is False
