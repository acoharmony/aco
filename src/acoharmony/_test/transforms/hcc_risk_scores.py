# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.hcc_risk_scores module."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import logging
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest


class _MockStorage:
    """Minimal storage_config replacement: get_path(tier) returns a fixed dir.

    ``tier`` may be a MedallionLayer enum or a plain string. Directory
    names map to the same strings MedallionLayer emits for
    ``data_tier`` (``bronze``, ``silver``, ``gold``).
    """

    def __init__(self, root: Path):
        self._root = root

    def get_path(self, tier):
        from acoharmony.medallion import MedallionLayer
        tier_name = tier.data_tier if isinstance(tier, MedallionLayer) else str(tier).lower()
        p = self._root / tier_name
        p.mkdir(parents=True, exist_ok=True)
        return p


@pytest.fixture
def seeded_executor(tmp_path: Path):
    """Populate minimal eligibility + diagnosis fixtures and return a
    SimpleNamespace mock executor pointed at them."""
    root = tmp_path / "workspace"
    storage = _MockStorage(root)
    gold = storage.get_path("gold")
    silver = storage.get_path("silver")

    # Gold eligibility — 3 benes: one AD low-risk, one AD high-risk, one ESRD.
    pl.DataFrame(
        {
            "member_id": ["LOWRISK_AD", "HIGHRISK_AD", "ESRD_BENE"],
            "birth_date": [date(1955, 1, 1), date(1950, 6, 15), date(1948, 3, 1)],
            "gender": ["F", "M", "F"],
            "original_reason_entitlement_code": ["0", "0", "2"],
            "medicare_status_code": [None, None, None],
            "dual_status_code": ["NA", "NA", "NA"],
        }
    ).write_parquet(gold / "eligibility.parquet")

    # Silver int_diagnosis_deduped — HIGHRISK_AD has diabetes + MS + HF + COPD;
    # LOWRISK_AD has a minor dx; ESRD_BENE has dialysis. clm_from_dt is
    # present for per-PY windowing — use a date in the prior calendar
    # year of the test PY (2026) so CMS-HCC Prospective picks these
    # dx's up, and also in the same year so CMMI-HCC Concurrent picks
    # them up. We use two dates per bene, one in each window.
    pl.DataFrame(
        {
            "current_bene_mbi_id": [
                "LOWRISK_AD", "LOWRISK_AD",
                "HIGHRISK_AD", "HIGHRISK_AD", "HIGHRISK_AD", "HIGHRISK_AD",
                "HIGHRISK_AD", "HIGHRISK_AD", "HIGHRISK_AD", "HIGHRISK_AD",
                "ESRD_BENE", "ESRD_BENE",
            ],
            "clm_dgns_cd": [
                "Z00.00", "Z00.00",
                "E1165", "G35", "I509", "J449",
                "E1165", "G35", "I509", "J449",
                "N185", "N185",
            ],
            "clm_from_dt": [
                # LOWRISK — prior year + same year
                date(2025, 6, 1), date(2026, 6, 1),
                # HIGHRISK — prior year (CMS-HCC) four codes
                date(2025, 2, 1), date(2025, 5, 1), date(2025, 8, 1), date(2025, 11, 1),
                # HIGHRISK — same year (CMMI) four codes
                date(2026, 2, 1), date(2026, 5, 1), date(2026, 8, 1), date(2026, 11, 1),
                # ESRD — prior year + same year
                date(2025, 6, 1), date(2026, 6, 1),
            ],
        }
    ).write_parquet(silver / "int_diagnosis_deduped.parquet")

    return SimpleNamespace(
        storage_config=storage,
        logger=logging.getLogger("test_hcc_risk_scores"),
        performance_year=2026,
    )


class TestExecute:
    @pytest.mark.unit
    def test_produces_one_row_per_mbi_per_model_per_check(self, seeded_executor):
        """Per FOG line 1406 the dx window tracks the check date, so
        every (mbi, model) pair now emits four rows per PY — one per
        check date. PY2026 A&D gets V24, V28, and CMMI across 4 checks
        (12 rows); PY2026 ESRD gets ESRD_V24 across 4 checks (4 rows)."""
        lf = execute(seeded_executor)
        df = lf.collect()
        per_mbi = df.group_by("mbi").agg(pl.len().alias("n"))
        per_mbi_dict = {row["mbi"]: row["n"] for row in per_mbi.to_dicts()}
        assert per_mbi_dict["LOWRISK_AD"] == 12   # 3 models * 4 checks
        assert per_mbi_dict["HIGHRISK_AD"] == 12  # 3 models * 4 checks
        assert per_mbi_dict["ESRD_BENE"] == 4     # 1 model * 4 checks

    @pytest.mark.unit
    def test_check_date_column_populated(self, seeded_executor):
        """Every score row carries the check_date it was computed for —
        the eligibility transform joins on (mbi, check_date), so a null
        check_date would silently drop the row on join."""
        from datetime import date
        df = execute(seeded_executor).collect()
        assert df["check_date"].null_count() == 0
        expected = {
            date(2026, 1, 1), date(2026, 4, 1),
            date(2026, 7, 1), date(2026, 10, 1),
        }
        assert set(df["check_date"].unique().to_list()) == expected

    @pytest.mark.unit
    def test_highrisk_ad_scores_exceed_lowrisk(self, seeded_executor):
        """A beneficiary with four major HCCs (diabetes, MS, HF, COPD)
        should score strictly higher than a beneficiary with a benign
        health-maintenance code. Exact thresholds depend on model
        version — locking in only the ordering."""
        df = execute(seeded_executor).collect()
        hr_max = df.filter(pl.col("mbi") == "HIGHRISK_AD")["total_risk_score"].max()
        lr_max = df.filter(pl.col("mbi") == "LOWRISK_AD")["total_risk_score"].max()
        assert hr_max > lr_max

    @pytest.mark.unit
    def test_esrd_bene_uses_esrd_model_only(self, seeded_executor):
        df = execute(seeded_executor).collect()
        esrd = df.filter(pl.col("mbi") == "ESRD_BENE")
        assert esrd["model_version"].unique().to_list() == ["cms_hcc_esrd_v24"]
        assert esrd["cohort"].unique().to_list() == ["ESRD"]

    @pytest.mark.unit
    def test_cmmi_score_populates_for_ad_benes(self, seeded_executor):
        """CMMI-HCC Concurrent should produce a non-null score once the
        dx→HCC mapping is in place — once per check date."""
        df = execute(seeded_executor).collect()
        cmmi_hr = df.filter(
            (pl.col("mbi") == "HIGHRISK_AD")
            & (pl.col("model_version") == "cmmi_concurrent")
        )
        assert cmmi_hr.height == 4  # one per check date
        assert cmmi_hr["total_risk_score"].null_count() == 0

    @pytest.mark.unit
    def test_missing_diagnosis_file_does_not_crash(self, tmp_path):
        """If int_diagnosis_deduped is absent, scoring still runs — all
        beneficiaries get demographic-only scores."""
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        gold = storage.get_path("gold")
        pl.DataFrame(
            {
                "member_id": ["BENE"],
                "birth_date": [date(1955, 1, 1)],
                "gender": ["F"],
                "original_reason_entitlement_code": ["0"],
                "medicare_status_code": [None],
                "dual_status_code": ["NA"],
            }
        ).write_parquet(gold / "eligibility.parquet")

        executor = SimpleNamespace(
            storage_config=storage,
            performance_year=2026,
        )
        df = execute(executor).collect()
        assert df.height >= 1


class TestComputeAge:
    """Unit tests for _compute_age edge cases."""

    @pytest.mark.unit
    def test_null_birth_date_returns_zero(self):
        from acoharmony._transforms.hcc_risk_scores import _compute_age

        assert _compute_age(None, date(2026, 1, 1)) == 0

    @pytest.mark.unit
    def test_birthday_not_yet_reached_subtracts_one(self):
        from acoharmony._transforms.hcc_risk_scores import _compute_age

        # Born June 15, as-of Jan 1 — birthday hasn't arrived yet.
        assert _compute_age(date(1950, 6, 15), date(2026, 1, 1)) == 75

    @pytest.mark.unit
    def test_birthday_already_passed_does_not_subtract(self):
        from acoharmony._transforms.hcc_risk_scores import _compute_age

        # Born Jan 1, as-of Jan 1 — exact birthday.
        assert _compute_age(date(1950, 1, 1), date(2026, 1, 1)) == 76


class TestResolvePerformanceYears:
    """Unit tests for _resolve_performance_years precedence branches."""

    @pytest.mark.unit
    def test_performance_years_list_takes_precedence(self):
        from acoharmony._transforms.hcc_risk_scores import _resolve_performance_years

        executor = SimpleNamespace(performance_years=[2024, 2025], performance_year=2026)
        assert _resolve_performance_years(executor) == [2024, 2025]

    @pytest.mark.unit
    def test_performance_year_singular_used_when_list_absent(self):
        from acoharmony._transforms.hcc_risk_scores import _resolve_performance_years

        executor = SimpleNamespace(performance_year=2025)
        assert _resolve_performance_years(executor) == [2025]

    @pytest.mark.unit
    def test_default_range_used_when_both_absent(self):
        import datetime
        from acoharmony._transforms.hcc_risk_scores import (
            _resolve_performance_years,
            DEFAULT_FIRST_PY,
        )

        executor = SimpleNamespace()  # no performance_year(s)
        result = _resolve_performance_years(executor)
        expected = list(range(DEFAULT_FIRST_PY, datetime.date.today().year + 1))
        assert result == expected


class TestExecuteEmptyPyList:
    """Edge-case: passing performance_years=[] must return an empty LazyFrame."""

    @pytest.mark.unit
    def test_empty_performance_years_returns_empty_frame(self, tmp_path: Path):
        root = tmp_path / "workspace_empty"
        storage = _MockStorage(root)
        gold = storage.get_path("gold")
        pl.DataFrame(
            {
                "member_id": ["BENE"],
                "birth_date": [date(1955, 1, 1)],
                "gender": ["F"],
                "original_reason_entitlement_code": ["0"],
                "medicare_status_code": [None],
                "dual_status_code": ["NA"],
            }
        ).write_parquet(gold / "eligibility.parquet")

        executor = SimpleNamespace(
            storage_config=storage,
            performance_years=[],  # explicit empty list
        )
        df = execute(executor).collect()
        assert df.height == 0


class TestDxCacheReuse:
    """Verify the dx_cache hit branch (290->292) fires — the False path of
    ``if key not in dx_cache`` when the same window is queried twice."""

    @pytest.mark.unit
    def test_dx_cache_hit_branch_fires_when_window_repeated(
        self, tmp_path: Path, monkeypatch
    ):
        """Monkeypatch _dx_window_for_check to return the same (begin, end)
        for every call so that the second check date hits the cache (the
        ``if key not in dx_cache: False`` branch at line 290)."""
        from datetime import date as _date
        import acoharmony._transforms.hcc_risk_scores as _mod

        fixed_window = (_date(2025, 1, 1), _date(2025, 12, 31))
        monkeypatch.setattr(_mod, "_dx_window_for_check", lambda *_a: fixed_window)

        root = tmp_path / "workspace_cache"
        storage = _MockStorage(root)
        gold = storage.get_path("gold")
        silver = storage.get_path("silver")

        pl.DataFrame(
            {
                "member_id": ["BENE_C"],
                "birth_date": [date(1950, 1, 1)],
                "gender": ["M"],
                "original_reason_entitlement_code": ["0"],
                "medicare_status_code": [None],
                "dual_status_code": ["NA"],
            }
        ).write_parquet(gold / "eligibility.parquet")

        pl.DataFrame(
            {
                "current_bene_mbi_id": ["BENE_C"],
                "clm_dgns_cd": ["E1165"],
                "clm_from_dt": [date(2025, 6, 1)],
            }
        ).write_parquet(silver / "int_diagnosis_deduped.parquet")

        executor = SimpleNamespace(
            storage_config=storage,
            performance_year=2026,  # 4 check dates all return the same window → 3 cache hits
        )
        df = execute(executor).collect()
        # 3 models * 4 checks = 12 rows for one AD bene
        assert df.height == 12


class TestMstatEsrdClassification:
    """MSTAT ∈ {11, 21, 31} must classify as ESRD even when OREC is blank
    or non-ESRD. Prior OREC-only logic silently routed these benes to
    CMS-HCC A&D and evaluated them against threshold 3.0 instead of the
    ESRD 0.35 — a silent under-match on criterion (b)."""

    @pytest.mark.unit
    def test_blank_orec_with_esrd_mstat_routes_to_esrd_model(self, tmp_path):
        """Bene with blank OREC + MSTAT='31' must be scored under CMS-HCC
        ESRD V24 only (not A&D V24/V28/CMMI)."""
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        gold = storage.get_path("gold")
        silver = storage.get_path("silver")

        pl.DataFrame(
            {
                "member_id": ["MSTAT_ESRD_BENE"],
                "birth_date": [date(1948, 3, 1)],
                "gender": ["F"],
                "original_reason_entitlement_code": [""],
                "medicare_status_code": ["31"],
                "dual_status_code": ["NA"],
            }
        ).write_parquet(gold / "eligibility.parquet")

        pl.DataFrame(
            {
                "current_bene_mbi_id": ["MSTAT_ESRD_BENE", "MSTAT_ESRD_BENE"],
                "clm_dgns_cd": ["N186", "N186"],
                "clm_from_dt": [date(2025, 6, 1), date(2026, 6, 1)],
            }
        ).write_parquet(silver / "int_diagnosis_deduped.parquet")

        executor = SimpleNamespace(
            storage_config=storage, performance_year=2026,
        )
        df = execute(executor).collect()
        assert df["cohort"].unique().to_list() == ["ESRD"]
        assert df["model_version"].unique().to_list() == ["cms_hcc_esrd_v24"]
        assert df.height == 4  # one per check date

    @pytest.mark.unit
    def test_mstat_esrd_bene_produces_nonzero_score(self, tmp_path):
        """The scoring-path OREC synthesis: when cohort is ESRD but raw
        OREC is not in {2,3}, the transform must synthesize a valid ESRD
        OREC so the CMS-HCC ESRD V24 model actually computes a score.
        Without synthesis the model returns 0.0 regardless of dx."""
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        gold = storage.get_path("gold")
        silver = storage.get_path("silver")

        pl.DataFrame(
            {
                "member_id": ["MSTAT_ESRD_BENE"],
                "birth_date": [date(1948, 3, 1)],
                "gender": ["M"],
                "original_reason_entitlement_code": ["0"],  # non-ESRD raw
                "medicare_status_code": ["11"],            # ESRD via mstat
                "dual_status_code": ["NA"],
            }
        ).write_parquet(gold / "eligibility.parquet")

        pl.DataFrame(
            {
                "current_bene_mbi_id": ["MSTAT_ESRD_BENE"] * 2,
                "clm_dgns_cd": ["N186", "N186"],
                "clm_from_dt": [date(2025, 6, 1), date(2026, 6, 1)],
            }
        ).write_parquet(silver / "int_diagnosis_deduped.parquet")

        executor = SimpleNamespace(
            storage_config=storage, performance_year=2026,
        )
        df = execute(executor).collect()
        esrd_rows = df.filter(pl.col("model_version") == "cms_hcc_esrd_v24")
        assert esrd_rows.height == 4  # 4 check dates
        # Every check yields a nonzero score. Under the old bug (OREC='0'
        # passed straight through), all 4 would be 0.0.
        assert (esrd_rows["total_risk_score"] > 0.0).all()

    @pytest.mark.unit
    def test_mstat_20_still_classifies_as_ad(self, tmp_path):
        """MSTAT='20' (Disabled, no ESRD) with blank OREC → A&D. This
        guards against over-matching — MSTAT alone must NOT flip benes
        to ESRD when neither code indicates ESRD."""
        root = tmp_path / "workspace"
        storage = _MockStorage(root)
        gold = storage.get_path("gold")
        silver = storage.get_path("silver")

        pl.DataFrame(
            {
                "member_id": ["DISABLED_BENE"],
                "birth_date": [date(1970, 1, 1)],
                "gender": ["F"],
                "original_reason_entitlement_code": [""],
                "medicare_status_code": ["20"],
                "dual_status_code": ["NA"],
            }
        ).write_parquet(gold / "eligibility.parquet")

        pl.DataFrame(
            {
                "current_bene_mbi_id": ["DISABLED_BENE"],
                "clm_dgns_cd": ["Z00.00"],
                "clm_from_dt": [date(2025, 6, 1)],
            }
        ).write_parquet(silver / "int_diagnosis_deduped.parquet")

        executor = SimpleNamespace(
            storage_config=storage, performance_year=2026,
        )
        df = execute(executor).collect()
        assert df["cohort"].unique().to_list() == ["AD"]
