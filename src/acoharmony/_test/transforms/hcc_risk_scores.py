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
    def test_produces_one_row_per_mbi_per_applicable_model(self, seeded_executor):
        """PY2026 AD gets V24, V28, and CMMI (3 rows each);
        PY2026 ESRD gets ESRD_V24 only (1 row)."""
        lf = execute(seeded_executor)
        df = lf.collect()
        per_mbi = df.group_by("mbi").agg(pl.len().alias("n"))
        per_mbi_dict = {row["mbi"]: row["n"] for row in per_mbi.to_dicts()}
        assert per_mbi_dict["LOWRISK_AD"] == 3   # V24, V28, CMMI
        assert per_mbi_dict["HIGHRISK_AD"] == 3  # V24, V28, CMMI
        assert per_mbi_dict["ESRD_BENE"] == 1    # ESRD_V24 only

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
        assert esrd["model_version"].to_list() == ["cms_hcc_esrd_v24"]
        assert esrd["cohort"].to_list() == ["ESRD"]

    @pytest.mark.unit
    def test_cmmi_score_populates_for_ad_benes(self, seeded_executor):
        """CMMI-HCC Concurrent should produce a non-null score once the
        dx→HCC mapping is in place."""
        df = execute(seeded_executor).collect()
        cmmi_hr = df.filter(
            (pl.col("mbi") == "HIGHRISK_AD")
            & (pl.col("model_version") == "cmmi_concurrent")
        )
        assert cmmi_hr.height == 1
        assert cmmi_hr["total_risk_score"][0] is not None

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
