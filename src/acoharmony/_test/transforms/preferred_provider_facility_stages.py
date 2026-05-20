"""Tests for the preferred_provider facility stage adapter modules.

Covers:
- acoharmony._transforms.preferred_provider_facility_rollup
- acoharmony._transforms.preferred_provider_facility_benes
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import polars as pl
import pytest


def _executor_with_paths(tmp_path):
    from acoharmony.medallion import MedallionLayer

    executor = MagicMock()
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    silver.mkdir()
    gold.mkdir()

    pl.DataFrame(
        {
            "base_provider_tin": ["111"],
            "organization_npi": ["NPI_FAC1"],
            "individual_npi": [None],
            "provider_legal_business_name": ["Facility One LLC"],
            "provider_type": ["Facility and Institutional Provider"],
            "provider_class": ["Preferred Provider"],
            "performance_year": ["PY2025"],
            "entity_id": ["D0259"],
            "entity_tin": ["881823607"],
            "entity_legal_business_name": ["HarmonyCares ACO LLC"],
        }
    ).write_parquet(silver / "participant_list.parquet")
    pl.DataFrame(
        {
            "facility_npi": ["NPI_FAC1", "NPI_FAC1"],
            "member_id": ["B1", "B2"],
            "claim_start_date": [date(2024, 1, 1), date(2024, 6, 1)],
            "claim_type": ["institutional", "institutional"],
            "paid_amount": [Decimal("10.00"), Decimal("20.00")],
        }
    ).write_parquet(gold / "medical_claim.parquet")

    def get_path(tier):
        if tier == MedallionLayer.SILVER:
            return silver
        if tier == MedallionLayer.GOLD:
            return gold
        return tmp_path

    executor.storage_config.get_path.side_effect = get_path
    return executor


class TestRollupStage:
    @pytest.mark.unit
    def test_execute_returns_collectable_lazyframe(self, tmp_path):
        from acoharmony._transforms import preferred_provider_facility_rollup as mod

        lf = mod.execute(_executor_with_paths(tmp_path))
        assert isinstance(lf, pl.LazyFrame)
        df = lf.collect()
        assert df.height == 1
        assert df["unique_bene_count"][0] == 2
        assert df["npi"][0] == "NPI_FAC1"


class TestBenesStage:
    @pytest.mark.unit
    def test_execute_returns_collectable_lazyframe(self, tmp_path):
        from acoharmony._transforms import preferred_provider_facility_benes as mod

        lf = mod.execute(_executor_with_paths(tmp_path))
        assert isinstance(lf, pl.LazyFrame)
        df = lf.collect()
        assert df.height == 2
        assert sorted(df["member_id"].to_list()) == ["B1", "B2"]
