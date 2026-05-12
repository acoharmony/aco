"""Tests for acoharmony._pipes._preferred_providers."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import logging
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import polars as pl
import pytest

import acoharmony  # noqa: F401  (imports register pipelines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_executor(tmp_path):
    """Mock executor with storage paths rooted in tmp_path."""
    from acoharmony.medallion import MedallionLayer

    executor = MagicMock()
    storage = MagicMock()
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    silver.mkdir(parents=True, exist_ok=True)
    gold.mkdir(parents=True, exist_ok=True)

    def get_path(tier):
        if tier == MedallionLayer.SILVER or tier == "silver":
            return silver
        if tier == MedallionLayer.GOLD or tier == "gold":
            return gold
        return tmp_path

    storage.get_path.side_effect = get_path
    executor.storage_config = storage
    return executor, silver, gold


def _write_participant(silver):
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


def _write_medical_claim(gold):
    pl.DataFrame(
        {
            "facility_npi": ["NPI_FAC1", "NPI_FAC1"],
            "member_id": ["B1", "B2"],
            "claim_start_date": [date(2024, 1, 5), date(2024, 6, 15)],
            "claim_type": ["institutional", "institutional"],
            "paid_amount": [Decimal("100.00"), Decimal("400.00")],
        }
    ).write_parquet(gold / "medical_claim.parquet")


@pytest.fixture
def logger():
    return MagicMock(spec=logging.Logger)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestModuleStructure:
    @pytest.mark.unit
    def test_module_imports(self):
        assert acoharmony._pipes._preferred_providers is not None

    @pytest.mark.unit
    def test_pipeline_registered(self):
        from acoharmony._pipes._registry import PipelineRegistry

        # The package-level autouse fixture clears the registry before each
        # test; re-import the pipe module so its @register_pipeline decorator
        # fires again.
        import importlib

        import acoharmony._pipes._preferred_providers as mod

        importlib.reload(mod)
        assert "preferred_providers" in PipelineRegistry.list_pipelines()


class TestPrerequisiteChecks:
    @pytest.mark.unit
    def test_missing_participant_list_raises(self, tmp_path, logger):
        from acoharmony._pipes._preferred_providers import (
            apply_preferred_providers_pipeline,
        )

        executor, _silver, gold = _make_executor(tmp_path)
        _write_medical_claim(gold)
        # participant_list deliberately absent
        inner = getattr(
            apply_preferred_providers_pipeline,
            "func",
            apply_preferred_providers_pipeline,
        )
        with pytest.raises(FileNotFoundError, match="participant_list.parquet"):
            inner(executor, logger, force=False)

    @pytest.mark.unit
    def test_missing_medical_claim_raises(self, tmp_path, logger):
        from acoharmony._pipes._preferred_providers import (
            apply_preferred_providers_pipeline,
        )

        executor, silver, _gold = _make_executor(tmp_path)
        _write_participant(silver)
        # medical_claim deliberately absent
        inner = getattr(
            apply_preferred_providers_pipeline,
            "func",
            apply_preferred_providers_pipeline,
        )
        with pytest.raises(FileNotFoundError, match="medical_claim.parquet"):
            inner(executor, logger, force=False)


class TestEndToEnd:
    @pytest.mark.unit
    def test_force_run_produces_both_gold_parquets(self, tmp_path, logger):
        from acoharmony._pipes._preferred_providers import (
            apply_preferred_providers_pipeline,
        )

        executor, silver, gold = _make_executor(tmp_path)
        _write_participant(silver)
        _write_medical_claim(gold)
        inner = getattr(
            apply_preferred_providers_pipeline,
            "func",
            apply_preferred_providers_pipeline,
        )
        result = inner(executor, logger, force=True)

        # Both stages reported in the result dict.
        assert set(result.keys()) == {
            "preferred_provider_facility_rollup",
            "preferred_provider_facility_benes",
        }
        # Both parquets exist with expected content.
        rollup = pl.read_parquet(gold / "preferred_provider_facility_rollup.parquet")
        benes = pl.read_parquet(gold / "preferred_provider_facility_benes.parquet")
        assert rollup.height == 1
        assert rollup["unique_bene_count"][0] == 2
        assert benes.height == 2
        assert sorted(benes["member_id"].to_list()) == ["B1", "B2"]


class TestAllPipelineOrder:
    @pytest.mark.unit
    def test_preferred_providers_in_all_pipeline_order(self):
        from acoharmony._pipes._all import PIPELINE_ORDER

        assert "preferred_providers" in PIPELINE_ORDER

    @pytest.mark.unit
    def test_preferred_providers_runs_after_cclf_gold_and_before_analytics(self):
        """Must come after cclf_gold (medical_claim) and before analytics_gold (consumer)."""
        from acoharmony._pipes._all import PIPELINE_ORDER

        order = PIPELINE_ORDER.index
        assert order("preferred_providers") > order("cclf_gold")
        assert order("preferred_providers") < order("analytics_gold")
