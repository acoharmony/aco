"""Tests for acoharmony._transforms._preferred_provider_claims."""

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

import acoharmony  # noqa: F401  (imported for side effects)
from acoharmony._transforms._preferred_provider_claims import (
    DEFAULT_BENES_FILENAME,
    DEFAULT_ROLLUP_FILENAME,
    apply_transform,
    execute_to_gold,
)


def _write_participant_fixture(silver_path):
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
    ).write_parquet(silver_path / "participant_list.parquet")


def _write_medical_claim_fixture(gold_path):
    pl.DataFrame(
        {
            "facility_npi": ["NPI_FAC1", "NPI_FAC1", "NPI_UNRELATED"],
            "member_id": ["B1", "B2", "B3"],
            "claim_start_date": [date(2024, 1, 5), date(2024, 6, 15), date(2025, 1, 1)],
            "claim_type": ["institutional", "institutional", "institutional"],
            "paid_amount": [Decimal("100.00"), Decimal("400.00"), Decimal("1.00")],
        }
    ).write_parquet(gold_path / "medical_claim.parquet")


class TestExecuteToGold:
    @pytest.mark.unit
    def test_writes_both_parquets_and_returns_paths(self, tmp_path):
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        silver.mkdir()
        gold.mkdir()
        _write_participant_fixture(silver)
        _write_medical_claim_fixture(gold)

        rollup_path, benes_path = execute_to_gold(silver, gold)

        assert rollup_path.exists()
        assert benes_path.exists()
        assert rollup_path.name == DEFAULT_ROLLUP_FILENAME
        assert benes_path.name == DEFAULT_BENES_FILENAME

        rollup = pl.read_parquet(rollup_path)
        benes = pl.read_parquet(benes_path)
        # One facility, two beneficiaries, $500 paid total.
        assert rollup.height == 1
        assert rollup["unique_bene_count"][0] == 2
        assert rollup["claim_count"][0] == 2
        assert rollup["total_paid_amount"][0] == Decimal("500.00")
        assert benes.height == 2
        assert sorted(benes["member_id"].to_list()) == ["B1", "B2"]

    @pytest.mark.unit
    def test_logger_messages_emitted_when_provided(self, tmp_path):
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        silver.mkdir()
        gold.mkdir()
        _write_participant_fixture(silver)
        _write_medical_claim_fixture(gold)
        logger = MagicMock()

        execute_to_gold(silver, gold, logger=logger)

        info_calls = [args[0] for args, _ in logger.info.call_args_list]
        assert any("preferred_provider_facility_rollup.parquet" in m for m in info_calls)
        assert any("preferred_provider_facility_benes.parquet" in m for m in info_calls)

    @pytest.mark.unit
    def test_custom_facet_parameters_threaded_through(self, tmp_path):
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        silver.mkdir()
        gold.mkdir()
        # Roster has only an Organizational Provider, NOT facility — so the
        # default facet would produce zero rows. Pass a different facet
        # and confirm the row falls through.
        pl.DataFrame(
            {
                "base_provider_tin": ["222"],
                "organization_npi": ["NPI_ORG"],
                "individual_npi": [None],
                "provider_legal_business_name": ["Org Group"],
                "provider_type": ["Organizational Provider"],
                "provider_class": ["Preferred Provider"],
                "performance_year": ["PY2025"],
                "entity_id": ["D0259"],
                "entity_tin": ["881823607"],
                "entity_legal_business_name": ["HarmonyCares ACO LLC"],
            }
        ).write_parquet(silver / "participant_list.parquet")
        pl.DataFrame(
            {
                "facility_npi": ["NPI_ORG"],
                "member_id": ["B1"],
                "claim_start_date": [date(2024, 1, 1)],
                "claim_type": ["professional"],
                "paid_amount": [Decimal("10.00")],
            }
        ).write_parquet(gold / "medical_claim.parquet")

        # Default facet → no match.
        rollup_default, _ = execute_to_gold(silver, gold)
        assert pl.read_parquet(rollup_default).height == 0

        # Custom facet → matches.
        rollup_custom, _ = execute_to_gold(
            silver,
            gold,
            provider_category="Preferred Provider",
            provider_type="Organizational",
        )
        assert pl.read_parquet(rollup_custom).height == 1


class TestApplyTransform:
    @pytest.mark.unit
    def test_apply_transform_writes_to_storage_and_returns_lazyframe(self, tmp_path):
        silver = tmp_path / "silver"
        gold = tmp_path / "gold"
        silver.mkdir()
        gold.mkdir()
        _write_participant_fixture(silver)
        _write_medical_claim_fixture(gold)

        catalog = MagicMock()
        catalog.storage_config.get_path.side_effect = lambda tier: {
            "silver": silver,
            "gold": gold,
        }[tier]
        logger = MagicMock()
        inner = getattr(apply_transform, "func", apply_transform)
        result = inner(pl.LazyFrame(), {}, catalog, logger)
        # Returned LazyFrame can be collected and contains the rollup.
        df = result.collect()
        assert df.height == 1
        assert df["unique_bene_count"][0] == 2
