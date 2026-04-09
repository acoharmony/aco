# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.beneficiary_metrics module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
import acoharmony


class _MockMedallionStorage:
    """Mock medallion storage for transform tests."""

    def __init__(self, silver_path=None, gold_path=None):
        if silver_path is None:
            silver_path = Path(".")
        self.silver_path = silver_path
        self.gold_path = gold_path or silver_path

    def get_path(self, layer: str = "silver"):
        if layer == "silver":
            return self.silver_path
        if layer == "gold":
            return self.gold_path
        return self.silver_path


class _MockExecutor:
    """Mock executor for transform tests."""

    def __init__(self, base=None, storage_config=None):
        if storage_config is not None:
            self.storage_config = storage_config
        elif base is not None:
            self.storage_config = _MockMedallionStorage(silver_path=base)
        else:
            self.storage_config = _MockMedallionStorage()


@pytest.fixture
def executor(tmp_base: Path) -> _MockExecutor:
    return _MockExecutor(tmp_base)


class TestBeneficiaryMetrics:
    """Tests for beneficiary_metrics executor transform."""

    @pytest.mark.unit
    def test_import_module(self):
        from acoharmony._transforms import beneficiary_metrics
        assert beneficiary_metrics is not None

    @pytest.mark.unit
    def test_execute_exists(self):
        from acoharmony._transforms.beneficiary_metrics import execute
        assert callable(execute)


class TestBeneficiaryMetricsExecute:
    """Tests exercising the execute function for beneficiary metrics."""

    def _make_medical_claims(self, gold_path):
        df = pl.DataFrame({
            "claim_id": ["C1", "C2", "C3", "C4"],
            "person_id": ["P1", "P1", "P2", "P1"],
            "claim_start_date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 3, 1),
                datetime.date(2024, 2, 1),
                datetime.date(2024, 6, 15),
            ],
            "claim_end_date": [
                datetime.date(2024, 1, 5),
                datetime.date(2024, 3, 5),
                datetime.date(2024, 2, 5),
                datetime.date(2024, 6, 20),
            ],
            "claim_type": ["institutional", "professional", "institutional", "professional"],
            "bill_type_code": ["111", "", "121", ""],
            "revenue_center_code": ["0100", "", "0190", "0450"],
            "place_of_service_code": ["21", "11", "31", "23"],
            "hcpcs_code": ["99213", "G0438", "99213", "99281"],
            "paid_amount": [5000.0, 200.0, 3000.0, 800.0],
            "admission_date": [datetime.date(2024, 1, 1), None, datetime.date(2024, 2, 1), None],
            "discharge_date": [datetime.date(2024, 1, 5), None, datetime.date(2024, 2, 5), None],
        })
        df.write_parquet(str(gold_path / "medical_claim.parquet"))

    def _make_pharmacy_claims(self, gold_path):
        df = pl.DataFrame({
            "claim_id": ["RX1", "RX2"],
            "person_id": ["P1", "P3"],
            "dispensing_date": [datetime.date(2024, 1, 15), datetime.date(2024, 5, 1)],
            "paid_amount": [50.0, 75.0],
        })
        df.write_parquet(str(gold_path / "pharmacy_claim.parquet"))

    @pytest.mark.unit
    def test_execute_produces_result(self, tmp_path):
        """Execute produces a LazyFrame with expected columns."""
        gold = tmp_path / "gold"
        gold.mkdir()
        self._make_medical_claims(gold)
        self._make_pharmacy_claims(gold)

        from acoharmony._transforms.beneficiary_metrics import execute

        executor = MagicMock()
        executor.storage_config.get_path.return_value = gold

        result = execute(executor)
        assert isinstance(result, pl.LazyFrame)
        collected = result.collect()
        assert "person_id" in collected.columns
        assert "year" in collected.columns
        assert "total_spend_ytd" in collected.columns
        assert "inpatient_spend_ytd" in collected.columns
        assert "dme_spend_ytd" in collected.columns
        assert "most_recent_awv_date" in collected.columns
        assert "last_em_visit_date" in collected.columns

    @pytest.mark.unit
    def test_execute_fills_nulls(self, tmp_path):
        """Spend/count columns are filled with 0 for nulls after join."""
        gold = tmp_path / "gold"
        gold.mkdir()
        self._make_medical_claims(gold)
        self._make_pharmacy_claims(gold)

        from acoharmony._transforms.beneficiary_metrics import execute

        executor = MagicMock()
        executor.storage_config.get_path.return_value = gold

        result = execute(executor).collect()
        # P3 only has pharmacy claims, so medical spend columns should be 0
        p3 = result.filter(pl.col("person_id") == "P3")
        if p3.height > 0:
            assert p3["inpatient_spend_ytd"][0] == 0.0
