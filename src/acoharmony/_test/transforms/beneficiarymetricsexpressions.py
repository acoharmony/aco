# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.beneficiary_metrics_expressions module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from datetime import date, datetime  # noqa: F811
from pathlib import Path

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


def _date(y: int, m: int, d: int) -> datetime.date:
    return datetime.date(y, m, d)
class TestBeneficiaryMetricsExpressions:
    """
    Tests for beneficiary_metrics expression components.

    Since beneficiary_metrics.execute() requires a full executor with storage,
    we test the expression classes it depends on and the aggregation logic.
    """

    @pytest.mark.unit
    def test_spend_category_inpatient(self):
        from acoharmony._expressions._spend_category import SpendCategoryExpression

        df = pl.DataFrame(
            {
                "bill_type_code": ["111", "131", "211", "811", None],
                "paid_amount": [100.0, 200.0, 300.0, 400.0, 500.0],
            }
        )

        result = df.with_columns(
            SpendCategoryExpression.is_inpatient_spend().alias("inpatient_spend")
        )

        assert result["inpatient_spend"].to_list() == [100.0, 0.0, 0.0, 0.0, 0.0]

    @pytest.mark.unit
    def test_spend_category_outpatient(self):
        from acoharmony._expressions._spend_category import SpendCategoryExpression

        df = pl.DataFrame(
            {
                "bill_type_code": ["131", "111", "211"],
                "paid_amount": [100.0, 200.0, 300.0],
            }
        )

        result = df.with_columns(
            SpendCategoryExpression.is_outpatient_spend().alias("outpatient_spend")
        )

        assert result["outpatient_spend"].to_list() == [100.0, 0.0, 0.0]

    @pytest.mark.unit
    def test_spend_category_snf(self):
        from acoharmony._expressions._spend_category import SpendCategoryExpression

        df = pl.DataFrame(
            {
                "bill_type_code": ["211", "221", "111"],
                "paid_amount": [100.0, 200.0, 300.0],
            }
        )

        result = df.with_columns(
            SpendCategoryExpression.is_snf_spend().alias("snf_spend")
        )

        assert result["snf_spend"].to_list() == [100.0, 200.0, 0.0]

    @pytest.mark.unit
    def test_spend_category_hospice(self):
        from acoharmony._expressions._spend_category import SpendCategoryExpression

        df = pl.DataFrame(
            {
                "bill_type_code": ["811", "821", "111"],
                "paid_amount": [100.0, 200.0, 300.0],
            }
        )

        result = df.with_columns(
            SpendCategoryExpression.is_hospice_spend().alias("hospice_spend")
        )

        assert result["hospice_spend"].to_list() == [100.0, 200.0, 0.0]

    @pytest.mark.unit
    def test_spend_category_home_health(self):
        from acoharmony._expressions._spend_category import SpendCategoryExpression

        df = pl.DataFrame(
            {
                "bill_type_code": ["321", "331", "341", "111"],
                "paid_amount": [100.0, 200.0, 300.0, 400.0],
            }
        )

        result = df.with_columns(
            SpendCategoryExpression.is_home_health_spend().alias("hh_spend")
        )

        assert result["hh_spend"].to_list() == [100.0, 200.0, 300.0, 0.0]

    @pytest.mark.unit
    def test_spend_category_part_b(self):
        from acoharmony._expressions._spend_category import SpendCategoryExpression

        df = pl.DataFrame(
            {
                "bill_type_code": [None, "", "111"],
                "paid_amount": [100.0, 200.0, 300.0],
            }
        )

        result = df.with_columns(
            SpendCategoryExpression.is_part_b_carrier_spend().alias("part_b_spend")
        )

        assert result["part_b_spend"].to_list() == [100.0, 200.0, 0.0]

    @pytest.mark.unit
    def test_utilization_inpatient_admission(self):
        from acoharmony._expressions._utilization import UtilizationExpression

        df = pl.DataFrame(
            {
                "bill_type_code": ["111", "121", "131", "211"],
            }
        )

        result = df.with_columns(
            UtilizationExpression.is_inpatient_admission().alias("ip_admit")
        )

        assert result["ip_admit"].to_list() == [1, 1, 0, 0]

    @pytest.mark.unit
    def test_utilization_er_visit(self):
        from acoharmony._expressions._utilization import UtilizationExpression

        df = pl.DataFrame(
            {
                "bill_type_code": ["111", "131", "999"],
                "revenue_center_code": ["0450", "0000", "9999"],
                "place_of_service_code": ["11", "11", "23"],
            }
        )

        result = df.with_columns(
            UtilizationExpression.is_er_visit().alias("er")
        )

        # First: revenue code 0450 -> ER
        assert result["er"][0] == 1
        # Third: POS 23 -> ER
        assert result["er"][2] == 1

    @pytest.mark.unit
    def test_utilization_em_visit(self):
        from acoharmony._expressions._utilization import UtilizationExpression

        df = pl.DataFrame(
            {
                "hcpcs_code": ["99213", "99281", "12345", "G0438"],
            }
        )

        result = df.with_columns(
            UtilizationExpression.is_em_visit().alias("em")
        )

        assert result["em"][0] == 1  # office visit
        assert result["em"][1] == 1  # ED E&M
        assert result["em"][2] == 0  # not E&M
        assert result["em"][3] == 0  # AWV, not E&M

    @pytest.mark.unit
    def test_utilization_awv(self):
        from acoharmony._expressions._utilization import UtilizationExpression

        df = pl.DataFrame(
            {
                "hcpcs_code": ["G0438", "G0439", "99213", None],
            }
        )

        result = df.with_columns(
            UtilizationExpression.is_awv().alias("awv")
        )

        assert result["awv"].to_list() == [1, 1, 0, 0]

    @pytest.mark.unit
    def test_clinical_hospice_admission(self):
        from acoharmony._expressions._clinical_indicators import (
            ClinicalIndicatorExpression,
        )

        df = pl.DataFrame(
            {
                "bill_type_code": ["811", "821", "111"],
            }
        )

        result = df.with_columns(
            ClinicalIndicatorExpression.is_hospice_admission().alias("hospice")
        )

        assert result["hospice"].to_list() == [1, 1, 0]

    @pytest.mark.unit
    def test_clinical_snf_admission(self):
        from acoharmony._expressions._clinical_indicators import (
            ClinicalIndicatorExpression,
        )

        df = pl.DataFrame(
            {
                "bill_type_code": ["211", "221", "111"],
            }
        )

        result = df.with_columns(
            ClinicalIndicatorExpression.is_snf_admission().alias("snf")
        )

        assert result["snf"].to_list() == [1, 1, 0]

    @pytest.mark.unit
    def test_clinical_irf_admission(self):
        from acoharmony._expressions._clinical_indicators import (
            ClinicalIndicatorExpression,
        )

        df = pl.DataFrame(
            {
                "bill_type_code": ["821", "111"],
            }
        )

        result = df.with_columns(
            ClinicalIndicatorExpression.is_irf_admission().alias("irf")
        )

        assert result["irf"].to_list() == [1, 0]

    @pytest.mark.unit
    def test_clinical_home_health(self):
        from acoharmony._expressions._clinical_indicators import (
            ClinicalIndicatorExpression,
        )

        df = pl.DataFrame(
            {
                "bill_type_code": ["321", "331", "341", "111"],
            }
        )

        result = df.with_columns(
            ClinicalIndicatorExpression.is_home_health_episode().alias("hh")
        )

        assert result["hh"].to_list() == [1, 1, 1, 0]

    @pytest.mark.unit
    def test_beneficiary_aggregation_logic(self):
        """Test the aggregation pattern used in beneficiary_metrics.execute."""
        from acoharmony._expressions._clinical_indicators import (
            ClinicalIndicatorExpression,
        )
        from acoharmony._expressions._spend_category import SpendCategoryExpression
        from acoharmony._expressions._utilization import UtilizationExpression

        medical_claim = pl.DataFrame(
            {
                "person_id": ["P1", "P1", "P1", "P2"],
                "claim_id": ["C1", "C2", "C3", "C4"],
                "claim_start_date": [
                    date(2024, 1, 5),
                    date(2024, 3, 10),
                    date(2024, 6, 1),
                    date(2024, 2, 1),
                ],
                "bill_type_code": ["111", "131", "811", ""],
                "paid_amount": [5000.0, 200.0, 1000.0, 300.0],
                "hcpcs_code": ["99213", "99213", "99213", "G0438"],
                "revenue_center_code": ["0000", "0000", "0000", "0000"],
                "place_of_service_code": ["11", "11", "11", "11"],
            }
        )

        # Apply expressions like beneficiary_metrics does
        result = medical_claim.with_columns(
            [
                SpendCategoryExpression.is_inpatient_spend().alias("inpatient_spend"),
                SpendCategoryExpression.is_outpatient_spend().alias("outpatient_spend"),
                SpendCategoryExpression.is_hospice_spend().alias("hospice_spend"),
                SpendCategoryExpression.is_part_b_carrier_spend().alias("part_b_spend"),
                UtilizationExpression.is_inpatient_admission().alias("ip_admit"),
                UtilizationExpression.is_em_visit().alias("em_visit"),
                UtilizationExpression.is_awv().alias("awv"),
                ClinicalIndicatorExpression.is_hospice_admission().alias("hospice_admit"),
            ]
        )

        assert result["inpatient_spend"][0] == 5000.0
        assert result["outpatient_spend"][1] == 200.0
        assert result["hospice_spend"][2] == 1000.0
        assert result["part_b_spend"][3] == 300.0
        assert result["ip_admit"][0] == 1
        assert result["em_visit"][0] == 1
        assert result["awv"][3] == 1
        assert result["hospice_admit"][2] == 1

        # Aggregate to person level like the transform does
        agg = result.group_by("person_id").agg(
            [
                pl.col("inpatient_spend").sum().alias("inpatient_spend_ytd"),
                pl.col("outpatient_spend").sum().alias("outpatient_spend_ytd"),
                pl.col("hospice_spend").sum().alias("hospice_spend_ytd"),
                pl.col("part_b_spend").sum().alias("part_b_spend_ytd"),
                pl.col("hospice_admit").max().alias("hospice_admission_ytd"),
            ]
        )

        p1 = agg.filter(pl.col("person_id") == "P1")
        assert p1["inpatient_spend_ytd"][0] == 5000.0
        assert p1["outpatient_spend_ytd"][0] == 200.0
        assert p1["hospice_spend_ytd"][0] == 1000.0
        assert p1["hospice_admission_ytd"][0] == 1

        p2 = agg.filter(pl.col("person_id") == "P2")
        assert p2["part_b_spend_ytd"][0] == 300.0

    @pytest.mark.unit
    def test_total_spend_calculation(self):
        """Test that total spend horizontal sum works."""
        df = pl.DataFrame(
            {
                "inpatient_spend_ytd": [1000.0],
                "outpatient_spend_ytd": [200.0],
                "snf_spend_ytd": [300.0],
                "hospice_spend_ytd": [0.0],
                "home_health_spend_ytd": [100.0],
                "part_b_carrier_spend_ytd": [50.0],
                "dme_spend_ytd": [25.0],
            }
        )

        result = df.with_columns(
            pl.sum_horizontal(
                [
                    "inpatient_spend_ytd",
                    "outpatient_spend_ytd",
                    "snf_spend_ytd",
                    "hospice_spend_ytd",
                    "home_health_spend_ytd",
                    "part_b_carrier_spend_ytd",
                    "dme_spend_ytd",
                ]
            ).alias("total_spend_ytd")
        )

        assert result["total_spend_ytd"][0] == 1675.0
