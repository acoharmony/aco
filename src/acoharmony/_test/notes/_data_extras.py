# © 2025 HarmonyCares
"""Tests for the patient/claims-oriented additions to acoharmony._notes._data."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from acoharmony._notes import DataPlugins


def _patched_data(storage_root: Path) -> tuple[DataPlugins, MagicMock]:
    """Build a DataPlugins whose .storage points at `storage_root`."""
    storage = MagicMock()
    storage.get_path.side_effect = lambda tier: str(storage_root / tier)
    plugin = DataPlugins()
    plugin._storage = storage
    return plugin, storage


def _write_parquet(path: Path, df: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


class TestDatasetMetadata:
    @pytest.mark.unit
    def test_skips_missing_files(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        out = plugin.dataset_metadata({"missing": ("missing.parquet", "d")})
        assert out == {}

    @pytest.mark.unit
    def test_collects_metadata(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame({"d": [date(2024, 1, 1), date(2024, 6, 1)], "v": [1, 2]})
        _write_parquet(tmp_path / "gold" / "demo.parquet", df)
        out = plugin.dataset_metadata({"demo": ("demo.parquet", "d")})
        assert out["demo"]["rows"] == 2
        assert out["demo"]["columns"] == 2
        assert out["demo"]["min_date"] == "2024-01-01"
        assert out["demo"]["max_date"] == "2024-06-01"
        assert out["demo"]["last_run"] is None

    @pytest.mark.unit
    def test_handles_null_dates(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame({"d": pl.Series([None, None], dtype=pl.Date), "v": [1, 2]})
        _write_parquet(tmp_path / "gold" / "demo.parquet", df)
        out = plugin.dataset_metadata({"demo": ("demo.parquet", "d")})
        assert out["demo"]["min_date"] == "N/A"
        assert out["demo"]["max_date"] == "N/A"

    @pytest.mark.unit
    def test_reads_tracking_last_run(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame({"d": [date(2024, 1, 1)], "v": [1]})
        _write_parquet(tmp_path / "gold" / "demo.parquet", df)
        tracking = tmp_path / "logs" / "tracking"
        tracking.mkdir(parents=True)
        (tracking / "demo_state.json").write_text(
            '{"last_run": "2024-04-01T08:00:00.000"}'
        )
        out = plugin.dataset_metadata({"demo": ("demo.parquet", "d")})
        assert out["demo"]["last_run"] == "2024-04-01T08:00:00"

    @pytest.mark.unit
    def test_empty_last_run_string_skips(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame({"d": [date(2024, 1, 1)], "v": [1]})
        _write_parquet(tmp_path / "gold" / "demo.parquet", df)
        tracking = tmp_path / "logs" / "tracking"
        tracking.mkdir(parents=True)
        (tracking / "demo_state.json").write_text('{"last_run": ""}')
        out = plugin.dataset_metadata({"demo": ("demo.parquet", "d")})
        assert out["demo"]["last_run"] is None

    @pytest.mark.unit
    def test_corrupt_tracking_silently_ignored(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame({"d": [date(2024, 1, 1)], "v": [1]})
        _write_parquet(tmp_path / "gold" / "demo.parquet", df)
        tracking = tmp_path / "logs" / "tracking"
        tracking.mkdir(parents=True)
        (tracking / "demo_state.json").write_text("not-json{")
        out = plugin.dataset_metadata({"demo": ("demo.parquet", "d")})
        assert out["demo"]["last_run"] is None


class TestResolveIdentity:
    @pytest.mark.unit
    def test_empty_mbi(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        result = plugin.resolve_identity("", timeline_lf=pl.LazyFrame())
        assert result == {"hcmpi": None, "current_mbi": "", "history": [], "chain_df": None}

    @pytest.mark.unit
    def test_unknown_mbi_returns_self(self) -> None:
        plugin = DataPlugins()
        timeline = pl.LazyFrame({"mbi": ["A", "B"], "chain_id": [1, 2], "hcmpi": [None, "H"], "hop_index": [0, 0]})
        result = plugin.resolve_identity("ZZZ", timeline_lf=timeline)
        assert result["hcmpi"] is None
        assert result["current_mbi"] == "ZZZ"
        assert result["history"] == ["ZZZ"]
        assert result["chain_df"] is None

    @pytest.mark.unit
    def test_resolves_chain(self) -> None:
        plugin = DataPlugins()
        timeline = pl.LazyFrame(
            {
                "mbi": ["OLD", "NEW", "OTHER"],
                "chain_id": [1, 1, 2],
                "hcmpi": [None, "HC1", "HC2"],
                "hop_index": [1, 0, 0],
            }
        )
        result = plugin.resolve_identity("OLD", timeline_lf=timeline)
        assert result["hcmpi"] == "HC1"
        assert result["current_mbi"] == "NEW"
        assert sorted(result["history"]) == ["NEW", "OLD"]
        assert result["chain_df"].height == 2

    @pytest.mark.unit
    def test_lazy_loads_default_silver(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame(
            {
                "mbi": ["A"],
                "chain_id": [1],
                "hcmpi": ["HC"],
                "hop_index": [0],
            }
        )
        _write_parquet(tmp_path / "silver" / "identity_timeline.parquet", df)
        result = plugin.resolve_identity("A")
        assert result["current_mbi"] == "A"
        assert result["hcmpi"] == "HC"

    @pytest.mark.unit
    def test_no_leaf_falls_back_to_input(self) -> None:
        plugin = DataPlugins()
        # All hop_indexes nonzero — no leaf row.
        timeline = pl.LazyFrame(
            {"mbi": ["A"], "chain_id": [1], "hcmpi": [None], "hop_index": [1]}
        )
        result = plugin.resolve_identity("A", timeline_lf=timeline)
        assert result["current_mbi"] == "A"

    @pytest.mark.unit
    def test_chain_id_resolves_but_chain_lookup_empty(self, monkeypatch) -> None:
        """Defensive: chain_id row exists but the secondary chain-id filter returns 0 rows."""
        plugin = DataPlugins()
        # Stage a fake LazyFrame whose .filter().collect() returns the right
        # shape on the first call (chain id lookup) and an empty frame on
        # the second (chain rows lookup).
        first = pl.LazyFrame({"mbi": ["A"], "chain_id": [1]})
        empty = pl.DataFrame({"mbi": pl.Series([], dtype=pl.Utf8), "chain_id": pl.Series([], dtype=pl.Int64)})

        class StubLF:
            def __init__(self, calls):
                self.calls = calls

            def filter(self, _expr):
                return self

            def select(self, *args):
                return self

            def unique(self):
                return self

            def collect(self):
                self.calls.append(1)
                if len(self.calls) == 1:
                    return first.collect()  # first call: chain_id lookup
                return empty  # second call: chain rows lookup

        result = plugin.resolve_identity("A", timeline_lf=StubLF([]))
        assert result["chain_df"] is None
        assert result["current_mbi"] == "A"


class TestGetDemographics:
    @pytest.mark.unit
    def test_empty_mbi(self) -> None:
        assert DataPlugins().get_demographics("") is None

    @pytest.mark.unit
    def test_match(self) -> None:
        lf = pl.LazyFrame({"bene_mbi_id": ["A", "B"], "bene_age": [70, 65]})
        df = DataPlugins().get_demographics("A", demographics_lf=lf)
        assert df is not None and df.height == 1
        assert df["bene_age"][0] == 70

    @pytest.mark.unit
    def test_no_match(self) -> None:
        lf = pl.LazyFrame({"bene_mbi_id": ["X"], "bene_age": [70]})
        assert DataPlugins().get_demographics("A", demographics_lf=lf) is None

    @pytest.mark.unit
    def test_loads_default_silver(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame({"bene_mbi_id": ["A"], "bene_age": [60]})
        _write_parquet(tmp_path / "silver" / "beneficiary_demographics.parquet", df)
        out = plugin.get_demographics("A")
        assert out is not None and out["bene_age"][0] == 60


class TestGetAlignment:
    @pytest.mark.unit
    def test_empty_mbi(self) -> None:
        assert DataPlugins().get_alignment("") is None

    @pytest.mark.unit
    def test_match(self) -> None:
        lf = pl.LazyFrame({"current_mbi": ["A", "B"], "year": [2024, 2024]})
        df = DataPlugins().get_alignment("A", alignment_lf=lf)
        assert df is not None and df.height == 1

    @pytest.mark.unit
    def test_no_match(self) -> None:
        lf = pl.LazyFrame({"current_mbi": ["X"]})
        assert DataPlugins().get_alignment("A", alignment_lf=lf) is None

    @pytest.mark.unit
    def test_loads_default_gold(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame({"current_mbi": ["A"], "y": [1]})
        _write_parquet(tmp_path / "gold" / "consolidated_alignment.parquet", df)
        out = plugin.get_alignment("A")
        assert out is not None and out.height == 1


class TestGetChronicConditions:
    @pytest.mark.unit
    def test_empty_mbi(self) -> None:
        assert DataPlugins().get_chronic_conditions("") is None

    @pytest.mark.unit
    def test_matches_current_mbi(self) -> None:
        lf = pl.LazyFrame({"person_id": ["A"], "diabetes": [True]})
        df = DataPlugins().get_chronic_conditions("A", conditions_lf=lf)
        assert df is not None and df.height == 1

    @pytest.mark.unit
    def test_falls_back_to_hcmpi(self) -> None:
        lf = pl.LazyFrame({"person_id": ["HC1"], "diabetes": [True]})
        df = DataPlugins().get_chronic_conditions("MBI", hcmpi="HC1", conditions_lf=lf)
        assert df is not None and df.height == 1

    @pytest.mark.unit
    def test_no_match(self) -> None:
        lf = pl.LazyFrame({"person_id": ["X"], "diabetes": [True]})
        assert DataPlugins().get_chronic_conditions("A", conditions_lf=lf) is None

    @pytest.mark.unit
    def test_loads_default_gold(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame({"person_id": ["A"], "v": [1]})
        _write_parquet(tmp_path / "gold" / "chronic_conditions_wide.parquet", df)
        out = plugin.get_chronic_conditions("A")
        assert out is not None and out.height == 1


class TestPatientLines:
    @pytest.mark.unit
    def test_medical_lines_empty(self) -> None:
        assert DataPlugins().get_patient_medical_lines("") is None

    @pytest.mark.unit
    def test_medical_lines_match(self) -> None:
        lf = pl.LazyFrame(
            {
                "person_id": ["A", "A", "B"],
                "claim_start_date": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 1, 1)],
                "paid_amount": [10.0, 20.0, 30.0],
            }
        )
        df = DataPlugins().get_patient_medical_lines("A", medical_lf=lf)
        assert df is not None and df.height == 2
        # sorted descending by date
        assert df["claim_start_date"][0] == date(2024, 6, 1)

    @pytest.mark.unit
    def test_medical_lines_no_match(self) -> None:
        lf = pl.LazyFrame(
            {"person_id": ["X"], "claim_start_date": [date(2024, 1, 1)], "paid_amount": [1.0]}
        )
        assert DataPlugins().get_patient_medical_lines("A", medical_lf=lf) is None

    @pytest.mark.unit
    def test_medical_lines_loads_default(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame(
            {"person_id": ["A"], "claim_start_date": [date(2024, 1, 1)], "paid_amount": [1.0]}
        )
        _write_parquet(tmp_path / "gold" / "medical_claim.parquet", df)
        assert plugin.get_patient_medical_lines("A").height == 1

    @pytest.mark.unit
    def test_pharmacy_lines_empty(self) -> None:
        assert DataPlugins().get_patient_pharmacy_lines("") is None

    @pytest.mark.unit
    def test_pharmacy_lines_match(self) -> None:
        lf = pl.LazyFrame(
            {
                "person_id": ["A"],
                "dispensing_date": [date(2024, 1, 1)],
                "paid_amount": [5.0],
            }
        )
        df = DataPlugins().get_patient_pharmacy_lines("A", pharmacy_lf=lf)
        assert df is not None and df.height == 1

    @pytest.mark.unit
    def test_pharmacy_lines_no_match(self) -> None:
        lf = pl.LazyFrame(
            {"person_id": ["X"], "dispensing_date": [date(2024, 1, 1)], "paid_amount": [5.0]}
        )
        assert DataPlugins().get_patient_pharmacy_lines("A", pharmacy_lf=lf) is None

    @pytest.mark.unit
    def test_pharmacy_lines_loads_default(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame(
            {"person_id": ["A"], "dispensing_date": [date(2024, 1, 1)], "paid_amount": [5.0]}
        )
        _write_parquet(tmp_path / "gold" / "pharmacy_claim.parquet", df)
        assert plugin.get_patient_pharmacy_lines("A").height == 1


def _medical_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "person_id": ["A", "A", "A", "A"],
            "claim_start_date": [
                date(2024, 1, 5),
                date(2024, 6, 1),
                date(2023, 4, 1),
                date(2023, 9, 1),
            ],
            "bill_type_code": ["111", "13X", None, "32X"],
            "paid_amount": [1000.0, 200.0, 50.0, 75.0],
            "revenue_center_code": ["0450", "0100", "0200", "0100"],
            "place_of_service_code": ["21", "11", "23", "11"],
            "hcpcs_code": ["99213", "G0438", None, "99281"],
        }
    )


def _pharmacy_lf() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "person_id": ["A", "A", "B"],
            "dispensing_date": [date(2024, 2, 1), date(2024, 5, 1), date(2024, 6, 1)],
            "paid_amount": [10.0, 20.0, 30.0],
        }
    )


class TestYearlySpendAndUtilization:
    @pytest.mark.unit
    def test_empty(self) -> None:
        empty_med = pl.LazyFrame(
            {
                "person_id": pl.Series([], dtype=pl.Utf8),
                "claim_start_date": pl.Series([], dtype=pl.Date),
                "bill_type_code": pl.Series([], dtype=pl.Utf8),
                "paid_amount": pl.Series([], dtype=pl.Float64),
                "revenue_center_code": pl.Series([], dtype=pl.Utf8),
                "place_of_service_code": pl.Series([], dtype=pl.Utf8),
                "hcpcs_code": pl.Series([], dtype=pl.Utf8),
            }
        )
        empty_rx = pl.LazyFrame(
            {
                "person_id": pl.Series([], dtype=pl.Utf8),
                "dispensing_date": pl.Series([], dtype=pl.Date),
                "paid_amount": pl.Series([], dtype=pl.Float64),
            }
        )
        out = DataPlugins().get_yearly_spend_and_utilization(
            "ZZZ", medical_lf=empty_med, pharmacy_lf=empty_rx
        )
        assert out.height == 0

    @pytest.mark.unit
    def test_combined_years(self) -> None:
        plugin = DataPlugins()
        out = plugin.get_yearly_spend_and_utilization("A", _medical_lf(), _pharmacy_lf())
        assert set(out["year"].to_list()) == {2023, 2024}
        assert out.filter(pl.col("year") == 2024)["inpatient_spend"].item() == 1000.0
        assert out.filter(pl.col("year") == 2024)["awv_visits"].item() == 1
        assert out.filter(pl.col("year") == 2024)["er_visits"].item() == 1
        assert out.filter(pl.col("year") == 2024)["pharmacy_spend"].item() == 30.0
        assert out.filter(pl.col("year") == 2024)["total_spend"].item() == 1230.0
        # 2023 has medical claims but no pharmacy fills.
        row23 = out.filter(pl.col("year") == 2023).to_dicts()[0]
        assert row23["pharmacy_spend"] == 0
        assert row23["pharmacy_claims_count"] == 0

    @pytest.mark.unit
    def test_pharmacy_only(self) -> None:
        empty_med = pl.LazyFrame(
            {
                "person_id": pl.Series([], dtype=pl.Utf8),
                "claim_start_date": pl.Series([], dtype=pl.Date),
                "bill_type_code": pl.Series([], dtype=pl.Utf8),
                "paid_amount": pl.Series([], dtype=pl.Float64),
                "revenue_center_code": pl.Series([], dtype=pl.Utf8),
                "place_of_service_code": pl.Series([], dtype=pl.Utf8),
                "hcpcs_code": pl.Series([], dtype=pl.Utf8),
            }
        )
        out = DataPlugins().get_yearly_spend_and_utilization(
            "A", medical_lf=empty_med, pharmacy_lf=_pharmacy_lf()
        )
        assert out.height == 1
        assert out["pharmacy_spend"].item() == 30.0
        assert out["inpatient_spend"].item() == 0.0

    @pytest.mark.unit
    def test_medical_only(self) -> None:
        empty_rx = pl.LazyFrame(
            {
                "person_id": pl.Series([], dtype=pl.Utf8),
                "dispensing_date": pl.Series([], dtype=pl.Date),
                "paid_amount": pl.Series([], dtype=pl.Float64),
            }
        )
        out = DataPlugins().get_yearly_spend_and_utilization(
            "A", medical_lf=_medical_lf(), pharmacy_lf=empty_rx
        )
        assert out.height == 2
        assert all(v == 0 for v in out["pharmacy_spend"].to_list())

    @pytest.mark.unit
    def test_loads_defaults(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        med = _medical_lf().collect()
        rx = _pharmacy_lf().collect()
        _write_parquet(tmp_path / "gold" / "medical_claim.parquet", med)
        _write_parquet(tmp_path / "gold" / "pharmacy_claim.parquet", rx)
        out = plugin.get_yearly_spend_and_utilization("A")
        assert out.height == 2


class TestSearchDefaultLazyframes:
    """Default-load paths for get_member_eligibility / claims helpers."""

    @pytest.mark.unit
    def test_eligibility_loads_default(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        df = pl.DataFrame(
            {col: [None] for col in [
                "person_id", "member_id", "subscriber_id", "gender", "race",
                "birth_date", "death_date", "death_flag",
                "enrollment_start_date", "enrollment_end_date",
                "payer", "payer_type", "plan",
            ]}
        ).with_columns(pl.col("member_id").fill_null("M1"))
        _write_parquet(tmp_path / "gold" / "eligibility.parquet", df)
        out = plugin.get_member_eligibility(["M1"])
        assert out is not None and out.height == 1

    @pytest.mark.unit
    def test_medical_claims_loads_default(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        cols = {col: [None] for col in [
            "claim_id", "claim_line_number", "claim_type", "member_id", "person_id",
            "claim_start_date", "claim_end_date", "claim_line_start_date",
            "claim_line_end_date", "admission_date", "discharge_date",
            "place_of_service_code", "bill_type_code", "revenue_center_code",
            "hcpcs_code", "hcpcs_modifier_1", "hcpcs_modifier_2",
            "rendering_npi", "rendering_tin", "billing_npi", "billing_tin",
            "facility_npi", "paid_amount", "allowed_amount", "charge_amount",
            "diagnosis_code_1", "diagnosis_code_2", "diagnosis_code_3",
        ]}
        df = pl.DataFrame(cols).with_columns(
            pl.col("member_id").fill_null("M1"),
            pl.col("claim_start_date").cast(pl.Date),
        )
        _write_parquet(tmp_path / "gold" / "medical_claim.parquet", df)
        out = plugin.get_medical_claims({"member_ids": ["M1"]})
        assert out is not None and out.height == 1

    @pytest.mark.unit
    def test_pharmacy_claims_loads_default(self, tmp_path: Path) -> None:
        plugin, _ = _patched_data(tmp_path)
        cols = {col: [None] for col in [
            "claim_id", "claim_line_number", "member_id", "person_id",
            "dispensing_date", "ndc_code", "prescribing_provider_npi",
            "dispensing_provider_npi", "quantity", "days_supply", "refills",
            "paid_date", "paid_amount", "allowed_amount", "charge_amount",
            "coinsurance_amount", "copayment_amount", "deductible_amount",
            "in_network_flag",
        ]}
        df = pl.DataFrame(cols).with_columns(
            pl.col("member_id").fill_null("M1"),
            pl.col("dispensing_date").cast(pl.Date),
        )
        _write_parquet(tmp_path / "gold" / "pharmacy_claim.parquet", df)
        out = plugin.get_pharmacy_claims(["M1"])
        assert out is not None and out.height == 1
