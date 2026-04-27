# © 2025 HarmonyCares
"""Tests for acoharmony._notes._gift_card (GiftCardPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._notes import GiftCardPlugins


def _gcm_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["M1", "M2", "M3"],
            "hcmpi": ["HC1", "HC2", "HC3"],
            "gift_card_status": ["send_gift_card", "sent", None],
            "awv_date": ["2024-06-01", "2024-05-01", None],
            "awv_status": ["completed", "completed", "pending"],
            "patientaddress": ["1 Main St", "", None],
            "patientaddress2": [None, None, None],
            "patientcity": ["Detroit", "", None],
            "patientstate": ["MI", "MI", "MI"],
            "patientzip": ["48226", "", None],
            "roll12_awv_enc": [1.0, 0.0, 0.0],
            "roll12_em": [3.0, 1.0, 0.0],
            "lc_status_current": ["active"] * 3,
            "payer": ["MA"] * 3,
            "payer_current": ["MA"] * 3,
            "total_count": [1] * 3,
        }
    )


def _bar_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "bene_mbi": ["M1", "M2"],
            "bene_date_of_death": [None, None],
            "start_date": [date(2024, 1, 1)] * 2,
            "end_date": [None, None],
            "bene_first_name": ["A", "B"],
            "bene_last_name": ["X", "Y"],
            "bene_address_line_1": ["1 Main St", "2 Oak St"],
            "bene_address_line_2": [None, None],
            "bene_city": ["Detroit", "Lansing"],
            "bene_state": ["MI", "MI"],
            "bene_zip_5": ["48226", "48933"],
        }
    )


def _hdai_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["M1", "M2"],
            "patient_dod": [None, None],
            "most_recent_awv_date": [date(2024, 6, 1), date(2024, 4, 1)],
            "enrollment_status": ["active", "active"],
            "last_em_visit": [date(2024, 5, 1), date(2024, 3, 1)],
            "patient_first_name": ["A", "B"],
            "patient_last_name": ["X", "Y"],
            "patient_address": ["1 Main St", "2 Oak St"],
            "patient_city": ["Detroit", "Lansing"],
            "patient_state": ["MI", "MI"],
            "patient_zip": ["48226", "48933"],
        }
    )


# ---------------------------------------------------------------------------
# get_most_recent_data
# ---------------------------------------------------------------------------


class TestGetMostRecentData:
    @pytest.mark.unit
    def test_empty(self) -> None:
        assert GiftCardPlugins().get_most_recent_data(pl.LazyFrame()).collect().is_empty()

    @pytest.mark.unit
    def test_dedupes_by_mbi(self) -> None:
        lf = pl.LazyFrame({"mbi": ["M1", "M1"], "x": [1, 2]})
        out = GiftCardPlugins().get_most_recent_data(lf).collect()
        assert out.height == 1

    @pytest.mark.unit
    def test_filter_by_file_date(self) -> None:
        lf = pl.LazyFrame(
            {
                "mbi": ["M1", "M2"],
                "file_date": [date(2024, 1, 1), date(2024, 6, 1)],
            }
        )
        out = GiftCardPlugins().get_most_recent_data(lf).collect()
        assert out["mbi"].to_list() == ["M2"]

    @pytest.mark.unit
    def test_processed_at_dedup_keeps_newest(self) -> None:
        lf = pl.LazyFrame(
            {
                "mbi": ["M1", "M1"],
                "processed_at": [date(2024, 1, 1), date(2024, 6, 1)],
                "x": [10, 20],
            }
        )
        out = GiftCardPlugins().get_most_recent_data(lf).collect()
        assert out["x"].to_list() == [20]


# ---------------------------------------------------------------------------
# Source loaders + overlap
# ---------------------------------------------------------------------------


class TestLoadSources:
    @pytest.mark.unit
    def test_missing_files(self, tmp_path: Path) -> None:
        out = GiftCardPlugins().load_sources(tmp_path)
        assert out["gcm"].collect().is_empty()
        assert out["bar"].collect().is_empty()
        assert out["hdai"].collect().is_empty()

    @pytest.mark.unit
    def test_with_files(self, tmp_path: Path) -> None:
        _gcm_df().write_parquet(tmp_path / "gcm.parquet")
        # BAR with extra cols required by current_reach expr
        bar = _bar_df().with_columns(
            pl.lit(date(2024, 6, 1)).alias("file_date"),
            pl.lit("ALGC25_2024-06.txt").alias("source_filename"),
        )
        bar.write_parquet(tmp_path / "bar.parquet")
        _hdai_df().with_columns(pl.lit(date(2024, 6, 1)).alias("file_date")).write_parquet(
            tmp_path / "hdai_reach.parquet"
        )
        with patch(
            "acoharmony._expressions._current_reach.build_current_reach_with_bar_expr",
            return_value=pl.lit(True),
        ):
            out = GiftCardPlugins().load_sources(tmp_path)
        assert out["gcm"].collect().height == 3


class TestDataOverview:
    @pytest.mark.unit
    def test_returns_counts(self) -> None:
        out = GiftCardPlugins().data_overview(
            _gcm_df().lazy(), _bar_df().lazy(), _hdai_df().lazy()
        )
        assert out["Record Count"].to_list() == [3, 2, 2]


class TestSourceOverlap:
    @pytest.mark.unit
    def test_overlap(self) -> None:
        out = GiftCardPlugins().source_overlap(
            _gcm_df().lazy(), _bar_df().lazy(), _hdai_df().lazy()
        )
        as_dict = {row["Category"]: row["Count"] for row in out.iter_rows(named=True)}
        # GCM has M1,M2,M3; BAR has M1,M2 → M3 not on BAR
        assert as_dict["GCM patients NOT on latest BAR"] == 1


class TestGiftCardStatusDist:
    @pytest.mark.unit
    def test_dist(self) -> None:
        out = GiftCardPlugins().gift_card_status_dist(_gcm_df().lazy())
        assert out.height == 3


class TestDeceasedCheck:
    @pytest.mark.unit
    def test_no_deceased(self) -> None:
        summary, deceased = GiftCardPlugins().deceased_check(
            _gcm_df().lazy(), _bar_df().lazy(), _hdai_df().lazy()
        )
        assert summary["Count"][1] == 0
        assert deceased.is_empty()

    @pytest.mark.unit
    def test_with_deceased(self) -> None:
        bar = _bar_df().with_columns(
            pl.Series("bene_date_of_death", [date(2024, 1, 1), None])
        )
        summary, deceased = GiftCardPlugins().deceased_check(
            _gcm_df().lazy(), bar.lazy(), _hdai_df().lazy()
        )
        assert summary["Count"][1] == 1
        assert deceased.height == 1


class TestAwvComparison:
    @pytest.mark.unit
    def test_comparison(self) -> None:
        summary, mismatches = GiftCardPlugins().awv_comparison(
            _gcm_df().lazy(), _bar_df().lazy(), _hdai_df().lazy()
        )
        # M1 GCM 2024-06-01 == HDAI 2024-06-01 → match
        # M2 GCM 2024-05-01 != HDAI 2024-04-01 → differ
        assert summary["Count"][2] == 1  # match
        assert summary["Count"][3] == 1  # differ
        assert mismatches.height == 1


class TestMissingAddresses:
    @pytest.mark.unit
    def test_missing(self) -> None:
        summary, missing = GiftCardPlugins().missing_addresses(_gcm_df().lazy())
        # Two rows missing (empty + null)
        assert summary["Count"][1] == 2
        assert missing.height == 2


class TestVisitMetrics:
    @pytest.mark.unit
    def test_metrics(self) -> None:
        out = GiftCardPlugins().visit_metrics(_gcm_df().lazy())
        assert "Avg AWV Visits" in out.columns


# ---------------------------------------------------------------------------
# Medical claim AWV
# ---------------------------------------------------------------------------


class TestMedicalClaimAwv:
    @pytest.mark.unit
    def test_missing_returns_empty(self, tmp_path: Path) -> None:
        out = GiftCardPlugins().medical_claim_awv(tmp_path).collect()
        assert out.is_empty()

    @pytest.mark.unit
    def test_filters_to_awv_codes(self, tmp_path: Path) -> None:
        pl.DataFrame(
            {
                "member_id": ["M1", "M1", "M2"],
                "hcpcs_code": ["G0438", "OTHER", "G0439"],
                "claim_line_start_date": [
                    date(2025, 6, 1),
                    date(2025, 7, 1),
                    date(2025, 5, 1),
                ],
                "claim_id": ["C1", "C2", "C3"],
                "source_filename": ["s1", "s2", "s3"],
            }
        ).write_parquet(tmp_path / "medical_claim.parquet")
        out = GiftCardPlugins().medical_claim_awv(tmp_path).collect()
        assert out.height == 2  # only AWV codes


# ---------------------------------------------------------------------------
# Three-source comparison
# ---------------------------------------------------------------------------


class TestThreeSourceComparison:
    @pytest.mark.unit
    def test_basic(self) -> None:
        claim_lf = pl.LazyFrame(
            {
                "mbi": ["M1"],
                "claim_awv_date": [date(2024, 6, 1)],
                "claim_awv_code": ["G0438"],
                "claim_id": ["C1"],
                "claim_source_date": ["src"],
            }
        )
        summary, disagree = GiftCardPlugins().three_source_comparison(
            _gcm_df().lazy(), _bar_df().lazy(), _hdai_df().lazy(), claim_lf
        )
        assert summary.height == 8
        assert "Total Current REACH BAR" in summary["Metric"].to_list()


# ---------------------------------------------------------------------------
# Address parsing
# ---------------------------------------------------------------------------


class TestParseAddress:
    @pytest.mark.unit
    def test_empty(self) -> None:
        assert GiftCardPlugins().parse_address("") == ("", None)

    @pytest.mark.unit
    def test_simple_with_usaddress(self) -> None:
        # Mock usaddress.tag to return the parsed components
        fake_module = type("M", (), {})()
        fake_module.tag = lambda s: (
            {
                "AddressNumber": "123",
                "StreetName": "Main",
                "StreetNamePostType": "St",
                "OccupancyType": "Apt",
                "OccupancyIdentifier": "4",
            },
            "Street Address",
        )
        with patch.dict("sys.modules", {"usaddress": fake_module}):
            street, apt = GiftCardPlugins().parse_address("123 Main St Apt 4")
        assert "123" in street
        assert apt is not None and "4" in apt

    @pytest.mark.unit
    def test_failure_returns_input(self) -> None:
        # No usaddress module → ImportError caught; returns input
        out = GiftCardPlugins().parse_address("garbage")
        assert out == ("garbage", None)


# ---------------------------------------------------------------------------
# Address enrichment + mailing list
# ---------------------------------------------------------------------------


class TestEnrichGcmAddresses:
    @pytest.mark.unit
    def test_fills_missing(self, tmp_path: Path) -> None:
        _gcm_df().write_parquet(tmp_path / "gcm.parquet")
        cclf8 = pl.DataFrame(
            {
                "bene_mbi_id": ["M2", "M3"],
                "file_date": [date(2024, 6, 1), date(2024, 6, 1)],
                "bene_line_1_adr": ["2 Oak St", "3 Pine St"],
                "bene_line_2_adr": [None, None],
                "bene_city": ["Lansing", "Ann Arbor"],
                "bene_state": ["MI", "MI"],
                "bene_zip": ["48933", "48104"],
            }
        )
        cclf8.write_parquet(tmp_path / "cclf8.parquet")
        out = GiftCardPlugins().enrich_gcm_addresses(tmp_path)
        m2 = next(r for r in out.iter_rows(named=True) if r["mbi"] == "M2")
        # Was empty in GCM → filled from CCLF8
        assert m2["patientaddress_enriched"] == "2 Oak St"


class TestApplyAddressParsing:
    @pytest.mark.unit
    def test_with_addr2_skipped(self) -> None:
        df = pl.DataFrame(
            {
                "address_line_1": ["123 Main St"],
                "address_line_2": ["Apt 5"],
            }
        )
        out = GiftCardPlugins()._apply_address_parsing(df)
        assert out["address_line_1"][0] == "123 Main St"
        assert out["address_line_2"][0] == "Apt 5"

    @pytest.mark.unit
    def test_null_addr1_passthrough(self) -> None:
        df = pl.DataFrame(
            {
                "address_line_1": [None],
                "address_line_2": [None],
            },
            schema={"address_line_1": pl.Utf8, "address_line_2": pl.Utf8},
        )
        out = GiftCardPlugins()._apply_address_parsing(df)
        assert out["address_line_1"][0] is None


class TestAnalyzeDuplicates:
    @pytest.mark.unit
    def test_no_duplicates(self) -> None:
        df = pl.DataFrame(
            {
                "hcmpi": ["HC1"],
                "mbi": ["M1"],
                "first_name": ["A"],
                "last_name": ["X"],
                "address_line_1": ["1 Main"],
                "city": ["Detroit"],
            }
        )
        summary, details = GiftCardPlugins().analyze_duplicates(df)
        assert details.is_empty()
        # "0" duplicate count
        assert summary.filter(pl.col("Metric").str.contains("Total Duplicate"))["Value"][0] == "0"

    @pytest.mark.unit
    def test_with_duplicates(self) -> None:
        df = pl.DataFrame(
            {
                "hcmpi": ["HC1", "HC1"],
                "mbi": ["M1", "M1"],
                "first_name": ["A", "A"],
                "last_name": ["X", "X"],
                "address_line_1": ["1 Main", "1 Main"],
                "city": ["Detroit", "Lansing"],
            }
        )
        summary, details = GiftCardPlugins().analyze_duplicates(df)
        assert details.height == 2


class TestFormatDirectDelivery:
    @pytest.mark.unit
    def test_output_columns(self) -> None:
        df = pl.DataFrame(
            {
                "first_name": ["A"],
                "last_name": ["X"],
                "mbi": ["M1"],
                "address_line_1": ["1 Main"],
                "address_line_2": [None],
                "city": ["Detroit"],
                "state": ["MI"],
                "zip": ["48226"],
            }
        )
        out = GiftCardPlugins().format_direct_delivery(df)
        assert "First Name" in out.columns
        assert out["Card Name"][0] == "Visa"


class TestMailingListSummary:
    @pytest.mark.unit
    def test_summary(self) -> None:
        df = pl.DataFrame(
            {
                "gcm_awv_date": ["2024-06-01", None],
                "hdai_awv_date": [date(2024, 6, 1), None],
                "gift_card_status": [None, "sent"],
            }
        )
        out = GiftCardPlugins().mailing_list_summary(df)
        as_dict = {row["Metric"]: row["Count"] for row in out.iter_rows(named=True)}
        assert as_dict["Total Eligible for Mailing"] == 2
        assert as_dict["Has AWV Date in GCM"] == 1
