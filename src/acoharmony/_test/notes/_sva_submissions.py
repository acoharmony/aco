# © 2025 HarmonyCares
"""Tests for acoharmony._notes._sva_submissions (SvaSubmissionsPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from acoharmony._notes import SvaSubmissionsPlugins


def _submissions_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "sva_id": ["S1", "S2", "S3", "S4"],
            "mbi": ["M1", "M2", "M3", "M1"],
            "beneficiary_first_name": ["A", "B", "C", "A"],
            "beneficiary_last_name": ["X", "Y", "Z", "X"],
            "signature_date": [
                "2024-06-01",
                "2024-06-15",
                None,
                "2024-06-20",
            ],
            "created_at": [
                "June 01, 2024, 09:00 AM",
                "June 15, 2024, 10:00 AM",
                "June 20, 2024, 11:00 AM",
                "June 25, 2024, 12:00 PM",
            ],
            "practice_name": ["P1", "P2", "P1", "P1"],
            "provider_name": ["Doc1", "Doc2", "Doc3", "Doc1"],
            "provider_npi": ["N1", "N2", "N3", "N1"],
            "tin": ["T1", "T2", "T3", "T1"],
            "city": ["Detroit"] * 4,
            "state": ["MI"] * 4,
            "zip": ["48226"] * 4,
            "address_primary_line": ["1 Main St"] * 4,
            "submission_source": ["EarthClassMail"] * 4,
            "transcriber_notes": [None, "deceased", None, None],
            "status": ["Completed", "Invalid", "Invalid", "Completed"],
            "network_id": ["NET1"] * 4,
            "file_date": [date(2024, 7, 1)] * 4,
        }
    )


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


class TestLoadSources:
    @pytest.mark.unit
    def test_missing_files(self, tmp_path: Path) -> None:
        out = SvaSubmissionsPlugins().load_sources(tmp_path)
        assert out["submissions"].is_empty()
        assert out["palmr"].is_empty()

    @pytest.mark.unit
    def test_with_files(self, tmp_path: Path) -> None:
        _submissions_df().write_parquet(tmp_path / "sva_submissions.parquet")
        out = SvaSubmissionsPlugins().load_sources(tmp_path)
        assert out["submissions"].height == 4

    @pytest.mark.unit
    def test_palmr_no_file_date_col(self, tmp_path: Path) -> None:
        pl.DataFrame({"prvdr_npi": ["N1"], "prvdr_tin": ["T1"], "bene_mbi": ["M1"]}).write_parquet(
            tmp_path / "palmr.parquet"
        )
        out = SvaSubmissionsPlugins().load_sources(tmp_path)
        assert out["palmr"].height == 1


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------


class TestDateHelpers:
    @pytest.mark.unit
    def test_default_with_sva(self) -> None:
        sva = pl.DataFrame({"file_date": ["2024-06-30"]})
        start, end = SvaSubmissionsPlugins().default_date_range(sva)
        assert start == date(2024, 7, 1)

    @pytest.mark.unit
    def test_default_no_sva(self) -> None:
        start, end = SvaSubmissionsPlugins().default_date_range(pl.DataFrame())
        assert start == date(2025, 11, 1)

    @pytest.mark.unit
    def test_parse_dates(self) -> None:
        out = SvaSubmissionsPlugins().parse_dates(_submissions_df())
        assert "created_date" in out.columns
        assert out["created_date"][0] == date(2024, 6, 1)

    @pytest.mark.unit
    def test_filter_date_range(self) -> None:
        plugin = SvaSubmissionsPlugins()
        df = plugin.parse_dates(_submissions_df())
        out = plugin.filter_date_range(df, date(2024, 6, 10), date(2024, 6, 30))
        assert out.height == 3


# ---------------------------------------------------------------------------
# Pipeline transformations
# ---------------------------------------------------------------------------


class TestPipelineTransforms:
    @pytest.mark.unit
    def test_identify_exclusions(self) -> None:
        plugin = SvaSubmissionsPlugins()
        df = plugin.parse_dates(_submissions_df())
        out, patterns = plugin.identify_exclusions(df)
        assert "should_exclude" in out.columns
        assert int(out["should_exclude"].sum()) == 1

    @pytest.mark.unit
    def test_flag_duplicate_completed(self) -> None:
        plugin = SvaSubmissionsPlugins()
        df = plugin.parse_dates(_submissions_df())
        flagged = plugin.flag_duplicate_completed(df, df)
        # M1 has Completed → all M1 rows flagged True; M2, M3 have only Invalid → False
        assert flagged.filter(pl.col("mbi") == "M1")["has_completed_sva"].all()

    @pytest.mark.unit
    def test_invalid_export(self) -> None:
        plugin = SvaSubmissionsPlugins()
        df = plugin.parse_dates(_submissions_df())
        df, _ = plugin.identify_exclusions(df)
        df = plugin.flag_duplicate_completed(df, df)
        out = plugin.invalid_export(df)
        # M3 is the only Invalid that's NOT excluded and has no completed SVA elsewhere
        assert out.height == 1
        assert out["mbi"][0] == "M3"

    @pytest.mark.unit
    def test_apply_pipeline(self) -> None:
        plugin = SvaSubmissionsPlugins()
        out = plugin.apply_pipeline(
            _submissions_df(),
            pl.DataFrame(),
            pl.DataFrame(),
            pl.DataFrame(),
            pl.DataFrame(),
            date(2024, 1, 1),
            date(2024, 12, 31),
        )
        assert "filtered" in out
        assert "valid" in out


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------


class TestEnrichValidRecords:
    @pytest.mark.unit
    def test_with_xwalk(self) -> None:
        plugin = SvaSubmissionsPlugins()
        df = pl.DataFrame(
            {
                "mbi": ["M1"],
                "tin": ["T1"],
                "provider_npi": ["N1"],
            }
        )
        xwalk = pl.DataFrame({"prvs_num": ["M1"], "crnt_num": ["M1_NEW"]})
        out = plugin.enrich_valid_records(df, xwalk, pl.DataFrame(), pl.DataFrame())
        assert out["crosswalk_mbi"][0] == "M1_NEW"
        assert out["tin_npi_match"][0] is False

    @pytest.mark.unit
    def test_with_participants(self) -> None:
        plugin = SvaSubmissionsPlugins()
        df = pl.DataFrame(
            {"mbi": ["M1"], "tin": ["T1"], "provider_npi": ["N1"]}
        )
        participants = pl.DataFrame(
            {"base_provider_tin": ["T1"], "individual_npi": ["N1"]}
        )
        out = plugin.enrich_valid_records(df, pl.DataFrame(), participants, pl.DataFrame())
        assert out["tin_npi_match"][0] is True

    @pytest.mark.unit
    def test_with_bar(self) -> None:
        plugin = SvaSubmissionsPlugins()
        df = pl.DataFrame(
            {"mbi": ["M1"], "tin": ["T1"], "provider_npi": ["N1"]}
        )
        bar = pl.DataFrame({"bene_mbi": ["M1"], "end_date": [None]})
        out = plugin.enrich_valid_records(df, pl.DataFrame(), pl.DataFrame(), bar, today=date(2024, 6, 1))
        assert out["current_bar_status"][0] == "Active"


# ---------------------------------------------------------------------------
# Summary stats
# ---------------------------------------------------------------------------


class TestSummaryStats:
    @pytest.mark.unit
    def test_summary(self) -> None:
        plugin = SvaSubmissionsPlugins()
        out = plugin.apply_pipeline(
            _submissions_df(),
            pl.DataFrame(),
            pl.DataFrame(),
            pl.DataFrame(),
            pl.DataFrame(),
            date(2024, 1, 1),
            date(2024, 12, 31),
        )
        stats = plugin.summary_stats(
            _submissions_df(),
            out["filtered"],
            out["flagged"],
            out["invalid"],
            out["valid"],
        )
        assert stats["total_all_time"] == 4

    @pytest.mark.unit
    def test_exclusion_breakdown(self) -> None:
        plugin = SvaSubmissionsPlugins()
        out = plugin.apply_pipeline(
            _submissions_df(),
            pl.DataFrame(),
            pl.DataFrame(),
            pl.DataFrame(),
            pl.DataFrame(),
            date(2024, 1, 1),
            date(2024, 12, 31),
        )
        breakdown, excluded = plugin.exclusion_breakdown(
            out["flagged"], out["exclusion_patterns"]
        )
        assert "Reason" in breakdown.columns
        assert excluded.height == 1

    @pytest.mark.unit
    def test_invalid_with_completed(self) -> None:
        plugin = SvaSubmissionsPlugins()
        out = plugin.apply_pipeline(
            _submissions_df(),
            pl.DataFrame(),
            pl.DataFrame(),
            pl.DataFrame(),
            pl.DataFrame(),
            date(2024, 1, 1),
            date(2024, 12, 31),
        )
        result = plugin.invalid_with_completed(out["flagged"])
        # No invalid M1 rows (M1 is completed) → empty
        assert result.is_empty()


# ---------------------------------------------------------------------------
# PALMR + provider enrichment
# ---------------------------------------------------------------------------


class TestPalmrPanel:
    @pytest.mark.unit
    def test_empty(self) -> None:
        out = SvaSubmissionsPlugins().palmr_panel_distribution(
            pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
        )
        assert out["available"] is False

    @pytest.mark.unit
    def test_with_data(self) -> None:
        palmr = pl.DataFrame(
            {
                "prvdr_npi": ["N1", "N1", "N2"],
                "prvdr_tin": ["T1", "T1", "T2"],
                "bene_mbi": ["M1", "M2", "M3"],
                "file_date": [date(2024, 6, 1)] * 3,
            }
        )
        out = SvaSubmissionsPlugins().palmr_panel_distribution(
            palmr, pl.DataFrame(), pl.DataFrame()
        )
        assert out["available"] is True
        assert out["total_npis"] == 2

    @pytest.mark.unit
    def test_with_bar_and_participants(self) -> None:
        palmr = pl.DataFrame(
            {
                "prvdr_npi": ["N1"],
                "prvdr_tin": ["T1"],
                "bene_mbi": ["M1"],
                "file_date": [date(2024, 6, 1)],
            }
        )
        bar = pl.DataFrame({"bene_mbi": ["M1"], "bene_state": ["MI"]})
        participants = pl.DataFrame(
            {
                "individual_npi": ["N1"],
                "base_provider_tin": ["T1"],
                "individual_first_name": ["First"],
                "individual_last_name": ["Last"],
            }
        )
        out = SvaSubmissionsPlugins().palmr_panel_distribution(palmr, bar, participants)
        assert out["state_by_provider"].height == 1


class TestEnrichProvidersWithStates:
    @pytest.mark.unit
    def test_empty(self) -> None:
        out = SvaSubmissionsPlugins().enrich_providers_with_states(
            pl.DataFrame(), pl.DataFrame()
        )
        assert out.is_empty()

    @pytest.mark.unit
    def test_with_data(self) -> None:
        palmr = pl.DataFrame(
            {
                "prvdr_npi": ["1376709493"],
                "prvdr_tin": ["T1"],
                "bene_mbi": ["M1"],
            }
        )
        participants = pl.DataFrame(
            {
                "individual_npi": ["1376709493", "1376709493"],
                "base_provider_tin": ["T1", "T1"],
                "individual_first_name": ["Rupen"] * 2,
                "individual_last_name": ["Amin"] * 2,
                "state_cd": ["NJ", "NJ"],
                "provider_type": ["Individual Practitioner", "Individual Provider"],
                "effective_start_date": [None, "2024-01-01"],
            }
        )
        out = SvaSubmissionsPlugins().enrich_providers_with_states(palmr, participants)
        assert out.height == 1


class TestPanelBalancing:
    @pytest.mark.unit
    def test_empty(self) -> None:
        out = SvaSubmissionsPlugins().panel_balancing_recommendations(
            pl.DataFrame(), pl.DataFrame(), set(), pl.DataFrame()
        )
        assert out == {}

    @pytest.mark.unit
    def test_with_data(self) -> None:
        valid = pl.DataFrame(
            {
                "mbi": ["M1"],
                "state": ["NJ"],
                "signature_date_parsed": [date(2024, 6, 1)],
            }
        )
        providers = pl.DataFrame(
            {
                "prvdr_npi": ["1376709493"],
                "prvdr_tin": ["T1"],
                "provider_state": ["NJ"],
                "provider_name": ["Rupen"],
                "earliest_effective_date": [None],
                "current_panel": [10],
            }
        )
        bar = pl.DataFrame({"bene_mbi": ["M1"], "bene_state": ["NJ"]})
        selected = {("1376709493", "T1", "NJ")}
        out = SvaSubmissionsPlugins().panel_balancing_recommendations(
            valid, providers, selected, bar
        )
        assert out["M1"]["npi"] == "1376709493"


class TestCmsExport:
    @pytest.mark.unit
    def test_empty(self) -> None:
        out = SvaSubmissionsPlugins().cms_sva_export(
            pl.DataFrame(), {}, pl.DataFrame(), pl.DataFrame()
        )
        assert out.is_empty()

    @pytest.mark.unit
    def test_with_data(self) -> None:
        valid = pl.DataFrame(
            {
                "mbi": ["M1"],
                "beneficiary_first_name": ["A"],
                "beneficiary_last_name": ["X"],
                "address_primary_line": ["1 Main"],
                "city": ["Detroit"],
                "state": ["MI"],
                "zip": ["48226"],
                "provider_name": ["Doc"],
                "provider_npi": ["N1"],
                "tin": ["T1"],
                "signature_date_parsed": [date(2024, 6, 1)],
            }
        )
        out = SvaSubmissionsPlugins().cms_sva_export(
            valid, {}, pl.DataFrame(), pl.DataFrame(), today=date(2024, 7, 1)
        )
        assert out.height == 1
        assert out["aco_id"][0] == "D0259"


# ---------------------------------------------------------------------------
# Snapshot diff + duplicate names
# ---------------------------------------------------------------------------


class TestSnapshotDiff:
    @pytest.mark.unit
    def test_empty(self) -> None:
        assert SvaSubmissionsPlugins().snapshot_diff(pl.DataFrame()) is None

    @pytest.mark.unit
    def test_only_one_snapshot(self) -> None:
        df = pl.DataFrame(
            {"mbi": ["M1"], "file_date": [date(2024, 6, 1)]}
        )
        out = SvaSubmissionsPlugins().snapshot_diff(df)
        assert out["available"] is False
        assert out["snapshot_count"] == 1

    @pytest.mark.unit
    def test_two_snapshots(self) -> None:
        df = pl.DataFrame(
            {
                "mbi": ["M1", "M2", "M1", "M3"],
                "beneficiary_first_name": ["A", "B", "A", "C"],
                "beneficiary_last_name": ["X", "Y", "X", "Z"],
                "status": ["Completed", "Completed", "Invalid", "Completed"],
                "created_at": ["2024-06-01"] * 4,
                "file_date": [
                    date(2024, 6, 1),
                    date(2024, 6, 1),
                    date(2024, 7, 1),
                    date(2024, 7, 1),
                ],
            }
        )
        out = SvaSubmissionsPlugins().snapshot_diff(df)
        assert out["available"] is True
        # M3 added (only in 7/1)
        assert out["new"].height == 1
        # M2 removed (only in 6/1)
        assert out["removed"].height == 1
        # M1 status changed
        assert out["status_changes"].height == 1


class TestDuplicateNames:
    @pytest.mark.unit
    def test_empty(self) -> None:
        out = SvaSubmissionsPlugins().duplicate_names(pl.DataFrame())
        assert out["available"] is False

    @pytest.mark.unit
    def test_no_duplicates(self) -> None:
        df = pl.DataFrame(
            {
                "beneficiary_first_name": ["A", "B"],
                "beneficiary_last_name": ["X", "Y"],
                "mbi": ["M1", "M2"],
                "status": ["Completed"] * 2,
                "created_at": ["2024-06-01"] * 2,
                "provider_name": ["Doc"] * 2,
                "provider_npi": ["N1", "N2"],
                "tin": ["T1", "T2"],
            }
        )
        out = SvaSubmissionsPlugins().duplicate_names(df)
        assert out["records"].is_empty()

    @pytest.mark.unit
    def test_with_duplicates(self) -> None:
        df = pl.DataFrame(
            {
                "beneficiary_first_name": ["A", "A"],
                "beneficiary_last_name": ["X", "X"],
                "mbi": ["M1", "M2"],
                "status": ["Completed"] * 2,
                "created_at": ["2024-06-01"] * 2,
                "provider_name": ["Doc"] * 2,
                "provider_npi": ["N1", "N2"],
                "tin": ["T1", "T2"],
            }
        )
        out = SvaSubmissionsPlugins().duplicate_names(df)
        assert out["groups"].height == 1
        assert out["records"].height == 2
