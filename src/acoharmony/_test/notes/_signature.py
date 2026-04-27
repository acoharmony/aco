# © 2025 HarmonyCares
"""Tests for acoharmony._notes._signature (SignaturePlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._notes import SignaturePlugins
from acoharmony._notes._signature import JANNETTE_NPI


def _bar_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "bene_mbi": ["M1", "M2", "M3", "M4"],
            "source_filename": ["ALGC25_2024-06.txt"] * 4,
            "bene_date_of_death": [None, date(2024, 1, 1), None, None],
            "end_date": [None, None, date(2023, 1, 1), None],
            "start_date": [date(2024, 1, 1)] * 4,
            "bene_first_name": ["A", "B", "C", "D"],
            "bene_last_name": ["X", "Y", "Z", "W"],
            "bene_gender": ["M"] * 4,
            "bene_state": ["MI"] * 4,
        }
    )


def _sva_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "bene_mbi": ["M1", "M2", "M4"],
            "aco_id": ["A1"] * 3,
            "sva_signature_date": [
                date(2024, 6, 1),
                date(2023, 1, 1),
                date(2025, 1, 1),
            ],
            "sva_npi": ["N1", JANNETTE_NPI, "N3"],
            "sva_tin": ["T1", "T2", "T3"],
            "sva_provider_name": ["Doc One", "Jannette", "Doc Three"],
            "source_filename": ["sva_2024-07.txt", "sva_2023-02.txt", "sva_2025-02.txt"],
            "file_date": [date(2024, 7, 1), date(2023, 2, 1), date(2025, 2, 1)],
        }
    )


def _provider_list() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "base_provider_tin": ["T1", "T3", "T3", "T9"],
            "individual_npi": ["N1", "N3", "N4", None],
            "individual_first_name": ["First1", "First3", "First4", None],
            "individual_last_name": ["Last1", "Last3", "Last4", None],
            "provider_class": ["Participant Provider"] * 4,
        }
    )


# ---------------------------------------------------------------------------
# Crosswalk
# ---------------------------------------------------------------------------


class TestCrosswalk:
    @pytest.mark.unit
    def test_load_crosswalk(self) -> None:
        plugin = SignaturePlugins()
        crosswalk = pl.LazyFrame(
            {
                "prvs_num": ["M1_OLD", "M2_OLD"],
                "crnt_num": ["M1", "M2"],
                "hcmpi": ["HC1", "HC2"],
            }
        )
        with patch(
            "acoharmony._transforms._identity_timeline.current_mbi_with_hcmpi_lookup_lazy",
            return_value=crosswalk,
        ):
            out = plugin.load_crosswalk(Path("/tmp"))
        assert out["mbi_map"]["M1_OLD"] == "M1"
        assert out["mbi_map"]["M1"] == "M1"
        assert out["mbi_to_hcmpi"]["M1_OLD"] == "HC1"
        assert out["mbi_history_count"]["M1"] == 1

    @pytest.mark.unit
    def test_standalone_crnt(self) -> None:
        plugin = SignaturePlugins()
        crosswalk = pl.LazyFrame(
            {
                "prvs_num": [None],
                "crnt_num": ["M1"],
                "hcmpi": ["HC1"],
            }
        )
        with patch(
            "acoharmony._transforms._identity_timeline.current_mbi_with_hcmpi_lookup_lazy",
            return_value=crosswalk,
        ):
            out = plugin.load_crosswalk(Path("/tmp"))
        assert out["mbi_map"]["M1"] == "M1"
        assert out["mbi_history_count"]["M1"] == 0


# ---------------------------------------------------------------------------
# raw loaders
# ---------------------------------------------------------------------------


class TestLoaders:
    @pytest.mark.unit
    def test_load_bar(self, tmp_path: Path) -> None:
        _bar_df().write_parquet(tmp_path / "bar.parquet")
        out = SignaturePlugins().load_bar(tmp_path)
        assert out.height == 4

    @pytest.mark.unit
    def test_load_provider_list(self, tmp_path: Path) -> None:
        _provider_list().write_parquet(tmp_path / "participant_list.parquet")
        out = SignaturePlugins().load_provider_list(tmp_path)
        assert ("T1", "N1") in out["valid_combos"]
        assert ("T3", "N4") in out["valid_combos"]
        assert out["npi_to_name"]["N1"] == "First1 Last1"

    @pytest.mark.unit
    def test_load_sva_with_pbvar(self, tmp_path: Path) -> None:
        _sva_df().write_parquet(tmp_path / "sva.parquet")
        pl.DataFrame(
            {
                "bene_mbi": ["M1"],
                "file_date": [date(2024, 6, 30)],
            }
        ).write_parquet(tmp_path / "pbvar.parquet")
        mbi_map = {"M1": "M1", "M2": "M2", "M4": "M4"}
        out = SignaturePlugins().load_sva(tmp_path, mbi_map)
        # PBVAR most recent = 2024-06-30; SVA 2024-07-01 > → pending = M1; SVA 2025-02-01 > → pending = M4
        assert out["pending"].height == 2
        assert out["approved"].height == 1
        assert out["most_recent_pbvar"] == date(2024, 6, 30)

    @pytest.mark.unit
    def test_load_sva_no_pbvar(self, tmp_path: Path) -> None:
        _sva_df().write_parquet(tmp_path / "sva.parquet")
        mbi_map = {"M1": "M1", "M2": "M2", "M4": "M4"}
        out = SignaturePlugins().load_sva(tmp_path, mbi_map)
        assert out["pending"].is_empty()
        assert out["approved"].height == 3
        assert out["most_recent_pbvar"] is None

    @pytest.mark.unit
    def test_load_pbvar_for_history_missing(self, tmp_path: Path) -> None:
        out = SignaturePlugins().load_pbvar_for_history(tmp_path, {})
        assert out.is_empty()

    @pytest.mark.unit
    def test_load_pbvar_for_history_empty(self, tmp_path: Path) -> None:
        # Empty PBVAR file
        pl.DataFrame({"bene_mbi": []}, schema={"bene_mbi": pl.Utf8}).write_parquet(
            tmp_path / "pbvar.parquet"
        )
        out = SignaturePlugins().load_pbvar_for_history(tmp_path, {})
        assert out.is_empty()

    @pytest.mark.unit
    def test_load_pbvar_for_history(self, tmp_path: Path) -> None:
        pbvar = pl.DataFrame(
            {
                "aco_id": ["A1"],
                "bene_mbi": ["M1"],
                "bene_first_name": ["A"],
                "bene_last_name": ["X"],
                "bene_line_1_address": ["123 Main"],
                "bene_city": ["Detroit"],
                "bene_state": ["MI"],
                "bene_zipcode": ["48226"],
                "provider_name": ["Org A"],
                "practitioner_name": ["Doc One"],
                "sva_npi": ["N1"],
                "sva_tin": ["T1"],
                "sva_signature_date": [date(2024, 1, 1)],
                "processed_at": [date(2024, 1, 5)],
                "source_file": ["pbvar1.txt"],
                "source_filename": ["PBVAR.txt"],
                "file_date": [date(2024, 1, 5)],
                "medallion_layer": ["silver"],
            }
        )
        pbvar.write_parquet(tmp_path / "pbvar.parquet")
        out = SignaturePlugins().load_pbvar_for_history(tmp_path, {"M1": "M1"})
        assert out.height == 1
        assert "sva_provider_name" in out.columns


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------


class TestSignatureHistory:
    @pytest.mark.unit
    def test_combine_signatures_no_pbvar(self) -> None:
        sva = pl.DataFrame({"normalized_mbi": ["M1"], "x": [1]})
        out = SignaturePlugins().combine_signatures(pl.DataFrame(), sva)
        assert out.height == 1

    @pytest.mark.unit
    def test_combine_signatures_with_pbvar(self) -> None:
        pbvar = pl.DataFrame({"normalized_mbi": ["M1"], "y": [10]})
        sva = pl.DataFrame({"normalized_mbi": ["M2"], "x": [1]})
        out = SignaturePlugins().combine_signatures(pbvar, sva)
        assert out.height == 2

    @pytest.mark.unit
    def test_combine_signatures_categorical_cast(self) -> None:
        pbvar = pl.DataFrame(
            {
                "normalized_mbi": pl.Series(["M1"], dtype=pl.Categorical),
                "y": [1],
            }
        )
        sva = pl.DataFrame({"normalized_mbi": ["M2"], "x": [1]})
        out = SignaturePlugins().combine_signatures(pbvar, sva)
        assert out.height == 2

    @pytest.mark.unit
    def test_signature_history(self) -> None:
        sigs = pl.DataFrame(
            {
                "normalized_mbi": ["M1", "M1", "M2"],
                "sva_signature_date": [
                    date(2023, 1, 1),
                    date(2024, 6, 1),
                    date(2024, 1, 1),
                ],
                "sva_npi": ["N1", "N1", "N2"],
                "sva_tin": ["T1", "T1", "T2"],
                "sva_provider_name": ["a", " A ", "b"],
                "source_filename": ["s1", "s2", "s3"],
            }
        )
        history, freq = SignaturePlugins().signature_history(sigs)
        assert history.height == 2
        m1 = next(r for r in history.iter_rows(named=True) if r["normalized_mbi"] == "M1")
        assert m1["total_signature_count"] == 2
        assert m1["most_recent_signature_date"] == date(2024, 6, 1)
        # M1 has 2 sigs, M2 has 1 sig → 2 distinct counts in freq
        assert freq.height == 2
        assert freq["beneficiary_count"].sum() == 2

    @pytest.mark.unit
    def test_count_signature_sources_empty(self, tmp_path: Path) -> None:
        out = SignaturePlugins().count_signature_sources(pl.DataFrame(), tmp_path)
        assert out["total_records"] == 0

    @pytest.mark.unit
    def test_count_signature_sources_with_pbvar(self, tmp_path: Path) -> None:
        (tmp_path / "pbvar.parquet").touch()
        sigs = pl.DataFrame(
            {
                "normalized_mbi": ["M1", "M2"],
                "source_filename": ["PBVAR_2024.txt", "sva_2024.txt"],
            }
        )
        out = SignaturePlugins().count_signature_sources(sigs, tmp_path)
        assert out["pbvar_records"] == 1
        assert out["sva_records"] == 1

    @pytest.mark.unit
    def test_count_signature_sources_no_pbvar(self, tmp_path: Path) -> None:
        sigs = pl.DataFrame(
            {
                "normalized_mbi": ["M1"],
                "source_filename": ["sva.txt"],
            }
        )
        out = SignaturePlugins().count_signature_sources(sigs, tmp_path)
        assert out["pbvar_records"] == 0
        assert out["sva_records"] == 1


# ---------------------------------------------------------------------------
# BAR cohort
# ---------------------------------------------------------------------------


class TestCurrentCohort:
    @pytest.mark.unit
    def test_with_algc25(self) -> None:
        out = SignaturePlugins().current_cohort(
            _bar_df(),
            mbi_map={"M1": "M1", "M2": "M2", "M3": "M3", "M4": "M4"},
            mbi_to_hcmpi={"M1": "HC1"},
            mbi_history_count={"M1": 0},
            today=date(2024, 6, 1),
        )
        # M2 deceased, M3 terminated → active = M1, M4
        assert out["active"].height == 2
        assert out["deceased"].height == 1
        assert out["terminated"].height == 1

    @pytest.mark.unit
    def test_no_algc25_falls_back(self) -> None:
        df = _bar_df().with_columns(
            pl.lit("OTHER_2024.txt").alias("source_filename")
        )
        out = SignaturePlugins().current_cohort(
            df,
            mbi_map={},
            mbi_to_hcmpi={},
            mbi_history_count={},
            today=date(2024, 6, 1),
        )
        assert out["most_recent_file"] == "OTHER_2024.txt"


# ---------------------------------------------------------------------------
# Thresholds + status categories
# ---------------------------------------------------------------------------


class TestDateThresholds:
    @pytest.mark.unit
    def test_thresholds(self) -> None:
        out = SignaturePlugins().date_thresholds(today=date(2025, 6, 1))
        assert out["current_year"] == 2025
        assert out["signature_cutoff"] == date(2024, 1, 1)


class TestSignatureStatusCategories:
    @pytest.mark.unit
    def test_categorises(self) -> None:
        plugin = SignaturePlugins()
        bar = _bar_df()
        cohort = plugin.current_cohort(
            bar,
            mbi_map={"M1": "M1", "M2": "M2", "M3": "M3", "M4": "M4"},
            mbi_to_hcmpi={},
            mbi_history_count={},
            today=date(2024, 6, 1),
        )["active"]
        history = pl.DataFrame(
            {
                "normalized_mbi": ["M1"],
                "total_signature_count": [1],
                "earliest_signature_date": [date(2024, 6, 1)],
                "most_recent_signature_date": [date(2024, 6, 1)],
                "most_recent_provider_npi": ["N1"],
                "most_recent_provider_tin": ["T1"],
                "most_recent_provider_name": ["Doc One"],
                "most_recent_source_file": ["s1"],
                "all_signature_dates": [[date(2024, 6, 1)]],
                "all_provider_npis": [["N1"]],
                "all_provider_tins": [["T1"]],
            }
        )
        thresholds = plugin.date_thresholds(today=date(2024, 12, 1))
        out = plugin.signature_status_categories(
            cohort,
            history,
            valid_combos={("T1", "N1")},
            npi_to_name={"N1": "Doc One"},
            thresholds=thresholds,
            today=date(2024, 12, 1),
        )
        as_dict = {row["bene_mbi"]: row for row in out.iter_rows(named=True)}
        assert as_dict["M1"]["signature_recency_status"] == "Current Year (2024)"
        # M4 has no history → Never Signed
        assert as_dict["M4"]["signature_recency_status"] == "Never Signed"


# ---------------------------------------------------------------------------
# Chase list
# ---------------------------------------------------------------------------


class TestChaseList:
    @pytest.mark.unit
    def test_builds(self) -> None:
        plugin = SignaturePlugins()
        # A pre-made status_categories with 4 rows
        cat = pl.DataFrame(
            {
                "bene_mbi": ["M1", "M2", "M3", "M4"],
                "normalized_mbi": ["M1", "M2", "M3", "M4"],
                "hcmpi": [""] * 4,
                "previous_mbi_count": [0] * 4,
                "bene_first_name": ["A", "B", "C", "D"],
                "bene_last_name": ["X", "Y", "Z", "W"],
                "bene_date_of_death": [None] * 4,
                "start_date": [date(2024, 1, 1)] * 4,
                "end_date": [None] * 4,
                "total_signature_count": [None, 1, 1, 1],
                "most_recent_signature_date": [
                    None,
                    date(2022, 1, 1),
                    date(2024, 6, 1),
                    date(2024, 6, 1),
                ],
                "most_recent_source_file": [None, "s2", "s3", "s4"],
                "days_since_last_signature": [None, 800, 100, 100],
                "most_recent_provider_npi": [None, "N2", "BAD_NPI", JANNETTE_NPI],
                "most_recent_provider_tin": [None, "T2", "TBAD", "TJ"],
                "most_recent_provider_name": [None, "B", "BAD", "JAN"],
                "provider_name_from_list": ["", "", "", ""],
                "all_signature_dates": [[] for _ in range(4)],
                "signature_recency_status": [
                    "Never Signed",
                    "Very Old (Pre-2022)",
                    "Invalid Provider",
                    "Current Year (2024)",
                ],
            }
        )
        thresholds = plugin.date_thresholds(today=date(2024, 12, 1))
        out = plugin.chase_list(cat, pl.DataFrame(), thresholds)
        # M1 (never), M2 (old), M3 (invalid), M4 (Jannette) → all 4
        assert out["chase_list"].height == 4
        reasons = sorted(out["with_reason"]["chase_reason"].to_list())
        assert "Never Signed" in reasons
        assert "Jannette Alignment" in reasons
        assert out["jannette_count"] == 1

    @pytest.mark.unit
    def test_excludes_pending(self) -> None:
        plugin = SignaturePlugins()
        cat = pl.DataFrame(
            {
                "bene_mbi": ["M1"],
                "normalized_mbi": ["M1"],
                "hcmpi": [""],
                "previous_mbi_count": [0],
                "bene_first_name": ["A"],
                "bene_last_name": ["X"],
                "bene_date_of_death": [None],
                "start_date": [date(2024, 1, 1)],
                "end_date": [None],
                "total_signature_count": [None],
                "most_recent_signature_date": [None],
                "most_recent_source_file": [None],
                "days_since_last_signature": [None],
                "most_recent_provider_npi": [None],
                "most_recent_provider_tin": [None],
                "most_recent_provider_name": [None],
                "provider_name_from_list": [""],
                "all_signature_dates": [[]],
                "signature_recency_status": ["Never Signed"],
            }
        )
        pending = pl.DataFrame({"normalized_mbi": ["M1"]})
        thresholds = plugin.date_thresholds(today=date(2024, 12, 1))
        out = plugin.chase_list(cat, pending, thresholds)
        assert out["chase_list"].is_empty()

    @pytest.mark.unit
    def test_empty_chase_stats(self) -> None:
        plugin = SignaturePlugins()
        cat = pl.DataFrame(
            schema={
                "bene_mbi": pl.Utf8,
                "normalized_mbi": pl.Utf8,
                "hcmpi": pl.Utf8,
                "previous_mbi_count": pl.Int64,
                "bene_first_name": pl.Utf8,
                "bene_last_name": pl.Utf8,
                "bene_date_of_death": pl.Date,
                "start_date": pl.Date,
                "end_date": pl.Date,
                "total_signature_count": pl.Int64,
                "most_recent_signature_date": pl.Date,
                "most_recent_source_file": pl.Utf8,
                "days_since_last_signature": pl.Int64,
                "most_recent_provider_npi": pl.Utf8,
                "most_recent_provider_tin": pl.Utf8,
                "most_recent_provider_name": pl.Utf8,
                "provider_name_from_list": pl.Utf8,
                "all_signature_dates": pl.List(pl.Date),
                "signature_recency_status": pl.Utf8,
            }
        )
        thresholds = plugin.date_thresholds(today=date(2024, 12, 1))
        out = plugin.chase_list(cat, pl.DataFrame(), thresholds)
        assert out["stats"]["total_chase"] == 0


# ---------------------------------------------------------------------------
# Provider analysis
# ---------------------------------------------------------------------------


class TestProviderAnalysis:
    @pytest.mark.unit
    def test_invalid_provider_summary_empty(self) -> None:
        empty = pl.DataFrame(
            schema={
                "signature_recency_status": pl.Utf8,
                "most_recent_provider_tin": pl.Utf8,
                "most_recent_provider_npi": pl.Utf8,
                "most_recent_provider_name": pl.Utf8,
                "most_recent_signature_date": pl.Date,
            }
        )
        out = SignaturePlugins().invalid_provider_summary(empty)
        assert out.is_empty()

    @pytest.mark.unit
    def test_invalid_provider_summary(self) -> None:
        df = pl.DataFrame(
            {
                "signature_recency_status": ["Invalid Provider"],
                "most_recent_provider_tin": ["TBAD"],
                "most_recent_provider_npi": ["NBAD"],
                "most_recent_provider_name": ["BAD"],
                "most_recent_signature_date": [date(2024, 6, 1)],
            }
        )
        out = SignaturePlugins().invalid_provider_summary(df)
        assert out.height == 1
        assert out["affected_beneficiaries"][0] == 1

    @pytest.mark.unit
    def test_provider_signature_stats(self) -> None:
        df = pl.DataFrame(
            {
                "most_recent_provider_npi": ["N1", "N1", None],
                "most_recent_provider_name": ["Doc One", "Doc One", None],
                "most_recent_signature_date": [
                    date(2024, 6, 1),
                    date(2023, 6, 1),
                    None,
                ],
                "signature_recency_status": [
                    "Current Year (2024)",
                    "Old (2023)",
                    "Never Signed",
                ],
            }
        )
        out = SignaturePlugins().provider_signature_stats(df)
        assert out.height == 1
        assert out["active_beneficiary_count"][0] == 2

    @pytest.mark.unit
    def test_chase_provider_summary_empty(self) -> None:
        empty = pl.DataFrame(
            schema={
                "most_recent_provider_npi": pl.Utf8,
                "most_recent_provider_name": pl.Utf8,
                "chase_reason": pl.Utf8,
            }
        )
        out = SignaturePlugins().chase_provider_summary(empty)
        assert out.is_empty()

    @pytest.mark.unit
    def test_chase_provider_summary(self) -> None:
        df = pl.DataFrame(
            {
                "most_recent_provider_npi": ["N1", "N1"],
                "most_recent_provider_name": ["Doc", "Doc"],
                "chase_reason": ["Old", "Invalid TIN/NPI Combo"],
            }
        )
        out = SignaturePlugins().chase_provider_summary(df)
        assert out["beneficiaries_needing_signature"][0] == 2


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


class TestExport:
    @pytest.mark.unit
    def test_export_chase_list(self, tmp_path: Path) -> None:
        df = pl.DataFrame(
            {
                "bene_mbi": ["M1"],
                "normalized_mbi": ["M1"],
                "hcmpi": [""],
                "previous_mbi_count": [0],
                "bene_first_name": ["A"],
                "bene_last_name": ["X"],
                "bene_gender": ["M"],
                "bene_state": ["MI"],
                "most_recent_signature_date": [None],
                "days_since_last_signature": [None],
                "signature_recency_status": ["Never Signed"],
                "most_recent_provider_npi": [None],
                "most_recent_provider_tin": [None],
                "most_recent_provider_name": [None],
                "chase_reason": ["Never Signed"],
            }
        )
        out_path = SignaturePlugins().export_chase_list(df, tmp_path / "out")
        assert out_path.exists()

    @pytest.mark.unit
    def test_export_analysis_results(self, tmp_path: Path) -> None:
        plugin = SignaturePlugins()
        cat = pl.DataFrame({"x": [1]})
        actions = pl.DataFrame({"x": [1]})
        providers = pl.DataFrame({"x": [1]})
        out = plugin.export_analysis_results(cat, actions, providers, tmp_path)
        for p in out.values():
            assert p.exists()


# ---------------------------------------------------------------------------
# SVA validation
# ---------------------------------------------------------------------------


class TestSvaValidation:
    @pytest.mark.unit
    def test_most_recent_dates(self, tmp_path: Path) -> None:
        plugin = SignaturePlugins()
        assert plugin.most_recent_sva_date(tmp_path) is None
        assert plugin.most_recent_pbvar_date(tmp_path) is None
        _sva_df().write_parquet(tmp_path / "sva.parquet")
        assert plugin.most_recent_sva_date(tmp_path) == date(2025, 2, 1)

    @pytest.mark.unit
    def test_validate_tin_npi_missing(self, tmp_path: Path) -> None:
        out, invalid = SignaturePlugins().validate_sva_tin_npi(tmp_path, set())
        assert out is None
        assert invalid.is_empty()

    @pytest.mark.unit
    def test_validate_tin_npi(self, tmp_path: Path) -> None:
        _sva_df().write_parquet(tmp_path / "sva.parquet")
        valid = {("T3", "N3")}
        out, invalid = SignaturePlugins().validate_sva_tin_npi(tmp_path, valid)
        # Most recent SVA file_date = 2025-02-01 → only M4's row
        assert out is not None
        assert out.height == 1
        assert invalid.is_empty()  # M4's combo (T3, N3) is valid

    @pytest.mark.unit
    def test_validate_status_empty(self) -> None:
        d, t = SignaturePlugins().validate_sva_status(
            None, pl.DataFrame(), {}, today=date(2024, 6, 1)
        )
        assert d.is_empty()
        assert t.is_empty()

    @pytest.mark.unit
    def test_validate_status(self) -> None:
        sva = _sva_df()
        out_d, out_t = SignaturePlugins().validate_sva_status(
            sva,
            _bar_df(),
            mbi_map={"M1": "M1", "M2": "M2", "M3": "M3", "M4": "M4"},
            today=date(2024, 6, 1),
        )
        # M2 deceased
        assert out_d.height == 1

    @pytest.mark.unit
    def test_pbvar_empty_returns_none(self, tmp_path: Path) -> None:
        # PBVAR file exists but empty
        pl.DataFrame({"file_date": []}, schema={"file_date": pl.Date}).write_parquet(
            tmp_path / "pbvar.parquet"
        )
        out = SignaturePlugins().most_recent_pbvar_date(tmp_path)
        assert out is None
