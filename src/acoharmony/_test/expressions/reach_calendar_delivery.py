# © 2025 HarmonyCares
# All rights reserved.

"""Tests for expressions._reach_calendar_delivery module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest


# ----------------------------------------------------------------------------
# classify_calendar_description
# ----------------------------------------------------------------------------


class TestClassifyCalendarDescription:
    """Coverage for every classifier branch."""

    @pytest.mark.unit
    @pytest.mark.parametrize("value", [None, ""])
    def test_empty_input_returns_none_triple(self, value):
        assert classify_calendar_description(value) == (None, None, None)

    @pytest.mark.unit
    def test_unknown_description_returns_none_schema(self):
        schema, period, py = classify_calendar_description("Some random text")
        assert schema is None
        assert period is None
        assert py is None

    @pytest.mark.unit
    def test_monthly_report_parses_month(self):
        schema, period, py = classify_calendar_description(
            "Monthly Expenditure Report - April"
        )
        assert schema == "mexpr"
        assert period == "M04"
        assert py is None

    @pytest.mark.unit
    def test_monthly_runout_has_higher_priority_than_monthly(self):
        schema, period, _ = classify_calendar_description(
            "Monthly Expenditure Run-out Report - January"
        )
        # Both rules resolve to mexpr, but the run-out rule is first — assert
        # behaviour is stable regardless of which matched first.
        assert schema == "mexpr"
        assert period == "M01"

    @pytest.mark.unit
    def test_cclf_management_outranks_plain_cclf(self):
        schema, _, _ = classify_calendar_description(
            "CCLF Claims Management (5-1) Report - July"
        )
        assert schema == "cclf_management_report"

    @pytest.mark.unit
    def test_plain_cclf_falls_through_to_cclf0(self):
        schema, period, _ = classify_calendar_description("CCLF - March")
        assert schema == "cclf0"
        assert period == "M03"

    @pytest.mark.unit
    def test_quarterly_benchmark_prefers_reach_bnmr(self):
        schema, period, py = classify_calendar_description(
            "Quarterly Benchmark Report - Q3 PY2025"
        )
        assert schema == "reach_bnmr"
        assert period == "Q3"
        assert py == 2025

    @pytest.mark.unit
    def test_semi_annual_tag_wins_over_month(self):
        schema, period, _ = classify_calendar_description(
            "Risk Score Report - S1 March"
        )
        # Q pattern isn't there, S1 beats month parsing since _SEMI_RX runs first.
        assert schema == "risk_adjustment_data"
        assert period == "S1"

    @pytest.mark.unit
    def test_annual_keyword_falls_back_to_A_bucket(self):
        schema, period, _ = classify_calendar_description("Annual Quality Report")
        assert schema == "annual_quality_report"
        assert period == "A"

    @pytest.mark.unit
    def test_py_tag_without_month_yields_A_bucket(self):
        schema, period, py = classify_calendar_description(
            "Signed Voluntary Alignment Response File - PY2024"
        )
        assert schema == "pbvar"
        # No Q/S/month tokens, but 'PY2024' triggers the annual fallback in
        # the period resolver.
        assert period == "A"
        assert py == 2024

    @pytest.mark.unit
    def test_final_keyword_falls_back_to_A_bucket(self):
        # "Final Reconciliation Benchmark Report" — the Benchmark stem resolves
        # to reach_bnmr, and 'Final' triggers the A-bucket fallback.
        schema, period, _ = classify_calendar_description(
            "Final Reconciliation Benchmark Report"
        )
        assert schema == "reach_bnmr"
        assert period == "A"

    @pytest.mark.unit
    def test_shadow_bundle_monthly_parses_month(self):
        schema, period, _ = classify_calendar_description(
            "Shadow Bundles Monthly Files - August"
        )
        # The "Shadow Bundles Monthly" description describes the delivered
        # zip archive (``.SBMON.*.zip``), which the schema registry pattern
        # matches to ``shadow_bundle_reach`` — not the individual SBM*
        # members (sbmdm, sbmhh, …). Use the archive schema so the calendar
        # key matches the delivery key.
        assert schema == "shadow_bundle_reach"
        assert period == "M08"

    @pytest.mark.unit
    def test_shadow_bundle_monthly_files_parenthesized(self):
        schema, _, _ = classify_calendar_description(
            "August Monthly Files (CY2024-CY2025)"
        )
        assert schema == "shadow_bundle_reach"

    @pytest.mark.unit
    def test_sbqr_parenthesized_quarterly(self):
        schema, period, _ = classify_calendar_description(
            "Q2 Quarterly Report (CY2024-CY2025Q1)"
        )
        assert schema == "sbqr"
        assert period == "Q2"

    @pytest.mark.unit
    def test_preliminary_beats_generic_benchmark(self):
        schema, _, _ = classify_calendar_description("Preliminary Benchmark Report")
        assert schema == "preliminary_benchmark_report_unredacted"

    @pytest.mark.unit
    def test_provisional_alignment_estimate_maps_to_paer(self):
        schema, _, _ = classify_calendar_description(
            "Provisional Alignment Estimate Report (PAER) #1"
        )
        assert schema == "preliminary_alignment_estimate"

    @pytest.mark.unit
    def test_beneficiary_level_report_maps_to_blqqr(self):
        schema, _, _ = classify_calendar_description(
            "Q1 Beneficiary-Level Reports (Quality)"
        )
        assert schema == "quarterly_beneficiary_level_quality_report"

    @pytest.mark.unit
    def test_hedr_transparency(self):
        schema, _, _ = classify_calendar_description(
            "S2 Beneficiary HEDR Transparency File"
        )
        assert schema == "beneficiary_hedr_transparency_files"

    @pytest.mark.unit
    def test_financial_guarantee(self):
        schema, _, _ = classify_calendar_description(
            "PY2024 Financial Guarantee Amounts to Be Shared"
        )
        assert schema == "aco_financial_guarantee_amount"

    @pytest.mark.unit
    def test_cisep_threshold(self):
        schema, _, _ = classify_calendar_description(
            "Estimated CI/SEP Change Threshold Report"
        )
        assert schema == "estimated_cisep_change_threshold_report"

    @pytest.mark.unit
    def test_provider_alignment(self):
        schema, period, _ = classify_calendar_description(
            "Provider Alignment Report - Q2"
        )
        assert schema == "palmr"
        assert period == "Q2"

    @pytest.mark.unit
    def test_prospective_plus(self):
        schema, _, _ = classify_calendar_description("Prospective Plus Opportunity Report")
        assert schema == "prospective_plus_opportunity_report"

    @pytest.mark.unit
    def test_signed_attestation(self):
        schema, _, _ = classify_calendar_description(
            "Q3 Signed Attestation Based Voluntary Alignment Response File"
        )
        assert schema == "pbvar"

    @pytest.mark.unit
    def test_alt_payment_arrangement(self):
        schema, period, _ = classify_calendar_description(
            "Alternative Payment Arrangement Report - Q4"
        )
        assert schema == "alternative_payment_arrangement_report"
        assert period == "Q4"

    @pytest.mark.unit
    def test_tparc_psrr(self):
        schema, _, _ = classify_calendar_description(
            "Provider Specific Payment Reduction Report - June"
        )
        assert schema == "tparc"

    @pytest.mark.unit
    def test_national_ref_pop_maps_to_mexpr(self):
        schema, _, _ = classify_calendar_description(
            "National Reference Population Data Report - July"
        )
        assert schema == "mexpr"


# ----------------------------------------------------------------------------
# build_calendar_reports_lf
# ----------------------------------------------------------------------------


@pytest.fixture
def calendar_parquet(tmp_path: Path) -> Path:
    """Two calendar snapshots so latest_only has something to filter."""
    rows = [
        # Older snapshot — should be dropped when latest_only=True.
        {
            "type": "Report",
            "description": "Monthly Expenditure Report - February",
            "category": "Finance",
            "start_date": date(2024, 2, 26),
            "file_date": "2024-01-01",
            "py": 2024,
        },
        # Latest snapshot — two reports + one non-report.
        {
            "type": "Report",
            "description": "Monthly Expenditure Report - April",
            "category": "Finance",
            "start_date": date(2024, 4, 25),
            "file_date": "2026-04-01",
            "py": 2024,
        },
        {
            "type": "Report",
            "description": "Quarterly Benchmark Report - Q2",
            "category": "Finance",
            "start_date": date(2024, 7, 15),
            "file_date": "2026-04-01",
            "py": 2024,
        },
        {
            "type": "Report",
            "description": "Something unrecognized",
            "category": "Finance",
            "start_date": date(2024, 4, 15),
            "file_date": "2026-04-01",
            "py": 2024,
        },
        {
            "type": "Report",
            # No period tokens → period falls back to start_date + quarterly
            # hint because the schema resolves to reach_bnmr.
            "description": "Quarterly Benchmark Report",
            "category": "Finance",
            "start_date": date(2024, 8, 20),
            "file_date": "2026-04-01",
            "py": 2024,
        },
        {
            "type": "Event",
            "description": "Some event",
            "category": "Alignment",
            "start_date": date(2024, 4, 1),
            "file_date": "2026-04-01",
            "py": None,
        },
    ]
    df = pl.DataFrame(rows)
    path = tmp_path / "reach_calendar.parquet"
    df.write_parquet(path)
    return path


class TestBuildCalendarReportsLf:
    @pytest.mark.unit
    def test_drops_non_report_rows(self, calendar_parquet):
        lf = build_calendar_reports_lf(calendar_parquet)
        df = lf.collect()
        assert df.filter(pl.col("description") == "Some event").height == 0

    @pytest.mark.unit
    def test_latest_only_filters_older_snapshots(self, calendar_parquet):
        lf = build_calendar_reports_lf(calendar_parquet, latest_only=True)
        df = lf.collect()
        assert (df["calendar_file_date"].unique().to_list() == ["2026-04-01"])

    @pytest.mark.unit
    def test_latest_only_false_keeps_history(self, calendar_parquet):
        lf = build_calendar_reports_lf(calendar_parquet, latest_only=False)
        df = lf.collect()
        assert set(df["calendar_file_date"].unique().to_list()) == {
            "2024-01-01",
            "2026-04-01",
        }

    @pytest.mark.unit
    def test_classifies_schema_and_period(self, calendar_parquet):
        df = build_calendar_reports_lf(calendar_parquet).collect()
        mexpr = df.filter(pl.col("schema_name") == "mexpr")
        assert mexpr["period"].to_list() == ["M04"]

    @pytest.mark.unit
    def test_unknown_description_has_null_schema(self, calendar_parquet):
        df = build_calendar_reports_lf(calendar_parquet).collect()
        unclassified = df.filter(pl.col("description") == "Something unrecognized")
        assert unclassified.height == 1
        assert unclassified["schema_name"].item() is None

    @pytest.mark.unit
    def test_period_falls_back_to_start_date_for_quarterly_schema(
        self, calendar_parquet
    ):
        df = build_calendar_reports_lf(calendar_parquet).collect()
        # "Quarterly Benchmark Report" with start_date 2024-08-20 → Q3.
        bnmr_no_period = df.filter(
            (pl.col("schema_name") == "reach_bnmr")
            & (pl.col("description") == "Quarterly Benchmark Report")
        )
        assert bnmr_no_period.height == 1
        assert bnmr_no_period["period"].item() == "Q3"


@pytest.fixture
def cadence_calendar_parquet(tmp_path: Path) -> Path:
    """Calendar parquet covering the tricky cadence cases: PAER description
    inconsistency, shadow-bundle month vs schedule-drop month, annual
    schemas."""
    rows = [
        # PAER with PY token but no month token — should NOT land as "A";
        # should take start_date month (November = M11).
        {
            "type": "Report",
            "description": "Release of PY2025 Preliminary Alignment Estimate Report, Round 1 of 2",
            "category": "Alignment",
            "start_date": date(2024, 11, 18),
            "file_date": "2026-04-01",
            "py": 2025,
        },
        # PAER with explicit month name.
        {
            "type": "Report",
            "description": "Provisional Alignment Estimate Report (PAER) #2 - December",
            "category": "Alignment",
            "start_date": date(2023, 12, 11),
            "file_date": "2026-04-01",
            "py": 2024,
        },
        # Shadow bundle: description says February, start_date is end of
        # March (CMS schedules delivery at end of next month). Period must
        # track the description's "February", not March.
        {
            "type": "Report",
            "description": "February Monthly Files (CY2024-CY2025)",
            "category": "Shadow Bundles",
            "start_date": date(2025, 3, 31),
            "file_date": "2026-04-01",
            "py": 2025,
        },
        # Annual schema with PY token — must land "A".
        {
            "type": "Report",
            "description": "PY2024 Financial Guarantee Amounts to Be Shared with ACOs Via 4i",
            "category": "Compliance",
            "start_date": date(2024, 2, 21),
            "file_date": "2026-04-01",
            "py": 2024,
        },
        # Monthly schema (tparc) described as "July" — period = M07.
        {
            "type": "Report",
            "description": "July Provider Specific Payment Reduction Report",
            "category": "Finance",
            "start_date": date(2024, 7, 31),
            "file_date": "2026-04-01",
            "py": 2024,
        },
    ]
    df = pl.DataFrame(rows)
    path = tmp_path / "reach_calendar_cadence.parquet"
    df.write_parquet(path)
    return path


class TestBuildCalendarReportsLfCadence:
    """Regression coverage for schema-cadence classification corner cases."""

    @pytest.mark.unit
    def test_paer_with_py_token_still_uses_start_date_month(
        self, cadence_calendar_parquet
    ):
        df = build_calendar_reports_lf(cadence_calendar_parquet).collect()
        paer_round1 = df.filter(
            pl.col("description").str.contains("Round 1 of 2")
        )
        # Would be "A" under generic classifier; PAER override forces M11.
        assert paer_round1["period"].item() == "M11"

    @pytest.mark.unit
    def test_paer_with_explicit_month_still_uses_start_date_month(
        self, cadence_calendar_parquet
    ):
        df = build_calendar_reports_lf(cadence_calendar_parquet).collect()
        paer_dec = df.filter(pl.col("description").str.contains(r"#2"))
        # start_date is 2023-12-11 → M12. Also matches "December" token —
        # confirming PAER override and description-derived period agree
        # when both are present.
        assert paer_dec["period"].item() == "M12"

    @pytest.mark.unit
    def test_shadow_bundle_description_month_wins_over_start_date(
        self, cadence_calendar_parquet
    ):
        """CMS schedules shadow-bundle drops in the month *after* the
        coverage month. The description's month token is the right bucket —
        the start_date month is when the drop is scheduled, not what
        period the data covers."""
        df = build_calendar_reports_lf(cadence_calendar_parquet).collect()
        feb_files = df.filter(pl.col("description").str.contains("February"))
        # Description says February; start_date is March 31. Period must
        # be M02, not M03.
        assert feb_files["period"].item() == "M02"

    @pytest.mark.unit
    def test_annual_schema_always_A_even_with_month_like_text(
        self, cadence_calendar_parquet
    ):
        df = build_calendar_reports_lf(cadence_calendar_parquet).collect()
        fgl = df.filter(pl.col("schema_name") == "aco_financial_guarantee_amount")
        assert fgl["period"].item() == "A"

    @pytest.mark.unit
    def test_monthly_schema_with_explicit_month_token(
        self, cadence_calendar_parquet
    ):
        df = build_calendar_reports_lf(cadence_calendar_parquet).collect()
        tparc_jul = df.filter(pl.col("schema_name") == "tparc")
        assert tparc_jul["period"].item() == "M07"


# ----------------------------------------------------------------------------
# _period_from_date / _filename_date / _filename_period / _filename_py
# ----------------------------------------------------------------------------


class TestInternalHelpers:
    @pytest.mark.unit
    def test_period_from_date_none(self):
        assert _period_from_date(None, quarterly=True) is None
        assert _period_from_date(None, quarterly=False) is None

    @pytest.mark.unit
    def test_period_from_date_monthly(self):
        assert _period_from_date(date(2024, 7, 15), quarterly=False) == "M07"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "d,expected",
        [
            (date(2024, 1, 1), "Q1"),
            (date(2024, 4, 1), "Q2"),
            (date(2024, 7, 1), "Q3"),
            (date(2024, 12, 31), "Q4"),
        ],
    )
    def test_period_from_date_quarterly_boundaries(self, d, expected):
        assert _period_from_date(d, quarterly=True) == expected

    @pytest.mark.unit
    def test_filename_date_uses_shared_extractor(self):
        # YYYYMMDD — handled by the shared extractor.
        assert _filename_date("ACO_REACH_Calendar_updated_20251202.xlsx") == date(
            2025, 12, 2
        )

    @pytest.mark.unit
    def test_filename_date_uses_d_convention_fallback(self):
        # ``P.D0259.MCMXP.RP.D240508.T*.xlsx`` — shared extractor returns None,
        # our ``.D{YYMMDD}.`` fallback picks it up.
        assert _filename_date("P.D0259.MCMXP.RP.D240508.T1.xlsx") == date(2024, 5, 8)

    @pytest.mark.unit
    def test_filename_date_returns_none_for_unparseable(self):
        assert _filename_date("random_name.csv") is None

    @pytest.mark.unit
    def test_filename_date_handles_malformed_iso_from_extractor(self, monkeypatch):
        """If the shared extractor somehow returns a malformed ISO string we
        should fall through to the D-convention, not crash."""
        monkeypatch.setattr(
            "acoharmony._expressions._reach_calendar_delivery.extract_file_date",
            lambda *_args, **_kw: "not-an-iso-date",
        )
        # Filename has a D{YYMMDD} chunk for the fallback to catch.
        assert _filename_date("P.D0259.X.D230405.T1.xlsx") == date(2023, 4, 5)

    @pytest.mark.unit
    def test_filename_period_empty_returns_none(self):
        assert _filename_period("", "mexpr") is None

    @pytest.mark.unit
    def test_filename_period_explicit_quarter(self):
        assert _filename_period("D0259.BLQQR.Q3.PY2024.D240930.T1.zip", "x") == "Q3"

    @pytest.mark.unit
    def test_filename_period_shadow_bundle_month(self):
        assert _filename_period("D0259.PY2024.05.SBMON.D240610.T1.zip", "sbmdm") == "M05"

    @pytest.mark.unit
    def test_filename_period_bar_uses_delivery_month(self):
        assert (
            _filename_period("P.D0259.ALGC24.RP.D240315.T1.xlsx", "bar") == "M03"
        )

    @pytest.mark.unit
    def test_filename_period_bar_without_date_returns_none(self):
        assert _filename_period("P.D0259.ALGC24.RP.NODATE.xlsx", "bar") is None

    @pytest.mark.unit
    def test_filename_period_quarterly_schema_falls_back_to_delivery_quarter(self):
        # reach_bnmr filename without an explicit .Q#. but with a D{YYMMDD}
        # delivery stamp — infer quarter from delivery month.
        assert (
            _filename_period("REACH.D0259.BNMR.D250513.T1.xlsx", "reach_bnmr") == "Q2"
        )

    @pytest.mark.unit
    def test_filename_date_prefers_delivery_stamp_over_py_marker(self):
        """Regression: shared extractor treats PY{YYYY} as an annual marker
        and returns 12/31. For monthly/quarterly files the ``.D{YYMMDD}.T``
        chunk is the real delivery date and must win — otherwise every file
        in a performance year collapses onto the same December 31 bucket."""
        # Monthly: April 25 delivery — PY2024 marker must NOT override.
        assert _filename_date(
            "REACH.D0259.MEXPR.04.PY2024.D240425.T1.xlsx"
        ) == date(2024, 4, 25)
        # Quarterly: May 13 delivery — same principle.
        assert _filename_date(
            "REACH.D0259.BNMR.PY2025.D250513.T1428470.xlsx"
        ) == date(2025, 5, 13)

    @pytest.mark.unit
    def test_filename_date_ignores_aco_id_d_chunk(self):
        """The ``.D0259.`` ACO id prefix must not be mistaken for a delivery
        date — the regex requires the D{YYMMDD} chunk to be immediately
        followed by a .T time token."""
        # Use a filename with an ACO-id `.D0259.` chunk only and no T token
        # to prove we don't falsely parse the ACO id as a delivery date.
        assert _filename_date("P.D0259.random_no_time.xlsx") is None

    @pytest.mark.unit
    def test_filename_period_monthly_schema_falls_back(self):
        assert (
            _filename_period("P.D0259.TPARC.RP.D240601.T1.txt", "tparc") == "M06"
        )

    @pytest.mark.unit
    def test_filename_period_unknown_schema_no_hints_returns_none(self):
        assert _filename_period("some_random_file.xlsx", "unknown_schema") is None

    @pytest.mark.unit
    def test_filename_period_annual_schema_returns_A(self):
        """Annual PY-wide schemas land with period='A' to match the calendar
        classifier's annual bucket — no date extraction needed."""
        assert (
            _filename_period(
                "D0259.FGL.PY2024.D240221.T1200000.pdf",
                "aco_financial_guarantee_amount",
            )
            == "A"
        )
        assert (
            _filename_period(
                "REACH.D0259.PLARU.PY2024.D231210.T1200000.xlsx", "plaru"
            )
            == "A"
        )
        assert (
            _filename_period(
                "D0259.BLAQR.PY2024.D241231.T0811250.zip",
                "annual_beneficiary_level_quality_report",
            )
            == "A"
        )

    @pytest.mark.unit
    def test_filename_period_quarterly_schema_without_Q_token(self):
        """Schemas like palmr/pbvar ship quarterly without an explicit
        ``.Q#.`` token — the cadence fallback derives Q from the D-stamp."""
        # palmr delivered in April → Q2.
        assert (
            _filename_period("P.D0259.PALMR.D240424.T1135150.csv", "palmr") == "Q2"
        )
        # pbvar delivered in October → Q4.
        assert (
            _filename_period("P.D0259.PBVAR.D231018.T0112000.xlsx", "pbvar") == "Q4"
        )

    @pytest.mark.unit
    def test_filename_period_monthly_cadence_covers_cclf_mgmt_and_paer(self):
        """cclf_management_report and preliminary_alignment_estimate are
        monthly schemas — period should be M## from the D-stamp month."""
        assert (
            _filename_period(
                "P.D0259.MCMXP.RP.D251006.T0200015.xlsx", "cclf_management_report"
            )
            == "M10"
        )
        assert (
            _filename_period(
                "REACH.D0259.PAER.PY2024.D231107.T1113370.xlsx",
                "preliminary_alignment_estimate",
            )
            == "M11"
        )

    @pytest.mark.unit
    def test_filename_period_annual_schema_without_dstamp_still_returns_A(self):
        """Annual bucket doesn't require a parseable delivery date — the
        bucket is the schema-level fallback, and the date comes from the
        hierarchy (remote / filename / download). Confirms the A fallback
        fires purely on the schema name."""
        assert (
            _filename_period("no-date-here.pdf", "aco_financial_guarantee_amount")
            == "A"
        )

    @pytest.mark.unit
    def test_filename_py_pyred_two_digit(self):
        assert _filename_py("P.D0259.PYRED25.RP.D250221.T1.xlsx") == 2025

    @pytest.mark.unit
    def test_filename_py_four_digit(self):
        assert _filename_py("REACH.D0259.BNMR.PY2024.D240502.T1.xlsx") == 2024

    @pytest.mark.unit
    def test_filename_py_missing_returns_none(self):
        assert _filename_py("P.D0259.ALGC24.RP.D240315.T1.xlsx") is None

    @pytest.mark.unit
    def test_parse_iso_none(self):
        assert _parse_iso(None) is None
        assert _parse_iso("") is None

    @pytest.mark.unit
    def test_parse_iso_z_suffix(self):
        dt = _parse_iso("2024-02-12T00:27:47.000Z")
        assert dt is not None
        # Z → UTC
        assert dt.tzinfo is not None

    @pytest.mark.unit
    def test_parse_iso_malformed(self):
        assert _parse_iso("not-a-timestamp") is None


# ----------------------------------------------------------------------------
# _schema_for_filename
# ----------------------------------------------------------------------------


class TestSchemaForFilename:
    @pytest.mark.unit
    def test_most_specific_pattern_wins(self):
        patterns = [
            {"pattern": "*", "file_type_code": 0, "schema_name": "generic"},
            {
                "pattern": "D????.PY????.??.SBM*.D??????.T*.*",
                "file_type_code": 243,
                "schema_name": "shadow_bundle_reach",
            },
            {
                "pattern": "D????.PY????.??.SBMDM.D??????.T*.csv",
                "file_type_code": 243,
                "schema_name": "sbmdm",
            },
        ]
        # The sbmdm pattern is strictly longer than shadow_bundle_reach's.
        assert (
            _schema_for_filename(
                "D0259.PY2024.03.SBMDM.D240408.T0804077.csv", 243, patterns
            )
            == "sbmdm"
        )

    @pytest.mark.unit
    def test_falls_back_to_file_type_code(self):
        patterns = [
            {"pattern": "no_match_*", "file_type_code": 999, "schema_name": "mystery"},
        ]
        # Filename doesn't match any pattern — fall back to ftc lookup.
        assert _schema_for_filename("something.xlsx", 999, patterns) == "mystery"

    @pytest.mark.unit
    def test_returns_none_when_nothing_matches_and_no_ftc(self):
        patterns = [
            {"pattern": "no_match_*", "file_type_code": 1, "schema_name": "x"},
        ]
        assert _schema_for_filename("file.xlsx", None, patterns) is None

    @pytest.mark.unit
    def test_returns_none_when_ftc_not_in_patterns(self):
        patterns = [
            {"pattern": "no_match_*", "file_type_code": 1, "schema_name": "x"},
        ]
        assert _schema_for_filename("file.xlsx", 999, patterns) is None


# ----------------------------------------------------------------------------
# build_deliveries_lf
# ----------------------------------------------------------------------------


@pytest.fixture
def state_patterns():
    """Pattern set the state_file fixture was built against."""
    return [
        {
            "pattern": "REACH.D*.BNMR.*.xlsx",
            "file_type_code": 215,
            "schema_name": "reach_bnmr",
        },
        {
            "pattern": "P.D????.TPARC.RP.D??????.T*.txt",
            "file_type_code": 157,
            "schema_name": "tparc",
        },
        {
            "pattern": "P.D????.ALGC??.RP.D??????.T*.xlsx",
            "file_type_code": 159,
            "schema_name": "bar",
        },
        {
            "pattern": "mystery-*",
            "file_type_code": 998,
            "schema_name": "mystery",
        },
    ]


@pytest.fixture
def state_file(tmp_path: Path) -> Path:
    """4icli-shaped state JSON exercising every actual_delivery_source branch.

    Hierarchy is: remote_metadata.created → filename D-stamp → download_timestamp → unknown.
    """
    state = {
        # Has remote_metadata.created → primary source. Filename has a later
        # D-stamp to prove the remote timestamp really does win over it.
        "remote-primary.xlsx": {
            "filename": "REACH.D0259.BNMR.PY2025.D250601.T1428470.xlsx",
            "file_type_code": 215,
            "category": "Reports",
            "remote_metadata": {"created": "2025-05-13T10:15:00.000Z"},
            "download_timestamp": "2025-06-15T00:00:00",
        },
        # No remote_metadata, but filename has a D-stamp → filename secondary.
        "filename-secondary.txt": {
            "filename": "P.D0259.TPARC.RP.D240601.T2209017.txt",
            "file_type_code": 157,
            "category": "Reports",
            "remote_metadata": {},
            "download_timestamp": "2025-10-07T00:00:00",  # much later than filename
        },
        # No remote_metadata and filename has no parseable date → download
        # timestamp is the only signal we have about when the file arrived.
        "download-tertiary.xlsx": {
            "filename": "mystery-no-d-stamp.xlsx",
            "file_type_code": 998,
            "category": "Reports",
            "remote_metadata": None,
            "download_timestamp": "2024-03-20T00:00:00",
        },
        # Nothing resolvable anywhere → actual source = 'unknown'.
        "all-null.xlsx": {
            "filename": "nothing-at-all.xlsx",
            "file_type_code": 998,
            "category": "Reports",
            "remote_metadata": None,
        },
        # Entry without filename → skipped entirely.
        "bogus-entry": {"category": "Reports"},
    }
    path = tmp_path / "4icli_state.json"
    path.write_text(json.dumps(state))
    return path


class TestBuildDeliveriesLf:
    @pytest.mark.unit
    def test_skips_entries_without_filename(self, state_file, state_patterns):
        df = build_deliveries_lf(state_file, patterns=state_patterns).collect()
        # bogus-entry had no filename.
        assert "bogus-entry" not in df["filename"].to_list()

    @pytest.mark.unit
    def test_remote_created_wins_when_present(self, state_file, state_patterns):
        df = build_deliveries_lf(state_file, patterns=state_patterns).collect()
        row = df.filter(pl.col("schema_name") == "reach_bnmr").row(0, named=True)
        # Remote says May 13; filename says June 1 — remote must win.
        assert row["actual_delivery_source"] == "remote_created"
        assert row["actual_delivery_date"] == date(2025, 5, 13)

    @pytest.mark.unit
    def test_filename_date_is_secondary(self, state_file, state_patterns):
        df = build_deliveries_lf(state_file, patterns=state_patterns).collect()
        row = df.filter(pl.col("schema_name") == "tparc").row(0, named=True)
        # Filename says June 1; download_timestamp is Oct 7 — filename wins,
        # because the CMS D-stamp is closer to "when CMS made it available"
        # than the date we pulled it.
        assert row["actual_delivery_source"] == "filename"
        assert row["actual_delivery_date"] == date(2024, 6, 1)

    @pytest.mark.unit
    def test_download_timestamp_is_tertiary(self, state_file, state_patterns):
        df = build_deliveries_lf(state_file, patterns=state_patterns).collect()
        row = df.filter(pl.col("filename") == "mystery-no-d-stamp.xlsx").row(
            0, named=True
        )
        # No remote, filename has no D-stamp — fall back to download timestamp.
        assert row["actual_delivery_source"] == "downloaded"
        assert row["actual_delivery_date"] == date(2024, 3, 20)

    @pytest.mark.unit
    def test_unknown_source_when_everything_missing(self, state_file, state_patterns):
        df = build_deliveries_lf(state_file, patterns=state_patterns).collect()
        row = df.filter(pl.col("filename") == "nothing-at-all.xlsx").row(
            0, named=True
        )
        assert row["actual_delivery_source"] == "unknown"
        assert row["actual_delivery_date"] is None

    @pytest.mark.unit
    def test_default_patterns_load_from_registry(self, state_file, monkeypatch):
        """When no patterns kwarg is supplied the function consults the
        registry. Validate by intercepting the registry call."""
        sentinel = [
            {
                "pattern": "REACH.D*.BNMR.*.xlsx",
                "file_type_code": 215,
                "schema_name": "reach_bnmr_from_registry",
            }
        ]
        monkeypatch.setattr(
            "acoharmony._expressions._reach_calendar_delivery._load_schema_patterns",
            lambda: sentinel,
        )
        df = build_deliveries_lf(state_file).collect()
        bnmr_row = df.filter(
            pl.col("filename") == "REACH.D0259.BNMR.PY2025.D250601.T1428470.xlsx"
        ).row(0, named=True)
        assert bnmr_row["schema_name"] == "reach_bnmr_from_registry"


class TestLoadSchemaPatternsShim:
    @pytest.mark.unit
    def test_returns_registry_patterns_list(self):
        patterns = _load_schema_patterns()
        assert isinstance(patterns, list)
        assert any(p.get("schema_name") == "reach_bnmr" for p in patterns)


# ----------------------------------------------------------------------------
# build_provenance_expr / build_provenance_join
# ----------------------------------------------------------------------------


class TestBuildProvenanceExpr:
    @pytest.mark.unit
    def test_status_classifier_covers_scheduled_branches(self):
        """Scheduled deliveries: expected is non-null, actual is non-null."""
        df = pl.DataFrame(
            {
                "expected": [
                    date(2024, 1, 1),  # on_time (diff = 0)
                    date(2024, 1, 1),  # on_time (diff = +1)
                    date(2024, 1, 1),  # early
                    date(2024, 1, 1),  # late
                ],
                "actual": [
                    date(2024, 1, 1),
                    date(2024, 1, 2),
                    date(2023, 12, 25),
                    date(2024, 1, 15),
                ],
            }
        )
        result = df.with_columns(
            build_provenance_expr(pl.col("expected"), pl.col("actual")).alias("status")
        )
        assert result["status"].to_list() == [
            "on_time",
            "on_time",
            "early",
            "late",
        ]

    @pytest.mark.unit
    def test_status_classifier_flags_unscheduled_when_expected_is_null(self):
        """A delivery with no matching calendar row → unscheduled."""
        df = pl.DataFrame(
            {
                "expected": pl.Series([None], dtype=pl.Date),
                "actual": [date(2024, 7, 15)],
            }
        )
        result = df.with_columns(
            build_provenance_expr(pl.col("expected"), pl.col("actual")).alias("status")
        )
        assert result["status"].to_list() == ["unscheduled"]


class TestBuildProvenanceJoin:
    """The join is delivery-centric: every output row corresponds to a real
    actual delivery. A calendar row that never produced a delivery does NOT
    appear — the whole point is provenance for *things we received*."""

    @pytest.mark.unit
    def test_on_time_and_late_and_early_buckets(self):
        calendar = pl.LazyFrame(
            {
                "schema_name": ["mexpr", "mexpr", "bar"],
                "period": ["M01", "M02", "M06"],
                "py": [2024, 2024, 2024],
                "expected_date": [
                    date(2024, 1, 15),
                    date(2024, 2, 15),
                    date(2024, 6, 10),  # BAR will be early (actual 2024-06-01)
                ],
                "category": ["Finance", "Finance", "Alignment"],
                "description": ["Jan mexpr", "Feb mexpr", "Jun BAR"],
                "calendar_file_date": ["2026-04-01"] * 3,
            }
        )
        deliveries = pl.LazyFrame(
            {
                "filename": ["mexpr_jan.xlsx", "mexpr_feb.xlsx", "bar_jun.xlsx"],
                "schema_name": ["mexpr", "mexpr", "bar"],
                "period": ["M01", "M02", "M06"],
                "py": [2024, 2024, 2024],
                "actual_delivery_date": [
                    date(2024, 1, 15),  # on_time
                    date(2024, 3, 10),  # late
                    date(2024, 6, 1),  # early
                ],
                "actual_delivery_source": [
                    "remote_created",
                    "remote_created",
                    "remote_created",
                ],
            }
        )
        df = build_provenance_join(calendar, deliveries).collect()
        assert df.height == 3
        mexpr_jan = df.filter(pl.col("period") == "M01").row(0, named=True)
        assert mexpr_jan["delivery_status"] == "on_time"
        assert mexpr_jan["delivery_diff_days"] == 0
        mexpr_feb = df.filter(pl.col("period") == "M02").row(0, named=True)
        assert mexpr_feb["delivery_status"] == "late"
        assert mexpr_feb["delivery_diff_days"] == 24
        bar_jun = df.filter(pl.col("period") == "M06").row(0, named=True)
        assert bar_jun["delivery_status"] == "early"
        assert bar_jun["delivery_diff_days"] == -9

    @pytest.mark.unit
    def test_unscheduled_deliveries_come_through_with_null_expected(self):
        """A delivery that isn't on the calendar is still a row — flagged
        unscheduled, with expected_date null."""
        calendar = pl.LazyFrame(
            schema={
                "schema_name": pl.String,
                "period": pl.String,
                "py": pl.Int64,
                "expected_date": pl.Date,
                "category": pl.String,
                "description": pl.String,
                "calendar_file_date": pl.String,
            }
        )
        deliveries = pl.LazyFrame(
            {
                "filename": ["unscheduled.xlsx"],
                "schema_name": ["mexpr"],
                "period": ["M07"],
                "py": [2024],
                "actual_delivery_date": [date(2024, 7, 15)],
                "actual_delivery_source": ["remote_created"],
            }
        )
        df = build_provenance_join(calendar, deliveries).collect()
        assert df.height == 1
        row = df.row(0, named=True)
        assert row["expected_date"] is None
        assert row["actual_delivery_date"] == date(2024, 7, 15)
        assert row["delivery_status"] == "unscheduled"
        # calendar_py is null but delivery_py filled it via coalesce.
        assert row["py"] == 2024

    @pytest.mark.unit
    def test_scheduled_but_not_delivered_calendar_rows_are_dropped(self):
        """Delivery-centric: a scheduled calendar row with no corresponding
        delivery must NOT appear in the output. We track things we received."""
        calendar = pl.LazyFrame(
            {
                "schema_name": ["mexpr", "bar"],
                "period": ["M01", "M02"],
                "py": [2024, 2024],
                "expected_date": [date(2024, 1, 15), date(2024, 2, 20)],
                "category": ["Finance", "Alignment"],
                "description": ["Jan mexpr", "Feb BAR"],
                "calendar_file_date": ["2026-04-01", "2026-04-01"],
            }
        )
        deliveries = pl.LazyFrame(
            {
                "filename": ["mexpr_jan.xlsx"],
                "schema_name": ["mexpr"],
                "period": ["M01"],
                "py": [2024],
                "actual_delivery_date": [date(2024, 1, 15)],
                "actual_delivery_source": ["remote_created"],
            }
        )
        df = build_provenance_join(calendar, deliveries).collect()
        assert df.height == 1
        assert df["schema_name"].to_list() == ["mexpr"]
        # The BAR row had no delivery; it must NOT appear.
        assert "bar" not in df["schema_name"].to_list()
        # And no status should ever be "missing" — the concept is gone.
        assert "missing" not in df["delivery_status"].to_list()

    @pytest.mark.unit
    def test_delivered_filenames_aggregated(self):
        calendar = pl.LazyFrame(
            {
                "schema_name": ["mexpr"],
                "period": ["M03"],
                "py": [2024],
                "expected_date": [date(2024, 3, 25)],
                "category": ["Finance"],
                "description": ["Mar mexpr"],
                "calendar_file_date": ["2026-04-01"],
            }
        )
        deliveries = pl.LazyFrame(
            {
                "filename": ["mexpr_mar_v1.xlsx", "mexpr_mar_v2.xlsx"],
                "schema_name": ["mexpr", "mexpr"],
                "period": ["M03", "M03"],
                "py": [2024, 2024],
                "actual_delivery_date": [date(2024, 3, 26), date(2024, 4, 10)],
                "actual_delivery_source": ["remote_created", "remote_created"],
            }
        )
        df = build_provenance_join(calendar, deliveries).collect()
        row = df.row(0, named=True)
        # Earliest delivery drives the diff.
        assert row["actual_delivery_date"] == date(2024, 3, 26)
        assert row["delivered_file_count"] == 2
        assert set(row["delivered_filenames"]) == {
            "mexpr_mar_v1.xlsx",
            "mexpr_mar_v2.xlsx",
        }

    @pytest.mark.unit
    def test_deliveries_with_null_schema_are_dropped(self):
        """Mirror-image of the calendar filter: a delivery with no classified
        schema can't be matched by report type, so it's excluded."""
        calendar = pl.LazyFrame(
            schema={
                "schema_name": pl.String,
                "period": pl.String,
                "py": pl.Int64,
                "expected_date": pl.Date,
                "category": pl.String,
                "description": pl.String,
                "calendar_file_date": pl.String,
            }
        )
        deliveries = pl.LazyFrame(
            {
                "filename": ["unknowable.xlsx", "mexpr_mar.xlsx"],
                "schema_name": [None, "mexpr"],
                "period": [None, "M03"],
                "py": [None, 2024],
                "actual_delivery_date": [date(2024, 3, 15), date(2024, 3, 25)],
                "actual_delivery_source": ["unknown", "remote_created"],
            }
        )
        df = build_provenance_join(calendar, deliveries).collect()
        assert df.height == 1
        assert df["schema_name"].to_list() == ["mexpr"]

    @pytest.mark.unit
    def test_deliveries_without_actual_date_are_dropped(self):
        """A delivery with no resolvable date (all three hierarchy levels
        failed) can't be bucketed — exclude it rather than emit a null row."""
        calendar = pl.LazyFrame(
            schema={
                "schema_name": pl.String,
                "period": pl.String,
                "py": pl.Int64,
                "expected_date": pl.Date,
                "category": pl.String,
                "description": pl.String,
                "calendar_file_date": pl.String,
            }
        )
        deliveries = pl.LazyFrame(
            {
                "filename": ["dateless.xlsx"],
                "schema_name": ["mexpr"],
                "period": ["M03"],
                "py": [2024],
                "actual_delivery_date": pl.Series([None], dtype=pl.Date),
                "actual_delivery_source": ["unknown"],
            }
        )
        df = build_provenance_join(calendar, deliveries).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_year_based_join_tolerates_calendar_py_mismatch(self):
        """Calendar's ``py`` is the performance year the data is *about*;
        filename-derived ``py`` is the coverage year. They can differ — e.g.
        a January 2024 runout for PY2023 has calendar py=2023 but a filename
        stamped with PY24. The join keys on year-of-date, so these match."""
        calendar = pl.LazyFrame(
            {
                "schema_name": ["mexpr"],
                "period": ["M01"],
                "py": [2023],  # PY the data is about
                "expected_date": [date(2024, 1, 20)],
                "category": ["Finance"],
                "description": ["Jan PY2023 runout"],
                "calendar_file_date": ["2026-04-01"],
            }
        )
        deliveries = pl.LazyFrame(
            {
                "filename": ["PYRED24.D240120.T1.xlsx"],
                "schema_name": ["mexpr"],
                "period": ["M01"],
                "py": [2024],  # coverage year baked into filename
                "actual_delivery_date": [date(2024, 1, 20)],
                "actual_delivery_source": ["remote_created"],
            }
        )
        df = build_provenance_join(calendar, deliveries).collect()
        assert df.height == 1
        row = df.row(0, named=True)
        assert row["delivery_status"] == "on_time"
        assert row["expected_date"] == date(2024, 1, 20)
