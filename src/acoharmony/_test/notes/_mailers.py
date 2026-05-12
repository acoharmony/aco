# © 2025 HarmonyCares
"""Tests for acoharmony._notes._mailers (MailersPlugins)."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from acoharmony._notes import MailersPlugins


def _mailed_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["M1", "M2", "M3", "M1"],
            "letter_id": ["L1", "L2", "L3", "L4"],
            "aco_id": ["A1", "A1", "A2", "A1"],
            "campaign_name": ["C1 (EN)", "C2 (ES)", "C1 (EN)", "C1 (EN)"],
            "status": ["Delivered", "Returned", "Delivered", "Delivered"],
            "send_date": [
                datetime(2024, 1, 5, 9),
                datetime(2024, 2, 1, 10),
                datetime(2024, 3, 1, 11),
                datetime(2024, 6, 1, 12),
            ],
        }
    )


def _emails_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["M1", "M2", "M3", ""],
            "email_id": ["E1", "E2", "E3", "E4"],
            "patient_id": ["P1", "P2", "P3", "P4"],
            "aco_id": ["A1", "A1", "A2", "A1"],
            "campaign": ["C1", "C2", "C1", "C2"],
            "practice": ["PR1", "PR1", "PR2", "PR2"],
            "status": ["Delivered", "Bounced", "Delivered", "Delivered"],
            "opened": [True, False, True, False],
            "clicked": [True, False, False, False],
            "send_date": [
                datetime(2024, 1, 5, 9),
                datetime(2024, 1, 8, 14),
                datetime(2024, 2, 5, 10),
                datetime(2024, 3, 12, 16),
            ],
        }
    )


def _bar_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "bene_mbi": ["M1", "M2"],
            "newly_aligned_flag": ["Y", "Y"],
            "file_date": [date(2024, 6, 1), date(2024, 6, 1)],
            "bene_date_of_death": [date(2024, 1, 1), None],
            "end_date": [None, date(2024, 1, 15)],
            "start_date": [date(2023, 1, 1), date(2023, 1, 1)],
        }
    )


def _sva_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "bene_mbi": ["M1", "M3"],
            "aco_id": ["A1", "A2"],
            "sva_signature_date": [date(2024, 1, 1), date(2024, 2, 1)],
            "file_date": [date(2024, 1, 5), date(2024, 2, 5)],
        }
    )


def _unsub_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "email_id": ["E1", "E2", "E3"],
            "patient_id": ["P1", "P2", "P3"],
            "event_name": ["unsubscribed", "complained", "unsubscribed"],
            "campaign_name": ["2024 Q1 (EN)", "2024 Q2 (ES)", "2024 Q1 (EN)"],
            "practice_id": ["PR1", "PR1", "PR2"],
            "email": ["a@gmail.com", "b@yahoo.com", "c@gmail.com"],
        }
    )


def _stub_catalog(plugin: MailersPlugins, table_map: dict[str, pl.DataFrame]) -> None:
    fake = MagicMock()

    def _scan(name: str) -> pl.LazyFrame:
        return table_map[name].lazy()

    fake.scan_table.side_effect = _scan
    plugin._catalog = fake


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


class TestLoaders:
    @pytest.mark.unit
    def test_load_mailed(self) -> None:
        plugin = MailersPlugins()
        raw = pl.DataFrame(
            {
                "mbi": ["M1"],
                "send_datetime": ["June 01, 2024, 09:00 AM"],
                "letter_id": ["L1"],
                "aco_id": ["A"],
                "campaign_name": ["C"],
                "status": ["Delivered"],
            }
        )
        _stub_catalog(plugin, {"mailed": raw})
        out = plugin.load_mailed()
        assert "send_date" in out.columns
        assert out["send_date"][0].year == 2024

    @pytest.mark.unit
    def test_load_emails(self) -> None:
        plugin = MailersPlugins()
        raw = pl.DataFrame(
            {
                "mbi": ["M1"],
                "send_datetime": ["June 01, 2024, 09:00 AM"],
                "email_id": ["E1"],
                "patient_id": ["P1"],
                "aco_id": ["A"],
                "campaign": ["C"],
                "practice": ["P"],
                "status": ["Delivered"],
                "has_been_opened": ["true"],
                "has_been_clicked": ["FALSE"],
            }
        )
        _stub_catalog(plugin, {"emails": raw})
        out = plugin.load_emails()
        assert out["opened"][0] is True
        assert out["clicked"][0] is False

    @pytest.mark.unit
    def test_load_mailed_already_datetime(self) -> None:
        plugin = MailersPlugins()
        raw = pl.DataFrame(
            {
                "mbi": ["M1"],
                "send_datetime": [datetime(2024, 6, 1, 9, 0)],
                "letter_id": ["L1"],
                "aco_id": ["A"],
                "campaign_name": ["C"],
                "status": ["Delivered"],
            }
        )
        _stub_catalog(plugin, {"mailed": raw})
        out = plugin.load_mailed()
        assert out["send_date"][0] == datetime(2024, 6, 1, 9, 0)

    @pytest.mark.unit
    def test_load_emails_already_bool_and_datetime(self) -> None:
        plugin = MailersPlugins()
        raw = pl.DataFrame(
            {
                "mbi": ["M1"],
                "send_datetime": [datetime(2024, 6, 1, 9, 0)],
                "email_id": ["E1"],
                "patient_id": ["P1"],
                "aco_id": ["A"],
                "campaign": ["C"],
                "practice": ["P"],
                "status": ["Delivered"],
                "has_been_opened": [True],
                "has_been_clicked": [False],
            }
        )
        _stub_catalog(plugin, {"emails": raw})
        out = plugin.load_emails()
        assert out["opened"][0] is True
        assert out["clicked"][0] is False

    @pytest.mark.unit
    def test_load_unsubscribes_dedupes(self) -> None:
        plugin = MailersPlugins()
        raw = pl.DataFrame(
            {
                "email_id": ["E1", "E1"],
                "patient_id": ["P1", "P1"],
                "event_name": ["unsubscribed", "unsubscribed"],
            }
        )
        _stub_catalog(plugin, {"email_unsubscribes": raw})
        assert plugin.load_unsubscribes().height == 1

    @pytest.mark.unit
    def test_load_pbvar(self) -> None:
        plugin = MailersPlugins()
        _stub_catalog(
            plugin,
            {
                "pbvar": pl.DataFrame(
                    {"bene_mbi": ["M1"], "aco_id": ["A1"], "file_date": [date(2024, 1, 1)]}
                )
            },
        )
        out = plugin.load_pbvar()
        assert out.height == 1

    @pytest.mark.unit
    def test_load_bar(self) -> None:
        plugin = MailersPlugins()
        _stub_catalog(plugin, {"bar": _bar_df()})
        out = plugin.load_bar()
        assert "bene_mbi" in out.columns

    @pytest.mark.unit
    def test_load_sva(self) -> None:
        plugin = MailersPlugins()
        _stub_catalog(plugin, {"sva": _sva_df()})
        out = plugin.load_sva()
        assert out.height == 2


# ---------------------------------------------------------------------------
# Mailing rollups
# ---------------------------------------------------------------------------


class TestMailingRollups:
    @pytest.mark.unit
    def test_status_counts(self) -> None:
        out = MailersPlugins().mailing_status_counts(_mailed_df())
        as_dict = {row["status"]: row["count"] for row in out.iter_rows(named=True)}
        assert as_dict["Delivered"] == 3
        assert as_dict["Returned"] == 1

    @pytest.mark.unit
    def test_campaign_stats(self) -> None:
        out = MailersPlugins().campaign_stats(_mailed_df())
        as_dict = {row["campaign_name"]: row for row in out.iter_rows(named=True)}
        assert as_dict["C1 (EN)"]["letters_sent"] == 3
        assert as_dict["C1 (EN)"]["delivered"] == 3
        assert as_dict["C1 (EN)"]["delivery_rate"] == 100.0

    @pytest.mark.unit
    def test_latest_bar_filters(self) -> None:
        bar = _bar_df()
        old = bar.with_columns(pl.lit(date(2023, 1, 1)).alias("file_date"))
        combined = pl.concat([bar, old])
        out = MailersPlugins().latest_bar(combined)
        assert out["file_date"].unique().to_list() == [date(2024, 6, 1)]
        assert "mbi" in out.columns

    @pytest.mark.unit
    def test_latest_bar_empty(self) -> None:
        empty = pl.DataFrame(schema=_bar_df().schema)
        out = MailersPlugins().latest_bar(empty)
        assert out.is_empty()

    @pytest.mark.unit
    def test_latest_sva_picks_latest(self) -> None:
        sva = pl.DataFrame(
            {
                "bene_mbi": ["M1", "M1"],
                "aco_id": ["A1", "A1"],
                "sva_signature_date": [date(2024, 1, 1), date(2024, 6, 1)],
                "file_date": [date(2024, 1, 5), date(2024, 6, 5)],
            }
        )
        out = MailersPlugins().latest_sva(sva)
        assert out.height == 1
        assert out["sva_signature_date"][0] == date(2024, 6, 1)

    @pytest.mark.unit
    def test_alignment_join(self) -> None:
        out = MailersPlugins().alignment_join(_mailed_df(), _bar_df(), _sva_df())
        assert out["total_mailed"] >= 3
        assert "with_bar" in out
        assert "effectiveness" in out

    @pytest.mark.unit
    def test_recent_activity_top_12(self) -> None:
        out = MailersPlugins().recent_activity(_mailed_df())
        assert out.height <= 12

    @pytest.mark.unit
    def test_aco_summary(self) -> None:
        out = MailersPlugins().aco_summary(_mailed_df())
        as_dict = {row["aco_id"]: row for row in out.iter_rows(named=True)}
        assert as_dict["A1"]["total_letters"] == 3

    @pytest.mark.unit
    def test_performance_summary(self) -> None:
        out = MailersPlugins().performance_summary(_mailed_df(), _bar_df())
        assert out["total_mailings"] == 4
        assert out["delivery_rate"] == pytest.approx(75.0)
        assert out["unique_beneficiaries"] == 3
        assert out["mailed_and_currently_aligned"] == 2

    @pytest.mark.unit
    def test_performance_summary_empty(self) -> None:
        out = MailersPlugins().performance_summary(
            pl.DataFrame(
                schema={
                    "mbi": pl.Utf8,
                    "campaign_name": pl.Utf8,
                    "status": pl.Utf8,
                }
            ),
            _bar_df(),
        )
        assert out["delivery_rate"] == 0.0
        assert out["current_alignment_rate"] == 0.0


# ---------------------------------------------------------------------------
# Email rollups
# ---------------------------------------------------------------------------


class TestEmailRollups:
    @pytest.mark.unit
    def test_email_engagement(self) -> None:
        out = MailersPlugins().email_engagement(_emails_df())
        assert out["total_emails"] == 4
        assert out["opens"] == 2
        assert out["clicks"] == 1
        assert out["click_to_open_rate"] == pytest.approx(50.0)

    @pytest.mark.unit
    def test_email_engagement_empty(self) -> None:
        empty = _emails_df().head(0)
        out = MailersPlugins().email_engagement(empty)
        assert out["open_rate"] == 0
        assert out["click_to_open_rate"] == 0

    @pytest.mark.unit
    def test_campaign_engagement(self) -> None:
        out = MailersPlugins().campaign_engagement(_emails_df())
        as_dict = {row["campaign"]: row for row in out.iter_rows(named=True)}
        assert as_dict["C1"]["emails_sent"] == 2

    @pytest.mark.unit
    def test_email_status_breakdown(self) -> None:
        out = MailersPlugins().email_status_breakdown(_emails_df())
        as_dict = {row["status"]: row for row in out.iter_rows(named=True)}
        assert as_dict["Delivered"]["count"] == 3

    @pytest.mark.unit
    def test_practice_engagement(self) -> None:
        out = MailersPlugins().practice_engagement(_emails_df())
        as_dict = {row["practice"]: row for row in out.iter_rows(named=True)}
        assert as_dict["PR1"]["emails_sent"] == 2

    @pytest.mark.unit
    def test_temporal_breakdown(self) -> None:
        out = MailersPlugins().temporal_breakdown(_emails_df())
        assert "monthly" in out
        assert "hourly" in out
        assert "weekday" in out
        assert out["monthly"].height >= 1
        # weekday names mapped
        names = out["weekday"]["day_name"].to_list()
        assert any(n in WEEKDAYS for n in names)

    @pytest.mark.unit
    def test_email_alignment_join(self) -> None:
        out = MailersPlugins().email_alignment_join(_emails_df(), _bar_df())
        assert "engagement_by_alignment" in out
        assert out["engagement_by_alignment"].height >= 1


WEEKDAYS = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}


# ---------------------------------------------------------------------------
# Unsubscribe / health
# ---------------------------------------------------------------------------


class TestUnsubscribes:
    @pytest.mark.unit
    def test_summary(self) -> None:
        out = MailersPlugins().unsubscribe_summary(_unsub_df(), _emails_df())
        assert out["unsubscribes"] == 2
        assert out["complaints"] == 1

    @pytest.mark.unit
    def test_summary_no_emails(self) -> None:
        empty = _emails_df().head(0)
        out = MailersPlugins().unsubscribe_summary(_unsub_df(), empty)
        assert out["unsubscribe_rate"] == 0.0

    @pytest.mark.unit
    def test_campaign_unsubscribes(self) -> None:
        out = MailersPlugins().campaign_unsubscribes(_unsub_df())
        assert out.height == 2

    @pytest.mark.unit
    def test_practice_unsubscribes(self) -> None:
        out = MailersPlugins().practice_unsubscribes(_unsub_df())
        assert out.height == 2
        assert "total_events" in out.columns

    @pytest.mark.unit
    def test_practice_unsubscribes_missing_event_col(self) -> None:
        # All events are unsubscribed → no "complained" column after pivot
        df = pl.DataFrame(
            {
                "patient_id": ["P1", "P2"],
                "practice_id": ["PR1", "PR2"],
                "event_name": ["unsubscribed", "unsubscribed"],
                "campaign_name": ["X", "Y"],
                "email": ["a@x.com", "b@y.com"],
                "email_id": ["E1", "E2"],
            }
        )
        out = MailersPlugins().practice_unsubscribes(df)
        assert "total_events" in out.columns

    @pytest.mark.unit
    def test_domain_unsubscribes(self) -> None:
        out = MailersPlugins().domain_unsubscribes(_unsub_df())
        domains = sorted(out["email_domain"].to_list())
        # Equality (rather than `"gmail.com" in domains`) avoids CodeQL's
        # py/incomplete-url-substring-sanitization false positive on the
        # in-operator pattern. These are email-address suffixes, not URLs,
        # so there's no real sanitization concern.
        assert domains == ["gmail.com", "yahoo.com"]

    @pytest.mark.unit
    def test_domain_unsubscribes_missing_event_col(self) -> None:
        df = pl.DataFrame(
            {
                "patient_id": ["P1"],
                "event_name": ["complained"],
                "email": ["x@gmail.com"],
                "email_id": ["E1"],
                "campaign_name": ["C"],
                "practice_id": ["PR"],
            }
        )
        out = MailersPlugins().domain_unsubscribes(df)
        assert "total_events" in out.columns

    @pytest.mark.unit
    def test_quarterly_trends(self) -> None:
        out = MailersPlugins().quarterly_trends(_unsub_df())
        assert out.height >= 1

    @pytest.mark.unit
    def test_language_breakdown(self) -> None:
        out = MailersPlugins().language_breakdown(_unsub_df())
        langs = out["language"].to_list()
        assert "English" in langs
        assert "Spanish" in langs

    @pytest.mark.unit
    def test_list_health(self) -> None:
        out = MailersPlugins().list_health(_emails_df(), _unsub_df())
        assert out["total_recipients"] == 4
        assert out["engaged_recipients"] >= 1
        assert out["list_health_score"] > 0

    @pytest.mark.unit
    def test_list_health_empty_emails(self) -> None:
        empty = _emails_df().head(0)
        out = MailersPlugins().list_health(empty, _unsub_df())
        assert out["total_recipients"] == 0
        assert out["list_health_score"] == 0


# ---------------------------------------------------------------------------
# Invalid mailings
# ---------------------------------------------------------------------------


class TestInvalidMailings:
    @pytest.mark.unit
    def test_after_death_and_end(self) -> None:
        out = MailersPlugins().invalid_mailings(_mailed_df(), _emails_df(), _bar_df())
        # M1 died 2024-01-01; mailed 2024-01-05 + 2024-06-01 → 2 letters after death
        assert out["mailed_after_death"].height == 2
        # M2 enrollment ended 2024-01-15; mailed 2024-02-01 → 1 letter
        assert out["mailed_after_end"].height == 1
        assert out["emails_after_death"].height == 1
        # M2's email (2024-01-08) is before its end_date (2024-01-15) → 0
        assert out["emails_after_end"].height == 0

    @pytest.mark.unit
    def test_summary_df(self) -> None:
        plugin = MailersPlugins()
        invalid = plugin.invalid_mailings(_mailed_df(), _emails_df(), _bar_df())
        out = plugin.invalid_summary_df(invalid)
        assert out.height == 4
        assert out.columns == ["Report Type", "Record Count", "Unique Beneficiaries"]

    @pytest.mark.unit
    def test_summary_df_empty_categories(self) -> None:
        plugin = MailersPlugins()
        empty = {
            "mailed_after_death": pl.DataFrame(schema={"mbi": pl.Utf8}),
            "mailed_after_end": pl.DataFrame(schema={"mbi": pl.Utf8}),
            "emails_after_death": pl.DataFrame(schema={"mbi": pl.Utf8}),
            "emails_after_end": pl.DataFrame(schema={"mbi": pl.Utf8}),
        }
        out = plugin.invalid_summary_df(empty)
        assert out["Unique Beneficiaries"].to_list() == [0, 0, 0, 0]

    @pytest.mark.unit
    def test_export_writes_workbook(self, tmp_path: Path) -> None:
        plugin = MailersPlugins()
        invalid = plugin.invalid_mailings(_mailed_df(), _emails_df(), _bar_df())
        out_path, sheets = plugin.export_invalid_mailings(invalid, tmp_path)
        assert out_path.exists()
        assert "Summary" in sheets
        # The non-empty buckets are written
        joined = " | ".join(sheets)
        assert "Letters After Death" in joined

    @pytest.mark.unit
    def test_export_skips_empty(self, tmp_path: Path) -> None:
        plugin = MailersPlugins()
        empty = {
            "mailed_after_death": pl.DataFrame(schema={"mbi": pl.Utf8, "campaign_name": pl.Utf8, "send_date_only": pl.Date, "death_date": pl.Date, "status": pl.Utf8, "days_after_death": pl.Int64}),
            "mailed_after_end": pl.DataFrame(schema={"mbi": pl.Utf8, "campaign_name": pl.Utf8, "send_date_only": pl.Date, "enrollment_end_date": pl.Date, "status": pl.Utf8, "days_after_end": pl.Int64}),
            "emails_after_death": pl.DataFrame(schema={"mbi": pl.Utf8, "campaign": pl.Utf8, "send_date_only": pl.Date, "death_date": pl.Date, "status": pl.Utf8, "opened": pl.Boolean, "clicked": pl.Boolean, "days_after_death": pl.Int64}),
            "emails_after_end": pl.DataFrame(schema={"mbi": pl.Utf8, "campaign": pl.Utf8, "send_date_only": pl.Date, "enrollment_end_date": pl.Date, "status": pl.Utf8, "opened": pl.Boolean, "clicked": pl.Boolean, "days_after_end": pl.Int64}),
        }
        out_path, sheets = plugin.export_invalid_mailings(empty, tmp_path)
        assert sheets == ["Summary"]
        assert out_path.exists()
