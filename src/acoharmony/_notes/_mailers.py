# © 2025 HarmonyCares
# All rights reserved.

"""
Mailer / email-campaign analytics.

Backs ``notebooks/mailers.py``: loads mailed / email / unsubscribe /
beneficiary tables from the catalog, produces campaign + ACO + temporal
rollups, BAR-alignment joins, list-health metrics, and an
"invalid mailings" report (mailings sent after death or enrollment end)
with an Excel export.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

WEEKDAY_MAP = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday",
}

SEND_DATETIME_FMT = "%B %d, %Y, %I:%M %p"


class MailersPlugins(PluginRegistry):
    """Mailer / email-campaign analytics."""

    # ---- catalog loaders ---------------------------------------------

    def _send_date_expr(self, df: pl.DataFrame) -> pl.Expr:
        """``send_date`` parsed from ``send_datetime`` whether it's a string or already temporal."""
        dt = df.schema.get("send_datetime")
        if dt == pl.Utf8:
            return (
                pl.col("send_datetime")
                .str.strptime(pl.Datetime, SEND_DATETIME_FMT)
                .alias("send_date")
            )
        return pl.col("send_datetime").alias("send_date")

    def _bool_expr(self, df: pl.DataFrame, col: str) -> pl.Expr:
        """Coerce a possibly-stringly-typed boolean column."""
        if df.schema.get(col) == pl.Utf8:
            return pl.col(col).str.to_lowercase().eq("true")
        return pl.col(col).cast(pl.Boolean)

    def load_mailed(self) -> pl.DataFrame:
        df = self.catalog.scan_table("mailed").collect()
        return df.with_columns(self._send_date_expr(df))

    def load_emails(self) -> pl.DataFrame:
        df = self.catalog.scan_table("emails").collect()
        return df.with_columns(
            self._send_date_expr(df),
            self._bool_expr(df, "has_been_opened").alias("opened"),
            self._bool_expr(df, "has_been_clicked").alias("clicked"),
        )

    def load_unsubscribes(self) -> pl.DataFrame:
        df = self.catalog.scan_table("email_unsubscribes").collect()
        return df.unique(subset=["email_id", "patient_id", "event_name"])

    def load_pbvar(self) -> pl.DataFrame:
        return (
            self.catalog.scan_table("pbvar")
            .select(["bene_mbi", "aco_id", "file_date"])
            .collect()
        )

    def load_bar(self) -> pl.DataFrame:
        return (
            self.catalog.scan_table("bar")
            .select(
                [
                    "bene_mbi",
                    "newly_aligned_flag",
                    "file_date",
                    "bene_date_of_death",
                    "end_date",
                    "start_date",
                ]
            )
            .collect()
        )

    def load_sva(self) -> pl.DataFrame:
        return (
            self.catalog.scan_table("sva")
            .select(["bene_mbi", "aco_id", "sva_signature_date", "file_date"])
            .collect()
        )

    # ---- mailing rollups ---------------------------------------------

    def mailing_status_counts(self, mailed_df: pl.DataFrame) -> pl.DataFrame:
        return (
            mailed_df.group_by("status")
            .agg(pl.count("letter_id").alias("count"))
            .sort("count", descending=True)
        )

    def campaign_stats(self, mailed_df: pl.DataFrame) -> pl.DataFrame:
        return (
            mailed_df.group_by("campaign_name")
            .agg(
                pl.count("letter_id").alias("letters_sent"),
                pl.col("mbi").n_unique().alias("unique_beneficiaries"),
                pl.col("status")
                .filter(pl.col("status") == "Delivered")
                .count()
                .alias("delivered"),
            )
            .with_columns(
                (pl.col("delivered") / pl.col("letters_sent") * 100)
                .round(1)
                .alias("delivery_rate")
            )
            .sort("letters_sent", descending=True)
        )

    def latest_bar(self, bar_df: pl.DataFrame) -> pl.DataFrame:
        """Filter BAR to its most-recent file_date snapshot, MBI-keyed."""
        if bar_df.is_empty():
            return bar_df.rename({"bene_mbi": "mbi"}) if "bene_mbi" in bar_df.columns else bar_df
        latest = bar_df["file_date"].max()
        return bar_df.filter(pl.col("file_date") == latest).rename(
            {"bene_mbi": "mbi"}
        )

    def latest_sva(self, sva_df: pl.DataFrame) -> pl.DataFrame:
        """One row per MBI: most recent signed SVA."""
        return (
            sva_df.filter(pl.col("sva_signature_date").is_not_null())
            .sort("file_date", descending=True)
            .group_by("bene_mbi")
            .first()
            .rename({"bene_mbi": "mbi"})
            .select(["mbi", "sva_signature_date", "aco_id"])
        )

    def alignment_join(
        self,
        mailed_df: pl.DataFrame,
        bar_df: pl.DataFrame,
        sva_df: pl.DataFrame,
    ) -> dict[str, Any]:
        """Join mailed beneficiaries with current alignment + SVA history."""
        mailed_benes = mailed_df.select(
            pl.col("mbi"),
            pl.col("aco_id").alias("mailed_aco_id"),
            pl.col("send_date"),
            pl.col("campaign_name"),
            pl.col("status"),
        ).unique(subset=["mbi", "campaign_name"])

        latest_bar = self.latest_bar(bar_df)
        with_bar = mailed_benes.join(
            latest_bar.select(["mbi", "newly_aligned_flag", "file_date"]),
            on="mbi",
            how="left",
        ).with_columns(
            pl.when(pl.col("newly_aligned_flag").is_not_null())
            .then(pl.lit("Currently Aligned"))
            .otherwise(pl.lit("Not Aligned"))
            .alias("alignment_status")
        )

        latest_sva = self.latest_sva(sva_df)
        with_sva = with_bar.join(
            latest_sva.select(["mbi", "sva_signature_date"]),
            on="mbi",
            how="left",
            suffix="_sva",
        ).with_columns(
            pl.when(pl.col("sva_signature_date").is_not_null())
            .then(pl.lit("Has SVA"))
            .otherwise(pl.lit("No SVA"))
            .alias("sva_status")
        )

        effectiveness = (
            with_bar.group_by(["campaign_name", "alignment_status"])
            .agg(pl.len().alias("count"))
            .pivot(index="campaign_name", on="alignment_status", values="count")
            .fill_null(0)
        )

        sva_impact = (
            with_sva.group_by(["alignment_status", "sva_status"])
            .agg(pl.len().alias("count"))
            .pivot(index="alignment_status", on="sva_status", values="count")
            .fill_null(0)
        )

        total_mailed = with_sva.height
        has_sva = with_sva.filter(pl.col("sva_status") == "Has SVA")
        sva_aligned = has_sva.filter(pl.col("alignment_status") == "Currently Aligned")
        sva_not_aligned = has_sva.filter(
            pl.col("alignment_status") == "Not Aligned"
        )

        return {
            "with_bar": with_bar,
            "with_sva": with_sva,
            "effectiveness": effectiveness,
            "sva_impact": sva_impact,
            "total_mailed": total_mailed,
            "unique_sva_mbis": has_sva["mbi"].n_unique(),
            "sva_aligned_count": sva_aligned["mbi"].n_unique(),
            "sva_not_aligned_count": sva_not_aligned["mbi"].n_unique(),
        }

    def recent_activity(self, mailed_df: pl.DataFrame) -> pl.DataFrame:
        return (
            mailed_df.with_columns(
                pl.col("send_date").dt.year().alias("year"),
                pl.col("send_date").dt.month().alias("month"),
            )
            .group_by(["year", "month", "aco_id"])
            .agg(
                pl.count("letter_id").alias("letters_sent"),
                pl.col("mbi").n_unique().alias("unique_beneficiaries"),
            )
            .sort(["year", "month"], descending=True)
            .head(12)
        )

    def aco_summary(self, mailed_df: pl.DataFrame) -> pl.DataFrame:
        return (
            mailed_df.group_by("aco_id")
            .agg(
                pl.count("letter_id").alias("total_letters"),
                pl.col("mbi").n_unique().alias("unique_beneficiaries"),
                pl.col("campaign_name").n_unique().alias("campaigns"),
                pl.col("send_date").max().alias("last_mailing"),
            )
            .sort("total_letters", descending=True)
        )

    def performance_summary(
        self, mailed_df: pl.DataFrame, bar_df: pl.DataFrame
    ) -> dict[str, Any]:
        """High-level mailing + alignment KPIs."""
        total = mailed_df.height
        latest_bar = self.latest_bar(bar_df)
        mailed_mbis = set(mailed_df["mbi"].unique().to_list())
        aligned_mbis = (
            set(latest_bar["mbi"].unique().to_list()) if "mbi" in latest_bar.columns else set()
        )
        intersect = len(mailed_mbis & aligned_mbis)
        return {
            "total_mailings": total,
            "unique_beneficiaries": mailed_df["mbi"].n_unique(),
            "delivery_rate": (
                (mailed_df["status"] == "Delivered").sum() / total * 100
                if total
                else 0.0
            ),
            "campaigns": mailed_df["campaign_name"].n_unique(),
            "mailed_and_currently_aligned": intersect,
            "mailed_not_aligned": len(mailed_mbis - aligned_mbis),
            "current_alignment_rate": (
                intersect / len(mailed_mbis) * 100 if mailed_mbis else 0.0
            ),
        }

    # ---- email rollups -----------------------------------------------

    def email_engagement(self, emails_df: pl.DataFrame) -> dict[str, Any]:
        total = emails_df.height
        opens = int(emails_df["opened"].sum() or 0)
        clicks = int(emails_df["clicked"].sum() or 0)
        return {
            "total_emails": total,
            "opens": opens,
            "clicks": clicks,
            "open_rate": (opens / total * 100) if total else 0.0,
            "click_rate": (clicks / total * 100) if total else 0.0,
            "click_to_open_rate": (clicks / opens * 100) if opens else 0.0,
        }

    def campaign_engagement(self, emails_df: pl.DataFrame) -> pl.DataFrame:
        return (
            emails_df.group_by("campaign")
            .agg(
                pl.count("email_id").alias("emails_sent"),
                pl.col("opened").sum().alias("opens"),
                pl.col("clicked").sum().alias("clicks"),
                pl.col("patient_id").n_unique().alias("unique_recipients"),
            )
            .with_columns(
                (pl.col("opens") / pl.col("emails_sent") * 100).round(1).alias("open_rate"),
                (pl.col("clicks") / pl.col("emails_sent") * 100).round(1).alias("click_rate"),
                (pl.col("clicks") / pl.col("opens") * 100).round(1).alias("click_to_open_rate"),
            )
            .sort("emails_sent", descending=True)
        )

    def email_status_breakdown(self, emails_df: pl.DataFrame) -> pl.DataFrame:
        return (
            emails_df.group_by("status")
            .agg(
                pl.count("email_id").alias("count"),
                pl.col("opened").sum().alias("opened_count"),
                pl.col("clicked").sum().alias("clicked_count"),
            )
            .sort("count", descending=True)
        )

    def practice_engagement(self, emails_df: pl.DataFrame, top_n: int = 10) -> pl.DataFrame:
        return (
            emails_df.group_by("practice")
            .agg(
                pl.count("email_id").alias("emails_sent"),
                pl.col("opened").sum().alias("opens"),
                pl.col("clicked").sum().alias("clicks"),
            )
            .with_columns(
                (pl.col("opens") / pl.col("emails_sent") * 100).round(1).alias("open_rate"),
                (pl.col("clicks") / pl.col("emails_sent") * 100).round(1).alias("click_rate"),
            )
            .sort("emails_sent", descending=True)
            .head(top_n)
        )

    def temporal_breakdown(self, emails_df: pl.DataFrame) -> dict[str, pl.DataFrame]:
        # send_date may land as Date (no time component) when the source column
        # is already a date — fall back to hour=0 in that case.
        cast_to_dt = (
            pl.col("send_date").cast(pl.Datetime)
            if emails_df.schema.get("send_date") == pl.Date
            else pl.col("send_date")
        )
        temporal = emails_df.with_columns(
            pl.col("send_date").dt.year().alias("year"),
            pl.col("send_date").dt.month().alias("month"),
            cast_to_dt.dt.hour().alias("hour"),
            pl.col("send_date").dt.weekday().alias("weekday"),
        )
        monthly = (
            temporal.group_by(["year", "month"])
            .agg(
                pl.count("email_id").alias("emails_sent"),
                pl.col("opened").sum().alias("opens"),
                pl.col("clicked").sum().alias("clicks"),
            )
            .with_columns(
                (pl.col("opens") / pl.col("emails_sent") * 100).round(1).alias("open_rate"),
                (pl.col("clicks") / pl.col("emails_sent") * 100).round(1).alias("click_rate"),
            )
            .sort(["year", "month"])
        )
        hourly = (
            temporal.group_by("hour")
            .agg(
                pl.count("email_id").alias("emails_sent"),
                pl.col("opened").mean().alias("avg_open_rate"),
                pl.col("clicked").mean().alias("avg_click_rate"),
            )
            .with_columns(
                (pl.col("avg_open_rate") * 100).round(1).alias("open_rate"),
                (pl.col("avg_click_rate") * 100).round(1).alias("click_rate"),
            )
            .sort("hour")
        )
        weekday = (
            temporal.group_by("weekday")
            .agg(
                pl.count("email_id").alias("emails_sent"),
                pl.col("opened").mean().alias("avg_open_rate"),
                pl.col("clicked").mean().alias("avg_click_rate"),
            )
            .with_columns(
                pl.col("weekday")
                .map_elements(
                    lambda x: WEEKDAY_MAP.get(x, str(x)), return_dtype=pl.Utf8
                )
                .alias("day_name"),
                (pl.col("avg_open_rate") * 100).round(1).alias("open_rate"),
                (pl.col("avg_click_rate") * 100).round(1).alias("click_rate"),
            )
            .sort("weekday")
        )
        return {"monthly": monthly, "hourly": hourly, "weekday": weekday}

    def email_alignment_join(
        self, emails_df: pl.DataFrame, bar_df: pl.DataFrame
    ) -> dict[str, pl.DataFrame]:
        recipients = emails_df.select(
            pl.col("mbi"),
            pl.col("aco_id").alias("email_aco_id"),
            pl.col("campaign"),
            pl.col("opened"),
            pl.col("clicked"),
            pl.col("send_date"),
        ).filter(pl.col("mbi").is_not_null() & (pl.col("mbi") != ""))

        latest_bar = self.latest_bar(bar_df)
        with_alignment = recipients.join(
            latest_bar.select(["mbi", "newly_aligned_flag", "file_date"]),
            on="mbi",
            how="left",
        ).with_columns(
            pl.when(pl.col("newly_aligned_flag").is_not_null())
            .then(pl.lit("Currently Aligned"))
            .otherwise(pl.lit("Not Aligned"))
            .alias("alignment_status")
        )

        engagement_by_alignment = (
            with_alignment.group_by("alignment_status")
            .agg(
                pl.len().alias("emails_sent"),
                pl.col("opened").sum().alias("opens"),
                pl.col("clicked").sum().alias("clicks"),
            )
            .with_columns(
                (pl.col("opens") / pl.col("emails_sent") * 100).round(1).alias("open_rate"),
                (pl.col("clicks") / pl.col("emails_sent") * 100).round(1).alias("click_rate"),
            )
        )

        campaign_alignment = (
            with_alignment.group_by(["campaign", "alignment_status"])
            .agg(
                pl.len().alias("count"),
                pl.col("opened").mean().alias("avg_open_rate"),
                pl.col("clicked").mean().alias("avg_click_rate"),
            )
            .with_columns(
                (pl.col("avg_open_rate") * 100).round(1).alias("open_rate"),
                (pl.col("avg_click_rate") * 100).round(1).alias("click_rate"),
            )
            .pivot(
                index="campaign",
                on="alignment_status",
                values=["count", "open_rate", "click_rate"],
            )
        )

        return {
            "engagement_by_alignment": engagement_by_alignment,
            "campaign_alignment": campaign_alignment,
        }

    # ---- unsubscribe / health -----------------------------------------

    def unsubscribe_summary(
        self, unsubscribes_df: pl.DataFrame, emails_df: pl.DataFrame
    ) -> dict[str, Any]:
        total_events = unsubscribes_df.height
        unique_patients = unsubscribes_df["patient_id"].n_unique()
        unsub = int((unsubscribes_df["event_name"] == "unsubscribed").sum() or 0)
        comp = int((unsubscribes_df["event_name"] == "complained").sum() or 0)
        total_emails = emails_df.height
        return {
            "total_events": total_events,
            "unique_patients": unique_patients,
            "unsubscribes": unsub,
            "complaints": comp,
            "unsubscribe_rate": (unsub / total_emails * 100) if total_emails else 0.0,
            "complaint_rate": (comp / total_emails * 100) if total_emails else 0.0,
        }

    def campaign_unsubscribes(self, unsubscribes_df: pl.DataFrame) -> pl.DataFrame:
        df = unsubscribes_df.filter(pl.col("event_name").is_not_null())
        out = (
            df.group_by(["campaign_name", "event_name"])
            .agg(
                pl.len().alias("event_count"),
                pl.col("patient_id").n_unique().alias("unique_patients"),
            )
            .pivot(
                index="campaign_name",
                on="event_name",
                values=["event_count", "unique_patients"],
            )
            .fill_null(0)
        )
        sort_col = (
            "event_count_unsubscribed"
            if "event_count_unsubscribed" in out.columns
            else out.columns[1]
        )
        return out.sort(sort_col, descending=True)

    def practice_unsubscribes(
        self, unsubscribes_df: pl.DataFrame, top_n: int = 10
    ) -> pl.DataFrame:
        df = unsubscribes_df.filter(pl.col("event_name").is_not_null())
        out = (
            df.group_by(["practice_id", "event_name"])
            .agg(pl.len().alias("count"))
            .pivot(index="practice_id", on="event_name", values="count")
            .fill_null(0)
        )
        cols = out.columns
        unsub_expr = (
            pl.col("unsubscribed").fill_null(0) if "unsubscribed" in cols else pl.lit(0)
        )
        comp_expr = (
            pl.col("complained").fill_null(0) if "complained" in cols else pl.lit(0)
        )
        return (
            out.with_columns((unsub_expr + comp_expr).alias("total_events"))
            .sort("total_events", descending=True)
            .head(top_n)
        )

    def domain_unsubscribes(
        self, unsubscribes_df: pl.DataFrame, top_n: int = 15
    ) -> pl.DataFrame:
        df = unsubscribes_df.filter(pl.col("event_name").is_not_null())
        out = (
            df.with_columns(
                pl.col("email")
                .str.extract(r"@(.+)$", 1)
                .fill_null("unknown")
                .alias("email_domain")
            )
            .group_by(["email_domain", "event_name"])
            .agg(pl.len().alias("count"))
            .pivot(index="email_domain", on="event_name", values="count")
            .fill_null(0)
        )
        cols = out.columns
        unsub_expr = (
            pl.col("unsubscribed").fill_null(0) if "unsubscribed" in cols else pl.lit(0)
        )
        comp_expr = (
            pl.col("complained").fill_null(0) if "complained" in cols else pl.lit(0)
        )
        return (
            out.with_columns((unsub_expr + comp_expr).alias("total_events"))
            .sort("total_events", descending=True)
            .head(top_n)
        )

    def quarterly_trends(self, unsubscribes_df: pl.DataFrame) -> pl.DataFrame:
        df = unsubscribes_df.filter(pl.col("event_name").is_not_null()).with_columns(
            pl.col("campaign_name").str.extract(r"(\d{4})\s+Q(\d+)", 1).alias("year"),
            pl.col("campaign_name").str.extract(r"(\d{4})\s+Q(\d+)", 2).alias("quarter"),
        ).filter(pl.col("year").is_not_null())
        return (
            df.group_by(["year", "quarter", "event_name"])
            .agg(
                pl.len().alias("event_count"),
                pl.col("patient_id").n_unique().alias("unique_patients"),
            )
            .pivot(
                index=["year", "quarter"],
                on="event_name",
                values=["event_count", "unique_patients"],
            )
            .fill_null(0)
            .sort(["year", "quarter"])
        )

    def language_breakdown(self, unsubscribes_df: pl.DataFrame) -> pl.DataFrame:
        return (
            unsubscribes_df.filter(pl.col("event_name").is_not_null()).with_columns(
                pl.when(pl.col("campaign_name").str.contains(r"\(EN\)"))
                .then(pl.lit("English"))
                .when(pl.col("campaign_name").str.contains(r"\(ES\)"))
                .then(pl.lit("Spanish"))
                .otherwise(pl.lit("Other"))
                .alias("language")
            )
            .group_by(["language", "event_name"])
            .agg(pl.len().alias("count"))
            .pivot(index="language", on="event_name", values="count")
            .fill_null(0)
        )

    def list_health(
        self, emails_df: pl.DataFrame, unsubscribes_df: pl.DataFrame
    ) -> dict[str, Any]:
        unsub_patients = set(
            unsubscribes_df.filter(pl.col("event_name") == "unsubscribed")[
                "patient_id"
            ].to_list()
        )
        comp_patients = set(
            unsubscribes_df.filter(pl.col("event_name") == "complained")[
                "patient_id"
            ].to_list()
        )
        if emails_df.is_empty():
            return {
                "total_recipients": 0,
                "engaged_recipients": 0,
                "engagement_rate": 0.0,
                "unsubscribed_recipients": len(unsub_patients),
                "unsubscribe_recipient_rate": 0.0,
                "complained_recipients": len(comp_patients),
                "complaint_recipient_rate": 0.0,
                "healthy_recipients": 0,
                "list_health_score": 0.0,
            }
        total = emails_df["patient_id"].n_unique()
        engaged = (
            emails_df.filter(pl.col("opened") | pl.col("clicked"))["patient_id"].n_unique()
        )
        bad = unsub_patients | comp_patients
        healthy = total - len(bad)
        return {
            "total_recipients": total,
            "engaged_recipients": engaged,
            "engagement_rate": (engaged / total * 100) if total else 0.0,
            "unsubscribed_recipients": len(unsub_patients),
            "unsubscribe_recipient_rate": (
                len(unsub_patients) / total * 100 if total else 0.0
            ),
            "complained_recipients": len(comp_patients),
            "complaint_recipient_rate": (
                len(comp_patients) / total * 100 if total else 0.0
            ),
            "healthy_recipients": healthy,
            "list_health_score": (healthy / total * 100) if total else 0.0,
        }

    # ---- invalid mailings (after death / enrollment end) -------------

    def invalid_mailings(
        self,
        mailed_df: pl.DataFrame,
        emails_df: pl.DataFrame,
        bar_df: pl.DataFrame,
    ) -> dict[str, pl.DataFrame]:
        bar_dates = (
            bar_df.select(["bene_mbi", "bene_date_of_death", "end_date", "file_date"])
            .rename({"bene_mbi": "mbi"})
            .with_columns(
                pl.col("bene_date_of_death").alias("death_date"),
                pl.col("end_date").alias("enrollment_end_date"),
            )
            .sort("file_date", descending=True)
            .group_by("mbi")
            .first()
        )

        mailed_with = (
            mailed_df.select(["mbi", "campaign_name", "send_date", "status"])
            .join(
                bar_dates.select(["mbi", "death_date", "enrollment_end_date"]),
                on="mbi",
                how="left",
            )
            .with_columns(pl.col("send_date").dt.date().alias("send_date_only"))
        )

        mailed_after_death = (
            mailed_with.filter(
                pl.col("death_date").is_not_null()
                & (pl.col("send_date_only") > pl.col("death_date"))
            )
            .select(
                ["mbi", "campaign_name", "send_date_only", "death_date", "status"]
            )
            .with_columns(
                (pl.col("send_date_only") - pl.col("death_date"))
                .dt.total_days()
                .alias("days_after_death")
            )
        )

        mailed_after_end = (
            mailed_with.filter(
                pl.col("enrollment_end_date").is_not_null()
                & (pl.col("send_date_only") > pl.col("enrollment_end_date"))
            )
            .select(
                [
                    "mbi",
                    "campaign_name",
                    "send_date_only",
                    "enrollment_end_date",
                    "status",
                ]
            )
            .with_columns(
                (pl.col("send_date_only") - pl.col("enrollment_end_date"))
                .dt.total_days()
                .alias("days_after_end")
            )
        )

        emails_with = (
            emails_df.filter(pl.col("mbi").is_not_null() & (pl.col("mbi") != ""))
            .select(["mbi", "campaign", "send_date", "status", "opened", "clicked"])
            .join(
                bar_dates.select(["mbi", "death_date", "enrollment_end_date"]),
                on="mbi",
                how="left",
            )
            .with_columns(pl.col("send_date").dt.date().alias("send_date_only"))
        )

        emails_after_death = (
            emails_with.filter(
                pl.col("death_date").is_not_null()
                & (pl.col("send_date_only") > pl.col("death_date"))
            )
            .select(
                [
                    "mbi",
                    "campaign",
                    "send_date_only",
                    "death_date",
                    "status",
                    "opened",
                    "clicked",
                ]
            )
            .with_columns(
                (pl.col("send_date_only") - pl.col("death_date"))
                .dt.total_days()
                .alias("days_after_death")
            )
        )

        emails_after_end = (
            emails_with.filter(
                pl.col("enrollment_end_date").is_not_null()
                & (pl.col("send_date_only") > pl.col("enrollment_end_date"))
            )
            .select(
                [
                    "mbi",
                    "campaign",
                    "send_date_only",
                    "enrollment_end_date",
                    "status",
                    "opened",
                    "clicked",
                ]
            )
            .with_columns(
                (pl.col("send_date_only") - pl.col("enrollment_end_date"))
                .dt.total_days()
                .alias("days_after_end")
            )
        )

        return {
            "mailed_after_death": mailed_after_death,
            "mailed_after_end": mailed_after_end,
            "emails_after_death": emails_after_death,
            "emails_after_end": emails_after_end,
        }

    def invalid_summary_df(
        self, invalid: dict[str, pl.DataFrame]
    ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "Report Type": [
                    "Letters After Death",
                    "Letters After End Date",
                    "Emails After Death",
                    "Emails After End Date",
                ],
                "Record Count": [
                    invalid["mailed_after_death"].height,
                    invalid["mailed_after_end"].height,
                    invalid["emails_after_death"].height,
                    invalid["emails_after_end"].height,
                ],
                "Unique Beneficiaries": [
                    (
                        invalid["mailed_after_death"]["mbi"].n_unique()
                        if invalid["mailed_after_death"].height
                        else 0
                    ),
                    (
                        invalid["mailed_after_end"]["mbi"].n_unique()
                        if invalid["mailed_after_end"].height
                        else 0
                    ),
                    (
                        invalid["emails_after_death"]["mbi"].n_unique()
                        if invalid["emails_after_death"].height
                        else 0
                    ),
                    (
                        invalid["emails_after_end"]["mbi"].n_unique()
                        if invalid["emails_after_end"].height
                        else 0
                    ),
                ],
            }
        )

    def export_invalid_mailings(
        self,
        invalid: dict[str, pl.DataFrame],
        gold_path: Path,
    ) -> tuple[Path, list[str]]:
        """Write the multi-sheet ``invalid_mailings_report.xlsx``."""
        excel_path = Path(gold_path) / "invalid_mailings_report.xlsx"
        sheets: list[str] = []

        summary = self.invalid_summary_df(invalid)
        workbook = summary.write_excel(
            workbook=str(excel_path),
            worksheet="Summary",
            table_style="Table Style Medium 1",
            column_widths={"Report Type": 30, "Record Count": 15, "Unique Beneficiaries": 20},
        )
        sheets.append("Summary")

        sheet_specs = [
            ("mailed_after_death", "Letters After Death", "Table Style Medium 2",
             {"mbi": 15, "campaign_name": 40, "days_after_death": 20}),
            ("mailed_after_end", "Letters After End Date", "Table Style Medium 3",
             {"mbi": 15, "campaign_name": 40, "days_after_end": 20}),
            ("emails_after_death", "Emails After Death", "Table Style Medium 4",
             {"mbi": 15, "campaign": 30, "days_after_death": 20}),
            ("emails_after_end", "Emails After End Date", "Table Style Medium 5",
             {"mbi": 15, "campaign": 30, "days_after_end": 20}),
        ]
        for key, sheet_name, style, widths in sheet_specs:
            df = invalid[key]
            if df.height == 0:
                continue
            df.write_excel(
                workbook=workbook,
                worksheet=sheet_name,
                table_style=style,
                column_widths=widths,
            )
            sheets.append(f"{sheet_name}: {df.height:,} records")

        return excel_path, sheets
