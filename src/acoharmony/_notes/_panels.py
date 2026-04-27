# © 2025 HarmonyCares
# All rights reserved.

"""
Composed panels: notebook-shaped marimo widgets.

Each method returns a single marimo widget (callout / vstack / Html) that
the notebook drops into a tab. Notebooks stay declarative: pass dataframes,
get a panel back.
"""

from __future__ import annotations

from typing import Literal

import polars as pl

from ._base import PluginRegistry
from ._ui import UIPlugins

ClaimKind = Literal["medical", "pharmacy"]

_STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
    "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
    "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
    "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY",
}


class PanelPlugins(PluginRegistry):
    """High-level marimo widget builders for patient and claims notebooks."""

    def __init__(self, ui: UIPlugins) -> None:
        super().__init__()
        self._ui = ui

    # ---- patient -------------------------------------------------------

    def identity_panel(
        self,
        mbi: str,
        hcmpi: str | None,
        current_mbi: str,
        history: list[str],
        demographics_df: pl.DataFrame | None,
    ):
        rows: list[tuple[str, str]] = [
            ("Query MBI", mbi),
            ("HCMPI", hcmpi or "Not Mapped"),
            ("Current MBI", current_mbi),
            ("Historical MBIs", ", ".join(history) or "None"),
        ]
        if demographics_df is not None and demographics_df.height > 0:
            row = demographics_df.to_dicts()[0]
            sex_code = str(row.get("bene_sex_cd", ""))
            sex = (
                "Male" if sex_code == "1"
                else "Female" if sex_code == "2"
                else sex_code
            )
            state = _STATE_FIPS_TO_ABBR.get(str(row.get("bene_fips_state_cd", "")), "")
            rows.extend(
                [
                    ("DOB", str(row.get("bene_dob")) if row.get("bene_dob") else "—"),
                    ("Sex", sex or "—"),
                    ("Age", str(row.get("bene_age")) if row.get("bene_age") is not None else "—"),
                    ("State", state or "—"),
                    ("ZIP", str(row.get("bene_zip_cd")) if row.get("bene_zip_cd") else "—"),
                    ("Medicare Status", str(row.get("bene_mdcr_stus_cd")) if row.get("bene_mdcr_stus_cd") else "—"),
                    ("Dual Status", str(row.get("bene_dual_stus_cd")) if row.get("bene_dual_stus_cd") else "—"),
                ]
            )
        else:
            rows.append(("Demographics", "Not found in beneficiary table"))

        body = "".join(
            f"<tr><td style='padding:0.4rem 0.75rem; font-weight:600; color:#1E3A8A;'>{label}</td>"
            f"<td style='padding:0.4rem 0.75rem;'>{value}</td></tr>"
            for label, value in rows
        )
        return self.mo.Html(
            f"""
            <div style="padding: 1rem; background: #EFF6FF; border-left: 4px solid #3B82F6; margin: 1rem 0;">
                <h3 style="margin-top: 0; color: #1E40AF;">Identity & Demographics</h3>
                <table style="width: 100%; border-collapse: collapse;">{body}</table>
            </div>
            """
        )

    def alignment_panel(self, alignment_df: pl.DataFrame | None):
        if alignment_df is None or alignment_df.height == 0:
            return self.mo.callout("No alignment record found for this MBI.", kind="warn")

        row = alignment_df.to_dicts()[0]
        # Fold ym_<YYYYMM>_<program> columns into year × program counts.
        year_program: dict[str, dict[str, int]] = {}
        for col in alignment_df.columns:
            if not col.startswith("ym_") or col.endswith("_first_claim"):
                continue
            parts = col.split("_")
            if len(parts) < 3 or len(parts[1]) != 6:
                continue
            year = parts[1][:4]
            program = parts[2]
            if not row.get(col):
                continue
            year_program.setdefault(year, {"reach": 0, "mssp": 0, "ffs": 0})
            if program in year_program[year]:
                year_program[year][program] += 1

        if year_program:
            yearly_df = pl.DataFrame(
                [
                    {"year": y, "reach": p["reach"], "mssp": p["mssp"], "ffs": p["ffs"]}
                    for y, p in sorted(year_program.items(), reverse=True)
                ]
            )
            yearly_view = self.mo.ui.table(yearly_df, selection=None)
        else:
            yearly_view = self.mo.md("_No aligned months found._")

        sections: list = [self.mo.md("**Aligned months by program**"), yearly_view]
        if row.get("has_voluntary_alignment"):
            sections.append(
                self.mo.callout(
                    self.mo.md(
                        f"**Voluntary Alignment:** {row.get('voluntary_alignment_type', 'Unknown')}  \n"
                        f"**Voluntary Date:** {row.get('voluntary_alignment_date', 'Unknown')}  \n"
                        f"**Provider:** {row.get('voluntary_provider_name', 'Unknown')} "
                        f"(NPI: {row.get('voluntary_provider_npi', 'N/A')})"
                    ),
                    kind="info",
                )
            )
        if row.get("has_valid_voluntary_alignment") or row.get("has_valid_historical_sva"):
            tags = []
            if row.get("has_valid_voluntary_alignment"):
                tags.append("Valid Voluntary SVA")
            if row.get("has_valid_historical_sva"):
                tags.append("Valid Historical SVA")
            sections.append(
                self.mo.callout(
                    self.mo.md(
                        f"**SVA Status:** {', '.join(tags) or 'No Valid SVA'}  \n"
                        f"**First SVA Submission:** {row.get('first_sva_submission_date', 'N/A')}  \n"
                        f"**Last SVA Submission:** {row.get('last_sva_submission_date', 'N/A')}  \n"
                        f"**Action Needed:** {row.get('sva_action_needed', 'None')}"
                    ),
                    kind="info",
                )
            )
        return self.mo.vstack(sections)

    def conditions_panel(self, conditions_df: pl.DataFrame | None):
        if conditions_df is None or conditions_df.height == 0:
            return self.mo.callout("No chronic conditions record found.", kind="info")
        row = conditions_df.to_dicts()[0]
        active = [
            col
            for col, val in row.items()
            if col not in ("person_id", "condition_count") and val in (True, 1, "1")
        ]
        bullets = "".join(f"<li>{c}</li>" for c in active) or "<li>None</li>"
        return self.mo.Html(
            f"""
            <div style="padding: 1rem; background: #FEF3C7; border-left: 4px solid #D97706;">
                <h3 style="margin-top: 0; color: #78350F;">Chronic Conditions</h3>
                <p><strong>Active Conditions:</strong> {len(active)}</p>
                <ul>{bullets}</ul>
            </div>
            """
        )

    def spend_util_panel(self, yearly_df: pl.DataFrame):
        if yearly_df.height == 0:
            return self.mo.callout(
                "No medical or pharmacy claims found for this patient.", kind="info"
            )
        spend_view = yearly_df.select(
            "year",
            pl.col("inpatient_spend").round(2),
            pl.col("outpatient_spend").round(2),
            pl.col("snf_spend").round(2),
            pl.col("hospice_spend").round(2),
            pl.col("home_health_spend").round(2),
            pl.col("part_b_carrier_spend").round(2),
            pl.col("pharmacy_spend").round(2),
            pl.col("total_spend").round(2),
        )
        util_view = yearly_df.select(
            "year",
            "ip_admissions",
            "er_visits",
            "em_visits",
            "awv_visits",
            "pharmacy_claims_count",
        )
        return self.mo.vstack(
            [
                self.mo.md("**Spend by category × year**"),
                self.mo.ui.table(spend_view, selection=None),
                self.mo.md("**Utilization × year**"),
                self.mo.ui.table(util_view, selection=None),
            ]
        )

    def claim_lines_panel(
        self,
        medical_df: pl.DataFrame | None,
        pharmacy_df: pl.DataFrame | None,
    ):
        sections: list = []
        if medical_df is not None and medical_df.height > 0:
            sections.append(
                self.mo.vstack(
                    [
                        self.mo.md(f"**🏥 Medical claim lines: {medical_df.height:,}**"),
                        self._ui.csv_download(
                            medical_df, "📥 Export Medical Lines CSV", "patient_medical_lines.csv"
                        ),
                        self.mo.ui.table(medical_df, pagination=True, page_size=20),
                    ]
                )
            )
        else:
            sections.append(
                self.mo.callout("No medical claim lines for this patient.", kind="info")
            )

        if pharmacy_df is not None and pharmacy_df.height > 0:
            sections.append(
                self.mo.vstack(
                    [
                        self.mo.md(f"**💊 Pharmacy claim lines: {pharmacy_df.height:,}**"),
                        self._ui.csv_download(
                            pharmacy_df,
                            "📥 Export Pharmacy Lines CSV",
                            "patient_pharmacy_lines.csv",
                        ),
                        self.mo.ui.table(pharmacy_df, pagination=True, page_size=20),
                    ]
                )
            )
        else:
            sections.append(
                self.mo.callout("No pharmacy claim lines for this patient.", kind="info")
            )
        return self.mo.vstack(sections)

    # ---- identity_timeline lookup -------------------------------------

    def identity_lookup_panel(self, result: dict):
        """Render :py:meth:`IdentityPlugins.resolve_as_of` output as HTML."""
        if "error" in result:
            return self.mo.md(f"⚠️ **{result['error']}**")
        if result.get("chain_id") is None:
            return self.mo.md(
                f"ℹ️ MBI `{result['input_mbi']}` not found in identity_timeline."
            )
        opt_out_badge = (
            "<span style='color:#DC2626;font-weight:600'>🚫 OPTED OUT "
            f"({', '.join(result['opt_out_reasons']) or 'no reason given'})</span>"
            if result["opted_out"]
            else "<span style='color:#16A34A'>✓ Not opted out</span>"
        )
        chain_pill = " → ".join(f"<code>{m}</code>" for m in result["chain_members"])
        return self.mo.md(
            f"""
            <div style="padding:1rem;background:#F9FAFB;border-left:4px solid #2563EB;margin:1rem 0;">
                <p style="margin:0;font-size:1rem;">
                    Input MBI: <code>{result['input_mbi']}</code><br>
                    Canonical MBI: <strong><code>{result['canonical_mbi']}</code></strong><br>
                    HCMPI: <code>{result['hcmpi'] or '(not mapped)'}</code><br>
                    {opt_out_badge}
                </p>
                <p style="margin:0.5rem 0 0 0;font-size:0.85rem;color:#374151;">
                    Chain (hop 0 → n): {chain_pill}<br>
                    Last observed: {result['last_observed']} | chain_id: <code>{result['chain_id']}</code>
                </p>
            </div>
            """
        )

    # ---- member claims search -----------------------------------------

    def eligibility_panel(
        self,
        eligibility_df: pl.DataFrame | None,
        member_ids: list[str],
    ):
        if not member_ids:
            return self.mo.callout(
                "ℹ️ Eligibility requires Member IDs. Search was by HCPCS / NPI / TIN only.",
                kind="info",
            )
        if eligibility_df is None or eligibility_df.height == 0:
            return self.mo.callout(
                f"⚠️ No eligibility records for: {', '.join(member_ids)}",
                kind="warn",
            )
        return self.mo.vstack(
            [
                self.mo.md(f"**{eligibility_df.height:,} eligibility record(s)**"),
                self._ui.csv_download(
                    eligibility_df, "📥 Export Eligibility CSV", "eligibility.csv"
                ),
                self.mo.ui.table(eligibility_df, selection=None),
            ]
        )

    def claims_summary_panel(self, df: pl.DataFrame | None, kind: ClaimKind):
        """Three-card summary + claim-type / NDC accordion for medical or pharmacy."""
        if df is None or df.height == 0:
            msg = "No medical claims for the search criteria." if kind == "medical" \
                else "No pharmacy claims for the searched members."
            return self.mo.callout(f"ℹ️ {msg}", kind="info")

        if kind == "medical":
            cards = self._ui.summary_cards_row(
                [
                    self._ui.summary_card_html(
                        "TOTAL CLAIM LINES",
                        f"{df.height:,}",
                        f"{df.select(pl.col('claim_id').n_unique()).item():,} unique claim IDs",
                        "#10B981",
                    ),
                    self._ui.summary_card_html(
                        "TOTAL PAID",
                        f"${df.select(pl.col('paid_amount').sum()).item() or 0:,.2f}",
                        f"Avg: ${df.select(pl.col('paid_amount').mean()).item() or 0:,.2f} per line",
                        "#3B82F6",
                    ),
                    self._ui.summary_card_html(
                        "DATE RANGE",
                        f"{df.select(pl.col('claim_start_date').min()).item()}",
                        f"to {df.select(pl.col('claim_start_date').max()).item()}",
                        "#F59E0B",
                    ),
                ]
            )
            claim_types = (
                df.group_by("claim_type")
                .agg(pl.len().alias("count"), pl.col("paid_amount").sum().alias("total_paid"))
                .sort("count", descending=True)
            )
            top_hcpcs = (
                df.filter(pl.col("hcpcs_code").is_not_null())
                .group_by("hcpcs_code")
                .agg(pl.len().alias("count"), pl.col("paid_amount").sum().alias("total_paid"))
                .sort("count", descending=True)
                .head(10)
            )
            return self.mo.vstack(
                [
                    cards,
                    self.mo.accordion(
                        {
                            "Claim Type Breakdown": self.mo.ui.table(claim_types, selection=None),
                            "Top 10 HCPCS Codes": (
                                self.mo.ui.table(top_hcpcs, selection=None)
                                if top_hcpcs.height > 0
                                else self.mo.md("No HCPCS codes found")
                            ),
                        }
                    ),
                ]
            )

        # pharmacy
        cards = self._ui.summary_cards_row(
            [
                self._ui.summary_card_html(
                    "TOTAL FILLS",
                    f"{df.height:,}",
                    f"{df.select(pl.col('claim_id').n_unique()).item():,} unique prescriptions",
                    "#8B5CF6",
                ),
                self._ui.summary_card_html(
                    "TOTAL PAID",
                    f"${df.select(pl.col('paid_amount').sum()).item() or 0:,.2f}",
                    f"Avg: ${df.select(pl.col('paid_amount').mean()).item() or 0:,.2f} per fill",
                    "#EC4899",
                ),
                self._ui.summary_card_html(
                    "AVG DAYS SUPPLY",
                    f"{df.select(pl.col('days_supply').mean()).item() or 0:.1f}",
                    f"Total qty: {df.select(pl.col('quantity').sum()).item() or 0:,.0f}",
                    "#14B8A6",
                ),
            ]
        )
        top_ndc = (
            df.filter(pl.col("ndc_code").is_not_null())
            .group_by("ndc_code")
            .agg(
                pl.len().alias("count"),
                pl.col("paid_amount").sum().alias("total_paid"),
                pl.col("quantity").sum().alias("total_quantity"),
            )
            .sort("count", descending=True)
            .head(10)
        )
        return self.mo.vstack(
            [
                cards,
                self.mo.accordion(
                    {
                        "Top 10 NDC Codes": (
                            self.mo.ui.table(top_ndc, selection=None)
                            if top_ndc.height > 0
                            else self.mo.md("No NDC codes found")
                        ),
                    }
                ),
            ]
        )

    def claims_detail_panel(self, df: pl.DataFrame | None, kind: ClaimKind):
        """Per-row breakouts (diagnoses / providers / drugs / pharmacies) + line table."""
        if df is None or df.height == 0:
            msg = "No medical claims to display." if kind == "medical" \
                else "No pharmacy claims to display."
            return self.mo.callout(f"ℹ️ {msg}", kind="info")

        if kind == "medical":
            diag_cols = [
                c for c in ("diagnosis_code_1", "diagnosis_code_2", "diagnosis_code_3")
                if c in df.columns
            ]
            if diag_cols:
                diag_long = pl.concat(
                    [
                        df.filter(pl.col(c).is_not_null()).select(
                            pl.col(c).alias("diagnosis_code"),
                            "paid_amount",
                            "allowed_amount",
                            "member_id",
                        )
                        for c in diag_cols
                    ]
                )
                diag_summary = (
                    diag_long.group_by("diagnosis_code")
                    .agg(
                        pl.len().alias("claim_count"),
                        pl.col("member_id").n_unique().alias("member_count"),
                        pl.col("paid_amount").sum().alias("total_paid"),
                        pl.col("allowed_amount").sum().alias("total_allowed"),
                        pl.col("paid_amount").mean().alias("avg_paid"),
                    )
                    .sort("claim_count", descending=True)
                    .head(20)
                )
            else:
                diag_summary = pl.DataFrame()

            provider_summary = (
                df.filter(pl.col("rendering_npi").is_not_null())
                .group_by("rendering_npi")
                .agg(
                    pl.len().alias("claim_count"),
                    pl.col("member_id").n_unique().alias("member_count"),
                    pl.col("paid_amount").sum().alias("total_paid"),
                    pl.col("allowed_amount").sum().alias("total_allowed"),
                    pl.col("paid_amount").mean().alias("avg_paid_per_claim"),
                )
                .sort("total_paid", descending=True)
                .head(15)
            )
            pos_summary = (
                df.filter(pl.col("place_of_service_code").is_not_null())
                .group_by("place_of_service_code")
                .agg(
                    pl.len().alias("claim_count"),
                    pl.col("paid_amount").sum().alias("total_paid"),
                    pl.col("allowed_amount").sum().alias("total_allowed"),
                    pl.col("paid_amount").mean().alias("avg_paid"),
                )
                .sort("total_paid", descending=True)
            )
            member_cost = (
                df.group_by("member_id")
                .agg(
                    pl.len().alias("claim_count"),
                    pl.col("paid_amount").sum().alias("total_paid"),
                    pl.col("allowed_amount").sum().alias("total_allowed"),
                    pl.col("charge_amount").sum().alias("total_charged"),
                )
                .sort("total_paid", descending=True)
            )
            return self.mo.vstack(
                [
                    self.mo.accordion(
                        {
                            "📊 Top Diagnoses (by frequency)": (
                                self.mo.ui.table(diag_summary, selection=None)
                                if diag_summary.height > 0
                                else self.mo.md("No diagnosis codes found")
                            ),
                            "👨‍⚕️ Top Rendering Providers (by paid)": self.mo.ui.table(provider_summary, selection=None),
                            "🏥 Place of Service Analysis": self.mo.ui.table(pos_summary, selection=None),
                            "💰 Member Cost Breakdown": self.mo.ui.table(member_cost, selection=None),
                        }
                    ),
                    self.mo.md("**Line-level claim records**"),
                    self._ui.csv_download(df, "📥 Export Medical Lines CSV", "medical_claims.csv"),
                    self.mo.ui.table(df, pagination=True, page_size=20),
                ]
            )

        # pharmacy
        ndc_summary = (
            df.filter(pl.col("ndc_code").is_not_null())
            .group_by("ndc_code")
            .agg(
                pl.len().alias("fill_count"),
                pl.col("member_id").n_unique().alias("member_count"),
                pl.col("paid_amount").sum().alias("total_paid"),
                pl.col("allowed_amount").sum().alias("total_allowed"),
                pl.col("quantity").sum().alias("total_quantity"),
                pl.col("days_supply").sum().alias("total_days_supply"),
                pl.col("paid_amount").mean().alias("avg_paid_per_fill"),
            )
            .sort("total_paid", descending=True)
            .head(20)
        )
        prescriber_summary = (
            df.filter(pl.col("prescribing_provider_npi").is_not_null())
            .group_by("prescribing_provider_npi")
            .agg(
                pl.len().alias("prescription_count"),
                pl.col("member_id").n_unique().alias("member_count"),
                pl.col("ndc_code").n_unique().alias("unique_drugs"),
                pl.col("paid_amount").sum().alias("total_paid"),
            )
            .sort("total_paid", descending=True)
            .head(15)
        )
        dispensing_summary = (
            df.filter(pl.col("dispensing_provider_npi").is_not_null())
            .group_by("dispensing_provider_npi")
            .agg(
                pl.len().alias("fill_count"),
                pl.col("member_id").n_unique().alias("member_count"),
                pl.col("ndc_code").n_unique().alias("unique_drugs"),
                pl.col("paid_amount").sum().alias("total_paid"),
            )
            .sort("total_paid", descending=True)
            .head(15)
        )
        member_pharm = (
            df.group_by("member_id")
            .agg(
                pl.len().alias("fill_count"),
                pl.col("ndc_code").n_unique().alias("unique_drugs"),
                pl.col("paid_amount").sum().alias("total_paid"),
                pl.col("allowed_amount").sum().alias("total_allowed"),
                pl.col("copayment_amount").sum().alias("total_copay"),
                pl.col("deductible_amount").sum().alias("total_deductible"),
                pl.col("days_supply").sum().alias("total_days_supply"),
            )
            .sort("total_paid", descending=True)
        )
        return self.mo.vstack(
            [
                self.mo.accordion(
                    {
                        "💊 Top Drugs (by paid)": self.mo.ui.table(ndc_summary, selection=None),
                        "👨‍⚕️ Top Prescribers (by paid)": self.mo.ui.table(prescriber_summary, selection=None),
                        "🏪 Top Dispensing Pharmacies (by paid)": self.mo.ui.table(dispensing_summary, selection=None),
                        "💰 Member Pharmacy Cost Breakdown": self.mo.ui.table(member_pharm, selection=None),
                    }
                ),
                self.mo.md("**Line-level fill records**"),
                self._ui.csv_download(df, "📥 Export Pharmacy Lines CSV", "pharmacy_claims.csv"),
                self.mo.ui.table(df, pagination=True, page_size=20),
            ]
        )
