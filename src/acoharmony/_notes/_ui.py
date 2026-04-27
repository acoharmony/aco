# © 2025 HarmonyCares
# All rights reserved.

"""Branded UI components: header, footer, callouts, summary cards, downloads."""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Any, Literal

import polars as pl

from ._base import PluginRegistry

ExportFormat = Literal["csv", "excel", "parquet"]
MedallionTier = Literal["bronze", "silver", "gold"]


class UIPlugins(PluginRegistry):
    """HarmonyCares-branded marimo UI components."""

    COLORS = {
        "primary_blue": "#2E3254",
        "secondary_blue": "#3d4466",
        "highlight_blue": "#60A5FA",
        "info_blue": "#3B82F6",
        "success_green": "#10B981",
        "warning_orange": "#F59E0B",
        "danger_red": "#EF4444",
        "purple": "#8B5CF6",
        "teal": "#14B8A6",
        "gray_light": "#E5E7EB",
        "gray_medium": "#9CA3AF",
        "gray_dark": "#1E40AF",
    }

    def branded_header(
        self,
        title: str,
        subtitle: str | None = None,
        metadata: dict[str, Any] | None = None,
        icon: str = "fa-solid fa-chart-line",
        show_logo: bool = True,
        show_timestamp: bool = True,
    ):
        metadata_html = ""
        if metadata:
            lines = []
            for name, meta in metadata.items():
                rows = meta.get("rows", 0)
                date_range = ""
                if "min_date" in meta and "max_date" in meta:
                    date_range = f" ({meta['min_date']} to {meta['max_date']})"
                lines.append(f"<strong>{name}:</strong> {rows:,} rows{date_range}")
            metadata_html = (
                f'<div style="background: #F3F4F6; padding: 1rem; border-radius: 8px; margin-top: 1rem;">'
                f'<p style="margin: 0; color: #374151; font-size: 0.875rem;">{"<br>".join(lines)}</p>'
                f"</div>"
            )

        logo_html = ""
        if show_logo:
            logo_html = (
                '<img src="https://harmonycaresaco.com/img/logo.svg" alt="HarmonyCares Logo" '
                'style="height: 60px; filter: brightness(0) invert(1);" '
                'onerror="this.style.display=\'none\'">'
            )

        timestamp_html = ""
        if show_timestamp:
            timestamp_html = (
                f'<p style="color: {self.COLORS["gray_medium"]}; margin: 0.5rem 0 0 0; font-size: 0.875rem;">'
                f'<i class="fa-solid fa-clock"></i> '
                f'{datetime.now().strftime("%B %d, %Y • %I:%M %p")}'
                f"</p>"
            )

        subtitle_html = ""
        if subtitle:
            subtitle_html = (
                f'<p style="color: {self.COLORS["gray_light"]}; margin: 0.5rem 0 0 0; font-size: 1rem;">{subtitle}</p>'
            )

        html = f"""
        <div style="background: linear-gradient(135deg, {self.COLORS['primary_blue']} 0%, {self.COLORS['secondary_blue']} 100%);
                    padding: 2rem;
                    border-radius: 12px;
                    margin-bottom: 2rem;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
            <div style="display: flex; align-items: center; justify-content: space-between;">
                <div>
                    <h1 style="color: #ffffff; margin: 0; font-size: 2rem; font-weight: 700;">
                        <i class="{icon}" style="margin-right: 0.75rem; color: {self.COLORS['highlight_blue']};"></i>
                        {title}
                    </h1>
                    {subtitle_html}
                </div>
                <div style="text-align: right;">
                    {logo_html}
                    {timestamp_html}
                </div>
            </div>
            {metadata_html}
        </div>
        """
        return self.mo.md(html)

    def info_callout(self, message: str, data_source: str | None = None):
        body = f"<strong>Data Source:</strong> {data_source}" if data_source else message
        html = f"""
        <div style="background: #EFF6FF; border-left: 4px solid {self.COLORS['info_blue']};
                    padding: 1rem 1.5rem; border-radius: 8px; margin-bottom: 2rem;">
            <p style="margin: 0; color: {self.COLORS['gray_dark']}; font-weight: 500;">
                <i class="fa-solid fa-circle-info" style="color: {self.COLORS['info_blue']}; margin-right: 0.5rem;"></i>
                {body}
            </p>
        </div>
        """
        return self.mo.md(html)

    def summary_card_html(self, label: str, value: str, sub: str, color: str) -> str:
        """Compact metric card; intended to be composed inside ``summary_cards_row``."""
        return f"""
        <div style="padding: 1rem; background: {color}20; border-radius: 8px;">
            <h4 style="margin: 0; color: {color}; font-size: 0.875rem;">{label}</h4>
            <p style="margin: 0.5rem 0 0 0; font-size: 2rem; font-weight: 700; color: {color};">{value}</p>
            <p style="margin: 0; font-size: 0.75rem; color: {color};">{sub}</p>
        </div>
        """

    def summary_cards_row(self, cards_html: list[str]):
        grid = (
            '<div style="display: grid; grid-template-columns: repeat('
            f"{len(cards_html)}, 1fr); gap: 1rem; margin: 1.5rem 0;\">"
            + "".join(cards_html)
            + "</div>"
        )
        return self.mo.md(grid)

    def summary_cards(self, metrics: list[dict[str, Any]], columns: int = 3):
        """Auto-colored metric grid: each entry is ``{name, value, color?, icon?}``."""
        default_colors = [
            "success_green", "info_blue", "warning_orange",
            "purple", "teal", "danger_red",
        ]
        cards = []
        for i, m in enumerate(metrics):
            color = self.COLORS.get(
                m.get("color", default_colors[i % len(default_colors)]),
                self.COLORS["info_blue"],
            )
            value = m["value"]
            if isinstance(value, int | float) and "value_format" not in m:
                formatted = f"{value:,.0f}" if isinstance(value, int) else f"{value:,.2f}"
            else:
                formatted = str(value)
            icon_html = (
                f'<i class="{m.get("icon", "")}" style="margin-right: 0.5rem;"></i>'
                if m.get("icon")
                else ""
            )
            cards.append(
                f"""
                <div style="padding: 1.5rem; background: {color}20; border-radius: 8px; border: 2px solid {color}40;">
                    <h4 style="color: {color}; margin: 0 0 0.5rem 0; font-size: 0.875rem; font-weight: 600; text-transform: uppercase;">
                        {icon_html}{m["name"]}
                    </h4>
                    <p style="font-size: 2rem; font-weight: 700; margin: 0; color: {color};">{formatted}</p>
                </div>
                """
            )
        grid = (
            f'<div style="display: grid; grid-template-columns: repeat({columns}, 1fr); gap: 1rem; margin: 2rem 0;">'
            + "".join(cards)
            + "</div>"
        )
        return self.mo.md(grid)

    def csv_download(self, df: pl.DataFrame | None, label: str, filename: str):
        """``mo.download`` for a DataFrame as CSV; ``None`` when the frame is empty."""
        if df is None or df.height == 0:
            return None
        return self.mo.download(
            data=df.write_csv().encode(),
            filename=filename,
            mimetype="text/csv",
            label=label,
        )

    def download_button(
        self,
        df: pl.DataFrame,
        format: ExportFormat = "csv",
        filename: str | None = None,
        label: str | None = None,
        include_timestamp: bool = True,
    ):
        """Generic download button supporting csv / excel / parquet."""
        if filename is None:
            filename = "export"
        if include_timestamp:
            filename = f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        emoji = {"csv": "📥", "excel": "📊", "parquet": "📦"}
        if label is None:
            label = f"{emoji.get(format, '📥')} Download {format.upper()}"

        if format == "csv":
            data = df.write_csv().encode()
            mimetype = "text/csv"
            filename = f"{filename}.csv"
        elif format == "excel":
            def generate_excel() -> bytes:
                buf = BytesIO()
                df.write_excel(buf)
                buf.seek(0)
                return buf.getvalue()

            data = generate_excel
            mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"{filename}.xlsx"
        elif format == "parquet":
            buf = BytesIO()
            df.write_parquet(buf)
            buf.seek(0)
            data = buf.getvalue()
            mimetype = "application/octet-stream"
            filename = f"{filename}.parquet"
        else:
            raise ValueError(f"Unsupported format: {format}")

        return self.mo.download(data=data, filename=filename, label=label, mimetype=mimetype)

    def branded_footer(
        self,
        tier: MedallionTier | None = None,
        files: list[str] | None = None,
        tracker_name: str | None = None,
    ):
        tracking_html = ""
        if tracker_name:
            try:
                from acoharmony.tracking import TransformTracker

                tracker = TransformTracker(tracker_name)
                state = tracker.state
                tracking_html = (
                    f'<p style="color: {self.COLORS["gray_medium"]}; margin: 0.25rem 0; font-size: 0.875rem;">'
                    f'Last run: {state.last_run or "Never"} • Total runs: {state.total_runs or 0}</p>'
                )
            except Exception:  # ALLOWED: footer falls back silently when tracker unavailable
                pass

        data_source_html = ""
        if tier:
            data_source_html = f"<strong>Data Source:</strong> {tier.capitalize()} Tier"
            if files:
                data_source_html += f" • {', '.join(files)}"

        html = f"""
        <hr style="margin: 3rem 0 2rem 0; border: none; border-top: 1px solid #E5E7EB;">
        <div style="background: linear-gradient(135deg, {self.COLORS['primary_blue']} 0%, {self.COLORS['secondary_blue']} 100%);
                    padding: 1.5rem; border-radius: 8px; text-align: center;">
            <img src="https://harmonycaresaco.com/img/logo.svg" alt="HarmonyCares Logo"
                 style="height: 40px; filter: brightness(0) invert(1); margin-bottom: 1rem;"
                 onerror="this.style.display='none'">
            <p style="color: {self.COLORS['gray_light']}; margin: 0.5rem 0; font-size: 0.875rem;">© 2025 HarmonyCares ACO</p>
            {f'<p style="color: {self.COLORS["gray_medium"]}; margin: 0.25rem 0; font-size: 0.875rem;">{data_source_html}</p>' if data_source_html else ""}
            {tracking_html}
        </div>
        """
        return self.mo.md(html)
