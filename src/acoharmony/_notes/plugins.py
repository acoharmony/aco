# © 2025 HarmonyCares
# All rights reserved.

"""
Marimo Notebook Plugin Registry for ACOHarmony.

This module provides a DRY (Don't Repeat Yourself) plugin system for marimo notebooks,
eliminating code duplication across notebook implementations. All reusable functions
are registered here and can be called uniformly across notebooks.

Architecture:
    - setup: Environment initialization, path configuration, storage setup
    - ui: Branded UI components (headers, footers, buttons, cards)
    - data: Data loading utilities for gold/silver/bronze layers
    - analysis: Common analysis functions (summaries, aggregations, comparisons)
    - utils: General utilities (formatting, parsing, Excel generation)

Usage in marimo notebooks:
    ```python
    from acoharmony._notes.plugins import setup, ui, data, analysis

    # One-line environment setup
    env = setup.initialize()

    # Create branded header
    header = ui.branded_header("Dashboard Title", subtitle="Analysis Overview")

    # Load data
    claims = data.load_gold_dataset("medical_claim", lazy=True)
    ```
"""

import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Literal

import polars as pl

# Type aliases
MedallionTier = Literal["bronze", "silver", "gold"]
ExportFormat = Literal["csv", "excel", "parquet"]


class PluginRegistry:
    """Base class for plugin registries with lazy-loaded dependencies."""

    def __init__(self) -> None:
        self._mo = None
        self._storage = None
        self._catalog = None

    @property
    def mo(self):
        """Lazy-load marimo."""
        if self._mo is None:
            import marimo as mo

            self._mo = mo
        return self._mo

    @property
    def storage(self):
        """Lazy-load StorageBackend."""
        if self._storage is None:
            from acoharmony._store import StorageBackend

            self._storage = StorageBackend()
        return self._storage

    @property
    def catalog(self):
        """Lazy-load Catalog."""
        if self._catalog is None:
            from acoharmony import Catalog

            self._catalog = Catalog()
        return self._catalog


# ============================================================================
# SETUP PLUGINS - Environment initialization and configuration
# ============================================================================


class SetupPlugins(PluginRegistry):
    """Setup and initialization utilities for marimo notebooks."""

    def setup_project_path(self) -> Path:
        """
        Add ACOHarmony project to sys.path for imports.

        Returns:
            Path: Project root directory

        Example:
            ```python
            project_root = setup.setup_project_path()
            ```
        """
        project_root = Path("/home/care/acoharmony")
        if project_root.exists() and str(project_root / "src") not in sys.path:
            sys.path.insert(0, str(project_root / "src"))
        return project_root

    def initialize(
        self, setup_path: bool = True
    ) -> dict[str, Any]:
        """
        One-call initialization for marimo notebooks.

        Initializes:
            - Project path (optional)
            - Storage backend
            - Catalog
            - Medallion layer paths

        Args:
            setup_path: Whether to add project to sys.path (default: True)

        Returns:
            Dict with keys: storage, catalog, gold_path, silver_path, bronze_path

        Example:
            ```python
            env = setup.initialize()
            claims = pl.scan_parquet(env["gold_path"] / "medical_claim.parquet")
            ```
        """
        if setup_path:
            self.setup_project_path()

        return {
            "storage": self.storage,
            "catalog": self.catalog,
            "gold_path": self.get_medallion_path("gold"),
            "silver_path": self.get_medallion_path("silver"),
            "bronze_path": self.get_medallion_path("bronze"),
        }

    def get_medallion_path(self, tier: MedallionTier) -> Path:
        """
        Get path for medallion tier (bronze/silver/gold).

        Args:
            tier: Medallion tier name

        Returns:
            Path to tier directory

        Example:
            ```python
            gold_path = setup.get_medallion_path("gold")
            ```
        """
        # Use storage backend if available, otherwise fallback to default
        try:
            return Path(self.storage.get_path(tier))
        except Exception:
            # Fallback to default workspace paths
            return Path(f"/opt/s3/data/workspace/{tier}")


# ============================================================================
# UI PLUGINS - Branded interface components
# ============================================================================


class UIPlugins(PluginRegistry):
    """UI components for marimo notebooks with HarmonyCares branding."""

    # HarmonyCares color palette
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
        """
        Create branded header with HarmonyCares styling.

        Args:
            title: Main heading text
            subtitle: Optional subtitle/description
            metadata: Optional dict with dataset metadata to display
            icon: Font Awesome icon class (default: chart-line)
            show_logo: Whether to show HarmonyCares logo
            show_timestamp: Whether to show current timestamp

        Returns:
            marimo.md object with styled header HTML

        Example:
            ```python
            header = ui.branded_header(
                "Skin Substitutes Analysis",
                subtitle="Analyze claims 2024-2025",
                icon="fa-solid fa-hospital-user"
            )
            ```
        """
        # Build metadata section if provided
        metadata_html = ""
        if metadata:
            dataset_lines = []
            for name, meta in metadata.items():
                rows = meta.get("rows", 0)
                date_range = ""
                if "min_date" in meta and "max_date" in meta:
                    date_range = f" ({meta['min_date']} to {meta['max_date']})"
                dataset_lines.append(f"<strong>{name}:</strong> {rows:,} rows{date_range}")

            metadata_html = f"""
            <div style="background: #F3F4F6; padding: 1rem; border-radius: 8px; margin-top: 1rem;">
                <p style="margin: 0; color: #374151; font-size: 0.875rem;">
                    {'<br>'.join(dataset_lines)}
                </p>
            </div>
            """

        # Build logo section
        logo_html = ""
        if show_logo:
            logo_html = """
            <img src="https://harmonycaresaco.com/img/logo.svg"
                 alt="HarmonyCares Logo"
                 style="height: 60px; filter: brightness(0) invert(1);"
                 onerror="this.style.display='none'">
            """

        # Build timestamp section
        timestamp_html = ""
        if show_timestamp:
            timestamp_html = f"""
            <p style="color: {self.COLORS['gray_medium']}; margin: 0.5rem 0 0 0; font-size: 0.875rem;">
                <i class="fa-solid fa-clock"></i> {datetime.now().strftime("%B %d, %Y • %I:%M %p")}
            </p>
            """

        # Build subtitle
        subtitle_html = ""
        if subtitle:
            subtitle_html = f"""
            <p style="color: {self.COLORS['gray_light']}; margin: 0.5rem 0 0 0; font-size: 1rem;">
                {subtitle}
            </p>
            """

        header_html = f"""
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

        return self.mo.md(header_html)

    def info_callout(self, message: str, data_source: str | None = None):
        """
        Create info callout box with HarmonyCares styling.

        Args:
            message: Info message text
            data_source: Optional data source description

        Returns:
            marimo.md object with styled callout

        Example:
            ```python
            info = ui.info_callout(
                "Analysis complete",
                data_source="Tuva Gold Layer • Medicare CCLF"
            )
            ```
        """
        data_source_html = ""
        if data_source:
            data_source_html = f"<strong>Data Source:</strong> {data_source}"
        else:
            data_source_html = message

        html = f"""
        <div style="background: #EFF6FF;
                    border-left: 4px solid {self.COLORS['info_blue']};
                    padding: 1rem 1.5rem;
                    border-radius: 8px;
                    margin-bottom: 2rem;">
            <p style="margin: 0; color: {self.COLORS['gray_dark']}; font-weight: 500;">
                <i class="fa-solid fa-circle-info" style="color: {self.COLORS['info_blue']}; margin-right: 0.5rem;"></i>
                {data_source_html}
            </p>
        </div>
        """
        return self.mo.md(html)

    def summary_cards(
        self,
        metrics: list[dict[str, Any]],
        columns: int = 3,
    ):
        """
        Create summary metric cards in a grid layout.

        Args:
            metrics: List of dicts with keys: name, value, color (optional), icon (optional)
            columns: Number of columns in grid (default: 3)

        Returns:
            marimo.md object with card grid

        Example:
            ```python
            cards = ui.summary_cards([
                {"name": "Total Claims", "value": 1234, "icon": "fa-file-medical"},
                {"name": "Total Paid", "value": "$45,678", "color": "success_green"},
            ], columns=2)
            ```
        """
        # Auto-assign colors if not provided
        default_colors = [
            "success_green",
            "info_blue",
            "warning_orange",
            "purple",
            "teal",
            "danger_red",
        ]

        cards_html = []
        for i, metric in enumerate(metrics):
            name = metric["name"]
            value = metric["value"]
            color = metric.get("color", default_colors[i % len(default_colors)])
            icon = metric.get("icon", "")
            color_hex = self.COLORS.get(color, self.COLORS["info_blue"])

            # Format value if it's a number
            if isinstance(value, int | float) and "value_format" not in metric:
                formatted_value = f"{value:,.0f}" if isinstance(value, int) else f"{value:,.2f}"
            else:
                formatted_value = str(value)

            icon_html = f'<i class="{icon}" style="margin-right: 0.5rem;"></i>' if icon else ""

            card = f"""
            <div style="padding: 1.5rem;
                        background: {color_hex}20;
                        border-radius: 8px;
                        border: 2px solid {color_hex}40;">
                <h4 style="color: {color_hex}; margin: 0 0 0.5rem 0; font-size: 0.875rem; font-weight: 600; text-transform: uppercase;">
                    {icon_html}{name}
                </h4>
                <p style="font-size: 2rem; font-weight: 700; margin: 0; color: {color_hex};">
                    {formatted_value}
                </p>
            </div>
            """
            cards_html.append(card)

        grid_html = f"""
        <div style="display: grid;
                    grid-template-columns: repeat({columns}, 1fr);
                    gap: 1rem;
                    margin: 2rem 0;">
            {''.join(cards_html)}
        </div>
        """

        return self.mo.md(grid_html)

    def download_button(
        self,
        df: pl.DataFrame,
        format: ExportFormat = "csv",
        filename: str | None = None,
        label: str | None = None,
        include_timestamp: bool = True,
    ):
        """
        Create download button for DataFrame export.

        Args:
            df: Polars DataFrame to export
            format: Export format (csv, excel, parquet)
            filename: Custom filename (auto-generated if None)
            label: Button label (auto-generated if None)
            include_timestamp: Add timestamp to filename

        Returns:
            marimo.download object

        Example:
            ```python
            download = ui.download_button(
                claims_df,
                format="excel",
                filename="claims_analysis"
            )
            ```
        """
        # Auto-generate filename
        if filename is None:
            filename = "export"

        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename}_{timestamp}"

        # Auto-generate label
        emoji_map = {"csv": "📥", "excel": "📊", "parquet": "📦"}
        if label is None:
            label = f"{emoji_map.get(format, '📥')} Download {format.upper()}"

        # Generate data based on format
        if format == "csv":
            data = df.write_csv().encode()
            mimetype = "text/csv"
            filename = f"{filename}.csv"
        elif format == "excel":
            # Lazy load for better performance
            def generate_excel() -> bytes:
                buffer = BytesIO()
                df.write_excel(buffer)
                buffer.seek(0)
                return buffer.getvalue()

            data = generate_excel
            mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = f"{filename}.xlsx"
        elif format == "parquet":
            buffer = BytesIO()
            df.write_parquet(buffer)
            buffer.seek(0)
            data = buffer.getvalue()
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
        """
        Create branded footer with optional tracking metadata.

        Args:
            tier: Medallion tier (bronze/silver/gold)
            files: List of data files used
            tracker_name: Transform tracker name for metadata

        Returns:
            marimo.md object with styled footer

        Example:
            ```python
            footer = ui.branded_footer(
                tier="gold",
                files=["medical_claim.parquet", "eligibility.parquet"]
            )
            ```
        """
        # Build tracking info
        tracking_html = ""
        if tracker_name:
            try:
                from acoharmony.tracking import TransformTracker

                tracker = TransformTracker(tracker_name)
                state = tracker.state
                tracking_html = f"""
                <p style="color: {self.COLORS['gray_medium']}; margin: 0.25rem 0; font-size: 0.875rem;">
                    Last run: {state.last_run or 'Never'} •
                    Total runs: {state.total_runs or 0}
                </p>
                """
            except Exception:
                pass

        # Build data source info
        data_source_html = ""
        if tier:
            tier_display = tier.capitalize()
            data_source_html = f"<strong>Data Source:</strong> {tier_display} Tier"
            if files:
                files_str = ", ".join(files)
                data_source_html += f" • {files_str}"

        footer_html = f"""
        <hr style="margin: 3rem 0 2rem 0; border: none; border-top: 1px solid #E5E7EB;">
        <div style="background: linear-gradient(135deg, {self.COLORS['primary_blue']} 0%, {self.COLORS['secondary_blue']} 100%);
                    padding: 1.5rem;
                    border-radius: 8px;
                    text-align: center;">
            <img src="https://harmonycaresaco.com/img/logo.svg"
                 alt="HarmonyCares Logo"
                 style="height: 40px; filter: brightness(0) invert(1); margin-bottom: 1rem;"
                 onerror="this.style.display='none'">
            <p style="color: {self.COLORS['gray_light']}; margin: 0.5rem 0; font-size: 0.875rem;">
                © 2025 HarmonyCares ACO
            </p>
            {f'<p style="color: {self.COLORS["gray_medium"]}; margin: 0.25rem 0; font-size: 0.875rem;">{data_source_html}</p>' if data_source_html else ''}
            {tracking_html}
        </div>
        """

        return self.mo.md(footer_html)


# ============================================================================
# DATA PLUGINS - Data loading and access utilities
# ============================================================================


class DataPlugins(PluginRegistry):
    """Data loading utilities for gold/silver/bronze layers."""

    def load_gold_dataset(
        self,
        dataset_name: str,
        lazy: bool = True,
        path: Path | None = None,
    ) -> pl.LazyFrame | pl.DataFrame:
        """
        Load a dataset from the gold layer.

        Args:
            dataset_name: Dataset name (e.g., "medical_claim", "eligibility")
            lazy: Return LazyFrame for lazy evaluation (default: True)
            path: Custom path (uses storage backend if None)

        Returns:
            Polars LazyFrame or DataFrame

        Example:
            ```python
            claims_lf = data.load_gold_dataset("medical_claim", lazy=True)
            eligibility_df = data.load_gold_dataset("eligibility", lazy=False)
            ```
        """
        if path is None:
            gold_path = Path(self.storage.get_path("gold"))
        else:
            gold_path = path

        file_path = gold_path / f"{dataset_name}.parquet"

        if not file_path.exists():
            raise FileNotFoundError(f"Dataset not found: {file_path}")

        if lazy:
            return pl.scan_parquet(file_path)
        else:
            return pl.read_parquet(file_path)

    def load_silver_dataset(
        self,
        dataset_name: str,
        lazy: bool = True,
        path: Path | None = None,
    ) -> pl.LazyFrame | pl.DataFrame:
        """
        Load a dataset from the silver layer.

        Args:
            dataset_name: Dataset name (e.g., "identity_timeline", "beneficiary_demographics")
            lazy: Return LazyFrame for lazy evaluation (default: True)
            path: Custom path (uses storage backend if None)

        Returns:
            Polars LazyFrame or DataFrame

        Example:
            ```python
            crosswalk_lf = data.load_silver_dataset("identity_timeline", lazy=True)
            demographics_df = data.load_silver_dataset("beneficiary_demographics", lazy=False)
            ```
        """
        if path is None:
            silver_path = Path(self.storage.get_path("silver"))
        else:
            silver_path = path

        file_path = silver_path / f"{dataset_name}.parquet"

        if not file_path.exists():
            raise FileNotFoundError(f"Dataset not found: {file_path}")

        if lazy:
            return pl.scan_parquet(file_path)
        else:
            return pl.read_parquet(file_path)

    def get_member_eligibility(
        self,
        member_ids: list[str],
        eligibility_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        """
        Get eligibility information for specified members.

        Args:
            member_ids: List of member IDs to filter
            eligibility_lf: Optional LazyFrame (loads from gold if None)

        Returns:
            DataFrame with eligibility data or None if no matches

        Example:
            ```python
            eligibility = data.get_member_eligibility(["MBR001", "MBR002"])
            ```
        """
        if not member_ids:
            return None

        if eligibility_lf is None:
            eligibility_lf = self.load_gold_dataset("eligibility", lazy=True)

        result = (
            eligibility_lf.filter(pl.col("member_id").is_in(member_ids))
            .select(
                [
                    "person_id",
                    "member_id",
                    "subscriber_id",
                    "gender",
                    "race",
                    "birth_date",
                    "death_date",
                    "death_flag",
                    "enrollment_start_date",
                    "enrollment_end_date",
                    "payer",
                    "payer_type",
                    "plan",
                ]
            )
            .collect()
        )

        return result if result.height > 0 else None

    def get_medical_claims(
        self,
        filters: dict[str, Any] | None = None,
        medical_claim_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        """
        Get medical claims with flexible filtering.

        Args:
            filters: Dict with optional keys:
                - member_ids: List of member IDs
                - hcpcs_codes: List of HCPCS codes
                - npi_codes: List of NPI codes
                - tin_codes: List of TIN codes
                - start_date: Minimum claim start date
                - end_date: Maximum claim start date
            medical_claim_lf: Optional LazyFrame (loads from gold if None)

        Returns:
            DataFrame with medical claims or None if no matches

        Example:
            ```python
            claims = data.get_medical_claims({
                "member_ids": ["MBR001"],
                "hcpcs_codes": ["99213", "99214"],
                "start_date": "2024-01-01"
            })
            ```
        """
        if filters is None:
            filters = {}

        if medical_claim_lf is None:
            medical_claim_lf = self.load_gold_dataset("medical_claim", lazy=True)

        query = medical_claim_lf

        # Apply filters
        if "member_ids" in filters and filters["member_ids"]:
            query = query.filter(pl.col("member_id").is_in(filters["member_ids"]))

        if "hcpcs_codes" in filters and filters["hcpcs_codes"]:
            hcpcs_filter = (
                pl.col("hcpcs_code").is_in(filters["hcpcs_codes"])
                | pl.col("hcpcs_modifier_1").is_in(filters["hcpcs_codes"])
                | pl.col("hcpcs_modifier_2").is_in(filters["hcpcs_codes"])
            )
            query = query.filter(hcpcs_filter)

        if "npi_codes" in filters and filters["npi_codes"]:
            npi_filter = (
                pl.col("rendering_npi").is_in(filters["npi_codes"])
                | pl.col("billing_npi").is_in(filters["npi_codes"])
                | pl.col("facility_npi").is_in(filters["npi_codes"])
            )
            query = query.filter(npi_filter)

        if "tin_codes" in filters and filters["tin_codes"]:
            query = query.filter(pl.col("billing_tin").is_in(filters["tin_codes"]))

        if "start_date" in filters:
            query = query.filter(pl.col("claim_start_date") >= filters["start_date"])

        if "end_date" in filters:
            query = query.filter(pl.col("claim_start_date") <= filters["end_date"])

        # Standard column selection
        result = (
            query.select(
                [
                    "claim_id",
                    "claim_line_number",
                    "claim_type",
                    "member_id",
                    "person_id",
                    "claim_start_date",
                    "claim_end_date",
                    "claim_line_start_date",
                    "claim_line_end_date",
                    "admission_date",
                    "discharge_date",
                    "place_of_service_code",
                    "bill_type_code",
                    "revenue_center_code",
                    "hcpcs_code",
                    "hcpcs_modifier_1",
                    "hcpcs_modifier_2",
                    "rendering_npi",
                    "rendering_tin",
                    "billing_npi",
                    "billing_tin",
                    "facility_npi",
                    "paid_amount",
                    "allowed_amount",
                    "charge_amount",
                    "diagnosis_code_1",
                    "diagnosis_code_2",
                    "diagnosis_code_3",
                ]
            )
            .sort("claim_start_date", descending=True)
            .collect()
        )

        return result if result.height > 0 else None

    def get_pharmacy_claims(
        self,
        member_ids: list[str],
        pharmacy_claim_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        """
        Get pharmacy claims for specified members.

        Args:
            member_ids: List of member IDs
            pharmacy_claim_lf: Optional LazyFrame (loads from gold if None)

        Returns:
            DataFrame with pharmacy claims or None if no matches

        Example:
            ```python
            rx_claims = data.get_pharmacy_claims(["MBR001", "MBR002"])
            ```
        """
        if not member_ids:
            return None

        if pharmacy_claim_lf is None:
            pharmacy_claim_lf = self.load_gold_dataset("pharmacy_claim", lazy=True)

        result = (
            pharmacy_claim_lf.filter(pl.col("member_id").is_in(member_ids))
            .select(
                [
                    "claim_id",
                    "claim_line_number",
                    "member_id",
                    "person_id",
                    "dispensing_date",
                    "ndc_code",
                    "prescribing_provider_npi",
                    "dispensing_provider_npi",
                    "quantity",
                    "days_supply",
                    "refills",
                    "paid_date",
                    "paid_amount",
                    "allowed_amount",
                    "charge_amount",
                    "coinsurance_amount",
                    "copayment_amount",
                    "deductible_amount",
                    "in_network_flag",
                ]
            )
            .sort("dispensing_date", descending=True)
            .collect()
        )

        return result if result.height > 0 else None


# ============================================================================
# ANALYSIS PLUGINS - Common analysis functions
# ============================================================================


class AnalysisPlugins(PluginRegistry):
    """Analysis utilities for common data operations."""

    def compute_summary(
        self,
        df: pl.DataFrame,
        metrics: list[str] | None = None,
        group_by: str | None = None,
    ) -> dict[str, Any]:
        """
        Compute summary statistics for a DataFrame.

        Args:
            df: Polars DataFrame
            metrics: List of metric names to compute (auto-detected if None)
            group_by: Optional column to group by

        Returns:
            Dict with summary statistics

        Example:
            ```python
            summary = analysis.compute_summary(
                claims_df,
                metrics=["paid_amount", "allowed_amount"]
            )
            ```
        """
        summary = {
            "total_rows": df.height,
            "total_columns": df.width,
        }

        # Auto-detect numeric columns if metrics not specified
        if metrics is None:
            metrics = [col for col in df.columns if df[col].dtype in [pl.Float64, pl.Int64]]

        # Compute basic stats for numeric columns
        for metric in metrics:
            if metric in df.columns:
                summary[f"{metric}_sum"] = df[metric].sum()
                summary[f"{metric}_mean"] = df[metric].mean()
                summary[f"{metric}_max"] = df[metric].max()
                summary[f"{metric}_min"] = df[metric].min()

        # Date range if date columns exist
        date_cols = [col for col in df.columns if "date" in col.lower()]
        for date_col in date_cols:
            summary[f"{date_col}_min"] = df[date_col].min()
            summary[f"{date_col}_max"] = df[date_col].max()

        return summary

    def top_n_analysis(
        self,
        df: pl.DataFrame,
        group_col: str,
        metric_col: str,
        n: int = 10,
        agg_func: str = "sum",
    ) -> pl.DataFrame:
        """
        Compute top N analysis by grouping and aggregating.

        Args:
            df: Polars DataFrame
            group_col: Column to group by
            metric_col: Column to aggregate
            n: Number of top results to return
            agg_func: Aggregation function (sum, mean, count)

        Returns:
            DataFrame with top N results

        Example:
            ```python
            top_hcpcs = analysis.top_n_analysis(
                claims_df,
                group_col="hcpcs_code",
                metric_col="paid_amount",
                n=10
            )
            ```
        """
        agg_map = {
            "sum": pl.col(metric_col).sum(),
            "mean": pl.col(metric_col).mean(),
            "count": pl.len(),
        }

        if agg_func not in agg_map:
            raise ValueError(f"Unsupported aggregation: {agg_func}")

        result = (
            df.group_by(group_col)
            .agg([agg_map[agg_func].alias(f"{metric_col}_{agg_func}"), pl.len().alias("count")])
            .sort(f"{metric_col}_{agg_func}", descending=True)
            .head(n)
        )

        return result


# ============================================================================
# UTILITY PLUGINS - General utilities
# ============================================================================


class UtilityPlugins(PluginRegistry):
    """General utility functions for notebooks."""

    @staticmethod
    def format_size(size_bytes: int) -> str:
        """
        Format file size in human-readable format.

        Args:
            size_bytes: Size in bytes

        Returns:
            Formatted string (e.g., "1.5 MB")

        Example:
            ```python
            size_str = utils.format_size(1500000)  # "1.4 MB"
            ```
        """
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"

    @staticmethod
    def parse_input_list(input_text: str, delimiter: str = ",") -> list[str]:
        """
        Parse delimited input into list of strings.

        Args:
            input_text: Input string
            delimiter: Delimiter character (default: comma)

        Returns:
            List of trimmed strings

        Example:
            ```python
            ids = utils.parse_input_list("MBR001, MBR002, MBR003")
            # ["MBR001", "MBR002", "MBR003"]
            ```
        """
        if not input_text or not input_text.strip():
            return []
        return [item.strip() for item in input_text.split(delimiter) if item.strip()]

    @staticmethod
    def create_multi_sheet_excel(
        sheets: dict[str, pl.DataFrame],
        filename: str | None = None,
    ) -> bytes:
        """
        Create Excel workbook with multiple sheets.

        Args:
            sheets: Dict mapping sheet names to DataFrames
            filename: Optional filename (not used, for API consistency)

        Returns:
            Bytes of Excel workbook

        Example:
            ```python
            excel_data = utils.create_multi_sheet_excel({
                "Claims": claims_df,
                "Summary": summary_df
            })
            ```
        """
        buffer = BytesIO()

        # Write first sheet to create workbook
        first_sheet_name = list(sheets.keys())[0]
        first_df = sheets[first_sheet_name]
        first_df.write_excel(buffer, worksheet=first_sheet_name)

        # TODO: Polars doesn't support writing multiple sheets in one call yet
        # For now, return single sheet. Will need xlsxwriter for multi-sheet.
        buffer.seek(0)
        return buffer.getvalue()


setup = SetupPlugins()
ui = UIPlugins()
data = DataPlugins()
analysis = AnalysisPlugins()
utils = UtilityPlugins()

__all__ = ["setup", "ui", "data", "analysis", "utils"]
