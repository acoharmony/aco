import marimo

__generated_with = "0.23.0"
app = marimo.App(
    width="medium",
    html_head_file="/opt/s3/data/notebooks/harmonycares-head.html",
)

with app.setup(hide_code=True):
    # All imports in one place for consistency and unit testing
    import sys
    from datetime import datetime
    from pathlib import Path

    import marimo as mo
    import polars as pl

    # Add src to path for ACOHarmony imports
    # Add acoharmony to path dynamically
    project_root = Path("/home/care/acoharmony")
    if project_root.exists():
        sys.path.insert(0, str(project_root / "src"))

    # Import ACOHarmony modules
    from acoharmony import Catalog
    from acoharmony._store import StorageBackend


@app.cell(hide_code=True)
def _():
    """Initialize storage backend with profile-driven configuration

    Medallion Architecture:
    - Bronze: Raw ingested data (BAR, ALR source files)
    - Silver: Cleaned, standardized data (bar.parquet, alr.parquet, demographics)
    - Gold: Analytics-ready consolidated data (consolidated_alignment.parquet)

    Storage is profile-driven and can be configured for:
    - Local filesystem (dev/local profiles)
    - S3-compatible storage (production profile)
    """
    storage = StorageBackend()

    # Initialize Catalog with storage backend so it uses the correct medallion paths
    catalog = Catalog(storage_config=storage)

    # Get medallion layer paths
    silver_path = storage.get_path("silver")
    gold_path = storage.get_path("gold")

    # Storage profile information for debugging
    storage_profile = storage.profile
    storage_backend = storage.get_storage_type()
    return catalog, gold_path


@app.function(hide_code=True)
def create_branded_header(datetime, mo):
    """Create branded header for HarmonyCares"""
    return mo.md(f"""
    <div style="background: linear-gradient(135deg, #2E3254 0%, #3d4466 100%);
                padding: 2rem;
                border-radius: 12px;
                margin-bottom: 2rem;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
        <div style="display: flex; align-items: center; justify-content: space-between;">
            <div>
                <h1 style="color: #ffffff; margin: 0; font-size: 2rem; font-weight: 700;">
                    <i class="fa-solid fa-users-medical" style="margin-right: 0.75rem; color: #60A5FA;"></i>
                    Consolidated Alignments Dashboard
                </h1>
                <p style="color: #E5E7EB; margin: 0.5rem 0 0 0; font-size: 1rem;">
                    REACH & MSSP Enrollment Analytics
                </p>
            </div>
            <div style="text-align: right;">
                <img src="https://harmonycaresaco.com/img/logo.svg"
                     alt="HarmonyCares Logo"
                     style="height: 60px; filter: brightness(0) invert(1);"
                     onerror="this.style.display='none'">
                <p style="color: #9CA3AF; margin: 0.5rem 0 0 0; font-size: 0.875rem;">
                    <i class="fa-solid fa-clock"></i> {datetime.now().strftime("%B %d, %Y • %I:%M %p")}
                </p>
            </div>
        </div>
    </div>

    <div style="background: #EFF6FF;
                border-left: 4px solid #3B82F6;
                padding: 1rem 1.5rem;
                border-radius: 8px;
                margin-bottom: 2rem;">
        <p style="margin: 0; color: #1E40AF; font-weight: 500;">
            <i class="fa-solid fa-circle-info" style="color: #3B82F6; margin-right: 0.5rem;"></i>
            <strong>Data Source:</strong> Silver Layer • Consolidated Alignment Pipeline
        </p>
    </div>
    """)


@app.function(hide_code=True)
def display_alignment_trends(alignment_trends, mo):
    """Display alignment trends table with proper formatting"""
    if alignment_trends is not None and len(alignment_trends) > 0:
        return mo.vstack([mo.md("## Alignment Trends Table"), mo.ui.table(alignment_trends, page_size=50)])
    else:
        return mo.md("## Alignment Trends\n\n*No temporal trend data available*")


@app.cell(hide_code=True)
def _(table):
    def display_temporal_trends(temporal_alignment_trends, selected_month_display, mo):
        """Display temporal trends with summary metrics"""
        if temporal_alignment_trends is None or temporal_alignment_trends.height == 0:
            return mo.md("**No temporal trend data available**")

        # Calculate growth metrics
        first_total = temporal_alignment_trends["Total ACO"][0]
        last_total = temporal_alignment_trends["Total ACO"][-1]
        growth = last_total - first_total
        growth_pct = (growth / first_total * 100) if first_total > 0 else 0

        # Find peak month
        peak_idx = temporal_alignment_trends["Total ACO"].arg_max()
        peak_month = temporal_alignment_trends["Month"][peak_idx]
        peak_value = temporal_alignment_trends["Total ACO"][peak_idx]

        summary = mo.md(
            f"""
            ## Temporal Alignment Trends

            ### ACO Enrollment Over Time
            <div style="padding: 0.5rem 1rem; background: #E0F2FE; border-left: 4px solid #0284C7; margin: 1rem 0;">
                <p style="margin: 0; color: #075985; font-size: 0.9rem;">
                    📈 Tracking enrollment changes from {temporal_alignment_trends["Month"][0]} to {temporal_alignment_trends["Month"][-1]}
                </p>
            </div>

            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #DCFCE7; border-radius: 8px;">
                    <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Total Growth</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">
                        {growth:+,}
                    </p>
                    <p style="margin: 0; color: #059669; font-size: 0.75rem;">{growth_pct:+.1f}% change</p>
                </div>
                <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px;">
                    <h3 style="margin: 0; color: #4C1D95; font-size: 0.875rem;">Peak Enrollment</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #4C1D95;">
                        {peak_value:,}
                    </p>
                    <p style="margin: 0; color: #4C1D95; font-size: 0.75rem;">in {peak_month}</p>
                </div>
                <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                    <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">Current Total</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #92400E;">
                        {last_total:,}
                    </p>
                    <p style="margin: 0; color: #92400E; font-size: 0.75rem;">as of {selected_month_display}</p>
                </div>
            </div>
            """
        )

        _table = mo.ui.table(temporal_alignment_trends.to_dicts(), page_size=50)
        return mo.vstack([summary, table])

    return


@app.function(hide_code=True)
def display_transitions_matrix(transition_stats, prev_ym, curr_ym, mo):
    """Display alignment transitions matrix"""
    if not prev_ym or not curr_ym:
        return mo.md("**No transition data available for selected period**")

    prev_display = f"{prev_ym[:4]}-{prev_ym[4:]}"
    curr_display = f"{curr_ym[:4]}-{curr_ym[4:]}"

    # Create visual transition display
    total_transitions = transition_stats["count"].sum()
    transition_rows = []

    for _row in transition_stats.iter_rows():
        trans_type = _row[0]
        count = _row[1]
        _pct = (count / total_transitions * 100) if total_transitions > 0 else 0

        # Color coding based on transition type
        if "→ REACH" in trans_type:
            _icon = "📈"
        elif "→ MSSP" in trans_type:
            _icon = "📊"
        elif "→ None" in trans_type:
            _icon = "📉"
        elif "REACH → REACH" in trans_type or "MSSP → MSSP" in trans_type:
            _icon = "🔄"
        else:
            _icon = "➡️"

        transition_rows.append(
            {
                "Transition": f"{_icon} {trans_type}",
                "Count": f"{count:,}",
                "Percentage": f"{_pct:.1f}%",
            }
        )

    _header = mo.md(
        f"""
        ## Alignment Transitions Matrix

        ### Month-over-Month Changes ({prev_display} → {curr_display})

        <div style="padding: 0.5rem 1rem; background: #F0F9FF; border-left: 4px solid #0284C7; margin: 1rem 0;">
            <p style="margin: 0; color: #075985; font-size: 0.9rem;">
                🔄 Tracking how beneficiaries move between programs
            </p>
        </div>
        """
    )

    return mo.vstack([_header, mo.ui.table(transition_rows, page_size=50)])


@app.function(hide_code=True)
def display_campaign_effectiveness(campaign_metrics, mo):
    """Display quarterly campaign effectiveness table"""
    if campaign_metrics is None or len(campaign_metrics) == 0:
        return mo.md("### Campaign Effectiveness\n\n*No campaign data available*")

    # Format the data for display
    formatted_data = []
    for _row in campaign_metrics.to_dicts():
        formatted_data.append(
            {
                "Quarter": _row["campaign_period"],
                "Contacted": f"{_row['total_contacted']:,}",
                "Email Only": f"{_row['emailed']:,}",
                "Mail Only": f"{_row['mailed']:,}",
                "Both Channels": f"{_row.get('both_email_and_letter', 0):,}",
                "Valid SVA": f"{_row.get('signed_sva', 0):,}",
                "SVA Rate": f"{_row.get('overall_conversion_rate', 0):.1f}%",
            }
        )

    _header = mo.md(
        """
        ### Quarterly Campaign Effectiveness

        <div style="padding: 0.75rem 1rem; background: #F0FDF4; border-left: 4px solid #10B981; margin: 1rem 0;">
            <p style="margin: 0; color: #166534; font-size: 0.9rem;">
                📊 Conversion rates to valid SVA signatures by outreach quarter
            </p>
        </div>
        """
    )

    return mo.vstack([_header, mo.ui.table(formatted_data, page_size=50)])


@app.function(hide_code=True)
def display_enrollment_patterns(df, df_enriched, selected_ym, mo, pl):
    """Display enrollment patterns - current vs historical"""
    # HISTORICAL enrollment patterns (all-time)
    historical_enrollment = df.select(
        [
            pl.col("has_continuous_enrollment").sum().alias("continuous_count"),
            pl.col("has_program_transition").sum().alias("transition_count"),
            pl.col("months_in_reach").mean().alias("avg_reach_months"),
            pl.col("months_in_mssp").mean().alias("avg_mssp_months"),
            pl.col("total_aligned_months").mean().alias("avg_total_months"),
            pl.col("enrollment_gaps").mean().alias("avg_gaps"),
        ]
    ).collect()

    # CURRENT enrollment patterns (only those aligned in selected month)
    current_enrollment = None
    if selected_ym:
        _reach_col = f"ym_{selected_ym}_reach"
        _mssp_col = f"ym_{selected_ym}_mssp"

        # Filter for actively aligned beneficiaries (alive and not ended)
        currently_aligned_enroll = df_enriched.filter(
            (pl.col(_reach_col) | pl.col(_mssp_col)) &
            build_living_filter(df_enriched, pl)
        )

        current_enrollment = currently_aligned_enroll.select(
            [
                pl.col("has_continuous_enrollment").sum().alias("continuous_count"),
                pl.col("has_program_transition").sum().alias("transition_count"),
                pl.col("months_in_reach").mean().alias("avg_reach_months"),
                pl.col("months_in_mssp").mean().alias("avg_mssp_months"),
                pl.col("total_aligned_months").mean().alias("avg_total_months"),
                pl.col("enrollment_gaps").mean().alias("avg_gaps"),
                pl.len().alias("current_total"),
            ]
        ).collect()

    # Display enrollment patterns with current vs historical
    hist_continuous = historical_enrollment["continuous_count"][0]
    hist_transition = historical_enrollment["transition_count"][0]
    hist_avg_reach = historical_enrollment["avg_reach_months"][0] or 0
    hist_avg_mssp = historical_enrollment["avg_mssp_months"][0] or 0
    hist_avg_total = historical_enrollment["avg_total_months"][0] or 0
    hist_avg_gaps = historical_enrollment["avg_gaps"][0] or 0

    if current_enrollment is not None and len(current_enrollment) > 0:
        curr_continuous = current_enrollment["continuous_count"][0]
        curr_transition = current_enrollment["transition_count"][0]
        curr_avg_reach = current_enrollment["avg_reach_months"][0] or 0
        curr_avg_mssp = current_enrollment["avg_mssp_months"][0] or 0
        curr_avg_total = current_enrollment["avg_total_months"][0] or 0
        curr_avg_gaps = current_enrollment["avg_gaps"][0] or 0
        curr_total = current_enrollment["current_total"][0]

        return mo.md(
            f"""
            ## Enrollment Patterns

            ### Current Month ({selected_ym}) - {curr_total:,} Aligned
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px;">
                    <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Continuous Enrollment</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">{curr_continuous:,}</p>
                    <p style="margin: 0; color: #059669; font-size: 0.75rem;">{curr_continuous / curr_total * 100:.1f}% of current</p>
                </div>
                <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px;">
                    <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Program Transitions</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #1E40AF;">{curr_transition:,}</p>
                    <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">{curr_transition / curr_total * 100:.1f}% of current</p>
                </div>
                <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                    <h3 style="margin: 0; color: #D97706; font-size: 0.875rem;">Avg Gaps</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #D97706;">{curr_avg_gaps:.1f}</p>
                </div>
            </div>

            ### Historical (All-Time)
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Continuous Enrollment</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.25rem; font-weight: 600; color: #374151;">{hist_continuous:,}</p>
                </div>
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Program Transitions</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.25rem; font-weight: 600; color: #374151;">{hist_transition:,}</p>
                </div>
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Avg Gaps</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.25rem; font-weight: 600; color: #374151;">{hist_avg_gaps:.1f}</p>
                </div>
            </div>
            """
        )
    else:
        return mo.md(
            f"""
            ## Enrollment Patterns (Historical)

            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px;">
                    <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Continuous Enrollment</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">{hist_continuous:,}</p>
                </div>
                <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px;">
                    <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Program Transitions</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #1E40AF;">{hist_transition:,}</p>
                </div>
                <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                    <h3 style="margin: 0; color: #D97706; font-size: 0.875rem;">Avg Gaps</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #D97706;">{hist_avg_gaps:.1f}</p>
                </div>
            </div>
            """
        )


@app.function(hide_code=True)
def calculate_alignment_transitions(df_enriched, selected_ym, year_months, pl):
    """Calculate alignment transitions between consecutive months"""
    # Use transform function
    from acoharmony._transforms._notebook_transitions import calculate_alignment_transitions as _calculate_alignment_transitions
    return _calculate_alignment_transitions(df_enriched, selected_ym, year_months)


@app.function(hide_code=True)
def calculate_current_and_historical_sources(df_enriched, selected_ym, pl):
    """Calculate BOTH current and historical alignment source distributions"""
    # Use transform function
    from acoharmony._transforms._notebook_utilities import calculate_current_and_historical_sources as _calculate_current_and_historical_sources
    return _calculate_current_and_historical_sources(df_enriched, selected_ym)


@app.function(hide_code=True)
def analyze_enrollment_patterns(df, df_enriched, selected_ym, pl):
    """Analyze enrollment patterns - current vs historical"""

    # HISTORICAL enrollment patterns (all-time)
    historical_enrollment = df.select(
        [
            pl.col("has_continuous_enrollment").sum().alias("continuous_count"),
            pl.col("has_program_transition").sum().alias("transition_count"),
            pl.col("months_in_reach").mean().alias("avg_reach_months"),
            pl.col("months_in_mssp").mean().alias("avg_mssp_months"),
            pl.col("total_aligned_months").mean().alias("avg_total_months"),
            pl.col("enrollment_gaps").mean().alias("avg_gaps"),
        ]
    ).collect()

    # CURRENT enrollment patterns (only those aligned in selected month)
    current_enrollment = None
    if selected_ym:
        _reach_col = f"ym_{selected_ym}_reach"
        _mssp_col = f"ym_{selected_ym}_mssp"

        # Filter for actively aligned beneficiaries (alive and not ended)
        currently_aligned_enroll = df_enriched.filter(
            (pl.col(_reach_col) | pl.col(_mssp_col)) &
            build_living_filter(df_enriched, pl)
        )

        current_enrollment = currently_aligned_enroll.select(
            [
                pl.col("has_continuous_enrollment").sum().alias("continuous_count"),
                pl.col("has_program_transition").sum().alias("transition_count"),
                pl.col("months_in_reach").mean().alias("avg_reach_months"),
                pl.col("months_in_mssp").mean().alias("avg_mssp_months"),
                pl.col("total_aligned_months").mean().alias("avg_total_months"),
                pl.col("enrollment_gaps").mean().alias("avg_gaps"),
                pl.len().alias("current_total"),
            ]
        ).collect()

    return historical_enrollment, current_enrollment


@app.function(hide_code=True)
def calculate_month_over_month_comparison(df_enriched, selected_ym, year_months, pl):
    """Calculate month-over-month comparison metrics"""
    # Use transform function
    from acoharmony._transforms._notebook_transitions import calculate_month_over_month_comparison as _calculate_month_over_month_comparison
    return _calculate_month_over_month_comparison(df_enriched, selected_ym, year_months)


@app.function(hide_code=True)
def calculate_cohort_analysis(df_enriched, year_months, pl):
    """Cohort analysis for alignment patterns"""
    # Use transform function
    from acoharmony._transforms._notebook_cohort_analysis import calculate_cohort_analysis as _calculate_cohort_analysis
    return _calculate_cohort_analysis(df_enriched, year_months)


@app.cell(hide_code=True)
def _():
    """Import current REACH and MSSP attribution expressions"""
    from datetime import date, timedelta
    from acoharmony._expressions._current_reach import build_current_reach_expr
    from acoharmony._expressions._current_mssp import build_current_mssp_expr
    from acoharmony._expressions._enrollment_status import build_living_beneficiary_expr

    return (build_living_beneficiary_expr,)


@app.cell(hide_code=True)
def _(build_living_beneficiary_expr):
    def create_excel_workbook(
        current_alignment_stats,
        historical_stats,
        alignment_trends,
        transition_stats,
        vintage_distribution,
        df_enriched,
        df,
        most_recent_ym,
        pl,
        sva_stats,
        action_stats,
        outreach_metrics,
        office_stats,
        office_alignment_types,
        office_program_dist,
        office_transitions,
        office_metadata,
        office_campaign_metrics,
        office_vintage_distribution,
        year_over_year_newly_added_beneficiaries,
    ):
        """Create comprehensive Excel workbook with ALL alignment analysis results and source data (idempotent)"""
        from io import BytesIO
        from datetime import datetime
        import xlsxwriter

        # Create a BytesIO buffer to hold the Excel file
        buffer = BytesIO()

        # Create xlsxwriter workbook directly with optimization for large files
        workbook = xlsxwriter.Workbook(buffer, {
            'in_memory': True,
            'constant_memory': True,  # Use constant memory mode for large datasets
            'strings_to_numbers': True,
            'strings_to_urls': False,
            'nan_inf_to_errors': True  # Convert NaN/Inf to Excel errors instead of raising exceptions
        })

        # Helper function to write section with heading
        def write_section_heading(worksheet, row, title, heading_format):
            """Write a section heading and return next row number"""
            worksheet.write(row, 0, title, heading_format)
            return row + 1

        def write_dataframe_section(worksheet, row, title, dataframe, heading_format):
            """Write a section with heading followed by dataframe, return next row"""
            if dataframe is None:
                return row

            # Ensure dataframe is collected if it's a LazyFrame
            if hasattr(dataframe, 'collect'):
                dataframe = dataframe.collect()

            if dataframe.height == 0:
                return row

            # Write section heading
            row = write_section_heading(worksheet, row, title, heading_format)

            # Write column headers
            for col_idx, col_name in enumerate(dataframe.columns):
                worksheet.write(row, col_idx, str(col_name))
            row += 1

            # Write data rows
            for row_data in dataframe.iter_rows():
                for col_idx, value in enumerate(row_data):
                    # Handle None values
                    if value is None:
                        worksheet.write(row, col_idx, "")
                    # Handle lists, dicts, and other non-primitive types
                    elif isinstance(value, (list, dict, tuple)):
                        worksheet.write(row, col_idx, str(value))
                    else:
                        worksheet.write(row, col_idx, value)
                row += 1

            return row + 2  # Add spacing after section

        # Define formats for the summary sheet
        title_format = workbook.add_format({
            'bold': True,
            'font_size': 16,
            'font_color': '#2E3254',
            'bottom': 2
        })
        header_format = workbook.add_format({
            'bold': True,
            'font_size': 12,
            'bg_color': '#EFF6FF',
            'font_color': '#1E40AF',
            'border': 1
        })
        metric_label_format = workbook.add_format({
            'bold': True,
            'font_color': '#374151'
        })
        metric_value_format = workbook.add_format({
            'font_size': 14,
            'font_color': '#059669',
            'num_format': '#,##0'
        })

        # ===== Sheet 1: Executive Summary =====
        summary_ws = workbook.add_worksheet("Executive_Summary")
        summary_ws.set_column('A:A', 40)
        summary_ws.set_column('B:B', 25)

        row = 0
        summary_ws.write(row, 0, "Consolidated Alignment Analysis - Executive Summary", title_format)
        summary_ws.write(row, 1, datetime.now().strftime("%Y-%m-%d %H:%M"), metric_label_format)
        row += 2

        # Calculate correct metrics from the dataframes
        total_population = df.select(pl.len()).collect().item()

        # Current Program Enrollment (from temporal columns for most recent observation)
        # Use temporal columns for the selected observation month
        schema = df.collect_schema().names()

        if most_recent_ym:
            current_reach_col = f"ym_{most_recent_ym}_reach"
            current_mssp_col = f"ym_{most_recent_ym}_mssp"
            current_ffs_col = f"ym_{most_recent_ym}_ffs"

            # Filter for living beneficiaries
            living_expr = build_living_beneficiary_expr(schema)
            df_living = df.filter(living_expr)

            if current_reach_col in schema and current_mssp_col in schema and current_ffs_col in schema:
                current_stats = df_living.select([
                    pl.col(current_reach_col).sum().alias("current_reach"),
                    pl.col(current_mssp_col).sum().alias("current_mssp"),
                    pl.col(current_ffs_col).sum().alias("current_ffs"),
                ]).collect()

                current_reach = current_stats["current_reach"][0]
                current_mssp = current_stats["current_mssp"][0]
                current_ffs = current_stats["current_ffs"][0]
                total_aligned = current_reach + current_mssp
            else:
                current_reach = current_mssp = current_ffs = total_aligned = 0
        else:
            current_reach = current_mssp = current_ffs = total_aligned = 0

        summary_ws.write(row, 0, "CURRENT PROGRAM ENROLLMENT", header_format)
        row += 1
        summary_ws.write(row, 0, "Total Beneficiaries", metric_label_format)
        summary_ws.write(row, 1, total_population, metric_value_format)
        row += 1
        summary_ws.write(row, 0, f"REACH DCE Members ({most_recent_ym or 'N/A'})", metric_label_format)
        summary_ws.write(row, 1, current_reach, metric_value_format)
        row += 1
        summary_ws.write(row, 0, f"MSSP Members ({most_recent_ym or 'N/A'})", metric_label_format)
        summary_ws.write(row, 1, current_mssp, metric_value_format)
        row += 1
        summary_ws.write(row, 0, "Total ACO Aligned", metric_label_format)
        summary_ws.write(row, 1, total_aligned, metric_value_format)
        row += 1
        summary_ws.write(row, 0, "Fee-for-Service", metric_label_format)
        summary_ws.write(row, 1, current_ffs, metric_value_format)
        row += 2

        # Voluntary Alignment Summary
        summary_ws.write(row, 0, "VOLUNTARY ALIGNMENT STATUS", header_format)
        row += 1

        # Get SVA stats
        if sva_stats:
            summary_ws.write(row, 0, "Valid SVA Signatures (in REACH with valid provider)", metric_label_format)
            summary_ws.write(row, 1, sva_stats.get('has_valid_signature', 0), metric_value_format)
            row += 1
            summary_ws.write(row, 0, "Total with SVA Signatures (ever)", metric_label_format)
            summary_ws.write(row, 1, sva_stats.get('ever_voluntary', 0), metric_value_format)
            row += 1
            summary_ws.write(row, 0, "Total Contacted for Voluntary Alignment", metric_label_format)
            summary_ws.write(row, 1, sva_stats.get('total_contacted', 0), metric_value_format)
            row += 2

        # Action Stats
        if action_stats is not None and action_stats.height > 0:
            summary_ws.write(row, 0, "SVA ACTION BREAKDOWN", header_format)
            row += 1
            for action_row in action_stats.to_dicts():
                summary_ws.write(row, 0, f"  {action_row['sva_action_needed']}", metric_label_format)
                summary_ws.write(row, 1, action_row['count'], metric_value_format)
                row += 1
            row += 1

        # Vintage Cohorts
        if vintage_distribution is not None and vintage_distribution.height > 0:
            summary_ws.write(row, 0, "ENROLLMENT VINTAGE COHORTS", header_format)
            row += 1
            for cohort_row in vintage_distribution.to_dicts():
                if cohort_row['vintage_cohort'] != 'Never Enrolled':
                    summary_ws.write(row, 0, f"  {cohort_row['vintage_cohort']}", metric_label_format)
                    summary_ws.write(row, 1, cohort_row['count'], metric_value_format)
                    row += 1
            row += 1

        # Outreach Metrics
        if outreach_metrics:
            summary_ws.write(row, 0, "OUTREACH CAMPAIGN METRICS", header_format)
            row += 1
            summary_ws.write(row, 0, "Total Beneficiaries Emailed", metric_label_format)
            summary_ws.write(row, 1, outreach_metrics.get('total_emailed', 0), metric_value_format)
            row += 1
            summary_ws.write(row, 0, "Total Beneficiaries Mailed", metric_label_format)
            summary_ws.write(row, 1, outreach_metrics.get('total_mailed', 0), metric_value_format)
            row += 1
            summary_ws.write(row, 0, "Total Contacted (Email or Mail)", metric_label_format)
            summary_ws.write(row, 1, outreach_metrics.get('total_contacted', 0), metric_value_format)
            row += 1
            summary_ws.write(row, 0, "Contacted with Valid SVA", metric_label_format)
            summary_ws.write(row, 1, outreach_metrics.get('contacted_with_sva', 0), metric_value_format)
            row += 1
            summary_ws.write(row, 0, "Email Opened with Valid SVA", metric_label_format)
            summary_ws.write(row, 1, outreach_metrics.get('opened_with_sva', 0), metric_value_format)
            row += 1
            summary_ws.write(row, 0, "Contact to SVA Conversion Rate", metric_label_format)
            summary_ws.write(row, 1, f"{outreach_metrics.get('contacted_to_sva_rate', 0):.1f}%", metric_label_format)
            row += 2

        # Data Sources
        summary_ws.write(row, 0, "DATA SOURCES", header_format)
        row += 1
        summary_ws.write(row, 0, "• BAR files (REACH alignment)")
        row += 1
        summary_ws.write(row, 0, "• ALR files (MSSP alignment)")
        row += 1
        summary_ws.write(row, 0, "• SVA signatures & PBVAR responses")
        row += 1
        summary_ws.write(row, 0, "• Beneficiary demographics")
        row += 1
        summary_ws.write(row, 0, "• Fee-for-Service claims")
        row += 1
        summary_ws.write(row, 0, "• Email and mail campaign data")

        # Prepare all analysis dataframes once
        try:
            sva_action_df = df.group_by("sva_action_needed").agg([
                pl.len().alias("count"),
                pl.col("has_voluntary_alignment").sum().alias("has_sva"),
                pl.col("has_valid_voluntary_alignment").sum().alias("has_valid_sva"),
            ]).collect().sort("count", descending=True)
        except:
            sva_action_df = None

        try:
            outreach_priority_df = df.group_by("outreach_priority").agg([
                pl.len().alias("count"),
                pl.col("has_voluntary_outreach").sum().alias("contacted"),
                pl.col("voluntary_email_count").sum().alias("total_emails"),
                pl.col("voluntary_letter_count").sum().alias("total_letters"),
            ]).collect().sort("count", descending=True)
        except:
            outreach_priority_df = None

        try:
            program_dist_df = df.group_by("consolidated_program").agg([
                pl.len().alias("count"),
                pl.col("months_in_reach").mean().alias("avg_months_reach"),
                pl.col("months_in_mssp").mean().alias("avg_months_mssp"),
                pl.col("has_program_transition").sum().alias("transitions"),
            ]).collect().sort("count", descending=True)
        except:
            program_dist_df = None

        current_status_df = None
        if most_recent_ym:
            try:
                # Use temporal columns for the most recent observation month
                schema = df.collect_schema().names()
                reach_col = f"ym_{most_recent_ym}_reach"
                mssp_col = f"ym_{most_recent_ym}_mssp"
                ffs_col = f"ym_{most_recent_ym}_ffs"

                if all(col in schema for col in [reach_col, mssp_col, ffs_col]):
                    # Filter for living beneficiaries only
                    living_expr = build_living_beneficiary_expr(schema)
                    current_status_df = df.filter(living_expr).group_by([
                        pl.col(reach_col).alias("in_reach"),
                        pl.col(mssp_col).alias("in_mssp"),
                        pl.col(ffs_col).alias("in_ffs"),
                    ]).agg([
                        pl.len().alias("count"),
                        pl.col("has_voluntary_alignment").sum().alias("has_sva"),
                        pl.col("has_valid_voluntary_alignment").sum().alias("has_valid_sva"),
                    ]).collect().sort("count", descending=True)
            except:
                pass

        try:
            historical_aco_df = df.group_by(["ever_reach", "ever_mssp"]).agg([
                pl.len().alias("count"),
                pl.col("months_in_reach").mean().alias("avg_months_reach"),
                pl.col("months_in_mssp").mean().alias("avg_months_mssp"),
                pl.col("total_aligned_months").mean().alias("avg_total_months"),
                pl.col("has_program_transition").sum().alias("transitions"),
                pl.col("has_continuous_enrollment").sum().alias("continuous"),
                pl.col("enrollment_gaps").mean().alias("avg_gaps"),
            ]).collect().sort("count", descending=True)
        except:
            historical_aco_df = None

        try:
            voluntary_analysis_df = df.group_by([
                "has_voluntary_alignment",
                "has_valid_voluntary_alignment",
                "voluntary_alignment_type"
            ]).agg([
                pl.len().alias("count"),
                pl.col("ever_reach").sum().alias("ever_reach_count"),
                pl.col("ever_mssp").sum().alias("ever_mssp_count"),
                # Removed action_breakdown (value_counts returns lists which can't be written to Excel)
            ]).collect().sort("count", descending=True)
        except:
            voluntary_analysis_df = None

        try:
            enrollment_patterns_df = df.group_by([
                "has_continuous_enrollment",
                "has_program_transition",
            ]).agg([
                pl.len().alias("count"),
                pl.col("months_in_reach").mean().alias("avg_months_reach"),
                pl.col("months_in_mssp").mean().alias("avg_months_mssp"),
                pl.col("total_aligned_months").mean().alias("avg_total_months"),
                pl.col("enrollment_gaps").mean().alias("avg_gaps"),
                pl.col("observable_start").min().alias("earliest_start"),
                pl.col("observable_end").max().alias("latest_end"),
            ]).collect().sort("count", descending=True)
        except:
            enrollment_patterns_df = None

        try:
            if 'has_voluntary_outreach' in df_enriched.collect_schema().names():
                outreach_analysis_df = df_enriched.group_by([
                    "has_voluntary_outreach",
                    "outreach_priority",
                    "sva_action_needed"
                ]).agg([
                    pl.len().alias("count"),
                    pl.col("voluntary_email_count").sum().alias("total_emails"),
                    pl.col("voluntary_letter_count").sum().alias("total_letters"),
                    pl.col("voluntary_email_count").filter(pl.col("voluntary_email_count") > 0).len().alias("received_emails"),
                    pl.col("voluntary_letter_count").filter(pl.col("voluntary_letter_count") > 0).len().alias("received_letters"),
                ]).collect().sort("count", descending=True)
            else:
                outreach_analysis_df = None
        except:
            outreach_analysis_df = None

        # ===== Sheet 2: ALL Analysis Tables Consolidated on Single Sheet =====
        ws_analysis = workbook.add_worksheet("Alignment Analysis")
        row = 0

        # Current Analysis Section
        row = write_dataframe_section(ws_analysis, row, "## Current Summary Statistics", current_alignment_stats, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Current Enrollment Status", current_status_df, header_format)

        # Historical Analysis Section
        row = write_dataframe_section(ws_analysis, row, "## Historical Summary Statistics", historical_stats, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Historical ACO Program Participation", historical_aco_df, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Vintage Cohort Analysis", vintage_distribution, header_format)

        # Trends & Transitions Section
        row = write_dataframe_section(ws_analysis, row, "## Alignment Trends Over Time", alignment_trends, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Program Transitions Matrix", transition_stats, header_format)

        # Year-over-Year Newly Added Beneficiaries
        if year_over_year_newly_added_beneficiaries is not None:
            row = write_dataframe_section(ws_analysis, row, "## Year-over-Year Newly Added Beneficiaries (2025 → 2026)", year_over_year_newly_added_beneficiaries, header_format)

        # Voluntary Alignment & SVA Section
        row = write_dataframe_section(ws_analysis, row, "## SVA Action Breakdown", sva_action_df, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Voluntary Alignment Analysis", voluntary_analysis_df, header_format)

        # Enrollment Patterns Section
        row = write_dataframe_section(ws_analysis, row, "## Enrollment Pattern Details", enrollment_patterns_df, header_format)

        # Outreach & Program Section
        row = write_dataframe_section(ws_analysis, row, "## Outreach Priority Breakdown", outreach_priority_df, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Program Distribution", program_dist_df, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Outreach Campaign Analysis", outreach_analysis_df, header_format)

        # Office Analysis Section
        row = write_dataframe_section(ws_analysis, row, "## Office Enrollment Statistics", office_stats, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Office Alignment Types", office_alignment_types, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Office Program Distribution", office_program_dist, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Office Transitions", office_transitions, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Office Metadata", office_metadata, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Office Campaign Effectiveness", office_campaign_metrics, header_format)
        row = write_dataframe_section(ws_analysis, row, "## Office Vintage Distribution", office_vintage_distribution, header_format)

        # ===== Sheet 3: Full_Data (Complete consolidated_alignment from GOLD) =====
        # OPTIMIZATION: Collect once and reuse
        full_data_collected = None
        try:
            # Get all records - collect once for reuse
            full_data_collected = df.collect()
            if full_data_collected.height > 0:
                full_data_collected.write_excel(
                    workbook=workbook,
                    worksheet="Full_Data",
                    column_formats=None,
                    autofit=False,
                    float_precision=2
                )
        except Exception:
            try:
                subset = df.collect()
                if subset.height > 0:
                    subset.write_excel(workbook=workbook, worksheet="Full_Data")
            except:
                pass

        # ===== Sheet 4: Temporal_Enrollment (Month-by-month enrollment tracking) =====
        try:
            if full_data_collected is not None:
                ym_cols = sorted([col for col in full_data_collected.columns if col.startswith('ym_')])
                if ym_cols and 'current_mbi' in full_data_collected.columns:
                    ym_cols_with_mbi = ['current_mbi', 'bene_mbi', 'current_program'] + ym_cols
                    existing_ym_cols = [col for col in ym_cols_with_mbi if col in full_data_collected.columns]
                    ym_data = full_data_collected.select(existing_ym_cols)
                    if ym_data.height > 0:
                        ym_data.write_excel(
                            workbook=workbook,
                            worksheet="Temporal_Enrollment",
                            column_formats=None,
                            autofit=False,
                            float_precision=2
                        )
        except Exception:
            pass

        # ===== Sheet 5: Data_Dictionary =====
        try:
            data_dict_data = {
                "Sheet": [],
                "Column_Name": [],
                "Description": [],
                "Data_Type": []
            }
            key_columns = {
                "bene_mbi": ("Beneficiary Medicare Beneficiary Identifier", "String"),
                "current_program": ("Current enrollment program (REACH/MSSP/FFS)", "String"),
                "first_reach_date": ("First month beneficiary was aligned to REACH", "Date"),
                "last_reach_date": ("Last month beneficiary was aligned to REACH", "Date"),
                "first_mssp_date": ("First month beneficiary was aligned to MSSP", "Date"),
                "last_mssp_date": ("Last month beneficiary was aligned to MSSP", "Date"),
                "reach_months": ("Number of months aligned to REACH", "Integer"),
                "mssp_months": ("Number of months aligned to MSSP", "Integer"),
                "office_location": ("Office/market location", "String"),
                "birth_date": ("Beneficiary date of birth", "Date"),
                "death_date": ("Beneficiary date of death", "Date"),
            }
            for col, (desc, dtype) in key_columns.items():
                data_dict_data["Sheet"].append("Full_Data")
                data_dict_data["Column_Name"].append(col)
                data_dict_data["Description"].append(desc)
                data_dict_data["Data_Type"].append(dtype)
            data_dict_data["Sheet"].append("Metadata")
            data_dict_data["Column_Name"].append("Report_Generated")
            data_dict_data["Description"].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            data_dict_data["Data_Type"].append("Timestamp")
            data_dict_data["Sheet"].append("Metadata")
            data_dict_data["Column_Name"].append("Source_File")
            data_dict_data["Description"].append("consolidated_alignment.parquet (GOLD tier)")
            data_dict_data["Data_Type"].append("Path")
            data_dict_df = pl.DataFrame(data_dict_data)
            data_dict_df.write_excel(
                workbook=workbook,
                worksheet="Data_Dictionary",
                column_formats=None,
                autofit=True
            )
        except Exception:
            pass

        # ===== Sheet 6-10: Raw Source Data Tables =====
        try:
            from acoharmony._store import StorageBackend
            storage = StorageBackend()
            silver_path = storage.get_path("silver")

            # Sheet 6: BAR
            bar_path = silver_path / "bar.parquet"
            if bar_path.exists():
                bar_df = pl.scan_parquet(str(bar_path)).collect()
                if bar_df.height > 0:
                    bar_df.write_excel(workbook=workbook, worksheet="BAR", column_formats=None, autofit=False, float_precision=2)

            # Sheet 7: ALR
            alr_path = silver_path / "alr.parquet"
            if alr_path.exists():
                alr_df = pl.scan_parquet(str(alr_path)).collect()
                if alr_df.height > 0:
                    alr_df.write_excel(workbook=workbook, worksheet="ALR", column_formats=None, autofit=False, float_precision=2)

            # Sheet 8: PBAR
            pbar_path = silver_path / "pbar.parquet"
            if pbar_path.exists():
                pbar_df = pl.scan_parquet(str(pbar_path)).collect()
                if pbar_df.height > 0:
                    pbar_df.write_excel(workbook=workbook, worksheet="PBAR", column_formats=None, autofit=False, float_precision=2)

            # Sheet 9: PBVAR
            pbvar_path = silver_path / "pbvar.parquet"
            if pbvar_path.exists():
                pbvar_df = pl.scan_parquet(str(pbvar_path)).collect()
                if pbvar_df.height > 0:
                    pbvar_df.write_excel(workbook=workbook, worksheet="PBVAR", column_formats=None, autofit=False, float_precision=2)

            # Sheet 10: SVA
            sva_path = silver_path / "sva.parquet"
            if sva_path.exists():
                sva_df = pl.scan_parquet(str(sva_path)).collect()
                if sva_df.height > 0:
                    sva_df.write_excel(workbook=workbook, worksheet="SVA", column_formats=None, autofit=False, float_precision=2)
        except Exception:
            pass

        workbook.close()

        # Seek to beginning and return the buffer itself (not getvalue())
        # marimo.download expects BytesIO object for proper handling
        buffer.seek(0)
        return buffer

    return (create_excel_workbook,)


@app.cell(hide_code=True)
def _(create_excel_workbook):
    def display_excel_export_button(
        current_alignment_stats,
        historical_stats,
        alignment_trends,
        transition_stats,
        vintage_distribution,
        df,
        df_enriched,
        datetime,
        mo,
        most_recent_ym,
        pl,
        sva_stats,
        action_stats,
        outreach_metrics,
        office_stats,
        office_alignment_types,
        office_program_dist,
        office_transitions,
        office_metadata,
        office_campaign_metrics,
        office_vintage_distribution,
        newly_added_stats,
    ):
        """Create and display Excel export button with alignment analysis (idempotent with lazy loading)"""
        from datetime import datetime as dt

        # Create a lazy loading function that generates Excel data on-demand
        def generate_excel_data():
            """Lazy load Excel data when button is clicked"""
            try:
                # Prepare year-over-year newly added beneficiaries data for Excel
                _yoy_newly_added_df = None
                if newly_added_stats is not None and 'source_breakdown' in newly_added_stats:
                    _yoy_newly_added_df = newly_added_stats['source_breakdown']

                buffer = create_excel_workbook(
                    current_alignment_stats,
                    historical_stats,
                    alignment_trends,
                    transition_stats,
                    vintage_distribution,
                    df,
                    df_enriched,
                    most_recent_ym,
                    pl,
                    sva_stats,
                    action_stats,
                    outreach_metrics,
                    office_stats,
                    office_alignment_types,
                    office_program_dist,
                    office_transitions,
                    office_metadata,
                    office_campaign_metrics,
                    office_vintage_distribution,
                    _yoy_newly_added_df,
                )
                # Ensure we're at the start of the buffer
                buffer.seek(0)
                return buffer
            except Exception as e:
                # Log error and re-raise for debugging
                print(f"Error generating Excel workbook: {e}")
                import traceback
                traceback.print_exc()
                raise

        # Generate timestamp for filename
        timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
        filename = f"consolidated_alignments_{timestamp}.xlsx"

        # Return download button with lazy data generation
        return mo.download(
            data=generate_excel_data,
            filename=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            label=f"📥 Download Excel Report ({filename})",
        )

    return (display_excel_export_button,)


@app.function(hide_code=True)
def calculate_vintage_cohorts(df_enriched, most_recent_ym, pl):
    """Calculate vintage cohorts based on first enrollment date"""
    # Use transform function
    from acoharmony._transforms._notebook_vintage import calculate_vintage_cohorts as _calculate_vintage_cohorts
    return _calculate_vintage_cohorts(df_enriched, most_recent_ym)


@app.function(hide_code=True)
def calculate_vintage_distribution(vintage_df, most_recent_ym, pl):
    """Calculate vintage cohort statistics and distribution"""
    # Use transform function
    from acoharmony._transforms._notebook_vintage import calculate_vintage_distribution as _calculate_vintage_distribution
    return _calculate_vintage_distribution(vintage_df, most_recent_ym)


@app.function(hide_code=True)
def calculate_office_vintage_distribution(vintage_df, most_recent_ym, pl):
    """
    Calculate vintage cohort statistics by office_name.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Args:
        vintage_df: LazyFrame with vintage cohort data (includes office_name, office_location)
        most_recent_ym: Most recent year-month string (e.g., "202401")
        pl: Polars module

    Returns:
        DataFrame with vintage distribution by office_name, or None if yearmo not available
    """
    # Use transform function
    from acoharmony._transforms._notebook_vintage import calculate_office_vintage_distribution as _calculate_office_vintage_distribution
    return _calculate_office_vintage_distribution(vintage_df, most_recent_ym)


@app.function(hide_code=True)
def display_vintage_cohort_overview(vintage_distribution, mo, pl):
    """Display vintage cohort overview with metrics cards"""

    if vintage_distribution is not None and vintage_distribution.height > 0:
        # Filter out never enrolled for the main display
        _enrolled_only = vintage_distribution.filter(pl.col('vintage_cohort') != 'Never Enrolled')

        # Return the markdown to display it
        return mo.md(f"""
        ### Enrollment Vintage Cohorts

        <div style="background: #EFF6FF; border-left: 4px solid #3B82F6; padding: 1rem 1.5rem; border-radius: 8px; margin-bottom: 2rem;">
            <p style="margin: 0; color: #1E40AF;">
                <i class="fa-solid fa-calendar-days"></i>
                <strong>Cohort Analysis:</strong> Beneficiaries grouped by time since first enrollment (REACH or MSSP)
            </p>
        </div>

        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin: 1rem 0;">
            {"".join([
                f'''<div style="padding: 1rem; background: {'#DCFCE7' if i == 0 else '#FEF3C7' if i == 1 else '#DBEAFE' if i == 2 else '#E0E7FF'}; border-radius: 8px;">
                    <h3 style="margin: 0; color: {'#059669' if i == 0 else '#D97706' if i == 1 else '#1E40AF' if i == 2 else '#6366F1'}; font-size: 0.875rem;">{_row['vintage_cohort']}</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: {'#059669' if i == 0 else '#D97706' if i == 1 else '#1E40AF' if i == 2 else '#6366F1'};">{_row['count']:,}</p>
                    <p style="margin: 0; color: {'#059669' if i == 0 else '#D97706' if i == 1 else '#1E40AF' if i == 2 else '#6366F1'}; font-size: 0.75rem;">{_row['pct_of_enrolled']:.1f}% of enrolled</p>
                </div>'''
                for i, _row in enumerate(_enrolled_only.iter_rows(named=True))
            ])}
        </div>
        """)
    return None


@app.function(hide_code=True)
def display_vintage_cohort_table(vintage_distribution, mo, pl):
    """Display detailed vintage cohort metrics table"""

    if vintage_distribution is not None and vintage_distribution.height > 0:
        _enrolled_only = vintage_distribution.filter(pl.col('vintage_cohort') != 'Never Enrolled')

        # Create table data
        _table_data = []
        for _row in _enrolled_only.iter_rows(named=True):
            _table_data.append({
                "Vintage Cohort": _row['vintage_cohort'],
                "Total Count": f"{_row['count']:,}",
                "Current REACH": f"{_row['current_reach']:,} ({_row['pct_in_reach']:.1f}%)",
                "Current MSSP": f"{_row['current_mssp']:,} ({_row['pct_in_mssp']:.1f}%)",
                "Avg Months REACH": f"{_row['avg_months_reach']:.1f}",
                "Avg Months MSSP": f"{_row['avg_months_mssp']:.1f}",
                "Avg Total Months": f"{_row['avg_total_months']:.1f}",
                "Program Transitions": f"{_row['transitions']:,} ({_row['pct_with_transitions']:.1f}%)",
            })

        _table = mo.ui.table(
            _table_data,
            selection=None,
            label="Vintage Cohort Metrics",
            page_size=50
        )

        # Return the vstack to display it
        return mo.vstack([
            mo.md("""
            ### Cohort Metrics Detail

            <div style="padding: 0.75rem 1rem; background: #F0FDF4; border-left: 4px solid #10B981; margin: 1rem 0;">
                <p style="margin: 0; color: #166534; font-size: 0.9rem;">
                    📊 Time in programs and transition patterns by enrollment vintage
                </p>
            </div>
            """),
            _table
        ])
    return None


@app.function(hide_code=True)
def display_technical_appendix(mo):
    """Display technical appendix with pipeline documentation using nested accordions"""
    import inspect
    from acoharmony._transforms import _aco_alignment_temporal
    from acoharmony._transforms import _aco_alignment_voluntary
    from acoharmony._transforms import _aco_alignment_demographics
    from acoharmony._transforms import _aco_alignment_office
    from acoharmony._transforms import _aco_alignment_provider
    from acoharmony._transforms import _aco_alignment_metrics
    from acoharmony._transforms import _aco_alignment_metadata
    from acoharmony._expressions._aco_temporal_summary import build_summary_statistics_exprs
    from acoharmony._pipes._alignment import apply_alignment_pipeline as apply_aco_alignment_pipeline

    # Extract pipeline-level documentation
    pipeline_doc = inspect.getdoc(apply_aco_alignment_pipeline)

    # Extract docstrings from temporal alignment functions (Stage 0)
    stage0_doc = inspect.getdoc(_aco_alignment_temporal.apply_transform)
    aco_step1_doc = inspect.getdoc(_aco_alignment_temporal._determine_observable_range)
    aco_step2_doc = inspect.getdoc(_aco_alignment_temporal._build_mbi_map)
    aco_step3a_doc = inspect.getdoc(_aco_alignment_temporal._prepare_bar_data)
    aco_step3b_doc = inspect.getdoc(_aco_alignment_temporal._prepare_alr_data)
    aco_step3c_doc = inspect.getdoc(_aco_alignment_temporal._prepare_ffs_data)
    aco_step3d_doc = inspect.getdoc(_aco_alignment_temporal._prepare_demographics)
    aco_step4_doc = inspect.getdoc(_aco_alignment_temporal._build_temporal_matrix_vectorized)
    aco_step5_doc = inspect.getdoc(build_summary_statistics_exprs)

    # Extract docstrings from pipeline stage transforms (Stages 1-6)
    stage1_doc = inspect.getdoc(_aco_alignment_voluntary.apply_transform)
    stage2_doc = inspect.getdoc(_aco_alignment_demographics.apply_transform)
    stage3_doc = inspect.getdoc(_aco_alignment_office.apply_transform)
    stage4_doc = inspect.getdoc(_aco_alignment_provider.apply_transform)
    stage5_doc = inspect.getdoc(_aco_alignment_metrics.apply_transform)
    stage6_doc = inspect.getdoc(_aco_alignment_metadata.apply_transform)

    # Build nested accordion for Stage 0 (Temporal Matrix) internal steps
    stage0_steps_accordion = mo.accordion({
        "Step 1: Determine Observable Date Range": aco_step1_doc or "No documentation available",
        "Step 2: Build MBI Crosswalk Map": aco_step2_doc or "No documentation available",
        "Step 3a: Prepare BAR Data": aco_step3a_doc or "No documentation available",
        "Step 3b: Prepare ALR Data": aco_step3b_doc or "No documentation available",
        "Step 3c: Prepare FFS Data": aco_step3c_doc or "No documentation available",
        "Step 3d: Prepare Demographics Data": aco_step3d_doc or "No documentation available",
        "Step 4: Build Point-in-Time Temporal Matrix": aco_step4_doc or "No documentation available",
        "Step 5: Calculate Summary Statistics": aco_step5_doc or "No documentation available",
    }, multiple=True, lazy=True)

    # Build the main accordion structure
    main_accordion = mo.accordion({
        "📊 Pipeline Overview": f"""
{pipeline_doc or "No pipeline documentation available"}

### Data Flow Summary

```
Bronze (Raw CMS Files)
├── BAR files (REACH alignment)
├── ALR files (MSSP alignment)
├── FFS claims
├── Demographics
└── Crosswalk
        ↓
Pipeline Processing (7 Stages)
│
├── Stage 0: temporal_matrix (foundation)
├── Stage 1: voluntary_alignment
├── Stage 2: demographics
├── Stage 3: office_matching
├── Stage 4: provider_attribution
├── Stage 5: consolidated_metrics
└── Stage 6: metadata_and_actions
        ↓
Gold (Analytics-Ready)
└── consolidated_alignment (final joined view with all metrics)
```

### Key Technical Decisions

1. **Point-in-Time Tracking**: Each `ym_YYYYMM_*` column uses only data files available AS OF that month
2. **Mutual Exclusivity**: REACH and MSSP enrollment are mutually exclusive; REACH (BAR) takes precedence
3. **SVA Validity**: SVA signatures are ONLY valid for REACH DCE, not MSSP
4. **Provider Validation**: Only "Participant Providers" count as valid (not "Preferred Providers")
5. **FFS Logic**: FFS enrollment = (has_first_claim_date AND not_in_aco)
6. **Idempotent Stages**: All stages can be safely re-run
7. **Sequential Dependencies**: Each stage depends on the previous stage's output
        """,

        "🏗️ Stage 0: Temporal Matrix (Foundation)": mo.vstack([
            mo.md(f"""
### Stage 0: Temporal Matrix

{stage0_doc or "No documentation available"}

This is the foundation stage that builds the temporal tracking matrix from catalog sources.
            """),
            mo.md("#### Internal Processing Steps"),
            stage0_steps_accordion
        ]),

        "🤝 Stage 1: Voluntary Alignment": mo.vstack([
            mo.md(f"""
### Stage 1: Voluntary Alignment

{stage1_doc or "No documentation available"}
            """)
        ]),

        "👥 Stage 2: Demographics": mo.vstack([
            mo.md(f"""
### Stage 2: Demographics

{stage2_doc or "No documentation available"}
            """)
        ]),

        "🏢 Stage 3: Office Matching": mo.vstack([
            mo.md(f"""
### Stage 3: Office Matching

{stage3_doc or "No documentation available"}
            """)
        ]),

        "⚕️ Stage 4: Provider Attribution": mo.vstack([
            mo.md(f"""
### Stage 4: Provider Attribution

{stage4_doc or "No documentation available"}
            """)
        ]),

        "📈 Stage 5: Consolidated Metrics": mo.vstack([
            mo.md(f"""
### Stage 5: Consolidated Metrics

{stage5_doc or "No documentation available"}
            """)
        ]),

        "🏷️ Stage 6: Metadata & Actions": mo.vstack([
            mo.md(f"""
### Stage 6: Metadata and Action Flags

{stage6_doc or "No documentation available"}
            """)
        ]),
    }, multiple=True, lazy=False)

    return mo.vstack([
        mo.md("---"),
        mo.md("## Technical Appendix: Consolidated Alignment Pipeline"),
        mo.md("This section documents the complete data pipeline that produces the consolidated alignment dataset."),
        main_accordion
    ])


@app.function(hide_code=True)
def load_outreach_data(catalog):
    """
    Load outreach campaign data from catalog.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Loads email and mailed letter campaign data for outreach analysis.

    Args:
        catalog: ACOHarmony Catalog instance

    Returns:
        tuple[pl.LazyFrame, pl.LazyFrame]: (emails_df, mailed_df)
    """
    # Load email campaign data
    emails_df = catalog.scan_table("emails")

    # Load mailed letters data
    mailed_df = catalog.scan_table("mailed")

    return emails_df, mailed_df


@app.function(hide_code=True)
def load_consolidated_alignment_data(gold_path, pl):
    """
    Load consolidated alignment data from gold layer.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Loads the consolidated_alignment.parquet file from the gold tier
    using lazy evaluation for memory efficiency.

    INCLUDES ALL BENEFICIARIES (living and deceased) - individual calculations
    can filter by death_date as needed for their specific analysis.

    Args:
        gold_path: Path to gold tier storage
        pl: Polars module

    Returns:
        pl.LazyFrame: Consolidated alignment data (all beneficiaries)
    """
    consolidated_path = gold_path / "consolidated_alignment.parquet"
    df = pl.scan_parquet(str(consolidated_path))

    # Return all data - let downstream calculations filter as needed
    return df


@app.function(hide_code=True)
def build_living_filter(df, pl):
    """
    Build a Polars expression to filter for living beneficiaries.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Checks if death_date and bene_death_date columns exist and builds
    appropriate filter conditions.

    Args:
        df: LazyFrame to check for death columns
        pl: Polars module

    Returns:
        pl.Expr: Filter expression for living beneficiaries (or lit(True) if no death columns)
    """
    schema_names = df.collect_schema().names()
    has_death_date = "death_date" in schema_names
    has_bene_death_date = "bene_death_date" in schema_names

    # Build filter condition
    filter_condition = pl.lit(True)  # Start with no filter
    if has_death_date:
        filter_condition = filter_condition & pl.col("death_date").is_null()
    if has_bene_death_date:
        filter_condition = filter_condition & pl.col("bene_death_date").is_null()

    return filter_condition


@app.function(hide_code=True)
def get_sample_data(enriched_data, sample_size, pl):
    """
    Get a sample of enriched data for display or testing.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Args:
        enriched_data: LazyFrame with enriched alignment data
        sample_size: Number of records to sample
        pl: Polars module

    Returns:
        pl.DataFrame: Sample of enriched data with selected columns
    """
    # Define columns to include in sample
    desired_columns = [
        "current_mbi",
        "consolidated_program",
        "has_voluntary_alignment",
        "months_in_reach",
        "months_in_mssp",
        "office_location",
        "has_valid_voluntary_alignment",
        "has_voluntary_outreach",
        "voluntary_email_count",
        "voluntary_letter_count",
    ]

    # Filter to columns that actually exist
    available_columns = enriched_data.collect_schema().names()
    columns_to_select = [col for col in desired_columns if col in available_columns]

    # Ensure we have at least current_mbi
    if "current_mbi" not in columns_to_select and "current_mbi" in available_columns:
        columns_to_select.insert(0, "current_mbi")

    # Get sample
    if columns_to_select:
        sample = enriched_data.select(columns_to_select).head(sample_size).collect()
    else:
        sample = enriched_data.head(sample_size).collect()

    return sample


@app.function(hide_code=True)
def calculate_basic_stats(df, pl):
    """
    Calculate basic dataset statistics.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Args:
        df: LazyFrame with consolidated alignment data
        pl: Polars module

    Returns:
        dict: Contains total_records and total_columns
    """
    # Use transform function
    from acoharmony._transforms._notebook_utilities import calculate_basic_stats as _calculate_basic_stats
    return _calculate_basic_stats(df)


@app.function(hide_code=True)
def extract_year_months(df):
    """
    Extract available year-months from ym_* columns.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Scans column names for ym_YYYYMM_* pattern and extracts unique
    year-month values, returning them sorted.

    Args:
        df: LazyFrame with consolidated alignment data

    Returns:
        tuple[str | None, list[str]]: (most_recent_ym, year_months)
    """
    # Get all year-month columns
    ym_columns = [col for col in df.collect_schema().names() if col.startswith("ym_")]

    if ym_columns:
        # Extract unique year-months and find the most recent
        year_months = sorted(set(col.split("_")[1] for col in ym_columns))
        most_recent_ym = year_months[-1] if year_months else None
    else:
        most_recent_ym = None
        year_months = []

    return most_recent_ym, year_months


@app.function(hide_code=True)
def calculate_historical_program_distribution(df, pl):
    """
    Calculate HISTORICAL program distribution (ever aligned).

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Calculates how many beneficiaries were EVER in REACH, MSSP, both, or neither
    over the entire observable history.

    Args:
        df: LazyFrame with consolidated alignment data
        pl: Polars module

    Returns:
        pl.DataFrame: Historical alignment counts
    """
    # Use transform function
    from acoharmony._transforms._notebook_utilities import calculate_historical_program_distribution as _calculate_historical_program_distribution
    return _calculate_historical_program_distribution(df)


@app.function(hide_code=True)
def calculate_current_program_distribution(df, most_recent_ym, pl):
    """
    Calculate CURRENT program distribution based on most recent month.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Calculates current alignment status (REACH/MSSP/both/neither) for the
    most recent available month.

    Args:
        df: LazyFrame with consolidated alignment data
        most_recent_ym: Most recent year-month string
        pl: Polars module

    Returns:
        pl.DataFrame: Current alignment counts
    """
    # Use transform function
    from acoharmony._transforms._notebook_utilities import calculate_current_program_distribution as _calculate_current_program_distribution
    return _calculate_current_program_distribution(df, most_recent_ym)


@app.function(hide_code=True)
def prepare_voluntary_outreach_data(emails_df, mailed_df, pl):
    """
    Prepare VOLUNTARY ALIGNMENT outreach data for joining with alignment data.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Filters email and mailed campaigns for "ACO Voluntary Alignment" and creates:
    - Aggregated email/letter counts per MBI
    - Campaign-specific engagement tracking
    - Campaign period extraction (e.g., "2024_Q2")

    Args:
        emails_df: LazyFrame with email campaign data
        mailed_df: LazyFrame with mailed letters data
        pl: Polars module

    Returns:
        tuple: (email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis)
    """
    # Filter for voluntary alignment campaigns only and extract quarter
    voluntary_emails = (
        emails_df.filter(pl.col("campaign").str.contains("ACO Voluntary Alignment"))
        .with_columns(
            [
                # Extract year and quarter from campaign name (e.g., "2024 Q2 ACO Voluntary Alignment")
                pl.col("campaign").str.extract(r"(\d{4})\s+Q(\d)", 1).alias("campaign_year"),
                pl.col("campaign").str.extract(r"(\d{4})\s+Q(\d)", 2).alias("campaign_quarter"),
            ]
        )
        .with_columns(
            (pl.col("campaign_year") + "_Q" + pl.col("campaign_quarter")).alias("campaign_period")
        )
    )

    # Get unique MBIs that have received voluntary alignment emails
    email_mbis = (
        voluntary_emails.select(
            [
                "mbi",
                "send_datetime",
                "campaign",
                "campaign_period",
                "status",
                "has_been_opened",
                "has_been_clicked",
            ]
        )
        .filter(pl.col("mbi").is_not_null())
        .group_by("mbi")
        .agg(
            [
                pl.len().alias("voluntary_email_count"),
                pl.col("campaign").n_unique().alias("voluntary_email_campaigns"),
                pl.col("campaign_period").str.join(", ").alias("email_campaign_periods"),
                (pl.col("has_been_opened") == "true").sum().alias("voluntary_emails_opened"),
                (pl.col("has_been_clicked") == "true").sum().alias("voluntary_emails_clicked"),
                pl.col("send_datetime").max().alias("last_voluntary_email_date"),
            ]
        )
    )

    # Filter for voluntary alignment mailed campaigns
    voluntary_mailed = (
        mailed_df.filter(pl.col("campaign_name").str.contains("ACO Voluntary Alignment"))
        .with_columns(
            [
                # Extract year and quarter from campaign name
                pl.col("campaign_name").str.extract(r"(\d{4})\s+Q(\d)", 1).alias("campaign_year"),
                pl.col("campaign_name")
                .str.extract(r"(\d{4})\s+Q(\d)", 2)
                .alias("campaign_quarter"),
            ]
        )
        .with_columns(
            (pl.col("campaign_year") + "_Q" + pl.col("campaign_quarter")).alias("campaign_period")
        )
    )

    # Get unique MBIs that have received voluntary alignment letters
    mailed_mbis = (
        voluntary_mailed.select(
            ["mbi", "send_datetime", "campaign_name", "campaign_period", "status"]
        )
        .filter(pl.col("mbi").is_not_null())
        .group_by("mbi")
        .agg(
            [
                pl.len().alias("voluntary_letter_count"),
                pl.col("campaign_name").n_unique().alias("voluntary_letter_campaigns"),
                pl.col("campaign_period").str.join(", ").alias("letter_campaign_periods"),
                pl.col("send_datetime").max().alias("last_voluntary_letter_date"),
            ]
        )
    )

    # Also create campaign-specific aggregations for detailed analysis
    email_by_campaign = voluntary_emails.group_by(["campaign_period", "mbi"]).agg(
        [
            pl.len().alias("emails_sent"),
            (pl.col("has_been_opened") == "true").any().alias("opened"),
            (pl.col("has_been_clicked") == "true").any().alias("clicked"),
        ]
    )

    mailed_by_campaign = voluntary_mailed.group_by(["campaign_period", "mbi"]).agg(
        [pl.len().alias("letters_sent"), pl.col("status").first().alias("letter_status")]
    )

    return email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis


@app.function(hide_code=True)
def enrich_with_outreach_data(df, email_mbis, mailed_mbis, pl):
    """
    Enrich alignment data with VOLUNTARY ALIGNMENT outreach information.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Joins outreach data (emails/letters) to beneficiary alignment data and creates
    summary columns for outreach attempts, types, and engagement levels.

    Args:
        df: LazyFrame with consolidated alignment data
        email_mbis: LazyFrame with aggregated email outreach per MBI
        mailed_mbis: LazyFrame with aggregated mailed letter outreach per MBI
        pl: Polars module

    Returns:
        pl.LazyFrame: Enriched alignment data with outreach columns
    """
    # Join email outreach data
    df_with_emails = df.join(email_mbis, left_on="current_mbi", right_on="mbi", how="left")

    # Join mailed letter outreach data
    df_enriched = df_with_emails.join(
        mailed_mbis, left_on="current_mbi", right_on="mbi", how="left"
    )

    # Create outreach summary columns for VOLUNTARY ALIGNMENT campaigns
    df_enriched = df_enriched.with_columns(
        [
            # Total voluntary alignment outreach attempts
            (
                pl.col("voluntary_email_count").fill_null(0)
                + pl.col("voluntary_letter_count").fill_null(0)
            ).alias("voluntary_outreach_attempts"),
            # Has been contacted for voluntary alignment
            ((pl.col("voluntary_email_count") > 0) | (pl.col("voluntary_letter_count") > 0)).alias(
                "has_voluntary_outreach"
            ),
            # Voluntary outreach type
            pl.when((pl.col("voluntary_email_count") > 0) & (pl.col("voluntary_letter_count") > 0))
            .then(pl.lit("Email & Letter"))
            .when(pl.col("voluntary_email_count") > 0)
            .then(pl.lit("Email Only"))
            .when(pl.col("voluntary_letter_count") > 0)
            .then(pl.lit("Letter Only"))
            .otherwise(pl.lit("No Voluntary Outreach"))
            .alias("voluntary_outreach_type"),
            # Voluntary alignment engagement level
            pl.when(pl.col("voluntary_emails_clicked") > 0)
            .then(pl.lit("Clicked"))
            .when(pl.col("voluntary_emails_opened") > 0)
            .then(pl.lit("Opened"))
            .when((pl.col("voluntary_email_count") > 0) | (pl.col("voluntary_letter_count") > 0))
            .then(pl.lit("Contacted"))
            .otherwise(pl.lit("Not Contacted"))
            .alias("voluntary_engagement_level"),
            # Campaign periods contacted (for tracking which quarters)
            pl.when(
                pl.col("email_campaign_periods").is_not_null()
                & pl.col("letter_campaign_periods").is_not_null()
            )
            .then(pl.col("email_campaign_periods") + ", " + pl.col("letter_campaign_periods"))
            .when(pl.col("email_campaign_periods").is_not_null())
            .then(pl.col("email_campaign_periods"))
            .when(pl.col("letter_campaign_periods").is_not_null())
            .then(pl.col("letter_campaign_periods"))
            .otherwise(pl.lit(""))
            .alias("campaign_periods_contacted"),
        ]
    )

    return df_enriched


@app.function(hide_code=True)
def calculate_voluntary_alignment_stats(df_enriched, most_recent_ym, pl):
    """
    Calculate voluntary alignment statistics scoped to current status.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Calculates comprehensive SVA (Standard Voluntary Alignment) metrics including:
    - Current REACH beneficiaries with valid/expired SVA
    - MSSP beneficiaries eligible for SVA
    - Outreach contact statistics
    - Renewal needs analysis

    Args:
        df_enriched: LazyFrame with enriched alignment data (including outreach)
        most_recent_ym: Most recent year-month string (e.g., "202401")
        pl: Polars module

    Returns:
        dict: SVA statistics with 16 metrics (see docstring for details)
    """
    # Use transform function
    from acoharmony._transforms._notebook_voluntary_alignment import calculate_voluntary_alignment_stats as _calculate_voluntary_alignment_stats
    return _calculate_voluntary_alignment_stats(df_enriched, most_recent_ym)


@app.cell(hide_code=True)
def _(build_living_beneficiary_expr):
    def analyze_outreach_effectiveness(df_enriched, most_recent_ym, selected_ym, pl):
        """
        Analyze outreach effectiveness for voluntary alignment campaigns.

        IDEMPOTENT FUNCTION - same inputs always produce same outputs.

        Calculates comprehensive conversion metrics showing the TRUE effectiveness
        of voluntary alignment outreach by tracking beneficiaries from contact
        through SVA signature to REACH enrollment.

        Args:
            df_enriched: LazyFrame with enriched alignment and outreach data
            most_recent_ym: Most recent year-month for REACH context
            selected_ym: Selected year-month for current metrics (can be None)
            pl: Polars module

        Returns:
            dict: Outreach effectiveness metrics including:
                - total_population: Total beneficiaries
                - total_contacted: Total contacted for voluntary alignment
                - contacted_to_sva_rate: % of contacted who signed SVA
                - email_opened_to_sva_rate: % of email openers who signed SVA
                - email_clicked_to_sva_rate: % of email clickers who signed SVA
                - not_contacted_sva_rate: Baseline SVA rate (no outreach)
                - current_metrics: Current month metrics (if selected_ym provided)
        """
        # Calculate conversion metrics for ENTIRE POPULATION
        overall_stats = df_enriched.select(
            [
                pl.len().alias("total_population"),
                pl.col("has_voluntary_outreach").sum().alias("total_contacted"),
                (pl.col("voluntary_email_count") > 0).sum().alias("total_emailed"),
                (pl.col("voluntary_letter_count") > 0).sum().alias("total_mailed"),
                (pl.col("voluntary_emails_opened") > 0).sum().alias("total_email_opened"),
                (pl.col("voluntary_emails_clicked") > 0).sum().alias("total_email_clicked"),
                pl.col("has_valid_voluntary_alignment").sum().alias("total_with_valid_sva"),
            ]
        ).collect()

        # Calculate TRUE conversion rates from VOLUNTARY ALIGNMENT outreach to SVA signature
        contacted_to_sva = (
            df_enriched.filter(pl.col("has_voluntary_outreach"))
            .select(
                [
                    pl.len().alias("contacted_count"),
                    pl.col("has_valid_voluntary_alignment").sum().alias("contacted_with_sva"),
                ]
            )
            .collect()
        )

        email_opened_to_sva = (
            df_enriched.filter(pl.col("voluntary_emails_opened") > 0)
            .select(
                [
                    pl.len().alias("opened_count"),
                    pl.col("has_valid_voluntary_alignment").sum().alias("opened_with_sva"),
                ]
            )
            .collect()
        )

        email_clicked_to_sva = (
            df_enriched.filter(pl.col("voluntary_emails_clicked") > 0)
            .select(
                [
                    pl.len().alias("clicked_count"),
                    pl.col("has_valid_voluntary_alignment").sum().alias("clicked_with_sva"),
                ]
            )
            .collect()
        )

        not_contacted_sva = (
            df_enriched.filter(~pl.col("has_voluntary_outreach"))
            .select(
                [
                    pl.len().alias("not_contacted_count"),
                    pl.col("has_valid_voluntary_alignment").sum().alias("not_contacted_with_sva"),
                ]
            )
            .collect()
        )

        # Check REACH status for context using most recent observation
        schema = df_enriched.collect_schema().names()

        if most_recent_ym:
            reach_col = f"ym_{most_recent_ym}_reach"
            if reach_col in schema:
                contacted_sva_to_reach = (
                    df_enriched.filter(
                        pl.col("has_voluntary_outreach") & pl.col("has_valid_voluntary_alignment")
                    )
                    .select(
                        [
                            pl.len().alias("contacted_sva_count"),
                            pl.col(reach_col).sum().alias("contacted_sva_in_reach"),
                        ]
                    )
                    .collect()
                )
            else:
                contacted_sva_to_reach = pl.DataFrame({
                    "contacted_sva_count": [0],
                    "contacted_sva_in_reach": [0]
                })
        else:
            contacted_sva_to_reach = pl.DataFrame({
                "contacted_sva_count": [0],
                "contacted_sva_in_reach": [0]
            })

        outreach_metrics = {
            "total_population": overall_stats["total_population"][0],
            "total_contacted": overall_stats["total_contacted"][0],
            "total_emailed": overall_stats["total_emailed"][0],
            "total_mailed": overall_stats["total_mailed"][0],
            "total_with_valid_sva": overall_stats["total_with_valid_sva"][0],
            # TRUE conversion rates
            "contacted_to_sva_rate": (
                contacted_to_sva["contacted_with_sva"][0] / contacted_to_sva["contacted_count"][0] * 100
            )
            if contacted_to_sva["contacted_count"][0] > 0
            else 0,
            "email_opened_to_sva_rate": (
                email_opened_to_sva["opened_with_sva"][0] / email_opened_to_sva["opened_count"][0] * 100
            )
            if email_opened_to_sva["opened_count"][0] > 0
            else 0,
            "email_clicked_to_sva_rate": (
                email_clicked_to_sva["clicked_with_sva"][0]
                / email_clicked_to_sva["clicked_count"][0]
                * 100
            )
            if email_clicked_to_sva["clicked_count"][0] > 0
            else 0,
            "not_contacted_sva_rate": (
                not_contacted_sva["not_contacted_with_sva"][0]
                / not_contacted_sva["not_contacted_count"][0]
                * 100
            )
            if not_contacted_sva["not_contacted_count"][0] > 0
            else 0,
            # Raw numbers for transparency
            "contacted_with_sva": contacted_to_sva["contacted_with_sva"][0],
            "opened_with_sva": email_opened_to_sva["opened_with_sva"][0],
            "clicked_with_sva": email_clicked_to_sva["clicked_with_sva"][0],
            "not_contacted_with_sva": not_contacted_sva["not_contacted_with_sva"][0],
            # REACH conversion (for context)
            "contacted_sva_in_reach": contacted_sva_to_reach["contacted_sva_in_reach"][0]
            if len(contacted_sva_to_reach) > 0
            else 0,
            # Current metrics placeholder
            "current_metrics": None,
        }

        # Calculate CURRENT metrics if a month is selected
        if selected_ym:
            # Use temporal columns for the selected observation month
            schema = df_enriched.collect_schema().names()
            reach_col = f"ym_{selected_ym}_reach"
            mssp_col = f"ym_{selected_ym}_mssp"

            # Filter for actively aligned beneficiaries (in REACH or MSSP in selected month)
            # Also filter for living beneficiaries only
            living_expr = build_living_beneficiary_expr(schema)

            if reach_col in schema and mssp_col in schema:
                currently_aligned_outreach = df_enriched.filter(
                    (pl.col(reach_col) | pl.col(mssp_col)) & living_expr
                )
            else:
                currently_aligned_outreach = df_enriched.filter(pl.lit(False))  # Empty filter

            current_outreach_stats = currently_aligned_outreach.select(
                [
                    pl.len().alias("current_population"),
                    pl.col("has_voluntary_outreach").sum().alias("current_contacted"),
                    (pl.col("voluntary_email_count") > 0).sum().alias("current_emailed"),
                    (pl.col("voluntary_letter_count") > 0).sum().alias("current_mailed"),
                    pl.col("has_valid_voluntary_alignment").sum().alias("current_with_sva"),
                ]
            ).collect()

            curr_contacted_to_sva = (
                currently_aligned_outreach.filter(pl.col("has_voluntary_outreach"))
                .select(
                    [
                        pl.len().alias("contacted_count"),
                        pl.col("has_valid_voluntary_alignment").sum().alias("contacted_with_sva"),
                    ]
                )
                .collect()
            )

            curr_email_opened = (
                currently_aligned_outreach.filter(pl.col("voluntary_emails_opened") > 0)
                .select(
                    [
                        pl.len().alias("opened_count"),
                        pl.col("has_valid_voluntary_alignment").sum().alias("opened_with_sva"),
                    ]
                )
                .collect()
            )

            curr_email_clicked = (
                currently_aligned_outreach.filter(pl.col("voluntary_emails_clicked") > 0)
                .select(
                    [
                        pl.len().alias("clicked_count"),
                        pl.col("has_valid_voluntary_alignment").sum().alias("clicked_with_sva"),
                    ]
                )
                .collect()
            )

            curr_not_contacted = (
                currently_aligned_outreach.filter(~pl.col("has_voluntary_outreach"))
                .select(
                    [
                        pl.len().alias("not_contacted_count"),
                        pl.col("has_valid_voluntary_alignment").sum().alias("not_contacted_with_sva"),
                    ]
                )
                .collect()
            )

            outreach_metrics["current_metrics"] = {
                "total": current_outreach_stats["current_population"][0],
                "contacted": current_outreach_stats["current_contacted"][0],
                "emailed": current_outreach_stats["current_emailed"][0],
                "mailed": current_outreach_stats["current_mailed"][0],
                "with_sva": current_outreach_stats["current_with_sva"][0],
                "contacted_to_sva_rate": (
                    curr_contacted_to_sva["contacted_with_sva"][0]
                    / curr_contacted_to_sva["contacted_count"][0]
                    * 100
                )
                if curr_contacted_to_sva["contacted_count"][0] > 0
                else 0,
                "email_opened_to_sva_rate": (
                    curr_email_opened["opened_with_sva"][0] / curr_email_opened["opened_count"][0] * 100
                )
                if curr_email_opened["opened_count"][0] > 0
                else 0,
                "email_clicked_to_sva_rate": (
                    curr_email_clicked["clicked_with_sva"][0]
                    / curr_email_clicked["clicked_count"][0]
                    * 100
                )
                if curr_email_clicked["clicked_count"][0] > 0
                else 0,
                "not_contacted_sva_rate": (
                    curr_not_contacted["not_contacted_with_sva"][0]
                    / curr_not_contacted["not_contacted_count"][0]
                    * 100
                )
                if curr_not_contacted["not_contacted_count"][0] > 0
                else 0,
                "contacted_with_sva": curr_contacted_to_sva["contacted_with_sva"][0]
                if len(curr_contacted_to_sva) > 0
                else 0,
                "opened_with_sva": curr_email_opened["opened_with_sva"][0]
                if len(curr_email_opened) > 0
                else 0,
                "clicked_with_sva": curr_email_clicked["clicked_with_sva"][0]
                if len(curr_email_clicked) > 0
                else 0,
                "not_contacted_with_sva": curr_not_contacted["not_contacted_with_sva"][0]
                if len(curr_not_contacted) > 0
                else 0,
            }

        return outreach_metrics

    return (analyze_outreach_effectiveness,)


@app.function(hide_code=True)
def calculate_quarterly_campaign_effectiveness(df_enriched, email_by_campaign, mailed_by_campaign, pl):
    """
    Calculate quarterly campaign effectiveness analysis.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Analyzes voluntary alignment campaign performance by quarter, including:
    - Outreach volume (emails, letters, both)
    - Engagement metrics (opens, clicks)
    - Conversion rates to valid SVA signature
    - Channel-specific effectiveness

    Args:
        df_enriched: LazyFrame with enriched alignment data
        email_by_campaign: LazyFrame with email campaigns by period and MBI
        mailed_by_campaign: LazyFrame with mailed campaigns by period and MBI
        pl: Polars module

    Returns:
        pl.DataFrame: Campaign metrics by quarter with conversion rates
    """
    # Use transform function
    from acoharmony._transforms._notebook_outreach import calculate_quarterly_campaign_effectiveness as _calculate_quarterly_campaign_effectiveness
    return _calculate_quarterly_campaign_effectiveness(df_enriched, email_by_campaign, mailed_by_campaign)


@app.function(hide_code=True)
def calculate_office_campaign_effectiveness(df_enriched, email_by_campaign, mailed_by_campaign, pl):
    """
    Calculate campaign effectiveness by office_name.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Analyzes voluntary alignment campaign performance by office, including:
    - Outreach volume per office (emails, letters)
    - Engagement metrics by office
    - Conversion rates to valid SVA by office
    - Office-specific campaign performance

    Args:
        df_enriched: LazyFrame with enriched alignment data (must include office_name, office_location)
        email_by_campaign: LazyFrame with email campaigns by period and MBI
        mailed_by_campaign: LazyFrame with mailed campaigns by period and MBI
        pl: Polars module

    Returns:
        pl.DataFrame: Campaign metrics by office_name with conversion rates
    """
    # Use transform function
    from acoharmony._transforms._notebook_outreach import calculate_office_campaign_effectiveness as _calculate_office_campaign_effectiveness
    return _calculate_office_campaign_effectiveness(df_enriched, email_by_campaign, mailed_by_campaign)


@app.function(hide_code=True)
def calculate_enhanced_campaign_performance(emails_df, mailed_df, pl):
    """
    Calculate comprehensive campaign performance metrics for voluntary alignment outreach.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Calculates detailed performance metrics for both email and mail campaigns, including:
    - Delivery rates (excluding bounced, dropped, failed)
    - Email engagement rates (opens, clicks)
    - Unique recipient counts
    - Cost estimates

    Args:
        emails_df: LazyFrame with email campaign data
        mailed_df: LazyFrame with mail campaign data
        pl: Polars module

    Returns:
        dict: Campaign performance metrics with email and mail statistics
    """
    # Use transform function
    from acoharmony._transforms._notebook_outreach import calculate_enhanced_campaign_performance as _calculate_enhanced_campaign_performance
    return _calculate_enhanced_campaign_performance(emails_df, mailed_df)


@app.function(hide_code=True)
def analyze_sva_action_categories(df_enriched, pl):
    """
    Analyze SVA action needed categories across beneficiary population.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Groups beneficiaries by their `sva_action_needed` status and counts each category.
    This helps identify how many beneficiaries need SVA renewal, new signatures, etc.

    Args:
        df_enriched: LazyFrame with enriched alignment data (must have sva_action_needed column)
        pl: Polars module

    Returns:
        DataFrame: SVA action categories with counts, sorted by frequency descending
    """
    action_stats = (
        df_enriched.group_by("sva_action_needed")
        .agg(pl.len().alias("count"))
        .collect()
        .sort("count", descending=True)
    )
    return action_stats


@app.function(hide_code=True)
def calculate_enrollment_stats_for_selected_month(df, selected_ym, pl):
    """
    Calculate point-in-time enrollment statistics for a specific month.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Calculates enrollment counts for a selected month across:
    - REACH enrollment
    - MSSP enrollment
    - FFS (not in value-based programs)
    - Not enrolled (inactive beneficiaries)

    Args:
        df: LazyFrame with consolidated alignment data
        selected_ym: Year-month string (e.g., "202401")
        pl: Polars module

    Returns:
        dict: Enrollment statistics or None if selected_ym is None
    """
    if not selected_ym:
        return None

    # Get columns for selected month
    reach_col = f"ym_{selected_ym}_reach"
    mssp_col = f"ym_{selected_ym}_mssp"
    ffs_col = f"ym_{selected_ym}_ffs"

    # Filter for actively aligned beneficiaries (alive only)
    df_active = df.filter(
        build_living_filter(df, pl)
    )

    # Check if columns exist and get counts
    schema = df_active.collect_schema().names()

    stats = {}
    if reach_col in schema:
        stats["REACH"] = df_active.filter(pl.col(reach_col)).select(pl.len()).collect().item()
    else:
        stats["REACH"] = 0

    if mssp_col in schema:
        stats["MSSP"] = df_active.filter(pl.col(mssp_col)).select(pl.len()).collect().item()
    else:
        stats["MSSP"] = 0

    if ffs_col in schema:
        stats["FFS"] = df_active.filter(pl.col(ffs_col)).select(pl.len()).collect().item()
    else:
        stats["FFS"] = 0

    # Calculate not enrolled (total minus all programs)
    total = df.select(pl.len()).collect().item()
    stats["Not Enrolled"] = total - (stats["REACH"] + stats["MSSP"] + stats["FFS"])

    return stats


@app.function(hide_code=True)
def calculate_alignment_trends_over_time(df, year_months, pl):
    """
    Calculate alignment trends over time.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Tracks REACH and MSSP enrollment counts across all observable months
    to show enrollment trends over time.

    Args:
        df: LazyFrame with consolidated alignment data
        year_months: List of year-month strings to analyze
        pl: Polars module

    Returns:
        pl.DataFrame: Trends data with columns (year_month, REACH, MSSP, Total Aligned)
                      or None if no year_months available
    """
    # Use transform function
    from acoharmony._transforms._notebook_trends import calculate_alignment_trends_over_time as _calculate_alignment_trends_over_time
    return _calculate_alignment_trends_over_time(df, year_months)


@app.function(hide_code=True)
def calculate_office_enrollment_stats(df: pl.LazyFrame, selected_ym: str) -> pl.DataFrame | None:
    """
    Calculate office enrollment statistics by office_name for selected year-month.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    For each office (office_name), calculate:
    - Total beneficiaries assigned to that office
    - REACH enrollment count for the selected month
    - MSSP enrollment count for the selected month
    - Total ACO enrollment (REACH + MSSP)
    - FFS (never aligned) count
    - Valid SVA count
    - Penetration rates for each category

    Args:
        df: LazyFrame with consolidated alignment data
        selected_ym: Year-month string (e.g., "202401")

    Returns:
        DataFrame with office stats, or None if yearmo not available
    """
    # Use transform function
    from acoharmony._transforms._notebook_office_stats import calculate_office_enrollment_stats as _calculate_office_enrollment_stats
    return _calculate_office_enrollment_stats(df, selected_ym)


@app.function(hide_code=True)
def calculate_office_alignment_types(df: pl.LazyFrame, selected_ym: str) -> pl.DataFrame | None:
    """
    Calculate alignment type breakdown by office location.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    For each office location, breakdown enrolled beneficiaries by:
    - Voluntary alignment (SVA/PBVAR)
    - Claims-based alignment only
    - No valid alignment

    Args:
        df: LazyFrame with consolidated alignment data
        selected_ym: Year-month string (e.g., "202401")

    Returns:
        DataFrame with alignment type stats by office, or None if yearmo not available
    """
    # Use transform function
    from acoharmony._transforms._notebook_office_stats import calculate_office_alignment_types as _calculate_office_alignment_types
    return _calculate_office_alignment_types(df, selected_ym)


@app.function(hide_code=True)
def calculate_office_program_distribution(df: pl.LazyFrame, selected_ym: str) -> pl.DataFrame | None:
    """
    Calculate program distribution by office location.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    For each office location, count beneficiaries in:
    - REACH only
    - MSSP only
    - Both REACH and MSSP (historically)
    - Neither program (FFS)

    Args:
        df: LazyFrame with consolidated alignment data
        selected_ym: Year-month string (e.g., "202401")

    Returns:
        DataFrame with program distribution by office, or None if yearmo not available
    """
    # Use transform function
    from acoharmony._transforms._notebook_office_stats import calculate_office_program_distribution as _calculate_office_program_distribution
    return _calculate_office_program_distribution(df, selected_ym)


@app.function(hide_code=True)
def calculate_office_transition_stats(df: pl.LazyFrame) -> pl.DataFrame:
    """
    Calculate program transition statistics by office name and market.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    For each office (physical location) and market, calculate:
    - Count of beneficiaries who transitioned between programs
    - Count with continuous enrollment (no gaps)
    - Average months in REACH
    - Average months in MSSP

    Args:
        df: LazyFrame with consolidated alignment data

    Returns:
        DataFrame with transition stats by office_name and office_location (market)
    """
    # Use transform function
    from acoharmony._transforms._notebook_office_stats import calculate_office_transition_stats as _calculate_office_transition_stats
    return _calculate_office_transition_stats(df)


@app.function(hide_code=True)
def calculate_office_metadata(df: pl.LazyFrame) -> pl.DataFrame:
    """
    Calculate office location metadata showing office_name and market mapping.

    IDEMPOTENT FUNCTION - same inputs always produce same outputs.

    Returns a table showing:
    - office_location (market/service area)
    - office_name (physical office location)
    - Total beneficiaries assigned
    - Unique ZIP codes served

    Args:
        df: LazyFrame with consolidated alignment data

    Returns:
        DataFrame with office metadata
    """
    # Use transform function
    from acoharmony._transforms._notebook_office_stats import calculate_office_metadata as _calculate_office_metadata
    return _calculate_office_metadata(df)


@app.cell(hide_code=True)
def _(df_enriched, selected_ym, year_months):
    """Calculate alignment transitions using the idempotent function"""
    transition_stats, prev_ym, curr_ym = calculate_alignment_transitions(
        df_enriched, selected_ym, year_months, pl
    )

    # Return default values if calculation returns None
    if transition_stats is None:
        transition_stats = pl.DataFrame({"transition_type": ["No Data"], "count": [0]})
        prev_ym = None
        curr_ym = None
    return curr_ym, prev_ym, transition_stats


@app.cell(hide_code=True)
def _(df_enriched, selected_ym):
    """Calculate alignment sources using the idempotent function"""
    current_source_stats, historical_source_stats = calculate_current_and_historical_sources(
        df_enriched, selected_ym, pl
    )
    return current_source_stats, historical_source_stats


@app.cell(hide_code=True)
def _(df, df_enriched, selected_ym):
    """Analyze enrollment patterns using the idempotent function"""
    historical_enrollment, current_enrollment = analyze_enrollment_patterns(
        df, df_enriched, selected_ym, pl
    )
    return


@app.cell(hide_code=True)
def _(df_enriched, selected_ym, year_months):
    """Calculate month-over-month comparison using the idempotent function"""
    comparison_data = calculate_month_over_month_comparison(
        df_enriched, selected_ym, year_months, pl
    )
    return (comparison_data,)


@app.cell(hide_code=True)
def _(df_enriched, year_months):
    """Calculate cohort analysis using the idempotent function"""
    cohort_analysis = calculate_cohort_analysis(df_enriched, year_months, pl)
    return (cohort_analysis,)


@app.cell(hide_code=True)
def _(comparison_data):
    """Display month-over-month comparison"""
    if comparison_data:
        _reach_change = comparison_data["reach_change"]
        _mssp_change = comparison_data["mssp_change"]
        _total_change = comparison_data["total_aco_change"]
        _reach_pct = comparison_data["reach_pct_change"]
        _mssp_pct = comparison_data["mssp_pct_change"]
        _voluntary_change = comparison_data["voluntary_change"]
        _claims_change = comparison_data["claims_change"]

        # Color coding for changes
        _reach_color = "#059669" if _reach_change >= 0 else "#DC2626"
        _mssp_color = "#D97706" if _mssp_change >= 0 else "#DC2626"
        _total_color = "#4C1D95" if _total_change >= 0 else "#DC2626"

        mo.md(
            f"""
            ## Month-over-Month Performance
            ### Comparing {comparison_data["prev_month"]} → {comparison_data["curr_month"]}

            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #F0F9FF; border-radius: 8px;">
                    <h3 style="margin: 0; color: #0284C7; font-size: 0.875rem;">REACH Change</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: {_reach_color};">
                        {_reach_change:+,}
                    </p>
                    <p style="margin: 0; color: #0284C7; font-size: 0.75rem;">{_reach_pct:+.1f}% change</p>
                </div>
                <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                    <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">MSSP Change</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: {_mssp_color};">
                        {_mssp_change:+,}
                    </p>
                    <p style="margin: 0; color: #92400E; font-size: 0.75rem;">{_mssp_pct:+.1f}% change</p>
                </div>
                <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px;">
                    <h3 style="margin: 0; color: #4C1D95; font-size: 0.875rem;">Total ACO Change</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: {_total_color};">
                        {_total_change:+,}
                    </p>
                    <p style="margin: 0; color: #4C1D95; font-size: 0.75rem;">Net enrollment change</p>
                </div>
            </div>

            ### Alignment Source Changes
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #DCFCE7; border-radius: 8px;">
                    <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Voluntary Alignment</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">
                        {_voluntary_change:+,}
                    </p>
                    <p style="margin: 0; color: #059669; font-size: 0.75rem;">Change in SVA/PBVAR</p>
                </div>
                <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px;">
                    <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Claims-Based</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #1E40AF;">
                        {_claims_change:+,}
                    </p>
                    <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">Change in claims-based</p>
                </div>
            </div>
            """
        )
    else:
        mo.md("**No month-over-month comparison data available**")
    return


@app.cell(hide_code=True)
def _(cohort_analysis):
    """Display cohort retention analysis"""
    if cohort_analysis and len(cohort_analysis) > 0:
        # Format cohort data for display
        _header = mo.md(
            """
            ## Cohort Retention Analysis

            <div style="padding: 0.5rem 1rem; background: #E0F2FE; border-left: 4px solid #0284C7; margin: 1rem 0;">
                <p style="margin: 0; color: #075985; font-size: 0.9rem;">
                    📊 Tracking how well we retain beneficiaries over time
                </p>
            </div>
            """
        )

        # Build retention table
        _table_data = []
        for cohort in cohort_analysis:
            _row = {
                "Cohort": cohort["cohort"],
                "Initial Size": f"{cohort['initial_size']:,}",
                "Month 0": f"{cohort['month_0']:,}",
            }

            # Add retention percentages for available months
            for i in range(1, 4):
                if f"month_{i}" in cohort:
                    _row[f"Month {i}"] = (
                        f"{cohort[f'month_{i}']:,} ({cohort[f'retention_{i}']:.1f}%)"
                    )

            _table_data.append(_row)

        _table = mo.ui.table(_table_data, page_size=50)

        # Create summary
        if len(cohort_analysis) >= 3:
            avg_retention_1 = sum(c.get("retention_1", 0) for c in cohort_analysis) / len(
                cohort_analysis
            )
            avg_retention_2 = sum(
                c.get("retention_2", 0) for c in cohort_analysis if "retention_2" in c
            ) / max(1, len([c for c in cohort_analysis if "retention_2" in c]))

            _summary = mo.md(
                f"""
                ### Average Retention Rates
                - Month 1: {avg_retention_1:.1f}%
                - Month 2: {avg_retention_2:.1f}%
                """
            )

            mo.vstack([_header, _table, _summary])
        else:
            mo.vstack([_header, _table])
    else:
        mo.md("**Insufficient data for cohort analysis**")
    return


@app.cell(hide_code=True)
def _(catalog):
    """Load outreach data - emails and mailed letters"""
    emails_df, mailed_df = load_outreach_data(catalog)
    return emails_df, mailed_df


@app.cell(hide_code=True)
def _(gold_path):
    """Load the consolidated alignment data from gold layer"""
    df = load_consolidated_alignment_data(gold_path, pl)
    return (df,)


@app.cell(hide_code=True)
def _(df):
    """Calculate basic statistics using idempotent function"""
    basic_stats = calculate_basic_stats(df, pl)
    total_records = basic_stats["total_records"]
    total_columns = basic_stats["total_columns"]
    return total_columns, total_records


@app.cell(hide_code=True)
def _(df):
    """Calculate HISTORICAL program distribution using idempotent function"""
    historical_stats = calculate_historical_program_distribution(df, pl)
    return (historical_stats,)


@app.cell(hide_code=True)
def _(df):
    """Extract year-months using idempotent function"""
    most_recent_ym, year_months = extract_year_months(df)
    return most_recent_ym, year_months


@app.cell(hide_code=True)
def _(df, most_recent_ym):
    """Calculate CURRENT program distribution using idempotent function"""
    current_alignment_stats = calculate_current_program_distribution(df, most_recent_ym, pl)
    return (current_alignment_stats,)


@app.cell(hide_code=True)
def _(emails_df, mailed_df):
    """Prepare VOLUNTARY ALIGNMENT outreach data using idempotent function"""
    email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis = prepare_voluntary_outreach_data(
        emails_df, mailed_df, pl
    )
    return email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis


@app.cell(hide_code=True)
def _(df, email_mbis, mailed_mbis):
    """Enrich alignment data with outreach information using idempotent function"""
    df_enriched = enrich_with_outreach_data(df, email_mbis, mailed_mbis, pl)
    return (df_enriched,)


@app.cell(hide_code=True)
def _(df_enriched, most_recent_ym):
    """Calculate voluntary alignment statistics using idempotent function"""
    sva_stats = calculate_voluntary_alignment_stats(df_enriched, most_recent_ym, pl)
    return (sva_stats,)


@app.cell(hide_code=True)
def _(df_enriched):
    """Analyze SVA action needed categories using idempotent function"""
    action_stats = analyze_sva_action_categories(df_enriched, pl)
    return (action_stats,)


@app.cell(hide_code=True)
def _(
    analyze_outreach_effectiveness,
    df_enriched,
    most_recent_ym,
    selected_ym,
):
    """Analyze outreach effectiveness using idempotent function"""
    outreach_metrics = analyze_outreach_effectiveness(df_enriched, most_recent_ym, selected_ym, pl)
    return (outreach_metrics,)


@app.cell(hide_code=True)
def _(df, selected_ym):
    """Calculate office enrollment stats for selected month"""
    office_stats = calculate_office_enrollment_stats(df, selected_ym)
    return (office_stats,)


@app.cell(hide_code=True)
def _(df, selected_ym):
    """Calculate office alignment type breakdown for selected month"""
    office_alignment_types = calculate_office_alignment_types(df, selected_ym)
    return (office_alignment_types,)


@app.cell(hide_code=True)
def _(df, selected_ym):
    """Calculate office program distribution for selected month"""
    office_program_dist = calculate_office_program_distribution(df, selected_ym)
    return (office_program_dist,)


@app.cell(hide_code=True)
def _(df):
    """Calculate office transition stats (historical)"""
    office_transitions = calculate_office_transition_stats(df)
    return (office_transitions,)


@app.cell(hide_code=True)
def _(df):
    """Calculate office metadata with office_name and market mapping"""
    office_metadata = calculate_office_metadata(df)
    return (office_metadata,)


@app.cell(hide_code=True)
def _(office_stats, selected_ym):
    """Display office location enrollment statistics"""
    if office_stats is None or len(office_stats) == 0:
        mo.md("**No office location data available for selected period**")
    else:
        # Calculate totals
        total_benes = office_stats["total_beneficiaries"].sum()
        total_reach = office_stats["reach_count"].sum()
        total_mssp = office_stats["mssp_count"].sum()
        total_aco = office_stats["total_aco"].sum()
        total_sva = office_stats["valid_sva_count"].sum()

        office_enrollment_display_month = f"{selected_ym[:4]}-{selected_ym[4:]}"

        # Create summary header
        office_enrollment_header = mo.md(f"""
        ## 🏢 Office Location Enrollment Analysis

        ### Month: {office_enrollment_display_month}

        <div style="display: grid; grid-template-columns: repeat(5, 1fr); gap: 1rem; margin: 1.5rem 0;">
            <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Total Beneficiaries</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #111827;">{total_benes:,}</p>
            </div>
            <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">REACH Enrolled</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #1E3A8A;">{total_reach:,}</p>
                <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">{total_reach/total_benes*100 if total_benes > 0 else 0:.1f}%</p>
            </div>
            <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #5B21B6; font-size: 0.875rem;">MSSP Enrolled</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #6B21A8;">{total_mssp:,}</p>
                <p style="margin: 0; color: #5B21B6; font-size: 0.75rem;">{total_mssp/total_benes*100 if total_benes > 0 else 0:.1f}%</p>
            </div>
            <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Total ACO</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #047857;">{total_aco:,}</p>
                <p style="margin: 0; color: #059669; font-size: 0.75rem;">{total_aco/total_benes*100 if total_benes > 0 else 0:.1f}%</p>
            </div>
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">Valid SVA</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #78350F;">{total_sva:,}</p>
                <p style="margin: 0; color: #92400E; font-size: 0.75rem;">{total_sva/total_benes*100 if total_benes > 0 else 0:.1f}%</p>
            </div>
        </div>

        ### Enrollment by Office Location
        <div style="padding: 0.75rem 1rem; background: #EFF6FF; border-left: 4px solid #3B82F6; margin: 1rem 0;">
            <p style="margin: 0; color: #1E40AF; font-size: 0.875rem;">
                <i class="fa-solid fa-building"></i> <strong>{len(office_stats)} office locations</strong> with enrollment data
            </p>
        </div>
        """)

        # Format the table data for display
        office_enrollment_table_data = []
        for office_row in office_stats.iter_rows(named=True):
            office_enrollment_table_data.append({
                "Office Name": office_row["office_name"] if office_row["office_name"] else "Unknown",
                "Market": office_row["office_location"] if office_row["office_location"] else "Unknown",
                "Total Benes": f"{office_row['total_beneficiaries']:,}",
                "REACH": f"{office_row['reach_count']:,} ({office_row['reach_penetration']:.1f}%)",
                "MSSP": f"{office_row['mssp_count']:,} ({office_row['mssp_penetration']:.1f}%)",
                "Total ACO": f"{office_row['total_aco']:,} ({office_row['aco_penetration']:.1f}%)",
                "FFS": f"{office_row['ffs_count']:,}",
                "Valid SVA": f"{office_row['valid_sva_count']:,} ({office_row['sva_penetration']:.1f}%)",
            })
    return office_enrollment_header, office_enrollment_table_data


@app.cell(hide_code=True)
def _(office_alignment_types, selected_ym):
    """Display office location alignment type breakdown"""
    if office_alignment_types is None or len(office_alignment_types) == 0:
        mo.md("**No alignment type data available**")
    else:
        alignment_types_display_month = f"{selected_ym[:4]}-{selected_ym[4:]}"

        # Calculate totals
        alignment_types_total_enrolled = office_alignment_types["total_aligned"].sum()
        alignment_types_total_voluntary = office_alignment_types["voluntary_count"].sum()
        alignment_types_total_claims = office_alignment_types["claims_count"].sum()

        alignment_types_header = mo.md(f"""
        ## 📋 Alignment Type Breakdown by Office

        ### Month: {alignment_types_display_month}

        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1.5rem 0;">
            <div style="padding: 1rem; background: #F0FDF4; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #15803D; font-size: 0.875rem;">Voluntary Aligned</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #166534;">{alignment_types_total_voluntary:,}</p>
                <p style="margin: 0; color: #15803D; font-size: 0.75rem;">{alignment_types_total_voluntary/alignment_types_total_enrolled*100 if alignment_types_total_enrolled > 0 else 0:.1f}% of enrolled</p>
            </div>
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">Claims-Based Only</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #78350F;">{alignment_types_total_claims:,}</p>
                <p style="margin: 0; color: #92400E; font-size: 0.75rem;">{alignment_types_total_claims/alignment_types_total_enrolled*100 if alignment_types_total_enrolled > 0 else 0:.1f}% of enrolled</p>
            </div>
            <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #3730A3; font-size: 0.875rem;">Total Enrolled</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #312E81;">{alignment_types_total_enrolled:,}</p>
            </div>
        </div>
        """)

        # Format table
        alignment_types_table_data = []
        for alignment_type_row in office_alignment_types.iter_rows(named=True):
            alignment_types_table_data.append({
                "Office Name": alignment_type_row["office_name"] if alignment_type_row["office_name"] else "Unknown",
                "Market": alignment_type_row["office_location"] if alignment_type_row["office_location"] else "Unknown",
                "Total Aligned": f"{alignment_type_row['total_aligned']:,}",
                "Voluntary": f"{alignment_type_row['voluntary_count']:,} ({alignment_type_row['voluntary_pct']:.1f}%)",
                "Claims Only": f"{alignment_type_row['claims_count']:,} ({alignment_type_row['claims_pct']:.1f}%)",
            })
    return alignment_types_header, alignment_types_table_data


@app.cell(hide_code=True)
def _(office_program_dist):
    """Display office location program distribution"""
    if office_program_dist is None or len(office_program_dist) == 0:
        mo.md("**No program distribution data available**")
    else:
        # Calculate totals
        program_dist_total_reach_only = office_program_dist["reach_only_count"].sum()
        program_dist_total_mssp_only = office_program_dist["mssp_only_count"].sum()
        program_dist_total_both = office_program_dist["both_programs_count"].sum()
        program_dist_total_never = office_program_dist["neither_count"].sum()
        program_dist_total_all = program_dist_total_reach_only + program_dist_total_mssp_only + program_dist_total_both + program_dist_total_never

        program_dist_header = mo.md(f"""
        ## 🎯 Program Distribution by Office

        ### Historical Program Participation

        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin: 1.5rem 0;">
            <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">REACH Only</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #1E3A8A;">{program_dist_total_reach_only:,}</p>
                <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">{program_dist_total_reach_only/program_dist_total_all*100 if program_dist_total_all > 0 else 0:.1f}%</p>
            </div>
            <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #5B21B6; font-size: 0.875rem;">MSSP Only</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #6B21A8;">{program_dist_total_mssp_only:,}</p>
                <p style="margin: 0; color: #5B21B6; font-size: 0.75rem;">{program_dist_total_mssp_only/program_dist_total_all*100 if program_dist_total_all > 0 else 0:.1f}%</p>
            </div>
            <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Both Programs</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #047857;">{program_dist_total_both:,}</p>
                <p style="margin: 0; color: #059669; font-size: 0.75rem;">{program_dist_total_both/program_dist_total_all*100 if program_dist_total_all > 0 else 0:.1f}%</p>
            </div>
            <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Never Aligned</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #111827;">{program_dist_total_never:,}</p>
                <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">{program_dist_total_never/program_dist_total_all*100 if program_dist_total_all > 0 else 0:.1f}%</p>
            </div>
        </div>
        """)

        # Format table
        program_dist_table_data = []
        for program_dist_row in office_program_dist.iter_rows(named=True):
            total = program_dist_row['total_beneficiaries']
            reach_only = program_dist_row['reach_only_count']
            mssp_only = program_dist_row['mssp_only_count']
            both = program_dist_row['both_programs_count']
            neither = program_dist_row['neither_count']
            program_dist_table_data.append({
                "Office Name": program_dist_row["office_name"] if program_dist_row["office_name"] else "Unknown",
                "Market": program_dist_row["office_location"] if program_dist_row["office_location"] else "Unknown",
                "Total": f"{total:,}",
                "REACH Only": f"{reach_only:,} ({reach_only/total*100 if total > 0 else 0:.1f}%)",
                "MSSP Only": f"{mssp_only:,} ({mssp_only/total*100 if total > 0 else 0:.1f}%)",
                "Both": f"{both:,} ({both/total*100 if total > 0 else 0:.1f}%)",
                "Never": f"{neither:,} ({neither/total*100 if total > 0 else 0:.1f}%)",
            })
    return program_dist_header, program_dist_table_data


@app.cell(hide_code=True)
def _(office_transitions):
    """Display office location transition statistics"""
    if office_transitions is None or len(office_transitions) == 0:
        mo.md("**No transition data available**")
    else:
        # Calculate totals
        transitions_total_beneficiaries = office_transitions["total_beneficiaries"].sum()
        transitions_total_transitioned = office_transitions["transitioned_count"].sum()
        transitions_total_continuous = office_transitions["continuous_count"].sum()

        transitions_header = mo.md(f"""
        ## 🔄 Program Transition Statistics by Office

        ### Overall Metrics

        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1.5rem 0;">
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">Program Transitions</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #78350F;">{transitions_total_transitioned:,}</p>
                <p style="margin: 0; color: #92400E; font-size: 0.75rem;">{transitions_total_transitioned/transitions_total_beneficiaries*100 if transitions_total_beneficiaries > 0 else 0:.1f}% switched programs</p>
            </div>
            <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Continuous Enrollment</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #047857;">{transitions_total_continuous:,}</p>
                <p style="margin: 0; color: #059669; font-size: 0.75rem;">{transitions_total_continuous/transitions_total_beneficiaries*100 if transitions_total_beneficiaries > 0 else 0:.1f}% no gaps</p>
            </div>
            <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #3730A3; font-size: 0.875rem;">Total Beneficiaries</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #312E81;">{transitions_total_beneficiaries:,}</p>
            </div>
        </div>
        """)

        # Format table
        transitions_table_data = []
        for transition_row in office_transitions.iter_rows(named=True):
            transitions_table_data.append({
                "Office Name": transition_row["office_name"] if transition_row["office_name"] else "Unknown",
                "Market": transition_row["office_location"] if transition_row["office_location"] else "Unknown",
                "Total": f"{transition_row['total_beneficiaries']:,}",
                "Transitioned": f"{transition_row['transitioned_count']:,} ({transition_row['transition_pct']:.1f}%)",
                "Continuous": f"{transition_row['continuous_count']:,} ({transition_row['continuous_pct']:.1f}%)",
                "Avg REACH Months": f"{transition_row['avg_months_reach']:.1f}",
                "Avg MSSP Months": f"{transition_row['avg_months_mssp']:.1f}",
                "Avg Total Months": f"{transition_row['avg_total_months']:.1f}",
            })
    return transitions_header, transitions_table_data


@app.cell(hide_code=True)
def _(office_metadata):
    """Display office metadata with office_name and market mapping"""
    if office_metadata is None or len(office_metadata) == 0:
        mo.md("**No office metadata available**")
    else:
        # Calculate totals
        _total_offices = len(office_metadata)
        _total_benes_metadata = office_metadata["total_beneficiaries"].sum()
        _total_zips = office_metadata["unique_zips"].sum()

        metadata_header = mo.md(f"""
        ## 🗺️ Office Location Metadata

        ### Market & Office Name Mapping

        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1.5rem 0;">
            <div style="padding: 1rem; background: #F0FDF4; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #166534; font-size: 0.875rem;">Total Offices</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #15803D;">{_total_offices}</p>
            </div>
            <div style="padding: 1rem; background: #EFF6FF; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #1E3A8A; font-size: 0.875rem;">Total Beneficiaries</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #1E40AF;">{_total_benes_metadata:,}</p>
            </div>
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">Unique ZIP Codes</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #B45309;">{_total_zips:,}</p>
            </div>
        </div>

        <div style="padding: 0.75rem 1rem; background: #F0FDF4; border-left: 4px solid #10B981; margin: 1rem 0;">
            <p style="margin: 0; color: #166534; font-size: 0.875rem;">
                <i class="fa-solid fa-map-location-dot"></i> Shows the mapping between <strong>market areas</strong> (office_location) and <strong>physical offices</strong> (office_name)
            </p>
        </div>
        """)

        # Format table
        metadata_table_data = []
        for _metadata_row in office_metadata.iter_rows(named=True):
            metadata_table_data.append({
                "Market/Service Area": _metadata_row["office_location"] if _metadata_row["office_location"] else "Unknown",
                "Office Name": _metadata_row["office_name"] if _metadata_row["office_name"] else "Not Assigned",
                "Total Beneficiaries": f"{_metadata_row['total_beneficiaries']:,}",
                "Unique ZIPs Served": f"{_metadata_row['unique_zips']:,}",
            })
    return metadata_header, metadata_table_data


@app.cell(hide_code=True)
def _(outreach_metrics, selected_ym):
    """Display outreach effectiveness metrics - current vs historical"""

    # Historical metrics
    _total_pop = outreach_metrics["total_population"]
    _total_contacted = outreach_metrics["total_contacted"]
    _total_sva = outreach_metrics["total_with_valid_sva"]

    _contacted_rate = outreach_metrics["contacted_to_sva_rate"]
    _opened_rate = outreach_metrics["email_opened_to_sva_rate"]
    _clicked_rate = outreach_metrics["email_clicked_to_sva_rate"]
    _baseline_rate = outreach_metrics["not_contacted_sva_rate"]

    _contacted_sva = outreach_metrics["contacted_with_sva"]
    _opened_sva = outreach_metrics["opened_with_sva"]
    _clicked_sva = outreach_metrics["clicked_with_sva"]
    _not_contacted_sva = outreach_metrics["not_contacted_with_sva"]

    # Check if we have current metrics
    current = outreach_metrics.get("current_metrics")

    if current and selected_ym:
        # Display current vs historical
        mo.md(
            f"""
            ## 📊 Voluntary Alignment Campaign Effectiveness Analysis

            ### Current Month ({selected_ym}) - {current["total"]:,} Aligned
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px;">
                    <h3 style="margin: 0; color: #3730A3; font-size: 0.875rem;">Currently Aligned</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #312E81;">{current["total"]:,}</p>
                </div>
                <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px;">
                    <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Were Contacted</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #1E40AF;">{current["contacted"]:,}</p>
                    <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">{(current["contacted"] / current["total"] * 100 if current["total"] > 0 else 0):.1f}% of current</p>
                </div>
                <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px;">
                    <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Have Valid SVA</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">{current["with_sva"]:,}</p>
                    <p style="margin: 0; color: #059669; font-size: 0.75rem;">{(current["with_sva"] / current["total"] * 100 if current["total"] > 0 else 0):.1f}% of current</p>
                </div>
            </div>

            #### Current Conversion Rates
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #F0F9FF; border-radius: 8px;">
                    <h3 style="margin: 0; color: #075985; font-size: 0.875rem;">Contact → SVA</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #0C4A6E;">{current["contacted_to_sva_rate"]:.1f}%</p>
                    <p style="margin: 0; color: #0C4A6E; font-size: 0.75rem;">{current["contacted_with_sva"]:,} of {current["contacted"]:,} contacted</p>
                </div>
                <div style="padding: 1rem; background: #FEF2F2; border-radius: 8px;">
                    <h3 style="margin: 0; color: #991B1B; font-size: 0.875rem;">No Contact → SVA</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #7F1D1D;">{current["not_contacted_sva_rate"]:.1f}%</p>
                    <p style="margin: 0; color: #7F1D1D; font-size: 0.75rem;">{current["not_contacted_with_sva"]:,} without outreach</p>
                </div>
            </div>

            ### Historical (All-Time) - {_total_pop:,} Total Population
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Total Population</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.25rem; font-weight: 600; color: #374151;">{_total_pop:,}</p>
                </div>
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Ever Contacted</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.25rem; font-weight: 600; color: #374151;">{_total_contacted:,}</p>
                    <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">{_total_contacted / _total_pop * 100:.1f}% of all</p>
                </div>
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Total SVA Signatures</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.25rem; font-weight: 600; color: #374151;">{_total_sva:,}</p>
                    <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">{_total_sva / _total_pop * 100:.1f}% of all</p>
                </div>
            </div>

            ### Conversion Rate Comparison
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 2rem; margin: 1rem 0;">
                <div>
                    <h4 style="margin: 0 0 0.5rem 0; color: #111827;">Current Month Rates</h4>
                    <div style="padding: 0.75rem; background: #F0F9FF; border-radius: 6px; margin-bottom: 0.5rem;">
                        <p style="margin: 0; color: #075985; font-size: 0.75rem;">Email Opened → SVA</p>
                        <p style="margin: 0; font-size: 1.25rem; font-weight: 600; color: #0C4A6E;">{current["email_opened_to_sva_rate"]:.1f}%</p>
                    </div>
                    <div style="padding: 0.75rem; background: #DCFCE7; border-radius: 6px;">
                        <p style="margin: 0; color: #14532D; font-size: 0.75rem;">Email Clicked → SVA</p>
                        <p style="margin: 0; font-size: 1.25rem; font-weight: 600; color: #166534;">{current["email_clicked_to_sva_rate"]:.1f}%</p>
                    </div>
                </div>
                <div>
                    <h4 style="margin: 0 0 0.5rem 0; color: #6B7280;">Historical Rates</h4>
                    <div style="padding: 0.75rem; background: #F9FAFB; border-radius: 6px; margin-bottom: 0.5rem;">
                        <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">Email Opened → SVA</p>
                        <p style="margin: 0; font-size: 1.25rem; font-weight: 600; color: #374151;">{_opened_rate:.1f}%</p>
                    </div>
                    <div style="padding: 0.75rem; background: #F9FAFB; border-radius: 6px;">
                        <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">Email Clicked → SVA</p>
                        <p style="margin: 0; font-size: 1.25rem; font-weight: 600; color: #374151;">{_clicked_rate:.1f}%</p>
                    </div>
                </div>
            </div>

            ### 🎯 Key Insights

            <div style="padding: 1rem; background: #F9FAFB; border: 1px solid #E5E7EB; border-radius: 8px; margin: 1rem 0;">
                <p style="margin: 0.5rem 0; color: #4B5563; font-size: 0.875rem;">
                    <strong>Current Month Lift:</strong> {current["contacted_to_sva_rate"] - current["not_contacted_sva_rate"]:.1f}% improvement from outreach<br/>
                    <strong>Historical Lift:</strong> {_contacted_rate - _baseline_rate:.1f}% improvement over baseline<br/>
                    <strong>Coverage Gap:</strong> {100 - (_total_contacted / _total_pop * 100):.1f}% of population has never been contacted
                </p>
            </div>
            """
        )
    else:
        # Display historical only (no current month selected)
        mo.md(
            f"""
            ## 📊 Voluntary Alignment Campaign Effectiveness Analysis (Historical)

            ### Population Overview (Voluntary Alignment Campaigns Only)
            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #374151; font-size: 0.875rem;">Total Population</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #111827;">{_total_pop:,}</p>
                </div>
                <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px;">
                    <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Total Contacted</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #1E40AF;">{_total_contacted:,}</p>
                    <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">{_total_contacted / _total_pop * 100:.1f}% of population</p>
                </div>
                <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px;">
                    <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Valid SVA Signatures</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">{_total_sva:,}</p>
                    <p style="margin: 0; color: #059669; font-size: 0.75rem;">{_total_sva / _total_pop * 100:.1f}% of population</p>
                </div>
            </div>

            ### TRUE Conversion Rates (Across ENTIRE Population)

            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #F0F9FF; border-radius: 8px;">
                    <h3 style="margin: 0; color: #075985; font-size: 0.875rem;">Any Contact → SVA</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.75rem; font-weight: 600; color: #0C4A6E;">{_contacted_rate:.1f}%</p>
                    <p style="margin: 0; color: #0C4A6E; font-size: 0.75rem;">{_contacted_sva:,} of {_total_contacted:,} contacted signed</p>
                </div>
                <div style="padding: 1rem; background: #FEF2F2; border-radius: 8px;">
                    <h3 style="margin: 0; color: #991B1B; font-size: 0.875rem;">Not Contacted → SVA (Baseline)</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.75rem; font-weight: 600; color: #7F1D1D;">{_baseline_rate:.1f}%</p>
                    <p style="margin: 0; color: #7F1D1D; font-size: 0.75rem;">{_not_contacted_sva:,} signed without outreach</p>
                </div>
            </div>

            ### Email Engagement → SVA Conversion
            <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                    <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">Email Opened → SVA</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #78350F;">{_opened_rate:.1f}%</p>
                    <p style="margin: 0; color: #78350F; font-size: 0.75rem;">{_opened_sva:,} who opened signed</p>
                </div>
                <div style="padding: 1rem; background: #DCFCE7; border-radius: 8px;">
                    <h3 style="margin: 0; color: #14532D; font-size: 0.875rem;">Email Clicked → SVA</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #166534;">{_clicked_rate:.1f}%</p>
                    <p style="margin: 0; color: #166534; font-size: 0.75rem;">{_clicked_sva:,} who clicked signed</p>
                </div>
            </div>

            ### 🎯 Key Insights

            <div style="padding: 1rem; background: #F9FAFB; border: 1px solid #E5E7EB; border-radius: 8px; margin: 1rem 0;">
                <p style="margin: 0.5rem 0; color: #4B5563; font-size: 0.875rem;">
                    <strong>Lift from Outreach:</strong> {_contacted_rate - _baseline_rate:.1f}% improvement over baseline<br/>
                    <strong>Engagement Impact:</strong> Email clicks show {_clicked_rate - _opened_rate:.1f}% higher conversion than just opens<br/>
                    <strong>Coverage Gap:</strong> {100 - (_total_contacted / _total_pop * 100):.1f}% of population has never been contacted
                </p>
            </div>

            <div style="padding: 0.75rem; background: #FEF8E1; border-left: 4px solid #F59E0B; margin: 1rem 0;">
                <p style="margin: 0; color: #92400E; font-size: 0.8rem;">
                    <strong>⚠️ Note:</strong> These metrics ONLY include "ACO Voluntary Alignment" campaigns, excluding CAHPS reminders and other campaigns.
                    Conversion rates are calculated across the ENTIRE population, not just those in REACH, providing honest effectiveness metrics.
                </p>
            </div>
            """
        )
    return


@app.cell(hide_code=True)
def _(df_enriched, email_by_campaign, mailed_by_campaign):
    """Calculate quarterly campaign effectiveness using idempotent function"""
    campaign_metrics = calculate_quarterly_campaign_effectiveness(df_enriched, email_by_campaign, mailed_by_campaign, pl)
    return (campaign_metrics,)


@app.cell(hide_code=True)
def _(campaign_metrics):
    """Display quarterly campaign effectiveness table"""

    if campaign_metrics is not None and len(campaign_metrics) > 0:
        # Format the data for display
        _formatted_data = []
        for _row in campaign_metrics.to_dicts():
            _formatted_data.append(
                {
                    "Quarter": _row["campaign_period"],
                    "Contacted": f"{_row['total_contacted']:,}",
                    "Email Only": f"{_row['emailed']:,}",
                    "Mail Only": f"{_row['mailed']:,}",
                    "Both Channels": f"{_row.get('both_email_and_letter', 0):,}",  # Per marimo best practice: use .get() for optional fields
                    "Valid SVA": f"{_row.get('signed_sva', 0):,}",
                    "SVA Rate": f"{_row.get('overall_conversion_rate', 0):.1f}%",
                }
            )

        # Combine header and table using vstack
        mo.vstack(
            [
                mo.md(
                    """
                ### Quarterly Campaign Effectiveness

                <div style="padding: 0.75rem 1rem; background: #F0FDF4; border-left: 4px solid #10B981; margin: 1rem 0;">
                    <p style="margin: 0; color: #166534; font-size: 0.9rem;">
                        📊 Conversion rates to valid SVA signatures by outreach quarter
                    </p>
                </div>
                """
                ),
                mo.ui.table(_formatted_data, page_size=50),
            ]
        )
    else:
        mo.md("### Campaign Effectiveness\n\n*No campaign data available*")
    return


@app.cell(hide_code=True)
def _(df_enriched, email_by_campaign, mailed_by_campaign):
    """Calculate office-level campaign effectiveness"""
    office_campaign_metrics = calculate_office_campaign_effectiveness(df_enriched, email_by_campaign, mailed_by_campaign, pl)
    return (office_campaign_metrics,)


@app.cell(hide_code=True)
def _(office_campaign_metrics):
    """Display office-level campaign effectiveness table"""

    if office_campaign_metrics is not None and len(office_campaign_metrics) > 0:
        # Calculate totals across all offices
        _total_contacted = office_campaign_metrics["total_contacted"].sum()
        _total_signed = office_campaign_metrics["signed_sva"].sum()
        _overall_rate = (_total_signed / _total_contacted * 100) if _total_contacted > 0 else 0
        _num_offices = len(office_campaign_metrics)

        # Format the data for display
        office_campaign_data = []
        for _row in office_campaign_metrics.to_dicts():
            office_campaign_data.append({
                "Office Name": _row["office_name"] if _row["office_name"] else "Unknown",
                "Market": _row["office_location"] if _row["office_location"] else "Unknown",
                "Contacted": f"{_row['total_contacted']:,}",
                "Emailed": f"{_row['emailed']:,}",
                "Mailed": f"{_row['mailed']:,}",
                "Both": f"{_row.get('both_email_and_letter', 0):,}",
                "Signed SVA": f"{_row.get('signed_sva', 0):,}",
                "Conv. Rate": f"{_row.get('overall_conversion_rate', 0):.1f}%",
                "Email→SVA": f"{_row.get('email_to_sva_rate', 0):.1f}%",
                "Mail→SVA": f"{_row.get('letter_to_sva_rate', 0):.1f}%",
            })

        office_campaign_header = mo.md(f"""
        ## 📧 Campaign Effectiveness by Office

        ### Voluntary Alignment Outreach Performance

        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1.5rem 0;">
            <div style="padding: 1rem; background: #EFF6FF; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Offices with Campaigns</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #1E3A8A;">{_num_offices}</p>
            </div>
            <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Total Contacted</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #1E3A8A;">{_total_contacted:,}</p>
            </div>
            <div style="padding: 1rem; background: #DCFCE7; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #166534; font-size: 0.875rem;">Overall Conversion</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #15803D;">{_overall_rate:.1f}%</p>
                <p style="margin: 0; color: #166534; font-size: 0.75rem;">{_total_signed:,} signed SVA</p>
            </div>
        </div>

        <div style="padding: 0.75rem 1rem; background: #DBEAFE; border-left: 4px solid #3B82F6; margin: 1rem 0;">
            <p style="margin: 0; color: #1E40AF; font-size: 0.9rem;">
                <i class="fa-solid fa-chart-line"></i> Conversion rates to valid SVA signatures by office location
            </p>
        </div>
        """)
    return office_campaign_data, office_campaign_header


@app.cell(hide_code=True)
def _(df, year_months):
    """Calculate alignment trends over time using idempotent function"""
    alignment_trends = calculate_alignment_trends_over_time(df, year_months, pl)
    return (alignment_trends,)


@app.cell(hide_code=True)
def _(year_months):
    """Create the interactive month slider"""
    if year_months and len(year_months) > 0:
        # Create month selector with proper formatting
        month_slider = mo.ui.slider(
            start=0,
            stop=len(year_months) - 1,
            step=1,
            value=len(year_months) - 1,  # Default to most recent
            show_value=False,  # We'll show our own formatted value
            label="Select Month",
        )
    else:
        month_slider = None
    return (month_slider,)


@app.cell(hide_code=True)
def _(month_slider, year_months):
    """Create the slider display with instructions"""
    if month_slider is not None and year_months:
        # Get the selected month for display
        _selected = year_months[month_slider.value]
        _formatted = f"{_selected[:4]}-{_selected[4:6]}"

        slider_display = mo.vstack(
            [
                mo.md("## 📅 Interactive Time Travel"),
                mo.md(
                    f"""
                <div style="padding: 1rem; background: #F0F9FF; border-radius: 8px; margin: 1rem 0;">
                    <p style="margin: 0 0 0.5rem 0; font-weight: 600; color: #0C4A6E;">
                        Select Month to Analyze: <span style="color: #0284C7; font-size: 1.25rem;">{_formatted}</span>
                    </p>
                    <p style="margin: 0; color: #075985; font-size: 0.875rem;">
                        Move the slider to see how alignment, demographics, and outreach effectiveness change over time.
                        All "Current" metrics below will update to show the selected month.
                    </p>
                </div>
                """
                ),
                month_slider,
                mo.md(
                    f"""
                <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 0.5rem; margin: 1rem 0;">
                    <div style="padding: 0.5rem; background: #F9FAFB; border-radius: 4px; text-align: center;">
                        <span style="color: #6B7280; font-size: 0.75rem;">Earliest</span><br/>
                        <strong style="color: #374151;">{year_months[0][:4]}-{year_months[0][4:6]}</strong>
                    </div>
                    <div style="padding: 0.5rem; background: #DBEAFE; border-radius: 4px; text-align: center;">
                        <span style="color: #3B82F6; font-size: 0.75rem;">Selected</span><br/>
                        <strong style="color: #1E40AF;">{_formatted}</strong>
                    </div>
                    <div style="padding: 0.5rem; background: #F9FAFB; border-radius: 4px; text-align: center;">
                        <span style="color: #6B7280; font-size: 0.75rem;">Latest</span><br/>
                        <strong style="color: #374151;">{year_months[-1][:4]}-{year_months[-1][4:6]}</strong>
                    </div>
                </div>
                """
                ),
            ]
        )
    else:
        slider_display = mo.md("*No temporal data available for interactive analysis*")
    return (slider_display,)


@app.cell(hide_code=True)
def _(month_slider, year_months):
    """Get the currently selected month from slider"""
    if month_slider is not None and year_months:
        selected_ym = year_months[month_slider.value]
        selected_month_display = f"{selected_ym[:4]}-{selected_ym[4:6]}"
    else:
        selected_ym = year_months[-1] if year_months else None
        selected_month_display = f"{selected_ym[:4]}-{selected_ym[4:6]}" if selected_ym else "N/A"
    return selected_month_display, selected_ym


@app.cell(hide_code=True)
def _(year_months):
    # Create temporal_coverage from year_months data
    if year_months and len(year_months) > 0:
        temporal_coverage = {
            "total_months": len(year_months),
            "first_month": year_months[0],
            "last_month": year_months[-1],
        }
    else:
        temporal_coverage = {"total_months": 0, "first_month": None, "last_month": None}

    if temporal_coverage["total_months"] > 0:
        _first = temporal_coverage["first_month"]
        _last = temporal_coverage["last_month"]
        _total = temporal_coverage["total_months"]

        # Format year-month strings
        if _first:
            _first_formatted = f"{_first[:4]}-{_first[4:6]}"
        else:
            _first_formatted = "N/A"

        if _last:
            _last_formatted = f"{_last[:4]}-{_last[4:6]}"
        else:
            _last_formatted = "N/A"

        mo.md(
            f"""
            ## Temporal Coverage

            <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Date Range</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.25rem; font-weight: 600; color: #2E3254;">{_first_formatted} to {_last_formatted}</p>
                </div>
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Total Months</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #2E3254;">{_total}</p>
                </div>
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Point-in-Time Windows</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.25rem; font-weight: 600; color: #2E3254;">✓ Enabled</p>
                </div>
            </div>

            **Note:** Each year-month column reflects only the information available at that point in time to prevent data leakage in temporal analysis.
            """
        )
    else:
        mo.md("## Temporal Coverage\n\n*No temporal columns found in dataset*")
    return


@app.cell(hide_code=True)
def _(df, selected_ym):
    """Calculate enrollment stats for selected month using idempotent function"""
    selected_month_stats = calculate_enrollment_stats_for_selected_month(df, selected_ym, pl)
    return (selected_month_stats,)


@app.cell(hide_code=True)
def _(selected_month_display, selected_month_stats):
    """Display point-in-time enrollment status for selected month"""
    if selected_month_stats:
        reach_count = selected_month_stats.get("REACH", 0)
        mssp_count = selected_month_stats.get("MSSP", 0)
        ffs_count = selected_month_stats.get("FFS", 0)
        not_enrolled = selected_month_stats.get("Not Enrolled", 0)
        total_aligned = reach_count + mssp_count
        total_population = reach_count + mssp_count + ffs_count + not_enrolled

        mo.md(
            f"""
            ## 📊 Point-in-Time Snapshot: {selected_month_display}

            ### Enrollment Distribution
            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin: 1rem 0;">
                <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px;">
                    <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">REACH</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">{reach_count:,}</p>
                    <p style="margin: 0; color: #059669; font-size: 0.75rem;">{reach_count / total_population * 100:.1f}% of pop</p>
                </div>
                <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px;">
                    <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">MSSP</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #1E40AF;">{mssp_count:,}</p>
                    <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">{mssp_count / total_population * 100:.1f}% of pop</p>
                </div>
                <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                    <h3 style="margin: 0; color: #D97706; font-size: 0.875rem;">FFS Only</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #D97706;">{ffs_count:,}</p>
                    <p style="margin: 0; color: #D97706; font-size: 0.75rem;">{ffs_count / total_population * 100:.1f}% of pop</p>
                </div>
                <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                    <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Not Active</h3>
                    <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #6B7280;">{not_enrolled:,}</p>
                    <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">{not_enrolled / total_population * 100:.1f}% of pop</p>
                </div>
            </div>

            ### Value-Based Care Coverage at {selected_month_display}
            <div style="padding: 1rem; background: #F0F9FF; border-radius: 8px; margin: 1rem 0;">
                <div style="display: grid; grid-template-columns: 2fr 1fr 1fr; gap: 1rem;">
                    <div>
                        <p style="margin: 0; color: #0C4A6E; font-size: 1.25rem; font-weight: 600;">
                            {total_aligned:,} in Value-Based Programs
                        </p>
                        <p style="margin: 0.25rem 0 0 0; color: #075985; font-size: 0.875rem;">
                            {total_aligned / total_population * 100:.1f}% coverage rate
                        </p>
                    </div>
                    <div>
                        <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">Opportunity</p>
                        <p style="margin: 0; color: #374151; font-size: 1.25rem; font-weight: 600;">{ffs_count:,}</p>
                        <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">in FFS</p>
                    </div>
                    <div>
                        <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">Total Gap</p>
                        <p style="margin: 0; color: #374151; font-size: 1.25rem; font-weight: 600;">{total_population - total_aligned:,}</p>
                        <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">not aligned</p>
                    </div>
                </div>
            </div>
            """
        )
    else:
        mo.md(
            "*No enrollment data available for selected month*"
        )  # Per marimo docs: display-only cells don't need returns
    return


@app.cell(hide_code=True)
def _(current_source_stats, historical_source_stats, selected_month_display):
    """Display alignment source analysis"""
    mo.md("## Alignment Source Analysis")

    if current_source_stats is not None and historical_source_stats is not None:
        # Format current source data
        curr_table = []
        for _row in current_source_stats.to_dicts():
            curr_table.append(
                {"Source": _row["current_alignment_source"], "Count": f"{_row['count']:,}"}
            )

        # Format historical source data
        hist_table = []
        for _row in historical_source_stats.to_dicts():
            hist_table.append(
                {"Source": _row["primary_alignment_source"], "Count": f"{_row['count']:,}"}
            )

        mo.vstack(
            [
                mo.md(f"### Current Alignment Sources ({selected_month_display})"),
                mo.ui.table(curr_table, page_size=50),
                mo.md("### Historical Alignment Sources (All-Time)"),
                mo.ui.table(hist_table, page_size=50),
            ]
        )
    return


@app.cell(hide_code=True)
def _(df_enriched, most_recent_ym):
    """Calculate vintage cohorts using the idempotent function"""
    vintage_df = calculate_vintage_cohorts(df_enriched, most_recent_ym, pl)
    return (vintage_df,)


@app.cell(hide_code=True)
def _(most_recent_ym, vintage_df):
    """Calculate vintage cohort statistics using the idempotent function"""
    vintage_distribution = calculate_vintage_distribution(vintage_df, most_recent_ym, pl)
    return (vintage_distribution,)


@app.cell(hide_code=True)
def _():
    """Display branded header"""
    create_branded_header(datetime, mo)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    <div style="display: flex; align-items: center; margin-bottom: 2rem; padding-bottom: 1rem; border-bottom: 2px solid #E5E7EB;">
        <i class="fa-solid fa-users-medical" style="color: #2E3254; font-size: 2.5rem; margin-right: 1rem;"></i>
        <div>
            <h1 style="color: #2E3254; margin: 0; font-weight: 700;">Consolidated Alignments Analysis</h1>
            <p style="color: #6B7280; margin: 0.25rem 0 0 0; font-size: 1.1rem;">Comprehensive beneficiary alignment insights across ACO programs</p>
        </div>
    </div>

    This notebook analyzes the consolidated alignment dataset that combines:
    - <i class="fa-solid fa-hospital"></i> ACO program alignment (REACH & MSSP)
    - <i class="fa-solid fa-file-signature"></i> Voluntary alignment (SVA & PBVAR)
    - <i class="fa-solid fa-user"></i> Beneficiary demographics
    - <i class="fa-solid fa-calendar-check"></i> Temporal tracking with point-in-time windows
    """)# Per marimo docs: cells that only display output don't need to return anything
    # https://docs.marimo.io/guides/reactivity/ - cells contribute to reactive graph through variable definitions
    return


@app.cell(hide_code=True)
def _(total_columns, total_records):
    mo.md(
        f"""
    ## Dataset Overview

    <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; margin: 1rem 0;">
        <div style="padding: 1rem; background: #F9FAFB; border-radius: 8px;">
            <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Total Records</h3>
            <p style="margin: 0.25rem 0 0 0; font-size: 1.75rem; font-weight: 600; color: #2E3254;">{total_records:,}</p>
        </div>
        <div style="padding: 1rem; background: #F9FAFB; border-radius: 8px;">
            <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Total Columns</h3>
            <p style="margin: 0.25rem 0 0 0; font-size: 1.75rem; font-weight: 600; color: #2E3254;">{total_columns}</p>
        </div>
    </div>
    """
    )  # Per marimo docs: display-only cells don't need returns
    return


@app.cell(hide_code=True)
def _(
    action_stats,
    alignment_trends,
    current_alignment_stats,
    df,
    df_enriched,
    display_excel_export_button,
    historical_stats,
    most_recent_ym,
    newly_added_stats,
    office_alignment_types,
    office_campaign_metrics,
    office_metadata,
    office_program_dist,
    office_stats,
    office_transitions,
    office_vintage_distribution,
    outreach_metrics,
    sva_stats,
    transition_stats,
    vintage_distribution,
):
    """Display Excel export button"""
    display_excel_export_button(
        current_alignment_stats,
        historical_stats,
        alignment_trends,
        transition_stats,
        vintage_distribution,
        df,
        df_enriched,
        datetime,
        mo,
        most_recent_ym,
        pl,
        sva_stats,
        action_stats,
        outreach_metrics,
        office_stats,
        office_alignment_types,
        office_program_dist,
        office_transitions,
        office_metadata,
        office_campaign_metrics,
        office_vintage_distribution,
        newly_added_stats,
    )
    return


@app.cell(hide_code=True)
def _(df, df_enriched, selected_ym):
    """Display enrollment patterns analysis"""
    display_enrollment_patterns(df, df_enriched, selected_ym, mo, pl)
    return


@app.cell(hide_code=True)
def _(slider_display):
    """Display the slider interface"""
    # Per marimo docs: displaying UI elements in cells
    slider_display
    return


@app.cell(hide_code=True)
def _(curr_ym, prev_ym, transition_stats):
    """Display the alignment transitions matrix"""

    display_transitions_matrix(transition_stats, prev_ym, curr_ym, mo)
    # TODO From feedback session with DL clarify no change
    return


@app.cell(hide_code=True)
def _(build_living_beneficiary_expr, df, most_recent_ym):
    # Calculate detailed enrollment status using temporal columns for the selected observation
    curr_schema = df.collect_schema().names()

    if most_recent_ym:
        curr_month_display = f"{most_recent_ym[:4]}-{most_recent_ym[4:6]}"
        reach_col = f"ym_{most_recent_ym}_reach"
        mssp_col = f"ym_{most_recent_ym}_mssp"
        ffs_col = f"ym_{most_recent_ym}_ffs"

        # Filter for living beneficiaries only
        living_expr = build_living_beneficiary_expr(curr_schema)
        df_living = df.filter(living_expr)

        # Calculate counts from temporal columns for this observation month
        if reach_col in curr_schema and mssp_col in curr_schema and ffs_col in curr_schema:
            curr_reach = df_living.filter(pl.col(reach_col)).select(pl.len()).collect().item()
            curr_mssp = df_living.filter(pl.col(mssp_col)).select(pl.len()).collect().item()
            curr_ffs = df_living.filter(pl.col(ffs_col)).select(pl.len()).collect().item()
        else:
            curr_reach = curr_mssp = curr_ffs = 0

        # Deceased count (all deceased, not just in this month)
        curr_deceased = df.filter(pl.col("death_date").is_not_null()).select(pl.len()).collect().item() if "death_date" in curr_schema else 0

        # Totals
        curr_total = df.select(pl.len()).collect().item()
        curr_living = df_living.select(pl.len()).collect().item()
        curr_unknown = curr_living - (curr_reach + curr_mssp + curr_ffs)
    else:
        curr_month_display = "N/A"
        curr_reach = curr_mssp = curr_ffs = curr_deceased = curr_unknown = curr_total = curr_living = 0

    mo.md(
        f"""
        ## Current Enrollment Status
        ### As of {curr_month_display} (Most Recent Data)

        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px;">
                <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">ACO REACH</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #1E40AF;">{curr_reach:,}</p>
                <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">Currently in REACH program</p>
            </div>
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                <h3 style="margin: 0; color: #D97706; font-size: 0.875rem;">MSSP</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #D97706;">{curr_mssp:,}</p>
                <p style="margin: 0; color: #D97706; font-size: 0.75rem;">Currently in MSSP</p>
            </div>
            <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px;">
                <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Traditional FFS</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">{curr_ffs:,}</p>
                <p style="margin: 0; color: #059669; font-size: 0.75rem;">Have FFS claims, not in ACO</p>
            </div>
        </div>

        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #FEE2E2; border-radius: 8px;">
                <h3 style="margin: 0; color: #DC2626; font-size: 0.875rem;">Deceased</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #DC2626;">{curr_deceased:,}</p>
                <p style="margin: 0; color: #DC2626; font-size: 0.75rem;">All deceased beneficiaries</p>
            </div>
            <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px;">
                <h3 style="margin: 0; color: #6B7280; font-size: 0.875rem;">Unknown Status</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #6B7280;">{curr_unknown:,}</p>
                <p style="margin: 0; color: #6B7280; font-size: 0.75rem;">Living, not in REACH/MSSP/FFS</p>
            </div>
        </div>

        <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px; margin: 1rem 0;">
            <h3 style="margin: 0; color: #4C1D95; font-size: 0.875rem;">Total Beneficiaries</h3>
            <p style="margin: 0.25rem 0 0 0; font-size: 2rem; font-weight: 700; color: #4C1D95;">{curr_total:,}</p>
        </div>

        **Note:**
        - **Enrollment counts** (REACH/MSSP/FFS) include only living beneficiaries actively enrolled as of {curr_month_display}
        - **Deceased count** includes all beneficiaries who have passed away, regardless of their last enrollment status
        - **Unknown Status** includes living beneficiaries not in REACH/MSSP/FFS (likely Medicare Advantage or inactive)
        - **Total** = REACH + MSSP + FFS + Deceased + Unknown Status
        """
    )
    # Return variables for debug cell
    return (
        curr_deceased,
        curr_ffs,
        curr_living,
        curr_month_display,
        curr_mssp,
        curr_reach,
        curr_total,
        curr_unknown,
    )


@app.cell(hide_code=True)
def _(historical_stats):
    _reach = historical_stats["ever_reach_count"][0]
    _mssp = historical_stats["ever_mssp_count"][0]
    _both = historical_stats["ever_both_count"][0]
    _never = historical_stats["never_aligned_count"][0]

    mo.md(
        f"""
        ## Historical ACO Program Participation
        ### All-Time Alignment History

        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px;">
                <h3 style="margin: 0; color: #4C1D95; font-size: 0.875rem;">Ever in REACH</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #4C1D95;">{_reach:,}</p>
            </div>
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">Ever in MSSP</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #92400E;">{_mssp:,}</p>
            </div>
            <div style="padding: 1rem; background: #ECFDF5; border-radius: 8px;">
                <h3 style="margin: 0; color: #064E3B; font-size: 0.875rem;">Ever in Both</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #064E3B;">{_both:,}</p>
            </div>
            <div style="padding: 1rem; background: #F9FAFB; border-radius: 8px;">
                <h3 style="margin: 0; color: #374151; font-size: 0.875rem;">Never Aligned</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #374151;">{_never:,}</p>
            </div>
        </div>

        **Note:** These counts include all beneficiaries who have EVER been aligned, not just current alignment.
        """
    )  # Per marimo docs: display-only cells don't need returns
    return


@app.cell(hide_code=True)
def _(sva_stats):
    _curr_vol = sva_stats["currently_voluntary"]
    _curr_claims = sva_stats["currently_claims"]
    _reach_renewal = sva_stats["reach_needs_renewal"]
    _reach_contacted = sva_stats["reach_contacted"]
    _reach_engaged = sva_stats["reach_email_engaged"]
    _reach_needs_outreach = sva_stats["reach_needs_outreach"]
    _mssp_eligible = sva_stats["mssp_sva_eligible"]
    _mssp_expired = sva_stats["mssp_expired_sva"]
    _mssp_contacted = sva_stats["mssp_contacted"]
    _mssp_needs_outreach = sva_stats["mssp_needs_outreach"]
    _ever_vol = sva_stats["ever_voluntary"]
    _valid_sig = sva_stats["has_valid_signature"]
    _total_contacted = sva_stats["total_contacted"]
    _total_engaged = sva_stats["total_email_engaged"]

    mo.md(
        f"""
        ## Voluntary Alignment & Outreach Analysis

        ### Current REACH Alignment Source
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px;">
                <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Voluntary (SVA)</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">{_curr_vol:,}</p>
                <p style="margin: 0; color: #059669; font-size: 0.75rem;">In REACH via SVA</p>
            </div>
            <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px;">
                <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Claims-Based</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #1E40AF;">{_curr_claims:,}</p>
                <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">In REACH via claims</p>
            </div>
            <div style="padding: 1rem; background: #FEE2E2; border-radius: 8px;">
                <h3 style="margin: 0; color: #DC2626; font-size: 0.875rem;">Invalid SVA</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #DC2626;">{_reach_renewal:,}</p>
                <p style="margin: 0; color: #DC2626; font-size: 0.75rem;">In REACH with invalid provider</p>
            </div>
        </div>

        ### MSSP SVA Opportunities
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                <h3 style="margin: 0; color: #D97706; font-size: 0.875rem;">SVA Eligible</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #D97706;">{_mssp_eligible:,}</p>
                <p style="margin: 0; color: #D97706; font-size: 0.75rem;">MSSP beneficiaries who could sign SVA</p>
            </div>
            <div style="padding: 1rem; background: #FBBF24; border-radius: 8px;">
                <h3 style="margin: 0; color: #B45309; font-size: 0.875rem;">Has SVA (Invalid)</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #B45309;">{_mssp_expired:,}</p>
                <p style="margin: 0; color: #B45309; font-size: 0.75rem;">MSSP with SVA (not valid for MSSP)</p>
            </div>
        </div>

        ### SVA Signature Status

        <div style="padding: 0.5rem 1rem; background: #DCFCE7; border-left: 4px solid #16A34A; margin: 1rem 0;">
            <p style="margin: 0; color: #166534; font-size: 0.9rem;">
                <strong>⚠️ Important:</strong> "Valid for REACH" means: has valid Participant Provider AND currently enrolled in REACH
            </p>
        </div>

        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #EDE9FE; border-radius: 8px;">
                <h3 style="margin: 0; color: #7C3AED; font-size: 0.875rem;">Ever Had SVA</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #7C3AED;">{_ever_vol:,}</p>
                <p style="margin: 0; color: #7C3AED; font-size: 0.75rem;">All with SVA signature</p>
            </div>
            <div style="padding: 1rem; background: #DCFCE7; border-radius: 8px;">
                <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Valid for REACH</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">{_valid_sig:,}</p>
                <p style="margin: 0; color: #059669; font-size: 0.75rem;">Valid provider + in REACH</p>
            </div>
            <div style="padding: 1rem; background: #FEE2E2; border-radius: 8px;">
                <h3 style="margin: 0; color: #DC2626; font-size: 0.875rem;">Invalid/Not in REACH</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #DC2626;">{_ever_vol - _valid_sig:,}</p>
                <p style="margin: 0; color: #DC2626; font-size: 0.75rem;">Invalid provider or not in REACH</p>
            </div>
        </div>

        ### Outreach Effectiveness - REACH Program
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #DCFCE7; border-radius: 8px;">
                <h3 style="margin: 0; color: #166534; font-size: 0.875rem;">Contacted</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #166534;">{_reach_contacted:,}</p>
                <p style="margin: 0; color: #166534; font-size: 0.75rem;">Current REACH beneficiaries reached</p>
            </div>
            <div style="padding: 1rem; background: #FEF9C3; border-radius: 8px;">
                <h3 style="margin: 0; color: #713F12; font-size: 0.875rem;">Email Engaged</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #713F12;">{_reach_engaged:,}</p>
                <p style="margin: 0; color: #713F12; font-size: 0.75rem;">Opened or clicked emails</p>
            </div>
            <div style="padding: 1rem; background: #FEE2E2; border-radius: 8px;">
                <h3 style="margin: 0; color: #991B1B; font-size: 0.875rem;">Needs Outreach</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #991B1B;">{_reach_needs_outreach:,}</p>
                <p style="margin: 0; color: #991B1B; font-size: 0.75rem;">Claims-based, never contacted</p>
            </div>
        </div>

        ### Outreach Opportunities - MSSP Program
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px;">
                <h3 style="margin: 0; color: #312E81; font-size: 0.875rem;">MSSP Contacted</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #312E81;">{_mssp_contacted:,}</p>
                <p style="margin: 0; color: #312E81; font-size: 0.75rem;">MSSP beneficiaries reached</p>
            </div>
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                <h3 style="margin: 0; color: #78350F; font-size: 0.875rem;">MSSP Needs Outreach</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #78350F;">{_mssp_needs_outreach:,}</p>
                <p style="margin: 0; color: #78350F; font-size: 0.75rem;">No SVA, never contacted</p>
            </div>
        </div>

        ### Overall Outreach Summary
        <div style="padding: 1rem; background: #F3F4F6; border-radius: 8px; margin: 1rem 0;">
            <p style="margin: 0; font-size: 0.875rem; color: #6B7280;">
                Total beneficiaries contacted: <strong>{_total_contacted:,}</strong> |
                Email engaged: <strong>{_total_engaged:,}</strong>
            </p>
        </div>

        **Key Insights:**
        - SVA is only valid for beneficiaries with Participant Provider AND currently in REACH
        - "Preferred Provider" is NOT valid - must be "Participant Provider"
        - MSSP beneficiaries can sign SVA to potentially move to REACH (but it won't be valid until they're in REACH)
        - Invalid signatures may need provider updates or beneficiary moved to REACH
        - Outreach effectiveness shows engagement opportunities
        """
    )  # Per marimo docs: display-only cells don't need returns
    return


@app.cell(hide_code=True)
def _(office_enrollment_header, office_enrollment_table_data):
    mo.vstack([office_enrollment_header, mo.ui.table(office_enrollment_table_data, page_size=50, show_column_summaries=True )])
    return


@app.cell(hide_code=True)
def _(alignment_types_header, alignment_types_table_data):
    mo.vstack([alignment_types_header, mo.ui.table(alignment_types_table_data, page_size=50)])
    return


@app.cell(hide_code=True)
def _(program_dist_header, program_dist_table_data):
    mo.vstack([program_dist_header, mo.ui.table(program_dist_table_data, page_size=50)])
    return


@app.cell(hide_code=True)
def _(transitions_header, transitions_table_data):
    mo.vstack([transitions_header, mo.ui.table(transitions_table_data, page_size=50)])
    return


@app.cell(hide_code=True)
def _(metadata_header, metadata_table_data):
    mo.vstack([metadata_header, mo.ui.table(metadata_table_data, page_size=50)])
    return


@app.cell(hide_code=True)
def _(office_campaign_data, office_campaign_header):
    mo.vstack([office_campaign_header, mo.ui.table(office_campaign_data, page_size=50)])
    return


@app.cell(hide_code=True)
def _(office_campaign_data):
    mo.ui.table(office_campaign_data)
    return


@app.cell(hide_code=True)
def _(office_vintage_data, office_vintage_header):
    mo.vstack([office_vintage_header, mo.ui.table(office_vintage_data, page_size=50)])
    return


@app.cell(hide_code=True)
def _(emails_df, mailed_df):
    """Display enhanced campaign performance using idempotent function"""
    campaign_perf = calculate_enhanced_campaign_performance(emails_df, mailed_df, pl)

    # Extract metrics for display
    email = campaign_perf["email"]
    mail = campaign_perf["mail"]

    mo.md(
        f"""
        ## Outreach Campaign Performance Metrics

        ### Email Campaign Performance
        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px;">
                <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Emails Sent</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #1E40AF;">{email['total_sent']:,}</p>
                <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">{email['unique_recipients']:,} unique recipients</p>
            </div>
            <div style="padding: 1rem; background: #DCFCE7; border-radius: 8px;">
                <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Delivery Rate</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">{email['delivery_rate']:.1f}%</p>
                <p style="margin: 0; color: #059669; font-size: 0.75rem;">{email['delivered']:,} delivered</p>
            </div>
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px;">
                <h3 style="margin: 0; color: #D97706; font-size: 0.875rem;">Open Rate</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #D97706;">{email['open_rate']:.1f}%</p>
                <p style="margin: 0; color: #D97706; font-size: 0.75rem;">{email['opened']:,} opened</p>
            </div>
            <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px;">
                <h3 style="margin: 0; color: #4C1D95; font-size: 0.875rem;">Click Rate</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #4C1D95;">{email['click_rate']:.1f}%</p>
                <p style="margin: 0; color: #4C1D95; font-size: 0.75rem;">{email['clicked']:,} clicked from {email['opened']:,} opened</p>
            </div>
        </div>

        ### Mail Campaign Performance
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1rem 0;">
            <div style="padding: 1rem; background: #F3E8FF; border-radius: 8px;">
                <h3 style="margin: 0; color: #7C3AED; font-size: 0.875rem;">Letters Sent</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #7C3AED;">{mail['total_sent']:,}</p>
                <p style="margin: 0; color: #7C3AED; font-size: 0.75rem;">{mail['unique_recipients']:,} unique recipients</p>
            </div>
            <div style="padding: 1rem; background: #FECACA; border-radius: 8px;">
                <h3 style="margin: 0; color: #DC2626; font-size: 0.875rem;">Delivery Rate</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #DC2626;">{mail['delivery_rate']:.1f}%</p>
                <p style="margin: 0; color: #DC2626; font-size: 0.75rem;">{mail['delivered']:,} delivered</p>
            </div>
            <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px;">
                <h3 style="margin: 0; color: #059669; font-size: 0.875rem;">Estimated Cost</h3>
                <p style="margin: 0.25rem 0 0 0; font-size: 1.5rem; font-weight: 600; color: #059669;">${mail['estimated_cost']:,.0f}</p>
                <p style="margin: 0; color: #059669; font-size: 0.75rem;">$0.65 per letter</p>
            </div>
        </div>
        """
    )  # Per marimo docs: display-only cells don't need returns
    return


@app.cell(hide_code=True)
def _(alignment_trends):
    # Use the display function - last expression is the output
    display_alignment_trends(alignment_trends, mo)
    return


@app.cell(hide_code=True)
def _():
    mo.md("""
    ## Vintage Cohort Analysis
    """)
    return


@app.cell(hide_code=True)
def _(vintage_distribution):
    """Display vintage cohort overview using the idempotent function"""
    display_vintage_cohort_overview(vintage_distribution, mo, pl)
    return


@app.cell(hide_code=True)
def _(vintage_distribution):
    """Display vintage cohort detailed metrics using the idempotent function"""
    display_vintage_cohort_table(vintage_distribution, mo, pl)
    return


@app.cell(hide_code=True)
def _(most_recent_ym, vintage_df):
    """Calculate office-level vintage distribution"""
    office_vintage_distribution = calculate_office_vintage_distribution(vintage_df, most_recent_ym, pl)
    return (office_vintage_distribution,)


@app.cell(hide_code=True)
def _(office_vintage_distribution):
    """Display office-level vintage cohort distribution"""

    if office_vintage_distribution is not None and len(office_vintage_distribution) > 0:
        # Calculate summary metrics
        _total_benes_vintage = office_vintage_distribution["count"].sum()
        _total_enrolled = office_vintage_distribution["currently_enrolled"].sum()
        _avg_tenure = office_vintage_distribution["avg_total_months"].mean()
        _num_offices_vintage = office_vintage_distribution["office_name"].n_unique()

        # Format the data for display
        office_vintage_data = []
        for _row in office_vintage_distribution.to_dicts():
            office_vintage_data.append({
                "Office Name": _row["office_name"] if _row["office_name"] else "Unknown",
                "Market": _row["office_location"] if _row["office_location"] else "Unknown",
                "Vintage Cohort": _row["vintage_cohort"],
                "Count": f"{_row['count']:,}",
                "Currently Enrolled": f"{_row['currently_enrolled']:,}",
                "% Currently": f"{_row.get('pct_currently_enrolled', 0):.1f}%",
                "% REACH": f"{_row.get('pct_in_reach', 0):.1f}%",
                "% MSSP": f"{_row.get('pct_in_mssp', 0):.1f}%",
                "Avg Total Months": f"{_row.get('avg_total_months', 0):.1f}",
                "% Transitions": f"{_row.get('pct_with_transitions', 0):.1f}%",
            })

        office_vintage_header = mo.md(f"""
        ## 📅 Vintage Cohort Analysis by Office

        ### Enrollment History by Office Location

        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin: 1.5rem 0;">
            <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">Offices Analyzed</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #B45309;">{_num_offices_vintage}</p>
            </div>
            <div style="padding: 1rem; background: #F0FDF4; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #166534; font-size: 0.875rem;">Total Beneficiaries</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #15803D;">{_total_benes_vintage:,}</p>
            </div>
            <div style="padding: 1rem; background: #EFF6FF; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">Currently Enrolled</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #1E3A8A;">{_total_enrolled:,}</p>
                <p style="margin: 0; color: #1E40AF; font-size: 0.75rem;">{(_total_enrolled/_total_benes_vintage*100):.1f}% retention</p>
            </div>
            <div style="padding: 1rem; background: #E0E7FF; border-radius: 8px; text-align: center;">
                <h3 style="margin: 0; color: #5B21B6; font-size: 0.875rem;">Avg Tenure</h3>
                <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #6B21A8;">{_avg_tenure:.1f}</p>
                <p style="margin: 0; color: #5B21B6; font-size: 0.75rem;">months enrolled</p>
            </div>
        </div>

        <div style="padding: 0.75rem 1rem; background: #FEF3C7; border-left: 4px solid #F59E0B; margin: 1rem 0;">
            <p style="margin: 0; color: #92400E; font-size: 0.9rem;">
                <i class="fa-solid fa-history"></i> Beneficiary tenure and program participation patterns by office
            </p>
        </div>
        """)
    return office_vintage_data, office_vintage_header


@app.cell(hide_code=True)
def _():
    """Display technical appendix using the idempotent function"""
    display_technical_appendix(mo)
    return


@app.cell(hide_code=True)
def _(df):
    """Calculate newly added beneficiary statistics"""

    # Check if transition columns exist
    _schema_cols = df.collect_schema().names()
    has_transitions = 'newly_added_2025_to_2026' in _schema_cols

    if not has_transitions:
        newly_added_stats = None
    else:
        # Get newly added beneficiaries
        _newly_added_df = df.filter(
            pl.col('newly_added_2025_to_2026') == True
        )

        # Overall counts
        _total_newly_added = _newly_added_df.select(pl.len()).collect()[0, 0]

        # Source breakdown
        _source_breakdown = (
            _newly_added_df
            .group_by('newly_added_source_2025_to_2026')
            .agg(pl.len().alias('count'))
            .sort('count', descending=True)
            .collect()
        )

        # First REACH month distribution
        _first_month_dist = (
            _newly_added_df
            .group_by('first_reach_month_2026')
            .agg(pl.len().alias('count'))
            .sort('first_reach_month_2026')
            .collect()
        )

        # Office breakdown
        _office_breakdown = (
            _newly_added_df
            .group_by(['office_location', 'newly_added_source_2025_to_2026'])
            .agg(pl.len().alias('count'))
            .collect()
            .pivot(
                on='newly_added_source_2025_to_2026',
                index='office_location',
                values='count',
                aggregate_function='sum'
            )
            .fill_null(0)
            .sort('office_location')
        )

        newly_added_stats = {
            'total': _total_newly_added,
            'source_breakdown': _source_breakdown,
            'first_month_dist': _first_month_dist,
            'office_breakdown': _office_breakdown
        }
    return (newly_added_stats,)


@app.cell(hide_code=True)
def _(newly_added_stats):
    """Format year-over-year newly added beneficiaries for display"""

    # Extract data
    _total = newly_added_stats['total'] if newly_added_stats is not None else 0
    _source_breakdown = (
        newly_added_stats['source_breakdown']
        if newly_added_stats is not None
        else pl.DataFrame({"newly_added_source_2025_to_2026": [], "count": []})
    )
    # Calculate top sources
    _top_3_sources = _source_breakdown.head(3)
    _top_1 = _top_3_sources[0, 'newly_added_source_2025_to_2026'] if len(_top_3_sources) > 0 else "N/A"
    _top_1_count = _top_3_sources[0, 'count'] if len(_top_3_sources) > 0 else 0
    _top_2 = _top_3_sources[1, 'newly_added_source_2025_to_2026'] if len(_top_3_sources) > 1 else "N/A"
    _top_2_count = _top_3_sources[1, 'count'] if len(_top_3_sources) > 1 else 0
    _top_3 = _top_3_sources[2, 'newly_added_source_2025_to_2026'] if len(_top_3_sources) > 2 else "N/A"
    _top_3_count = _top_3_sources[2, 'count'] if len(_top_3_sources) > 2 else 0
    yoy_newly_added_header = mo.md(f"""
    ## ➕ Newly Added Beneficiaries (2025 → 2026)
    ### Overview - {_total:,} New Beneficiaries in 2026
    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; margin: 1.5rem 0;">
        <div style="padding: 1rem; background: #D1FAE5; border-radius: 8px; text-align: center;">
            <h3 style="margin: 0; color: #065F46; font-size: 0.875rem;">{_top_1}</h3>
            <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #047857;">{_top_1_count:,}</p>
            <p style="margin: 0; color: #059669; font-size: 0.75rem;">{_top_1_count/_total*100 if _total > 0 else 0:.1f}% of new beneficiaries</p>
        </div>
        <div style="padding: 1rem; background: #DBEAFE; border-radius: 8px; text-align: center;">
            <h3 style="margin: 0; color: #1E40AF; font-size: 0.875rem;">{_top_2}</h3>
            <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #1E40AF;">{_top_2_count:,}</p>
            <p style="margin: 0; color: #3B82F6; font-size: 0.75rem;">{_top_2_count/_total*100 if _total > 0 else 0:.1f}% of new beneficiaries</p>
        </div>
        <div style="padding: 1rem; background: #FEF3C7; border-radius: 8px; text-align: center;">
            <h3 style="margin: 0; color: #92400E; font-size: 0.875rem;">{_top_3}</h3>
            <p style="margin: 0.5rem 0 0 0; font-size: 1.75rem; font-weight: 700; color: #B45309;">{_top_3_count:,}</p>
            <p style="margin: 0; color: #D97706; font-size: 0.75rem;">{_top_3_count/_total*100 if _total > 0 else 0:.1f}% of new beneficiaries</p>
        </div>
    </div>
    <div style="padding: 0.75rem 1rem; background: #EFF6FF; border-left: 4px solid #3B82F6; margin: 1rem 0;">
        <p style="margin: 0; color: #1E40AF; font-size: 0.875rem;">
            <i class="fa-solid fa-circle-info"></i> Beneficiaries who were <strong>NOT in REACH in December 2025</strong> but <strong>ARE in REACH in January 2026</strong>
        </p>
    </div>
    ### Source Category Breakdown
    """)
    # Source descriptions
    _descriptions = {
        "From MSSP": "Transitioned from MSSP program in December 2025",
        "Pending SVA": "Submitted voluntary alignment but not yet accepted",
        "New Enrollment": "First appeared in REACH data in 2026",
        "Returning (Had SVA)": "Previously in REACH with voluntary alignment",
        "Returning (Claims-based)": "Previously in REACH without voluntary alignment",
        "Other ACO Transfer": "Transferred from another ACO (P2 code)",
        "From MA": "Previously enrolled in Medicare Advantage (E2 code)",
        "SVA Campaign (Current Year)": "2026 SVA campaign with acceptance",
        "SVA Campaign (Previous Year)": "2025 SVA campaign with acceptance",
        "PBVAR Accepted": "Accepted via PBVAR (A0/A1 codes)",
        "Unknown": "Source could not be determined"
    }
    # Format table
    yoy_newly_added_table_data = []
    for row in _source_breakdown.iter_rows(named=True):
        _source = row['newly_added_source_2025_to_2026']
        _count = row['count']
        _pct = (_count / _total * 100) if _total > 0 else 0
        # Add icon based on source
        if 'MSSP' in _source:
            _icon = "🔄"
        elif 'SVA Campaign' in _source:
            _icon = "📝"
        elif 'Pending' in _source:
            _icon = "⏳"
        elif 'Returning' in _source:
            _icon = "↩️"
        elif 'New Enrollment' in _source:
            _icon = "🆕"
        elif 'Transfer' in _source:
            _icon = "🔀"
        elif 'MA' in _source:
            _icon = "🏥"
        elif 'PBVAR' in _source:
            _icon = "✅"
        else:
            _icon = "❓"
        yoy_newly_added_table_data.append({
            "Source Category": f"{_icon} {_source}",
            "Count": f"{_count:,}",
            "Percentage": f"{_pct:.1f}%",
            "Description": _descriptions.get(_source, "")
        })
    return yoy_newly_added_header, yoy_newly_added_table_data


@app.cell(hide_code=True)
def _(yoy_newly_added_header, yoy_newly_added_table_data):
    """Display year-over-year newly added beneficiaries"""
    yoy_newly_added_header
    mo.ui.table(
        yoy_newly_added_table_data,
        selection=None,
        pagination=True,
        page_size=100,
        label="Newly Added Beneficiaries by Source"
    )
    return


@app.cell(hide_code=True)
def _(df, gold_path):
    """Calculate detailed newly aligned and no longer aligned beneficiaries"""

    _schema = df.collect_schema().names()
    # Newly aligned: NOT in REACH Dec 2025, but ARE in REACH Jan 2026
    newly_aligned_beneficiaries = (
        df.filter(
            (pl.col('ym_202512_reach') == False) &
            (pl.col('ym_202601_reach') == True)
        )
        .select([
            pl.col('current_mbi').alias('MBI'),
            pl.col('bene_first_name').alias('First Name'),
            pl.col('bene_last_name').alias('Last Name'),
            pl.col('birth_date').alias('Date of Birth'),
            pl.col('sex').alias('Sex'),
            pl.col('race').alias('Race'),
            pl.col('bene_city').alias('City'),
            pl.col('bene_state').alias('State'),
            pl.col('bene_zip_5').alias('ZIP'),
            pl.col('newly_added_source_2025_to_2026').alias('Source'),
            pl.col('office_location'),
            # Comprehensive notes with patient identifiers and history
            pl.concat_str([
                pl.lit('MBI: '),
                pl.col('current_mbi'),
                pl.lit(' | Previous MBIs: '),
                pl.col('previous_mbi_count').cast(pl.String),
                pl.lit(' | MBI Stability: '),
                pl.col('mbi_stability'),
                pl.lit(' | REACH: '),
                pl.when(pl.col('first_reach_date').is_not_null())
                .then(pl.col('first_reach_date').cast(pl.String))
                .otherwise(pl.lit('Never')),
                pl.lit(' to '),
                pl.when(pl.col('last_reach_date').is_not_null())
                .then(pl.col('last_reach_date').cast(pl.String))
                .otherwise(pl.lit('Present')),
                pl.lit(' ('),
                pl.col('months_in_reach').cast(pl.String),
                pl.lit(' months) | MSSP: '),
                pl.when(pl.col('first_mssp_date').is_not_null())
                .then(pl.col('first_mssp_date').cast(pl.String))
                .otherwise(pl.lit('Never')),
                pl.lit(' to '),
                pl.when(pl.col('last_mssp_date').is_not_null())
                .then(pl.col('last_mssp_date').cast(pl.String))
                .otherwise(pl.lit('N/A')),
                pl.lit(' ('),
                pl.col('months_in_mssp').cast(pl.String),
                pl.lit(' months) | SVA: First '),
                pl.when(pl.col('first_sva_submission_date').is_not_null())
                .then(pl.col('first_sva_submission_date').cast(pl.String))
                .otherwise(pl.lit('None')),
                pl.lit(', Last '),
                pl.when(pl.col('last_sva_submission_date').is_not_null())
                .then(pl.col('last_sva_submission_date').cast(pl.String))
                .otherwise(pl.lit('None')),
                pl.lit(', Expires '),
                pl.when(pl.col('last_signature_expiry_date').is_not_null())
                .then(pl.col('last_signature_expiry_date').cast(pl.String))
                .otherwise(pl.lit('N/A'))
            ], separator='').alias('Comprehensive Notes')
        ])
        .collect()
    )

    # Get no longer aligned base data
    no_longer_aligned_base = df.filter(
        (pl.col('ym_202512_reach') == True) &
        (pl.col('ym_202601_reach') == False)
    ).collect()

    # Load medical claims and calculate hospice spend + E&M visits for 2025
    from acoharmony._expressions._utilization import UtilizationExpression

    medical_claims_path = gold_path / "medical_claim.parquet"
    claims_2025 = pl.scan_parquet(str(medical_claims_path)).filter(
        pl.col('claim_start_date').dt.year() == 2025
    )

    # Hospice spend
    hospice_spend_2025 = (
        claims_2025
        .filter(
            pl.col('bill_type_code').str.starts_with('81') |
            pl.col('bill_type_code').str.starts_with('82')
        )
        .group_by('member_id')
        .agg(pl.col('paid_amount').sum().alias('hospice_spend_2025'))
    )

    # E&M visits using existing expression
    em_visits_2025 = (
        claims_2025
        .with_columns([
            UtilizationExpression.is_em_visit().alias('is_em')
        ])
        .filter(pl.col('is_em') == 1)
        .group_by('member_id')
        .agg(pl.len().alias('em_visits_2025'))
    )

    # Join hospice spend and E&M visits with no longer aligned
    no_longer_aligned_with_claims = (
        no_longer_aligned_base.lazy()
        .join(hospice_spend_2025, left_on='current_mbi', right_on='member_id', how='left')
        .join(em_visits_2025, left_on='current_mbi', right_on='member_id', how='left')
        .with_columns([
            pl.col('hospice_spend_2025').fill_null(0),
            pl.col('em_visits_2025').fill_null(0)
        ])
        .collect()
    )

    # Also join E&M for newly aligned
    newly_aligned_with_em = (
        newly_aligned_beneficiaries.lazy()
        .join(em_visits_2025, left_on='MBI', right_on='member_id', how='left')
        .with_columns([
            pl.col('em_visits_2025').fill_null(0)
        ])
        .collect()
    )

    # Format newly aligned with E&M
    newly_aligned_beneficiaries = newly_aligned_with_em.select([
        pl.col('MBI'),
        pl.col('First Name'),
        pl.col('Last Name'),
        pl.col('Date of Birth'),
        pl.col('Sex'),
        pl.col('Race'),
        pl.col('City'),
        pl.col('State'),
        pl.col('ZIP'),
        pl.col('Source'),
        # Office closing flag for Indiana/Kentucky offices
        pl.col('office_location').is_in(['South Bend / Indianapolis', 'Lexington']).fill_null(False).alias('Office Closing'),
        # Update comprehensive notes to include E&M
        (pl.col('Comprehensive Notes') + pl.lit(' | E&M Visits 2025: ') + pl.col('em_visits_2025').cast(pl.String)).alias('Comprehensive Notes')
    ])

    # Format for display
    no_longer_aligned_beneficiaries = no_longer_aligned_with_claims.select([
        pl.col('current_mbi').alias('MBI'),
        pl.col('bene_first_name').alias('First Name'),
        pl.col('bene_last_name').alias('Last Name'),
        pl.col('birth_date').alias('Date of Birth'),
        pl.col('sex').alias('Sex'),
        pl.col('race').alias('Race'),
        pl.col('bene_city').alias('City'),
        pl.col('bene_state').alias('State'),
        pl.col('bene_zip_5').alias('ZIP'),
        # Individual flag columns for each reason
        pl.col('expired_sva_2025').fill_null(False).alias('Expired SVA'),
        pl.col('lost_provider_2025').fill_null(False).alias('Lost Aligned Provider'),
        pl.col('moved_ma_2025').fill_null(False).alias('Moved to MA'),
        pl.col('ym_202601_mssp').fill_null(False).alias('Moved to MSSP'),
        # Hospice flag
        (pl.col('hospice_spend_2025') > 0).alias('Hospice'),
        # Office closing flag for Indiana/Kentucky offices
        pl.col('office_location').is_in(['South Bend / Indianapolis', 'Lexington']).fill_null(False).alias('Office Closing'),
        # Determine primary reason for unalignment (mutually exclusive categories)
        pl.when(
            pl.col('expired_sva_2025').fill_null(False) &
            pl.col('lost_provider_2025').fill_null(False)
        )
        .then(pl.lit('Expired SVA | Lost Aligned Provider'))
        .when(pl.col('expired_sva_2025').fill_null(False))
        .then(pl.lit('Expired SVA'))
        .when(pl.col('lost_provider_2025').fill_null(False))
        .then(pl.lit('Lost Aligned Provider'))
        .when(pl.col('moved_ma_2025').fill_null(False))
        .then(pl.lit('Moved to MA'))
        .when(pl.col('ym_202601_mssp').fill_null(False))
        .then(pl.lit('Moved to MSSP'))
        .otherwise(pl.lit('Unresolved'))
        .alias('Reason for Unalignment'),
        # Comprehensive notes with patient identifiers and history
        pl.concat_str([
            pl.lit('MBI: '),
            pl.col('current_mbi'),
            pl.lit(' | Previous MBIs: '),
            pl.col('previous_mbi_count').cast(pl.String),
            pl.lit(' | MBI Stability: '),
            pl.col('mbi_stability'),
            pl.lit(' | REACH: '),
            pl.when(pl.col('first_reach_date').is_not_null())
            .then(pl.col('first_reach_date').cast(pl.String))
            .otherwise(pl.lit('Never')),
            pl.lit(' to '),
            pl.when(pl.col('last_reach_date').is_not_null())
            .then(pl.col('last_reach_date').cast(pl.String))
            .otherwise(pl.lit('N/A')),
            pl.lit(' ('),
            pl.col('months_in_reach').cast(pl.String),
            pl.lit(' months) | MSSP: '),
            pl.when(pl.col('first_mssp_date').is_not_null())
            .then(pl.col('first_mssp_date').cast(pl.String))
            .otherwise(pl.lit('Never')),
            pl.lit(' to '),
            pl.when(pl.col('last_mssp_date').is_not_null())
            .then(pl.col('last_mssp_date').cast(pl.String))
            .otherwise(pl.lit('N/A')),
            pl.lit(' ('),
            pl.col('months_in_mssp').cast(pl.String),
            pl.lit(' months) | SVA: First '),
            pl.when(pl.col('first_sva_submission_date').is_not_null())
            .then(pl.col('first_sva_submission_date').cast(pl.String))
            .otherwise(pl.lit('None')),
            pl.lit(', Last '),
            pl.when(pl.col('last_sva_submission_date').is_not_null())
            .then(pl.col('last_sva_submission_date').cast(pl.String))
            .otherwise(pl.lit('None')),
            pl.lit(', Expires '),
            pl.when(pl.col('last_signature_expiry_date').is_not_null())
            .then(pl.col('last_signature_expiry_date').cast(pl.String))
            .otherwise(pl.lit('N/A')),
            pl.lit(' | Hospice Spend 2025: $'),
            pl.col('hospice_spend_2025').cast(pl.String),
            pl.lit(' | E&M Visits 2025: '),
            pl.col('em_visits_2025').cast(pl.String)
        ], separator='').alias('Comprehensive Notes')
    ])
    return newly_aligned_beneficiaries, no_longer_aligned_beneficiaries


@app.cell(hide_code=True)
def _(newly_aligned_beneficiaries, no_longer_aligned_beneficiaries):
    """Format detailed beneficiary tables for display"""
    newly_aligned_detail_header = mo.md(f"""
    ### 📋 Detailed List - Newly Aligned Beneficiaries
    **{len(newly_aligned_beneficiaries):,} beneficiaries** newly aligned to REACH in January 2026
    """)
    newly_aligned_detail_data = newly_aligned_beneficiaries
    no_longer_aligned_detail_header = mo.md(f"""
    ### 📋 Detailed List - No Longer Aligned Beneficiaries
    **{len(no_longer_aligned_beneficiaries):,} beneficiaries** who left REACH between December 2025 and January 2026
    """)
    no_longer_aligned_detail_data = no_longer_aligned_beneficiaries
    return (
        newly_aligned_detail_data,
        newly_aligned_detail_header,
        no_longer_aligned_detail_data,
        no_longer_aligned_detail_header,
    )


@app.cell(hide_code=True)
def _(newly_aligned_detail_data, newly_aligned_detail_header):
    """Display newly aligned beneficiaries detail table"""
    mo.vstack([
        newly_aligned_detail_header,
        mo.ui.table(
            newly_aligned_detail_data,
            selection=None,
            pagination=True,
            page_size=25,
            label="Newly Aligned Beneficiaries - Detail"
        )
    ])
    return


@app.cell(hide_code=True)
def _(no_longer_aligned_detail_data, no_longer_aligned_detail_header):
    """Display no longer aligned beneficiaries detail table"""
    mo.vstack([
        no_longer_aligned_detail_header,
        mo.ui.table(
            no_longer_aligned_detail_data,
            selection=None,
            pagination=True,
            page_size=25,
            label="No Longer Aligned Beneficiaries - Detail"
        )
    ])
    return


@app.cell(hide_code=True)
def _():
    """Display HarmonyCares branded footer"""
    mo.md("""
    ---
    <div style="background: linear-gradient(135deg, #1E3A8A 0%, #3B82F6 100%);
                padding: 2rem;
                border-radius: 12px;
                margin-top: 3rem;
                text-align: center;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
        <div style="display: flex; align-items: center; justify-content: center; gap: 1rem; margin-bottom: 1rem;">
            <img src="https://harmonycaresaco.com/img/logo.svg"
                 alt="HarmonyCares Logo"
                 style="height: 50px; filter: brightness(0) invert(1);"
                 onerror="this.style.display='none'">
            <div style="border-left: 2px solid #60A5FA; height: 50px;"></div>
            <div style="text-align: left;">
                <p style="margin: 0; font-weight: 700; font-size: 1.125rem; color: #ffffff;">
                    HarmonyCares ACO
                </p>
                <p style="margin: 0; color: #E5E7EB; font-size: 0.875rem;">
                    Data Platform & Analytics
                </p>
            </div>
        </div>

        <div style="border-top: 1px solid #4B5563; padding-top: 1.5rem; margin-top: 1.5rem;">
            <p style="margin: 0 0 0.5rem 0; color: #E5E7EB; font-size: 0.875rem;">
                <i class="fa-solid fa-database" style="color: #60A5FA;"></i>
                <strong>Data Source:</strong> Gold Layer (Medallion Architecture)
            </p>
            <p style="margin: 0; color: #D1D5DB; font-size: 0.75rem;">
                <i class="fa-solid fa-folder-open"></i> /opt/s3/data/workspace/gold/ •
                <i class="fa-solid fa-file"></i> consolidated_alignment.parquet
            </p>
        </div>

        <div style="margin-top: 1.5rem; padding-top: 1.5rem; border-top: 1px solid #4B5563;">
            <p style="margin: 0; color: #D1D5DB; font-size: 0.75rem;">
                © 2025 HarmonyCares. Built with
                <i class="fa-solid fa-heart" style="color: #EF4444;"></i>
                using ACOHarmony Platform
            </p>
        </div>
    </div>
    """)
    return


@app.cell(hide_code=True)
def _(
    curr_deceased,
    curr_ffs,
    curr_living,
    curr_month_display,
    curr_mssp,
    curr_reach,
    curr_total,
    curr_unknown,
    outreach_metrics,
):
    """Debug cell: Return JSON summary of all tile variables"""
    import json

    debug_summary = {
        "current_enrollment_status": {
            "reach": curr_reach,
            "mssp": curr_mssp,
            "ffs": curr_ffs,
            "deceased": curr_deceased,
            "unknown": curr_unknown,
            "total_living": curr_living,
            "total_beneficiaries": curr_total,
            "month": curr_month_display,
            "validation": {
                "total_equals_sum": curr_total == (curr_reach + curr_mssp + curr_ffs + curr_deceased + curr_unknown),
                "living_equals_sum": curr_living == (curr_reach + curr_mssp + curr_ffs + curr_unknown),
            }
        },
        "outreach_metrics": {
            "total_population": outreach_metrics.get("total_population"),
            "total_contacted": outreach_metrics.get("total_contacted"),
            "total_with_valid_sva": outreach_metrics.get("total_with_valid_sva"),
            "contacted_to_sva_rate": outreach_metrics.get("contacted_to_sva_rate"),
            "email_opened_to_sva_rate": outreach_metrics.get("email_opened_to_sva_rate"),
            "email_clicked_to_sva_rate": outreach_metrics.get("email_clicked_to_sva_rate"),
            "not_contacted_sva_rate": outreach_metrics.get("not_contacted_sva_rate"),
            "current_metrics": outreach_metrics.get("current_metrics"),
        }
    }

    print("=" * 80)
    print("DEBUG SUMMARY - All Tile Variables")
    print("=" * 80)
    print(json.dumps(debug_summary, indent=2))
    print("=" * 80)
    return


if __name__ == "__main__":
    app.run()
