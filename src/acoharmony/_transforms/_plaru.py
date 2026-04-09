# © 2025 HarmonyCares
# All rights reserved.

"""
PLARU (Preliminary Alternative Payment Arrangement Report Unredacted) transform.

WHY THIS EXISTS
===============
PLARU reports from CMS REACH contain payment calculations across 9 sheets with different
structures. Parser outputs a single LazyFrame with all sheets (via _output_table partition).
This transform applies sheet-specific expression logic to clean and normalize each table type.

Without these transforms, the parquet outputs would have:
- Generic column names (column_1, column_2) instead of semantic names
- Un-pivoted metadata (vertical instead of horizontal)
- Multi-row headers uncombined (separate parent/child rows)
- Mixed data types from Excel type inference

PROBLEM SOLVED
==============
Parser gives us raw structure. Transforms apply business logic:

  report_parameters: column_1="REACH ACO Identifier", column_2="D0259"
  → AFTER pivot transform → reach_aco_identifier="D0259" (single row)

  payment_history: Row 4 has headers ["Payment Date", "Base PCC Total", ...]
  → AFTER header extraction → payment_date, base_pcc_total columns

  base_pcc_pmt_detailed: Row 4=["Jan", "Feb"], Row 5=["Amount", "Amount"]
  → AFTER multi-level header → jan_amount, feb_amount columns

Result: Queryable parquet files with proper column names and normalized structure.

What is PLARU?
==============
PLARU is a CMS REACH report containing:
- Report parameters (ACO info, performance period)
- Payment history (monthly payment amounts)
- Payment calculation details (Base PCC, Enhanced PCC, APO)
- Claims/provider data (aggregated metrics)

Sheet-Specific Transformations
===============================

Sheet 0: report_parameters

Layout: Long-format key-value pairs
Transform: KeyValuePivotExpression
Result: Single-row metadata table

Sheet 1: payment_history

Layout: Standard table with header row
Transform: Standard parsing (no transform needed)
Result: Payment history table

Sheet 2: base_pcc_pmt_detailed

Layout: Multi-level headers with dynamic columns
Transform: MultiLevelHeaderExpression or DynamicMetaDetectExpression
Result: Detailed payment calculation table

Sheets 3-8: Various calculation sheets

Layouts: Mix of standard tables and multi-level headers
Transforms: Applied based on layout detection
"""

import polars as pl
from pydantic import BaseModel, Field

from .._decor8 import transform, transform_method
from .._expressions._append_detect import AppendDetectConfig, AppendDetectExpression
from .._expressions._dynamic_meta_detect import (
    DynamicMetaConfig,
    DynamicMetaDetectExpression,
)
from .._expressions._matrix_extractor import (
    MatrixExtractor,
    MatrixExtractorConfig,
)
from .._expressions._multi_level_header import (
    MultiLevelHeaderConfig,
    MultiLevelHeaderExpression,
)
from .._expressions._table_layout_detector import (
    TableLayout,
    TableLayoutDetector,
)
from .._trace import TracerWrapper
from ._key_value_pivot import (
    KeyValuePivotConfig,
    KeyValuePivotExpression,
)
from ._registry import TransformRegistry

tracer = TracerWrapper("transform.plaru")


@transform(name="clean_plaru_sheet", tier=["silver"], sql_enabled=False)
@transform_method(enable_composition=True, threshold=1.0)
def clean_plaru_sheet(df: pl.LazyFrame, sheet_type: str, sheet_config: dict) -> pl.LazyFrame:
    """
    Clean and transform PLARU sheet data for analytics using schema definitions.

        This function:
        - Drops columns specified in schema (Excel metadata)
        - Casts columns to types defined in schema
        - Renames columns as specified in schema
        - Removes rows that are just labels/headers
        - Keeps only data rows suitable for ML/analytics

        Parameters

        df : pl.LazyFrame
            Transformed dataframe with proper column names
        sheet_type : str
            Type of sheet (e.g., 'base_pcc_pmt_detailed')
        sheet_config : dict
            Sheet configuration from schema with silver_columns, drop_columns, etc.

        Returns

        pl.LazyFrame
            Cleaned, analytics-ready dataframe
    """
    df_collected = df.collect()
    drop_columns = sheet_config.get("drop_columns") or []
    cols_to_drop = [col for col in drop_columns if col in df_collected.columns]
    if cols_to_drop:
        df_collected = df_collected.drop(cols_to_drop)

    silver_columns = sheet_config.get("silver_columns") or []
    if silver_columns:
        rename_map = {}
        type_map = {}

        for col_def in silver_columns:
            col_name = col_def.get("name")
            source_name = col_def.get("source_name")
            data_type = col_def.get("data_type")

            if source_name and source_name in df_collected.columns:
                rename_map[source_name] = col_name

            if data_type == "float":
                type_map[col_name] = pl.Float64
            elif data_type == "integer":
                type_map[col_name] = pl.Int64
            elif data_type == "string":
                type_map[col_name] = pl.Utf8

        if rename_map:
            df_collected = df_collected.rename(rename_map)

        cast_expressions = []
        for col in df_collected.columns:
            if col in type_map:
                cast_expressions.append(pl.col(col).cast(type_map[col], strict=False).alias(col))
            else:
                cast_expressions.append(pl.col(col))

        if cast_expressions:
            df_collected = df_collected.select(cast_expressions)
    else:
        pass

    if "metric" in df_collected.columns:
        df_collected = df_collected.filter(
            pl.col("metric").is_not_null() & (pl.col("metric") != "")
        )

    return df_collected.lazy()


class PLARUSheetConfig(BaseModel):
    """
    Configuration for processing a single PLARU sheet.

        Attributes

        sheet_index : int
            0-indexed sheet number

        sheet_type : str
            Logical name for the sheet (e.g., "report_parameters")

        transform_type : str
            Type of transform to apply
            Options: "key_value_pivot", "multi_level_header", "matrix_extractor",
                    "dynamic_meta_detect", "append_detect", "standard"

        pivot_config : Optional[KeyValuePivotConfig]
            Config for key-value pivot (if transform_type = "key_value_pivot")

        header_config : Optional[MultiLevelHeaderConfig]
            Config for multi-level headers (if transform_type = "multi_level_header")

        matrix_config : Optional[MatrixExtractorConfig]
            Config for matrix extraction (if transform_type = "matrix_extractor")

        meta_config : Optional[DynamicMetaConfig]
            Config for dynamic meta-detection (if transform_type = "dynamic_meta_detect")

        append_config : Optional[AppendDetectConfig]
            Config for append-detection (if transform_type = "append_detect")

        auto_detect : bool
            Whether to auto-detect layout if no config provided
            Default: False
    """

    sheet_index: int = Field(description="0-indexed sheet number")
    sheet_type: str = Field(description="Logical sheet name")
    transform_type: str = Field(description="Transform type to apply", default="standard")
    pivot_config: KeyValuePivotConfig | None = None
    header_config: MultiLevelHeaderConfig | None = None
    meta_config: DynamicMetaConfig | None = None
    append_config: AppendDetectConfig | None = None
    matrix_config: MatrixExtractorConfig | None = None
    auto_detect: bool = Field(default=False, description="Auto-detect layout")
    data_start_row: int | None = Field(
        default=None, description="Row where data starts (after headers)"
    )
    silver_columns: list[dict] | None = Field(
        default=None, description="Silver tier column definitions from schema"
    )
    drop_columns: list[str] | None = Field(
        default=None, description="Columns to drop in silver tier"
    )


@transform(name="process_plaru_sheet", tier=["silver"], sql_enabled=False)
@TransformRegistry.register("plaru", name="plaru_sheet_processor")
@transform_method(
    enable_composition=True,
    threshold=10.0,
)
def process_plaru_sheet(df: pl.LazyFrame, config: PLARUSheetConfig) -> pl.LazyFrame:
    """
    Process a PLARU sheet based on its configuration.

        WHY: Applies sheet-specific expression logic to normalize column names and structure.

        Parameters

        df : pl.LazyFrame
            Input LazyFrame from parser

        config : PLARUSheetConfig
            Sheet-specific configuration

        Returns

        pl.LazyFrame
    """
    # Auto-detect layout if configured
    if config.auto_detect and config.transform_type == "standard":
        df_sample = df.collect()
        layout = TableLayoutDetector.detect(df_sample)

        if layout == TableLayout.LONG_KEY_VALUE:
            config.transform_type = "key_value_pivot"
            config.pivot_config = KeyValuePivotConfig()
        elif layout == TableLayout.MULTI_LEVEL_HEADER:
            config.transform_type = "multi_level_header"
            # Would need additional config for header rows
        df = df_sample.lazy()

    # Apply transform based on type
    if config.transform_type == "key_value_pivot":
        if not config.pivot_config:
            raise ValueError("pivot_config required for key_value_pivot transform")

        df_collected = df.collect()
        pivoted = KeyValuePivotExpression.build(df_collected, config.pivot_config)
        return pivoted

    elif config.transform_type == "multi_level_header":
        if not config.header_config:
            raise ValueError("header_config required for multi_level_header transform")

        tracking_columns = [
            "processed_at",
            "source_file",
            "source_filename",
            "file_date",
            "sheet_type",
            "_output_table",
        ]

        df_collected = df.collect()
        existing_tracking = [col for col in tracking_columns if col in df_collected.columns]
        data_columns = [col for col in df_collected.columns if col not in tracking_columns]

        if existing_tracking:
            df_tracking = df_collected.select(existing_tracking)
            df_data = df_collected.select(data_columns).lazy()
            df_transformed = MultiLevelHeaderExpression.apply(
                df_data, config.header_config, data_start_row=config.data_start_row
            )
            df_transformed_collected = df_transformed.collect()
            if len(df_tracking) != len(df_transformed_collected):
                df_tracking = df_tracking.slice(len(df_tracking) - len(df_transformed_collected))
            df_result = pl.concat([df_transformed_collected, df_tracking], how="horizontal")
            sheet_cfg = {
                "silver_columns": config.silver_columns,
                "drop_columns": config.drop_columns,
            }
            df_cleaned = clean_plaru_sheet(df_result.lazy(), config.sheet_type, sheet_cfg)
            return df_cleaned
        else:
            df_transformed = MultiLevelHeaderExpression.apply(
                df, config.header_config, data_start_row=config.data_start_row
            )
            sheet_cfg = {
                "silver_columns": config.silver_columns,
                "drop_columns": config.drop_columns,
            }
            df_cleaned = clean_plaru_sheet(df_transformed, config.sheet_type, sheet_cfg)
            return df_cleaned

    elif config.transform_type == "matrix_extractor":
        if not config.matrix_config:
            raise ValueError("matrix_config required for matrix_extractor transform")

        tracking_columns = [
            "processed_at",
            "source_file",
            "source_filename",
            "file_date",
            "sheet_type",
            "_output_table",
        ]

        df_collected = df.collect()
        existing_tracking = [col for col in tracking_columns if col in df_collected.columns]
        data_columns = [col for col in df_collected.columns if col not in tracking_columns]

        df_tracking = None
        if existing_tracking:
            df_tracking = df_collected.select(existing_tracking)

        df_data = df_collected.select(data_columns)

        extractor = MatrixExtractor(config.matrix_config)
        result_dfs = extractor.extract(df_data)

        if result_dfs:
            df_result = pl.concat(result_dfs, how="diagonal")

            if df_tracking is not None and len(df_tracking) > 0:
                df_tracking_single = df_tracking.head(1)
                df_tracking_repeated = pl.concat(
                    [df_tracking_single] * len(df_result), how="vertical"
                )
                df_result = pl.concat([df_result, df_tracking_repeated], how="horizontal")
            return df_result.lazy()
        else:
            if df_tracking is not None:
                return df_tracking.lazy()
            else:
                return pl.DataFrame().lazy()

    elif config.transform_type == "dynamic_meta_detect":
        if not config.meta_config:
            raise ValueError("meta_config required for dynamic_meta_detect transform")

        result_lazy, metadata = DynamicMetaDetectExpression.apply(df, config.meta_config)
        return result_lazy

    elif config.transform_type == "append_detect":
        if not config.append_config:
            raise ValueError("append_config required for append_detect transform")
        df_collected = df.collect()
        AppendDetectExpression.apply(df_collected, config.append_config)
        return df_collected.lazy()

    elif config.transform_type == "standard":
        sheet_cfg = {"silver_columns": config.silver_columns, "drop_columns": config.drop_columns}
        df_cleaned = clean_plaru_sheet(df, config.sheet_type, sheet_cfg)
        return df_cleaned

    else:
        raise ValueError(f"Unknown transform_type: {config.transform_type}")

PLARU_SHEET_CONFIGS = {
    0: PLARUSheetConfig(
        sheet_index=0,
        sheet_type="report_parameters",
        transform_type="key_value_pivot",
        pivot_config=KeyValuePivotConfig(
            key_column="column_1",  # First column (Excel uses 1-based indexing)
            value_column="column_2",  # Second column
            skip_empty_values=True,
            sanitize_keys=True,
        ),
    ),
    1: PLARUSheetConfig(
        sheet_index=1,
        sheet_type="payment_history",
        transform_type="standard",  # Standard table with header row 4
    ),
    2: PLARUSheetConfig(
        sheet_index=2,
        sheet_type="base_pcc_pmt_detailed",
        transform_type="multi_level_header",
        header_config=MultiLevelHeaderConfig(
            header_rows=[4, 5],  # Rows 4-5 are headers
            separator="_",
            skip_empty_parts=True,
            sanitize_names=True,
        ),
    ),
    3: PLARUSheetConfig(
        sheet_index=3,
        sheet_type="enhanced_pcc_pmt_detailed",
        transform_type="multi_level_header",
        header_config=MultiLevelHeaderConfig(
            header_rows=[4, 5],  # Rows 4-5 are headers
            separator="_",
            skip_empty_parts=True,
            sanitize_names=True,
        ),
    ),
    4: PLARUSheetConfig(
        sheet_index=4,
        sheet_type="base_pcc_pct",
        transform_type="multi_level_header",
        header_config=MultiLevelHeaderConfig(
            header_rows=[5, 6],  # Rows 5-6 are headers
            separator="_",
            skip_empty_parts=True,
            sanitize_names=True,
        ),
    ),
    5: PLARUSheetConfig(
        sheet_index=5,
        sheet_type="enhanced_pcc_pct_ceil",
        transform_type="multi_level_header",
        header_config=MultiLevelHeaderConfig(
            header_rows=[5, 6],  # Rows 5-6 are headers
            separator="_",
            skip_empty_parts=True,
            sanitize_names=True,
        ),
    ),
    6: PLARUSheetConfig(
        sheet_index=6,
        sheet_type="apo_pmt_detailed",
        transform_type="multi_level_header",
        header_config=MultiLevelHeaderConfig(
            header_rows=[4, 5],  # Rows 4-5 are headers
            separator="_",
            skip_empty_parts=True,
            sanitize_names=True,
        ),
    ),
    7: PLARUSheetConfig(
        sheet_index=7,
        sheet_type="apo_pbpm",
        transform_type="multi_level_header",
        header_config=MultiLevelHeaderConfig(
            header_rows=[5, 6],  # Rows 5-6 are headers
            separator="_",
            skip_empty_parts=True,
            sanitize_names=True,
        ),
    ),
    8: PLARUSheetConfig(
        sheet_index=8,
        sheet_type="data_claims_prvdr",
        transform_type="standard",  # Standard table with header row 0
    ),
}


@transform(name="plaru_workbook", tier=["silver"], sql_enabled=False)
@TransformRegistry.register("plaru", name="plaru_full_transform")
@transform_method(
    enable_composition=True,
    threshold=30.0,
)
def transform_plaru_workbook(
    sheets: dict[int, pl.LazyFrame], configs: dict[int, PLARUSheetConfig] | None = None
) -> dict[str, pl.LazyFrame]:
    """
    Transform all PLARU sheets in a workbook.

        Parameters

        sheets : dict[int, pl.LazyFrame]
            Dictionary mapping sheet index to LazyFrame

        configs : Optional[dict[int, PLARUSheetConfig]]
            Sheet configurations. If None, uses PLARU_SHEET_CONFIGS.

        Returns

        dict[str, pl.LazyFrame]
            Dictionary mapping sheet_type to transformed LazyFrame
    """
    if configs is None:
        configs = PLARU_SHEET_CONFIGS

    transformed = {}

    for sheet_idx, df_lazy in sheets.items():
        if sheet_idx not in configs:
            # Skip unconfigured sheets
            continue

        config = configs[sheet_idx]

        try:
            # Apply transformation
            transformed_df = process_plaru_sheet(df_lazy, config)
            transformed[config.sheet_type] = transformed_df

        except Exception as e:  # ALLOWED: Continues processing remaining items despite error
            print(f"Error processing sheet {sheet_idx} ({config.sheet_type}): {e}")
            transformed[config.sheet_type] = df_lazy

    return transformed
