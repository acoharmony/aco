# © 2025 HarmonyCares
# All rights reserved.

"""
Matrix-based data extraction for complex Excel layouts.

WHY THIS EXISTS
===============
Healthcare Excel reports (PLARU, PALMR, BNMR) contain multiple data matrices
within a single sheet, where:
1. Each matrix has its own dimensions (rows × columns)
2. Supporting labels are OFFSET from the matrix (left/above)
3. Label combinations from different offsets form field names
4. "Breaks" (empty rows/columns) separate distinct matrices

PROBLEM SOLVED
==============
Instead of assuming a single table with headers at the top, this recognizes
that a worksheet contains multiple independent data matrices, each with its
own context labels at various offsets.

Example structure:
```
         A              B                    C        D        E        F
1
2                                      Projected Experience
3                                         Jan      Feb      Mar
4   Row Label    Prospective Payment      123      456      789
5                Calculation
6
7   Another      Eligible Months
8   Section      AD Claims Aligned         100      200      300
```

The matrix D4:F4 should extract:
- D4: prospective_payment_calculation_projected_experience_jan = 123
- E4: prospective_payment_calculation_projected_experience_feb = 456
- F4: prospective_payment_calculation_projected_experience_mar = 789

Core Concepts

**Data Matrix**: A rectangular region containing actual data values
  - Detected by finding continuous non-empty cells
  - Bounded by empty rows/columns (breaks)

**Label Offsets**: Supporting information at fixed offsets from the matrix
  - row_label_offset: Columns to the left (e.g., -2 = 2 columns left)
  - col_label_offset: Rows above (e.g., -1 = 1 row above)

**Label Hierarchy**: Multiple label sources combine to form field names
  - Column headers (offset above)
  - Row headers (offset left)
  - Parent headers (further offsets for spanning labels)

**Break Detection**: Identifying matrix boundaries
  - Empty rows/columns
  - Pattern changes (text vs numbers)
  - Specific marker values

Configuration Schema

```yaml
matrix_extraction:
  # How to detect matrices
  detection:
    min_rows: 2              # Minimum rows for a valid matrix
    min_cols: 2              # Minimum columns for a valid matrix
    break_rows: 1            # Empty rows needed to separate matrices
    break_cols: 1            # Empty columns needed to separate matrices

  # Where to find labels relative to matrix
  label_offsets:
    # Column labels (above the matrix)
    column_labels:
      - offset: -2           # 2 rows above matrix (spanning parent)
        forward_fill: true   # Fill None values from left
      - offset: -1           # 1 row above matrix (direct header)
        forward_fill: false

    # Row labels (left of the matrix)
    row_labels:
      - offset: -2           # 2 columns left (category)
        forward_fill: true
      - offset: -1           # 1 column left (subcategory)
        forward_fill: false

  # How to combine labels
  naming:
    separator: "_"           # Join character
    skip_empty: true         # Skip None/empty parts
    sanitize: true           # Clean and lowercase
    order: ["row", "col"]    # Order to combine labels
```
"""

from dataclasses import dataclass
from typing import Literal

import polars as pl
from pydantic import BaseModel, Field

from .._trace import TracerWrapper
from ._registry import register_expression

tracer = TracerWrapper("expression.matrix_extractor")


class LabelOffset(BaseModel):
    """
    Configuration for a single label source at a specific offset.

        Attributes

        offset : int
            Offset from the matrix edge (negative = above/left)
            For column labels: -1 = 1 row above, -2 = 2 rows above
            For row labels: -1 = 1 column left, -2 = 2 columns left

        forward_fill : bool
            Whether to forward-fill None/empty values
            Used for spanning headers that apply to multiple cells
            Default: False

        required : bool
            Whether this label level is required (vs optional)
            Default: True
    """

    offset: int = Field(description="Offset from matrix edge (negative)")
    forward_fill: bool = Field(default=False, description="Forward-fill None values")
    required: bool = Field(default=True, description="Label is required")


class SectionDetectionConfig(BaseModel):
    """
    Configuration for detecting hierarchical sections within data.

        Attributes

        section_column : int
            Column index containing section headers (e.g., Column B = index 1)
            Default: 1

        section_markers : list[str]
            Keywords that indicate a row is a section header
            Example: ["Total", "Projected", "Payment"]
            Default: []

        aggregation_markers : list[str]
            Keywords that indicate a row is an aggregation/summary
            Example: ["Total", "Average", "Sum"]
            Default: ["Total"]

        hierarchy_depth : int
            Maximum depth of section nesting
            Example: 2 = parent section + sub-section
            Default: 2

        detect_bold : bool
            Whether to use bold formatting to detect section headers
            (Requires reading Excel formatting, may not work with CSV)
            Default: False
    """

    section_column: int = Field(default=1, description="Column with section headers")
    section_markers: list[str] = Field(default_factory=list, description="Section header keywords")
    aggregation_markers: list[str] = Field(
        default_factory=lambda: ["Total"], description="Aggregation row keywords"
    )
    hierarchy_depth: int = Field(default=2, description="Max section nesting depth")
    detect_bold: bool = Field(default=False, description="Use bold formatting for detection")


class DetectionConfig(BaseModel):
    r"""
    Configuration for detecting data matrices.

        Attributes

        min_rows : int
            Minimum rows for a valid matrix
            Default: 2

        min_cols : int
            Minimum columns for a valid matrix
            Default: 2

        break_rows : int
            Number of empty rows needed to separate matrices
            Default: 1

        break_cols : int
            Number of empty columns needed to separate matrices
            Default: 1

        value_pattern : str | None
            Regex pattern that data values must match (optional)
            Example: r"^\d+\.?\d*$" for numbers only
            Default: None

        section_detection : SectionDetectionConfig | None
            Configuration for hierarchical section detection
            If provided, will detect and include section context in labels
            Default: None
    """

    min_rows: int = Field(default=2, description="Minimum matrix rows")
    min_cols: int = Field(default=2, description="Minimum matrix columns")
    break_rows: int = Field(default=1, description="Empty rows to separate matrices")
    break_cols: int = Field(default=1, description="Empty columns to separate matrices")
    value_pattern: str | None = Field(default=None, description="Regex for data values")
    section_detection: SectionDetectionConfig | None = Field(
        default=None, description="Section hierarchy detection"
    )


class LabelOffsetsConfig(BaseModel):
    """
    Configuration for all label sources.

        Attributes

        column_labels : list[LabelOffset]
            Label sources above the matrix (row offsets)
            Ordered from furthest to closest (e.g., [-2, -1])

        row_labels : list[LabelOffset]
            Label sources left of the matrix (column offsets)
            Ordered from furthest to closest (e.g., [-2, -1])
    """

    column_labels: list[LabelOffset] = Field(
        default_factory=list, description="Labels above matrix"
    )
    row_labels: list[LabelOffset] = Field(default_factory=list, description="Labels left of matrix")


class NamingConfig(BaseModel):
    """
    Configuration for combining labels into field names.

        Attributes

        separator : str
            Character to join label parts
            Default: "_"

        skip_empty : bool
            Skip None/empty label parts when combining
            Default: True

        sanitize : bool
            Clean and lowercase field names
            Default: True

        order : list[Literal["row", "col"]]
            Order to combine row and column labels
            Default: ["row", "col"] means row labels come first
    """

    separator: str = Field(default="_", description="Separator character")
    skip_empty: bool = Field(default=True, description="Skip empty parts")
    sanitize: bool = Field(default=True, description="Sanitize names")
    order: list[Literal["row", "col"]] = Field(
        default=["row", "col"], description="Order to combine labels"
    )


class MatrixExtractorConfig(BaseModel):
    """
    Complete configuration for matrix-based extraction.

        Attributes

        detection : DetectionConfig
            How to detect and separate matrices

        label_offsets : LabelOffsetsConfig
            Where to find labels relative to matrices

        naming : NamingConfig
            How to combine labels into field names
    """

    detection: DetectionConfig = Field(
        default_factory=DetectionConfig, description="Matrix detection config"
    )
    label_offsets: LabelOffsetsConfig = Field(
        default_factory=LabelOffsetsConfig, description="Label offset config"
    )
    naming: NamingConfig = Field(default_factory=NamingConfig, description="Naming config")


@dataclass
class SectionContext:
    """
    Hierarchical section context for a row.

        Attributes

        parent_section : str | None
            Top-level parent section (e.g., "Prospective Payment Calculation")

        current_section : str | None
            Current section header (e.g., "Eligible Months", "Benchmark PBPM")

        row_label : str
            The specific row label (e.g., "AD Claims Aligned")

        is_section_header : bool
            Whether this row IS a section header (vs data row)

        is_aggregation : bool
            Whether this row is an aggregation/total row
    """

    parent_section: str | None
    current_section: str | None
    row_label: str
    is_section_header: bool
    is_aggregation: bool


@dataclass
class MatrixRegion:
    """
    A detected data matrix with its boundaries.

        Attributes

        start_row : int
            First row of the matrix (0-indexed)
        end_row : int
            Last row of the matrix (0-indexed, inclusive)
        start_col : int
            First column of the matrix (0-indexed)
        end_col : int
            Last column of the matrix (0-indexed, inclusive)
        data : pl.DataFrame
            The actual matrix data
        section_contexts : dict[int, SectionContext]
            Mapping from row index to section context
            Only for rows within this matrix
    """

    start_row: int
    end_row: int
    start_col: int
    end_col: int
    data: pl.DataFrame
    section_contexts: dict[int, SectionContext] = None

    def __post_init__(self):
        if self.section_contexts is None:
            self.section_contexts = {}


@register_expression(
    "matrix_extractor",
    schemas=["bronze", "silver"],
    description="Extract multiple data matrices with offset labels from complex Excel layouts",
)
class MatrixExtractor:
    """
    Extract multiple data matrices from a single sheet.

        This class detects independent data regions within a sheet and extracts
        each matrix with its associated labels from offset positions.

        The approach:
        1. Detect matrix boundaries using break detection
        2. For each matrix, extract labels from specified offsets
        3. Combine labels hierarchically to create field names
        4. Return list of transformed DataFrames

    """

    def __init__(self, config: MatrixExtractorConfig):
        """
        Initialize the matrix extractor.

                Parameters

                config : MatrixExtractorConfig
                    Complete extraction configuration
        """
        self.config = config

    def detect_section_hierarchy(
        self, df: pl.DataFrame, section_cfg: SectionDetectionConfig
    ) -> dict[int, SectionContext]:
        """
        Detect hierarchical sections in the data.

                Parameters

                df : pl.DataFrame
                    Full DataFrame
                section_cfg : SectionDetectionConfig
                    Section detection configuration

                Returns

                dict[int, SectionContext]
                    Mapping from row index to section context

                Notes

                This detects hierarchical patterns like:
                - Row 2: "Prospective Payment Calculation" (parent)
                - Row 3: "Eligible Months" (section header)
                - Row 4: "AD Claims Aligned" (data row)
                - Row 5: "AD Voluntarily Aligned" (data row)
                - Row 8: "Total Projected Eligible Months" (aggregation)
                - Row 9: "Assumed Retention Rate" (new standalone section)
                - Row 10: "Benchmark PBPM" (new section header)
        """
        contexts = {}
        parent_section = None
        current_section = None
        section_col = section_cfg.section_column

        # Get the section column as a list
        section_values = df[df.columns[section_col]].to_list()

        for row_idx, value in enumerate(section_values):
            if value is None or str(value).strip() == "":
                # Empty row - might be a break
                continue

            value_str = str(value).strip()

            # Check if this is an aggregation row
            is_aggregation = any(marker in value_str for marker in section_cfg.aggregation_markers)

            # Heuristic: Section headers typically:
            # 1. Don't have subcategory prefixes (no "AD", "ESRD")
            # 2. Appear after empty rows or at start
            # 3. Are followed by detail rows
            is_section_header = self._is_section_header(
                value_str, row_idx, section_values, section_cfg
            )

            if is_section_header:
                # This is a section header
                # Determine if it's a parent or child section
                if self._is_parent_section(value_str, section_cfg):
                    # Top-level parent
                    parent_section = value_str
                    current_section = None
                else:
                    # Sub-section under current parent
                    current_section = value_str

                contexts[row_idx] = SectionContext(
                    parent_section=parent_section,
                    current_section=current_section,
                    row_label=value_str,
                    is_section_header=True,
                    is_aggregation=False,
                )
            else:
                # This is a data row
                contexts[row_idx] = SectionContext(
                    parent_section=parent_section,
                    current_section=current_section,
                    row_label=value_str,
                    is_section_header=False,
                    is_aggregation=is_aggregation,
                )

        return contexts

    def _is_section_header(
        self, value: str, row_idx: int, all_values: list, section_cfg: SectionDetectionConfig
    ) -> bool:
        """
        Determine if a row is a section header vs data row.

                Heuristics:
                - Doesn't start with common subcategory prefixes
                - Previous row might be empty
                - Contains section marker keywords
                - Next rows are indented or subcategorized
        """
        # Check for explicit markers
        if section_cfg.section_markers:
            if any(marker in value for marker in section_cfg.section_markers):
                return True

        # Check if it has subcategory prefixes (indicates data row, not header)
        subcategory_prefixes = ["AD ", "ESRD ", "Part ", "Total ", "Projected "]
        has_subcategory = any(value.startswith(prefix) for prefix in subcategory_prefixes)

        if has_subcategory:
            # Exception: "Total X" might still be a section header if it's standalone
            # e.g., "Total Projected Eligible Months" is data
            # but "Benchmark Total" is a section header
            if value.startswith("Total ") and "Total" == value.split()[0]:
                # Check if next row is a subcategory
                if row_idx + 1 < len(all_values):
                    next_val = all_values[row_idx + 1]
                    if next_val and any(
                        str(next_val).startswith(p) for p in ["AD ", "ESRD ", "Part "]
                    ):
                        return True  # Next row is subcategory, so this is header
            return False

        # Check if previous row is empty (indicates section break)
        if row_idx > 0:
            prev_val = all_values[row_idx - 1]
            if prev_val is None or str(prev_val).strip() == "":
                return True

        # Check if next row has subcategory (indicates this is header)
        if row_idx + 1 < len(all_values):
            next_val = all_values[row_idx + 1]
            if next_val and any(str(next_val).startswith(p) for p in ["AD ", "ESRD ", "Part "]):
                return True

        # Default: assume it's a section header if no subcategory prefix
        return True

    def _is_parent_section(self, value: str, section_cfg: SectionDetectionConfig) -> bool:
        """
        Determine if a section is a top-level parent vs sub-section.

                Parent sections are typically:
                - "Prospective Payment Calculation"
                - "Retrospective Payment Calculation"

                Sub-sections are typically:
                - "Eligible Months"
                - "Benchmark PBPM"
                - "Assumed Retention Rate"
        """
        parent_keywords = ["Prospective Payment", "Retrospective Payment", "Calculation"]
        return any(keyword in value for keyword in parent_keywords)

    def detect_matrices(self, df: pl.DataFrame) -> list[MatrixRegion]:
        """
        Detect all data matrices in the DataFrame.

                Parameters

                df : pl.DataFrame
                    Raw input data

                Returns

                list[MatrixRegion]
                    List of detected matrix regions with their boundaries

                Notes

                Strategy:
                1. Find columns that contain data (not all None/empty)
                2. Group contiguous data columns into matrices
                3. For each column group, find row range with data
                4. Create MatrixRegion for each detected region
        """
        with tracer.span("detect_matrices"):
            detection_cfg = self.config.detection
            matrices = []

            # Get section contexts if section detection is enabled
            section_contexts = {}
            if detection_cfg.section_detection:
                section_contexts = self.detect_section_hierarchy(
                    df, detection_cfg.section_detection
                )

            # Identify data columns (skip metadata columns like processed_at, source_file, etc.)
            metadata_cols = [
                "processed_at",
                "source_file",
                "source_filename",
                "file_date",
                "sheet_type",
                "_output_table",
            ]
            data_cols = [(i, col) for i, col in enumerate(df.columns) if col not in metadata_cols]

            # Group contiguous data columns into matrices
            # A break of empty columns separates matrices
            col_groups = []
            current_group = []

            for col_idx, col_name in data_cols:
                # Check if this column has any non-null values
                has_data = df[col_name].drop_nulls().len() > 0

                if has_data:
                    current_group.append((col_idx, col_name))
                else:
                    # Empty column - might be a break
                    if len(current_group) >= detection_cfg.min_cols:
                        col_groups.append(current_group)
                    current_group = []

            # Don't forget the last group
            if len(current_group) >= detection_cfg.min_cols:
                col_groups.append(current_group)

            # For each column group, detect row boundaries
            for col_group in col_groups:
                col_indices = [idx for idx, _ in col_group]
                col_names = [name for _, name in col_group]

                start_col = min(col_indices)
                end_col = max(col_indices)

                # Find rows with data in these columns
                # Create a mask: True if row has any non-null value in these columns
                mask = pl.lit(False)
                for col_name in col_names:
                    mask = mask | df[col_name].is_not_null()

                data_rows = df.with_row_count("_row_idx").filter(mask)["_row_idx"].to_list()

                if len(data_rows) < detection_cfg.min_rows:
                    continue

                start_row = min(data_rows)
                end_row = max(data_rows)

                # Extract the data for this matrix
                matrix_data = df.slice(start_row, end_row - start_row + 1).select(col_names)

                # Get section contexts for rows in this matrix
                matrix_section_contexts = {
                    row_idx - start_row: section_contexts[row_idx]
                    for row_idx in range(start_row, end_row + 1)
                    if row_idx in section_contexts
                }

                region = MatrixRegion(
                    start_row=start_row,
                    end_row=end_row,
                    start_col=start_col,
                    end_col=end_col,
                    data=matrix_data,
                    section_contexts=matrix_section_contexts,
                )
                matrices.append(region)

            return matrices

    def extract_labels(
        self,
        df: pl.DataFrame,
        region: MatrixRegion,
        label_offsets: list[LabelOffset],
        axis: Literal["row", "col"],
    ) -> dict[int, list[str]]:
        """
        Extract labels at specified offsets for a matrix.

                Parameters

                df : pl.DataFrame
                    Full DataFrame
                region : MatrixRegion
                    The matrix region to extract labels for
                label_offsets : list[LabelOffset]
                    List of offset configurations
                axis : Literal["row", "col"]
                    Which axis to extract labels from

                Returns

                dict[int, list[str]]
                    Mapping from matrix index to combined labels
                    For column labels: {col_idx: [label1, label2, ...]}
                    For row labels: {row_idx: [label1, label2, ...]}
        """
        result = {}

        if axis == "col":
            # Extract column labels (from rows above the matrix)
            for col_idx in range(region.start_col, region.end_col + 1):
                labels = []
                col_name = df.columns[col_idx]

                for offset_cfg in label_offsets:
                    # Get the row at this offset
                    label_row = region.start_row + offset_cfg.offset

                    if label_row < 0 or label_row >= len(df):
                        continue

                    # Get the value at this position
                    value = df[col_name][label_row]

                    if value is not None:
                        value_str = str(value).strip()
                        if value_str:
                            labels.append(value_str)

                result[col_idx] = labels

            # Apply forward_fill if needed
            for offset_cfg in label_offsets:
                if offset_cfg.forward_fill:
                    self._forward_fill_labels(result, axis="col")

        else:  # axis == "row"
            # Extract row labels (from columns left of the matrix)
            # For rows, we use the section context instead of offset columns
            for row_idx in range(region.start_row, region.end_row + 1):
                labels = []
                relative_row = row_idx - region.start_row

                # Use section context if available
                if relative_row in region.section_contexts:
                    context = region.section_contexts[relative_row]

                    # Skip section header rows
                    if context.is_section_header:
                        continue

                    # Build hierarchy: parent -> section -> row_label
                    if context.parent_section:
                        labels.append(context.parent_section)
                    if context.current_section:
                        labels.append(context.current_section)
                    if context.row_label:
                        labels.append(context.row_label)

                result[row_idx] = labels

        return result

    def _forward_fill_labels(self, labels_dict: dict[int, list[str]], axis: Literal["row", "col"]):
        """
        Forward-fill None/empty labels from previous non-empty value.

                This handles spanning headers like "Projected Experience" that
                apply to multiple columns (Jan, Feb, Mar).

                Parameters

                labels_dict : dict[int, list[str]]
                    Mapping from index to labels (modified in-place)
                axis : Literal["row", "col"]
                    Which axis we're filling
        """
        indices = sorted(labels_dict.keys())
        current_fill = None

        for idx in indices:
            labels = labels_dict[idx]

            if labels:
                # Update current fill value
                current_fill = labels
            elif current_fill:
                # Use current fill for empty labels
                labels_dict[idx] = current_fill.copy()

    def combine_labels(
        self, row_labels: dict[int, list[str]], col_labels: dict[int, list[str]]
    ) -> dict[tuple[int, int], str]:
        """
        Combine row and column labels into field names.

                Parameters

                row_labels : dict[int, list[str]]
                    Row labels for each row index
                col_labels : dict[int, list[str]]
                    Column labels for each column index

                Returns

                dict[tuple[int, int], str]
                    Mapping from (row_idx, col_idx) to field name
        """
        field_names = {}
        naming_cfg = self.config.naming

        for row_idx, row_label_list in row_labels.items():
            for col_idx, col_label_list in col_labels.items():
                # Combine labels according to order configuration
                parts = []

                for axis in naming_cfg.order:
                    if axis == "row":
                        parts.extend(row_label_list)
                    else:  # axis == "col" – Pydantic Literal enforces only "row"/"col"
                        parts.extend(col_label_list)

                # Filter empty parts if configured
                if naming_cfg.skip_empty:
                    parts = [p for p in parts if p and str(p).strip()]

                # Join with separator
                field_name = naming_cfg.separator.join(parts)

                # Sanitize if configured
                if naming_cfg.sanitize:
                    field_name = self._sanitize_field_name(field_name)

                field_names[(row_idx, col_idx)] = field_name

        return field_names

    def _sanitize_field_name(self, name: str) -> str:
        """
        Sanitize a field name to be valid and clean.

                Parameters

                name : str
                    Original field name

                Returns

                str
                    Sanitized field name (lowercase, underscores, alphanumeric)
        """
        import re

        # Convert to lowercase
        name = name.lower()

        # Replace spaces and hyphens with underscores
        name = name.replace(" ", "_").replace("-", "_")

        # Remove special characters except underscores
        name = re.sub(r"[^a-z0-9_]", "", name)

        # Replace multiple underscores with single
        name = re.sub(r"_+", "_", name)

        # Remove leading/trailing underscores
        name = name.strip("_")

        # Ensure it doesn't start with a number
        if name and name[0].isdigit():
            name = f"col_{name}"

        return name

    def extract(self, df: pl.DataFrame) -> list[pl.DataFrame]:
        """
        Extract all matrices from the DataFrame.

                Parameters

                df : pl.DataFrame
                    Raw input data

                Returns

                list[pl.DataFrame]
                    List of extracted and transformed matrices
        """
        with tracer.span("extract_matrices", num_rows=len(df), num_cols=len(df.columns)):
            # Detect all matrices
            regions = self.detect_matrices(df)

            # Extract each matrix
            results = []
            for region in regions:
                # Extract labels
                row_labels = self.extract_labels(
                    df, region, self.config.label_offsets.row_labels, axis="row"
                )
                col_labels = self.extract_labels(
                    df, region, self.config.label_offsets.column_labels, axis="col"
                )

                # Combine labels into field names
                field_names = self.combine_labels(row_labels, col_labels)

                # Transform the matrix data into tidy format
                # Convert from wide format (columns = different measures)
                # to long format (one row per measure)
                records = []

                for (row_idx, col_idx), field_name in field_names.items():
                    # Get the actual data value
                    relative_row = row_idx - region.start_row
                    relative_col = col_idx - region.start_col

                    if relative_row >= len(region.data) or relative_col >= len(region.data.columns):
                        continue

                    value = region.data[region.data.columns[relative_col]][relative_row]

                    # Skip section header rows (they don't have data)
                    if relative_row in region.section_contexts:
                        context = region.section_contexts[relative_row]
                        if context.is_section_header:
                            continue

                    # Create a record with the field name and value
                    records.append(
                        {
                            "field_name": field_name,
                            "value": value,
                            "row_idx": row_idx,
                            "col_idx": col_idx,
                        }
                    )

                # Convert records to DataFrame
                if records:
                    result_df = pl.DataFrame(records)
                    results.append(result_df)

            return results
