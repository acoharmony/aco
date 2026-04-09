# © 2025 HarmonyCares
# All rights reserved.

"""
Excel structure comparison tool for identifying differences across files.

WHY THIS EXISTS
===============
CMS reports evolve over time - new columns appear, headers shift, table sizes change.
Schemas must adapt to these changes while remaining stable for consistent fields.

To design robust schemas, we need to know:
1. What structure is CONSISTENT across all files (can hardcode)
2. What structure VARIES between files (need dynamic detection)
3. What columns are STABLE vs TRANSIENT

Without this analysis, schemas either break on new files or are overly generic.

PROBLEM SOLVED
==============
We have 3 PLARU files from different years (PY2023, PY2024, PY2025).
Need to compare their structures to determine:
- Which sheets have consistent dimensions vs variable dimensions
- Which header rows are stable vs shifting
- Which columns appear in all files vs some files
- Which data sections have consistent boundaries vs variable boundaries

This tool compares multiple Excel analysis JSONs and generates a diff report showing:
- Consistent fields: Same across all files → can hardcode in schema
- Variable fields: Different across files → need dynamic detection
- New/removed fields: Only in some files → need optional handling

Output: JSON diff report saved to /opt/s3/data/workspace/logs/dev/
"""

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class FieldComparison(BaseModel):
    """
    Comparison result for a single field across files.

        Attributes

        field_path : str
            Dot-notation path to the field (e.g., "sheets.0.dimensions.rows")

        values : dict[str, Any]
            Mapping from filename to value for this field

        is_consistent : bool
            Whether all files have the same value

        variance_description : Optional[str]
            Human-readable description of variance if not consistent
    """

    field_path: str = Field(description="Dot-notation field path")
    values: dict[str, Any] = Field(description="Filename to value mapping")
    is_consistent: bool = Field(description="Whether values are consistent")
    variance_description: str | None = Field(
        default=None, description="Description of variance"
    )


class SheetComparison(BaseModel):
    """
    Comparison of a single sheet across files.

        Attributes

        sheet_index : int
            0-indexed sheet number

        sheet_name : str
            Sheet name (if consistent)

        dimension_consistency : FieldComparison
            Comparison of dimensions (rows, cols)

        header_consistency : FieldComparison
            Comparison of header row detection

        column_consistency : dict[str, Any]
            Column-level comparison results

        files_present : list[str]
            Which files contain this sheet
    """

    sheet_index: int = Field(description="Sheet index")
    sheet_name: str = Field(description="Sheet name")
    dimension_consistency: FieldComparison = Field(description="Dimension comparison")
    header_consistency: FieldComparison = Field(description="Header comparison")
    column_consistency: dict[str, Any] = Field(
        default_factory=dict, description="Column comparisons"
    )
    files_present: list[str] = Field(description="Files containing this sheet")


class ExcelDiffReport(BaseModel):
    """
    Complete diff report comparing multiple Excel files.

        Attributes

        files_compared : list[str]
            List of filenames compared

        file_count : int
            Number of files compared

        sheet_comparisons : list[SheetComparison]
            Per-sheet comparison results

        global_consistency : dict[str, FieldComparison]
            File-level consistency checks

        summary : dict[str, Any]
            High-level summary of differences
    """

    files_compared: list[str] = Field(description="Files compared")
    file_count: int = Field(description="Number of files")
    sheet_comparisons: list[SheetComparison] = Field(
        description="Sheet-level comparisons"
    )
    global_consistency: dict[str, FieldComparison] = Field(
        description="Global field comparisons"
    )
    summary: dict[str, Any] = Field(description="Summary statistics")


class ExcelDiffAnalyzer:
    """
    Compare multiple Excel analysis JSONs to identify structural differences.

        WHY: Determines what's consistent vs variable across files to inform schema design.

        The class handles:
        - Loading multiple analysis JSON files
        - Comparing sheet structures
        - Identifying consistent vs variable fields
        - Generating diff reports

    """

    def __init__(self, analysis_files: list[str | Path]):
        """
        Initialize diff analyzer with analysis JSON files.

                Parameters

                analysis_files : list[str | Path]
                    Paths to Excel analysis JSON files to compare
        """
        self.analysis_files = [Path(f) for f in analysis_files]
        self.analyses: dict[str, dict] = {}

    def load_analyses(self) -> None:
        """
        Load all analysis JSON files.

                WHY: Loads complete analysis data for comparison.
        """
        for file_path in self.analysis_files:
            if not file_path.exists():
                raise FileNotFoundError(f"Analysis file not found: {file_path}")

            with open(file_path) as f:
                data = json.load(f)
                # Use filename as key
                self.analyses[file_path.name] = data

    def compare_values(
        self, values: dict[str, Any], field_path: str
    ) -> FieldComparison:
        """
        Compare values for a field across files.

                Parameters

                values : dict[str, Any]
                    Mapping from filename to value

                field_path : str
                    Dot-notation path to field

                Returns

                FieldComparison
                    Comparison result
        """
        unique_values = {
            str(v) for v in values.values() if v is not None
        }  # Convert to str for comparison

        is_consistent = len(unique_values) <= 1

        variance_desc = None
        if not is_consistent:
            # Build description of variance
            value_counts = defaultdict(list)
            for filename, value in values.items():
                value_counts[str(value)].append(filename)

            variance_parts = [
                f"{value}: {', '.join(files)}" for value, files in value_counts.items()
            ]
            variance_desc = " | ".join(variance_parts)

        return FieldComparison(
            field_path=field_path,
            values=values,
            is_consistent=is_consistent,
            variance_description=variance_desc,
        )

    def compare_sheet_dimensions(self, sheet_idx: int) -> FieldComparison:
        """
        Compare sheet dimensions across files.

                Parameters

                sheet_idx : int
                    Sheet index to compare

                Returns

                FieldComparison
                    Dimension comparison result
        """
        dimensions = {}

        for filename, analysis in self.analyses.items():
            sheets_dict = analysis.get("sheets", {})
            # Find sheet with matching index
            sheet = None
            for _sheet_key, sheet_data in sheets_dict.items():
                if sheet_data.get("sheet_index") == sheet_idx:
                    sheet = sheet_data
                    break

            if sheet:
                dims = sheet.get("dimensions", {})
                rows = dims.get("rows", 0)
                cols = dims.get("cols", 0)
                dimensions[filename] = f"{rows}r × {cols}c"
            else:
                dimensions[filename] = None

        return self.compare_values(dimensions, f"sheets.{sheet_idx}.dimensions")

    def compare_sheet_headers(self, sheet_idx: int) -> FieldComparison:
        """
        Compare header row detection across files.

                Parameters

                sheet_idx : int
                    Sheet index to compare

                Returns

                FieldComparison
                    Header comparison result
        """
        headers = {}

        for filename, analysis in self.analyses.items():
            sheets_dict = analysis.get("sheets", {})
            # Find sheet with matching index
            sheet = None
            for _sheet_key, sheet_data in sheets_dict.items():
                if sheet_data.get("sheet_index") == sheet_idx:
                    sheet = sheet_data
                    break

            if sheet:
                potential_headers = sheet.get("potential_headers", [])
                if potential_headers:
                    # Get best header (first one)
                    best_header = potential_headers[0]
                    row_idx = best_header.get("row_index")
                    confidence = best_header.get("confidence")
                    # Convert confidence to float if it's a string
                    try:
                        conf_float = float(confidence) if confidence is not None else 0.0
                        headers[filename] = f"Row {row_idx} (conf: {conf_float:.2f})"
                    except (ValueError, TypeError):
                        headers[filename] = f"Row {row_idx} (conf: {confidence})"
                else:
                    headers[filename] = "No headers detected"
            else:
                headers[filename] = None

        return self.compare_values(headers, f"sheets.{sheet_idx}.best_header")

    def compare_sheet_columns(self, sheet_idx: int) -> dict[str, Any]:
        """
        Compare column structures across files.

                Parameters

                sheet_idx : int
                    Sheet index to compare

                Returns

                dict[str, Any]
                    Column comparison results
        """
        column_counts = {}
        column_sets = {}

        for filename, analysis in self.analyses.items():
            sheets_dict = analysis.get("sheets", {})
            # Find sheet with matching index
            sheet = None
            for _sheet_key, sheet_data in sheets_dict.items():
                if sheet_data.get("sheet_index") == sheet_idx:
                    sheet = sheet_data
                    break

            if sheet:
                dims = sheet.get("dimensions", {})
                cols = dims.get("cols", 0)
                column_counts[filename] = cols

                # Get header values if available
                potential_headers = sheet.get("potential_headers", [])
                if potential_headers:
                    best_header = potential_headers[0]
                    header_values = best_header.get("values", [])
                    # Filter out empty strings
                    header_values = [v for v in header_values if v and str(v).strip()]
                    column_sets[filename] = set(header_values)
                else:
                    column_sets[filename] = set()
            else:
                column_counts[filename] = 0
                column_sets[filename] = set()

        # Find common columns and unique columns
        all_columns = set()
        for cols in column_sets.values():
            all_columns.update(cols)

        common_columns = all_columns.copy()
        for cols in column_sets.values():
            if cols:  # Only intersect with non-empty sets
                common_columns &= cols

        unique_per_file = {}
        for filename, cols in column_sets.items():
            unique = cols - common_columns
            if unique:
                unique_per_file[filename] = list(unique)

        return {
            "column_counts": column_counts,
            "consistent_count": len(set(column_counts.values())) == 1,
            "total_unique_columns": len(all_columns),
            "common_columns": list(common_columns),
            "common_column_count": len(common_columns),
            "unique_per_file": unique_per_file,
        }

    def compare_sheets(self) -> list[SheetComparison]:
        """
        Compare all sheets across files.

                WHY: Identifies which sheets have consistent vs variable structure.

                Returns

                list[SheetComparison]
                    List of sheet comparison results
        """
        sheet_comparisons = []

        # Determine max sheet count
        max_sheets = 0
        for analysis in self.analyses.values():
            sheets_dict = analysis.get("sheets", {})
            for sheet_data in sheets_dict.values():
                max_sheets = max(max_sheets, sheet_data.get("sheet_index", 0) + 1)

        for sheet_idx in range(max_sheets):
            # Determine which files have this sheet
            files_present = []
            sheet_name = None

            for filename, analysis in self.analyses.items():
                sheets_dict = analysis.get("sheets", {})
                # Find sheet with matching index
                for _sheet_key, sheet_data in sheets_dict.items():
                    if sheet_data.get("sheet_index") == sheet_idx:
                        files_present.append(filename)
                        if sheet_name is None:
                            sheet_name = sheet_data.get("sheet_name", f"Sheet{sheet_idx}")
                        break

            if not sheet_name:
                sheet_name = f"Sheet{sheet_idx}"

            # Compare dimensions
            dim_comparison = self.compare_sheet_dimensions(sheet_idx)

            # Compare headers
            header_comparison = self.compare_sheet_headers(sheet_idx)

            # Compare columns
            column_comparison = self.compare_sheet_columns(sheet_idx)

            sheet_comp = SheetComparison(
                sheet_index=sheet_idx,
                sheet_name=sheet_name,
                dimension_consistency=dim_comparison,
                header_consistency=header_comparison,
                column_consistency=column_comparison,
                files_present=files_present,
            )

            sheet_comparisons.append(sheet_comp)

        return sheet_comparisons

    def generate_summary(
        self, sheet_comparisons: list[SheetComparison]
    ) -> dict[str, Any]:
        """
        Generate summary statistics.

                Parameters

                sheet_comparisons : list[SheetComparison]
                    Sheet comparison results

                Returns

                dict[str, Any]
                    Summary statistics
        """
        total_sheets = len(sheet_comparisons)
        consistent_dimensions = sum(
            1 for sc in sheet_comparisons if sc.dimension_consistency.is_consistent
        )
        consistent_headers = sum(
            1 for sc in sheet_comparisons if sc.header_consistency.is_consistent
        )
        consistent_columns = sum(
            1 for sc in sheet_comparisons if sc.column_consistency.get("consistent_count", False)
        )

        # Find sheets present in all files
        all_files = set(self.analyses.keys())
        sheets_in_all = sum(
            1 for sc in sheet_comparisons if set(sc.files_present) == all_files
        )

        return {
            "total_sheets": total_sheets,
            "sheets_in_all_files": sheets_in_all,
            "sheets_with_consistent_dimensions": consistent_dimensions,
            "sheets_with_consistent_headers": consistent_headers,
            "sheets_with_consistent_column_count": consistent_columns,
            "dimension_consistency_rate": (
                consistent_dimensions / total_sheets if total_sheets > 0 else 0
            ),
            "header_consistency_rate": (
                consistent_headers / total_sheets if total_sheets > 0 else 0
            ),
            "column_consistency_rate": (
                consistent_columns / total_sheets if total_sheets > 0 else 0
            ),
        }

    def analyze(self) -> ExcelDiffReport:
        """
        Perform complete diff analysis.

                WHY: Generates comprehensive comparison report for schema design.

                Returns

                ExcelDiffReport
                    Complete diff report
        """
        # Load all analyses
        self.load_analyses()

        # Compare sheets
        sheet_comparisons = self.compare_sheets()

        # Global consistency checks
        global_consistency = {}

        # Check file-level metadata
        filenames = {}
        for filename, analysis in self.analyses.items():
            filenames[filename] = analysis.get("file_name", "")

        global_consistency["filenames"] = self.compare_values(
            filenames, "file_name"
        )

        # Generate summary
        summary = self.generate_summary(sheet_comparisons)

        return ExcelDiffReport(
            files_compared=list(self.analyses.keys()),
            file_count=len(self.analyses),
            sheet_comparisons=sheet_comparisons,
            global_consistency=global_consistency,
            summary=summary,
        )

    def save_report(
        self, report: ExcelDiffReport, output_filename: str = "excel_structure_diff.json"
    ) -> Path:
        """
        Save diff report to dev logs.

                Parameters

                report : ExcelDiffReport
                    Diff report to save

                output_filename : str
                    Output filename (saved to /opt/s3/data/workspace/logs/dev/)

                Returns

                Path
                    Path to saved file
        """
        dev_logs = Path("/opt/s3/data/workspace/logs/dev")
        dev_logs.mkdir(parents=True, exist_ok=True)

        output_path = dev_logs / output_filename

        with open(output_path, "w") as f:
            # Convert to dict for JSON serialization
            report_dict = report.model_dump()
            json.dump(report_dict, f, indent=2)

        return output_path


def compare_plaru_files() -> Path:
    """
    Compare all PLARU analysis files.

        WHY: Identifies structural differences across PLARU years for schema design.

        Returns

        Path
            Path to saved diff report
    """
    dev_logs = Path("/opt/s3/data/workspace/logs/dev")

    # Find all PLARU analysis files
    plaru_files = sorted(dev_logs.glob("REACH.D0259.PLARU.*.json"))

    if len(plaru_files) < 2:
        raise ValueError(
            f"Need at least 2 PLARU analysis files to compare. Found: {len(plaru_files)}"
        )

    print(f"Comparing {len(plaru_files)} PLARU analysis files:")
    for f in plaru_files:
        print(f"  - {f.name}")

    # Create analyzer
    analyzer = ExcelDiffAnalyzer(plaru_files)

    # Run analysis
    print("\nAnalyzing differences...")
    report = analyzer.analyze()

    # Save report
    output_path = analyzer.save_report(report, "plaru_structure_diff.json")

    print(f"\nDiff report saved to: {output_path}")
    print("\nSummary:")
    print(f"  Files compared: {report.file_count}")
    print(f"  Total sheets: {report.summary['total_sheets']}")
    print(
        f"  Sheets in all files: {report.summary['sheets_in_all_files']}"
    )
    print(
        f"  Sheets with consistent dimensions: {report.summary['sheets_with_consistent_dimensions']}"
    )
    print(
        f"  Sheets with consistent headers: {report.summary['sheets_with_consistent_headers']}"
    )
    print(
        f"  Sheets with consistent column count: {report.summary['sheets_with_consistent_column_count']}"
    )

    return output_path


if __name__ == "__main__":
    compare_plaru_files()
