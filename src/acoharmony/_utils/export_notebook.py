# © 2025 HarmonyCares
# All rights reserved.

"""
Export marimo notebooks to HTML with static tables.

This script exports marimo notebooks to HTML format, converting scrollable
tables to static (fully expanded) tables for better printing/sharing.
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path


def export_notebook_to_html(notebook_path: Path, output_path: Path | None = None) -> Path:
    """
    Export a marimo notebook to HTML with static tables.

    Args:
        notebook_path: Path to the marimo notebook (.py file)
        output_path: Optional output path for HTML file

    Returns:
        Path to the generated HTML file
    """
    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook not found: {notebook_path}")

    if output_path is None:
        output_dir = Path("/home/care/kcorwin/Downloads")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / notebook_path.with_suffix(".html").name

    # Export using marimo CLI
    print(f"Exporting {notebook_path.name} to HTML...")
    result = subprocess.run(
        ["marimo", "export", "html", str(notebook_path), "-o", str(output_path)],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error exporting notebook: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"marimo export failed: {result.stderr}")

    print(f"Exported to {output_path}")

    # Post-process HTML to make tables static
    print("Converting scrollable tables to static...")
    html_content = output_path.read_text(encoding="utf-8")
    modified_html = make_tables_static(html_content)
    output_path.write_text(modified_html, encoding="utf-8")

    print(f"Export complete: {output_path}")
    return output_path


def make_tables_static(html_content: str) -> str:
    """
    Convert scrollable tables in HTML to static tables.

    This removes max-height, overflow, and scroll-related CSS properties
    from table containers while preserving all other styling.

    Args:
        html_content: Raw HTML content

    Returns:
        Modified HTML with static tables
    """
    # CSS to inject that overrides scrollable table behavior
    static_table_css = """
<style>
/* Override scrollable tables to be static for export */
.marimo-table-container,
[class*="table-container"],
[class*="dataframe-container"],
div[style*="overflow: auto"],
div[style*="overflow-y: auto"],
div[style*="overflow-x: auto"],
div[style*="max-height"] {
    max-height: none !important;
    overflow: visible !important;
    overflow-x: visible !important;
    overflow-y: visible !important;
}

/* Ensure tables expand fully */
table {
    width: 100% !important;
}

/* Remove any scroll shadows or indicators */
[class*="scroll-shadow"],
[class*="fade-overlay"] {
    display: none !important;
}

/* Ensure all rows are visible */
tbody {
    max-height: none !important;
    overflow: visible !important;
}

/* Print-friendly adjustments */
@media print {
    .marimo-table-container,
    [class*="table-container"] {
        page-break-inside: avoid;
    }

    table {
        font-size: 10pt;
    }
}
</style>
"""

    # Insert custom CSS before closing </head> tag
    if "</head>" in html_content:
        html_content = html_content.replace("</head>", f"{static_table_css}\n</head>")
    else:
        # Fallback: insert at beginning of body
        html_content = html_content.replace("<body", f"{static_table_css}\n<body")

    # Remove inline styles that cause scrolling
    # Match style attributes with overflow/max-height
    patterns = [
        # Remove max-height from inline styles
        (r'style="([^"]*?)max-height:\s*[^;]+;?([^"]*?)"', r'style="\1\2"'),
        # Remove overflow: auto/scroll from inline styles
        (r'style="([^"]*?)overflow(?:-[xy])?:\s*(?:auto|scroll)[^;]*;?([^"]*?)"', r'style="\1\2"'),
    ]

    for pattern, replacement in patterns:
        html_content = re.sub(pattern, replacement, html_content, flags=re.IGNORECASE)

    # Clean up empty style attributes
    html_content = re.sub(r'style="\s*"', "", html_content)

    return html_content


def main():
    """Main entry point for the export script."""
    parser = argparse.ArgumentParser(
        description="Export marimo notebook to HTML with static tables"
    )
    parser.add_argument(
        "notebook",
        type=Path,
        nargs="?",
        default=Path("/opt/s3/data/notebooks/consolidated_alignments.py"),
        help="Path to marimo notebook (default: consolidated_alignments.py)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output HTML file path (default: same name with .html extension)",
    )

    args = parser.parse_args()

    try:
        output_path = export_notebook_to_html(args.notebook, args.output)
        print(f"\nSuccess! HTML exported to: {output_path}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
