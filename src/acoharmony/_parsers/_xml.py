# © 2025 HarmonyCares
# All rights reserved.

"""
Generic XML parser for structured data files.

Parses XML documents with repeated row elements into Polars DataFrames.
Supports schema-driven parsing with row_tag specification.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import polars as pl

from .._log import LogWriter
from ._registry import register_parser

logger = LogWriter("parsers.xml")


@register_parser("xml", description="Generic XML parser with row-level extraction")
def parse_xml(
    file_path: Path,
    schema: dict[str, Any],
    **kwargs,
) -> pl.LazyFrame:
    """
    Parse XML file into LazyFrame using schema configuration.

    Extracts repeated row elements based on schema's row_tag, converting
    each element's child tags into DataFrame columns.

    Args:
        file_path: Path to XML file
        schema: Table metadata dictionary with file_format.row_tag
        **kwargs: Additional parsing options

    Returns:
        pl.LazyFrame with columns from XML row elements

    Example XML structure:
        <Root>
            <Header>...</Header>
            <Beneficiarys>
                <Beneficiary>
                    <MBI>5XU6K54UD46</MBI>
                    <FirstName>PATRICIA</FirstName>
                    ...
                </Beneficiary>
            </Beneficiarys>
        </Root>

        With row_tag="Beneficiary", extracts each Beneficiary as a row.
    """
    logger.info(f"Parsing XML file: {file_path}")

    # Get row tag from schema
    # Schema can be either dict or TableMetadata object
    if hasattr(schema, "file_format"):
        # TableMetadata object
        file_format = schema.file_format
        row_tag = file_format.get("row_tag")
    else:
        # Dict (for testing)
        file_format = schema.get("file_format", {})
        row_tag = file_format.get("row_tag")

    if not row_tag:
        raise ValueError("XML schema must specify 'row_tag' in file_format")

    logger.info(f"Using row tag: {row_tag}")

    try:
        # Parse XML
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Extract all row elements
        rows = []
        for row_elem in root.iter(row_tag):
            row_data = {}

            # Extract all child element values
            for child in row_elem:
                tag = child.tag
                # Handle nested elements (like BeneExcReasons/BeneExcReason)
                if len(child) > 0:
                    # If child has sub-elements, extract them
                    sub_values = [subchild.text or "" for subchild in child]
                    # Join multiple values or take first
                    row_data[tag] = "|".join(sub_values) if len(sub_values) > 1 else (sub_values[0] if sub_values else "")
                else:
                    row_data[tag] = child.text or ""

            rows.append(row_data)

        logger.info(f"Extracted {len(rows)} rows from XML")

        # Convert to DataFrame
        if not rows:
            # Empty file - create empty DataFrame with schema columns
            if hasattr(schema, "columns"):
                columns_spec = schema.columns
                empty_data = {col.get("name", f"col_{i}"): [] for i, col in enumerate(columns_spec)}
            else:
                columns_spec = schema.get("columns", [])
                empty_data = {col.get("name", f"col_{i}"): [] for i, col in enumerate(columns_spec)}
            df = pl.DataFrame(empty_data)
        else:
            df = pl.DataFrame(rows)

        return df.lazy()

    except ET.ParseError as e:
        logger.error(f"XML parse error in {file_path}: {e}")
        raise ValueError(f"Invalid XML file: {e}") from e
    except Exception as e:
        logger.error(f"Error parsing XML file {file_path}: {e}")
        raise
