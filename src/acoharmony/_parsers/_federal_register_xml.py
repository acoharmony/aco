# © 2025 HarmonyCares
# All rights reserved.

"""
Federal Register XML parser.

Parses Federal Register documents in XML format to extract structured content
including individual paragraphs, sections, and metadata.

Federal Register XML Schema:
- URL pattern: https://www.federalregister.gov/documents/full_text/xml/{YYYY}/{MM}/{DD}/{document_number}.xml
- Contains full structured document with paragraph IDs
- Better for extracting specific paragraphs than HTML
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import polars as pl

from .._log import LogWriter

logger = LogWriter("parsers.federal_register_xml")


def parse_federal_register_xml(file_path: Path) -> pl.LazyFrame:
    """
    Parse Federal Register XML document.

    Extracts structured content including paragraphs with IDs, sections,
    and document metadata.

    Args:
        file_path: Path to Federal Register XML file

    Returns:
        pl.LazyFrame with columns:
            - paragraph_id: Paragraph identifier (e.g., "p-3", "p-45")
            - paragraph_text: Full paragraph text content
            - section_title: Section heading (if applicable)
            - page_number: Page number where paragraph appears
            - document_number: Federal Register document number

    """
    logger.info(f"Parsing Federal Register XML: {file_path}")

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        paragraphs = []

        # Extract document number from root or filename
        doc_number = root.get("doc-number", file_path.stem)

        # Federal Register XML uses <P> tags with various attributes
        # Common patterns:
        # - <P>text</P> for regular paragraphs
        # - <P id="p-3">text</P> for identified paragraphs
        # - <FP> for flush paragraphs
        # - <HD> for headers/section titles

        current_section = ""
        paragraph_counter = 0

        # Recursively walk all elements to find text content
        for elem in root.iter():
            # Track section headers
            if elem.tag in ["HD", "HD1", "HD2", "HD3"]:
                text = _get_element_text(elem)
                if text:
                    current_section = text
                    logger.debug(f"Found section: {current_section[:50]}...")

            # Extract paragraphs
            if elem.tag in ["P", "FP"]:
                paragraph_counter += 1

                # Get paragraph ID from attribute or generate sequential
                para_id = elem.get("id", "")
                if not para_id:
                    # Check for N attribute (paragraph number)
                    n_attr = elem.get("N", "")
                    if n_attr:
                        para_id = f"p-{n_attr}"
                    else:
                        para_id = f"p-{paragraph_counter}"

                # Extract text content
                text = _get_element_text(elem)

                if text and len(text.strip()) > 0:
                    # Get page number if available
                    page_num = elem.get("page", "")

                    paragraphs.append({
                        "paragraph_id": para_id,
                        "paragraph_text": text.strip(),
                        "section_title": current_section,
                        "page_number": page_num,
                        "document_number": doc_number,
                    })

        logger.info(f"Extracted {len(paragraphs)} paragraphs from Federal Register XML")

        if not paragraphs:
            logger.warning("No paragraphs found in XML, returning empty structure")
            return pl.LazyFrame({
                "paragraph_id": [],
                "paragraph_text": [],
                "section_title": [],
                "page_number": [],
                "document_number": [],
            })

        # Convert to LazyFrame
        df = pl.DataFrame(paragraphs)
        return df.lazy()

    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to parse Federal Register XML: {e}")
        raise


def _get_element_text(elem: ET.Element) -> str:
    """
    Extract all text content from an XML element and its children.

    Handles nested elements like <E> (emphasis), <SU> (superscript), etc.

    Args:
        elem: XML element

    Returns:
        str: Combined text content
    """
    # Get direct text
    text_parts = []

    if elem.text:
        text_parts.append(elem.text)

    # Get text from all children recursively
    for child in elem:
        child_text = _get_element_text(child)
        if child_text:
            text_parts.append(child_text)

        # Get tail text (text after child element)
        if child.tail:
            text_parts.append(child.tail)

    return " ".join(text_parts).strip()


def extract_paragraph_by_id(file_path: Path, paragraph_id: str) -> str:
    """
    Extract specific paragraph text by ID from Federal Register XML.

    Args:
        file_path: Path to Federal Register XML file
        paragraph_id: Paragraph identifier (e.g., "p-3", "3", "p-45")

    Returns:
        str: Paragraph text or empty string if not found

    """
    # Normalize paragraph ID
    if not paragraph_id.startswith("p-"):
        paragraph_id = f"p-{paragraph_id}"

    try:
        df = parse_federal_register_xml(file_path).collect()

        # Filter for specific paragraph
        result = df.filter(pl.col("paragraph_id") == paragraph_id)

        if result.height > 0:
            return result["paragraph_text"][0]

        logger.warning(f"Paragraph {paragraph_id} not found in XML")
        return ""

    except Exception as e:
        logger.error(f"Failed to extract paragraph {paragraph_id}: {e}")
        return ""
