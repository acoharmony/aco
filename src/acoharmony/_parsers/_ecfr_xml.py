# © 2025 HarmonyCares
# All rights reserved.

"""
eCFR (Electronic Code of Federal Regulations) XML parser.

Parses eCFR documents in XML format to extract structured content
including sections, parts, and regulatory text.

eCFR XML Schema:
- URL pattern: https://www.ecfr.gov/api/versioner/v1/full/{date}/title-{title}.xml
- Contains full CFR title with all sections, parts, and subparts
- Hierarchical structure with clear section identifiers
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import polars as pl

from .._log import LogWriter

logger = LogWriter("parsers.ecfr_xml")


def parse_ecfr_xml(file_path: Path, target_section: str | None = None) -> pl.LazyFrame:
    """
    Parse eCFR XML document.

    Extracts sections with their text content, subject headings, and metadata.

    Args:
        file_path: Path to eCFR XML file
        target_section: Optional specific section to extract (e.g., "414.2")

    Returns:
        pl.LazyFrame with columns:
            - section_number: Section identifier (e.g., "414.2", "414.10")
            - section_title: Section subject/heading
            - section_text: Full section text content
            - part_number: Part number (e.g., "414")
            - subpart: Subpart designation (if applicable)
            - authority: Authority citation
            - source: Source citation

    """
    logger.info(f"Parsing eCFR XML: {file_path}")
    if target_section:
        logger.info(f"Target section: {target_section}")

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        sections = []

        # eCFR XML uses <SECTION> tags with NUM attribute for section numbers
        # Structure: <CFRGRANULE> -> <PART> -> <SECTION>
        for section_elem in root.iter("SECTION"):
            # Get section number from NUM attribute
            section_num = section_elem.get("NUM", "")

            # If target section specified, skip non-matching sections
            if target_section and section_num != target_section:
                continue

            # Extract section subject/title (in <SUBJECT> tag)
            subject_elem = section_elem.find("SUBJECT")
            section_title = _get_element_text(subject_elem) if subject_elem is not None else ""

            # Extract section text from all <P> tags
            section_text_parts = []
            for p_elem in section_elem.iter("P"):
                text = _get_element_text(p_elem)
                if text:
                    section_text_parts.append(text)

            section_text = " ".join(section_text_parts)

            # Get part number from parent PART element
            part_elem = _find_parent_element(root, section_elem, "PART")
            part_number = ""
            if part_elem is not None:
                # Part number might be in <HD> or <RESERVED> tags
                hd_elem = part_elem.find(".//HD[@SOURCE='HED']")
                if hd_elem is not None:
                    hd_text = _get_element_text(hd_elem)
                    # Extract part number from "Part 414" or similar
                    import re
                    part_match = re.search(r"Part\s+(\d+)", hd_text)
                    if part_match:
                        part_number = part_match.group(1)

            # Get subpart if present
            subpart = ""
            subpart_elem = _find_parent_element(root, section_elem, "SUBPART")
            if subpart_elem is not None:
                subpart_hd = subpart_elem.find(".//HD[@SOURCE='HED']")
                if subpart_hd is not None:
                    subpart = _get_element_text(subpart_hd)

            # Extract authority and source from parent elements
            authority = ""
            source = ""

            # Look for AUTH and SOURCE tags in parent context
            if part_elem is not None:
                auth_elem = part_elem.find(".//AUTH")
                if auth_elem is not None:
                    authority = _get_element_text(auth_elem)

                source_elem = part_elem.find(".//SOURCE")
                if source_elem is not None:
                    source = _get_element_text(source_elem)

            if section_num and section_text:
                sections.append({
                    "section_number": section_num,
                    "section_title": section_title,
                    "section_text": section_text,
                    "part_number": part_number,
                    "subpart": subpart,
                    "authority": authority,
                    "source": source,
                })

        logger.info(f"Extracted {len(sections)} sections from eCFR XML")

        if not sections:
            logger.warning("No sections found in XML, returning empty structure")
            return pl.LazyFrame({
                "section_number": [],
                "section_title": [],
                "section_text": [],
                "part_number": [],
                "subpart": [],
                "authority": [],
                "source": [],
            })

        # Convert to LazyFrame
        df = pl.DataFrame(sections)
        return df.lazy()

    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to parse eCFR XML: {e}")
        raise


def _get_element_text(elem: ET.Element | None) -> str:
    """
    Extract all text content from an XML element and its children.

    Args:
        elem: XML element

    Returns:
        str: Combined text content
    """
    if elem is None:
        return ""

    text_parts = []

    if elem.text:
        text_parts.append(elem.text)

    for child in elem:
        child_text = _get_element_text(child)
        if child_text:
            text_parts.append(child_text)

        if child.tail:
            text_parts.append(child.tail)

    return " ".join(text_parts).strip()


def _find_parent_element(root: ET.Element, target: ET.Element, parent_tag: str) -> ET.Element | None:
    """
    Find parent element with specific tag.

    Args:
        root: Root element to search from
        target: Target element to find parent of
        parent_tag: Tag name of parent to find

    Returns:
        ET.Element | None: Parent element or None
    """
    for parent in root.iter(parent_tag):
        if target in list(parent.iter()):
            return parent
    return None


def extract_section_by_number(file_path: Path, section_number: str) -> dict[str, str]:
    """
    Extract specific section by number from eCFR XML.

    Args:
        file_path: Path to eCFR XML file
        section_number: Section number (e.g., "414.2")

    Returns:
        dict: Section data with keys: section_number, section_title, section_text

    """
    try:
        df = parse_ecfr_xml(file_path, target_section=section_number).collect()

        if df.height > 0:
            row = df.row(0, named=True)
            return {
                "section_number": row["section_number"],
                "section_title": row["section_title"],
                "section_text": row["section_text"],
                "part_number": row["part_number"],
                "subpart": row["subpart"],
                "authority": row["authority"],
                "source": row["source"],
            }

        logger.warning(f"Section {section_number} not found in XML")
        return {}

    except Exception as e:
        logger.error(f"Failed to extract section {section_number}: {e}")
        return {}
