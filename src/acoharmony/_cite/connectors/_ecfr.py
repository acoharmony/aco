# © 2025 HarmonyCares
# All rights reserved.

"""
eCFR (Electronic Code of Federal Regulations) connector for processing ecfr.gov content.

Handles CFR sections, parts, and titles with full metadata extraction.

Features:
- Detects ecfr.gov URLs
- Uses eCFR API for metadata and structure
- Downloads XML for specific titles
- Extracts individual section text
- Handles current and dated versions
"""

from __future__ import annotations

import re
from pathlib import Path

from ._url import host_matches
from typing import TYPE_CHECKING

import polars as pl
import requests

from ..._log import LogWriter
from ..._parsers._ecfr_xml import extract_section_by_number

if TYPE_CHECKING:
    pass

logger = LogWriter("connectors.ecfr")


class ECFRConnector:
    """
    Connector for Electronic Code of Federal Regulations documents.

    Handles ecfr.gov URLs with full metadata extraction
    including section-level citations.
    """

    # eCFR API base URL
    API_BASE = "https://www.ecfr.gov/api/versioner/v1"

    @staticmethod
    def can_handle(url: str) -> bool:
        """
        Check if URL is an eCFR document.

        Args:
            url: URL to check

        Returns:
            True if URL is from ecfr.gov
        """
        return host_matches(url, "ecfr.gov") and (
            "/title-" in url.lower() or "/part-" in url.lower()
        )

    @staticmethod
    def parse_url(url: str) -> dict[str, str | None]:
        """
        Parse eCFR URL to extract title, part, and section.

        URL patterns:
        - https://www.ecfr.gov/current/title-42/section-414.2
        - https://www.ecfr.gov/current/title-42/chapter-IV/part-414/section-414.2
        - https://www.ecfr.gov/on/2024-11-01/title-42/section-414.2

        Args:
            url: eCFR URL

        Returns:
            dict with 'title', 'section', 'part', 'date'
        """
        result = {
            "title": None,
            "section": None,
            "part": None,
            "date": None,
        }

        # Extract title number
        title_match = re.search(r"/title-(\d+)", url)
        if title_match:
            result["title"] = title_match.group(1)

        # Extract section number (e.g., 414.2, 425.100)
        section_match = re.search(r"/section-([0-9.]+)", url)
        if section_match:
            result["section"] = section_match.group(1)

            # Infer part from section (part is before the decimal)
            section_num = section_match.group(1)
            if "." in section_num:
                result["part"] = section_num.split(".")[0]

        # Extract part if explicitly in URL
        part_match = re.search(r"/part-(\d+)", url)
        if part_match:
            result["part"] = part_match.group(1)

        # Extract date if present (versioned URL)
        date_match = re.search(r"/on/(\d{4}-\d{2}-\d{2})", url)
        if date_match:
            result["date"] = date_match.group(1)

        return result

    @staticmethod
    def get_latest_date(title: str) -> str | None:
        """
        Get the latest available date for a CFR title from the versions API.

        Args:
            title: CFR title number (e.g., "42")

        Returns:
            Latest date in YYYY-MM-DD format or None if failed
        """
        api_url = f"{ECFRConnector.API_BASE}/versions/title-{title}.json"

        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Extract all unique dates and return the most recent
            dates = sorted({v["date"] for v in data.get("content_versions", [])})
            if dates:
                latest = dates[-1]
                logger.info(f"Latest available date for CFR Title {title}: {latest}")
                return latest
            return None
        except (requests.RequestException, KeyError, ValueError) as e:
            logger.error(f"Failed to fetch latest eCFR date: {e}")
            return None

    @staticmethod
    def get_latest_structure(title: str, date: str | None = None) -> dict | None:
        """
        Get structure information for a CFR title.

        Args:
            title: CFR title number (e.g., "42")
            date: Optional specific date (YYYY-MM-DD format), fetches latest if not provided

        Returns:
            dict with structure metadata or None if failed
        """
        if not date:
            date = ECFRConnector.get_latest_date(title)
            if not date:
                logger.error("Could not determine latest date for structure API")
                return None

        api_url = f"{ECFRConnector.API_BASE}/structure/{date}/title-{title}.json"

        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch eCFR structure: {e}")
            return None

    @staticmethod
    def construct_xml_url(title: str, date: str | None = None) -> str | None:
        """
        Construct eCFR XML URL for a title.

        Args:
            title: CFR title number (e.g., "42")
            date: Optional specific date (YYYY-MM-DD format), fetches latest if not provided

        Returns:
            str: XML URL or None if date cannot be determined

        """
        if not date:
            date = ECFRConnector.get_latest_date(title)
            if not date:
                logger.error("Could not determine latest date for XML URL")
                return None

        xml_url = f"{ECFRConnector.API_BASE}/full/{date}/title-{title}.xml"
        return xml_url

    @staticmethod
    def download_xml(xml_url: str, save_path: Path, force: bool = False) -> bool:
        """
        Download eCFR XML document with caching support.

        Args:
            xml_url: URL to XML document
            save_path: Path to save XML file
            force: If True, re-download even if file exists

        Returns:
            bool: True if successful (includes cached files)
        """
        # Check if file already exists and is valid
        if not force and save_path.exists():
            file_size = save_path.stat().st_size
            if file_size > 0:
                logger.info(
                    f"Using cached eCFR XML: {save_path} ({file_size:,} bytes) - skipping download"
                )
                return True
            else:
                logger.warning(f"Found empty cached file {save_path}, re-downloading")

        try:
            logger.info(f"Downloading eCFR XML: {xml_url}")
            response = requests.get(xml_url, timeout=120)  # CFR titles can be large
            response.raise_for_status()

            # Ensure parent directory exists
            save_path.parent.mkdir(parents=True, exist_ok=True)

            with open(save_path, "wb") as f:
                f.write(response.content)

            logger.info(f"Downloaded XML to {save_path} ({len(response.content):,} bytes)")
            return True

        except requests.RequestException as e:
            logger.error(f"Failed to download eCFR XML: {e}")
            return False

    @staticmethod
    def process(
        url: str,
        html_path: Path,
        base_citation: pl.DataFrame,
    ) -> list[pl.DataFrame] | None:
        """
        Process eCFR document and generate citations.

        Args:
            url: Source URL
            html_path: Path to downloaded HTML (parent directory used for XML)
            base_citation: Base citation DataFrame

        Returns:
            List of DataFrames (parent + optional section) or None if failed
        """
        logger.info(f"Processing eCFR document: {url}")

        # Parse URL
        url_info = ECFRConnector.parse_url(url)
        title = url_info["title"]
        section = url_info["section"]
        part = url_info["part"]
        date = url_info["date"]

        if not title:
            logger.error(f"Could not parse title from URL: {url}")
            return None

        # Get structure metadata
        structure = ECFRConnector.get_latest_structure(title)
        if not structure:
            logger.warning("Could not fetch structure metadata, continuing without it")
            structure = {}

        citations = []

        # Extract title name from structure
        title_name = structure.get("title", f"Title {title}")
        if isinstance(title_name, dict):
            title_name = title_name.get("name", f"Title {title}")

        # Build parent citation for the CFR title/part
        parent_title = f"{title} CFR"
        if section:
            parent_title = f"{title} CFR § {section}"
        elif part:
            parent_title = f"{title} CFR Part {part}"

        parent_df = base_citation.clone()
        parent_df = parent_df.with_columns(
            [
                pl.lit("Office of the Federal Register").alias("author"),
                pl.lit("National Archives and Records Administration").alias("author_full"),
                pl.lit(parent_title).alias("title"),
                pl.lit(title).alias("cfr_title"),
                pl.lit(part if part else "").alias("cfr_part"),
                pl.lit(section if section else "").alias("cfr_section"),
                pl.lit("ecfr").alias("citation_type"),
                pl.lit(True).alias("is_parent_citation"),
                pl.lit(1 if section else 0).alias("child_count"),
            ]
        )
        citations.append(parent_df)

        # If section-specific, extract section text from XML
        if section:
            logger.info(f"Extracting section {section} from CFR Title {title}")

            # Construct XML URL
            xml_url = ECFRConnector.construct_xml_url(title, date)
            if not xml_url:
                logger.warning("Could not construct XML URL, returning parent citation only")
                return citations

            # Determine XML date for cache filename
            if not date:
                date = ECFRConnector.get_latest_date(title)

            # Store XML in cites/raw/xml/ directory (sibling to html directory)
            # Path structure: cites/raw/xml/ecfr-title-{title}-{date}.xml
            raw_base = html_path.parent.parent  # Go up from cites/raw/html/ to cites/raw/
            xml_dir = raw_base / "xml"
            xml_filename = f"ecfr-title-{title}-{date}.xml" if date else f"ecfr-title-{title}.xml"
            xml_path = xml_dir / xml_filename

            if ECFRConnector.download_xml(xml_url, xml_path):
                # Extract section from XML
                try:
                    section_data = extract_section_by_number(xml_path, section)

                    if section_data and section_data.get("section_text"):
                        logger.info(
                            f"Extracted section {section} from XML ({len(section_data['section_text'])} chars)"
                        )

                        child_df = base_citation.clone()
                        child_df = child_df.with_columns(
                            [
                                pl.lit("Office of the Federal Register").alias("author"),
                                pl.lit(
                                    f"{title} CFR § {section} - {section_data.get('section_title', '')}"
                                ).alias("title"),
                                pl.lit(section_data.get("section_text", "")).alias("content"),
                                pl.lit(section_data.get("section_title", "")).alias("abstract"),
                                pl.lit(title).alias("cfr_title"),
                                pl.lit(section_data.get("part_number", part)).alias("cfr_part"),
                                pl.lit(section).alias("cfr_section"),
                                pl.lit(section_data.get("section_title", "")).alias("section_title"),
                                pl.lit(section_data.get("subpart", "")).alias("subpart"),
                                pl.lit(section_data.get("authority", "")).alias("authority"),
                                pl.lit(section_data.get("source", "")).alias("source_citation"),
                                pl.lit(url).alias("section_url"),
                                pl.lit("ecfr_section").alias("citation_type"),
                                pl.lit(False).alias("is_parent_citation"),
                                pl.lit(1).alias("child_sequence"),
                            ]
                        )
                        citations.append(child_df)
                    else:
                        logger.warning(
                            f"Could not extract section {section}, returning parent only"
                        )

                except Exception as e:
                    logger.error(f"Failed to extract section from XML: {e}")
                    logger.warning("Returning parent citation only")

        logger.info(f"Generated {len(citations)} citations for eCFR document")
        return citations
