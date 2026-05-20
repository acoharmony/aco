# © 2025 HarmonyCares
# All rights reserved.

"""
Federal Register connector for processing federalregister.gov content.

Handles Federal Register documents including rules, proposed rules, notices, etc.
Supports both document-level and paragraph-level citations.

Features:
- Detects federalregister.gov URLs
- Uses Federal Register API for metadata extraction
- Handles paragraph-level citations (e.g., /d/2024-25382/p-45)
- Extracts CFR references, agency info, docket numbers, RINs
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import requests

from ._url import host_matches
from bs4 import BeautifulSoup

from ..._log import LogWriter
from ..._parsers._federal_register_xml import extract_paragraph_by_id

if TYPE_CHECKING:
    pass

logger = LogWriter("connectors.federal_register")


class FederalRegisterConnector:
    """
    Connector for Federal Register documents.

    Handles federalregister.gov URLs with full metadata extraction
    including paragraph-level citations.
    """

    # Federal Register API base URL
    API_BASE = "https://www.federalregister.gov/api/v1"

    @staticmethod
    def can_handle(url: str) -> bool:
        """
        Check if URL is a Federal Register document.

        Args:
            url: URL to check

        Returns:
            True if URL is from federalregister.gov
        """
        return host_matches(url, "federalregister.gov") and "/d/" in url.lower()

    @staticmethod
    def parse_url(url: str) -> dict[str, str | None]:
        """
        Parse Federal Register URL to extract document number and paragraph.

        URL patterns:
        - https://www.federalregister.gov/d/2024-25382
        - https://www.federalregister.gov/d/2024-25382/p-45

        Args:
            url: Federal Register URL

        Returns:
            dict with 'document_number' and optional 'paragraph_number'
        """
        # Extract document number
        doc_match = re.search(r"/d/([A-Za-z0-9-]+)", url)
        if not doc_match:
            return {"document_number": None, "paragraph_number": None}

        document_number = doc_match.group(1)

        # Extract paragraph number if present
        para_match = re.search(r"/p-(\d+)", url)
        paragraph_number = para_match.group(1) if para_match else None

        return {
            "document_number": document_number,
            "paragraph_number": paragraph_number,
        }

    @staticmethod
    def fetch_document_metadata(document_number: str) -> dict | None:
        """
        Fetch document metadata from Federal Register API.

        Args:
            document_number: Document number (e.g., "2024-25382")

        Returns:
            dict with document metadata or None if failed
        """
        api_url = f"{FederalRegisterConnector.API_BASE}/documents/{document_number}.json"

        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Federal Register metadata: {e}")
            return None

    @staticmethod
    def construct_xml_url(document_number: str, publication_date: str) -> str:
        """
        Construct Federal Register XML URL from document number and publication date.

        Args:
            document_number: Document number (e.g., "2024-25382")
            publication_date: Publication date in YYYY-MM-DD format

        Returns:
            str: XML URL

        """
        try:
            # Parse date
            date_obj = datetime.strptime(publication_date, "%Y-%m-%d")
            year = date_obj.year
            month = f"{date_obj.month:02d}"
            day = f"{date_obj.day:02d}"

            xml_url = f"https://www.federalregister.gov/documents/full_text/xml/{year}/{month}/{day}/{document_number}.xml"
            return xml_url

        except Exception as e:
            logger.error(f"Failed to construct XML URL: {e}")
            return ""

    @staticmethod
    def download_xml(xml_url: str, save_path: Path) -> bool:
        """
        Download Federal Register XML document with caching.

        Checks if XML file already exists before downloading.
        Maintains inventory of downloaded documents to avoid re-downloading.

        Args:
            xml_url: URL to XML document
            save_path: Path to save XML file

        Returns:
            bool: True if successful (either downloaded or cached)
        """
        try:
            # Check if XML already exists (download cache)
            if save_path.exists():
                file_size = save_path.stat().st_size
                logger.info(
                    f"Using cached Federal Register XML: {save_path.name} (size: {file_size} bytes)"
                )
                return True

            # Need to download
            logger.info(f"Downloading Federal Register XML: {xml_url}")
            response = requests.get(xml_url, timeout=30)
            response.raise_for_status()

            with open(save_path, "wb") as f:
                f.write(response.content)

            file_size = save_path.stat().st_size
            logger.info(f"Downloaded XML to {save_path} (size: {file_size} bytes)")
            return True

        except requests.RequestException as e:
            logger.error(f"Failed to download XML: {e}")
            return False

    @staticmethod
    def extract_paragraph_text(html_path: Path, paragraph_number: str) -> str:
        """
        Extract text from specific paragraph in Federal Register document.

        Federal Register uses multiple paragraph numbering schemes:
        - Full text HTML: Sequential paragraph numbers (p-1, p-2, p-3, etc.)
        - Print view: Page-level paragraphs

        This extracts the full document text if specific paragraph not found,
        as Federal Register documents are meant to be cited as wholes.

        Args:
            html_path: Path to downloaded HTML
            paragraph_number: Paragraph number to extract

        Returns:
            str: Paragraph text or full document excerpt
        """
        try:
            with open(html_path, encoding="utf-8", errors="replace") as f:
                html_content = f.read()

            soup = BeautifulSoup(html_content, "html.parser")

            # Try multiple paragraph identification patterns
            selectors = [
                f"p[id='p-{paragraph_number}']",
                f"[id='p-{paragraph_number}']",
                f"[data-page='{paragraph_number}']",
                f"p.paragraph[data-number='{paragraph_number}']",
            ]

            for selector in selectors:
                elements = soup.select(selector)
                if elements:
                    text = elements[0].get_text(strip=True, separator=" ")
                    if text:
                        return text

            # Federal Register paragraph URLs often redirect to document with anchor
            # If specific paragraph not found, extract from full text div
            full_text_div = soup.find("div", class_="full-text")
            if full_text_div:
                # Get all paragraphs and try Nth paragraph (1-indexed)
                paragraphs = full_text_div.find_all("p")
                try:
                    para_idx = int(paragraph_number) - 1
                except (ValueError, TypeError):
                    para_idx = -1
                if paragraphs and 0 <= para_idx < len(paragraphs):
                    text = paragraphs[para_idx].get_text(strip=True, separator=" ")
                    if text:
                        return text

            # Final fallback: return first substantial paragraph as context
            logger.info(
                f"Specific paragraph {paragraph_number} not found, extracting document excerpt"
            )
            paragraphs = soup.find_all("p", class_="")
            for para in paragraphs:
                text = para.get_text(strip=True)
                if len(text) > 100:  # Substantial paragraph
                    return f"[Document excerpt - full document should be cited] {text[:500]}..."

            logger.warning(f"Could not extract any substantial text for paragraph {paragraph_number}")
            return ""

        except Exception as e:
            logger.error(f"Failed to extract paragraph text: {e}")
            return ""

    @staticmethod
    def process(
        url: str,
        html_path: Path,
        base_citation: pl.DataFrame,
    ) -> list[pl.DataFrame] | None:
        """
        Process Federal Register document and generate citations.

        Args:
            url: Source URL
            html_path: Path to downloaded HTML
            base_citation: Base citation DataFrame

        Returns:
            List of DataFrames (parent + optional paragraph) or None if failed
        """
        logger.info(f"Processing Federal Register document: {url}")

        # Parse URL
        url_info = FederalRegisterConnector.parse_url(url)
        document_number = url_info["document_number"]
        paragraph_number = url_info["paragraph_number"]

        if not document_number:
            logger.error(f"Could not parse document number from URL: {url}")
            return None

        # Fetch metadata from API
        metadata = FederalRegisterConnector.fetch_document_metadata(document_number)
        if not metadata:
            logger.error(f"Could not fetch metadata for document: {document_number}")
            return None

        citations = []

        # Extract metadata fields
        agencies = metadata.get("agencies", [])
        agency_names = [a.get("name", "") for a in agencies] if agencies else []

        docket_ids = metadata.get("docket_ids", [])
        cfr_references = metadata.get("cfr_references", [])
        cfr_titles = []
        for cfr in cfr_references:
            title = cfr.get("title")
            part = cfr.get("part")
            if title and part:
                cfr_titles.append(f"{title} CFR {part}")

        document_citation = metadata.get("citation", "")
        document_type = metadata.get("type", "")
        title = metadata.get("title", "")
        abstract = metadata.get("abstract", "")
        publication_date = metadata.get("publication_date", "")
        start_page = metadata.get("start_page", "")
        end_page = metadata.get("end_page", "")
        page_count = (
            int(end_page) - int(start_page) + 1
            if start_page and end_page
            else None
        )

        regulation_id_numbers = metadata.get("regulation_id_numbers", [])
        html_url = metadata.get("html_url", "")
        pdf_url = metadata.get("pdf_url", "")

        # Build parent citation
        parent_df = base_citation.clone()
        parent_df = parent_df.with_columns(
            [
                pl.lit(", ".join(agency_names)).alias("author"),
                pl.lit(agency_names[0] if agency_names else "").alias("author_primary"),
                pl.lit(title).alias("title"),
                pl.lit(abstract).alias("abstract"),
                pl.lit(document_number).alias("document_number"),
                pl.lit(document_citation).alias("document_citation"),
                pl.lit(document_type).alias("document_type"),
                pl.lit(publication_date).alias("publication_date"),
                pl.lit(start_page).alias("start_page"),
                pl.lit(end_page).alias("end_page"),
                pl.lit(page_count).alias("page_count"),
                pl.lit(", ".join(docket_ids)).alias("docket_ids"),
                pl.lit(", ".join(cfr_titles)).alias("cfr_references"),
                pl.lit(", ".join(regulation_id_numbers)).alias("regulation_id_numbers"),
                pl.lit(html_url).alias("html_url"),
                pl.lit(pdf_url).alias("pdf_url"),
                pl.lit("federal_register").alias("citation_type"),
                pl.lit(True).alias("is_parent_citation"),
                pl.lit(1 if paragraph_number else 0).alias("child_count"),
            ]
        )
        citations.append(parent_df)

        # If paragraph-specific, create child citation
        if paragraph_number:
            logger.info(f"Extracting paragraph {paragraph_number}")

            # Try to get paragraph text from XML (preferred method)
            paragraph_text = ""

            # Construct XML URL from metadata
            xml_url = FederalRegisterConnector.construct_xml_url(
                document_number, publication_date
            )

            if xml_url:
                # Download XML to temp location
                xml_path = html_path.parent / f"{document_number}.xml"
                if FederalRegisterConnector.download_xml(xml_url, xml_path):
                    # Extract paragraph from XML
                    try:
                        paragraph_text = extract_paragraph_by_id(xml_path, paragraph_number)
                        logger.info(
                            f"Extracted paragraph {paragraph_number} from XML ({len(paragraph_text)} chars)"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to extract from XML: {e}, trying HTML fallback")
                        paragraph_text = ""

            # Fallback to HTML if XML extraction failed
            if not paragraph_text:
                logger.info("Trying HTML fallback for paragraph extraction")
                paragraph_text = FederalRegisterConnector.extract_paragraph_text(
                    html_path, paragraph_number
                )

            if paragraph_text:
                child_df = base_citation.clone()
                child_df = child_df.with_columns(
                    [
                        # Author information
                        pl.lit(", ".join(agency_names)).alias("author"),
                        pl.lit(agency_names[0] if agency_names else "").alias("author_primary"),
                        # Title and content - paragraph-specific
                        pl.lit(
                            f"{title} - Paragraph {paragraph_number}"
                        ).alias("title"),
                        pl.lit(paragraph_text).alias("content"),
                        pl.lit(paragraph_text).alias("abstract"),
                        # Document identifiers - inherited from parent
                        pl.lit(document_number).alias("document_number"),
                        pl.lit(paragraph_number).alias("paragraph_number"),
                        pl.lit(document_citation).alias("document_citation"),
                        pl.lit(document_type).alias("document_type"),
                        # Publication metadata - inherited from parent
                        pl.lit(publication_date).alias("publication_date"),
                        pl.lit(start_page).alias("start_page"),
                        pl.lit(end_page).alias("end_page"),
                        pl.lit(page_count).alias("page_count"),
                        # Regulatory metadata - inherited from parent
                        pl.lit(", ".join(docket_ids)).alias("docket_ids"),
                        pl.lit(", ".join(cfr_titles)).alias("cfr_references"),
                        pl.lit(", ".join(regulation_id_numbers)).alias("regulation_id_numbers"),
                        # URLs - paragraph URL + parent URLs
                        pl.lit(url).alias("paragraph_url"),
                        pl.lit(html_url).alias("parent_url"),
                        pl.lit(html_url).alias("html_url"),
                        pl.lit(pdf_url).alias("pdf_url"),
                        # Citation structure
                        pl.lit("federal_register_paragraph").alias("citation_type"),
                        pl.lit(False).alias("is_parent_citation"),
                        pl.lit(1).alias("child_sequence"),
                    ]
                )
                citations.append(child_df)
            else:
                logger.warning(
                    f"Could not extract paragraph {paragraph_number}, returning parent only"
                )

        logger.info(f"Generated {len(citations)} citations for Federal Register document")
        return citations
