"""Unit tests for Federal Register XML parser coverage gaps."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path

import pytest


class TestFederalRegisterXMLCoverage:
    """Coverage tests for Federal Register XML parser."""

    @pytest.mark.unit
    def test_section_header_text_extraction(self, tmp_path: Path) -> None:
        """Test extraction of section header with text content."""

        # Create XML with section header
        xml_content = """<?xml version="1.0"?>
        <RULE>
            <PREAMB>
                <HD SOURCE="H1">Section 1 Title</HD>
                <P>Paragraph content here.</P>
            </PREAMB>
        </RULE>
        """

        xml_file = tmp_path / "test.xml"
        xml_file.write_text(xml_content)

        result = parse_federal_register_xml(xml_file)
        df = result.collect()

        # Should extract section header
        assert len(df) >= 1
        assert any("Section 1 Title" in str(section) for section in df["section_title"])

    @pytest.mark.unit
    def test_element_tail_text_extraction(self, tmp_path: Path) -> None:
        """Test extraction of tail text after child elements."""

        # Create XML with tail text (text after nested elements)
        xml_content = """<?xml version="1.0"?>
        <RULE>
            <PREAMB>
                <P>Start text <E T="03">emphasized</E> and tail text after element.</P>
            </PREAMB>
        </RULE>
        """

        xml_file = tmp_path / "test_tail.xml"
        xml_file.write_text(xml_content)

        result = parse_federal_register_xml(xml_file)
        df = result.collect()

        # Should extract paragraph including tail text
        assert len(df) >= 1
        # The tail text should be included in the paragraph
        paragraph_texts = [str(text) for text in df["paragraph_text"]]
        assert any("tail text" in text for text in paragraph_texts)


class TestFederalRegisterCoverageGaps:
    """Additional tests for _federal_register_xml coverage gaps."""

    @pytest.mark.unit
    def test_general_exception(self, tmp_path):
        """Cover the generic Exception handler (non-ParseError)."""
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = tmp_path / "fr.xml"
        p.mkdir()
        with pytest.raises(Exception, match=".*"):
            parse_federal_register_xml(p)

    @pytest.mark.unit
    def test_empty_paragraph_text_skipped(self, tmp_path):
        """Cover paragraph with whitespace-only text."""
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = tmp_path / "fr.xml"
        root = ET.Element("RULE", attrib={"doc-number": "DOC1"})
        pe = ET.SubElement(root, "P")
        pe.text = "   "
        pe2 = ET.SubElement(root, "P")
        pe2.text = "Real content"
        ET.ElementTree(root).write(p)
        df = parse_federal_register_xml(p).collect()
        assert df.height == 1


class TestFederalRegisterXml:
    """Tests for acoharmony._parsers._federal_register_xml."""

    def _make_fr_xml(self, tmp_path, paragraphs=None):
        root = ET.Element("RULE", attrib={"doc-number": "2024-12345"})
        hd = ET.SubElement(root, "HD")
        hd.text = "Section Title"
        if paragraphs is None:
            paragraphs = [
                {"tag": "P", "text": "First paragraph.", "attrs": {"id": "p-1"}},
                {"tag": "P", "text": "Second paragraph.", "attrs": {"N": "2"}},
                {"tag": "FP", "text": "Flush paragraph.", "attrs": {}},
            ]
        for para in paragraphs:
            elem = ET.SubElement(root, para["tag"], attrib=para.get("attrs", {}))
            elem.text = para["text"]
        p = tmp_path / "fr.xml"
        ET.ElementTree(root).write(p)
        return p

    @pytest.mark.unit
    def test_parse_federal_register_xml_basic(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = self._make_fr_xml(tmp_path)
        df = parse_federal_register_xml(p).collect()
        assert df.height == 3
        assert "p-1" in df["paragraph_id"].to_list()
        assert df["document_number"][0] == "2024-12345"
        assert df["section_title"][0] == "Section Title"

    @pytest.mark.unit
    def test_parse_federal_register_xml_empty(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = tmp_path / "fr.xml"
        root = ET.Element("RULE")
        ET.ElementTree(root).write(p)
        df = parse_federal_register_xml(p).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_parse_federal_register_xml_no_doc_number(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = tmp_path / "fr_doc.xml"
        root = ET.Element("RULE")
        pe = ET.SubElement(root, "P")
        pe.text = "Content"
        ET.ElementTree(root).write(p)
        df = parse_federal_register_xml(p).collect()
        assert df["document_number"][0] == "fr_doc"

    @pytest.mark.unit
    def test_parse_federal_register_xml_page_attr(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = tmp_path / "fr.xml"
        root = ET.Element("RULE", attrib={"doc-number": "DOC1"})
        pe = ET.SubElement(root, "P", attrib={"page": "42"})
        pe.text = "Page content"
        ET.ElementTree(root).write(p)
        df = parse_federal_register_xml(p).collect()
        assert df["page_number"][0] == "42"

    @pytest.mark.unit
    def test_parse_federal_register_xml_n_attr(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = tmp_path / "fr.xml"
        root = ET.Element("RULE", attrib={"doc-number": "DOC1"})
        pe = ET.SubElement(root, "P", attrib={"N": "5"})
        pe.text = "Numbered paragraph"
        ET.ElementTree(root).write(p)
        df = parse_federal_register_xml(p).collect()
        assert df["paragraph_id"][0] == "p-5"

    @pytest.mark.unit
    def test_get_element_text(self):
        from acoharmony._parsers._federal_register_xml import _get_element_text

        elem = ET.Element("P")
        elem.text = "Start "
        child = ET.SubElement(elem, "E")
        child.text = "emphasis"
        child.tail = " tail"
        result = _get_element_text(elem)
        assert "Start" in result
        assert "emphasis" in result
        assert "tail" in result

    @pytest.mark.unit
    def test_get_element_text_no_text(self):
        from acoharmony._parsers._federal_register_xml import _get_element_text

        elem = ET.Element("P")
        assert _get_element_text(elem) == ""

    @pytest.mark.unit
    def test_extract_paragraph_by_id(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import extract_paragraph_by_id

        p = self._make_fr_xml(tmp_path)
        result = extract_paragraph_by_id(p, "p-1")
        assert "First paragraph" in result

    @pytest.mark.unit
    def test_extract_paragraph_by_id_no_prefix(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import extract_paragraph_by_id

        p = self._make_fr_xml(tmp_path)
        result = extract_paragraph_by_id(p, "1")
        assert "First paragraph" in result

    @pytest.mark.unit
    def test_extract_paragraph_by_id_not_found(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import extract_paragraph_by_id

        p = self._make_fr_xml(tmp_path)
        result = extract_paragraph_by_id(p, "p-999")
        assert result == ""

    @pytest.mark.unit
    def test_extract_paragraph_by_id_error(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import extract_paragraph_by_id

        p = tmp_path / "bad.xml"
        p.write_text("not xml")
        result = extract_paragraph_by_id(p, "p-1")
        assert result == ""

    @pytest.mark.unit
    def test_parse_federal_register_xml_invalid_xml(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = tmp_path / "bad.xml"
        p.write_text("not xml")
        with pytest.raises(ET.ParseError):
            parse_federal_register_xml(p)

    @pytest.mark.unit
    def test_parse_federal_register_xml_hd_variants(self, tmp_path):
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = tmp_path / "fr.xml"
        root = ET.Element("RULE", attrib={"doc-number": "DOC1"})
        for tag in ["HD1", "HD2", "HD3"]:
            hd = ET.SubElement(root, tag)
            hd.text = f"Header {tag}"
        pe = ET.SubElement(root, "P")
        pe.text = "Content"
        ET.ElementTree(root).write(p)
        df = parse_federal_register_xml(p).collect()
        assert df["section_title"][0] == "Header HD3"

    @pytest.mark.unit
    def test_empty_header_does_not_update_section(self, tmp_path):
        """Cover branch 73->78: HD element with empty text is skipped."""
        from acoharmony._parsers._federal_register_xml import parse_federal_register_xml

        p = tmp_path / "fr.xml"
        root = ET.Element("RULE", attrib={"doc-number": "DOC1"})
        # First header with text sets the section
        hd1 = ET.SubElement(root, "HD")
        hd1.text = "Real Section"
        # Second header with NO text — should not overwrite current_section
        hd2 = ET.SubElement(root, "HD")
        # hd2 has no .text at all, so _get_element_text returns ""
        pe = ET.SubElement(root, "P")
        pe.text = "Content after empty header"
        ET.ElementTree(root).write(p)
        df = parse_federal_register_xml(p).collect()
        # The section should still be "Real Section", not ""
        assert df["section_title"][0] == "Real Section"

    @pytest.mark.unit
    def test_get_element_text_multiple_children_empty_first(self):
        """Cover branches 151->155 and 155->149: empty child with no tail followed by another child."""
        from acoharmony._parsers._federal_register_xml import _get_element_text

        elem = ET.Element("P")
        elem.text = "Start "
        # First child: no text, no tail -> child_text is falsy (151->155),
        # child.tail is falsy (155 is false), then loop back to 149
        child1 = ET.SubElement(elem, "BR")
        # child1.text is None, child1.tail is None
        # Second child: has text so the loop iterates at least twice
        child2 = ET.SubElement(elem, "E")
        child2.text = "second"
        child2.tail = " end"
        result = _get_element_text(elem)
        assert "Start" in result
        assert "second" in result
        assert "end" in result
