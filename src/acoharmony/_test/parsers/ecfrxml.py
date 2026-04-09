"""Tests for acoharmony._parsers._ecfr_xml module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
import xml.etree.ElementTree as ET
import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._ecfr_xml is not None

class TestEcfrXml:
    """Tests for acoharmony._parsers._ecfr_xml."""

    def _make_ecfr_xml(self, tmp_path, sections=None):
        if sections is None:
            sections = [
                {
                    "num": "414.2",
                    "subject": "Definitions",
                    "paragraphs": ["Para 1 text.", "Para 2 text."],
                }
            ]
        root = ET.Element("CFRGRANULE")
        part = ET.SubElement(root, "PART")
        hd = ET.SubElement(part, "HD", SOURCE="HED")
        hd.text = "Part 414"
        subpart = ET.SubElement(part, "SUBPART")
        subpart_hd = ET.SubElement(subpart, "HD", SOURCE="HED")
        subpart_hd.text = "Subpart A"
        auth = ET.SubElement(part, "AUTH")
        auth.text = "42 U.S.C. 1395"
        source = ET.SubElement(part, "SOURCE")
        source.text = "Source citation"
        for sec in sections:
            section = ET.SubElement(subpart, "SECTION", NUM=sec["num"])
            subj = ET.SubElement(section, "SUBJECT")
            subj.text = sec.get("subject", "")
            for para_text in sec.get("paragraphs", []):
                p = ET.SubElement(section, "P")
                p.text = para_text
        p = tmp_path / "ecfr.xml"
        tree = ET.ElementTree(root)
        tree.write(p)
        return p

    @pytest.mark.unit
    def test_parse_ecfr_xml_basic(self, tmp_path):
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        p = self._make_ecfr_xml(tmp_path)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 1
        assert df["section_number"][0] == "414.2"
        assert "Definitions" in df["section_title"][0]
        assert "414" in df["part_number"][0]
        assert "Subpart A" in df["subpart"][0]

    @pytest.mark.unit
    def test_parse_ecfr_xml_target_section(self, tmp_path):
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        p = self._make_ecfr_xml(
            tmp_path,
            sections=[
                {"num": "414.2", "subject": "Definitions", "paragraphs": ["Text."]},
                {"num": "414.10", "subject": "Scope", "paragraphs": ["More text."]},
            ],
        )
        df = parse_ecfr_xml(p, target_section="414.10").collect()
        assert df.height == 1
        assert df["section_number"][0] == "414.10"

    @pytest.mark.unit
    def test_parse_ecfr_xml_empty(self, tmp_path):
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        p = tmp_path / "ecfr.xml"
        root = ET.Element("CFRGRANULE")
        ET.ElementTree(root).write(p)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_get_element_text(self):
        from acoharmony._parsers._ecfr_xml import _get_element_text

        assert _get_element_text(None) == ""
        elem = ET.Element("P")
        elem.text = "Hello"
        assert _get_element_text(elem) == "Hello"
        child = ET.SubElement(elem, "E")
        child.text = "world"
        child.tail = " end"
        assert "world" in _get_element_text(elem)
        assert "end" in _get_element_text(elem)

    @pytest.mark.unit
    def test_find_parent_element(self):
        from acoharmony._parsers._ecfr_xml import _find_parent_element

        root = ET.Element("ROOT")
        part = ET.SubElement(root, "PART")
        section = ET.SubElement(part, "SECTION")
        assert _find_parent_element(root, section, "PART") is part
        assert _find_parent_element(root, section, "NONEXISTENT") is None

    @pytest.mark.unit
    def test_extract_section_by_number(self, tmp_path):
        from acoharmony._parsers._ecfr_xml import extract_section_by_number

        p = self._make_ecfr_xml(tmp_path)
        result = extract_section_by_number(p, "414.2")
        assert result["section_number"] == "414.2"

    @pytest.mark.unit
    def test_extract_section_by_number_not_found(self, tmp_path):
        from acoharmony._parsers._ecfr_xml import extract_section_by_number

        p = self._make_ecfr_xml(tmp_path)
        result = extract_section_by_number(p, "999.99")
        assert result == {}

    @pytest.mark.unit
    def test_extract_section_by_number_error(self, tmp_path):
        from acoharmony._parsers._ecfr_xml import extract_section_by_number

        p = tmp_path / "bad.xml"
        p.write_text("not xml")
        result = extract_section_by_number(p, "414.2")
        assert result == {}

    @pytest.mark.unit
    def test_parse_ecfr_xml_no_text_section(self, tmp_path):
        """Section with number but no paragraph text should not be included."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        p = self._make_ecfr_xml(
            tmp_path, sections=[{"num": "414.2", "subject": "Empty", "paragraphs": []}]
        )
        df = parse_ecfr_xml(p).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_parse_ecfr_xml_no_part_hd(self, tmp_path):
        """Section not inside a PART should still parse with empty part_number."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        root = ET.Element("CFRGRANULE")
        section = ET.SubElement(root, "SECTION", NUM="1.1")
        subj = ET.SubElement(section, "SUBJECT")
        subj.text = "Test"
        p_elem = ET.SubElement(section, "P")
        p_elem.text = "Content"
        p = tmp_path / "ecfr.xml"
        ET.ElementTree(root).write(p)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 1
        assert df["part_number"][0] == ""
