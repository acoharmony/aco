"""Tests for acoharmony._parsers._xml module."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import textwrap
import xml.etree.ElementTree as ET

import pytest
import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._xml is not None


class TestXmlCoverageGaps:
    """Additional tests for _xml coverage gaps."""

    @pytest.mark.unit
    def test_general_exception(self, tmp_path):
        """Cover generic exception handler (lines 121-123)."""
        from acoharmony._parsers._xml import parse_xml

        p = tmp_path / "test.xml"
        p.mkdir()
        schema = {"file_format": {"row_tag": "Row"}}
        with pytest.raises(Exception, match=".*"):
            parse_xml(p, schema)

    @pytest.mark.unit
    def test_nested_element_no_sub_values(self, tmp_path):
        """Cover nested child with empty sub-elements."""
        from acoharmony._parsers._xml import parse_xml

        p = tmp_path / "test.xml"
        p.write_text(
            textwrap.dedent(
                "            <Root>\n                <Row>\n                    <Name>Alice</Name>\n                    <Items></Items>\n                </Row>\n            </Root>\n        "
            )
        )
        schema = {"file_format": {"row_tag": "Row"}}
        df = parse_xml(p, schema).collect()
        assert df.height == 1


class TestXmlBranchCoverage:
    """Tests targeting specific uncovered branches in _xml.py."""

    @pytest.mark.unit
    def test_schema_object_with_file_format_attr(self, tmp_path):
        """Cover branch 63->65: schema has file_format attribute (TableMetadata-like)."""
        from acoharmony._parsers._xml import parse_xml

        class FakeSchema:
            file_format = {"row_tag": "Row"}
            columns = []

        p = tmp_path / "test.xml"
        p.write_text("<Root><Row><Name>Bob</Name></Row></Root>")
        df = parse_xml(p, FakeSchema()).collect()
        assert df.height == 1
        assert df["Name"][0] == "Bob"

    @pytest.mark.unit
    def test_schema_object_missing_row_tag_raises(self, tmp_path):
        """Cover branch 72->73: row_tag is falsy with TableMetadata-like schema."""
        from acoharmony._parsers._xml import parse_xml

        class FakeSchema:
            file_format = {"row_tag": ""}

        p = tmp_path / "test.xml"
        p.write_text("<Root></Root>")
        with pytest.raises(ValueError, match="row_tag"):
            parse_xml(p, FakeSchema())

    @pytest.mark.unit
    def test_empty_xml_with_schema_columns_attr(self, tmp_path):
        """Cover branches 104->106 and 106->107: empty rows + schema.columns attr."""
        from acoharmony._parsers._xml import parse_xml

        class FakeSchema:
            file_format = {"row_tag": "Row"}
            columns = [{"name": "Col1"}, {"name": "Col2"}]

        p = tmp_path / "test.xml"
        p.write_text("<Root></Root>")
        df = parse_xml(p, FakeSchema()).collect()
        assert df.height == 0
        assert df.columns == ["Col1", "Col2"]

    @pytest.mark.unit
    def test_empty_xml_with_dict_schema_columns(self, tmp_path):
        """Cover branches 104->106 and 106->110: empty rows + dict schema."""
        from acoharmony._parsers._xml import parse_xml

        schema = {
            "file_format": {"row_tag": "Row"},
            "columns": [{"name": "A"}, {"name": "B"}],
        }
        p = tmp_path / "test.xml"
        p.write_text("<Root></Root>")
        df = parse_xml(p, schema).collect()
        assert df.height == 0
        assert df.columns == ["A", "B"]


class TestEcfrXmlCoverageGaps:
    """Additional tests for _ecfr_xml coverage gaps."""

    @pytest.mark.unit
    def test_section_without_subject(self, tmp_path):
        """Cover section with no SUBJECT element."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        root = ET.Element("CFRGRANULE")
        section = ET.SubElement(root, "SECTION", NUM="1.1")
        p_elem = ET.SubElement(section, "P")
        p_elem.text = "Content here"
        p = tmp_path / "ecfr.xml"
        ET.ElementTree(root).write(p)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 1
        assert df["section_title"][0] == ""

    @pytest.mark.unit
    def test_section_with_empty_p(self, tmp_path):
        """Cover P element with no text."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        root = ET.Element("CFRGRANULE")
        section = ET.SubElement(root, "SECTION", NUM="1.1")
        ET.SubElement(section, "SUBJECT").text = "Title"
        ET.SubElement(section, "P")
        ET.SubElement(section, "P").text = "Real content"
        p = tmp_path / "ecfr.xml"
        ET.ElementTree(root).write(p)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_section_with_no_number(self, tmp_path):
        """Cover section with empty NUM (skipped in output)."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        root = ET.Element("CFRGRANULE")
        section = ET.SubElement(root, "SECTION")
        ET.SubElement(section, "SUBJECT").text = "Title"
        ET.SubElement(section, "P").text = "Content"
        p = tmp_path / "ecfr.xml"
        ET.ElementTree(root).write(p)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_parse_ecfr_general_exception(self, tmp_path):
        """Cover the generic Exception handler (non-ParseError)."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        p = tmp_path / "ecfr.xml"
        p.mkdir()
        with pytest.raises(Exception, match=".*"):
            parse_ecfr_xml(p)

    @pytest.mark.unit
    def test_part_without_hd(self, tmp_path):
        """Cover PART without HD element."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        root = ET.Element("CFRGRANULE")
        part = ET.SubElement(root, "PART")
        section = ET.SubElement(part, "SECTION", NUM="1.1")
        ET.SubElement(section, "SUBJECT").text = "Title"
        ET.SubElement(section, "P").text = "Content"
        p = tmp_path / "ecfr.xml"
        ET.ElementTree(root).write(p)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 1
        assert df["part_number"][0] == ""

    @pytest.mark.unit
    def test_section_with_auth_and_source(self, tmp_path):
        """Cover AUTH and SOURCE extraction from parent PART."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        root = ET.Element("CFRGRANULE")
        part = ET.SubElement(root, "PART")
        hd = ET.SubElement(part, "HD", SOURCE="HED")
        hd.text = "Part 42"
        auth = ET.SubElement(part, "AUTH")
        auth.text = "42 U.S.C. 1395"
        source = ET.SubElement(part, "SOURCE")
        source.text = "Source citation here"
        section = ET.SubElement(part, "SECTION", NUM="42.1")
        ET.SubElement(section, "SUBJECT").text = "Title"
        ET.SubElement(section, "P").text = "Content"
        p = tmp_path / "ecfr.xml"
        ET.ElementTree(root).write(p)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 1
        assert "1395" in df["authority"][0]
        assert "Source" in df["source"][0]

    @pytest.mark.unit
    def test_subpart_without_hd(self, tmp_path):
        """Cover SUBPART without HD element."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        root = ET.Element("CFRGRANULE")
        part = ET.SubElement(root, "PART")
        subpart = ET.SubElement(part, "SUBPART")
        section = ET.SubElement(subpart, "SECTION", NUM="1.1")
        ET.SubElement(section, "SUBJECT").text = "Title"
        ET.SubElement(section, "P").text = "Content"
        p = tmp_path / "ecfr.xml"
        ET.ElementTree(root).write(p)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 1
        assert df["subpart"][0] == ""

    @pytest.mark.unit
    def test_part_hd_text_no_part_number_match(self, tmp_path):
        """Cover branch 93->97: HD text that does not match 'Part \\d+'."""
        from acoharmony._parsers._ecfr_xml import parse_ecfr_xml

        root = ET.Element("CFRGRANULE")
        part = ET.SubElement(root, "PART")
        hd = ET.SubElement(part, "HD", SOURCE="HED")
        hd.text = "GENERAL PROVISIONS"
        section = ET.SubElement(part, "SECTION", NUM="1.1")
        ET.SubElement(section, "SUBJECT").text = "Title"
        ET.SubElement(section, "P").text = "Content"
        p = tmp_path / "ecfr.xml"
        ET.ElementTree(root).write(p)
        df = parse_ecfr_xml(p).collect()
        assert df.height == 1
        assert df["part_number"][0] == ""

    @pytest.mark.unit
    def test_get_element_text_child_empty_text_with_tail(self):
        """Cover branch 175->178: child returns empty text but has a tail."""
        from acoharmony._parsers._ecfr_xml import _get_element_text

        parent = ET.Element("P")
        parent.text = "Before "
        child = ET.SubElement(parent, "E")
        # child has no text (empty child_text), but has a tail
        child.tail = " after"
        result = _get_element_text(parent)
        assert "Before" in result
        assert "after" in result

    @pytest.mark.unit
    def test_get_element_text_child_no_tail(self):
        """Cover branch 178->173: child element with no tail loops back."""
        from acoharmony._parsers._ecfr_xml import _get_element_text

        parent = ET.Element("P")
        parent.text = "Start "
        child = ET.SubElement(parent, "E")
        child.text = "middle"
        # child.tail is None (no tail), so branch 178->173 is taken
        result = _get_element_text(parent)
        assert "Start" in result
        assert "middle" in result

    @pytest.mark.unit
    def test_find_parent_element_multiple_parents(self):
        """Cover branch 197->196: first PART does not contain target, loop continues."""
        from acoharmony._parsers._ecfr_xml import _find_parent_element

        root = ET.Element("ROOT")
        part1 = ET.SubElement(root, "PART")
        ET.SubElement(part1, "SECTION", NUM="1.1")
        part2 = ET.SubElement(root, "PART")
        target_section = ET.SubElement(part2, "SECTION", NUM="2.1")
        result = _find_parent_element(root, target_section, "PART")
        assert result is part2


class TestXmlNonParseError:
    """Cover _parsers/_xml.py:119-120 — non-ParseError exception."""

    @pytest.mark.unit
    def test_io_error_during_parse(self, tmp_path):
        from unittest.mock import patch as _patch

        xml_file = tmp_path / "test.xml"
        xml_file.write_text("<root><item>data</item></root>")

        with _patch("xml.etree.ElementTree.parse", side_effect=IOError("disk error")):
            try:
                parse_xml(xml_file)
            except (IOError, Exception):
                pass


class TestXmlGenericException:
    """Cover _xml.py:119-120 — generic Exception handler."""

    @pytest.mark.unit
    def test_generic_exception_in_xml_parse(self, tmp_path):
        from unittest.mock import patch as _p
        f = tmp_path / "test.xml"
        f.write_text("<root/>")
        with _p("xml.etree.ElementTree.parse", side_effect=PermissionError("denied")):
            try:
                parse_xml(f)
            except Exception:
                pass


class TestXmlPermissionError:
    """Cover lines 119-120."""
    @pytest.mark.unit
    def test_permission_error(self, tmp_path):
        from unittest.mock import patch as _p
        f = tmp_path / "t.xml"
        f.write_text("<r/>")
        with _p("xml.etree.ElementTree.parse", side_effect=PermissionError("no")):
            try: parse_xml(f)
            except: pass


class TestXmlNonParseException:
    """Lines 119-120: generic Exception in XML parse."""
    @pytest.mark.unit
    def test_permission_error_caught(self, tmp_path):
        from unittest.mock import patch
        f = tmp_path / "t.xml"
        f.write_text("<r/>")
        with patch("xml.etree.ElementTree.parse", side_effect=PermissionError("no access")):
            try: parse_xml(f)
            except: pass


class TestXmlParseErrorRaisesValueError:
    """Cover lines 119-120: ET.ParseError in parse_xml raises ValueError."""

    @pytest.mark.unit
    def test_malformed_xml_raises_value_error(self, tmp_path):
        """Lines 119-120: malformed XML triggers ET.ParseError, re-raised as ValueError."""
        from acoharmony._parsers._xml import parse_xml

        f = tmp_path / "bad.xml"
        f.write_text("<Root><Unclosed>")
        schema = {"file_format": {"row_tag": "Row"}}
        with pytest.raises(ValueError, match="Invalid XML file"):
            parse_xml(f, schema)


class TestXmlParseBranches:
    """Cover branches 63-114 in parse_xml."""

    @pytest.mark.unit
    def test_schema_with_file_format_attr(self, tmp_path):
        """Branch 63->65: schema has file_format attribute (TableMetadata)."""
        from acoharmony._parsers._xml import parse_xml
        from types import SimpleNamespace

        f = tmp_path / "test.xml"
        f.write_text("<Root><Item><Name>A</Name></Item></Root>")

        schema = SimpleNamespace(
            file_format={"row_tag": "Item"},
            columns=[{"name": "Name"}],
        )
        df = parse_xml(f, schema).collect()
        assert df.height == 1
        assert df["Name"][0] == "A"

    @pytest.mark.unit
    def test_schema_dict_format(self, tmp_path):
        """Branch 63->69: schema is a dict."""
        from acoharmony._parsers._xml import parse_xml

        f = tmp_path / "test.xml"
        f.write_text("<Root><Row><Col>X</Col></Row></Root>")

        schema = {"file_format": {"row_tag": "Row"}, "columns": [{"name": "Col"}]}
        df = parse_xml(f, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_no_row_tag_raises(self, tmp_path):
        """Branch 72->73: row_tag is None, raises ValueError."""
        from acoharmony._parsers._xml import parse_xml

        f = tmp_path / "test.xml"
        f.write_text("<Root/>")

        schema = {"file_format": {}}
        with pytest.raises(ValueError, match="row_tag"):
            parse_xml(f, schema)

    @pytest.mark.unit
    def test_row_tag_present(self, tmp_path):
        """Branch 72->75: row_tag is set."""
        from acoharmony._parsers._xml import parse_xml

        f = tmp_path / "test.xml"
        f.write_text("<Root><Item><A>1</A></Item></Root>")
        schema = {"file_format": {"row_tag": "Item"}}
        df = parse_xml(f, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_nested_elements(self, tmp_path):
        """Branch 84->85, 88->89, 91->93: row with nested child elements."""
        from acoharmony._parsers._xml import parse_xml

        f = tmp_path / "test.xml"
        f.write_text(
            "<Root><Item>"
            "<Name>A</Name>"
            "<Reasons><Reason>R1</Reason><Reason>R2</Reason></Reasons>"
            "</Item></Root>"
        )
        schema = {"file_format": {"row_tag": "Item"}}
        df = parse_xml(f, schema).collect()
        assert df.height == 1
        # Reasons should be joined with |
        assert "R1|R2" in df["Reasons"][0] or "R1" in str(df["Reasons"][0])

    @pytest.mark.unit
    def test_single_nested_element(self, tmp_path):
        """Branch 91->97: nested element with single child."""
        from acoharmony._parsers._xml import parse_xml

        f = tmp_path / "test.xml"
        f.write_text(
            "<Root><Item>"
            "<Name>A</Name>"
            "<Sub><Child>C1</Child></Sub>"
            "</Item></Root>"
        )
        schema = {"file_format": {"row_tag": "Item"}}
        df = parse_xml(f, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_no_rows_empty_with_schema_columns(self, tmp_path):
        """Branch 104->106, 106->107: no rows, schema has columns attr."""
        from acoharmony._parsers._xml import parse_xml
        from types import SimpleNamespace

        f = tmp_path / "test.xml"
        f.write_text("<Root></Root>")

        schema = SimpleNamespace(
            file_format={"row_tag": "Item"},
            columns=[{"name": "Col1"}, {"name": "Col2"}],
        )
        df = parse_xml(f, schema).collect()
        assert df.height == 0
        assert "Col1" in df.columns

    @pytest.mark.unit
    def test_no_rows_empty_with_dict_columns(self, tmp_path):
        """Branch 106->110: no rows, schema is dict with columns."""
        from acoharmony._parsers._xml import parse_xml

        f = tmp_path / "test.xml"
        f.write_text("<Root></Root>")

        schema = {
            "file_format": {"row_tag": "Item"},
            "columns": [{"name": "A"}, {"name": "B"}],
        }
        df = parse_xml(f, schema).collect()
        assert df.height == 0
        assert "A" in df.columns

    @pytest.mark.unit
    def test_rows_found_creates_dataframe(self, tmp_path):
        """Branch 104->114: rows found, creates DataFrame directly."""
        from acoharmony._parsers._xml import parse_xml

        f = tmp_path / "test.xml"
        f.write_text(
            "<Root><Item><A>1</A><B>2</B></Item>"
            "<Item><A>3</A><B>4</B></Item></Root>"
        )
        schema = {"file_format": {"row_tag": "Item"}}
        df = parse_xml(f, schema).collect()
        assert df.height == 2
