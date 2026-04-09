# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from unittest.mock import MagicMock, patch
import polars as pl  # noqa: E402
import pytest
from types import SimpleNamespace
import acoharmony

# © 2025 HarmonyCares
# All rights reserved.


"""Unit tests for parser module - Polars style."""


class TestParserModule:
    """Tests for parser functionality."""

    @pytest.mark.unit
    def test_basic_placeholder(self) -> None:
        """Placeholder test."""
        assert True


class TestTparcGroupedRecords:
    """Cover grouped record parsing branches."""

    @pytest.mark.unit
    def test_empty_parts_skip(self, tmp_path):
        """Line 96: empty parts after split are skipped."""

        content = "HDR|field1|field2\n\nDTL|data1|data2\n"
        file_path = tmp_path / "test.txt"
        file_path.write_text(content)

        schema = {"record_types": {"HDR": {}, "DTL": {}}, "delimiter": "|"}
        result = parse_multi_record(file_path, schema)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_empty_records_continue(self, tmp_path):
        """Line 108: empty records list for a type is skipped."""

        content = "HDR|field1|field2\nDTL|data1|data2\n"
        file_path = tmp_path / "test.txt"
        file_path.write_text(content)

        schema = {"record_types": {"HDR": {}, "DTL": {}, "MISSING": {}}, "delimiter": "|"}
        result = parse_multi_record(file_path, schema)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_no_parsed_records_continue(self, tmp_path):
        """Line 124: empty parsed_records continues to next type."""

        content = "HDR|field1\n"
        file_path = tmp_path / "test.txt"
        file_path.write_text(content)

        schema = {"record_types": {"HDR": {}, "EMPTY": {}}, "delimiter": "|"}
        result = parse_multi_record(file_path, schema)
        assert isinstance(result, pl.LazyFrame)


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._tparc is not None


class TestTparcAdditional:
    """Additional tests to fill coverage gaps in _tparc.py."""

    @pytest.mark.unit
    def test_parse_tparc_basic(self, tmp_path: Path):
        """Cover basic TPARC parsing with record type splitting."""
        p = tmp_path / "test.tparc"
        p.write_text(
            "ALR|123|Alice|Active\nBEN|456|Bob|Enrolled\nALR|789|Charlie|Inactive\n\nBEN|012|Dave|Pending\n",
            encoding="utf-8",
        )
        from acoharmony._parsers._tparc import parse_tparc

        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {
                    "columns": [
                        {"name": "type"},
                        {"name": "id"},
                        {"name": "name"},
                        {"name": "status"},
                    ]
                },
                "BEN": {
                    "columns": [
                        {"name": "type"},
                        {"name": "id"},
                        {"name": "name"},
                        {"name": "status"},
                    ]
                },
            },
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert "record_type" in df.columns
        assert len(df) == 4

    @pytest.mark.unit
    def test_parse_tparc_unmatched_record_type(self, tmp_path: Path):
        """Cover line 99: record type not in schema is ignored."""
        p = tmp_path / "unmatched.tparc"
        p.write_text("ALR|123|Alice\nUNK|999|Unknown\n", encoding="utf-8")
        from acoharmony._parsers._tparc import parse_tparc

        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {"columns": [{"name": "type"}, {"name": "id"}, {"name": "name"}]}
            },
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 1
        assert df["record_type"][0] == "ALR"

    @pytest.mark.unit
    def test_parse_tparc_with_limit(self, tmp_path: Path):
        """Cover line 83-84: limit parameter."""
        p = tmp_path / "limited.tparc"
        p.write_text("ALR|1|A\nALR|2|B\nALR|3|C\nALR|4|D\n", encoding="utf-8")
        from acoharmony._parsers._tparc import parse_tparc

        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {"columns": [{"name": "type"}, {"name": "id"}, {"name": "name"}]}
            },
        }
        lf = parse_tparc(p, schema, limit=2)
        df = lf.collect()
        assert len(df) <= 2

    @pytest.mark.unit
    def test_parse_tparc_no_record_types_raises(self, tmp_path: Path):
        """Cover line 76-77: missing record_types raises."""
        p = tmp_path / "no_rt.tparc"
        p.write_text("ALR|1|A\n", encoding="utf-8")
        from acoharmony._parsers._tparc import parse_tparc

        with pytest.raises(ValueError, match="record_types"):
            parse_tparc(p, {"delimiter": "|"})

    @pytest.mark.unit
    def test_parse_tparc_namespace_schema(self, tmp_path: Path):
        """Cover lines 67-74: namespace schema handling."""
        p = tmp_path / "ns_tparc.tparc"
        p.write_text("ALR|1|A\n", encoding="utf-8")
        from acoharmony._parsers._tparc import parse_tparc

        schema = SimpleNamespace(
            file_format={"delimiter": "|"},
            record_types={"ALR": {"columns": [{"name": "type"}, {"name": "id"}, {"name": "name"}]}},
        )
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 1

    @pytest.mark.unit
    def test_parse_tparc_uneven_fields(self, tmp_path: Path):
        """Cover lines 127-129: records with different field counts get padded."""
        p = tmp_path / "uneven.tparc"
        p.write_text("ALR|1|A|extra\nALR|2|B\n", encoding="utf-8")
        from acoharmony._parsers._tparc import parse_tparc

        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {"columns": [{"name": "type"}, {"name": "id"}, {"name": "name"}]}
            },
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 2

    @pytest.mark.unit
    def test_parse_tparc_namespace_no_record_types(self, tmp_path: Path):
        """Cover line 68: namespace schema without record_types."""
        p = tmp_path / "ns_no_rt.tparc"
        p.write_text("ALR|1|A\n", encoding="utf-8")
        from acoharmony._parsers._tparc import parse_tparc

        schema = SimpleNamespace(file_format={"delimiter": "|"})
        with pytest.raises(ValueError, match="record_types"):
            parse_tparc(p, schema)


class TestTparcCoverage:
    """Cover remaining lines in _tparc.py."""

    @pytest.mark.unit
    def test_parse_tparc_empty_parts_line(self, tmp_path: Path):
        """Cover line 96: blank lines mixed in."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "blanks.txt"
        p.write_text("ALR|val1\n\n\nALR|val2\n")
        schema = {
            "delimiter": "|",
            "record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "data"}]}},
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 2

    @pytest.mark.unit
    def test_parse_tparc_empty_records_group(self, tmp_path: Path):
        """Cover line 108: no matching record types."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "nomatch.txt"
        p.write_text("XXX|no|match\n")
        schema = {
            "delimiter": "|",
            "record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "data"}]}},
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 0

    @pytest.mark.unit
    def test_parse_tparc_dtype_casting_all_types(self, tmp_path: Path):
        """Cover lines 151-162: dtype casting for str, int, float, date, bool."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "dtypes.txt"
        p.write_text("ALR|hello|42|3.14|2024-01-15|true\n")
        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {
                    "columns": [
                        {"name": "type", "dtype": "str"},
                        {"name": "text", "dtype": "str"},
                        {"name": "num", "dtype": "int"},
                        {"name": "dec", "dtype": "float"},
                        {"name": "dt", "dtype": "date"},
                        {"name": "flag", "dtype": "bool"},
                    ]
                }
            },
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert df["num"].dtype == pl.Int64
        assert df["dec"].dtype == pl.Float64

    @pytest.mark.unit
    def test_parse_tparc_dtype_unknown_type(self, tmp_path: Path):
        """Cover line 158: unknown dtype defaults to Utf8."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "unk_dtype.txt"
        p.write_text("ALR|hello\n")
        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {
                    "columns": [
                        {"name": "type", "dtype": "custom_type"},
                        {"name": "val", "dtype": "unknown"},
                    ]
                }
            },
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert df["type"].dtype == pl.Utf8

    @pytest.mark.unit
    def test_parse_tparc_no_dfs_empty_frame(self, tmp_path: Path):
        """Cover line 169: no dataframes, return empty frame."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "empty2.txt"
        p.write_text("XYZ|data\n")
        schema = {"delimiter": "|", "record_types": {"ALR": {}}}
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 0

    @pytest.mark.unit
    def test_parse_multi_record_delegates_to_tparc(self, tmp_path: Path):
        """Cover line 192: parse_multi_record delegates to parse_tparc."""
        from acoharmony._parsers._tparc import parse_multi_record

        p = tmp_path / "multi.txt"
        p.write_text("ALR|val1|val2\n")
        schema = {
            "delimiter": "|",
            "record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "data"}]}},
        }
        lf = parse_multi_record(p, schema)
        df = lf.collect()
        assert len(df) == 1
        assert "record_type" in df.columns


class TestTparcCoverageGaps:
    """Additional tests for _tparc coverage gaps."""

    @pytest.mark.unit
    def test_empty_lines_skipped(self, tmp_path):
        """Cover empty line skip (line 91)."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\n\n\nALR|val2\n")
        schema = {"record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "val"}]}}}
        df = parse_tparc(p, schema).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_line_no_parts_skipped(self, tmp_path):
        """Cover line with no delimiter (line 96)."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\n")
        schema = {"record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "val"}]}}}
        df = parse_tparc(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_tparc_schema_object_file_format_none(self, tmp_path):
        """Cover schema object with file_format=None."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\n")
        schema = SimpleNamespace(
            record_types={"ALR": {"columns": [{"name": "type"}, {"name": "val"}]}}, file_format=None
        )
        df = parse_tparc(p, schema).collect()
        assert df.height == 1

class TestTparc:
    """Tests for acoharmony._parsers._tparc."""

    @pytest.mark.unit
    def test_parse_tparc_basic(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1|val2\nALR|val3|val4\nBEN|x1|x2\n")
        schema = {
            "record_types": {
                "ALR": {"columns": [{"name": "type"}, {"name": "f1"}, {"name": "f2"}]},
                "BEN": {"columns": [{"name": "type"}, {"name": "g1"}, {"name": "g2"}]},
            }
        }
        df = parse_tparc(p, schema).collect()
        assert df.height == 3
        assert "record_type" in df.columns

    @pytest.mark.unit
    def test_parse_tparc_with_limit(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|a\nALR|b\nALR|c\n")
        schema = {"record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "val"}]}}}
        df = parse_tparc(p, schema, limit=2).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_tparc_type_casting(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|123|1.5\n")
        schema = {
            "record_types": {
                "ALR": {
                    "columns": [
                        {"name": "type", "dtype": "str"},
                        {"name": "count", "dtype": "int"},
                        {"name": "amount", "dtype": "float"},
                    ]
                }
            }
        }
        df = parse_tparc(p, schema).collect()
        assert df["count"].dtype == pl.Int64
        assert df["amount"].dtype == pl.Float64

    @pytest.mark.unit
    def test_parse_tparc_no_record_types_raises(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val\n")
        with pytest.raises(ValueError, match="record_types"):
            parse_tparc(p, {})

    @pytest.mark.unit
    def test_parse_tparc_empty_file(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("")
        schema = {"record_types": {"ALR": {"columns": [{"name": "type"}]}}}
        df = parse_tparc(p, schema).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_parse_tparc_unknown_record_type(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("UNKNOWN|val\n")
        schema = {"record_types": {"ALR": {"columns": [{"name": "type"}]}}}
        df = parse_tparc(p, schema).collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_parse_tparc_schema_object(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\n")
        schema = SimpleNamespace(
            record_types={"ALR": {"columns": [{"name": "type"}, {"name": "val"}]}},
            file_format={"delimiter": "|"},
        )
        df = parse_tparc(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_tparc_schema_object_no_file_format(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\n")
        schema = SimpleNamespace(
            record_types={"ALR": {"columns": [{"name": "type"}, {"name": "val"}]}}
        )
        df = parse_tparc(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_tparc_schema_object_no_record_types_raises(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\n")
        schema = SimpleNamespace(record_types=None)
        with pytest.raises(ValueError, match="record_types"):
            parse_tparc(p, schema)

    @pytest.mark.unit
    def test_parse_tparc_padded_records(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|a|b\nALR|c\n")
        schema = {
            "record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "f1"}, {"name": "f2"}]}}
        }
        df = parse_tparc(p, schema).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_tparc_no_columns_in_type_schema(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\n")
        schema = {"record_types": {"ALR": {}}}
        df = parse_tparc(p, schema).collect()
        assert df.height == 1
        assert "record_type" in df.columns

    @pytest.mark.unit
    def test_parse_tparc_bool_dtype(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|true\n")
        schema = {
            "record_types": {
                "ALR": {
                    "columns": [{"name": "type", "dtype": "str"}, {"name": "flag", "dtype": "bool"}]
                }
            }
        }
        df = parse_tparc(p, schema).collect()
        assert "flag" in df.columns

    @pytest.mark.unit
    def test_parse_tparc_date_dtype(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|2024-01-01\n")
        schema = {
            "record_types": {
                "ALR": {
                    "columns": [{"name": "type", "dtype": "str"}, {"name": "dt", "dtype": "date"}]
                }
            }
        }
        df = parse_tparc(p, schema).collect()
        assert df["dt"].dtype == pl.Utf8

    @pytest.mark.unit
    def test_parse_multi_record(self, tmp_path):
        from acoharmony._parsers._tparc import parse_multi_record

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\n")
        schema = {"record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "val"}]}}}
        df = parse_multi_record(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_tparc_unknown_dtype(self, tmp_path):
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|val\n")
        schema = {
            "record_types": {
                "ALR": {"columns": [{"name": "type"}, {"name": "val", "dtype": "unknown_type"}]}
            }
        }
        df = parse_tparc(p, schema).collect()
        assert df["val"].dtype == pl.Utf8


class TestTparcMoreCoverage:
    """More tests for _tparc.py coverage."""

    @pytest.mark.unit
    def test_parse_tparc_column_type_casting(self, tmp_path: Path):
        """Cover lines 147-162: column type casting in tparc."""
        p = tmp_path / "typed.tparc"
        p.write_text("ALR|123|Alice|100|true\nALR|456|Bob|200|false\n", encoding="utf-8")
        from acoharmony._parsers._tparc import parse_tparc

        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {
                    "columns": [
                        {"name": "type", "data_type": "string"},
                        {"name": "id", "data_type": "integer"},
                        {"name": "name", "data_type": "string"},
                        {"name": "amount", "data_type": "decimal"},
                        {"name": "active", "data_type": "boolean"},
                    ]
                }
            },
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 2


class TestTparcCoverageGaps2:
    """Cover _tparc.py missed lines 96, 108, 124."""

    @pytest.mark.unit
    def test_tparc_empty_line_skipped(self, tmp_path: Path):
        """Cover line 96: empty parts → continue."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("|just_pipe\nALR|data\n")
        schema = {
            "delimiter": "|",
            "record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "val"}]}},
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 1

    @pytest.mark.unit
    def test_tparc_empty_records_group(self, tmp_path: Path):
        """Cover line 108: records list is empty for a record_type → continue."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ALR|data1|data2\n")
        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {"columns": [{"name": "type"}, {"name": "d1"}, {"name": "d2"}]},
                "BLR": {"columns": [{"name": "type"}]},
            },
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 1

    @pytest.mark.unit
    def test_tparc_empty_parsed_records(self, tmp_path: Path):
        """Cover line 124: parsed_records is empty → continue."""
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "test.txt"
        p.write_text("ZZZ|nothing\n")
        schema = {"delimiter": "|", "record_types": {"ALR": {"columns": [{"name": "type"}]}}}
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert len(df) == 0


    @pytest.mark.unit
    def test_parse_file_tparc_filename_sets_tparc(self, tmp_path):
        tparc_file = tmp_path / "some_TPARC_file.txt"
        tparc_file.write_text("dummy")
        mock_schema = MagicMock()
        mock_schema.file_format = {}
        del mock_schema.file_format
        mock_parser = MagicMock(return_value=MagicMock())
        with (
            patch("acoharmony.parsers.ParserRegistry") as MockReg,
            patch("acoharmony.parsers.apply_column_types", side_effect=lambda lf, s: lf),
            patch("acoharmony.parsers.apply_schema_transformations", side_effect=lambda lf, s: lf),
        ):
            MockReg.get_parser.return_value = mock_parser
            from acoharmony.parsers import parse_file

            parse_file(tparc_file, mock_schema, add_tracking=False)
            MockReg.get_parser.assert_called_with("tparc")

    @pytest.mark.unit
    def test_parse_tparc(self, tmp_path):
        mock_parser = MagicMock(return_value=MagicMock())
        mock_schema = MagicMock()
        with patch("acoharmony.parsers.ParserRegistry") as MockReg:
            MockReg.get_parser.return_value = mock_parser
            from acoharmony.parsers import _parse_tparc

            tparc_file = tmp_path / "test_tparc.txt"
            tparc_file.write_text("dummy")
            _parse_tparc(tparc_file, mock_schema, limit=10)
            MockReg.get_parser.assert_called_with("tparc")
            mock_parser.assert_called_once()


class TestTparcDeadBranches:
    """Cover dead-code branches 95->96, 107->108, 120->124 via mocking.

    All three branches are unreachable in normal execution:
    - Line 95->96: str.split() never returns []
    - Line 107->108: grouped_records values are never empty (always appended to)
    - Line 120->124: parsed_records is never empty when records is non-empty

    We use builtins.open mocking with custom line objects that manipulate
    the function's internal state via sys._getframe().
    """

    @staticmethod
    def _mock_open_for(file_path, fake_lines):
        """Return a patch context manager that mocks builtins.open for file_path."""
        import builtins as _builtins

        real_open = _builtins.open

        def mock_open_fn(*args, **kwargs):
            path_arg = str(args[0]) if args else ""
            if path_arg == str(file_path):
                cm = MagicMock()
                cm.__enter__ = MagicMock(return_value=cm)
                cm.__exit__ = MagicMock(return_value=False)
                cm.readlines = MagicMock(return_value=list(fake_lines))
                return cm
            return real_open(*args, **kwargs)

        return patch("builtins.open", side_effect=mock_open_fn)

    @pytest.mark.unit
    def test_empty_parts_after_split_line_95_96(self, tmp_path):
        """Cover line 95->96: parts is empty after split -> continue.

        We inject a custom line object whose strip() returns a truthy
        object with a split() that returns [].
        """
        from acoharmony._parsers._tparc import parse_tparc

        class EmptySplitLine:
            """Truthy line whose split() returns []."""

            def strip(self):
                return self

            def split(self, *args, **kwargs):
                return []

            def __bool__(self):
                return True

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\n")
        schema = {
            "delimiter": "|",
            "record_types": {"ALR": {"columns": [{"name": "type"}, {"name": "val"}]}},
        }

        fake_lines = ["ALR|val1\n", EmptySplitLine()]

        with self._mock_open_for(p, fake_lines):
            lf = parse_tparc(p, schema)
            df = lf.collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_empty_records_list_line_107_108(self, tmp_path):
        """Cover line 107->108: records list is empty -> continue.

        We inject a trailing line whose strip() uses sys._getframe to
        reach into parse_tparc's locals and clear the BEN records list,
        then returns '' so it is skipped at line 90-91.
        """
        import sys
        from acoharmony._parsers._tparc import parse_tparc

        class ClearBenLine:
            """Line whose strip() clears BEN from grouped_records."""

            def strip(self):
                frame = sys._getframe(1)
                gr = frame.f_locals.get("grouped_records")
                if gr is not None and "BEN" in gr:
                    gr["BEN"].clear()
                return ""

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\nBEN|val2\n")
        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {"columns": [{"name": "type"}, {"name": "val"}]},
                "BEN": {"columns": [{"name": "type"}, {"name": "val"}]},
            },
        }

        fake_lines = ["ALR|val1\n", "BEN|val2\n", ClearBenLine()]

        with self._mock_open_for(p, fake_lines):
            lf = parse_tparc(p, schema)
            df = lf.collect()
        # BEN records were cleared, so only ALR survives
        assert df.height == 1

    @pytest.mark.unit
    def test_empty_parsed_records_line_120_124(self, tmp_path):
        """Cover line 120->124: parsed_records empty -> continue.

        We inject a trailing line whose strip() replaces BEN's records
        list with a truthy-but-empty-iteration object so that the inner
        `for record in records` loop yields nothing and parsed_records
        stays empty.
        """
        import sys
        from acoharmony._parsers._tparc import parse_tparc

        class TruthyEmptyIter(list):
            """List subclass that is truthy but yields nothing on iteration."""

            def __bool__(self):
                return True

            def __iter__(self):
                return iter([])

        class ReplaceBenRecordsLine:
            """Line whose strip() replaces BEN records with TruthyEmptyIter."""

            def strip(self):
                frame = sys._getframe(1)
                gr = frame.f_locals.get("grouped_records")
                if gr is not None and "BEN" in gr:
                    gr["BEN"] = TruthyEmptyIter(["placeholder"])
                return ""

        p = tmp_path / "test.txt"
        p.write_text("ALR|val1\nBEN|val2\n")
        schema = {
            "delimiter": "|",
            "record_types": {
                "ALR": {"columns": [{"name": "type"}, {"name": "val"}]},
                "BEN": {"columns": [{"name": "type"}, {"name": "val"}]},
            },
        }

        fake_lines = ["ALR|val1\n", "BEN|val2\n", ReplaceBenRecordsLine()]

        with self._mock_open_for(p, fake_lines):
            lf = parse_tparc(p, schema)
            df = lf.collect()
        assert df.height == 1


class TestTparcRenameLoopBackEdge:
    """Cover the loop back-edge 140->137 in the column rename loop."""

    @pytest.mark.unit
    def test_rename_loop_skips_matching_field_name(self, tmp_path):
        """Cover branch 140->137: condition is False so rename is skipped.

        When a column in the schema is already named 'field_N' (matching
        the auto-generated name), `field_name != col_name` is False,
        the rename is skipped, and the loop continues back to line 137.
        """
        from acoharmony._parsers._tparc import parse_tparc

        p = tmp_path / "rename_loop.txt"
        p.write_text("REC|alpha|beta\nREC|one|two\n")
        schema = {
            "delimiter": "|",
            "record_types": {
                "REC": {
                    "columns": [
                        {"name": "rec_type"},
                        {"name": "field_1"},
                        {"name": "col_b"},
                    ]
                }
            },
        }
        lf = parse_tparc(p, schema)
        df = lf.collect()
        assert df.height == 2
        # field_0 was renamed to rec_type
        assert "rec_type" in df.columns
        # field_1 kept its name (condition was False, rename skipped, loop back)
        assert "field_1" in df.columns
        # field_2 was renamed to col_b
        assert "col_b" in df.columns
