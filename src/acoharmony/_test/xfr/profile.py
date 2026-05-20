# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _xfr.profile module."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from acoharmony._xfr.profile import (
    CompositeRule,
    DirectoryVerifier,
    LiteralPatternRule,
    LogVerifier,
    MonthlyMatchRule,
    SchemaPatternRule,
    TransferProfile,
    _ensure_profiles_imported,
    _extract_d_token_date,
    _flatten_patterns,
    _reset_registry_for_tests,
    list_profiles,
    register_profile,
    resolve_profile,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class TestFlattenPatterns:
    @pytest.mark.unit
    def test_string(self):
        assert _flatten_patterns("a*.zip") == ["a*.zip"]

    @pytest.mark.unit
    def test_list(self):
        assert _flatten_patterns(["a*.zip", "b*.csv"]) == ["a*.zip", "b*.csv"]

    @pytest.mark.unit
    def test_list_drops_non_strings(self):
        assert _flatten_patterns(["a*.zip", 123, None]) == ["a*.zip"]

    @pytest.mark.unit
    def test_nested_dict(self):
        result = _flatten_patterns({"x": "a*.zip", "y": ["b*.csv", "c*.tsv"]})
        assert sorted(result) == ["a*.zip", "b*.csv", "c*.tsv"]

    @pytest.mark.unit
    def test_unrecognized_returns_empty(self):
        assert _flatten_patterns(42) == []


class TestExtractDTokenDate:
    @pytest.mark.unit
    def test_typical(self):
        assert _extract_d_token_date("P.D0259.ACO.ZCY26.D260413.T1042070.zip") == date(2026, 4, 13)

    @pytest.mark.unit
    def test_no_token_returns_none(self):
        assert _extract_d_token_date("nothing.txt") is None

    @pytest.mark.unit
    def test_invalid_date_returns_none(self):
        # Month 99 — invalid
        assert _extract_d_token_date("P.X.D269999.T0.zip") is None


# ---------------------------------------------------------------------------
# LiteralPatternRule
# ---------------------------------------------------------------------------


class TestLiteralPatternRuleMatching:
    @pytest.mark.unit
    def test_matches_pattern(self):
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=None)
        assert rule.matches("a.zip") is True

    @pytest.mark.unit
    def test_pattern_no_match(self):
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=None)
        assert rule.matches("a.txt") is False

    @pytest.mark.unit
    def test_date_floor_excludes_old(self):
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=date(2026, 4, 1))
        assert rule.matches("P.D.D260331.T0.zip") is False

    @pytest.mark.unit
    def test_date_floor_includes_recent(self):
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=date(2026, 4, 1))
        assert rule.matches("P.D.D260415.T0.zip") is True

    @pytest.mark.unit
    def test_date_floor_no_token_passes(self):
        # No D-token in filename: don't reject
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=date(2026, 4, 1))
        assert rule.matches("plain.zip") is True

    @pytest.mark.unit
    def test_date_floor_none(self):
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=None)
        # Old token — would be rejected if floor were set
        assert rule.matches("P.D.D200101.T0.zip") is True

    @pytest.mark.unit
    def test_date_floor_month_start_resolved_dynamically(self):
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor="month_start")
        from datetime import datetime as _dt

        # Pin "now" to mid-April 2026 so floor is 2026-04-01.
        with patch("acoharmony._xfr.profile.datetime") as mock_dt:
            mock_dt.now.return_value = _dt(2026, 4, 15)
            mock_dt.side_effect = _dt
            assert rule.matches("P.D.D260331.T0.zip") is False
            assert rule.matches("P.D.D260415.T0.zip") is True

    @pytest.mark.unit
    def test_date_floor_invalid_value_raises(self):
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor="next_tuesday")
        with pytest.raises(ValueError, match="date_floor"):
            rule.matches("a.zip")


class TestLiteralPatternRuleListing:
    @pytest.mark.unit
    def test_lists_matching_files(self, tmp_path: Path):
        (tmp_path / "a.zip").touch()
        (tmp_path / "b.txt").touch()
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=None)
        assert rule.applicable_filenames(tmp_path) == ["a.zip"]

    @pytest.mark.unit
    def test_missing_dir_returns_empty(self, tmp_path: Path):
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=None)
        assert rule.applicable_filenames(tmp_path / "nope") == []

    @pytest.mark.unit
    def test_skips_subdirectories(self, tmp_path: Path):
        (tmp_path / "a.zip").touch()
        (tmp_path / "sub").mkdir()
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=None)
        assert rule.applicable_filenames(tmp_path) == ["a.zip"]

    @pytest.mark.unit
    def test_multiple_dirs_dedupes(self, tmp_path: Path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.zip").touch()
        (d2 / "a.zip").touch()
        (d2 / "b.zip").touch()
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=None)
        assert rule.applicable_filenames([d1, d2]) == ["a.zip", "b.zip"]

    @pytest.mark.unit
    def test_multiple_dirs_skips_missing(self, tmp_path: Path):
        d1 = tmp_path / "d1"
        d1.mkdir()
        (d1 / "a.zip").touch()
        rule = LiteralPatternRule(patterns=("*.zip",), date_floor=None)
        assert rule.applicable_filenames([d1, tmp_path / "missing"]) == ["a.zip"]


# ---------------------------------------------------------------------------
# SchemaPatternRule  (uses the schema registry)
# ---------------------------------------------------------------------------


class TestSchemaPatternRule:
    @pytest.mark.unit
    def test_uses_registered_schema_patterns(self, tmp_path: Path):
        (tmp_path / "P.D0259.ALGC26.RP.D260421.T1144542.xlsx").touch()
        (tmp_path / "noise.txt").touch()
        rule = SchemaPatternRule(schemas=("bar",), date_floor=None)
        assert rule.applicable_filenames(tmp_path) == [
            "P.D0259.ALGC26.RP.D260421.T1144542.xlsx"
        ]

    @pytest.mark.unit
    def test_date_floor_explicit_date(self, tmp_path: Path):
        rule = SchemaPatternRule(schemas=("bar",), date_floor=date(2026, 4, 1))
        old = "P.D0259.ALGC26.RP.D260201.T1.xlsx"
        new = "P.D0259.ALGC26.RP.D260421.T1.xlsx"
        (tmp_path / old).touch()
        (tmp_path / new).touch()
        names = rule.applicable_filenames(tmp_path)
        assert names == [new]

    @pytest.mark.unit
    def test_date_floor_month_start(self, tmp_path: Path):
        rule = SchemaPatternRule(schemas=("bar",), date_floor="month_start")
        from datetime import datetime as _dt

        with patch("acoharmony._xfr.profile.datetime") as mock_dt:
            mock_dt.now.return_value = _dt(2026, 4, 15)
            mock_dt.side_effect = _dt
            assert rule.matches("P.D0259.ALGC26.RP.D260415.T1.xlsx") is True
            assert rule.matches("P.D0259.ALGC26.RP.D260315.T1.xlsx") is False

    @pytest.mark.unit
    def test_invalid_date_floor_raises(self):
        rule = SchemaPatternRule(schemas=("bar",), date_floor="last_friday")
        with pytest.raises(ValueError, match="date_floor"):
            rule.matches("P.D0259.ALGC26.RP.D260415.T1.xlsx")

    @pytest.mark.unit
    def test_skips_subdirectories_and_missing_dir(self, tmp_path: Path):
        (tmp_path / "P.D0259.ALGC26.RP.D260415.T1.xlsx").touch()
        (tmp_path / "subdir").mkdir()
        rule = SchemaPatternRule(schemas=("bar",), date_floor=None)
        # Subdir is not iterated; missing dir returns empty
        names = rule.applicable_filenames(tmp_path)
        assert names == ["P.D0259.ALGC26.RP.D260415.T1.xlsx"]
        assert rule.applicable_filenames(tmp_path / "missing") == []

    @pytest.mark.unit
    def test_filename_without_d_token_passes_floor(self):
        # Force SchemaPatternRule to advertise a permissive pattern that
        # has no D-token requirement, then feed it a filename without
        # one. Exercises the "token_date is None → return True" branch.
        rule = SchemaPatternRule(schemas=(), date_floor=date(2099, 1, 1))
        with patch.object(SchemaPatternRule, "_patterns", return_value=["*.dat"]):
            assert rule.matches("plain.dat") is True

    @pytest.mark.unit
    def test_with_key_filter(self, tmp_path: Path):
        # cclf0 has reach_monthly=...ZC0Y... and mssp_weekly=...ZC0WY...
        (tmp_path / "P.D0259.ACO.ZC0Y26.D260415.T1.zip").touch()
        (tmp_path / "P.A2671.ACO.ZC0WY26.D260415.T1.zip").touch()
        rule_monthly_only = SchemaPatternRule(
            schemas=(("cclf0", ("reach_monthly",)),), date_floor=None
        )
        names = rule_monthly_only.applicable_filenames(tmp_path)
        assert "P.D0259.ACO.ZC0Y26.D260415.T1.zip" in names
        assert "P.A2671.ACO.ZC0WY26.D260415.T1.zip" not in names

    @pytest.mark.unit
    def test_unknown_schema_returns_no_patterns(self, tmp_path: Path):
        (tmp_path / "anything.xlsx").touch()
        rule = SchemaPatternRule(schemas=("does_not_exist",), date_floor=None)
        assert rule.applicable_filenames(tmp_path) == []

    @pytest.mark.unit
    def test_handles_comma_separated_patterns(self, tmp_path: Path):
        # cclf0's four_icli filePattern is a comma-separated list. Our
        # rule uses metadata.file_patterns, but verify the comma split
        # path is exercised by feeding a literal comma-string through.
        from acoharmony._xfr.profile import _flatten_patterns

        pats = _flatten_patterns({"k": "a, b, c"})
        rule = SchemaPatternRule(schemas=(), date_floor=None)
        assert rule._patterns() == []
        # Direct path via matches() with comma-string patterns
        from acoharmony._xfr import profile as _p

        with patch.object(_p.SchemaPatternRule, "_patterns", return_value=["a, b"]):
            assert rule.matches("a") is True
            assert rule.matches("b") is True
            assert rule.matches("c") is False


# ---------------------------------------------------------------------------
# CompositeRule
# ---------------------------------------------------------------------------


class TestCompositeRule:
    @pytest.mark.unit
    def test_or_combines_rules(self, tmp_path: Path):
        (tmp_path / "a.zip").touch()
        (tmp_path / "b.csv").touch()
        (tmp_path / "c.txt").touch()
        rule = CompositeRule(
            rules=(
                LiteralPatternRule(patterns=("*.zip",), date_floor=None),
                LiteralPatternRule(patterns=("*.csv",), date_floor=None),
            )
        )
        names = rule.applicable_filenames(tmp_path)
        assert names == ["a.zip", "b.csv"]
        assert rule.matches("a.zip") is True
        assert rule.matches("b.csv") is True
        assert rule.matches("c.txt") is False

    @pytest.mark.unit
    def test_dedupes_overlap(self, tmp_path: Path):
        (tmp_path / "a.zip").touch()
        rule = CompositeRule(
            rules=(
                LiteralPatternRule(patterns=("*.zip",), date_floor=None),
                LiteralPatternRule(patterns=("a.*",), date_floor=None),
            )
        )
        assert rule.applicable_filenames(tmp_path) == ["a.zip"]

    @pytest.mark.unit
    def test_empty_rules(self, tmp_path: Path):
        (tmp_path / "a.zip").touch()
        rule = CompositeRule(rules=())
        assert rule.applicable_filenames(tmp_path) == []
        assert rule.matches("a.zip") is False


# ---------------------------------------------------------------------------
# Verifiers
# ---------------------------------------------------------------------------


class TestDirectoryVerifier:
    @pytest.mark.unit
    def test_present(self, tmp_path: Path):
        (tmp_path / "a.zip").touch()
        v = DirectoryVerifier(destination=tmp_path)
        assert v.state_for("a.zip") == "placed"

    @pytest.mark.unit
    def test_absent(self, tmp_path: Path):
        v = DirectoryVerifier(destination=tmp_path)
        assert v.state_for("missing.zip") is None

    @pytest.mark.unit
    def test_destination_dir_missing(self, tmp_path: Path):
        v = DirectoryVerifier(destination=tmp_path / "nope")
        assert v.state_for("a.zip") is None


class TestLogVerifier:
    @pytest.mark.unit
    def test_uses_parser_to_find_uploads(self, tmp_path: Path):
        log = tmp_path / "log.log"
        log.write_text(
            "------------------------------------------------------------------------\n"
            "Date : 4/27/2026 9:22:39 AM\n"
            "------------------------------------------------------------------------\n"
            "4/27/2026 9:22:40 AM : Upload file C:\\src\\foo.zip to /home/HarmonyCaresHDAI/FromHC/foo.zip.\n"
        )
        v = LogVerifier(log_path=log, upload_dest_prefix="/home/HarmonyCaresHDAI/FromHC/")
        assert v.state_for("foo.zip") == "sent"
        assert v.state_for("bar.zip") is None

    @pytest.mark.unit
    def test_log_missing_returns_none(self, tmp_path: Path):
        v = LogVerifier(log_path=tmp_path / "missing.log")
        assert v.state_for("anything") is None

    @pytest.mark.unit
    def test_empty_log_returns_none(self, tmp_path: Path):
        log = tmp_path / "empty.log"
        log.write_text("")
        v = LogVerifier(log_path=log)
        assert v.state_for("foo.zip") is None

    @pytest.mark.unit
    def test_log_with_no_uploads(self, tmp_path: Path):
        log = tmp_path / "log.log"
        log.write_text(
            "------------------------------------------------------------------------\n"
            "Date : 4/27/2026 9:22:39 AM\n"
            "------------------------------------------------------------------------\n"
            "4/27/2026 9:22:40 AM : Authentication succeeded\n"
            "4/27/2026 9:22:41 AM : SFTP connection closed\n"
        )
        v = LogVerifier(log_path=log)
        assert v.state_for("foo.zip") is None

    @pytest.mark.unit
    def test_prefix_filters(self, tmp_path: Path):
        log = tmp_path / "log.log"
        log.write_text(
            "------------------------------------------------------------------------\n"
            "Date : 4/27/2026 9:22:39 AM\n"
            "------------------------------------------------------------------------\n"
            "4/27/2026 9:22:40 AM : Upload file C:\\src\\foo.zip to /home/SOMEONE_ELSE/foo.zip.\n"
        )
        v = LogVerifier(log_path=log, upload_dest_prefix="/home/HarmonyCaresHDAI/FromHC/")
        # Wrong prefix → not "sent" for us
        assert v.state_for("foo.zip") is None

    @pytest.mark.unit
    def test_no_prefix_accepts_all(self, tmp_path: Path):
        log = tmp_path / "log.log"
        log.write_text(
            "------------------------------------------------------------------------\n"
            "Date : 4/27/2026 9:22:39 AM\n"
            "------------------------------------------------------------------------\n"
            "4/27/2026 9:22:40 AM : Upload file C:\\src\\foo.zip to /any/prefix/foo.zip.\n"
        )
        v = LogVerifier(log_path=log, upload_dest_prefix="")
        assert v.state_for("foo.zip") == "sent"

    @pytest.mark.unit
    def test_caches_parse_result(self, tmp_path: Path):
        log = tmp_path / "log.log"
        log.write_text(
            "------------------------------------------------------------------------\n"
            "Date : 4/27/2026 9:22:39 AM\n"
            "------------------------------------------------------------------------\n"
            "4/27/2026 9:22:40 AM : Upload file C:\\src\\foo.zip to /home/HarmonyCaresHDAI/FromHC/foo.zip.\n"
        )
        v = LogVerifier(log_path=log, upload_dest_prefix="/home/HarmonyCaresHDAI/FromHC/")
        # Parse on first call
        v.state_for("foo.zip")
        # Subsequent calls should not reparse — patch the parser and
        # confirm it isn't called.
        with patch("acoharmony._parsers._mabel_log.parse_mabel_log") as parser:
            v.state_for("anything")
            v.state_for("else")
            parser.assert_not_called()

    @pytest.mark.unit
    def test_handles_io_error_gracefully(self, tmp_path: Path, monkeypatch):
        log = tmp_path / "log.log"
        log.write_text("anything")  # actual content doesn't matter
        v = LogVerifier(log_path=log)

        original_exists = Path.exists

        def flaky_exists(self, *a, **k):
            if self == log:
                raise OSError("mount flake")
            return original_exists(self, *a, **k)

        monkeypatch.setattr(Path, "exists", flaky_exists)
        # Should NOT raise — caller gets None ("no signal") for everything.
        assert v.state_for("foo.zip") is None


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    @pytest.mark.unit
    def test_register_and_resolve(self, tmp_path: Path):
        profile = TransferProfile(
            name="x",
            description="d",
            source_dirs=(tmp_path,),
            destination=tmp_path / "dst",
            source_rule=LiteralPatternRule(patterns=("*",)),
        )
        register_profile(profile)
        # Skip auto-discovery for this test (we just want our profile)
        with patch("acoharmony._xfr.profile._ensure_profiles_imported"):
            assert resolve_profile("x") is profile

    @pytest.mark.unit
    def test_resolve_unknown_raises(self):
        with patch("acoharmony._xfr.profile._ensure_profiles_imported"):
            with pytest.raises(KeyError, match="Unknown xfr profile"):
                resolve_profile("does_not_exist")

    @pytest.mark.unit
    def test_list_profiles_sorted(self, tmp_path: Path):
        for name in ("zebra", "alpha", "mango"):
            register_profile(
                TransferProfile(
                    name=name,
                    description="",
                    source_dirs=(tmp_path,),
                    destination=tmp_path / name,
                    source_rule=LiteralPatternRule(patterns=("*",)),
                )
            )
        with patch("acoharmony._xfr.profile._ensure_profiles_imported"):
            assert [p.name for p in list_profiles()] == ["alpha", "mango", "zebra"]

    @pytest.mark.unit
    def test_register_overwrites(self, tmp_path: Path):
        first = TransferProfile(
            name="x",
            description="first",
            source_dirs=(tmp_path,),
            destination=tmp_path / "x",
            source_rule=LiteralPatternRule(patterns=("*",)),
        )
        second = TransferProfile(
            name="x",
            description="second",
            source_dirs=(tmp_path,),
            destination=tmp_path / "x",
            source_rule=LiteralPatternRule(patterns=("*",)),
        )
        register_profile(first)
        register_profile(second)
        with patch("acoharmony._xfr.profile._ensure_profiles_imported"):
            assert resolve_profile("x").description == "second"

    @pytest.mark.unit
    def test_ensure_profiles_imported_is_idempotent(self):
        # Just exercise: it should run without error.
        _ensure_profiles_imported()
        _ensure_profiles_imported()


# ---------------------------------------------------------------------------
# TransferProfile
# ---------------------------------------------------------------------------


class TestTransferProfile:
    @pytest.mark.unit
    def test_dest_filename_no_rename(self, tmp_path: Path):
        profile = TransferProfile(
            name="x",
            description="",
            source_dirs=(tmp_path,),
            destination=tmp_path / "dst",
            source_rule=LiteralPatternRule(patterns=("*",)),
        )
        assert profile.dest_filename("a.zip") == "a.zip"

    @pytest.mark.unit
    def test_dest_filename_with_rename(self, tmp_path: Path):
        profile = TransferProfile(
            name="x",
            description="",
            source_dirs=(tmp_path,),
            destination=tmp_path / "dst",
            source_rule=LiteralPatternRule(patterns=("*",)),
            rename=lambda n: n.upper(),
        )
        assert profile.dest_filename("a.zip") == "A.ZIP"

    @pytest.mark.unit
    def test_find_source_path_first_match_wins(self, tmp_path: Path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.zip").write_text("first")
        (d2 / "a.zip").write_text("second")
        profile = TransferProfile(
            name="x",
            description="",
            source_dirs=(d1, d2),
            destination=tmp_path / "dst",
            source_rule=LiteralPatternRule(patterns=("*",)),
        )
        found = profile.find_source_path("a.zip")
        assert found == d1 / "a.zip"

    @pytest.mark.unit
    def test_find_source_path_falls_through(self, tmp_path: Path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d2 / "a.zip").touch()
        profile = TransferProfile(
            name="x",
            description="",
            source_dirs=(d1, d2),
            destination=tmp_path / "dst",
            source_rule=LiteralPatternRule(patterns=("*",)),
        )
        found = profile.find_source_path("a.zip")
        assert found == d2 / "a.zip"

    @pytest.mark.unit
    def test_find_source_path_none_when_missing(self, tmp_path: Path):
        profile = TransferProfile(
            name="x",
            description="",
            source_dirs=(tmp_path,),
            destination=tmp_path / "dst",
            source_rule=LiteralPatternRule(patterns=("*",)),
        )
        assert profile.find_source_path("missing.zip") is None


# ---------------------------------------------------------------------------
# Aliases
# ---------------------------------------------------------------------------


class TestAliases:
    @pytest.mark.unit
    def test_monthly_match_rule_is_schema_pattern_rule(self):
        assert MonthlyMatchRule is SchemaPatternRule


# ---------------------------------------------------------------------------
# Test helper hatch
# ---------------------------------------------------------------------------


class TestResetRegistryHelper:
    @pytest.mark.unit
    def test_reset_clears(self, tmp_path: Path):
        register_profile(
            TransferProfile(
                name="x",
                description="",
                source_dirs=(tmp_path,),
                destination=tmp_path / "x",
                source_rule=LiteralPatternRule(patterns=("*",)),
            )
        )
        _reset_registry_for_tests()
        with patch("acoharmony._xfr.profile._ensure_profiles_imported"):
            assert list_profiles() == []
