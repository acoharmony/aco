# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _xfr._profiles.hdai module."""

from __future__ import annotations

import importlib

import pytest

from acoharmony._xfr.profile import (
    CompositeRule,
    LiteralPatternRule,
    LogVerifier,
    SchemaPatternRule,
)


class TestHdaiProfile:
    @pytest.mark.unit
    def test_registered(self):
        # Re-import to (re)register since the conftest resets the registry
        # before each test.
        import acoharmony._xfr._profiles.hdai as hdai_module

        importlib.reload(hdai_module)
        from acoharmony._xfr.profile import resolve_profile

        profile = resolve_profile("hdai")
        assert profile.name == "hdai"
        assert profile.destination.name == "Outbound"

    @pytest.mark.unit
    def test_composite_rule_components(self):
        import acoharmony._xfr._profiles.hdai as hdai_module

        importlib.reload(hdai_module)
        from acoharmony._xfr.profile import resolve_profile

        profile = resolve_profile("hdai")
        rule = profile.source_rule
        assert isinstance(rule, CompositeRule)
        types = {type(r) for r in rule.rules}
        assert LiteralPatternRule in types
        assert SchemaPatternRule in types

    @pytest.mark.unit
    def test_zip_pattern_includes_monthly_excludes_weekly(self):
        import acoharmony._xfr._profiles.hdai as hdai_module

        importlib.reload(hdai_module)
        from acoharmony._xfr.profile import resolve_profile

        profile = resolve_profile("hdai")
        rule = profile.source_rule
        # Pluck the literal pattern rule
        literal = next(r for r in rule.rules if isinstance(r, LiteralPatternRule))
        # ZCY → match (monthly bundle)
        assert literal.matches("P.A2671.ACO.ZCY26.D260413.T1042070.zip")
        assert literal.matches("P.D0259.ACO.ZCY26.D260415.T1.zip")
        # ZCWY → no match (weekly variant)
        assert not literal.matches("P.A2671.ACO.ZCWY26.D260415.T1.zip")

    @pytest.mark.unit
    def test_verifier_is_log_verifier(self):
        import acoharmony._xfr._profiles.hdai as hdai_module

        importlib.reload(hdai_module)
        from acoharmony._xfr.profile import resolve_profile

        profile = resolve_profile("hdai")
        assert isinstance(profile.verifier, LogVerifier)
        assert profile.verifier.upload_dest_prefix == "/home/HarmonyCaresHDAI/FromHC/"

    @pytest.mark.unit
    def test_two_source_dirs(self):
        import acoharmony._xfr._profiles.hdai as hdai_module

        importlib.reload(hdai_module)
        from acoharmony._xfr.profile import resolve_profile

        profile = resolve_profile("hdai")
        assert len(profile.source_dirs) == 2
        names = {p.name for p in profile.source_dirs}
        assert names == {"bronze", "archive"}
