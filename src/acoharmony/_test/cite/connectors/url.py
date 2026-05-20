"""Tests for acoharmony._cite.connectors._url module."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._cite.connectors._url import host_matches


class TestHostMatches:
    """Tests for host_matches helper."""

    @pytest.mark.unit
    def test_exact_host_match(self):
        assert host_matches("https://cms.gov/some/path", "cms.gov") is True

    @pytest.mark.unit
    def test_subdomain_match(self):
        assert host_matches("https://www.cms.gov/some/path", "cms.gov") is True

    @pytest.mark.unit
    def test_deep_subdomain_match(self):
        assert host_matches("https://api.data.cms.gov/foo", "cms.gov") is True

    @pytest.mark.unit
    def test_unrelated_host_does_not_match(self):
        assert host_matches("https://attacker.com/path?fake=cms.gov", "cms.gov") is False

    @pytest.mark.unit
    def test_substring_attack_blocked(self):
        # Suffix-misuse attack: foocms.gov is not a subdomain of cms.gov
        assert host_matches("https://foocms.gov/path", "cms.gov") is False

    @pytest.mark.unit
    def test_case_insensitive(self):
        assert host_matches("https://CMS.GOV/path", "cms.gov") is True
        assert host_matches("https://cms.gov/path", "CMS.GOV") is True

    @pytest.mark.unit
    def test_invalid_url_returns_false(self):
        # Cover the ValueError branch: urlparse on a malformed value with invalid bracket
        assert host_matches("http://[bad", "cms.gov") is False

    @pytest.mark.unit
    def test_no_host_returns_false(self):
        # Cover the "host is None/empty" branch
        assert host_matches("not-a-url", "cms.gov") is False
        assert host_matches("", "cms.gov") is False
