"""Tests for public IP parsing."""

from __future__ import annotations

import pytest

from acoharmony._auth.public_ip import parse_ip_values


@pytest.mark.unit
def test_parse_ip_values_accepts_lists_and_comma_strings() -> None:
    assert parse_ip_values(["203.0.113.10, 203.0.113.11", "203.0.113.10"]) == [
        "203.0.113.10",
        "203.0.113.11",
    ]


@pytest.mark.unit
def test_parse_ip_values_rejects_invalid_ip() -> None:
    with pytest.raises(ValueError, match="not-an-ip"):
        parse_ip_values(["not-an-ip"])
