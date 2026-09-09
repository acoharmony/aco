"""Tests for public IP parsing."""

from __future__ import annotations

import pytest

from acoharmony._auth.public_ip import fetch_public_ip, parse_ip_values


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


@pytest.mark.unit
def test_fetch_public_ip_keeps_multiple_observed_egress_ips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Response:
        def __init__(self, value: str) -> None:
            self.value = value

        def read(self) -> bytes:
            return self.value.encode()

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    responses = {
        "https://api.ipify.org": "68.134.99.31",
        "https://checkip.amazonaws.com": "68.134.99.31\n",
        "https://ifconfig.me/ip": "136.226.69.108",
        "https://icanhazip.com": "136.226.69.108\n",
    }

    def fake_urlopen(url: str, timeout: float) -> Response:
        del timeout
        return Response(responses[url])

    monkeypatch.setattr("acoharmony._auth.public_ip.urllib.request.urlopen", fake_urlopen)

    result = fetch_public_ip()

    assert result.value == "68.134.99.31"
    assert result.observed_values == ("68.134.99.31", "136.226.69.108")
