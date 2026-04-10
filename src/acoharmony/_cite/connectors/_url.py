# © 2025 HarmonyCares
# All rights reserved.

"""URL host validation helper for connector dispatch."""

from __future__ import annotations

from urllib.parse import urlparse


def host_matches(url: str, suffix: str) -> bool:
    """Return True if `url`'s netloc equals `suffix` or is a subdomain of it.

    Substring checks like ``"cms.gov" in url`` are unsafe because an attacker
    URL like ``https://attacker.com/?fake=cms.gov`` would match. We parse the
    URL and validate the host against the suffix.
    """
    try:
        host = urlparse(url).hostname
    except ValueError:
        return False
    if not host:
        return False
    host = host.lower()
    suffix = suffix.lower()
    return host == suffix or host.endswith("." + suffix)
