# © 2025 HarmonyCares
# All rights reserved.

"""
Vendored third-party dependencies.

Each subdirectory under ``_depends/`` is a snapshot of an external library
committed to this repository at a fixed version. We vendor instead of
declaring a runtime dependency when:

  1. Correctness requires pinning to a specific upstream state (e.g. the
     HCC risk calculation must be frozen at a known set of CMS coefficients
     for reconciliation tie-outs to be reproducible), or
  2. The upstream may ship breaking changes in a patch release that would
     silently break an internal invariant, or
  3. We need to patch the library for our use case without forking.

Each vendored library carries its original ``LICENSE`` file verbatim and
a ``VENDORING.md`` that records the upstream source URL, version, SHA-256
of the original sdist, and the date of vendoring. To update, download the
new sdist, verify the new SHA, and replace the directory contents — do
NOT hand-edit files in place.
"""
