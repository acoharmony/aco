# © 2025 HarmonyCares
# All rights reserved.

"""
Base class for plugin registries.

Each plugin registry is a singleton (instantiated once at module import) that
exposes lazily-bound dependencies: ``mo``, ``storage``, ``catalog``. Lazy
binding lets us import the registry from non-marimo contexts (tests,
scripts) without hard-failing on optional deps.
"""

from __future__ import annotations


class PluginRegistry:
    """Lazy-loaded mo / storage / catalog accessors for plugin singletons."""

    def __init__(self) -> None:
        self._mo = None
        self._storage = None
        self._catalog = None

    @property
    def mo(self):
        if self._mo is None:
            import marimo as mo

            self._mo = mo
        return self._mo

    @property
    def storage(self):
        if self._storage is None:
            from acoharmony._store import StorageBackend

            self._storage = StorageBackend()
        return self._storage

    @property
    def catalog(self):
        if self._catalog is None:
            from acoharmony import Catalog

            self._catalog = Catalog()
        return self._catalog
