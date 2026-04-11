"""
Sample data stubs for the vendored hccinfhir copy.

The upstream ``hccinfhir.samples`` module ships ~2 MB of X12 820/834/837
sample files plus an API to load them. acoharmony does not process X12
(we use CCLF) so the sample_files directory was removed when vendoring
to keep the committed footprint small.

All symbols the upstream ``__init__.py`` imports from this module are
preserved so ``from acoharmony._depends import hccinfhir`` still works,
but any attempt to *call* them raises ``NotImplementedError`` with a
clear message pointing at the upstream repo.

If you need sample data for testing, install the upstream package
separately in a sandbox and copy the sample files you need — do not
re-vendor them here without a concrete acoharmony use case.
"""

from __future__ import annotations

from typing import Any


_REMOVED_MSG = (
    "hccinfhir sample data was removed when vendoring into acoharmony. "
    "The acoharmony copy ships only the HCC risk-score calculation "
    "surface; X12 820/834/837 sample files are not included. See "
    "https://github.com/mimilabs/hccinfhir for upstream samples."
)


class SampleData:
    """Stub that preserves the upstream class name for import compatibility."""

    @staticmethod
    def get_eob_sample(case_number: int = 1) -> dict[str, Any]:
        raise NotImplementedError(_REMOVED_MSG)

    @staticmethod
    def get_eob_sample_list(limit: int | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError(_REMOVED_MSG)

    @staticmethod
    def get_837_sample(case_number: int = 0) -> str:
        raise NotImplementedError(_REMOVED_MSG)

    @staticmethod
    def get_837_sample_list(case_numbers: list[int] | None = None) -> list[str]:
        raise NotImplementedError(_REMOVED_MSG)

    @staticmethod
    def get_834_sample(case_number: int = 1) -> str:
        raise NotImplementedError(_REMOVED_MSG)

    @staticmethod
    def get_820_sample(case_number: int = 1) -> str:
        raise NotImplementedError(_REMOVED_MSG)

    @staticmethod
    def list_available_samples() -> dict[str, Any]:
        raise NotImplementedError(_REMOVED_MSG)


def get_eob_sample(case_number: int = 1) -> dict[str, Any]:
    raise NotImplementedError(_REMOVED_MSG)


def get_eob_sample_list(limit: int | None = None) -> list[dict[str, Any]]:
    raise NotImplementedError(_REMOVED_MSG)


def get_837_sample(case_number: int = 0) -> str:
    raise NotImplementedError(_REMOVED_MSG)


def get_837_sample_list(case_numbers: list[int] | None = None) -> list[str]:
    raise NotImplementedError(_REMOVED_MSG)


def get_834_sample(case_number: int = 1) -> str:
    raise NotImplementedError(_REMOVED_MSG)


def get_820_sample(case_number: int = 1) -> str:
    raise NotImplementedError(_REMOVED_MSG)


def list_available_samples() -> dict[str, Any]:
    raise NotImplementedError(_REMOVED_MSG)
