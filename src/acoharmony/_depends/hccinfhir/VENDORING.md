# Vendored: hccinfhir

This directory is a snapshot of the upstream
[`hccinfhir`](https://github.com/mimilabs/hccinfhir) library,
committed to this repository at a fixed version so that the CMS HCC
risk-score calculation used by BNMR reconciliation is reproducible from
a known state.

## Upstream snapshot

| Field | Value |
| --- | --- |
| Upstream project | `hccinfhir` |
| Upstream version | `0.3.3` |
| PyPI sdist | `https://files.pythonhosted.org/packages/source/h/hccinfhir/hccinfhir-0.3.3.tar.gz` |
| sdist SHA-256 | `c4d81c99f0f519522acd183e68671aafc218bb1b68e5f19ebdad7b5384b9aa6b` |
| Upstream license | Apache License 2.0 (preserved in `LICENSE`) |
| Vendored on | 2026-04-11 |
| Vendored by | acoharmony reconciliation work |

The `LICENSE` file in this directory is the verbatim Apache-2.0 license
shipped by the upstream sdist and MUST NOT be removed.

## Modifications applied during vendoring

The upstream source was modified in a small number of strictly mechanical
ways so it could live at the path
`src/acoharmony/_depends/hccinfhir/` instead of at the top-level
`hccinfhir` package. All changes are listed here so a future update can
reapply them cleanly.

1. **Absolute → relative imports.** Every `from hccinfhir.<module> import
   ...` statement was rewritten to `from .<module> import ...`. This
   affected 36 import sites across 16 Python files. The `if __name__ ==
   '__main__':` demo block in `extractor_820.py` also had a
   `from hccinfhir import get_820_sample` statement rewritten to
   `from . import get_820_sample`.

2. **String-literal package reference in `utils.py`.** The call
   `importlib.resources.path('hccinfhir.data', file_path)` was changed to
   `importlib.resources.path('acoharmony._depends.hccinfhir.data',
   file_path)`. This is the one place upstream uses a string literal
   instead of a relative import to reach its own data directory. A
   nearby error message was updated to match the new package path.

3. **`sample_files/` directory removed.** Upstream ships roughly 2 MB of
   X12 820/834/837 sample files. acoharmony uses CCLF, not X12, so these
   were deleted to keep the committed footprint small. The
   `samples.py` module is still present because the upstream
   `__init__.py` imports it eagerly, but its entire body was replaced
   with stub classes and functions that preserve the same names
   (`SampleData`, `get_eob_sample`, …, `list_available_samples`) and
   raise `NotImplementedError` at call time with a message pointing at
   the upstream repo. This keeps `from acoharmony._depends import
   hccinfhir` working but makes any accidental use of the samples API
   fail loudly.

4. **Nothing else.** The model files (`model_calculate.py`,
   `model_coefficients.py`, `model_demographics.py`, `model_dx_to_cc.py`,
   `model_edits.py`, `model_hierarchies.py`, `model_interactions.py`) and
   every `data/*.csv` coefficient table are **byte-identical** to the
   upstream sdist. The calculation logic and CMS reference data were
   deliberately not touched.

## Known upstream issues preserved in this vendor

- **Version bug:** `__init__.py` declares `__version__ = "0.3.1"` even
  though the sdist we pulled is `0.3.3`. Upstream forgot to bump the
  module-level `__version__` when cutting the release. The real version
  is the one recorded above in the sdist SHA; do not rely on the
  Python-level `__version__` attribute.

- **Coefficients only back to 2025.** The `data/` directory ships
  `ra_coefficients_2025.csv`, `ra_coefficients_2026.csv`, and
  `ra_proposed_coefficients_2027.csv`, but does NOT include historical
  2023 or 2024 tables. BNMR reconciliation against PY2023 and PY2024
  will need historical CMS coefficient tables provided separately
  (tracked as follow-up work in the reconciliation PR arc).

## How to update

1. Download the new sdist from PyPI.
2. Record its new SHA-256 in this file.
3. Extract it and diff against the current vendored copy.
4. If only source files changed (no data files), re-apply the three
   mechanical edits above (relative imports, `utils.py` string literal,
   `samples.py` stub replacement) and verify the import test in
   `src/acoharmony/_test/depends/hccinfhir.py` still passes.
5. If data files changed, carefully review the diff — a CMS coefficient
   change is a **reconciliation-affecting** event and should be its own
   reviewed PR.

Do not hand-edit files in this directory for any other reason. If we
need to patch behavior, prefer wrapping in `acoharmony._risk` instead
of modifying the vendored source.
