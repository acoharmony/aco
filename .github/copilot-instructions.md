# Copilot instructions for acoharmony/aco

Purpose: concise guidance for Copilot sessions to assist contributors and automation.

---

## Quick environment & Python
- Requires Python 3.13.x (pyproject.toml: `requires-python = "==3.13.*"`).
- Project uses Hatch/hatchling + `uv` (CI uses the `uv` workspace helper). Devs can use either `uv` or `hatch`/pip workflows.

## Build, test, and lint (commands observed in repo/CI)
- Install runtime/dev deps (CI):
  - `uv python install 3.13`
  - `uv sync --all-extras`
- Install locally (alternative):
  - `python -m pip install -e '.[dev]'`

- Build wheel:
  - `hatch build`  OR  `python -m hatchling build`

- Run full test suite (CI style, includes coverage collection):
  - `uv run coverage run -m pytest src/acoharmony/_test/ --no-cov -q --tb=short`
  - Locally: `coverage run -m pytest src/acoharmony/_test/`

- Run a single test file/function (use explicit path)
  - Single file: `pytest src/acoharmony/_test/path/to/test_file.py`
  - Single test: `pytest src/acoharmony/_test/path/to/test_file.py::TestClass::test_method`

- Coverage reporting
  - `uv run coverage report --show-missing`
  - `uv run coverage xml -o .dev/coverage.xml`

- Lint / format
  - `ruff check src`  (configuration lives in pyproject.toml under `[tool.ruff]`)
  - `ruff format .` to auto-format according to ruff.format settings
  - CI uses `uv run ...` wrappers; local `ruff` invocation is acceptable.

Notes:
- Coverage is collected with `coverage run -m pytest` (pytest-cov is intentionally disabled in pytest config).
- Pytest config is in pyproject.toml (`[tool.pytest.ini_options]`).

---

## High-level architecture (big picture)
- Purpose: acoharmony is a healthcare data processing platform focused on medallion architecture (Bronze → Silver → Gold) and schema-driven parsing/transform pipelines.

- Entry points
  - CLI: `aco` console script (pyproject `[project.scripts]`) — central orchestrator for transforms, pipelines, storage management, and dev utilities.

- Core subsystems
  - Parsers: `src/acoharmony/_parsers` exposed through `src/acoharmony/parsers.py`. Uses a ParserRegistry and returns polars LazyFrame objects. Parsers apply column typing, schema transformations, and (optionally) source tracking.
  - Transforms / Pipelines: `src/acoharmony/_transforms` (re-exported by `transforms.py`). Transform functions and pipelines are registered via decorators (e.g., `register_pipeline`, `register_crosswalk`). A TransformRunner/Catalog layer orchestrates execution order and dependencies.
  - Storage abstraction: `src/acoharmony/_store.py` provides StorageBackend which maps profiles (local/dev/staging/prod) to storage paths (local filesystem, RustFS/S3-compatible, Databricks). Profiles are loaded from packaged app config (`src/acoharmony/_config/aco.toml`).
  - Tracking & metadata: Parsers and transforms add source-tracking columns and medallion metadata; tracking utilities live under `src/acoharmony/tracking.py`.
  - Packaging & config: project built with hatchling; internal config and profiles are shipped under `src/acoharmony/_config/` so package consumers can load config via importlib.resources.

---

## Key conventions and repo-specific patterns
- Python version locked to 3.13 (tests and tooling assume py313 target in ruff).

- Tests
  - Tests live under `src/acoharmony/_test` (pytest `testpaths` configured accordingly).
  - pytest discovery is nonstandard: `python_files = ["*.py"]` and `python_classes = ["Test*"]`. Many test files do not use the `test_` filename prefix — run tests by path.
  - Common pytest markers (declared in pyproject): `unit`, `integration`, `reconciliation`, `slow`, `requires_data`. Use `-m` to filter.

- Coverage
  - coverage settings in pyproject: report output to `.test-state/coverage.json`, `data_file = ".test-state/.coverage"`, and coverage is run via `coverage run -m pytest` (not pytest-cov).

- Linting & formatting
  - Ruff is configured in pyproject (line-length=100, target-version=py313). Per-file ignores exist for test folders and vendor directories — don't change ruff rules without checking pyproject.

- Re-export pattern
  - Many public modules are compatibility wrappers that re-export implementations from `_`-prefixed subpackages (e.g., `parsers.py` re-exports from `_parsers`, `transforms.py` re-exports from `_transforms`). Edits to public API should usually be made in the underlying `_` packages.

- Transform registration
  - Use provided decorators to register transforms/pipelines rather than mutating registries directly. The runtime discovers and orders transforms via the registry.

- Parser behavior
  - parse_file detects formats by schema or file extension, applies `apply_column_types` and `apply_schema_transformations`, and can add source tracking (default True). Tests and pipelines rely on this predictable behavior.

- Profiles & storage paths
  - Default local workspace: `/opt/s3/data/workspace` (StorageBackend). Use StorageBackend.get_path/get_data_path for resolving medallion paths programmatically.

- Packaging exclusions
  - The build excludes docs, deploy artifacts, and certain internal dev/_docs/_deploy directories — see `[tool.hatch.build].exclude` in pyproject.toml.

---

## Where to look for specific tasks
- Parsers and registration: `src/acoharmony/_parsers` and `src/acoharmony/parsers.py`
- Transform registration and pipeline defs: `src/acoharmony/_transforms` and `src/acoharmony/transforms.py`
- CLI orchestration: `src/acoharmony/cli.py` (entrypoint behavior and flags)
- Storage & profiles: `src/acoharmony/_store.py` and `src/acoharmony/_config`
- Tests: `src/acoharmony/_test` (pytest config in pyproject)

---

If updating these instructions:
- Preserve the commands shown in CI (`.github/workflows/ci.yml`) as the canonical automation flow.

---

---

## Release process and images
- Standard release path:
  - `scripts/release_after_ci.sh`
- The release helper pushes HEAD, waits for CI, tags the next patch version, waits for the GitHub Release workflow, then builds and pushes GHCR images from a clean worktree.
- To enable the same behavior after commits on `main` in a local checkout:
  - `git config acoharmony.releaseAfterCommit true`
- Image builds are still delegated to `.dev/release-images.sh` internally.
  - `./.dev/release-images.sh v0.0.37 --no-push`  # build only
- Versioning note: Hatch is configured to derive the package version from VCS tags (hatch-vcs). Do not edit `src/acoharmony/_version.py` by hand — create the tag described above to bump the release version.

Generated by: repository scan of pyproject.toml, CI workflow, and top-level package modules.
