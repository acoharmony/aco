# CMS Public Use Files (PUF) Module

## Overview

The `_puf` module provides a comprehensive inventory and management system for CMS Public Use Files, with initial focus on the Medicare Physician Fee Schedule (PFS) dataset. It maintains structured metadata for all PFS rules and associated data files from 2002-present, enabling automated batch downloading and processing via integration with the `_cite` module.

## Module Structure

```
_puf/
├── __init__.py                 # Public API exports
├── models.py                   # Pydantic data models
├── pfs_inventory.py            # PFS inventory loader and query functions
├── pfs_data.yaml               # Complete PFS inventory (2002-2024)
├── utils.py                    # Batch processing utilities
├── migrate_legacy_dict.py      # Migration script from legacy dictionary
├── MIGRATION_PLAN.md           # Migration documentation
└── README.md                   # This file
```

## Key Features

[SUCCESS] **Comprehensive Inventory**: 19 years of PFS data (2002-2024), 445+ files
[SUCCESS] **Pydantic Validation**: Type-safe models with automatic validation
[SUCCESS] **YAML Configuration**: Human-readable, version-controlled data inventory
[SUCCESS] **Schema Mapping**: Links PUF files to ACOHarmony schemas (e.g., `pprvu_inputs`)
[SUCCESS] **Batch Processing**: Generate download tasks for automated retrieval
[SUCCESS] **Search & Filter**: Query by year, rule type, category, or schema
[SUCCESS] **Integration**: Seamless integration with `_cite` module for downloads

## Quick Start

### Basic Usage

```python
from acoharmony._puf import pfs_inventory

# Get complete inventory
inv = pfs_inventory.get_inventory()
print(f"Years available: {inv.list_available_years()}")

# Get specific year
year_2024 = pfs_inventory.get_year("2024")
print(f"2024 rules: {list(year_2024.rules.keys())}")

# Get all files for a year and rule type
files = pfs_inventory.get_files_for_year("2024", rule_type="Final")
print(f"Found {len(files)} files")
```

### Query by Category

```python
from acoharmony._puf import pfs_inventory
from acoharmony._puf.models import FileCategory

# Get all GPCI files across all years
gpci_files = pfs_inventory.get_files_by_category(FileCategory.GPCI)

# Returns list of tuples: (year, rule_type, FileMetadata)
for year, rule_type, file_meta in gpci_files:
    print(f"{year} {rule_type}: {file_meta.url}")
```

### Query by Schema Mapping

```python
# Get all files that map to pprvu_inputs schema
rvu_files = pfs_inventory.get_files_by_schema("pprvu_inputs")

for year, rule_type, file_meta in rvu_files:
    print(f"{year}: {file_meta.key}")
```

### Search Files

```python
# Search for files containing "telehealth"
results = pfs_inventory.search_files("telehealth")

for year, rule_type, file_key, file_meta in results:
    print(f"{year} {rule_type}: {file_key}")
```

## Batch Downloading

### Create Download Tasks

```python
from acoharmony._puf import pfs_inventory

# Create tasks for 2024 Final Rule
tasks = pfs_inventory.create_download_tasks(
    year="2024",
    rule_type="Final",
    priority=1,
    tags=["pfs", "2024", "analysis"]
)

print(f"Created {len(tasks)} download tasks")
```

### Batch Download with Progress Tracking

```python
from acoharmony._puf import utils

# Download all files
results = utils.batch_download(
    tasks=tasks,
    max_workers=4,
    delay_between_downloads=1.0,
    skip_existing=True
)

print(f"Downloaded: {results['downloaded']}")
print(f"Skipped: {results['skipped']}")
print(f"Failed: {results['failed']}")
```

### Generate Download Manifest

```python
# Create manifest for documentation
manifest_df = utils.generate_download_manifest(
    tasks,
    output_path="/opt/s3/data/workspace/pfs_2024_manifest.parquet"
)

print(manifest_df)
```

## Data Models

### FileMetadata

Individual file metadata with automatic URL format detection:

```python
from acoharmony._puf.models import FileMetadata, FileCategory

file = FileMetadata(
    key="addenda",
    url="https://www.cms.gov/files/zip/cy-2024-pfs-final-rule-addenda.zip",
    category=FileCategory.ADDENDA,
    schema_mapping="pprvu_inputs",
    description="Final RVU file - maps to pprvu_inputs schema"
)
```

### RuleMetadata

Federal Register rule metadata:

```python
from acoharmony._puf.models import RuleMetadata, RuleType

rule = RuleMetadata(
    year="CY 2024",
    rule_type=RuleType.FINAL,
    citation="88 FR 12345",
    doc_id="2023-12345",
    link="https://www.federalregister.gov/...",
    files={...}  # Dict of FileMetadata
)
```

### DownloadTask

Ready-to-execute download task:

```python
from acoharmony._puf.models import DownloadTask

task = DownloadTask(
    file_metadata=file,
    year="2024",
    rule_type=RuleType.FINAL,
    priority=5,
    force_refresh=False,
    tags=["pfs", "rvu"],
    note="Q4 2024 analysis"
)

# Convert to _cite.transform_cite() kwargs
cite_kwargs = task.to_cite_kwargs()
```

## File Categories

The module categorizes 40+ file types:

### RVU and Payment Files
- `addenda` - Main RVU addendum files → `pprvu_inputs` schema
- `pprvu` - Physician Fee Schedule RVUs
- `pe_rvu` - Practice Expense RVUs → `pe_summary` schema
- `conversion_factor` - Annual conversion factors

### Geographic Files
- `gpci` - Geographic Practice Cost Indices → `gpci_inputs` schema
- `gaf` - Geographic Adjustment Factors → `gaf_inputs` schema

### Practice Expense Inputs
- `direct_pe_inputs` - Direct PE inputs → `pe_inputs_equipment`, `pe_inputs_supplies`
- `clinical_labor` - Clinical labor codes → `pe_inputs_labor` schema
- `pe_worksheet` - Sample PE worksheets
- `indirect_cost_indices` - Indirect cost allocation

### Policy Lists
- `telehealth` - Telehealth service lists
- `designated_care` - Care management services
- `invasive_cardiology` - Invasive cardiology codes
- `mppr` - Multiple procedure payment reduction
- `opps_cap` - Outpatient cap lists

### Impact and Analysis
- `impact` - Payment impact files
- `specialty_assignment` - Specialty assignment tables
- `specialty_impacts` - Specialty-specific impacts
- `misvalued_codes` - Potentially misvalued codes

[See `models.py` for complete list]

## Integration with _cite Module

The `_puf` module integrates seamlessly with the existing `_cite` citation and download system:

```python
from acoharmony._transforms._cite import transform_cite
from acoharmony._puf import pfs_inventory

# Create download task
tasks = pfs_inventory.create_download_tasks("2024", rule_type="Final")

# Execute via _cite
for task in tasks[:5]:  # Download first 5
    cite_kwargs = task.to_cite_kwargs()
    result = transform_cite(**cite_kwargs)

    # File is now in corpus, tracked by state
    print(f"Downloaded: {task.file_metadata.key}")
```

## Utilities

### Check Download Status

```python
from acoharmony._puf import utils

# Check which files are already downloaded
status = utils.check_download_status(tasks)

print(f"Processed: {status['processed']}")
print(f"Not processed: {status['not_processed']}")
```

### Validate Downloads

```python
# Validate downloaded files exist and are valid
validation = utils.validate_file_downloads(tasks, check_file_size=True)

print(f"Valid: {validation['valid']}")
print(f"Invalid: {validation['invalid']}")
print(f"Missing: {validation['missing']}")
```

### Get Corpus Files

```python
# Get corpus files for a specific year
corpus_files = utils.get_corpus_files_for_year("2024", rule_type="Final")

for path in corpus_files:
    print(f"Corpus file: {path}")
```

## YAML Configuration

The inventory is maintained in `pfs_data.yaml`:

```yaml
dataset_name: "Medicare Physician Fee Schedule"
dataset_key: "pfs"
source_agency: "CMS"
description: |
  Annual Medicare Physician Fee Schedule rules...

years:
  "2024":
    Proposed:
      year: "CY 2024"
      citation: "88 FR XXXXX"
      files:
        addenda:
          url: "https://www.cms.gov/..."
          category: "addenda"
          schema_mapping: "pprvu_inputs"
    Final:
      year: "CY 2024"
      files:
        ...
```

## Statistics

**Current Inventory (as of 2024)**:
- **Years**: 19 (2002-2024)
- **Total Files**: 445+
- **Rule Types**: Proposed, Final, Correction
- **Categories**: 40+ distinct file types
- **Schema Mappings**: 8 primary schemas

**Coverage by Year**:
- 2002-2011: Basic Federal Register links
- 2013-2019: Expanded file collections
- 2020-2024: Comprehensive (20-30+ files per rule)

## Extending the Module

### Adding New Years

1. Update `pfs_data.yaml` with new year data
2. Follow existing structure
3. Validate: `pfs_inventory.load_inventory(force_reload=True)`

### Adding New File Categories

1. Add to `FileCategory` enum in `models.py`
2. Update migration script category mappings
3. Re-run migration if needed

### Adding New Datasets

Create similar structure for other CMS datasets:
- Create `{dataset}_data.yaml`
- Create `{dataset}_inventory.py` loader
- Follow PFS pattern

## Migration from Legacy

The module includes a migration script to convert legacy Python dictionaries:

```bash
uv run python src/acoharmony/_puf/migrate_legacy_dict.py
```

This generates `pfs_data_complete.yaml` from the original dictionary structure.

## API Reference

### pfs_inventory

- `get_inventory()` - Get loaded inventory
- `load_inventory(force_reload=False)` - Load from YAML
- `get_year(year)` - Get year inventory
- `get_rule(year, rule_type)` - Get specific rule
- `get_files_for_year(year, rule_type=None)` - Get files
- `get_files_by_category(category, year=None)` - Query by category
- `get_files_by_schema(schema_name)` - Query by schema mapping
- `create_download_tasks(...)` - Generate download tasks
- `list_available_years()` - List all years
- `get_latest_year()` - Get most recent year
- `search_files(search_term, search_in="all")` - Search files

### utils

- `batch_download(tasks, ...)` - Batch download files
- `generate_download_manifest(tasks, output_path=None)` - Create manifest
- `check_download_status(tasks)` - Check download status
- `validate_file_downloads(tasks, ...)` - Validate downloads
- `get_corpus_files_for_year(year, rule_type=None)` - Get corpus files

## Examples

See the comprehensive test in the module for complete usage examples.

## License

© 2025 HarmonyCares. All rights reserved.
