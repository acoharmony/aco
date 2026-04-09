# © 2025 HarmonyCares
# All rights reserved.

"""
Tuva Health seed data integration for ACO Harmony.

DEPRECATION NOTICE
==================

The Tuva integration has been deprecated. Only seed/reference data management remains.

This module now provides ONLY:
- TuvaSeedManager: Download and convert Tuva reference data (terminology, value sets, etc.)

All other Tuva integration components (TuvaBridge, TuvaRunner, TuvaPipeline, etc.)
have been removed. Use acoharmony's native Polars-based transforms instead.

Reference Data Available
========================

Tuva provides valuable reference data including:
- Medical code terminologies (ICD-10-CM, ICD-9-CM, HCPCS, CPT, NDC)
- Value sets for quality measures
- CMS-HCC risk adjustment mappings
- Clinical classification systems (CCSR)
- ED classification tables

These reference datasets are downloaded from the public Tuva S3 bucket and
converted to parquet format in the silver layer for use in transforms.

Usage
=====

```python
from acoharmony._tuva import TuvaSeedManager

# Initialize seed manager
seed_manager = TuvaSeedManager()

# Download and convert all reference data to silver layer
seed_manager.sync_all_seeds()
```

Or via CLI:

```bash
# List available seeds
aco tuva seeds list

# Download and convert all seeds
aco tuva seeds sync
```

"""

from .seed_manager import TuvaSeedManager

__all__ = [
    "TuvaSeedManager",
]
