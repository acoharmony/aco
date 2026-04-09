# _expressions

Expression builders for healthcare data transformations.

## Purpose

Generate Polars expressions for data processing. Registered via decorators. Organized by domain.

## Structure

- **60 modules** (~13,000 lines)
- **Decorator-based registration** via `@register_expression`
- **Metadata-driven** (tier, idempotency, SQL support)
- **Domain-organized** (CCLF, clinical, financial, quality)

## Core Components

### Registry (`_registry.py`)

Central registry for expression discovery.

```python
@register_expression("cclf_adr", schemas=["silver"], dataset_types=["claims"])
class CclfAdrExpression:
    @expression(name="negate_cancellations", tier=["silver"], idempotent=True)
    def negate_cancellations_header() -> list[pl.Expr]:
        return [pl.when(pl.col("clm_adjsmt_type_cd") == "1")
                .then(-pl.col("clm_pmt_amt"))
                .otherwise(pl.col("clm_pmt_amt"))
                .alias("clm_pmt_amt")]
```

**Registry API:**
- `ExpressionRegistry.list_builders()` - List all expressions
- `ExpressionRegistry.get_builder(name)` - Get expression class
- `ExpressionRegistry.is_applicable(name, schema, dataset_type)` - Check applicability
- `ExpressionRegistry.list_for_schema(schema)` - Filter by schema

## Expression Categories

### CCLF Processing
- `_cclf_adr.py` - ADR logic per CMS Implementation Guide
- `_cclf_claim_filters.py` - Claim validation
- `_bene_mbi_map.py` - MBI crosswalk

### Clinical & Quality
- `_cms_hcc.py` - CMS-HCC risk model (17k lines)
- `_chronic_conditions.py` - Chronic condition detection
- `_ccsr.py` - Clinical classification
- `_ed_classification.py` - ED visit classification
- `_readmissions.py` - Readmission detection

### Financial
- `_financial_pmpm.py` - PMPM calculations
- `_pfs_rate_calc.py` - PFS rate lookups (15k lines)
- `_spend_category.py` - Spending categorization

### Utilization & Analytics
- `_service_category.py` - Service categorization (12k lines)
- `_utilization.py` - Utilization metrics
- `_clinical_indicators.py` - Clinical indicators

### Attribution & Alignment
- `_provider_alignment.py` - Provider alignment
- `_provider_attribution.py` - Attribution logic
- `_voluntary_alignment.py` - Voluntary alignment

### Document Processing
- `_cite_extraction.py` - CITE field extraction
- `_matrix_extractor.py` - Matrix table extraction (31k lines)
- `_multi_level_header.py` - Multi-level header parsing (12k lines)
- `_table_layout_detector.py` - Table layout detection (13k lines)

### Reference & Crosswalks
- `_ent_xwalk.py` - Enterprise crosswalk (15k lines)
- `_file_version.py` - File version tracking

## Usage

```python
from acoharmony._expressions import CclfAdrExpression

# Apply expression to LazyFrame
df = df.with_columns(CclfAdrExpression.negate_cancellations_header())
```

## Key Features

- **Idempotent** - Safe to apply multiple times
- **Deterministic** - Same input = same output
- **Fail-fast** - No silent failures
- **Testable** - Pure functions
- **SQL-compatible** - Marked expressions convert to SQL

## Metadata

Each expression declares:
- `name` - Unique identifier
- `tier` - Applicable tiers (bronze/silver/gold)
- `idempotent` - Safe for multiple applications
- `sql_enabled` - SQL conversion support
- `schemas` - Applicable schemas
- `dataset_types` - Applicable datasets
