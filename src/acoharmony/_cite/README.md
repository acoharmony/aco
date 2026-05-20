# _cite

CMS CITE document processing.

## Purpose

Download, parse, extract, and process CMS CITE documents. Automate reference data extraction.

## CITE Documents

CMS Internet-Only Manual (IOM) documents containing:
- PFS rates
- HCPCS codes
- Payment policies
- Coverage guidelines

## Workflow

1. **Download** - Fetch CITE PDFs from CMS
2. **Extract** - Extract tables and text
3. **Parse** - Structure data into tables
4. **Process** - Clean and standardize
5. **Store** - Save to bronze tier

## Key Features

- **Automated downloads** - Fetch latest CITE docs
- **Table extraction** - Extract tables from PDFs
- **Multi-level headers** - Handle complex table structures
- **Matrix extraction** - Extract matrix-structured data
- **Version tracking** - Track document versions

## Integration

- `_parsers/` - PDF parsing
- `_expressions/` - CITE-specific expressions
- `_transforms/` - CITE processing transforms
- `_tables/` - CITE table definitions

## Usage

```python
from acoharmony._cite import download_cite, extract_tables

# Download CITE document
pdf_path = download_cite("MLN123456")

# Extract tables
tables = extract_tables(pdf_path)
```
