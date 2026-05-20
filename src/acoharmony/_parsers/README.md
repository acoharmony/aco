# _parsers

File parsers for healthcare data sources.

## Purpose

Parse CCLF files, CITE documents, reference files. Extract structured data.

## Parsers

### CCLF Parsers
Parse CMS Claim and Claim Line Feed files.

- Fixed-width format
- Pipe-delimited format
- Header detection
- Type coercion

### CITE Parsers
Parse CMS CITE documents (PDF).

- Table extraction
- Multi-level headers
- Matrix structures
- Text extraction

### Reference Parsers
Parse reference data files.

- PFS rates
- ICD codes
- HCPCS codes
- Crosswalks

## Usage

```python
from acoharmony._parsers import parse_cclf

df = parse_cclf("cclf1", file_path="/data/cclf1.txt")
```

## Key Features

- **Schema-driven** - Uses `_schemas/` definitions
- **Type coercion** - Automatic type conversion
- **Error handling** - Parse errors logged, not silent
- **Lazy loading** - Returns LazyFrame when possible
- **Format detection** - Auto-detect file format

## Integration

- `_schemas/` - Schema definitions
- `_tables/` - Table metadata
- `_cite/` - CITE processing
