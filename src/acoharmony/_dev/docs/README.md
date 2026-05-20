# docs

Documentation generation from code.

## Purpose

Generate documentation from code introspection and templates.

## Modules

- `orchestrator.py` - Orchestrate all doc generation
- `pipelines.py` - Pipeline documentation
- `connectors.py` - Connector documentation
- `notebooks.py` - Notebook documentation
- `lineage.py` - Lineage diagrams

## Usage

```python
from acoharmony._dev import generate_all_documentation

generate_all_documentation()
```

## Output

Docs written to `docs/docs/` for Docusaurus.
