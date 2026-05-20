# Fixes applied to the bundled `consolidated_alignments.py` notebook

These fixes were made while wiring the 7 `notebookalignment*functions.py`
tests into CI. Each entry describes a latent bug that surfaced when running
the notebook's `app.run()` against minimal test fixtures (schemas with only
the columns referenced in code, no live parquet data).

The fixes live in the **bundled copy** at
`src/acoharmony/_test/_fixtures/notebooks/consolidated_alignments.py` and
need to be **ported back** to the canonical copy at
`/opt/s3/data/notebooks/consolidated_alignments.py`.

Each entry below gives: line range in the bundled copy, the bug, and the
minimal fix applied. Line numbers are approximate because I'm editing the
same file iteratively — diff the two files for the authoritative fix list.

---

## 1. `newly_added_stats` can be `None` but display cells unconditionally subscript it

**Cell producing it (~line 4142):**
```python
_schema_cols = df.collect_schema().names()
has_transitions = 'newly_added_2025_to_2026' in _schema_cols
if not has_transitions:
    newly_added_stats = None
else:
    # ... compute dict ...
```

**Cell consuming it (~line 4205):** `_total = newly_added_stats['total']` —
crashes with `TypeError: 'NoneType' object is not subscriptable` whenever the
upstream schema lacks `newly_added_2025_to_2026`.

**Fix:** Guard the extraction. If `newly_added_stats is None`, default the
totals to 0/empty DataFrame so the markdown header renders "0 New
Beneficiaries" instead of crashing. Guarded each of the `_top_N` lookups with
`len(_top_3_sources) > N` the same way the existing code already did for
indices 1 and 2.

Also normalized the `_top_1` lookup, which previously assumed at least one
source row — now defaults to `"N/A"` / `0` if the breakdown is empty.

---
