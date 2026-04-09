#!/usr/bin/env python3
"""Check which schemas have tracking and logging."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
from pathlib import Path

# Get all schemas
schemas_dir = Path("/home/care/acoharmony/src/acoharmony/_schemas")
all_schemas = {f.stem for f in schemas_dir.glob("*.yml")}
print(f"Total schemas: {len(all_schemas)}")

# Get tracked schemas
tracking_dir = Path("/home/care/acoharmony/data/logs/tracking")
tracked_schemas = set()

for state_file in tracking_dir.glob("*_state.json"):
    name = state_file.stem.replace("_state", "")
    # Remove prefixes like "raw_to_parquet_"
    if name.startswith("raw_to_parquet_"):
        name = name.replace("raw_to_parquet_", "")
    tracked_schemas.add(name)

print(f"Tracked schemas: {len(tracked_schemas)}")

# Find untracked schemas
untracked = all_schemas - tracked_schemas
print(f"\nUntracked schemas ({len(untracked)}):")
for schema in sorted(untracked):
    print(f"  - {schema}")

# Check recent logs
log_files = list(Path("/home/care/acoharmony/data/logs").glob("*.jsonl"))
if log_files:
    latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
    print(f"\nLatest omnibus log: {latest_log.name}")

    # Sample last few entries
    with open(latest_log) as f:
        lines = f.readlines()
        print(f"Total log entries: {len(lines)}")

        if lines:
            print("\nLast 3 log entries:")
            for line in lines[-3:]:
                entry = json.loads(line)
                print(
                    f"  {entry.get('timestamp', 'N/A')}: {entry.get('level', 'N/A')} - {entry.get('message', 'N/A')[:80]}"
                )

# Check which schemas are referenced in logs
print("\nChecking which schemas appear in logs...")
schemas_in_logs = set()
if log_files:
    for log_file in log_files[-2:]:  # Check last 2 log files
        with open(log_file) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    msg = entry.get("message", "")
                    for schema in all_schemas:
                        if schema in msg:
                            schemas_in_logs.add(schema)
                except Exception:
                    pass

print(f"Schemas mentioned in recent logs: {len(schemas_in_logs)}")
never_logged = all_schemas - schemas_in_logs
if never_logged:
    print(f"\nSchemas never mentioned in logs ({len(never_logged)}):")
    for schema in sorted(never_logged)[:10]:
        print(f"  - {schema}")
    if len(never_logged) > 10:
        print(f"  ... and {len(never_logged) - 10} more")
