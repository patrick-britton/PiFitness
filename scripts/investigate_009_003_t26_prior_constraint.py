#!/usr/bin/env python3
"""
009-003 T26 / Bug 009-003-26-1 — archaeology: what unique constraint did
activities.activity_details carry BEFORE the 2026-09-27/28 repair?
================================================================================

Read-only. Answers the question "was the original arbiter (activity_id,
elapsed_duration_s)?" from evidence rather than memory:

1. the pre-repair schema documentation (memory-bank/schema_documentation.json,
   generated 2026-06-24) entry for activities.activity_details;
2. every relation in the database whose name contains 'activity_details'
   (looking for the salvage script's `<table>_corrupt` / `<table>_salvage`
   leftovers — pg_salvage.py renames the old table and its indexes and keeps
   the saved constraint definitions, so remnants preserve the prior state);
3. the full index/constraint set on activities.activity_details now;
4. any unique index anywhere in the database on (activity_id,
   elapsed_duration_s), (elapsed_duration_s) or (ts_utc) — the three candidate
   arbiters;
5. every routine whose source mentions activity_details, with the ON CONFLICT
   targets it uses (catches stale variants in *_migration schemas).
"""
import json
import re
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / 'backend' / '.env')

from backend_functions.database_functions import sql_to_dict

DOC_FILE = project_root / 'memory-bank' / 'schema_documentation.json'


def section(title):
    print(f"\n=== {title} ===")


def documented_pre_repair_state():
    """What the 2026-06-24 schema documentation recorded for this table."""
    section('1. pre-repair documentation (memory-bank/schema_documentation.json)')
    if not DOC_FILE.exists():
        print('  documentation file not found')
        return
    doc = json.loads(DOC_FILE.read_text(encoding='utf-8'))
    print(f"  generated_at: {doc.get('generated_at')}")
    schemas = doc.get('schemas', {})
    hit = None
    for schema_name, schema in schemas.items():
        tables = schema.get('tables', schema) if isinstance(schema, dict) else {}
        if isinstance(tables, dict):
            for table_name, table in tables.items():
                if table_name == 'activity_details' and schema_name == 'activities':
                    hit = (schema_name, table)
    if not hit:
        print("  no activities.activity_details entry found; top-level schema keys:",
              list(schemas)[:20])
        return
    schema_name, table = hit
    if isinstance(table, dict):
        print(f"  entry keys: {list(table)}")
        for key, value in table.items():
            if key.lower() in ('columns',):
                names = [c.get('column_name') or c.get('name') for c in value] if isinstance(value, list) else value
                print(f"  {key} ({len(names)}): {names}")
            else:
                text = json.dumps(value)
                print(f"  {key}: {text[:400]}")
    else:
        print(f'  {schema_name}.activity_details: {str(table)[:400]}')
