#!/usr/bin/env python3
"""
Validate a SQL query against the local schema cache.
Usage: python validate_sql.py --sql "SELECT * FROM activities.activity_summary"
"""

import argparse
import json
import re
import sys
from pathlib import Path
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / "backend" / ".env"
load_dotenv(dotenv_path=ENV_PATH)
CACHE_FILE = PROJECT_ROOT / 'scripts' / 'schema_cache.json'

def load_cache():
    if not CACHE_FILE.exists():
        print("❌ Schema cache not found. Run `python scripts/schema_cache.py` first.")
        sys.exit(1)
    with open(CACHE_FILE) as f:
        return json.load(f)

def extract_table_names(sql):
    """Extract table references from SQL (simplistic)."""
    # Remove comments and string literals to avoid false positives
    sql_clean = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    sql_clean = re.sub(r"'.*?'", '', sql_clean)
    sql_clean = re.sub(r'".*?"', '', sql_clean)

    # Find FROM / JOIN table references
    # This is a basic regex – for production you might use sqlparse
    pattern = r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*\.?[a-zA-Z_][a-zA-Z0-9_]*)'
    tables = re.findall(pattern, sql_clean, re.IGNORECASE)
    # Also catch table names without schema prefix (assume public? but we want schema)
    # For safety, we'll just look for fully qualified names.
    # We'll keep only those that contain a dot.
    tables = [t for t in tables if '.' in t]
    return tables

def validate(sql):
    cache = load_cache()
    tables = extract_table_names(sql)
    errors = []

    for full_name in tables:
        if full_name not in cache:
            # Try to find a partial match (maybe schema omitted)
            # Our cache uses "schema.table" – we'll warn if not found.
            errors.append(f"❌ Unknown table: {full_name}")
        else:
            # Check columns – we can extract column references from the SELECT and WHERE clauses
            # For simplicity, we'll just check that the table exists.
            # A more thorough check would parse the SQL, but that's complex.
            pass

    # Also check for potential column errors by scanning for identifiers that look like columns
    # This is a stub – we'll just print a warning.
    if not errors:
        print("✅ SQL validation passed (table checks).")
        return True
    else:
        for err in errors:
            print(err)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", help="SQL query to validate")
    args = parser.parse_args()

    if not args.sql:
        print("Please provide SQL via --sql")
        sys.exit(1)

    success = validate(args.sql)
    sys.exit(0 if success else 1)