#!/usr/bin/env python3
"""
Build a local cache of all table columns in the PiFitness database.
Run this whenever the schema changes (or daily).
"""

import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / "backend" / ".env"
load_dotenv(dotenv_path=ENV_PATH)

from backend_functions.database_functions import con_cur

# Schemas we care about – adjust as needed
TARGET_SCHEMAS = [
    'activities', 'activities_migration',
    'health', 'health_migration',
    'music', 'music_migration',
    'staging', 'staging_migration',
    'logging', 'tasks', 'api_services'
]

CACHE_FILE = PROJECT_ROOT / 'scripts' / 'schema_cache.json'

def build_cache():
    conn, cursor = con_cur()
    cache = {}

    for schema in TARGET_SCHEMAS:
        # Get all tables and views in this schema (views included so
        # validate_sql.py can check queries that reference views)
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type IN ('BASE TABLE', 'VIEW')
        """, (schema,))
        tables = cursor.fetchall()

        for (table_name,) in tables:
            # Get columns for this table
            cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            columns = cursor.fetchall()
            cache[f"{schema}.{table_name}"] = [
                {'name': col[0], 'type': col[1], 'nullable': col[2] == 'YES'}
                for col in columns
            ]

    cursor.close()
    conn.close()

    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2, sort_keys=True)

    print(f"✅ Schema cache written to {CACHE_FILE}")
    print(f"   Total tables cached: {len(cache)}")

if __name__ == "__main__":
    build_cache()