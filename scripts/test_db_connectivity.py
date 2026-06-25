#!/usr/bin/env python3
"""
Database Connectivity Test Script
=================================
Tests connection to the PostgreSQL database and reports environment info.
Can be run on both Windows (dev) and Linux (Pi) to validate configuration.

Usage:
    python scripts/test_db_connectivity.py
    python scripts/test_db_connectivity.py --verbose   # shows extended info
"""

import os
import sys
import platform
import argparse
from datetime import datetime

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

# Load .env file from backend directory
env_path = os.path.join(project_root, 'backend', '.env')
if os.path.exists(env_path):
    from dotenv import load_dotenv
    load_dotenv(env_path)


def test_connection() -> dict:
    """
    Test database connectivity and return status information.

    Returns:
        dict: Connection status dictionary with environment and DB info.
    """
    from backend_functions.database_functions import get_conn, sql_to_dict

    result = {
        "timestamp": datetime.now().isoformat(),
        "platform": platform.system(),
        "hostname": platform.node(),
        "python_version": sys.version,
        "config": {},
        "connection": None,
        "queries": {},
    }

    # Collect configured DB parameters (mask password)
    result["config"]["PG_HOST"] = os.getenv("PG_HOST", "NOT SET")
    result["config"]["PG_PORT"] = os.getenv("PG_PORT", "NOT SET")
    result["config"]["PG_DB"] = os.getenv("PG_DB", "NOT SET")
    result["config"]["PG_USER"] = os.getenv("PG_USER", "NOT SET")
    pw = os.getenv("PG_PASSWORD", "")
    result["config"]["PG_PASSWORD"] = "***" + pw[-4:] if len(pw) > 4 else "NOT SET"
    result["config"]["PGSSLMODE"] = os.getenv("PGSSLMODE", "disable")

    # Test 1: Basic connection
    try:
        conn = get_conn()
        result["connection"] = "OK"
    except Exception as e:
        result["connection"] = f"FAILED: {e}"
        return result

    # Test 2: Simple query (SELECT 1)
    try:
        rows = sql_to_dict("SELECT 1 AS test_col")
        result["queries"]["select_1"] = {
            "status": "OK",
            "result": rows[0]["test_col"] if rows else None,
        }
    except Exception as e:
        result["queries"]["select_1"] = f"FAILED: {e}"

    # Test 3: PostgreSQL version
    try:
        rows = sql_to_dict("SELECT version() AS pg_version")
        result["queries"]["pg_version"] = rows[0]["pg_version"] if rows else None
    except Exception as e:
        result["queries"]["pg_version"] = f"FAILED: {e}"

    # Test 4: Count schemas (excluding system)
    try:
        rows = sql_to_dict("""
            SELECT COUNT(*) AS schema_count
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        """)
        result["queries"]["schema_count"] = rows[0]["schema_count"] if rows else 0
    except Exception as e:
        result["queries"]["schema_count"] = f"FAILED: {e}"

    # Test 5: Check PostGIS availability
    try:
        rows = sql_to_dict("""
            SELECT COUNT(*) AS postgis_available
            FROM pg_extension WHERE extname = 'postgis'
        """)
        result["queries"]["postgis_available"] = (
            rows[0]["postgis_available"] > 0 if rows else False
        )
    except Exception as e:
        result["queries"]["postgis_available"] = f"FAILED: {e}"

    conn.close()
    return result


def pretty_print(result: dict, verbose: bool = False):
    """Print connectivity results in a human-readable format."""
    print("=" * 60)
    print("  PiFitness Database Connectivity Test")
    print("=" * 60)
    print(f"  Timestamp:    {result['timestamp']}")
    print(f"  Platform:     {result['platform']}")
    print(f"  Hostname:     {result['hostname']}")
    print(f"  Python:       {result['python_version'].split()[0]}")
    print()

    print("  [Config]")
    print(f"    PG_HOST:       {result['config']['PG_HOST']}")
    print(f"    PG_PORT:       {result['config']['PG_PORT']}")
    print(f"    PG_DB:         {result['config']['PG_DB']}")
    print(f"    PG_USER:       {result['config']['PG_USER']}")
    print(f"    PG_PASSWORD:   {result['config']['PG_PASSWORD']}")
    print(f"    PGSSLMODE:     {result['config']['PGSSLMODE']}")
    print()

    print("  [Connection]")
    print(f"    Database:      {result['connection']}")
    print()

    print("  [Queries]")
    for key, value in result["queries"].items():
        if isinstance(value, dict):
            print(f"    {key}: {value['status']}")
            if verbose and "result" in value and value["result"] is not None:
                print(f"      ├─ Result: {value['result']}")
        else:
            print(f"    {key}: {value}")

    print()
    if result["connection"] == "OK":
        print("  ✅ All connectivity checks passed!")
    else:
        print("  ❌ Connection failed. Check .env configuration.")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Test database connectivity for PiFitness"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show extended query result information"
    )
    args = parser.parse_args()

    result = test_connection()
    pretty_print(result, verbose=args.verbose)

    # Exit with non-zero code on failure
    if result["connection"] != "OK":
        sys.exit(1)

    # Also exit non-zero if critical queries failed
    for key in ["select_1", "pg_version"]:
        if isinstance(result["queries"].get(key), str) and result["queries"][key].startswith("FAILED"):
            sys.exit(1)


if __name__ == "__main__":
    main()