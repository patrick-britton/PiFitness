#!/usr/bin/env python3
"""tools/db/dev_reset.py - Drop and recreate the DEV database from a template, then stamp the D8 marker.

Usage:
    python tools/db/dev_reset.py

Steps:
  1. Connect to the postgres admin database on DEV host/port.
  2. Terminate all connections to pifitness_dev.
  3. Create pifitness_dev_template from the current pifitness_dev (template clone).
  4. Drop pifitness_dev.
  5. Recreate pifitness_dev from the template (CREATE DATABASE ... TEMPLATE).
  6. Drop the template.
  7. Stamp the D8 marker: ALTER DATABASE pifitness_dev SET pifitness.env = 'dev'.
  8. Print elapsed reset time.

This is a DEV-only safety tool. It never touches PRD.
"""

import os
import sys
import time

# Ensure backend/ is importable for config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "backend"))

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from pifitness.core.config import get_settings


ADMIN_DB = "postgres"
DEV_DB = "pifitness_dev"
TEMPLATE_DB = "pifitness_dev_template"


def _conn_params():
    s = get_settings()
    return dict(
        host=s.db_host,
        port=s.db_port,
        user=s.db_user,
        password=s.db_password,
        sslmode=s.db_sslmode,
    )


def _admin_conn():
    params = _conn_params()
    params["dbname"] = ADMIN_DB
    c = psycopg2.connect(**params)
    c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return c


def _terminate_connections(cur, dbname):
    cur.execute(
        """
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = %s AND pid <> pg_backend_pid()
        """,
        (dbname,),
    )


def main():
    t0 = time.perf_counter()
    print(f"dev_reset: resetting {DEV_DB} on {get_settings().db_host}:{get_settings().db_port}")

    admin = _admin_conn()
    cur = admin.cursor()

    # Step 1: if DEV exists, clone it into a template database first (preserves
    # TimescaleDB extensions and schema), then drop DEV and recreate from template.
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DEV_DB,))
    dev_exists = cur.fetchone()

    if dev_exists:
        # Clean up any leftover template from a previous run
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TEMPLATE_DB,))
        if cur.fetchone():
            _terminate_connections(cur, TEMPLATE_DB)
            cur.execute(f'DROP DATABASE "{TEMPLATE_DB}"')
            print(f"  dropped stale template {TEMPLATE_DB}")

        _terminate_connections(cur, DEV_DB)
        cur.execute(f'CREATE DATABASE "{TEMPLATE_DB}" TEMPLATE = "{DEV_DB}"')
        print(f"  created template {TEMPLATE_DB} from {DEV_DB}")

        _terminate_connections(cur, DEV_DB)
        cur.execute(f'DROP DATABASE "{DEV_DB}"')
        print(f"  dropped {DEV_DB}")

    # Step 2: create DEV from template (or fresh if no template/source)
    if dev_exists:
        cur.execute(f'CREATE DATABASE "{DEV_DB}" TEMPLATE = "{TEMPLATE_DB}"')
        print(f"  created {DEV_DB} from template")
        _terminate_connections(cur, TEMPLATE_DB)
        cur.execute(f'DROP DATABASE "{TEMPLATE_DB}"')
        print(f"  dropped template {TEMPLATE_DB}")
    else:
        cur.execute(f'CREATE DATABASE "{DEV_DB}"')
        print(f"  created fresh {DEV_DB}")

    cur.close()
    admin.close()

    # Step 3: stamp the D8 marker on DEV
    dev_params = _conn_params()
    dev_params["dbname"] = DEV_DB
    dev_conn = psycopg2.connect(**dev_params)
    dev_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    dev_cur = dev_conn.cursor()
    dev_cur.execute("ALTER DATABASE pifitness_dev SET pifitness.env = 'dev'")
    dev_cur.close()
    dev_conn.close()
    print("  stamped pifitness.env = 'dev' on pifitness_dev")

    elapsed = time.perf_counter() - t0
    print(f"dev_reset: complete in {elapsed:.2f}s")


if __name__ == "__main__":
    main()