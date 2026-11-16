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
  8. Replay pending migrations from deploy/migrations/ onto pifitness_dev.
  9. Print elapsed reset time.

This is a DEV-only safety tool. It never touches PRD.
"""

import os
import subprocess
import sys
import tempfile
import time

# DEV-only: tools must run with APP_ENV=dev (D8). Set before config import so
# load_dotenv does not override it.
os.environ.setdefault("APP_ENV", "dev")

# Ensure backend/ is importable for config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "backend"))

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from pifitness.core.config import get_settings

# Shared env-file helpers (replaces migrate.py's non-migration-specific symbols).
sys.path.insert(0, os.path.dirname(__file__))
import _env  # noqa: E402


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

    # Replay pending migrations so DEV converges to the migration baseline
    # after the template clone (or after a PRD restore via sync-prod-to-dev.ps1).
    _replay_pending_migrations()

    elapsed = time.perf_counter() - t0
    print(f"dev_reset: complete in {elapsed:.2f}s")

def _write_dev_env_file() -> str:
    """Write a temporary dotenv file for the DEV database connection.

    Returns the path to the temp file. The caller is responsible for removing
    it after dbmate completes.
    """
    s = get_settings()
    lines = [
        f"APP_ENV=dev",
        f"PG_HOST={s.db_host}",
        f"PG_DB={DEV_DB}",
        f"PG_USER={s.db_user}",
        f"PG_PASSWORD={s.db_password}",
        f"PG_PORT={s.db_port}",
        f"PGSSLMODE={s.db_sslmode}",
    ]
    fd, path = tempfile.mkstemp(prefix="pifitness-dev-env-", suffix=".env")
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write("\n".join(lines) + "\n")
    os.chmod(path, 0o600)
    return path


def _replay_pending_migrations():
    """Apply pending dbmate migrations from deploy/migrations/ onto DEV.

    Runs after the template clone or a PRD restore so pifitness_dev converges
    to the current migration baseline. All migrations run through dbmate
    dynamically — none are hard-coded or skipped. If a migration fails (for
    example, migration 001's _migration pre-check on post-INT19b DEV), dbmate
    exits non-zero and this function propagates the failure so the operator
    remediates in DEV before attempting test.

    Migration 001 (INT-19c) runs exactly like every other migration: its up
    pre-check requires the _migration schema. On DEV after INT-19b that schema
    is absent, so dbmate reports failure and the reset stops. The operator must
    remediate in DEV (or roll the migration back) before proceeding.
    """
    s = get_settings()

    # Ensure the schema_migrations table is dbmate-compatible (dirty column)
    # before dbmate attempts to write to it.
    dev_params = _conn_params()
    dev_params["dbname"] = DEV_DB
    admin_conn = psycopg2.connect(**dev_params)
    admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        cur = admin_conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version BIGINT PRIMARY KEY,
                dirty BOOLEAN NOT NULL DEFAULT false,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                filename TEXT,
                checksum TEXT
            )
            """
        )
        # Add dirty column if the table predates dbmate (migrate.py ledger lacked it).
        cur.execute(
            "ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS dirty BOOLEAN NOT NULL DEFAULT false"
        )
        cur.close()
    finally:
        admin_conn.close()

    # Write the env file dbmate_guard.sh will read. This is the sole source of
    # connection settings for dbmate; backend/.env and inherited shell env are
    # not loaded by the guard wrapper.
    env_file = _write_dev_env_file()
    try:
        guard = os.path.join(os.path.dirname(__file__), "dbmate_guard.sh")
        result = subprocess.run(
            ["bash", guard, "up", "--env-file", env_file],
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"dbmate up failed with exit code {result.returncode}; "
                "stopping DEV reset. Remediate in DEV before attempting test."
            )
    finally:
        try:
            os.unlink(env_file)
        except OSError:
            pass


if __name__ == "__main__":
    main()