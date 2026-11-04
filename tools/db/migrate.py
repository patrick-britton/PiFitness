#!/usr/bin/env python3
"""tools/db/migrate.py - dbmate-style migration runner for deploy/migrations/.

Usage:
    python tools/db/migrate.py status        Show applied vs pending migrations
    python tools/db/migrate.py up            Apply all pending migrations
    python tools/db/migrate.py down          Roll back the last applied migration
    python tools/db/migrate.py baseline      Mark current DEV schema as the baseline (version 0)

DEV-only: refuses to run unless APP_ENV=dev and the database pifitness.env marker is 'dev'.
Never touches PRD.
"""

import os
import re
import sys

os.environ.setdefault("APP_ENV", "dev")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "backend"))

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from pifitness.core.config import get_settings

MIGRATIONS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "deploy", "migrations"
)
SCHEMA_MIGRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def _conn_params():
    s = get_settings()
    return dict(
        host=s.db_host,
        port=s.db_port,
        user=s.db_user,
        password=s.db_password,
        sslmode=s.db_sslmode,
    )


def _dev_conn():
    params = _conn_params()
    params["dbname"] = "pifitness_dev"
    c = psycopg2.connect(**params)
    return c


def _ensure_schema_migrations(cur):
    cur.execute(SCHEMA_MIGRATIONS_SQL)


def _list_migration_files():
    files = []
    for fname in os.listdir(MIGRATIONS_DIR):
        if not fname.endswith(".sql"):
            continue
        m = re.match(r"^(\d+)__", fname)
        if not m:
            continue
        version = int(m.group(1))
        files.append((version, fname, os.path.join(MIGRATIONS_DIR, fname)))
    files.sort(key=lambda x: x[0])
    return files


def _parse_sections(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    up_match = re.search(r"--\s*migrate:up\s*\n(.*?)(?:--\s*migrate:down\s*\n|$)", content, re.DOTALL)
    down_match = re.search(r"--\s*migrate:down\s*\n(.*)$", content, re.DOTALL)
    up_sql = up_match.group(1).strip() if up_match else ""
    down_sql = down_match.group(1).strip() if down_match else ""
    return up_sql, down_sql


def _applied_versions(cur):
    cur.execute("SELECT version FROM schema_migrations ORDER BY version")
    return {row[0] for row in cur.fetchall()}


def cmd_status():
    conn = _dev_conn()
    cur = conn.cursor()
    _ensure_schema_migrations(cur)
    applied = _applied_versions(cur)
    conn.commit()
    migrations = _list_migration_files()
    print("Migration status:")
    print(f"{'Version':<10} {'Status':<10} {'Name'}")
    print("-" * 60)
    for version, fname, _ in migrations:
        status = "applied" if version in applied else "pending"
        print(f"{version:<10} {status:<10} {fname}")
    conn.close()


def cmd_up():
    conn = _dev_conn()
    cur = conn.cursor()
    _ensure_schema_migrations(cur)
    applied = _applied_versions(cur)
    migrations = _list_migration_files()
    pending = [(v, fn, fp) for v, fn, fp in migrations if v not in applied]
    if not pending:
        print("No pending migrations.")
        conn.close()
        return
    for version, fname, fp in pending:
        up_sql, _ = _parse_sections(fp)
        if not up_sql:
            print(f"Skipping {fname}: no -- migrate:up section found.")
            continue
        print(f"Applying {fname} ...")
        cur.execute(up_sql)
        cur.execute(
            "INSERT INTO schema_migrations (version) VALUES (%s)",
            (version,),
        )
        conn.commit()
        print(f"  {fname} applied.")
    conn.close()


def cmd_down():
    conn = _dev_conn()
    cur = conn.cursor()
    _ensure_schema_migrations(cur)
    applied = _applied_versions(cur)
    migrations = _list_migration_files()
    applied_map = {v: (fn, fp) for v, fn, fp in migrations if v in applied}
    if not applied_map:
        print("No migrations to roll back.")
        conn.close()
        return
    version = max(applied_map.keys())
    fname, fp = applied_map[version]
    _, down_sql = _parse_sections(fp)
    if not down_sql:
        print(f"Skipping {fname}: no -- migrate:down section found (no rollback).")
        conn.close()
        return
    print(f"Rolling back {fname} ...")
    cur.execute(down_sql)
    cur.execute("DELETE FROM schema_migrations WHERE version = %s", (version,))
    conn.commit()
    print(f"  {fname} rolled back.")
    conn.close()


def cmd_baseline():
    conn = _dev_conn()
    cur = conn.cursor()
    _ensure_schema_migrations(cur)
    cur.execute("SELECT 1 FROM schema_migrations WHERE version = 0")
    if cur.fetchone():
        print("Baseline already recorded (version 0 present in schema_migrations).")
        conn.close()
        return
    cur.execute("INSERT INTO schema_migrations (version) VALUES (0)")
    conn.commit()
    print("Baseline recorded: version 0 inserted into schema_migrations.")
    print("All migrations are now pending; run 'python tools/db/migrate.py up' to apply them.")
    conn.close()


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/db/migrate.py {status|up|down|baseline}")
        sys.exit(1)
    command = sys.argv[1].lower()
    if command == "status":
        cmd_status()
    elif command == "up":
        cmd_up()
    elif command == "down":
        cmd_down()
    elif command == "baseline":
        cmd_baseline()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
