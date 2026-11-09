#!/usr/bin/env python3
"""tools/db/migrate.py - dbmate-style migration runner for deploy/migrations/.

Usage:
    python tools/db/migrate.py status        Show applied vs pending migrations
    python tools/db/migrate.py up            Apply all pending migrations
    python tools/db/migrate.py down          Roll back the last applied migration
    python tools/db/migrate.py baseline      Mark current schema as the baseline (version 0)

The migration tool runs only against a confirmed local database:
    APP_ENV=dev  -> pifitness_dev, marker must be 'dev'
    APP_ENV=test -> pifitness_test, marker must be 'test'
PRD (APP_ENV unset/prod), remote hosts, unmarked databases, and any mismatch
between the environment-implied database name and the configured database are refused.
Never touches PRD.
"""

import os
import re
import sys

from dotenv import load_dotenv

import psycopg2

_ENV_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "backend", ".env")
if os.path.exists(_ENV_PATH):
    load_dotenv(_ENV_PATH)

MIGRATIONS_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "deploy", "migrations"
)
SCHEMA_MIGRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


APP_ENV_DEV = "dev"
APP_ENV_TEST = "test"
DB_DEV = "pifitness_dev"
DB_TEST = "pifitness_test"
LOCAL_HOSTS = {"localhost", "127.0.0.1"}


def _conn_params():
    return dict(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "15432")),
        user=os.getenv("PG_USER", "dev"),
        password=os.getenv("PG_PASSWORD", ""),
        sslmode=os.getenv("PGSSLMODE", "disable"),
    )


def _resolve_db_name(app_env):
    """Return the database name implied by APP_ENV, or refuse non-migration targets."""
    if app_env == APP_ENV_DEV:
        return DB_DEV
    if app_env == APP_ENV_TEST:
        return DB_TEST
    raise RuntimeError(
        "migrations are not permitted for APP_ENV=%s; "
        "run with APP_ENV=dev (pifitness_dev) or APP_ENV=test (pifitness_test)"
        % (app_env or "unset")
    )


def _validate_target(app_env, params):
    """Refuse remote hosts and database names that contradict APP_ENV."""
    host = params.get("host", "localhost")
    if host not in LOCAL_HOSTS:
        raise RuntimeError(
            "refusing to migrate remote host %r; migrations run only against localhost"
            % (host,)
        )
    configured_db = os.getenv("PG_DB")
    if configured_db is not None and configured_db != _resolve_db_name(app_env):
        raise RuntimeError(
            "PG_DB=%r does not match the database implied by APP_ENV=%s (%r); "
            "refusing to migrate"
            % (configured_db, app_env or "unset", _resolve_db_name(app_env))
        )


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


def _connect(dbname):
    params = _conn_params()
    params["dbname"] = dbname
    return psycopg2.connect(**params)


def _read_marker(cur):
    cur.execute("SHOW pifitness.env")
    row = cur.fetchone()
    return row[0] if row else None


def _guard(app_env, marker):
    """Require a confirmed, matching marker on the migration target.

    The migration tool is stricter than the general startup guard: an unmarked
    database is refused because the marker is the only evidence that the target
    is the intended DEV or TEST database.
    """
    if marker is None:
        target = DB_DEV if app_env == APP_ENV_DEV else DB_TEST
        raise RuntimeError(
            "database %s is unmarked (pifitness.env is not set); refusing to migrate"
            % (target,)
        )
    expected_mark = "dev" if app_env == APP_ENV_DEV else "test"
    if marker != expected_mark:
        raise RuntimeError(
            "APP_ENV=%s but database marker is %r; refusing to migrate (expected %r)"
            % (app_env, marker, expected_mark)
        )


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


def cmd_status(app_env):
    dbname = _resolve_db_name(app_env)
    params = _conn_params()
    _validate_target(app_env, params)
    conn = _connect(dbname)
    cur = conn.cursor()
    _ensure_schema_migrations(cur)
    marker = _read_marker(cur)
    _guard(app_env, marker)
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


def cmd_up(app_env):
    dbname = _resolve_db_name(app_env)
    params = _conn_params()
    _validate_target(app_env, params)
    conn = _connect(dbname)
    cur = conn.cursor()
    _ensure_schema_migrations(cur)
    marker = _read_marker(cur)
    _guard(app_env, marker)
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


def cmd_down(app_env):
    dbname = _resolve_db_name(app_env)
    params = _conn_params()
    _validate_target(app_env, params)
    conn = _connect(dbname)
    cur = conn.cursor()
    _ensure_schema_migrations(cur)
    marker = _read_marker(cur)
    _guard(app_env, marker)
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


def cmd_baseline(app_env):
    dbname = _resolve_db_name(app_env)
    params = _conn_params()
    _validate_target(app_env, params)
    conn = _connect(dbname)
    cur = conn.cursor()
    _ensure_schema_migrations(cur)
    marker = _read_marker(cur)
    _guard(app_env, marker)
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
    app_env = os.getenv("APP_ENV", "")
    if command == "status":
        cmd_status(app_env)
    elif command == "up":
        cmd_up(app_env)
    elif command == "down":
        cmd_down(app_env)
    elif command == "baseline":
        cmd_baseline(app_env)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
