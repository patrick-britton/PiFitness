#!/usr/bin/env python3
"""tools/db/migrate.py - dbmate-style migration runner for deploy/migrations/.

Usage:
    python tools/db/migrate.py status --env-file PATH
    python tools/db/migrate.py up --env-file PATH
    python tools/db/migrate.py down --env-file PATH
    python tools/db/migrate.py baseline --env-file PATH

PRD commands additionally require --allow-prd. The selected environment file is
this tool's sole source of connection settings; backend/.env and inherited shell
environment variables are not loaded.

Database safety rules:
    APP_ENV=dev  -> local database marked pifitness.env='dev'
    APP_ENV=test -> local database marked pifitness.env='test'
    APP_ENV=prod -> local database with no pifitness.env marker, and --allow-prd

The actual database identity and server port are checked after connecting. All
safety checks complete before migration bookkeeping DDL is allowed.
"""

import argparse
import ipaddress
import os
import re
import sys

from dotenv import dotenv_values
import psycopg2


MIGRATIONS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "deploy", "migrations")
)
SCHEMA_MIGRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

APP_ENV_DEV = "dev"
APP_ENV_TEST = "test"
APP_ENV_PROD = "prod"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _load_env_file(path):
    """Load and validate migration settings exclusively from the selected file."""
    env_path = os.path.abspath(os.path.expanduser(path))
    if not os.path.isfile(env_path):
        raise RuntimeError(
            "Migration environment file does not exist: %s" % env_path
        )

    # Do not interpolate from inherited shell variables.
    values = dotenv_values(env_path, interpolate=False)
    required = ("APP_ENV", "PG_HOST", "PG_DB", "PG_USER", "PG_PASSWORD")
    missing = [
        key
        for key in required
        if not isinstance(values.get(key), str) or not values[key].strip()
    ]
    if missing:
        raise RuntimeError(
            "Migration environment file is missing required settings: %s"
            % ", ".join(missing)
        )

    config = {
        key: value.strip() if isinstance(value, str) else ""
        for key, value in values.items()
    }
    config["APP_ENV"] = config["APP_ENV"].lower()
    if config["APP_ENV"] not in {
        APP_ENV_DEV,
        APP_ENV_TEST,
        APP_ENV_PROD,
    }:
        raise RuntimeError(
            "Unsupported APP_ENV=%r; expected dev, test, or prod."
            % config["APP_ENV"]
        )

    config["PG_HOST"] = config["PG_HOST"].lower()
    config["PG_PORT"] = config.get("PG_PORT") or "5432"
    config["PGSSLMODE"] = config.get("PGSSLMODE") or "disable"

    try:
        port = int(config["PG_PORT"])
    except ValueError:
        raise RuntimeError("PG_PORT must be an integer.") from None
    if not 1 <= port <= 65535:
        raise RuntimeError("PG_PORT must be between 1 and 65535.")
    config["PG_PORT"] = str(port)

    return config


def _conn_params(config):
    """Build connection arguments from the explicitly selected env file."""
    return {
        "host": config["PG_HOST"],
        "port": int(config["PG_PORT"]),
        "user": config["PG_USER"],
        "password": config["PG_PASSWORD"],
        "sslmode": config["PGSSLMODE"],
        "connect_timeout": 5,
    }


def _resolve_db_name(config):
    """Return the database name from the selected migration configuration."""
    app_env = config["APP_ENV"]
    if app_env not in {APP_ENV_DEV, APP_ENV_TEST, APP_ENV_PROD}:
        raise RuntimeError(
            "Migrations require APP_ENV=dev, test, or prod; got %r"
            % (app_env or "unset",)
        )

    dbname = config["PG_DB"].strip()
    if not dbname:
        raise RuntimeError(
            "PG_DB is missing from the selected migration environment file."
        )
    return dbname


def _validate_target(config, params, allow_prd=False):
    """Refuse unauthorized PRD access and all non-loopback database hosts."""
    app_env = config["APP_ENV"]
    _resolve_db_name(config)

    if app_env == APP_ENV_PROD and not allow_prd:
        raise RuntimeError(
            "PRD migrations require the explicit --allow-prd flag."
        )
    if app_env != APP_ENV_PROD and allow_prd:
        raise RuntimeError("--allow-prd is valid only with APP_ENV=prod.")

    host = params.get("host", "").strip().lower()
    if host not in LOCAL_HOSTS:
        raise RuntimeError(
            "Refusing to migrate remote host %r; migrations run only against localhost."
            % host
        )


def _connect(dbname, params):
    """Connect to the selected database using its validated configuration."""
    connection_params = dict(params)
    connection_params["dbname"] = dbname
    return psycopg2.connect(**connection_params)


def _validate_connection_identity(cur, expected_dbname, params):
    """Verify the connected database, server address, and TCP port."""
    cur.execute(
        "SELECT current_database(), inet_server_addr(), inet_server_port()"
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Unable to verify the connected database identity.")

    actual_dbname, server_addr, actual_port = row
    expected_port = int(params["port"])

    if actual_dbname != expected_dbname:
        raise RuntimeError(
            "Connected to database %r, expected %r; refusing to migrate."
            % (actual_dbname, expected_dbname)
        )

    if server_addr is None:
        raise RuntimeError(
            "PostgreSQL did not report a TCP server address; refusing to migrate."
        )
    try:
        is_loopback = ipaddress.ip_address(str(server_addr)).is_loopback
    except ValueError:
        is_loopback = False
    if not is_loopback:
        raise RuntimeError(
            "Connected PostgreSQL server address %r is not loopback; refusing to migrate."
            % (str(server_addr),)
        )

    if actual_port != expected_port:
        raise RuntimeError(
            "Connected to PostgreSQL port %r, expected %r; refusing to migrate."
            % (actual_port, expected_port)
        )


def _read_marker(cur):
    """Return the database marker, or None when it is not configured."""
    cur.execute("SELECT current_setting('pifitness.env', true)")
    row = cur.fetchone()
    marker = row[0] if row else None
    return marker or None


def _guard(app_env, marker):
    """Require the database marker appropriate to the selected environment."""
    if app_env == APP_ENV_DEV:
        expected_mark = "dev"
    elif app_env == APP_ENV_TEST:
        expected_mark = "test"
    elif app_env == APP_ENV_PROD:
        if marker is not None:
            raise RuntimeError(
                "APP_ENV=prod but database marker is %r; "
                "PRD must remain unmarked. Refusing to migrate."
                % (marker,)
            )
        return
    else:
        raise RuntimeError(
            "Unsupported APP_ENV=%r; expected dev, test, or prod."
            % app_env
        )

    if marker is None:
        raise RuntimeError(
            "Database is unmarked (pifitness.env is not set); "
            "refusing to migrate for APP_ENV=%s." % app_env
        )
    if marker != expected_mark:
        raise RuntimeError(
            "APP_ENV=%s but database marker is %r; refusing to migrate "
            "(expected %r)." % (app_env, marker, expected_mark)
        )


def _open_validated_connection(config, allow_prd=False):
    """Connect and validate endpoint, actual identity, and marker before DDL."""
    dbname = _resolve_db_name(config)
    params = _conn_params(config)
    _validate_target(config, params, allow_prd=allow_prd)
    conn = _connect(dbname, params)
    cur = conn.cursor()
    try:
        _validate_connection_identity(cur, dbname, params)
        _guard(config["APP_ENV"], _read_marker(cur))
    except Exception:
        cur.close()
        conn.close()
        raise
    return conn, cur


def _ensure_schema_migrations(cur):
    cur.execute(SCHEMA_MIGRATIONS_SQL)


def _list_migration_files():
    files = []
    for fname in os.listdir(MIGRATIONS_DIR):
        if not fname.endswith(".sql"):
            continue
        match = re.match(r"^(\d+)__", fname)
        if not match:
            continue
        version = int(match.group(1))
        files.append((version, fname, os.path.join(MIGRATIONS_DIR, fname)))
    files.sort(key=lambda item: item[0])
    return files


def _parse_sections(filepath):
    with open(filepath, "r", encoding="utf-8") as migration_file:
        content = migration_file.read()
    up_match = re.search(
        r"--\s*migrate:up\s*\n(.*?)(?:--\s*migrate:down\s*\n|$)",
        content,
        re.DOTALL,
    )
    down_match = re.search(
        r"--\s*migrate:down\s*\n(.*)$", content, re.DOTALL
    )
    up_sql = up_match.group(1).strip() if up_match else ""
    down_sql = down_match.group(1).strip() if down_match else ""
    return up_sql, down_sql


def _applied_versions(cur):
    cur.execute("SELECT version FROM schema_migrations ORDER BY version")
    return {row[0] for row in cur.fetchall()}


def cmd_status(config, allow_prd=False):
    """Print migration status without creating or modifying database objects."""
    conn, cur = _open_validated_connection(config, allow_prd=allow_prd)
    try:
        cur.execute("SELECT to_regclass('schema_migrations')")
        row = cur.fetchone()
        applied = _applied_versions(cur) if row and row[0] is not None else set()

        migrations = _list_migration_files()
        print("Migration status:")
        print(f"{'Version':<10} {'Status':<10} {'Name'}")
        print("-" * 60)
        for version, fname, _ in migrations:
            status = "applied" if version in applied else "pending"
            print(f"{version:<10} {status:<10} {fname}")
    finally:
        cur.close()
        conn.close()


def cmd_up(config, allow_prd=False):
    """Apply all pending migrations after validating the database target."""
    conn, cur = _open_validated_connection(config, allow_prd=allow_prd)
    try:
        _ensure_schema_migrations(cur)
        conn.commit()

        applied = _applied_versions(cur)
        migrations = _list_migration_files()
        pending = [item for item in migrations if item[0] not in applied]
        if not pending:
            print("No pending migrations.")
            return

        for version, fname, filepath in pending:
            up_sql, _ = _parse_sections(filepath)
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
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def cmd_down(config, allow_prd=False):
    """Roll back the latest applied migration that has a down section."""
    conn, cur = _open_validated_connection(config, allow_prd=allow_prd)
    try:
        _ensure_schema_migrations(cur)
        conn.commit()

        applied = _applied_versions(cur)
        migrations = _list_migration_files()
        applied_map = {
            version: (fname, filepath)
            for version, fname, filepath in migrations
            if version in applied
        }
        if not applied_map:
            print("No migrations to roll back.")
            return

        version = max(applied_map)
        fname, filepath = applied_map[version]
        _, down_sql = _parse_sections(filepath)
        if not down_sql:
            print(
                f"Skipping {fname}: no -- migrate:down section found (no rollback)."
            )
            return

        print(f"Rolling back {fname} ...")
        cur.execute(down_sql)
        cur.execute("DELETE FROM schema_migrations WHERE version = %s", (version,))
        conn.commit()
        print(f"  {fname} rolled back.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def cmd_baseline(config, allow_prd=False):
    """Record the version-zero baseline after validating the database target."""
    conn, cur = _open_validated_connection(config, allow_prd=allow_prd)
    try:
        _ensure_schema_migrations(cur)
        conn.commit()

        cur.execute("SELECT 1 FROM schema_migrations WHERE version = 0")
        if cur.fetchone():
            print("Baseline already recorded (version 0 present in schema_migrations).")
            return

        cur.execute("INSERT INTO schema_migrations (version) VALUES (0)")
        conn.commit()
        print("Baseline recorded: version 0 inserted into schema_migrations.")
        print(
            "All migrations are now pending; run "
            "python tools/db/migrate.py up --env-file PATH to apply them."
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Safely inspect and apply PiFitness database migrations."
    )
    parser.add_argument(
        "command",
        choices=("status", "up", "down", "baseline"),
        help="migration operation",
    )
    parser.add_argument(
        "--env-file",
        required=True,
        help="explicit environment file containing migration connection settings",
    )
    parser.add_argument(
        "--allow-prd",
        action="store_true",
        help="explicitly authorize an operation against an unmarked PRD database",
    )
    return parser


def main(argv=None):
    args = _build_parser().parse_args(argv)
    try:
        config = _load_env_file(args.env_file)
        commands = {
            "status": cmd_status,
            "up": cmd_up,
            "down": cmd_down,
            "baseline": cmd_baseline,
        }
        commands[args.command](config, allow_prd=args.allow_prd)
    except (OSError, RuntimeError, psycopg2.Error) as exc:
        print("ERROR: %s" % exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
