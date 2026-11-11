#!/usr/bin/env python3
"""Safely inspect and apply PiFitness SQL migrations.

Examples:
    python tools/db/migrate.py status --env-file PATH --format jsonl
    python tools/db/migrate.py up --env-file PATH --format jsonl
    python tools/db/migrate.py up --env-file PATH --adopt-legacy-checksums
    python tools/db/migrate.py down --env-file PATH
    python tools/db/migrate.py baseline --env-file PATH

PRD commands additionally require --allow-prd. The selected environment file is
this tool's sole source of connection settings; backend/.env and inherited shell
environment variables are not loaded.

JSON Lines output is intended for deployment orchestration. Every migration
start/result is a separate JSON object on stdout; human diagnostics go to
stderr and the process exit code remains authoritative.
"""

import argparse
import hashlib
import ipaddress
import json
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
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    filename TEXT,
    checksum TEXT
)
"""
SCHEMA_MIGRATIONS_UPGRADE_SQL = (
    "ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS filename TEXT",
    "ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT",
)

APP_ENV_DEV = "dev"
APP_ENV_TEST = "test"
APP_ENV_PROD = "prod"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
OUTPUT_TEXT = "text"
OUTPUT_JSONL = "jsonl"


class MigrationIntegrityError(RuntimeError):
    """The database migration ledger does not match the checked-out package."""

    def __init__(self, message, *, version=None, filename=None):
        super().__init__(message)
        self.version = version
        self.filename = filename


class MigrationExecutionError(RuntimeError):
    """A specific migration failed while being applied."""

    def __init__(self, version, filename, checksum, cause, rollback_error=None):
        self.version = version
        self.filename = filename
        self.checksum = checksum
        self.cause = cause
        self.sqlstate = getattr(cause, "pgcode", None)
        self.rollback_error = rollback_error
        super().__init__(
            "migration %s (%s) failed%s: %s"
            % (
                version,
                filename,
                " [SQLSTATE %s]" % self.sqlstate if self.sqlstate else "",
                str(cause).strip(),
            )
        )


def _emit_event(output_format, event, **fields):
    """Emit one machine-readable event or a compact human-readable message."""
    if output_format == OUTPUT_JSONL:
        payload = {"event": event}
        payload.update(fields)
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        return

    if event == "migration_status":
        print("%s  %-8s  %s" % (fields.get("version", "-"), fields["status"], fields["file"]))
        if fields.get("integrity") == "legacy_unverified":
            print("  WARNING: legacy migration record has no stored checksum: %s" % fields["file"])
    elif event == "migration_started":
        print("Applying %s ..." % fields["file"])
    elif event == "migration_succeeded":
        print("  %s applied." % fields["file"])
    elif event == "migration_failed":
        print("  FAILED: %s (version %s): %s" % (fields["file"], fields["version"], fields["message"]))
    elif event == "migration_legacy_checksum_adopted":
        print("  Recorded checksum for previously applied migration %s." % fields["file"])
    elif event == "migration_summary":
        print(fields.get("message", "Migration operation complete."))
    elif event == "migration_integrity_error":
        print("ERROR: %s" % fields.get("message", "migration integrity failure"), file=sys.stderr)


def _load_env_file(path):
    """Load and validate migration settings exclusively from the selected file."""
    env_path = os.path.abspath(os.path.expanduser(path))
    if not os.path.isfile(env_path):
        raise RuntimeError("Migration environment file does not exist: %s" % env_path)

    # Do not interpolate from inherited shell variables.
    values = dotenv_values(env_path, interpolate=False)
    required = ("APP_ENV", "PG_HOST", "PG_DB", "PG_USER", "PG_PASSWORD")
    missing = [
        key for key in required
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
    if config["APP_ENV"] not in {APP_ENV_DEV, APP_ENV_TEST, APP_ENV_PROD}:
        raise RuntimeError(
            "Unsupported APP_ENV=%r; expected dev, test, or prod." % config["APP_ENV"]
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
        raise RuntimeError("PG_DB is missing from the selected migration environment file.")
    return dbname


def _validate_target(config, params, allow_prd=False):
    """Refuse unauthorized PRD access and all non-loopback database hosts."""
    app_env = config["APP_ENV"]
    _resolve_db_name(config)
    if app_env == APP_ENV_PROD and not allow_prd:
        raise RuntimeError("PRD migrations require the explicit --allow-prd flag.")
    if app_env != APP_ENV_PROD and allow_prd:
        raise RuntimeError("--allow-prd is valid only with APP_ENV=prod.")
    host = params.get("host", "").strip().lower()
    if host not in LOCAL_HOSTS:
        raise RuntimeError(
            "Refusing to migrate remote host %r; migrations run only against localhost." % host
        )


def _connect(dbname, params):
    """Connect to the selected database using its validated configuration."""
    connection_params = dict(params)
    connection_params["dbname"] = dbname
    return psycopg2.connect(**connection_params)


def _normalize_server_address(server_addr):
    """Return the canonical IP address from PostgreSQL's inet text output.

    PostgreSQL may render inet_server_addr() as an address with a prefix,
    e.g. ``::1/128`` or ``127.0.0.1/32``. ip_address() rejects those strings;
    ip_interface() correctly parses both forms and exposes the host address.
    """
    raw = str(server_addr).strip()
    if not raw:
        raise ValueError("empty PostgreSQL server address")
    try:
        return ipaddress.ip_address(raw)
    except ValueError:
        try:
            return ipaddress.ip_interface(raw).ip
        except ValueError as exc:
            raise ValueError("invalid PostgreSQL server address %r" % raw) from exc


def _validate_connection_identity(cur, expected_dbname, params):
    """Verify the connected database, server address, and TCP port."""
    cur.execute("SELECT current_database(), inet_server_addr(), inet_server_port()")
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
        raise RuntimeError("PostgreSQL did not report a TCP server address; refusing to migrate.")
    try:
        normalized_address = _normalize_server_address(server_addr)
    except ValueError as exc:
        raise RuntimeError("%s; refusing to migrate." % exc) from exc
    if not normalized_address.is_loopback:
        raise RuntimeError(
            "Connected PostgreSQL server address %r is not loopback; refusing to migrate."
            % str(normalized_address)
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
                "APP_ENV=prod but database marker is %r; PRD must remain unmarked. "
                "Refusing to migrate." % marker
            )
        return
    else:
        raise RuntimeError("Unsupported APP_ENV=%r; expected dev, test, or prod." % app_env)

    if marker is None:
        raise RuntimeError(
            "Database is unmarked (pifitness.env is not set); refusing to migrate for APP_ENV=%s."
            % app_env
        )
    if marker != expected_mark:
        raise RuntimeError(
            "APP_ENV=%s but database marker is %r; refusing to migrate (expected %r)."
            % (app_env, marker, expected_mark)
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
    """Create/upgrade the ledger; caller must have validated the DB target."""
    cur.execute(SCHEMA_MIGRATIONS_SQL)
    for statement in SCHEMA_MIGRATIONS_UPGRADE_SQL:
        cur.execute(statement)


def _sha256_file(filepath):
    """Hash migration content with platform-independent newline handling.

    Git checkouts on Windows may materialize LF files as CRLF. Normalize CRLF
    and lone CR to LF before hashing so the same migration has the same ledger
    checksum on Windows and Linux. All other bytes (including whitespace,
    comments, and SQL text) remain significant.
    """
    with open(filepath, "rb") as source:
        content = source.read()
    normalized = content.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(normalized).hexdigest()


def _list_migration_files():
    files = []
    for fname in os.listdir(MIGRATIONS_DIR):
        if not fname.endswith(".sql"):
            continue
        match = re.match(r"^(\d+)__", fname)
        if not match:
            continue
        version = int(match.group(1))
        filepath = os.path.join(MIGRATIONS_DIR, fname)
        files.append((version, fname, filepath))
    files.sort(key=lambda item: item[0])
    seen = {}
    for version, fname, _ in files:
        if version in seen:
            raise RuntimeError(
                "Duplicate migration version %s: %s and %s" % (version, seen[version], fname)
            )
        seen[version] = fname
    return files


def _parse_sections(filepath):
    with open(filepath, "r", encoding="utf-8") as migration_file:
        content = migration_file.read()
    up_match = re.search(
        r"^\s*--\s*migrate:up\s*\r?\n(.*?)(?:^\s*--\s*migrate:down\s*\r?\n|\Z)",
        content,
        re.DOTALL | re.MULTILINE | re.IGNORECASE,
    )
    down_match = re.search(
        r"^\s*--\s*migrate:down\s*\r?\n(.*)\Z",
        content,
        re.DOTALL | re.MULTILINE | re.IGNORECASE,
    )
    up_sql = up_match.group(1).strip() if up_match else ""
    down_sql = down_match.group(1).strip() if down_match else ""
    return up_sql, down_sql


def _applied_versions(cur):
    cur.execute("SELECT version FROM schema_migrations ORDER BY version")
    return {row[0] for row in cur.fetchall()}


def _ledger_metadata_columns(cur):
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = 'schema_migrations' "
        "AND column_name IN ('filename', 'checksum')"
    )
    return {row[0] for row in cur.fetchall()}


def _current_migration_map(migrations):
    return {version: (fname, filepath, _sha256_file(filepath)) for version, fname, filepath in migrations}


def _read_extended_ledger(cur):
    cur.execute(
        "SELECT version, filename, checksum FROM schema_migrations ORDER BY version"
    )
    return {row[0]: (row[1], row[2]) for row in cur.fetchall()}


def _validate_extended_ledger(ledger, current_map, *, allow_legacy=False):
    """Validate applied rows; legacy checksum adoption is opt-in and explicit."""
    legacy = []
    for version, (stored_filename, stored_checksum) in sorted(ledger.items()):
        if version == 0:
            continue
        current = current_map.get(version)
        if current is None:
            raise MigrationIntegrityError(
                "database records applied migration version %s, but that version is "
                "absent from this package" % version,
                version=version,
                filename=stored_filename,
            )
        current_filename, _, current_checksum = current
        if not stored_checksum or not stored_filename:
            if allow_legacy:
                legacy.append((version, current_filename, current_checksum))
                continue
            raise MigrationIntegrityError(
                "applied migration %s (%s) has no stored checksum; review the current "
                "file and rerun up with --adopt-legacy-checksums to establish its "
                "integrity baseline" % (version, current_filename),
                version=version,
                filename=current_filename,
            )
        if stored_filename != current_filename or stored_checksum != current_checksum:
            raise MigrationIntegrityError(
                "applied migration %s does not match the checked-out package "
                "(database file=%r, package file=%r, stored checksum=%s, current checksum=%s)"
                % (version, stored_filename, current_filename, stored_checksum, current_checksum),
                version=version,
                filename=current_filename,
            )
    return legacy


def _emit_status(config, migrations, applied, metadata, output_format):
    current_map = _current_migration_map(migrations)
    integrity_errors = []
    legacy_versions = []
    for version, fname, filepath in migrations:
        checksum = _sha256_file(filepath)
        status = "applied" if version in applied else "pending"
        integrity = "not_applicable"
        if status == "applied":
            if metadata is None or version not in metadata or not metadata[version][0] or not metadata[version][1]:
                integrity = "legacy_unverified"
                legacy_versions.append(version)
            else:
                stored_filename, stored_checksum = metadata[version]
                if stored_filename != fname or stored_checksum != checksum:
                    integrity = "checksum_mismatch"
                    integrity_errors.append({"version": version, "file": fname})
                else:
                    integrity = "verified"
        _emit_event(
            output_format,
            "migration_status",
            environment=config["APP_ENV"],
            database=config["PG_DB"],
            version=version,
            file=fname,
            status=status,
            checksum=checksum,
            integrity=integrity,
        )

    missing_from_package = sorted(version for version in applied if version != 0 and version not in current_map)
    for version in missing_from_package:
        integrity_errors.append({"version": version, "file": None})
        _emit_event(
            output_format,
            "migration_integrity_error",
            environment=config["APP_ENV"],
            database=config["PG_DB"],
            version=version,
            file=None,
            message="applied migration version is absent from the checked-out package",
        )

    pending_count = sum(1 for version, _, _ in migrations if version not in applied)
    applied_count = sum(1 for version, _, _ in migrations if version in applied)
    summary = {
        "event": "migration_summary",
        "environment": config["APP_ENV"],
        "database": config["PG_DB"],
        "total": len(migrations),
        "applied": applied_count,
        "pending": pending_count,
        "legacy_unverified_versions": legacy_versions,
        "requires_legacy_checksum_adoption": bool(legacy_versions),
        "integrity_errors": integrity_errors,
        "has_pending": pending_count > 0,
        "safe_to_apply": not integrity_errors and not legacy_versions,
        "ok": not integrity_errors,
    }
    if output_format == OUTPUT_JSONL:
        print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    else:
        print("Migration status:")
        print("  %s applied; %s pending; %s total" % (applied_count, pending_count, len(migrations)))
        if legacy_versions:
            print("  WARNING: legacy checksums unavailable for versions: %s" % ", ".join(map(str, legacy_versions)))
    if integrity_errors:
        raise MigrationIntegrityError(
            "migration ledger integrity check failed; inspect the preceding status events"
        )
    return summary


def cmd_status(config, allow_prd=False, output_format=OUTPUT_TEXT):
    """Print migration status without creating or modifying database objects."""
    conn, cur = _open_validated_connection(config, allow_prd=allow_prd)
    try:
        cur.execute("SELECT to_regclass('schema_migrations')")
        row = cur.fetchone()
        migrations = _list_migration_files()
        if not row or row[0] is None:
            applied = set()
            metadata = {}
        else:
            applied = _applied_versions(cur)
            columns = _ledger_metadata_columns(cur)
            if {"filename", "checksum"}.issubset(columns):
                metadata = _read_extended_ledger(cur)
            elif columns:
                raise MigrationIntegrityError(
                    "schema_migrations has an incomplete checksum metadata upgrade"
                )
            else:
                metadata = None
        return _emit_status(config, migrations, applied, metadata, output_format)
    finally:
        cur.close()
        conn.close()


def _prepare_extended_ledger(cur, conn):
    _ensure_schema_migrations(cur)
    conn.commit()


def cmd_up(config, allow_prd=False, output_format=OUTPUT_TEXT, adopt_legacy_checksums=False):
    """Apply pending migrations with per-file result events and integrity checks."""
    conn, cur = _open_validated_connection(config, allow_prd=allow_prd)
    try:
        _prepare_extended_ledger(cur, conn)
        migrations = _list_migration_files()
        current_map = _current_migration_map(migrations)
        ledger = _read_extended_ledger(cur)
        try:
            legacy = _validate_extended_ledger(
                ledger, current_map, allow_legacy=adopt_legacy_checksums
            )
        except MigrationIntegrityError as exc:
            _emit_event(
                output_format,
                "migration_integrity_error",
                environment=config["APP_ENV"],
                database=config["PG_DB"],
                version=exc.version,
                file=exc.filename,
                message=str(exc),
            )
            _emit_event(
                output_format,
                "migration_summary",
                environment=config["APP_ENV"],
                database=config["PG_DB"],
                ok=False,
                message="Migration integrity validation failed; no migrations were run.",
            )
            raise
        if legacy:
            for version, filename, checksum in legacy:
                cur.execute(
                    "UPDATE schema_migrations SET filename = %s, checksum = %s "
                    "WHERE version = %s AND (filename IS NULL OR checksum IS NULL)",
                    (filename, checksum, version),
                )
                _emit_event(
                    output_format,
                    "migration_legacy_checksum_adopted",
                    version=version,
                    file=filename,
                    checksum=checksum,
                    explicit_operator_adoption=True,
                )
            conn.commit()
            ledger = _read_extended_ledger(cur)

        applied = set(ledger)
        pending = [item for item in migrations if item[0] not in applied]
        if not pending:
            _emit_event(
                output_format,
                "migration_summary",
                environment=config["APP_ENV"],
                database=config["PG_DB"],
                total=len(migrations),
                applied=len([version for version, _, _ in migrations if version in applied]),
                pending=0,
                ok=True,
                message="No pending migrations.",
            )
            return {"ok": True, "pending": 0}

        succeeded = []
        for version, filename, filepath in pending:
            checksum = current_map[version][2]
            try:
                _emit_event(
                    output_format,
                    "migration_started",
                    version=version,
                    file=filename,
                    checksum=checksum,
                )
                up_sql, _ = _parse_sections(filepath)
                if not up_sql:
                    raise RuntimeError("missing or empty -- migrate:up section")
                cur.execute(up_sql)
                cur.execute(
                    "INSERT INTO schema_migrations (version, filename, checksum) "
                    "VALUES (%s, %s, %s)",
                    (version, filename, checksum),
                )
                conn.commit()
                succeeded.append(version)
                _emit_event(
                    output_format,
                    "migration_succeeded",
                    version=version,
                    file=filename,
                    checksum=checksum,
                )
            except Exception as exc:
                rollback_error = None
                try:
                    conn.rollback()
                except Exception as rollback_exc:  # preserve both failures
                    rollback_error = str(rollback_exc)
                event_fields = {
                    "version": version,
                    "file": filename,
                    "checksum": checksum,
                    "exception_type": type(exc).__name__,
                    "sqlstate": getattr(exc, "pgcode", None),
                    "message": str(exc).strip(),
                    "rollback_error": rollback_error,
                }
                _emit_event(output_format, "migration_failed", **event_fields)
                _emit_event(
                    output_format,
                    "migration_summary",
                    environment=config["APP_ENV"],
                    database=config["PG_DB"],
                    total=len(migrations),
                    succeeded_versions=succeeded,
                    failed_version=version,
                    failed_file=filename,
                    pending_after_failure=len(pending) - len(succeeded),
                    ok=False,
                    message="Migration run failed at %s." % filename,
                )
                raise MigrationExecutionError(
                    version, filename, checksum, exc, rollback_error=rollback_error
                ) from exc

        _emit_event(
            output_format,
            "migration_summary",
            environment=config["APP_ENV"],
            database=config["PG_DB"],
            total=len(migrations),
            succeeded_versions=succeeded,
            pending=0,
            ok=True,
            message="All pending migrations applied successfully.",
        )
        return {"ok": True, "succeeded_versions": succeeded}
    finally:
        cur.close()
        conn.close()


def cmd_down(config, allow_prd=False, output_format=OUTPUT_TEXT):
    """Roll back the latest applied migration that has a down section."""
    conn, cur = _open_validated_connection(config, allow_prd=allow_prd)
    try:
        _prepare_extended_ledger(cur, conn)
        migrations = _list_migration_files()
        current_map = _current_migration_map(migrations)
        ledger = _read_extended_ledger(cur)
        _validate_extended_ledger(ledger, current_map, allow_legacy=False)
        applied_map = {
            version: (filename, filepath)
            for version, filename, filepath in migrations
            if version in ledger
        }
        if not applied_map:
            _emit_event(output_format, "migration_summary", ok=True, pending=0, message="No migrations to roll back.")
            return
        version = max(applied_map)
        filename, filepath = applied_map[version]
        _, down_sql = _parse_sections(filepath)
        if not down_sql:
            raise MigrationIntegrityError(
                "migration %s (%s) has no -- migrate:down section; no rollback was performed"
                % (version, filename),
                version=version,
                filename=filename,
            )
        try:
            cur.execute(down_sql)
            cur.execute("DELETE FROM schema_migrations WHERE version = %s", (version,))
            conn.commit()
        except Exception as exc:
            conn.rollback()
            _emit_event(
                output_format,
                "migration_rollback_failed",
                version=version,
                file=filename,
                checksum=current_map[version][2],
                sqlstate=getattr(exc, "pgcode", None),
                message=str(exc).strip(),
            )
            raise MigrationExecutionError(
                version, filename, current_map[version][2], exc
            ) from exc
        _emit_event(
            output_format,
            "migration_rolled_back",
            version=version,
            file=filename,
            checksum=current_map[version][2],
        )
    finally:
        cur.close()
        conn.close()


def cmd_baseline(config, allow_prd=False, output_format=OUTPUT_TEXT):
    """Record the version-zero baseline after validating the database target."""
    conn, cur = _open_validated_connection(config, allow_prd=allow_prd)
    try:
        _prepare_extended_ledger(cur, conn)
        cur.execute("SELECT 1 FROM schema_migrations WHERE version = 0")
        if cur.fetchone():
            _emit_event(output_format, "migration_summary", ok=True, message="Baseline already recorded.")
            return
        cur.execute("INSERT INTO schema_migrations (version) VALUES (0)")
        conn.commit()
        _emit_event(output_format, "migration_summary", ok=True, message="Baseline recorded: version 0.")
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
    parser.add_argument(
        "--format",
        choices=(OUTPUT_TEXT, OUTPUT_JSONL),
        default=OUTPUT_TEXT,
        dest="output_format",
        help="output format (jsonl emits one machine-readable event per line)",
    )
    parser.add_argument(
        "--adopt-legacy-checksums",
        action="store_true",
        help=(
            "explicitly record current file hashes for already-applied legacy rows "
            "that predate checksum tracking; review those files first"
        ),
    )
    return parser


def main(argv=None):
    args = _build_parser().parse_args(argv)
    try:
        config = _load_env_file(args.env_file)
        if args.command != "up" and args.adopt_legacy_checksums:
            raise RuntimeError("--adopt-legacy-checksums is valid only with the up command.")
        commands = {
            "status": cmd_status,
            "up": cmd_up,
            "down": cmd_down,
            "baseline": cmd_baseline,
        }
        if args.command == "up":
            commands[args.command](
                config,
                allow_prd=args.allow_prd,
                output_format=args.output_format,
                adopt_legacy_checksums=args.adopt_legacy_checksums,
            )
        else:
            commands[args.command](
                config,
                allow_prd=args.allow_prd,
                output_format=args.output_format,
            )
    except (OSError, RuntimeError, psycopg2.Error) as exc:
        print("ERROR: %s" % exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
