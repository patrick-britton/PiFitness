#!/usr/bin/env python3
"""Replace TEST directly from the latest validated PRD nightly backup.

This is a destructive rebuild, not a rollback transaction: TEST is already
known to be unsuitable when this command is invoked. The PRD archive is
validated before TEST is dropped. The helper then recreates the configured
TEST database, restores the archive, provisions TEST ownership/permissions,
sets the TEST-only marker, and verifies the result.

A mode-0600 journal distinguishes an incomplete restore from a completed,
PRD-derived TEST baseline. Completed baselines are deliberately retained if a
later bootstrap step or migration fails; they are not replaced with the old
TEST database and do not require another 1.2-GB restore on retry.

This helper never creates a PRD dump, changes PRD, runs app migrations, or
starts/stops application services.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

if __package__:
    from . import compare_schema, inspect_test_db, migrate as migration_runner
else:
    import compare_schema
    import inspect_test_db
    import migrate as migration_runner

DEFAULT_BACKUP_DIR = "~/Documents/DB_Backups"
DEFAULT_BACKUP_GLOB = "pifitness_prd_*.dump"
DEFAULT_MAX_BACKUP_AGE_HOURS = 24
DB_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
ARCHIVE_DB_RE = re.compile(r"^;\s*dbname:\s*(\S+)\s*$", re.MULTILINE | re.IGNORECASE)
BACKUP_NAME_TIME_RE = re.compile(r"_(\d{8}_\d{6})\.dump$", re.IGNORECASE)


class RecoveryError(RuntimeError):
    """A TEST database replacement could not be completed safely."""


def _json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        os.chmod(path.parent, 0o700)
    except OSError:
        pass
    tmp = path.with_name(path.name + ".tmp.%d" % os.getpid())
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        os.chmod(path, 0o600)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _json_read(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise RecoveryError("Cannot read recovery journal %s: %s" % (path, exc)) from exc
    if not isinstance(payload, dict):
        raise RecoveryError("Recovery journal must contain a JSON object: %s" % path)
    return payload


def _qident(value: str) -> str:
    if not DB_NAME_RE.fullmatch(value):
        raise RecoveryError("Unsafe PostgreSQL identifier %r." % value)
    return '"%s"' % value.replace('"', '""')


def _qliteral(value: str) -> str:
    if "\x00" in value:
        raise RecoveryError("NUL byte is not permitted in a SQL string literal.")
    return "'%s'" % value.replace("'", "''")


def _safe_run(
    args: list[str], *, env: dict[str, str] | None = None,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> subprocess.CompletedProcess:
    try:
        result = runner(
            args, check=False, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=env,
        )
    except OSError as exc:
        raise RecoveryError("Could not execute %s: %s" % (args[0], exc)) from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RecoveryError(
            "%s failed with exit code %s%s"
            % (args[0], result.returncode, ": " + detail if detail else ".")
        )
    return result


def _backup_time(path: Path) -> dt.datetime:
    """Use the timestamp in the established backup filename, else file mtime."""
    match = BACKUP_NAME_TIME_RE.search(path.name)
    if match:
        try:
            # The existing nightly job builds this timestamp with `date`, so
            # it is in the Pi's local timezone, not UTC. Interpret it using the
            # host timezone before comparing it with the current UTC clock.
            local_timestamp = dt.datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
            return local_timestamp.astimezone().astimezone(dt.timezone.utc)
        except ValueError:
            pass
    return dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _latest_backup(
    backup_dir: str | Path,
    backup_glob: str,
    expected_prd_db: str,
    *,
    max_age_hours: float = DEFAULT_MAX_BACKUP_AGE_HOURS,
    now: dt.datetime | None = None,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> tuple[Path, str, str]:
    directory = Path(os.path.abspath(os.path.expanduser(str(backup_dir))))
    if directory.is_symlink() or not directory.is_dir():
        raise RecoveryError(
            "PRD backup directory is missing, not a directory, or a symlink: %s" % directory
        )
    if not backup_glob or "/" in backup_glob or "\\" in backup_glob:
        raise RecoveryError("Backup glob must be a non-empty filename pattern, not a path.")
    if max_age_hours <= 0:
        raise RecoveryError("Maximum backup age must be a positive number of hours.")

    matches = [p for p in directory.glob(backup_glob) if not p.is_symlink() and p.is_file()]
    if not matches:
        raise RecoveryError(
            "No PRD backup matched %r in %s. No database has been changed."
            % (backup_glob, directory)
        )
    matches.sort(key=lambda p: (_backup_time(p), p.name), reverse=True)
    selected = matches[0]
    stat = selected.stat()
    if stat.st_size <= 0:
        raise RecoveryError("Newest matching PRD backup is empty: %s" % selected)

    now_utc = now or dt.datetime.now(dt.timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=dt.timezone.utc)
    backup_time = _backup_time(selected)
    age = now_utc.astimezone(dt.timezone.utc) - backup_time
    if age.total_seconds() < -300:
        raise RecoveryError(
            "Newest PRD backup timestamp is more than five minutes in the future: %s (%s)"
            % (selected, backup_time.isoformat())
        )
    if age.total_seconds() > max_age_hours * 3600:
        raise RecoveryError(
            "Newest PRD backup is %.1f hours old; the maximum permitted age is %.1f hours: %s"
            % (age.total_seconds() / 3600, max_age_hours, selected)
        )

    restore_bin = shutil.which("pg_restore")
    if not restore_bin:
        raise RecoveryError("pg_restore is not installed; cannot validate the PRD backup archive.")
    listed = _safe_run([restore_bin, "--list", str(selected)], runner=runner)
    archive_listing = listed.stdout or ""
    db_match = ARCHIVE_DB_RE.search(archive_listing)
    if db_match is None:
        raise RecoveryError(
            "Newest matching backup does not identify its source database in the pg_restore archive header: %s"
            % selected
        )
    archive_db = db_match.group(1)
    if archive_db != expected_prd_db:
        raise RecoveryError(
            "Newest matching backup is for database %r, not configured PRD database %r: %s"
            % (archive_db, expected_prd_db, selected)
        )
    if not re.search(r"^;\s*Format:\s*CUSTOM\s*$", archive_listing, re.MULTILINE | re.IGNORECASE):
        raise RecoveryError("Expected a PostgreSQL custom-format backup (.dump): %s" % selected)
    return selected, backup_time.isoformat(), _sha256_file(selected)


def _pgpass_field(value: str) -> str:
    return value.replace("\\", "\\\\").replace(":", "\\:")


def _make_pgpass(temp_dir: Path, config: dict[str, str]) -> Path:
    path = temp_dir / "pgpass"
    fields = (config["PG_HOST"], config["PG_PORT"], "*", config["PG_USER"], config["PG_PASSWORD"])
    if any("\n" in value or "\r" in value for value in fields):
        raise RecoveryError("PostgreSQL connection settings contain a newline; cannot create a safe pgpass file.")
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(":".join(_pgpass_field(value) for value in fields) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(path, 0o600)
    return path


def _connect(config: dict[str, str], dbname: str):
    params = migration_runner._conn_params(config)
    params["dbname"] = dbname
    return migration_runner.psycopg2.connect(**params)


def _admin_connection(config: dict[str, str]):
    conn = _connect(config, "postgres")
    conn.autocommit = True
    return conn


def _server_identity(conn, expected_port: int) -> dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT current_database(), inet_server_addr()::text, inet_server_port()")
        row = cur.fetchone()
    finally:
        cur.close()
    if not row:
        raise RecoveryError("Could not establish PostgreSQL server identity.")
    database, address, port = row
    if database != "postgres":
        raise RecoveryError("Database administration connection landed in %r, not postgres." % database)
    if address is None or port is None:
        raise RecoveryError("PostgreSQL did not report a TCP server address and port.")
    try:
        normalized = migration_runner._normalize_server_address(address)
    except ValueError as exc:
        raise RecoveryError(str(exc)) from exc
    if not normalized.is_loopback:
        raise RecoveryError("Database recovery is restricted to loopback; server is %s." % normalized)
    if int(port) != int(expected_port):
        raise RecoveryError("Database administration connection used port %s, expected %s." % (port, expected_port))
    return {"server_address": str(normalized), "server_port": int(port)}


def _test_role_login_preflight(
    test_config: dict[str, str], admin_conn, expected_server: dict[str, Any],
) -> dict[str, Any]:
    """Validate TEST credentials before dropping the existing database when possible.

    PostgreSQL normally grants CONNECT on the maintenance ``postgres`` database
    to PUBLIC. If this cluster has revoked that privilege, do not change global
    role/database grants merely for the probe; the TEST credentials are still
    verified against the restored TEST database before a baseline is accepted.
    """
    test_role = test_config["PG_USER"]
    admin_cur = admin_conn.cursor()
    try:
        admin_cur.execute(
            "SELECT has_database_privilege(%s, 'postgres', 'CONNECT')", (test_role,)
        )
        row = admin_cur.fetchone()
    finally:
        admin_cur.close()
    if not row or not row[0]:
        return {
            "checked": False,
            "reason": "TEST role lacks CONNECT on the maintenance postgres database; "
                      "the configured credentials will be verified against TEST after restore.",
        }

    conn = _connect(test_config, "postgres")
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT current_database(), current_user, inet_server_addr()::text, inet_server_port()"
            )
            row = cur.fetchone()
        finally:
            cur.close()
        if not row:
            raise RecoveryError("TEST-role credential preflight returned no server identity.")
        database, user, address, port = row
        if database != "postgres" or user != test_role:
            raise RecoveryError(
                "TEST credential preflight connected as database/user %r/%r; expected postgres/%r."
                % (database, user, test_role)
            )
        if address is None or port is None:
            raise RecoveryError("TEST-role credential preflight returned an incomplete server endpoint.")
        normalized = migration_runner._normalize_server_address(address)
        if not normalized.is_loopback:
            raise RecoveryError("TEST-role credential preflight connected to non-loopback server %s." % normalized)
        if str(normalized) != expected_server["server_address"] or int(port) != int(expected_server["server_port"]):
            raise RecoveryError("TEST-role credential preflight reached a different PostgreSQL endpoint than PRD.")
        return {"checked": True, "database": "postgres", "user": user}
    finally:
        conn.close()


def _catalog_db(cur, dbname: str) -> dict[str, Any] | None:
    cur.execute(
        "SELECT oid, datname, datallowconn, datistemplate, pg_encoding_to_char(encoding), "
        "datcollate, datctype, pg_get_userbyid(datdba) "
        "FROM pg_database WHERE datname = %s", (dbname,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "oid": int(row[0]), "name": row[1], "allow_connections": bool(row[2]),
        "is_template": bool(row[3]), "encoding": row[4], "collate": row[5],
        "ctype": row[6], "owner": row[7],
    }


def _catalog_role_is_superuser(cur, role: str) -> bool:
    cur.execute("SELECT rolsuper FROM pg_roles WHERE rolname = %s", (role,))
    row = cur.fetchone()
    return bool(row and row[0])


def _role_exists(cur, role: str) -> bool:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    return cur.fetchone() is not None


def _role_attributes(cur, role: str) -> dict[str, bool] | None:
    """Read the TEST role's login/superuser attributes before any DROP."""
    cur.execute(
        "SELECT rolcanlogin, rolsuper FROM pg_roles WHERE rolname = %s", (role,)
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {"can_login": bool(row[0]), "is_superuser": bool(row[1])}


def _read_extension_specs(conn) -> list[dict[str, str]]:
    """Read extension names, exact versions, and installation schemas from PRD."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT e.extname, e.extversion, n.nspname "
            "FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace "
            "ORDER BY e.extname"
        )
        return [
            {"name": str(row[0]), "version": str(row[1]), "schema": str(row[2])}
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def _trusted_qident(value: str) -> str:
    """Quote an identifier read from trusted PostgreSQL catalog metadata."""
    if not value or "\x00" in value:
        raise RecoveryError("Invalid identifier in PostgreSQL catalog metadata.")
    return '"%s"' % value.replace('"', '""')


def _load_extension_specs(prd_config: dict[str, str], prd_db: str) -> list[dict[str, str]]:
    """Preflight every PRD extension version before TEST is dropped."""
    conn = _connect(prd_config, prd_db)
    try:
        specs = _read_extension_specs(conn)
        cur = conn.cursor()
        try:
            for extension in specs:
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_available_extension_versions "
                    "WHERE name = %s AND version = %s)",
                    (extension["name"], extension["version"]),
                )
                row = cur.fetchone()
                if not row or not row[0]:
                    raise RecoveryError(
                        "PRD uses extension %s version %s, but that exact version is not "
                        "available on this PostgreSQL server. TEST was not changed."
                        % (extension["name"], extension["version"])
                    )
        finally:
            cur.close()
        return specs
    finally:
        conn.close()


def _set_marker(admin_conn, dbname: str) -> None:
    cur = admin_conn.cursor()
    try:
        cur.execute("ALTER DATABASE %s SET pifitness.env = 'test'" % _qident(dbname))
    finally:
        cur.close()


def _create_test_database(admin_conn, *, test_db: str, test_role: str, prd_db: str) -> None:
    cur = admin_conn.cursor()
    try:
        source = _catalog_db(cur, prd_db)
        if source is None:
            raise RecoveryError("Configured PRD database disappeared during TEST recovery.")
        cur.execute(
            "CREATE DATABASE %s WITH OWNER = %s TEMPLATE = template0 ENCODING = %s "
            "LC_COLLATE = %s LC_CTYPE = %s"
            % (
                _qident(test_db), _qident(test_role), _qliteral(source["encoding"]),
                _qliteral(source["collate"]), _qliteral(source["ctype"]),
            )
        )
    finally:
        cur.close()


def _timescaledb_version(extension_specs: list[dict[str, str]]) -> str | None:
    return next((item["version"] for item in extension_specs if item["name"] == "timescaledb"), None)


def _install_timescaledb(
    admin_config: dict[str, str], test_db: str,
    extension_specs: list[dict[str, str]],
) -> None:
    """Install TimescaleDB before invoking its pre-restore hook.

    Other extensions are created by pg_restore as the PRD administrator. Only
    TimescaleDB must exist before pg_restore starts because its pre-restore
    function is called first.
    """
    extension = next((item for item in extension_specs if item["name"] == "timescaledb"), None)
    if extension is None:
        return
    if extension["schema"] not in {"public", "pg_catalog"}:
        raise RecoveryError(
            "TimescaleDB is installed in schema %r; automatic restore currently requires "
            "its schema to be public or pg_catalog. TEST has not been changed." % extension["schema"]
        )
    conn = _connect(admin_config, test_db)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(
            "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s VERSION %s"
            % (
                _trusted_qident(extension["name"]),
                _trusted_qident(extension["schema"]),
                _qliteral(extension["version"]),
            )
        )
    finally:
        cur.close()
        conn.close()


def _restore_archive(
    backup: Path,
    test_db: str,
    admin_config: dict[str, str],
    *,
    temp_dir: Path,
    timescaledb_version: str | None,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> None:
    restore_bin = shutil.which("pg_restore")
    if not restore_bin:
        raise RecoveryError("pg_restore is not installed.")
    pgpass = _make_pgpass(temp_dir, admin_config)
    env = os.environ.copy()
    env.pop("PGPASSWORD", None)
    env["PGPASSFILE"] = str(pgpass)
    env["PGSSLMODE"] = admin_config.get("PGSSLMODE", "disable")

    # Extensions were installed as the PRD administrator before the restore.
    # The existing test-up.sh fallback uses these TimescaleDB hooks around
    # pg_restore. Restore ordinary database objects as the PRD administrator,
    # then transfer only database-local objects to the TEST application role.
    # Avoid REASSIGN OWNED because it can also alter cluster-shared ownership.
    if timescaledb_version:
        conn = _connect(admin_config, test_db)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute("SELECT timescaledb_pre_restore()")
        finally:
            cur.close()
            conn.close()

    args = [
        restore_bin, "--exit-on-error", "--no-owner", "--no-acl",
        "--host", admin_config["PG_HOST"], "--port", admin_config["PG_PORT"],
        "--username", admin_config["PG_USER"], "--dbname", test_db, str(backup),
    ]
    _safe_run(args, env=env, runner=runner)

    if timescaledb_version:
        conn = _connect(admin_config, test_db)
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute("SELECT timescaledb_post_restore()")
        finally:
            cur.close()
            conn.close()


def _provision_ownership(
    admin_config: dict[str, str], test_db: str, test_role: str,
    extension_specs: list[dict[str, str]],
) -> None:
    """Transfer database-local application objects, never cluster-shared objects.

    pg_restore runs as the PRD administrator so extension-owned internal data
    (notably TimescaleDB catalog data) can be restored. Ownership is then
    transferred object-by-object while excluding extension members. Never use
    REASSIGN OWNED here: PostgreSQL also reassigns shared database/tablespace
    ownership, which could change PRD's owner.
    """
    conn = _connect(admin_config, test_db)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        source_role = _qliteral(admin_config["PG_USER"])
        target_role = _qliteral(test_role)
        target_ident = _qident(test_role)
        cur.execute("""
DO $pifitness_owner_transfer$
DECLARE
    source_owner oid := (SELECT oid FROM pg_roles WHERE rolname = %s);
    target_owner name := %s;
    obj record;
BEGIN
    IF source_owner IS NULL THEN
        RAISE EXCEPTION 'Restore owner role disappeared before ownership transfer';
    END IF;

    -- Application schemas. Extension-owned schemas are left under the
    -- extension installer; their usage is granted below.
    FOR obj IN
        SELECT n.nspname
        FROM pg_namespace n
        WHERE n.nspowner = source_owner
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%%'
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.classid = 'pg_namespace'::regclass
                AND d.objid = n.oid AND d.deptype = 'e'
          )
    LOOP
        EXECUTE format('ALTER SCHEMA %%I OWNER TO %%I', obj.nspname, target_owner);
    END LOOP;

    -- Tables, partitioned tables, views, materialized views, foreign tables,
    -- and sequences. Exclude extension members so Timescale internals remain
    -- owned by their extension administrator.
    FOR obj IN
        SELECT n.nspname, c.relname, c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relowner = source_owner
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%%'
          AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.classid = 'pg_class'::regclass
                AND d.objid = c.oid AND d.deptype = 'e'
          )
    LOOP
        IF obj.relkind = 'S' THEN
            EXECUTE format('ALTER SEQUENCE %%I.%%I OWNER TO %%I', obj.nspname, obj.relname, target_owner);
        ELSIF obj.relkind = 'v' THEN
            EXECUTE format('ALTER VIEW %%I.%%I OWNER TO %%I', obj.nspname, obj.relname, target_owner);
        ELSIF obj.relkind = 'm' THEN
            EXECUTE format('ALTER MATERIALIZED VIEW %%I.%%I OWNER TO %%I', obj.nspname, obj.relname, target_owner);
        ELSIF obj.relkind = 'f' THEN
            EXECUTE format('ALTER FOREIGN TABLE %%I.%%I OWNER TO %%I', obj.nspname, obj.relname, target_owner);
        ELSE
            EXECUTE format('ALTER TABLE %%I.%%I OWNER TO %%I', obj.nspname, obj.relname, target_owner);
        END IF;
    END LOOP;

    -- User-defined domains and types not tied to a table row type.
    FOR obj IN
        SELECT n.nspname, t.typname, t.typtype
        FROM pg_type t
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typowner = source_owner
          AND t.typrelid = 0
          AND t.typtype IN ('b', 'd', 'e', 'r', 'm')
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%%'
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.classid = 'pg_type'::regclass
                AND d.objid = t.oid AND d.deptype = 'e'
          )
    LOOP
        IF obj.typtype = 'd' THEN
            EXECUTE format('ALTER DOMAIN %%I.%%I OWNER TO %%I', obj.nspname, obj.typname, target_owner);
        ELSE
            EXECUTE format('ALTER TYPE %%I.%%I OWNER TO %%I', obj.nspname, obj.typname, target_owner);
        END IF;
    END LOOP;

    -- Functions, procedures, aggregates, and window functions.
    FOR obj IN
        SELECT n.nspname, p.proname, p.prokind,
               pg_get_function_identity_arguments(p.oid) AS identity_args
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proowner = source_owner
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND n.nspname NOT LIKE 'pg_toast%%'
          AND NOT EXISTS (
              SELECT 1 FROM pg_depend d
              WHERE d.classid = 'pg_proc'::regclass
                AND d.objid = p.oid AND d.deptype = 'e'
          )
    LOOP
        IF obj.prokind = 'a' THEN
            EXECUTE format('ALTER AGGREGATE %%I.%%I(%%s) OWNER TO %%I', obj.nspname, obj.proname, obj.identity_args, target_owner);
        ELSIF obj.prokind = 'p' THEN
            EXECUTE format('ALTER PROCEDURE %%I.%%I(%%s) OWNER TO %%I', obj.nspname, obj.proname, obj.identity_args, target_owner);
        ELSE
            EXECUTE format('ALTER FUNCTION %%I.%%I(%%s) OWNER TO %%I', obj.nspname, obj.proname, obj.identity_args, target_owner);
        END IF;
    END LOOP;

    -- Less common database-local objects that pg_restore may create.
    FOR obj IN
        SELECT n.nspname, c.collname
        FROM pg_collation c JOIN pg_namespace n ON n.oid = c.collnamespace
        WHERE c.collowner = source_owner AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_collation'::regclass AND d.objid = c.oid AND d.deptype = 'e')
    LOOP
        EXECUTE format('ALTER COLLATION %%I.%%I OWNER TO %%I', obj.nspname, obj.collname, target_owner);
    END LOOP;

    FOR obj IN
        SELECT n.nspname, c.conname
        FROM pg_conversion c JOIN pg_namespace n ON n.oid = c.connamespace
        WHERE c.conowner = source_owner AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_conversion'::regclass AND d.objid = c.oid AND d.deptype = 'e')
    LOOP
        EXECUTE format('ALTER CONVERSION %%I.%%I OWNER TO %%I', obj.nspname, obj.conname, target_owner);
    END LOOP;

    FOR obj IN SELECT oid FROM pg_largeobject_metadata WHERE lomowner = source_owner
    LOOP
        EXECUTE format('ALTER LARGE OBJECT %%s OWNER TO %%I', obj.oid, target_owner);
    END LOOP;
END
$pifitness_owner_transfer$;
""" % (source_role, target_role))
        cur.execute("ALTER SCHEMA public OWNER TO %s" % target_ident)
        cur.execute(
            "GRANT CONNECT, TEMPORARY ON DATABASE %s TO %s" % (_qident(test_db), target_ident)
        )
        cur.execute("GRANT USAGE, CREATE ON SCHEMA public TO %s" % target_ident)
        schemas = sorted({item["schema"] for item in extension_specs} - {"public", "pg_catalog", "information_schema"})
        for schema in schemas:
            ident = _trusted_qident(schema)
            cur.execute("GRANT USAGE ON SCHEMA %s TO %s" % (ident, target_ident))
            cur.execute("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %s TO %s" % (ident, target_ident))
            cur.execute("GRANT USAGE ON ALL TYPES IN SCHEMA %s TO %s" % (ident, target_ident))
            cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s" % (ident, target_ident))
            cur.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s" % (ident, target_ident))
    finally:
        cur.close()
        conn.close()


def _set_database_owner(admin_conn, test_db: str, test_role: str) -> None:
    cur = admin_conn.cursor()
    try:
        cur.execute("ALTER DATABASE %s OWNER TO %s" % (_qident(test_db), _qident(test_role)))
    finally:
        cur.close()


def _verify_database(
    config: dict[str, str], test_db: str, *, expected_marker: str,
    expected_port: int, expected_user: str, probe_ddl: bool,
) -> dict[str, Any]:
    conn = _connect(config, test_db)
    try:
        cur = conn.cursor()
        try:
            migration_runner._validate_connection_identity(cur, test_db, {"port": expected_port})
            cur.execute(
                "SELECT current_setting('pifitness.env', true), current_user, "
                "(SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()), "
                "(SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'public')"
            )
            row = cur.fetchone()
            if not row:
                raise RecoveryError("No database identity/permission row returned for %s." % test_db)
            marker, current_user, database_owner, schema_owner = row
            marker = str(marker) if marker else None
            if marker != expected_marker:
                raise RecoveryError("Database %s marker is %r, expected %r." % (test_db, marker, expected_marker))
            if current_user != expected_user:
                raise RecoveryError(
                    "TEST connection authenticated as %r, expected configured TEST role %r."
                    % (current_user, expected_user)
                )
            if database_owner != expected_user or schema_owner != expected_user:
                raise RecoveryError(
                    "TEST ownership is not fully provisioned (database owner=%r, public schema owner=%r, expected=%r)."
                    % (database_owner, schema_owner, expected_user)
                )
            cur.execute(
                "SELECT has_database_privilege(current_user, current_database(), 'CONNECT'), "
                "has_database_privilege(current_user, current_database(), 'TEMPORARY'), "
                "has_schema_privilege(current_user, 'public', 'USAGE'), "
                "has_schema_privilege(current_user, 'public', 'CREATE')"
            )
            privileges = cur.fetchone()
            if not privileges or not all(bool(value) for value in privileges):
                raise RecoveryError(
                    "TEST role %s lacks required database/public-schema privileges: %r"
                    % (current_user, privileges)
                )
            if probe_ddl:
                probe_name = "pifitness_restore_probe_%d" % os.getpid()
                cur.execute("CREATE TABLE public.%s (id BIGINT)" % _qident(probe_name))
                conn.rollback()
            return {
                "database": test_db, "marker": marker, "user": str(current_user),
                "database_owner": str(database_owner), "public_schema_owner": str(schema_owner),
                "privileges": {
                    "connect": bool(privileges[0]), "temporary": bool(privileges[1]),
                    "schema_usage": bool(privileges[2]), "schema_create": bool(privileges[3]),
                },
                "ddl_probe": bool(probe_ddl),
            }
        finally:
            cur.close()
    finally:
        conn.close()


def _terminate_db_sessions(admin_conn, dbname: str) -> None:
    cur = admin_conn.cursor()
    try:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()", (dbname,),
        )
    finally:
        cur.close()


def _drop_database(admin_conn, dbname: str) -> None:
    cur = admin_conn.cursor()
    try:
        info = _catalog_db(cur, dbname)
        if info is None:
            return
        if info["is_template"]:
            raise RecoveryError("Refusing to drop database %s because it is marked as a template." % dbname)
        cur.execute("ALTER DATABASE %s ALLOW_CONNECTIONS false" % _qident(dbname))
        _terminate_db_sessions(admin_conn, dbname)
        cur.execute("DROP DATABASE %s" % _qident(dbname))
    finally:
        cur.close()


def _db_oid(admin_conn, dbname: str) -> int | None:
    cur = admin_conn.cursor()
    try:
        info = _catalog_db(cur, dbname)
        return int(info["oid"]) if info else None
    finally:
        cur.close()


def _validate_restore_config(test_config: dict[str, str], prd_config: dict[str, str]):
    test_db, prd_db, test_params, prd_params = inspect_test_db._check_config_pair(test_config, prd_config)
    test_role = test_params["user"]
    admin_role = prd_params["user"]
    for value, label in ((test_db, "TEST database"), (prd_db, "PRD database"),
                         (test_role, "TEST role"), (admin_role, "PRD role")):
        if not DB_NAME_RE.fullmatch(value):
            raise RecoveryError("Configured %s must be a simple PostgreSQL identifier." % label)
    if test_role == admin_role:
        raise RecoveryError("TEST application role and PRD administration role must be distinct.")
    return test_db, prd_db, test_params, prd_params


def restore_test_database(
    test_config: dict[str, str],
    prd_config: dict[str, str],
    *,
    backup_dir: str,
    backup_glob: str,
    state_file: str,
    schema_diff_file: str | None = None,
    max_backup_age_hours: float = DEFAULT_MAX_BACKUP_AGE_HOURS,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> dict[str, Any]:
    """Drop an unsuitable TEST database and restore a validated PRD backup."""
    test_db, prd_db, test_params, prd_params = _validate_restore_config(test_config, prd_config)

    # Do every filesystem/archive check before the destructive DROP DATABASE.
    backup, backup_time, backup_sha256 = _latest_backup(
        backup_dir, backup_glob, prd_db,
        max_age_hours=max_backup_age_hours, runner=runner,
    )
    state_path = Path(os.path.abspath(os.path.expanduser(state_file)))
    state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(state_path.parent, 0o700)
    state: dict[str, Any] = {
        "event": "test_database_recovery",
        "status": "backup_validated",
        "test_database": test_db,
        "prd_database": prd_db,
        "test_role": test_params["user"],
        "admin_role": prd_params["user"],
        "backup_path": str(backup),
        "backup_timestamp_utc": backup_time,
        "backup_size_bytes": backup.stat().st_size,
        "backup_sha256": backup_sha256,
        "max_backup_age_hours": max_backup_age_hours,
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    _json_write(state_path, state)

    admin_conn = None
    destructive_started = False
    try:
        admin_conn = _admin_connection(prd_config)
        state["server"] = _server_identity(admin_conn, int(prd_params["port"]))
        admin_cur = admin_conn.cursor()
        try:
            if not _catalog_role_is_superuser(admin_cur, prd_params["user"]):
                raise RecoveryError(
                    "Configured PRD database user %s must be a PostgreSQL superuser for safe TEST replacement."
                    % prd_params["user"]
                )
            test_role_attributes = _role_attributes(admin_cur, test_params["user"])
            if test_role_attributes is None:
                raise RecoveryError(
                    "Configured TEST application role %s does not exist in this PostgreSQL cluster."
                    % test_params["user"]
                )
            if not test_role_attributes["can_login"]:
                raise RecoveryError(
                    "Configured TEST application role %s cannot log in. TEST was not changed."
                    % test_params["user"]
                )
            if test_role_attributes["is_superuser"]:
                raise RecoveryError(
                    "Configured TEST application role %s must not be a PostgreSQL superuser. TEST was not changed."
                    % test_params["user"]
                )
            state["test_role_attributes"] = test_role_attributes
            state["test_role_login_preflight"] = _test_role_login_preflight(
                test_config, admin_conn, state["server"]
            )
            prd_info = _catalog_db(admin_cur, prd_db)
            if prd_info is None:
                raise RecoveryError("Configured PRD database %s does not exist." % prd_db)
            state["prd_database_oid"] = prd_info["oid"]
            state["prd_database_owner"] = prd_info["owner"]
            existing = _catalog_db(admin_cur, test_db)
            if existing and existing["is_template"]:
                raise RecoveryError("Existing TEST database is marked as a PostgreSQL template; refusing replacement.")
            state["previous_test_database_oid"] = existing["oid"] if existing else None
            state["previous_test_database_owner"] = existing["owner"] if existing else None
            state["status"] = "preflight_passed"
            _json_write(state_path, state)
        finally:
            admin_cur.close()

        # Extension installations are database-local, but their files and
        # versions are cluster-installed. Verify every exact PRD extension
        # version before dropping TEST, then recreate them under the admin role.
        extension_specs = _load_extension_specs(prd_config, prd_db)
        timescaledb_version = _timescaledb_version(extension_specs)
        state["extensions"] = extension_specs
        state["timescaledb_version"] = timescaledb_version
        state["status"] = "restore_preflight_passed"
        _json_write(state_path, state)

        # The old TEST database is already classified as unusable. Replace it
        # directly to avoid retaining a second 1.2-GB database during restore.
        destructive_started = True
        _drop_database(admin_conn, test_db)
        state["status"] = "old_test_removed"
        _json_write(state_path, state)

        _create_test_database(
            admin_conn, test_db=test_db, test_role=test_params["user"], prd_db=prd_db
        )
        state["status"] = "new_test_created"
        state["new_test_database_oid"] = _db_oid(admin_conn, test_db)
        _json_write(state_path, state)

        with tempfile.TemporaryDirectory(prefix="pifitness-db-restore-", dir=str(state_path.parent)) as temp:
            temp_dir = Path(temp)
            os.chmod(temp_dir, 0o700)
            state["status"] = "restore_started"
            _json_write(state_path, state)
            _install_timescaledb(prd_config, test_db, extension_specs)
            _restore_archive(
                backup, test_db, prd_config, temp_dir=temp_dir,
                timescaledb_version=timescaledb_version, runner=runner,
            )

        state["status"] = "archive_restored"
        _json_write(state_path, state)
        _provision_ownership(prd_config, test_db, test_params["user"], extension_specs)
        _set_database_owner(admin_conn, test_db, test_params["user"])
        _set_marker(admin_conn, test_db)
        state["status"] = "test_identity_provisioned"
        _json_write(state_path, state)

        verified = _verify_database(
            test_config, test_db, expected_marker="test",
            expected_port=int(test_params["port"]), expected_user=test_params["user"],
            probe_ddl=True,
        )
        # Record the live-PRD schema comparison as diagnostic evidence. A
        # nightly archive can legitimately predate a schema change made to live
        # PRD after the dump; that does not invalidate the restored backup as a
        # PRD-derived baseline. The migration ledger is checked on every retry.
        try:
            comparison = compare_schema.compare_databases(
                test_config, prd_config, diff_file=schema_diff_file
            )
            if comparison.get("ok") is not True or comparison.get("status") not in {"identical", "different"}:
                comparison = {
                    "ok": False,
                    "status": "error",
                    "error": comparison.get("error") or "Schema comparator returned an invalid result.",
                }
        except Exception as comparison_exc:
            comparison = {"ok": False, "status": "error", "error": str(comparison_exc)}

        # Confirm the PRD database object itself was not dropped/recreated
        # during TEST recovery. The helper issues no DDL or DML against PRD.
        final_prd_oid = _db_oid(admin_conn, prd_db)
        state["prd_database_identity_unchanged"] = (
            final_prd_oid is not None and final_prd_oid == state["prd_database_oid"]
        )
        if not state["prd_database_identity_unchanged"]:
            raise RecoveryError(
                "PRD database identity changed while TEST recovery was running; "
                "the restored TEST database will not be accepted as a baseline."
            )

        state["verification"] = verified
        state["schema_comparison"] = {
            "ok": comparison.get("ok"),
            "status": comparison.get("status"),
            "statement_count": comparison.get("statement_count"),
            "diff_fingerprint": comparison.get("diff_fingerprint"),
            "error": comparison.get("error"),
        }
        state["new_test_database_oid"] = _db_oid(admin_conn, test_db)
        state["status"] = "baseline_ready"
        state["baseline_ready_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        _json_write(state_path, state)
        return {
            "ok": True,
            "test_database": test_db,
            "prd_database": prd_db,
            "marker": "test",
            "backup_path": str(backup),
            "backup_timestamp_utc": backup_time,
            "backup_sha256": backup_sha256,
            "timescaledb_version": timescaledb_version,
            "previous_test_database_discarded": state["previous_test_database_oid"] is not None,
            "prd_database_identity_unchanged": state["prd_database_identity_unchanged"],
            "database_oid": state["new_test_database_oid"],
            "verification": verified,
            "schema_comparison": state["schema_comparison"],
            "state_file": str(state_path),
            "status": "baseline_ready",
        }
    except Exception as exc:
        state["error"] = str(exc)
        state["failed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        if destructive_started:
            # A partial restore is never considered a valid TEST DB. Remove it
            # now so the next attempt cannot mistake it for a usable baseline.
            try:
                if admin_conn is not None:
                    _drop_database(admin_conn, test_db)
                state["status"] = "restore_failed_test_database_removed"
            except Exception as cleanup_exc:
                state["status"] = "restore_failed_cleanup_required"
                state["cleanup_error"] = str(cleanup_exc)
        else:
            state["status"] = "failed_before_database_change"
        _json_write(state_path, state)
        if state.get("status") == "restore_failed_cleanup_required":
            raise RecoveryError(
                "TEST restore failed: %s. Partial TEST database cleanup also failed: %s. "
                "Inspect PostgreSQL database names before retrying. Journal: %s"
                % (exc, state.get("cleanup_error"), state_path)
            ) from exc
        if not destructive_started:
            raise RecoveryError(
                "TEST recovery preflight failed before the existing TEST database was changed: %s. "
                "The existing TEST database remains untouched. Journal: %s"
                % (exc, state_path)
            ) from exc
        raise RecoveryError(
            "TEST replacement failed: %s. The partial replacement was removed; the previously "
            "rejected TEST database was not retained. The next attempt can retry from the "
            "validated PRD backup. Journal: %s"
            % (exc, state_path)
        ) from exc
    finally:
        if admin_conn is not None:
            admin_conn.close()


def recover_pending_states(
    test_config: dict[str, str], prd_config: dict[str, str], state_dir: str,
) -> dict[str, Any]:
    """Clean partial restores; never revert a completed PRD-derived baseline."""
    test_db, prd_db, test_params, prd_params = _validate_restore_config(test_config, prd_config)
    directory = Path(os.path.abspath(os.path.expanduser(state_dir)))
    if not directory.exists():
        return {"ok": True, "cleaned": [], "baseline_ready": []}

    admin_conn = _admin_connection(prd_config)
    cleaned: list[str] = []
    baselines: list[str] = []
    ignored: list[str] = []
    try:
        _server_identity(admin_conn, int(prd_params["port"]))
        current_oid = _db_oid(admin_conn, test_db)
        for path in sorted(directory.glob("test-db-recovery-*.json")):
            state = _json_read(path)
            if state.get("test_database") != test_db or state.get("prd_database") != prd_db:
                continue
            status = state.get("status")
            if status == "baseline_ready":
                if current_oid is not None and current_oid == state.get("new_test_database_oid"):
                    try:
                        verified = _verify_database(
                            test_config, test_db, expected_marker="test",
                            expected_port=int(test_params["port"]),
                            expected_user=test_params["user"], probe_ddl=False,
                        )
                    except Exception as exc:
                        state["status"] = "baseline_no_longer_valid"
                        state["baseline_validation_error"] = str(exc)
                        _json_write(path, state)
                    else:
                        state["last_validated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
                        _json_write(path, state)
                        baselines.append(str(path))
                continue
            if status == "restore_failed_cleanup_required":
                # Clean up only the partial database created by this journal.
                # A subsequent retry may already have installed a newer good
                # baseline; never let an older journal drop that database.
                journal_oid = state.get("new_test_database_oid")
                if current_oid is not None and journal_oid is not None and current_oid == journal_oid:
                    _drop_database(admin_conn, test_db)
                    current_oid = None
                state["status"] = "restore_failed_test_database_removed"
                state["recovered_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
                _json_write(path, state)
                cleaned.append(str(path))
            elif status in {
                "old_test_removed", "new_test_created", "restore_started",
                "archive_restored", "test_identity_provisioned",
            }:
                # An incomplete journal may remove only the replacement DB
                # whose OID it recorded. An old journal with no matching OID
                # must not delete a database created by a later successful run.
                journal_oid = state.get("new_test_database_oid")
                if current_oid is not None and journal_oid is not None and current_oid == journal_oid:
                    _drop_database(admin_conn, test_db)
                    current_oid = None
                state["status"] = "interrupted_restore_cleaned"
                state["recovered_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
                _json_write(path, state)
                cleaned.append(str(path))
            elif status in {"backup_validated", "preflight_passed", "restore_preflight_passed",
                            "failed_before_database_change", "restore_failed_test_database_removed",
                            "interrupted_restore_cleaned", "baseline_no_longer_valid"}:
                ignored.append(str(path))
        return {"ok": True, "cleaned": cleaned, "baseline_ready": baselines, "ignored": ignored}
    finally:
        admin_conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-env-file", required=True)
    parser.add_argument("--prd-env-file", required=True)
    sub = parser.add_subparsers(dest="command", required=True)

    restore = sub.add_parser("restore", help="replace TEST from the latest PRD nightly backup")
    restore.add_argument("--backup-dir", default=DEFAULT_BACKUP_DIR)
    restore.add_argument("--backup-glob", default=DEFAULT_BACKUP_GLOB)
    restore.add_argument("--max-backup-age-hours", type=float, default=DEFAULT_MAX_BACKUP_AGE_HOURS)
    restore.add_argument("--state-file", required=True)
    restore.add_argument("--diff-file")

    pending = sub.add_parser("recover-pending", help="clean interrupted restores and validate retained baselines")
    pending.add_argument("--state-dir", required=True)

    args = parser.parse_args(argv)
    try:
        test_config = migration_runner._load_env_file(args.test_env_file)
        prd_config = migration_runner._load_env_file(args.prd_env_file)
        if args.command == "restore":
            result = restore_test_database(
                test_config, prd_config,
                backup_dir=args.backup_dir, backup_glob=args.backup_glob,
                state_file=args.state_file, max_backup_age_hours=args.max_backup_age_hours,
                schema_diff_file=args.diff_file,
            )
        else:
            result = recover_pending_states(test_config, prd_config, args.state_dir)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True, separators=(",", ":")))
        print("ERROR: %s" % exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
