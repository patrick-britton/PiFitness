#!/usr/bin/env python3
"""Read-only inspection and decision report for PiFitness TEST vs PRD.

This module performs no DDL and never refreshes, renames, drops, or modifies a
 database. It:
- validates the explicitly supplied TEST and PRD migration configurations;
- validates PRD identity/marker through the migration runner's guarded path;
- checks whether the configured TEST database exists in the same cluster;
- if it exists, connects without applying the TEST marker guard so a missing or
  wrong marker can be reported as a policy observation (not an inspection error);
- verifies TEST's application role, database ownership, and required grants;
- invokes MigraDiff only when identity and permissions are valid;
- passes observations to the pure TEST database policy.

The deployment orchestrator is responsible for replacing TEST from the latest
validated PRD backup when policy requests it. This inspector is strictly
read-only; all PostgreSQL sessions are marked read-only.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

import psycopg2

if __package__:
    from . import compare_schema, _env, test_db_policy
else:
    import compare_schema
    import _env
    import test_db_policy


class TestDatabaseInspectionError(RuntimeError):
    """The TEST/PRD database state could not be established safely."""


def _validate_target(config: dict[str, str], params: dict[str, Any], *, allow_prd: bool) -> None:
    """Validate that config targets a local database with matching APP_ENV marker."""
    app_env = str(config.get("APP_ENV", ""))
    host = str(params.get("host", ""))
    if not _env.is_loopback(_env.normalize_server_address(host)):
        raise TestDatabaseInspectionError(
            "remote host %r; migrations run only against localhost." % host
        )
    if app_env == _env.APP_ENV_PROD and not allow_prd:
        raise TestDatabaseInspectionError(
            "PRD access requires explicit --allow-prd authorization."
        )
    if app_env != _env.APP_ENV_PROD and allow_prd:
        raise TestDatabaseInspectionError(
            "--allow-prd is valid only with APP_ENV=prod."
        )


def _check_config_pair(
    test_config: dict[str, str], prd_config: dict[str, str]
) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    """Validate the intended local TEST/PRD configuration pair."""
    if test_config.get("APP_ENV") != _env.APP_ENV_TEST:
        raise TestDatabaseInspectionError("TEST env file must specify APP_ENV=test.")
    if prd_config.get("APP_ENV") != _env.APP_ENV_PROD:
        raise TestDatabaseInspectionError("PRD env file must specify APP_ENV=prod.")

    test_db = _env.resolve_db_name(test_config)
    prd_db = _env.resolve_db_name(prd_config)
    if test_db == prd_db:
        raise TestDatabaseInspectionError(
            "TEST and PRD env files resolve to the same database %r; refusing inspection."
            % test_db
        )

    test_params = _env.conn_params(test_config)
    prd_params = _env.conn_params(prd_config)
    _validate_target(test_config, test_params, allow_prd=False)
    _validate_target(prd_config, prd_params, allow_prd=True)

    if (
        test_params["host"] != prd_params["host"]
        or test_params["port"] != prd_params["port"]
    ):
        raise TestDatabaseInspectionError(
            "TEST and PRD env files must target the same local host and port."
        )
    return test_db, prd_db, test_params, prd_params


def _open_read_only_connection(
    config: dict[str, str], *, allow_prd: bool
) -> tuple[Any, Any, dict[str, Any]]:
    """Connect to the configured local database in a read-only session."""
    dbname = _env.resolve_db_name(config)
    params = _env.conn_params(config)
    _validate_target(config, params, allow_prd=allow_prd)
    conn = psycopg2.connect(dbname=dbname, **params)
    try:
        # Enforce read-only at the PostgreSQL transaction/session boundary.
        conn.set_session(readonly=True)
        cur = conn.cursor()
    except Exception:
        conn.close()
        raise
    return conn, cur, params


def _read_identity(cur: Any, *, expected_database: str, expected_port: int) -> dict[str, Any]:
    cur.execute(
        "SELECT current_database(), inet_server_addr()::text, "
        "inet_server_port(), current_setting('pifitness.env', true)"
    )
    row = cur.fetchone()
    if not row:
        raise TestDatabaseInspectionError(
            "PostgreSQL returned no database identity row."
        )
    database, server_address, server_port, marker = row
    if database != expected_database:
        raise TestDatabaseInspectionError(
            "Connected to database %r, expected %r." % (database, expected_database)
        )
    if server_address is None or server_port is None:
        raise TestDatabaseInspectionError(
            "PostgreSQL did not report a complete TCP database identity."
        )
    try:
        normalized_address = _env.normalize_server_address(server_address)
    except ValueError as exc:
        raise TestDatabaseInspectionError(str(exc)) from exc
    if not _env.is_loopback(normalized_address):
        raise TestDatabaseInspectionError(
            "PostgreSQL server address %r is not loopback; expected localhost, 127.0.0.1, or ::1."
            % str(normalized_address)
        )
    if int(server_port) != int(expected_port):
        raise TestDatabaseInspectionError(
            "Connected to PostgreSQL port %r, expected %r."
            % (server_port, expected_port)
        )
    return {
        "database": str(database),
        # Store a canonical bare IP address so ::1 and ::1/128 compare equal.
        "server_address": str(normalized_address),
        "server_port": int(server_port),
        "marker": str(marker) if marker else None,
    }


def _read_test_database_catalog(prd_cur: Any, test_db: str) -> dict[str, Any] | None:
    """Read TEST existence from the already validated PRD connection."""
    prd_cur.execute(
        "SELECT datname, datallowconn, datistemplate, pg_get_userbyid(datdba), oid "
        "FROM pg_database WHERE datname = %s",
        (test_db,),
    )
    row = prd_cur.fetchone()
    if row is None:
        return None
    database, allow_connections, is_template, owner, database_oid = row
    if database != test_db:
        raise TestDatabaseInspectionError(
            "PostgreSQL catalog returned unexpected database %r; expected %r."
            % (database, test_db)
        )
    return {
        "database": database,
        "allow_connections": bool(allow_connections),
        "is_template": bool(is_template),
        "owner": str(owner),
        "oid": int(database_oid),
    }


def _find_matching_baseline_journal(
    state_dir: str | Path | None, *, test_db: str, prd_db: str, database_oid: int,
) -> tuple[Path, dict[str, Any]] | None:
    """Find a completed PRD restore journal for this exact current DB OID."""
    if not state_dir:
        return None
    supplied_directory = Path(state_dir).expanduser()
    if supplied_directory.is_symlink():
        raise TestDatabaseInspectionError(
            "TEST recovery journal directory must not be a symlink: %s" % supplied_directory
        )
    directory = supplied_directory.resolve()
    if not directory.is_dir():
        return None
    for path in sorted(directory.glob("test-db-recovery-*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        if path.is_symlink() or not path.is_file():
            continue
        try:
            with path.open(encoding="utf-8") as stream:
                state = json.load(stream)
        except (OSError, json.JSONDecodeError) as exc:
            raise TestDatabaseInspectionError(
                "Could not read TEST recovery provenance journal %s: %s" % (path, exc)
            ) from exc
        if not isinstance(state, dict):
            raise TestDatabaseInspectionError(
                "TEST recovery provenance journal is not a JSON object: %s" % path
            )
        backup_path = state.get("backup_path")
        backup_sha256 = state.get("backup_sha256")
        backup_timestamp = state.get("backup_timestamp_utc")
        checksum_valid = (
            isinstance(backup_sha256, str)
            and len(backup_sha256) == 64
            and all(char in "0123456789abcdef" for char in backup_sha256.lower())
        )
        if (
            state.get("status") == "baseline_ready"
            and state.get("test_database") == test_db
            and state.get("prd_database") == prd_db
            and state.get("new_test_database_oid") == database_oid
            and isinstance(backup_path, str) and bool(backup_path)
            and checksum_valid
            and isinstance(backup_timestamp, str) and bool(backup_timestamp)
        ):
            return path, state
    return None


def _write_temp_env_file(config: dict[str, str]) -> str:
    """Write a temporary dotenv file from a loaded config dict.

    Returns the path. The caller is responsible for removing it.
    """
    lines = [
        f"APP_ENV={config.get('APP_ENV', 'dev')}",
        f"PG_HOST={config.get('PG_HOST', 'localhost')}",
        f"PG_DB={config.get('PG_DB', 'pifitness')}",
        f"PG_USER={config.get('PG_USER', '')}",
        f"PG_PASSWORD={config.get('PG_PASSWORD', '')}",
        f"PG_PORT={config.get('PG_PORT', '5432')}",
        f"PGSSLMODE={config.get('PGSSLMODE', 'disable')}",
    ]
    fd, path = tempfile.mkstemp(prefix="pifitness-inspect-env-", suffix=".env")
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write("\n".join(lines) + "\n")
    os.chmod(path, 0o600)
    return path


def _read_migration_ledger_status(test_config: dict[str, str]) -> dict[str, Any]:
    """Run dbmate's read-only status check silently via the guard wrapper.

    Returns a dict compatible with migrate.py's cmd_status JSONL output:
    ok, safe_to_apply, applied, environment, database.
    """
    guard = os.path.join(os.path.dirname(__file__), "dbmate_guard.sh")
    env_file = _write_temp_env_file(test_config)
    try:
        result = subprocess.run(
            ["bash", guard, "status", "--env-file", env_file],
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        try:
            os.unlink(env_file)
        except OSError:
            pass

    # dbmate status exits 0 when all migrations are applied; non-zero means
    # pending migrations or a dirty ledger.
    all_applied = result.returncode == 0
    applied_count = 0
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("version") or line.startswith("----"):
            continue
        parts = line.split()
        if parts and parts[0].isdigit():
            applied_count += 1

    summary = {
        "ok": all_applied,
        "safe_to_apply": all_applied,
        "applied": applied_count,
        "environment": test_config.get("APP_ENV"),
        "database": test_config.get("PG_DB"),
    }
    if not all_applied:
        summary["status"] = "pending_migrations"
        summary["error"] = (
            result.stderr.strip() or "dbmate status reported pending migrations."
        )
    return summary


def _read_test_permissions(
    prd_config: dict[str, str],
    *,
    test_db: str,
    test_role: str,
    expected_port: int,
    prd_identity: dict[str, Any],
) -> dict[str, Any]:
    """Inspect TEST grants as the PRD administrator without modifying TEST."""
    admin_test_config = dict(prd_config)
    admin_test_config["PG_DB"] = test_db
    conn, cur, params = _open_read_only_connection(admin_test_config, allow_prd=True)
    try:
        identity = _read_identity(cur, expected_database=test_db, expected_port=expected_port)
        _verify_same_server(identity, prd_identity, label="TEST admin permission inspection")
        cur.execute(
            "SELECT pg_get_userbyid(datdba), "
            "(SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'public') "
            "FROM pg_database WHERE datname = current_database()"
        )
        ownership = cur.fetchone()
        if not ownership:
            raise TestDatabaseInspectionError("Could not inspect TEST database/schema ownership.")
        database_owner, schema_owner = ownership
        cur.execute(
            "SELECT has_database_privilege(%s, current_database(), 'CONNECT'), "
            "has_database_privilege(%s, current_database(), 'TEMPORARY'), "
            "has_schema_privilege(%s, 'public', 'USAGE'), "
            "has_schema_privilege(%s, 'public', 'CREATE')",
            (test_role, test_role, test_role, test_role),
        )
        privileges = cur.fetchone()
        if not privileges:
            raise TestDatabaseInspectionError("Could not inspect TEST application-role privileges.")
        owner_ok = database_owner == test_role and schema_owner == test_role
        grants_ok = all(bool(value) for value in privileges)
        return {
            "ok": bool(owner_ok and grants_ok),
            "expected_role": test_role,
            "database_owner": str(database_owner),
            "public_schema_owner": str(schema_owner) if schema_owner is not None else None,
            "privileges": {
                "connect": bool(privileges[0]),
                "temporary": bool(privileges[1]),
                "schema_usage": bool(privileges[2]),
                "schema_create": bool(privileges[3]),
            },
        }
    finally:
        cur.close()
        conn.close()


def _verify_same_server(
    identity: dict[str, Any], prd_identity: dict[str, Any], *, label: str
) -> None:
    try:
        test_address = _env.normalize_server_address(identity["server_address"])
        prd_address = _env.normalize_server_address(prd_identity["server_address"])
    except (KeyError, ValueError) as exc:
        raise TestDatabaseInspectionError(
            "%s/PRD server endpoint could not be normalized: %s" % (label, exc)
        ) from exc
    if test_address != prd_address or identity["server_port"] != prd_identity["server_port"]:
        raise TestDatabaseInspectionError(
            "%s resolved to a different PostgreSQL server endpoint than PRD "
            "(TEST=%s:%s, PRD=%s:%s)."
            % (
                label,
                test_address,
                identity["server_port"],
                prd_address,
                prd_identity["server_port"],
            )
        )


def _redact_error(message: str, configs: tuple[dict[str, str], ...]) -> str:
    result = message
    for config in configs:
        password = str(config.get("PG_PASSWORD") or "")
        if password:
            result = result.replace(password, "<redacted>")
    return result


def inspect_test_database(
    test_config: dict[str, str],
    prd_config: dict[str, str],
    *,
    refresh_from_prd: bool = False,
    compare_fn: Callable[..., dict[str, Any]] | None = None,
    diff_file: str | None = None,
    baseline_state_dir: str | None = None,
) -> dict[str, Any]:
    """Inspect TEST and return a policy decision without changing either DB."""
    compare = compare_fn or compare_schema.compare_databases
    report: dict[str, Any] = {
        "event": "test_database_inspection",
        "ok": False,
        "test": None,
        "prd": None,
        "schema_comparison": None,
        "decision": None,
    }
    configs = (test_config, prd_config)
    stage = "Validate TEST and PRD environment configuration"

    try:
        test_db, prd_db, test_params, _ = _check_config_pair(
            test_config, prd_config
        )
        report["test"] = {"database": test_db, "exists": None, "marker": None}

        stage = "Connect to PRD and verify its database identity and marker"
        # Both sessions are enforced read-only. PRD additionally receives the
        # migration runner's normal unmarked-PRD marker guard.
        prd_conn, prd_cur, prd_params = _open_read_only_connection(
            prd_config, allow_prd=True
        )
        try:
            prd_identity = _read_identity(
                prd_cur, expected_database=prd_db, expected_port=prd_params["port"]
            )
            _env.guard_marker(_env.APP_ENV_PROD, prd_identity["marker"])
            report["prd"] = prd_identity
            stage = "Check whether the TEST database exists in the PRD PostgreSQL cluster"
            catalog = _read_test_database_catalog(prd_cur, test_db)
        finally:
            prd_cur.close()
            prd_conn.close()

        if catalog is None:
            report["test"].update({"exists": False, "marker": None})
            observation = test_db_policy.DatabaseObservation(exists=False)
        else:
            if catalog["is_template"]:
                raise TestDatabaseInspectionError(
                    "Configured TEST database is marked as a PostgreSQL template; refusing inspection."
                )
            if not catalog["allow_connections"]:
                raise TestDatabaseInspectionError(
                    "TEST database exists but disallows connections; marker cannot be verified safely."
                )

            stage = "Connect to TEST as the local database administrator and verify its identity/marker"
            # Inspect as the validated PRD administrator first. That allows
            # policy to detect missing TEST-role CONNECT/USAGE/CREATE grants
            # instead of turning a repairable permission problem into a generic
            # connection failure.
            admin_test_config = dict(prd_config)
            admin_test_config["PG_DB"] = test_db
            test_admin_conn, test_admin_cur, test_admin_params = _open_read_only_connection(
                admin_test_config, allow_prd=True
            )
            try:
                test_identity = _read_identity(
                    test_admin_cur, expected_database=test_db,
                    expected_port=test_admin_params["port"],
                )
                _verify_same_server(test_identity, report["prd"], label="TEST")
            finally:
                test_admin_cur.close()
                test_admin_conn.close()

            report["test"].update(
                {
                    "exists": True,
                    "marker": test_identity["marker"],
                    "server_address": test_identity["server_address"],
                    "server_port": test_identity["server_port"],
                    "database_oid": catalog["oid"],
                }
            )

            if test_identity["marker"] != "test":
                observation = test_db_policy.DatabaseObservation(
                    exists=True, marker=test_identity["marker"]
                )
            else:
                stage = "Inspect TEST database ownership and application-role permissions"
                permissions = _read_test_permissions(
                    prd_config,
                    test_db=test_db,
                    test_role=test_params["user"],
                    expected_port=int(prd_params["port"]),
                    prd_identity=report["prd"],
                )
                report["test"]["permissions"] = permissions
                if not permissions["ok"]:
                    observation = test_db_policy.DatabaseObservation(
                        exists=True, marker="test", permissions_ok=False
                    )
                else:
                    stage = "Connect to TEST using its configured application role"
                    test_conn, test_cur, test_params = _open_read_only_connection(
                        test_config, allow_prd=False
                    )
                    try:
                        test_identity = _read_identity(
                            test_cur, expected_database=test_db, expected_port=test_params["port"]
                        )
                        _verify_same_server(test_identity, report["prd"], label="TEST")
                    finally:
                        test_cur.close()
                        test_conn.close()

                    stage = "Check completed PRD restore provenance and migration ledger"
                    baseline = _find_matching_baseline_journal(
                        baseline_state_dir,
                        test_db=test_db,
                        prd_db=prd_db,
                        database_oid=catalog["oid"],
                    )
                    if baseline is not None:
                        baseline_path, baseline_state = baseline
                        try:
                            migration_status = _read_migration_ledger_status(test_config)
                        except Exception as exc:
                            error = _redact_error(str(exc), configs)
                            report["error"] = error
                            report["failure"] = _failure_diagnostic(exc, stage, error)
                            report["schema_comparison"] = {
                                "ok": False,
                                "status": "error",
                                "error": error,
                            }
                            observation = test_db_policy.DatabaseObservation(
                                exists=True,
                                marker="test",
                                permissions_ok=True,
                                schema_status="error",
                                inspection_error=error,
                            )
                        else:
                            applied_count = int(migration_status.get("applied", 0))
                            report["baseline_provenance"] = {
                                    "journal": str(baseline_path),
                                    "database_oid": catalog["oid"],
                                    "backup_path": baseline_state.get("backup_path"),
                                    "backup_timestamp_utc": baseline_state.get("backup_timestamp_utc"),
                                    "migration_status": migration_status,
                                }
                            report["schema_comparison"] = {
                                "ok": True,
                                "status": "migration_managed" if applied_count > 0 else "restored_baseline",
                                "statement_count": None,
                                "message": (
                                    "TEST matches a completed, verified PRD backup restore. "
                                    "Its migration ledger is safe for the current package; "
                                    "do not re-clone this database on retry."
                                ),
                            }
                            observation = test_db_policy.DatabaseObservation(
                                exists=True,
                                marker="test",
                                permissions_ok=True,
                                baseline_provenance_valid=True,
                            )
                    if baseline is None:
                        stage = "Compare TEST and PRD schemas using MigraDiff"
                        try:
                            comparison = compare(
                                test_config,
                                prd_config,
                                diff_file=diff_file,
                            )
                        except Exception as exc:
                            error = _redact_error(str(exc), configs)
                            report["error"] = error
                            report["failure"] = _failure_diagnostic(exc, stage, error)
                            report["schema_comparison"] = {
                                "ok": False,
                                "status": "error",
                                "error": error,
                            }
                            observation = test_db_policy.DatabaseObservation(
                                exists=True,
                                marker="test",
                                permissions_ok=True,
                                schema_status="error",
                                inspection_error=error,
                            )
                        else:
                            status = comparison.get("status")
                            if status not in {"identical", "different"} or comparison.get("ok") is not True:
                                error = "Schema comparator returned an invalid or unsuccessful report."
                                report["error"] = error
                                report["failure"] = _failure_diagnostic(
                                    compare_schema.SchemaComparisonError(error), stage, error
                                )
                                report["schema_comparison"] = {
                                    "ok": False,
                                    "status": "error",
                                    "error": error,
                                }
                                observation = test_db_policy.DatabaseObservation(
                                    exists=True,
                                    marker="test",
                                    permissions_ok=True,
                                    schema_status="error",
                                    inspection_error=error,
                                )
                            else:
                                report["schema_comparison"] = comparison
                                observation = test_db_policy.DatabaseObservation(
                                    exists=True,
                                    marker="test",
                                    permissions_ok=True,
                                    schema_status=status,
                                )

        stage = "Evaluate the TEST reuse/refresh policy"
        decision = test_db_policy.decide_test_database(
            observation, refresh_from_prd=refresh_from_prd
        )
        report["decision"] = decision.to_dict()
        report["ok"] = decision.action != "abort"
        return report

    except Exception as exc:
        error = _redact_error(str(exc), configs)
        failure = _failure_diagnostic(exc, stage, error)
        decision = test_db_policy.decide_test_database(
            test_db_policy.DatabaseObservation(
                exists=bool(report.get("test") and report["test"].get("exists")),
                inspection_error=error,
            ),
            refresh_from_prd=refresh_from_prd,
        )
        report["decision"] = decision.to_dict()
        report["ok"] = False
        report["error"] = error
        report["failure"] = failure
        return report


def _failure_diagnostic(exc: Exception, stage: str, detail: str) -> dict[str, str]:
    """Map an inspection exception to stable, actionable operator diagnostics."""
    lower = detail.lower()
    code = "database_inspection_failed"
    summary = "The read-only database inspection could not complete."
    next_action = "Review the failing stage and diagnostic detail; no database changes were made."

    if "not loopback" in lower:
        code = "postgres_server_not_loopback"
        summary = "PostgreSQL reported a server address outside the local loopback range."
        next_action = (
            "Check PG_HOST in both existing environment files and confirm PostgreSQL is "
            "listening locally. This deployment intentionally refuses remote database endpoints."
        )
    elif "invalid postgresql server address" in lower:
        code = "postgres_server_address_invalid"
        summary = "PostgreSQL returned a server address that could not be parsed."
        next_action = (
            "Check the PostgreSQL server endpoint returned by inet_server_addr(). "
            "The inspector accepts bare IPs and inet values with a prefix such as ::1/128."
        )
    elif "remote host" in lower or "same local host and port" in lower:
        code = "database_target_configuration_mismatch"
        summary = "TEST and PRD are not configured for the same local PostgreSQL endpoint."
        next_action = (
            "Check PG_HOST and PG_PORT in both existing environment files. "
            "Both must refer to the same local PostgreSQL listener."
        )
    elif "marker" in lower or "unmarked" in lower:
        code = "database_environment_marker_invalid"
        summary = "A database environment marker does not match the expected environment."
        next_action = (
            "Confirm APP_ENV=test and the TEST database marker is 'test'; PRD must remain unmarked. "
            "Do not drop or refresh a database solely to bypass this diagnostic."
        )
    elif "permission" in lower or "ownership" in lower or "privilege" in lower:
        code = "test_database_permissions_invalid"
        summary = "The TEST application role does not have the required database ownership or privileges."
        next_action = (
            "The bootstrap policy will rebuild TEST from the latest validated PRD backup and "
            "provision ownership and grants for the configured TEST role."
        )
    elif "migradiff" in lower or "schema comparator" in lower or "schema comparison" in lower:
        code = "schema_comparison_failed"
        summary = "The database identities were checked, but MigraDiff could not complete the schema comparison."
        next_action = (
            "Check that MigraDiff 1.7.2 is installed in the TEST virtual environment, "
            "both database users can read schema metadata, and PostgreSQL is accepting connections."
        )
    elif any(token in lower for token in (
        "could not connect", "connection refused", "password authentication failed",
        "timeout expired", "no password supplied", "permission denied",
    )):
        code = "postgres_connection_or_authorization_failed"
        summary = "A PostgreSQL connection or authorization check failed."
        next_action = (
            "Verify PG_HOST, PG_PORT, PG_DB, PG_USER, and PG_PASSWORD in the existing "
            "environment files, and confirm the corresponding PostgreSQL role can connect."
        )
    elif "app_env" in lower or "environment file" in lower:
        code = "environment_configuration_invalid"
        summary = "The TEST or PRD environment configuration did not pass validation."
        next_action = (
            "TEST backend configuration must contain APP_ENV=test; PRD backend configuration "
            "must contain APP_ENV=prod. Confirm database names and local host/port settings."
        )

    return {
        "code": code,
        "stage": stage,
        "summary": summary,
        "detail": detail,
        "next_action": next_action,
    }


def format_failure_report(report: dict[str, Any]) -> str:
    """Render the structured failure report as concise operator-facing text."""
    failure = report.get("failure") or {}
    lines = [
        "ERROR: Read-only TEST/PRD database inspection failed.",
        "Stage: %s" % (failure.get("stage") or "Database inspection"),
        "Code: %s" % (failure.get("code") or "database_inspection_failed"),
        "Problem: %s" % (failure.get("summary") or "The inspection could not complete."),
    ]
    detail = failure.get("detail") or report.get("error")
    if detail:
        lines.append("Detail: %s" % detail)
    if failure.get("next_action"):
        lines.append("Next action: %s" % failure["next_action"])
    lines.append("No database was modified by this inspection.")
    return "\n".join(lines)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only inspection and database decision for TEST vs PRD."
    )
    parser.add_argument("--test-env-file", required=True, help="TEST migration env file")
    parser.add_argument("--prd-env-file", required=True, help="PRD migration env file")
    parser.add_argument(
        "--allow-prd",
        action="store_true",
        help="explicitly authorize read-only inspection of the unmarked PRD database",
    )
    parser.add_argument(
        "--refresh-from-prd",
        action="store_true",
        help="record the operator's explicit request to refresh TEST from PRD",
    )
    parser.add_argument(
        "--diff-file",
        help="optional private JSON artifact for the full MigraDiff report",
    )
    parser.add_argument(
        "--baseline-state-dir",
        help="private evidence directory containing completed TEST restore journals",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if not args.allow_prd:
        report = {
            "event": "test_database_inspection",
            "ok": False,
            "decision": {"action": "abort", "reason": "--allow-prd is required"},
            "failure": {
                "code": "prd_read_authorization_missing",
                "stage": "Validate explicit PRD inspection authorization",
                "summary": "Read-only PRD inspection requires explicit authorization.",
                "detail": "The --allow-prd flag was not supplied.",
                "next_action": "Rerun the read-only inspector with --allow-prd.",
            },
        }
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
        print(format_failure_report(report), file=sys.stderr)
        return 1

    try:
        test_config = _env.load_env_file(args.test_env_file)
        prd_config = _env.load_env_file(args.prd_env_file)
        report = inspect_test_database(
            test_config,
            prd_config,
            refresh_from_prd=args.refresh_from_prd,
            diff_file=args.diff_file,
            baseline_state_dir=args.baseline_state_dir,
        )
    except Exception as exc:
        error = str(exc)
        failure = _failure_diagnostic(
            exc, "Load and validate the existing TEST and PRD environment files", error
        )
        report = {
            "event": "test_database_inspection",
            "ok": False,
            "decision": {"action": "abort", "reason": error},
            "error": error,
            "failure": failure,
        }

    # Machine-readable evidence is always written to stdout. When invoked by
    # bootstrap, stdout is retained as a JSON report and stderr is shown to the
    # operator as an actionable explanation.
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    if not report.get("ok"):
        print(format_failure_report(report), file=sys.stderr)
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
