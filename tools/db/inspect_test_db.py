#!/usr/bin/env python3
"""Read-only inspection and decision report for PiFitness TEST vs PRD.

This module performs no DDL and never refreshes, renames, drops, or modifies a
 database. It:
- validates the explicitly supplied TEST and PRD migration configurations;
- validates PRD identity/marker through the migration runner's guarded path;
- checks whether the configured TEST database exists in the same cluster;
- if it exists, connects without applying the TEST marker guard so a missing or
  wrong marker can be reported as a policy observation (not an inspection error);
- invokes the MigraDiff adapter only when TEST exists and has marker `test`;
- passes the observation to the pure TEST database policy.

The deployment orchestrator is responsible for preserving TEST before any
refresh and for performing database provisioning/rollback. This inspector is
strictly read-only; both PostgreSQL sessions are marked read-only.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable

if __package__:
    from . import compare_schema, migrate as migration_runner, test_db_policy
else:
    import compare_schema
    import migrate as migration_runner
    import test_db_policy


class TestDatabaseInspectionError(RuntimeError):
    """The TEST/PRD database state could not be established safely."""


def _check_config_pair(
    test_config: dict[str, str], prd_config: dict[str, str]
) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    """Validate the intended local TEST/PRD configuration pair."""
    if test_config.get("APP_ENV") != migration_runner.APP_ENV_TEST:
        raise TestDatabaseInspectionError("TEST env file must specify APP_ENV=test.")
    if prd_config.get("APP_ENV") != migration_runner.APP_ENV_PROD:
        raise TestDatabaseInspectionError("PRD env file must specify APP_ENV=prod.")

    test_db = migration_runner._resolve_db_name(test_config)
    prd_db = migration_runner._resolve_db_name(prd_config)
    if test_db == prd_db:
        raise TestDatabaseInspectionError(
            "TEST and PRD env files resolve to the same database %r; refusing inspection."
            % test_db
        )

    test_params = migration_runner._conn_params(test_config)
    prd_params = migration_runner._conn_params(prd_config)
    migration_runner._validate_target(test_config, test_params, allow_prd=False)
    migration_runner._validate_target(prd_config, prd_params, allow_prd=True)

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
    dbname = migration_runner._resolve_db_name(config)
    params = migration_runner._conn_params(config)
    migration_runner._validate_target(config, params, allow_prd=allow_prd)
    conn = migration_runner._connect(dbname, params)
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
        normalized_address = migration_runner._normalize_server_address(server_address)
    except ValueError as exc:
        raise TestDatabaseInspectionError(str(exc)) from exc
    if not normalized_address.is_loopback:
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
        "SELECT datname, datallowconn, datistemplate "
        "FROM pg_database WHERE datname = %s",
        (test_db,),
    )
    row = prd_cur.fetchone()
    if row is None:
        return None
    database, allow_connections, is_template = row
    if database != test_db:
        raise TestDatabaseInspectionError(
            "PostgreSQL catalog returned unexpected database %r; expected %r."
            % (database, test_db)
        )
    return {
        "database": database,
        "allow_connections": bool(allow_connections),
        "is_template": bool(is_template),
    }


def _verify_same_server(
    identity: dict[str, Any], prd_identity: dict[str, Any], *, label: str
) -> None:
    try:
        test_address = migration_runner._normalize_server_address(identity["server_address"])
        prd_address = migration_runner._normalize_server_address(prd_identity["server_address"])
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
            migration_runner._guard(
                migration_runner.APP_ENV_PROD, prd_identity["marker"]
            )
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

            stage = "Connect to TEST and verify its database identity and marker"
            # Intentionally bypass only the TEST marker guard here. This
            # read-only connection is necessary to observe a missing or wrong
            # marker so policy can request a preserved refresh.
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

            report["test"].update(
                {
                    "exists": True,
                    "marker": test_identity["marker"],
                    "server_address": test_identity["server_address"],
                    "server_port": test_identity["server_port"],
                }
            )

            if test_identity["marker"] != "test":
                observation = test_db_policy.DatabaseObservation(
                    exists=True, marker=test_identity["marker"]
                )
            else:
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
                            schema_status="error",
                            inspection_error=error,
                        )
                    else:
                        report["schema_comparison"] = comparison
                        observation = test_db_policy.DatabaseObservation(
                            exists=True,
                            marker="test",
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
        test_config = migration_runner._load_env_file(args.test_env_file)
        prd_config = migration_runner._load_env_file(args.prd_env_file)
        report = inspect_test_database(
            test_config,
            prd_config,
            refresh_from_prd=args.refresh_from_prd,
            diff_file=args.diff_file,
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
