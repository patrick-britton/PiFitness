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
import ipaddress
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
        loopback = ipaddress.ip_address(str(server_address)).is_loopback
    except ValueError:
        loopback = False
    if not loopback:
        raise TestDatabaseInspectionError(
            "PostgreSQL server address %r is not loopback." % server_address
        )
    if int(server_port) != int(expected_port):
        raise TestDatabaseInspectionError(
            "Connected to PostgreSQL port %r, expected %r."
            % (server_port, expected_port)
        )
    return {
        "database": str(database),
        "server_address": str(server_address),
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
    if (
        identity["server_address"] != prd_identity["server_address"]
        or identity["server_port"] != prd_identity["server_port"]
    ):
        raise TestDatabaseInspectionError(
            "%s resolved to a different PostgreSQL server endpoint than PRD." % label
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

    try:
        test_db, prd_db, test_params, _ = _check_config_pair(
            test_config, prd_config
        )
        report["test"] = {"database": test_db, "exists": None, "marker": None}

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
                try:
                    comparison = compare(
                        test_config,
                        prd_config,
                        diff_file=diff_file,
                    )
                except Exception as exc:
                    error = _redact_error(str(exc), configs)
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

        decision = test_db_policy.decide_test_database(
            observation, refresh_from_prd=refresh_from_prd
        )
        report["decision"] = decision.to_dict()
        report["ok"] = decision.action != "abort"
        return report

    except Exception as exc:
        error = _redact_error(str(exc), configs)
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
        return report


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
        print(
            json.dumps(
                {
                    "event": "test_database_inspection",
                    "ok": False,
                    "decision": {"action": "abort", "reason": "--allow-prd is required"},
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
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
        report = {
            "event": "test_database_inspection",
            "ok": False,
            "decision": {"action": "abort", "reason": str(exc)},
        }

    output_stream = sys.stdout if report.get("ok") else sys.stderr
    print(json.dumps(report, sort_keys=True, separators=(",", ":")), file=output_stream)
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
