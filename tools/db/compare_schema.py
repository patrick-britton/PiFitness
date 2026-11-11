#!/usr/bin/env python3
"""Read-only TEST/PRD PostgreSQL schema comparison using MigraDiff.

This wrapper owns only PiFitness-specific safety and policy boundaries:
- explicit TEST/PRD migration env files;
- local endpoint, database identity, and marker verification;
- temporary, private libpq password file (passwords never appear in argv);
- structured MigraDiff JSON parsing;
- filtering out non-structural COMMENT statements and the migration ledger.

MigraDiff generates DDL as comparison output. This wrapper NEVER passes --apply
and never executes the generated SQL.

Exit codes:
    0: JSON result emitted; status is "identical" or "different"
    1: comparison could not be completed safely
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import importlib.metadata
import json
import os
import re
import subprocess
import sys
import sysconfig
import tempfile
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote, urlencode

if __package__:
    from . import migrate as migration_runner
else:
    import migrate as migration_runner


MIGRADIFF_DISTRIBUTION = "migradiff"
MIGRADIFF_REQUIRED_VERSION = "1.7.2"


class SchemaComparisonError(RuntimeError):
    """The requested schema comparison could not be completed safely."""


COMPARISON_SCOPE = {
    "included": [
        "schemas and application objects represented by MigraDiff",
        "tables, columns, defaults, constraints, indexes, sequences, views, routines, types, triggers, row-level-security policies, and extensions",
    ],
    "excluded": [
        "table and sequence data",
        "object ownership",
        "ACLs and grants (MigraDiff privilege comparison is not enabled)",
        "COMMENT ON statements (documentation metadata, not structural compatibility)",
        "the schema_migrations bookkeeping table and objects that reference it",
        "database-level roles and role memberships (cluster-level objects)",
    ],
}

_LEDGER_IDENTIFIER = re.compile(
    r"(?<![A-Za-z0-9_$])schema_migrations(?![A-Za-z0-9_$])", re.IGNORECASE
)


def _connection_identity(config: dict[str, str], *, allow_prd: bool) -> dict[str, Any]:
    """Open a guarded connection and return the actual PostgreSQL identity."""
    conn, cur = migration_runner._open_validated_connection(
        config, allow_prd=allow_prd
    )
    try:
        cur.execute(
            "SELECT current_database(), inet_server_addr()::text, inet_server_port(), "
            "current_setting('pifitness.env', true)"
        )
        row = cur.fetchone()
        if not row:
            raise SchemaComparisonError(
                "PostgreSQL returned no database identity row."
            )
        database, server_address, server_port, marker = row
        expected_database = migration_runner._resolve_db_name(config)
        if database != expected_database:
            raise SchemaComparisonError(
                "Connected to database %r, expected %r."
                % (database, expected_database)
            )
        if server_address is None or server_port is None:
            raise SchemaComparisonError(
                "PostgreSQL did not report a TCP server address and port."
            )
        return {
            "environment": config["APP_ENV"],
            "database": database,
            "server_address": str(migration_runner._normalize_server_address(server_address)),
            "server_port": int(server_port),
            "marker": marker or None,
        }
    finally:
        cur.close()
        conn.close()


def _validate_pair(
    test_config: dict[str, str], prd_config: dict[str, str]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate distinct TEST/PRD databases on the same local PostgreSQL server."""
    if test_config.get("APP_ENV") != migration_runner.APP_ENV_TEST:
        raise SchemaComparisonError("TEST env file must specify APP_ENV=test.")
    if prd_config.get("APP_ENV") != migration_runner.APP_ENV_PROD:
        raise SchemaComparisonError("PRD env file must specify APP_ENV=prod.")

    test_db = migration_runner._resolve_db_name(test_config)
    prd_db = migration_runner._resolve_db_name(prd_config)
    if test_db == prd_db:
        raise SchemaComparisonError(
            "TEST and PRD env files resolve to the same database %r; refusing comparison."
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
        raise SchemaComparisonError(
            "TEST and PRD env files must target the same local host and port."
        )

    test_identity = _connection_identity(test_config, allow_prd=False)
    prd_identity = _connection_identity(prd_config, allow_prd=True)
    test_server_address = migration_runner._normalize_server_address(
        test_identity["server_address"]
    )
    prd_server_address = migration_runner._normalize_server_address(
        prd_identity["server_address"]
    )
    # Keep the returned identity evidence canonical as well as comparing it
    # canonically, so callers never see both ::1 and ::1/128 for one endpoint.
    test_identity["server_address"] = str(test_server_address)
    prd_identity["server_address"] = str(prd_server_address)
    if (
        test_server_address != prd_server_address
        or test_identity["server_port"] != prd_identity["server_port"]
    ):
        raise SchemaComparisonError(
            "TEST and PRD connections resolved to different PostgreSQL server endpoints "
            "(TEST=%s:%s, PRD=%s:%s)."
            % (
                test_server_address,
                test_identity["server_port"],
                prd_server_address,
                prd_identity["server_port"],
            )
        )
    if test_identity["marker"] != "test":
        raise SchemaComparisonError("TEST database marker is not exactly 'test'.")
    if prd_identity["marker"] is not None:
        raise SchemaComparisonError(
            "PRD database must remain unmarked; found marker %r."
            % prd_identity["marker"]
        )
    return test_identity, prd_identity


def _resolve_migradiff_executable(explicit_path: str | None = None) -> str:
    """Find the MigraDiff console script belonging to this Python environment."""
    if explicit_path:
        candidate = os.path.abspath(os.path.expanduser(explicit_path))
        if not os.path.isfile(candidate):
            raise SchemaComparisonError(
                "MigraDiff executable does not exist: %s" % candidate
            )
        return candidate

    scripts_dir = Path(sysconfig.get_path("scripts"))
    candidates = [
        scripts_dir / ("migra.exe" if os.name == "nt" else "migra"),
        scripts_dir / "migra",
        scripts_dir / "migra.exe",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)

    raise SchemaComparisonError(
        "MigraDiff command 'migra' was not found in this Python environment. "
        "Install tools/db/requirements-schema-compare.txt first."
    )


def _verify_migradiff_version() -> str:
    try:
        installed = importlib.metadata.version(MIGRADIFF_DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError as exc:
        raise SchemaComparisonError(
            "Python distribution 'migradiff' is not installed in this environment."
        ) from exc
    if installed != MIGRADIFF_REQUIRED_VERSION:
        raise SchemaComparisonError(
            "Unsupported migradiff version %s; this integration is pinned to %s."
            % (installed, MIGRADIFF_REQUIRED_VERSION)
        )
    return installed


def _pgpass_escape(value: str) -> str:
    """Escape a libpq password-file field (colon and backslash delimiters)."""
    escaped = value.replace("\\", "\\\\").replace(":", "\\:")
    return "\\*" if escaped == "*" else escaped


def _pgpass_entry(config: dict[str, str]) -> str:
    params = migration_runner._conn_params(config)
    fields = (
        params["host"],
        str(params["port"]),
        migration_runner._resolve_db_name(config),
        params["user"],
        config["PG_PASSWORD"],
    )
    if any("\n" in value or "\r" in value for value in fields):
        raise SchemaComparisonError(
            "Connection settings cannot contain newline characters."
        )
    return ":".join(_pgpass_escape(str(value)) for value in fields)


def _database_url(config: dict[str, str]) -> str:
    """Build a password-free SQLAlchemy/libpq URL for MigraDiff's argv."""
    params = migration_runner._conn_params(config)
    host = str(params["host"])
    if ":" in host and not host.startswith("["):
        host = "[%s]" % host
    username = quote(str(params["user"]), safe="")
    database = quote(migration_runner._resolve_db_name(config), safe="")
    query = urlencode({"sslmode": str(params["sslmode"])})
    return "postgresql://%s@%s:%s/%s?%s" % (
        username,
        host,
        params["port"],
        database,
        query,
    )


@contextmanager
def _private_pgpassfile(
    test_config: dict[str, str], prd_config: dict[str, str]
) -> Iterator[str]:
    """Context manager yielding a temporary 0600 pgpass file, then deleting it."""
    fd, path = tempfile.mkstemp(prefix="pifitness-migradiff-", suffix=".pgpass")
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(_pgpass_entry(test_config) + "\n")
            stream.write(_pgpass_entry(prd_config) + "\n")
        if os.name != "nt":
            os.chmod(path, 0o600)
        yield path
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError:
            # A cleanup failure is surfaced only if the path remains. Avoid
            # masking a comparison exception with a secondary unlink error.
            if os.path.exists(path):
                raise


def _migradiff_environment(pgpassfile: str) -> dict[str, str]:
    """Remove inherited libpq overrides and supply only this run's credentials."""
    child_env = os.environ.copy()
    for key in list(child_env):
        if key.upper().startswith("PG"):
            child_env.pop(key, None)
    child_env["PGPASSFILE"] = pgpassfile
    child_env["PGCONNECT_TIMEOUT"] = "5"
    # Enforce read-only behavior inside MigraDiff's own PostgreSQL sessions,
    # not merely by avoiding an --apply flag in this wrapper.
    child_env["PGOPTIONS"] = "-c default_transaction_read_only=on"
    return child_env


def _is_ignored_statement(statement: dict[str, Any]) -> str | None:
    sql = str(statement.get("sql") or "").lstrip()
    if sql[:10].upper().startswith("COMMENT ON"):
        return "comment_metadata"
    if _LEDGER_IDENTIFIER.search(sql):
        return "migration_ledger"
    return None


def _redact_diagnostic(value: str, configs: tuple[dict[str, str], ...]) -> str:
    diagnostic = value
    for config in configs:
        password = str(config.get("PG_PASSWORD") or "")
        if password:
            diagnostic = diagnostic.replace(password, "<redacted>")
    return diagnostic


def _write_diff_file(path: str, payload: dict[str, Any]) -> None:
    """Create a private JSON diff artifact without overwriting an existing file."""
    target = Path(os.path.abspath(os.path.expanduser(path)))
    target.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(str(target), flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
        if os.name != "nt":
            os.chmod(target, 0o600)
    except Exception:
        try:
            os.unlink(target)
        except OSError:
            pass
        raise


def compare_databases(
    test_config: dict[str, str],
    prd_config: dict[str, str],
    *,
    migradiff_path: str | None = None,
    diff_file: str | None = None,
) -> dict[str, Any]:
    """Compare TEST to PRD using MigraDiff; perform no database writes.

    The CLI is invoked in JSON mode and with --force-destructive because the
    wrapper needs a complete description of every structural difference,
    including destructive differences. No --apply flag is ever passed; SQL is
    treated strictly as diagnostic output.
    """
    test_identity, prd_identity = _validate_pair(test_config, prd_config)
    version = _verify_migradiff_version()
    executable = _resolve_migradiff_executable(migradiff_path)
    test_url = _database_url(test_config)
    prd_url = _database_url(prd_config)
    command = [
        executable,
        "--output",
        "json",
        "--force-destructive",
        test_url,
        prd_url,
    ]

    with _private_pgpassfile(test_config, prd_config) as pgpassfile:
        child_env = _migradiff_environment(pgpassfile)
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=child_env,
                check=False,
            )
        except OSError as exc:
            raise SchemaComparisonError(
                "Unable to execute MigraDiff: %s" % exc
            ) from exc

    if result.returncode != 0:
        diagnostic = _redact_diagnostic(
            (result.stderr or result.stdout or "no diagnostic returned").strip(),
            (test_config, prd_config),
        )
        raise SchemaComparisonError(
            "MigraDiff failed (exit %s): %s" % (result.returncode, diagnostic)
        )

    try:
        engine_report = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        diagnostic = _redact_diagnostic(
            (result.stdout or result.stderr or "empty output").strip(),
            (test_config, prd_config),
        )
        raise SchemaComparisonError(
            "MigraDiff did not return valid JSON: %s" % diagnostic[:1000]
        ) from exc

    if not isinstance(engine_report, dict) or not isinstance(
        engine_report.get("statements"), list
    ):
        raise SchemaComparisonError(
            "MigraDiff JSON did not contain the expected statements array."
        )

    raw_statements = engine_report["statements"]
    included: list[dict[str, Any]] = []
    ignored: list[dict[str, Any]] = []
    for statement in raw_statements:
        if not isinstance(statement, dict) or not isinstance(statement.get("sql"), str):
            raise SchemaComparisonError(
                "MigraDiff returned a malformed statement entry."
            )
        reason = _is_ignored_statement(statement)
        if reason:
            ignored.append({"reason": reason, "statement": statement})
        else:
            included.append(statement)

    serialized_for_fingerprint = json.dumps(
        [
            {"sql": statement["sql"], "risk": statement.get("risk", "unknown")}
            for statement in included
        ],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    diff_fingerprint = hashlib.sha256(
        serialized_for_fingerprint.encode("utf-8")
    ).hexdigest()
    risk_order = {"safe": 0, "warning": 1, "destructive": 2}
    risk_level = "low"
    if included:
        max_risk = max(
            (risk_order.get(str(item.get("risk", "safe")), 1) for item in included),
            default=0,
        )
        risk_level = {0: "low", 1: "medium", 2: "high"}[max_risk]

    preview = [str(item["sql"]).strip()[:800] for item in included[:40]]
    payload = {
        "event": "schema_comparison",
        "ok": True,
        "status": "identical" if not included else "different",
        "comparison_engine": MIGRADIFF_DISTRIBUTION,
        "comparison_engine_version": version,
        "test": test_identity,
        "prd": prd_identity,
        "direction": "TEST-to-PRD; generated SQL is diagnostic only and is never executed",
        "statement_count": len(included),
        "ignored_statement_count": len(ignored),
        "risk_level": risk_level,
        "diff_fingerprint": diff_fingerprint,
        "diff_preview": preview,
        "diff_truncated": len(included) > len(preview),
        "scope": COMPARISON_SCOPE,
    }
    if diff_file:
        artifact = {
            "report": payload,
            "migradiff": engine_report,
            "ignored_statements": ignored,
        }
        _write_diff_file(diff_file, artifact)
        payload["diff_file"] = os.path.abspath(os.path.expanduser(diff_file))
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only TEST/PRD PostgreSQL schema comparison using MigraDiff."
    )
    parser.add_argument("--test-env-file", required=True, help="TEST migration env file")
    parser.add_argument("--prd-env-file", required=True, help="PRD migration env file")
    parser.add_argument(
        "--allow-prd",
        action="store_true",
        help="explicitly authorize read-only inspection of the unmarked PRD database",
    )
    parser.add_argument(
        "--diff-file",
        help="optional path for a private full JSON diff artifact (must not already exist)",
    )
    parser.add_argument(
        "--migradiff",
        help="optional explicit path to the MigraDiff console executable",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if not args.allow_prd:
        print(
            "ERROR: read-only PRD schema inspection still requires --allow-prd.",
            file=sys.stderr,
        )
        return 1
    try:
        test_config = migration_runner._load_env_file(args.test_env_file)
        prd_config = migration_runner._load_env_file(args.prd_env_file)
        report = compare_databases(
            test_config,
            prd_config,
            migradiff_path=args.migradiff,
            diff_file=args.diff_file,
        )
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "event": "schema_comparison",
                    "ok": False,
                    "status": "error",
                    "error": str(exc),
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
