"""Unit tests for read-only TEST database inspection and policy handoff."""

import json

import pytest

from tools.db import inspect_test_db

pytestmark = pytest.mark.unit


class FakeCursor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.current = None
        self.queries = []
        self.closed = False

    def execute(self, query, params=None):
        self.queries.append((query, params))
        if not self.responses:
            raise AssertionError("Unexpected SQL query: %s" % query)
        expected_kind, response = self.responses.pop(0)
        if expected_kind == "identity" and "current_database()" not in query:
            raise AssertionError("Expected identity query, got: %s" % query)
        if expected_kind == "catalog" and "FROM pg_database" not in query:
            raise AssertionError("Expected pg_database query, got: %s" % query)
        if expected_kind == "permissions_meta" and "FROM pg_database" not in query:
            raise AssertionError("Expected TEST ownership query, got: %s" % query)
        if expected_kind == "privileges" and "has_database_privilege" not in query:
            raise AssertionError("Expected TEST role privilege query, got: %s" % query)
        self.current = response

    def fetchone(self):
        return self.current

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def set_session(self, *, readonly=False):
        self.readonly = readonly

    def close(self):
        self.closed = True


def _config(env, db, *, host="localhost", port="5432", user=None):
    return {
        "APP_ENV": env,
        "PG_DB": db,
        "PG_HOST": host,
        "PG_PORT": str(port),
        "PG_USER": user or ("test_migrator" if env == "test" else "prd_migrator"),
        "PG_PASSWORD": "not-for-logging",
        "PGSSLMODE": "disable",
    }


def _identity(db, marker=None, address="127.0.0.1", port=5432):
    return (db, address, port, marker)


def _install_connections(
    monkeypatch,
    *,
    test_exists=True,
    marker="test",
    test_address="127.0.0.1",
    test_port=5432,
    catalog_allow_connections=True,
    catalog_is_template=False,
    test_connect_error=None,
    prd_marker=None,
    prd_address="127.0.0.1",
    test_permissions=(True, True, True, True),
    database_owner="test_migrator",
    schema_owner="test_migrator",
    catalog_oid=12345,
):
    prd_cursor = FakeCursor(
        [
            ("identity", _identity("personal_fitness", prd_marker, prd_address)),
            (
                "catalog",
                (
                    ("personal_fitness_test", catalog_allow_connections, catalog_is_template, database_owner, catalog_oid)
                    if test_exists
                    else None
                ),
            ),
        ]
    )
    prd_connection = FakeConnection(prd_cursor)

    test_admin_identity_cursor = FakeCursor(
        [("identity", _identity("personal_fitness_test", marker, test_address, test_port))]
    )
    test_admin_identity_connection = FakeConnection(test_admin_identity_cursor)

    permission_cursor = FakeCursor(
        [
            ("identity", _identity("personal_fitness_test", marker, test_address, test_port)),
            ("permissions_meta", (database_owner, schema_owner)),
            ("privileges", test_permissions),
        ]
    )
    permission_connection = FakeConnection(permission_cursor)

    test_app_cursor = FakeCursor(
        [("identity", _identity("personal_fitness_test", marker, test_address, test_port))]
    )
    test_app_connection = FakeConnection(test_app_cursor)
    admin_test_connections = [test_admin_identity_connection, permission_connection]

    def connect(dbname, params):
        if dbname == "personal_fitness":
            return prd_connection
        if dbname == "personal_fitness_test":
            if params.get("user") == "test_migrator":
                if test_connect_error:
                    raise test_connect_error
                return test_app_connection
            if not admin_test_connections:
                raise AssertionError("Unexpected extra admin TEST connection")
            return admin_test_connections.pop(0)
        raise AssertionError("Unexpected database connection: %s" % dbname)

    monkeypatch.setattr(inspect_test_db.migration_runner, "_connect", connect)
    return {
        "prd_cursor": prd_cursor,
        "prd_connection": prd_connection,
        "test_admin_identity_cursor": test_admin_identity_cursor,
        "test_admin_identity_connection": test_admin_identity_connection,
        "permission_cursor": permission_cursor,
        "permission_connection": permission_connection,
        "test_cursor": test_app_cursor,
        "test_connection": test_app_connection,
    }


def _configs():
    return (
        _config("test", "personal_fitness_test"),
        _config("prod", "personal_fitness"),
    )


def test_missing_test_database_selects_create_from_prd_without_comparison(monkeypatch):
    handles = _install_connections(monkeypatch, test_exists=False)
    compare_calls = []

    report = inspect_test_db.inspect_test_database(
        *_configs(), compare_fn=lambda *a, **k: compare_calls.append((a, k))
    )

    assert report["ok"] is True
    assert report["test"]["exists"] is False
    assert report["schema_comparison"] is None
    assert report["decision"]["action"] == "create_from_prd"
    assert compare_calls == []
    assert handles["prd_connection"].closed
    assert handles["prd_connection"].readonly is True


@pytest.mark.parametrize("marker", [None, "", "dev", "prod", "unexpected"])
def test_missing_or_wrong_test_marker_selects_refresh_without_comparison(
    monkeypatch, marker
):
    _install_connections(monkeypatch, marker=marker)
    compare_calls = []

    report = inspect_test_db.inspect_test_database(
        *_configs(), compare_fn=lambda *a, **k: compare_calls.append((a, k))
    )

    assert report["ok"] is True
    assert report["test"]["exists"] is True
    assert report["test"]["marker"] == (marker or None)
    assert report["schema_comparison"] is None
    assert report["decision"]["action"] == "refresh_from_prd"
    assert compare_calls == []


def test_identical_schema_reuses_test_and_offers_refresh(monkeypatch):
    handles = _install_connections(monkeypatch)
    compare_calls = []

    def compare(test_config, prd_config, **kwargs):
        compare_calls.append((test_config, prd_config, kwargs))
        return {"ok": True, "status": "identical", "statement_count": 0}

    report = inspect_test_db.inspect_test_database(*_configs(), compare_fn=compare)

    assert report["ok"] is True
    assert report["decision"]["action"] == "reuse_test"
    assert report["decision"]["offer_refresh"] is True
    assert report["schema_comparison"]["status"] == "identical"
    assert handles["prd_connection"].readonly is True
    assert handles["permission_connection"].readonly is True
    assert handles["test_connection"].readonly is True
    assert len(compare_calls) == 1



def test_bad_test_permissions_select_refresh_without_schema_comparison(monkeypatch):
    handles = _install_connections(monkeypatch, test_permissions=(True, True, True, False))
    compare_calls = []
    report = inspect_test_db.inspect_test_database(
        *_configs(), compare_fn=lambda *a, **k: compare_calls.append((a, k))
    )
    assert report["ok"] is True
    assert report["test"]["permissions"]["ok"] is False
    assert report["decision"]["action"] == "refresh_from_prd"
    assert compare_calls == []
    assert not handles["test_connection"].closed

def test_different_schema_selects_refresh(monkeypatch):
    _install_connections(monkeypatch)

    report = inspect_test_db.inspect_test_database(
        *_configs(),
        compare_fn=lambda *a, **k: {"ok": True, "status": "different", "statement_count": 2},
    )

    assert report["decision"]["action"] == "refresh_from_prd"
    assert report["schema_comparison"]["statement_count"] == 2


def test_explicit_refresh_is_reflected_in_decision(monkeypatch):
    _install_connections(monkeypatch)

    report = inspect_test_db.inspect_test_database(
        *_configs(),
        refresh_from_prd=True,
        compare_fn=lambda *a, **k: {"ok": True, "status": "identical", "statement_count": 0},
    )

    assert report["decision"]["action"] == "refresh_from_prd"
    assert "explicitly requested" in report["decision"]["reason"]


def test_schema_comparator_exception_fails_closed(monkeypatch):
    _install_connections(monkeypatch)

    def fail(*args, **kwargs):
        raise RuntimeError("MigraDiff connection failed")

    report = inspect_test_db.inspect_test_database(*_configs(), compare_fn=fail)

    assert report["ok"] is False
    assert report["decision"]["action"] == "abort"
    assert report["schema_comparison"]["status"] == "error"
    assert "MigraDiff connection failed" in report["decision"]["reason"]


def test_malformed_comparator_result_fails_closed(monkeypatch):
    _install_connections(monkeypatch)

    report = inspect_test_db.inspect_test_database(
        *_configs(), compare_fn=lambda *a, **k: {"ok": True, "status": "error"}
    )

    assert report["ok"] is False
    assert report["decision"]["action"] == "abort"


def test_test_database_on_different_server_fails_closed(monkeypatch):
    _install_connections(monkeypatch, test_address="127.0.0.2")
    compare_calls = []

    report = inspect_test_db.inspect_test_database(
        *_configs(), compare_fn=lambda *a, **k: compare_calls.append((a, k))
    )

    assert report["ok"] is False
    assert report["decision"]["action"] == "abort"
    assert "different PostgreSQL server endpoint" in report["error"]
    assert compare_calls == []




def test_ipv6_loopback_inet_prefix_is_accepted_and_normalized(monkeypatch):
    _install_connections(
        monkeypatch,
        test_address="::1/128",
        prd_address="::1",
    )

    report = inspect_test_db.inspect_test_database(
        *_configs(),
        compare_fn=lambda *a, **k: {"ok": True, "status": "identical", "statement_count": 0},
    )

    assert report["ok"] is True
    assert report["test"]["server_address"] == "::1"
    assert report["prd"]["server_address"] == "::1"
    assert report["decision"]["action"] == "reuse_test"


def test_failure_report_includes_stage_code_and_actionable_next_step(monkeypatch):
    _install_connections(monkeypatch, test_address="192.168.1.20")

    report = inspect_test_db.inspect_test_database(*_configs())
    failure = report["failure"]
    human = inspect_test_db.format_failure_report(report)

    assert report["ok"] is False
    assert failure["code"] == "postgres_server_not_loopback"
    assert "Connect to TEST" in failure["stage"]
    assert "not loopback" in failure["detail"]
    assert "PG_HOST" in failure["next_action"]
    assert "Problem:" in human
    assert "Next action:" in human
    assert "No database was modified" in human

def test_test_database_port_mismatch_fails_closed(monkeypatch):
    _install_connections(monkeypatch, test_port=5433)

    report = inspect_test_db.inspect_test_database(*_configs())

    assert report["decision"]["action"] == "abort"
    assert "Connected to PostgreSQL port 5433" in report["error"]


def test_test_database_connection_error_is_not_treated_as_bad_marker(monkeypatch):
    _install_connections(
        monkeypatch,
        test_connect_error=RuntimeError("permission denied for database"),
    )

    report = inspect_test_db.inspect_test_database(*_configs())

    assert report["ok"] is False
    assert report["decision"]["action"] == "abort"
    assert "permission denied" in report["error"]


def test_test_database_catalog_disallow_connections_fails_closed(monkeypatch):
    _install_connections(monkeypatch, catalog_allow_connections=False)

    report = inspect_test_db.inspect_test_database(*_configs())

    assert report["decision"]["action"] == "abort"
    assert "disallows connections" in report["error"]


def test_test_database_catalog_template_flag_fails_closed(monkeypatch):
    _install_connections(monkeypatch, catalog_is_template=True)

    report = inspect_test_db.inspect_test_database(*_configs())

    assert report["decision"]["action"] == "abort"
    assert "PostgreSQL template" in report["error"]


def test_prd_must_remain_unmarked(monkeypatch):
    _install_connections(monkeypatch, prd_marker="prod")

    report = inspect_test_db.inspect_test_database(*_configs())

    assert report["decision"]["action"] == "abort"
    assert "PRD must remain unmarked" in report["error"]


def test_same_test_and_prd_database_is_rejected(monkeypatch):
    test_config, prd_config = _configs()
    test_config["PG_DB"] = prd_config["PG_DB"]

    report = inspect_test_db.inspect_test_database(test_config, prd_config)

    assert report["decision"]["action"] == "abort"
    assert "same database" in report["error"]


def test_wrong_app_env_is_rejected(monkeypatch):
    test_config, prd_config = _configs()
    test_config["APP_ENV"] = "dev"

    report = inspect_test_db.inspect_test_database(test_config, prd_config)

    assert report["decision"]["action"] == "abort"
    assert "APP_ENV=test" in report["error"]


def test_nonlocal_host_is_rejected_before_connecting(monkeypatch):
    test_config, prd_config = _configs()
    test_config["PG_HOST"] = "db.example.com"
    prd_config["PG_HOST"] = "db.example.com"
    connect_calls = []
    monkeypatch.setattr(
        inspect_test_db.migration_runner,
        "_open_validated_connection",
        lambda *a, **k: connect_calls.append((a, k)),
    )

    report = inspect_test_db.inspect_test_database(test_config, prd_config)

    assert report["decision"]["action"] == "abort"
    assert "remote host" in report["error"]
    assert connect_calls == []


def test_diff_file_is_forwarded_to_comparator(monkeypatch, tmp_path):
    _install_connections(monkeypatch)
    path = str(tmp_path / "diff.json")
    seen = {}

    def compare(test_config, prd_config, **kwargs):
        seen.update(kwargs)
        return {"ok": True, "status": "identical", "statement_count": 0}

    report = inspect_test_db.inspect_test_database(
        *_configs(), compare_fn=compare, diff_file=path
    )

    assert report["ok"]
    assert seen["diff_file"] == path


def test_cli_requires_explicit_prd_authorization(capsys):
    code = inspect_test_db.main(
        [
            "--test-env-file", "test.env",
            "--prd-env-file", "prd.env",
        ]
    )

    assert code == 1
    captured = capsys.readouterr()
    assert '"action":"abort"' in captured.out
    assert "explicit authorization" in captured.err


def test_verified_baseline_with_checksum_valid_migrations_is_reused_without_schema_compare(monkeypatch, tmp_path):
    _install_connections(monkeypatch, catalog_oid=777)
    state = {
        "status": "baseline_ready",
        "test_database": "personal_fitness_test",
        "prd_database": "personal_fitness",
        "new_test_database_oid": 777,
        "backup_path": "/home/god/Documents/DB_Backups/pifitness_prd_20261111_090000.dump",
        "backup_timestamp_utc": "2026-11-11T17:00:00+00:00",
        "backup_sha256": "a" * 64,
    }
    (tmp_path / "test-db-recovery-baseline.json").write_text(json.dumps(state))
    compare_calls = []
    status_calls = []

    def status(config, **kwargs):
        status_calls.append((config, kwargs))
        return {"ok": True, "safe_to_apply": True, "environment": "test", "database": "personal_fitness_test", "applied": 2, "pending": 1, "total": 3}

    monkeypatch.setattr(inspect_test_db.migration_runner, "cmd_status", status)
    report = inspect_test_db.inspect_test_database(
        *_configs(),
        compare_fn=lambda *a, **k: compare_calls.append((a, k)),
        baseline_state_dir=str(tmp_path),
    )
    assert report["ok"] is True
    assert report["decision"]["action"] == "reuse_test"
    assert report["decision"]["offer_refresh"] is True
    assert report["schema_comparison"]["status"] == "migration_managed"
    assert report["baseline_provenance"]["database_oid"] == 777
    assert status_calls
    assert compare_calls == []


def test_baseline_journal_oid_mismatch_does_not_bypass_schema_comparison(monkeypatch, tmp_path):
    _install_connections(monkeypatch, catalog_oid=777)
    (tmp_path / "test-db-recovery-stale.json").write_text(json.dumps({
        "status": "baseline_ready",
        "test_database": "personal_fitness_test",
        "prd_database": "personal_fitness",
        "new_test_database_oid": 776,
    }))
    compare_calls = []
    report = inspect_test_db.inspect_test_database(
        *_configs(),
        compare_fn=lambda *a, **k: compare_calls.append((a, k)) or {
            "ok": True, "status": "different", "statement_count": 1,
        },
        baseline_state_dir=str(tmp_path),
    )
    assert report["decision"]["action"] == "refresh_from_prd"
    assert len(compare_calls) == 1


def test_corrupt_migration_ledger_fails_closed_instead_of_recloning(monkeypatch, tmp_path):
    _install_connections(monkeypatch, catalog_oid=777)
    (tmp_path / "test-db-recovery-baseline.json").write_text(json.dumps({
        "status": "baseline_ready",
        "test_database": "personal_fitness_test",
        "prd_database": "personal_fitness",
        "new_test_database_oid": 777,
        "backup_path": "/home/god/Documents/DB_Backups/pifitness_prd_20261111_090000.dump",
        "backup_timestamp_utc": "2026-11-11T17:00:00+00:00",
        "backup_sha256": "a" * 64,
    }))
    compare_calls = []

    def bad_status(*args, **kwargs):
        raise RuntimeError("applied migration checksum mismatch")

    monkeypatch.setattr(inspect_test_db.migration_runner, "cmd_status", bad_status)
    report = inspect_test_db.inspect_test_database(
        *_configs(),
        compare_fn=lambda *a, **k: compare_calls.append((a, k)),
        baseline_state_dir=str(tmp_path),
    )
    assert report["ok"] is False
    assert report["decision"]["action"] == "abort"
    assert "checksum mismatch" in report["error"]
    assert compare_calls == []


def test_verified_baseline_with_no_applied_migrations_is_reused_without_schema_compare(monkeypatch, tmp_path):
    _install_connections(monkeypatch, catalog_oid=777)
    (tmp_path / "test-db-recovery-baseline.json").write_text(json.dumps({
        "status": "baseline_ready",
        "test_database": "personal_fitness_test",
        "prd_database": "personal_fitness",
        "new_test_database_oid": 777,
        "backup_path": "/home/god/Documents/DB_Backups/pifitness_prd_20261111_090000.dump",
        "backup_timestamp_utc": "2026-11-11T17:00:00+00:00",
        "backup_sha256": "a" * 64,
    }))
    monkeypatch.setattr(
        inspect_test_db.migration_runner,
        "cmd_status",
        lambda *a, **k: {"ok": True, "safe_to_apply": True, "environment": "test", "database": "personal_fitness_test", "applied": 0, "pending": 2, "total": 2},
    )
    compare_calls = []
    report = inspect_test_db.inspect_test_database(
        *_configs(),
        compare_fn=lambda *a, **k: compare_calls.append((a, k)) or {
            "ok": True, "status": "identical", "statement_count": 0,
        },
        baseline_state_dir=str(tmp_path),
    )
    assert report["decision"]["action"] == "reuse_test"
    assert report["schema_comparison"]["status"] == "restored_baseline"
    assert len(compare_calls) == 0
