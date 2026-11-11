"""Unit tests for direct TEST replacement from a validated PRD archive."""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import types
from pathlib import Path
from subprocess import CompletedProcess

import pytest

if "psycopg2" not in sys.modules:
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        stub = types.ModuleType("psycopg2")
        stub.Error = Exception
        stub.connect = lambda **kwargs: (_ for _ in ()).throw(AssertionError("unexpected live DB connection"))
        sys.modules["psycopg2"] = stub

from tools.db import restore_test_db as recovery

pytestmark = pytest.mark.unit


class FakeCursor:
    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return FakeCursor()

    def close(self):
        self.closed = True


def _config(env, db, user):
    return {
        "APP_ENV": env,
        "PG_DB": db,
        "PG_HOST": "localhost",
        "PG_PORT": "5432",
        "PG_USER": user,
        "PG_PASSWORD": "secret-value",
        "PGSSLMODE": "disable",
    }


def test_latest_backup_selects_newest_matching_custom_archive(tmp_path, monkeypatch):
    older = tmp_path / "pifitness_prd_older.dump"
    newer = tmp_path / "pifitness_prd_newer.dump"
    older.write_bytes(b"older")
    newer.write_bytes(b"newer")
    os.utime(older, (100, 100))
    os.utime(newer, (200, 200))
    monkeypatch.setattr(recovery.shutil, "which", lambda name: "/usr/bin/pg_restore")

    def runner(args, **kwargs):
        return CompletedProcess(args, 0, stdout="; dbname: personal_fitness\n; Format: CUSTOM\n", stderr="")

    now = dt.datetime.fromtimestamp(300, tz=dt.timezone.utc)
    path, timestamp, checksum = recovery._latest_backup(
        tmp_path, "pifitness_prd_*.dump", "personal_fitness", now=now, runner=runner
    )
    assert path == newer
    assert timestamp.endswith("+00:00")
    assert checksum == recovery._sha256_file(newer)


def test_latest_backup_refuses_wrong_source_database(tmp_path, monkeypatch):
    backup = tmp_path / "pifitness_prd_latest.dump"
    backup.write_bytes(b"archive")
    os.utime(backup, (dt.datetime.now().timestamp(),) * 2)
    monkeypatch.setattr(recovery.shutil, "which", lambda name: "/usr/bin/pg_restore")

    def runner(args, **kwargs):
        return CompletedProcess(args, 0, stdout="; dbname: personal_fitness_test\n; Format: CUSTOM\n", stderr="")

    with pytest.raises(recovery.RecoveryError, match="not configured PRD"):
        recovery._latest_backup(tmp_path, "*.dump", "personal_fitness", runner=runner)


def test_latest_backup_refuses_older_than_24_hours(tmp_path, monkeypatch):
    backup = tmp_path / "pifitness_prd_old.dump"
    backup.write_bytes(b"archive")
    os.utime(backup, (100, 100))
    monkeypatch.setattr(recovery.shutil, "which", lambda name: "/usr/bin/pg_restore")

    def runner(args, **kwargs):
        return CompletedProcess(args, 0, stdout="; dbname: personal_fitness\n; Format: CUSTOM\n", stderr="")

    now = dt.datetime.fromtimestamp(100 + (25 * 3600), tz=dt.timezone.utc)
    with pytest.raises(recovery.RecoveryError, match="maximum permitted age"):
        recovery._latest_backup(tmp_path, "*.dump", "personal_fitness", now=now, runner=runner)


def test_latest_backup_does_not_fall_back_if_newest_archive_is_invalid(tmp_path, monkeypatch):
    older = tmp_path / "pifitness_prd_older.dump"
    newer = tmp_path / "pifitness_prd_newer.dump"
    older.write_bytes(b"good")
    newer.write_bytes(b"broken")
    now_epoch = dt.datetime.now(dt.timezone.utc).timestamp()
    os.utime(older, (now_epoch - 60, now_epoch - 60))
    os.utime(newer, (now_epoch, now_epoch))
    monkeypatch.setattr(recovery.shutil, "which", lambda name: "/usr/bin/pg_restore")

    def runner(args, **kwargs):
        return CompletedProcess(args, 0, stdout="invalid archive header", stderr="")

    with pytest.raises(recovery.RecoveryError, match="does not identify its source database"):
        recovery._latest_backup(tmp_path, "*.dump", "personal_fitness", runner=runner)


def test_latest_backup_rejects_symlinked_directory_and_path_glob(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(recovery.RecoveryError, match="symlink"):
        recovery._latest_backup(link, "*.dump", "personal_fitness")
    with pytest.raises(recovery.RecoveryError, match="filename pattern"):
        recovery._latest_backup(real, "../*.dump", "personal_fitness")


def _install_fake_recovery(monkeypatch, *, original_exists=True, restore_fails=False, schema_status="identical"):
    test_db = "personal_fitness_test"
    prd_db = "personal_fitness"
    dbs = {prd_db: {"oid": 1, "owner": "devuser", "is_template": False}}
    if original_exists:
        dbs[test_db] = {"oid": 2, "owner": "pifitness_test_app", "is_template": False}
    conn = FakeConnection()
    monkeypatch.setattr(recovery, "_admin_connection", lambda config: conn)
    monkeypatch.setattr(recovery, "_server_identity", lambda conn, port: {"server_address": "::1", "server_port": port})
    monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: role == "devuser")
    monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
        "can_login": True, "is_superuser": False,
    } if role == "pifitness_test_app" else None)
    monkeypatch.setattr(recovery, "_test_role_login_preflight", lambda *a, **k: {"checked": True})
    monkeypatch.setattr(recovery, "_catalog_db", lambda cur, name: dbs.get(name))
    monkeypatch.setattr(recovery, "_latest_backup", lambda *a, **k: (Path(__file__).resolve(), "2026-11-11T00:00:00+00:00", "a" * 64))
    extension_specs = [
        {"name": "plpgsql", "version": "1.0", "schema": "pg_catalog"},
        {"name": "timescaledb", "version": "2.20.0", "schema": "public"},
    ]
    monkeypatch.setattr(recovery, "_load_extension_specs", lambda *a, **k: extension_specs)
    monkeypatch.setattr(recovery, "_install_timescaledb", lambda *a, **k: None)

    def drop(_conn, name):
        dbs.pop(name, None)

    def create(_conn, *, test_db, test_role, prd_db):
        dbs[test_db] = {"oid": 3, "owner": test_role, "is_template": False}

    monkeypatch.setattr(recovery, "_drop_database", drop)
    monkeypatch.setattr(recovery, "_create_test_database", create)

    def restore(*args, **kwargs):
        if restore_fails:
            raise recovery.RecoveryError("injected pg_restore failure")
        assert kwargs["timescaledb_version"] == "2.20.0"

    monkeypatch.setattr(recovery, "_restore_archive", restore)
    monkeypatch.setattr(recovery, "_provision_ownership", lambda *a, **k: None)
    monkeypatch.setattr(recovery, "_set_database_owner", lambda *a, **k: None)
    monkeypatch.setattr(recovery, "_set_marker", lambda *a, **k: None)
    monkeypatch.setattr(recovery, "_verify_database", lambda *a, **k: {
        "database": test_db, "marker": "test", "user": "pifitness_test_app",
        "database_owner": "pifitness_test_app", "public_schema_owner": "pifitness_test_app",
    })
    monkeypatch.setattr(recovery.compare_schema, "compare_databases", lambda *a, **k: {
        "ok": schema_status in {"identical", "different"}, "status": schema_status, "statement_count": 0,
    })
    monkeypatch.setattr(recovery, "_db_oid", lambda _conn, name: dbs[name]["oid"] if name in dbs else None)
    return dbs, conn



def test_backup_filename_timestamp_uses_host_local_timezone(monkeypatch):
    import os
    import time

    if not hasattr(time, "tzset"):
        pytest.skip("Host timezone cannot be changed in this test runtime.")
    previous = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "America/Los_Angeles"
        time.tzset()
        stamp = recovery._backup_time(Path("pifitness_prd_20261111_090000.dump"))
        assert stamp.hour == 17  # 09:00 PST is 17:00 UTC on this date.
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


def test_all_prd_extension_versions_must_be_available_before_destructive_restore(monkeypatch):
    class Cursor:
        def __init__(self):
            self.calls = 0
        def execute(self, query, params=None):
            self.calls += 1
            assert "pg_available_extension_versions" in query
            assert params == ("timescaledb", "2.20.0")
        def fetchone(self):
            return (False,)
        def close(self):
            pass
    class Connection:
        def cursor(self):
            return Cursor()
        def close(self):
            pass
    monkeypatch.setattr(recovery, "_connect", lambda *a, **k: Connection())
    monkeypatch.setattr(recovery, "_read_extension_specs", lambda conn: [
        {"name": "timescaledb", "version": "2.20.0", "schema": "public"}
    ])
    with pytest.raises(recovery.RecoveryError, match="exact version is not available"):
        recovery._load_extension_specs(_config("prod", "personal_fitness", "devuser"), "personal_fitness")


def test_pg_restore_uses_admin_and_ownership_transfer_avoids_cluster_wide_reassign(monkeypatch, tmp_path):
    captured = {}
    class Cursor:
        def execute(self, query, params=None):
            captured.setdefault("sql", []).append(query)
        def close(self):
            pass
    class Connection:
        autocommit = False
        def cursor(self):
            return Cursor()
        def close(self):
            pass
    monkeypatch.setattr(recovery.shutil, "which", lambda name: "/usr/bin/pg_restore")
    monkeypatch.setattr(recovery, "_make_pgpass", lambda directory, config: directory / "pgpass")
    monkeypatch.setattr(recovery, "_connect", lambda *a, **k: Connection())
    monkeypatch.setattr(recovery, "_safe_run", lambda args, **kwargs: captured.update(args=args, env=kwargs["env"]))
    recovery._restore_archive(
        Path("/tmp/prd.dump"), "personal_fitness_test",
        _config("prod", "personal_fitness", "devuser"), temp_dir=tmp_path,
        timescaledb_version=None,
    )
    args = captured["args"]
    assert args[args.index("--username") + 1] == "devuser"
    assert "--role" not in args
    assert "--no-owner" in args and "--no-acl" in args


def test_ownership_provisioning_is_database_scoped_and_never_reassigns_shared_objects(monkeypatch):
    captured = []
    class Cursor:
        def execute(self, query, params=None):
            captured.append(query)
        def close(self):
            pass
    class Connection:
        autocommit = False
        def cursor(self):
            return Cursor()
        def close(self):
            pass
    monkeypatch.setattr(recovery, "_connect", lambda *a, **k: Connection())
    recovery._provision_ownership(
        _config("prod", "personal_fitness", "devuser"),
        "personal_fitness_test", "pifitness_test_app",
        [{"name": "timescaledb", "version": "2.20.0", "schema": "public"}],
    )
    ownership_sql = "\n".join(captured)
    assert "REASSIGN OWNED" not in ownership_sql
    assert "ALTER DATABASE" not in ownership_sql
    assert "pg_depend" in ownership_sql and "deptype = 'e'" in ownership_sql
    assert "ALTER TABLE" in ownership_sql
    assert "ALTER FUNCTION" in ownership_sql
    assert "ALTER LARGE OBJECT" in ownership_sql
    assert "ALTER SCHEMA public OWNER TO \"pifitness_test_app\"" in ownership_sql


def test_old_incomplete_journal_never_drops_a_newer_completed_baseline(tmp_path, monkeypatch):
    dbs, conn = _install_fake_recovery(monkeypatch, original_exists=False)
    dbs["personal_fitness_test"] = {"oid": 99, "owner": "pifitness_test_app", "is_template": False}
    state_dir = tmp_path / "states"
    state_dir.mkdir()
    (state_dir / "test-db-recovery-old.json").write_text(json.dumps({
        "test_database": "personal_fitness_test",
        "prd_database": "personal_fitness",
        "status": "restore_started",
        "new_test_database_oid": 55,
    }))
    (state_dir / "test-db-recovery-current.json").write_text(json.dumps({
        "test_database": "personal_fitness_test",
        "prd_database": "personal_fitness",
        "status": "baseline_ready",
        "new_test_database_oid": 99,
    }))
    monkeypatch.setattr(recovery, "_verify_database", lambda *a, **k: {"marker": "test"})
    result = recovery.recover_pending_states(
        _config("test", "personal_fitness_test", "pifitness_test_app"),
        _config("prod", "personal_fitness", "devuser"),
        str(state_dir),
    )
    assert result["ok"] is True
    assert dbs["personal_fitness_test"]["oid"] == 99
    assert conn.closed

def test_recovery_replaces_bad_test_and_keeps_only_verified_prd_baseline(tmp_path, monkeypatch):
    dbs, conn = _install_fake_recovery(monkeypatch)
    state_path = tmp_path / "recovery.json"
    result = recovery.restore_test_database(
        _config("test", "personal_fitness_test", "pifitness_test_app"),
        _config("prod", "personal_fitness", "devuser"),
        backup_dir=str(tmp_path), backup_glob="*.dump", state_file=str(state_path),
    )
    state = json.loads(state_path.read_text())
    assert result["ok"] is True
    assert result["status"] == "baseline_ready"
    assert result["previous_test_database_discarded"] is True
    assert result["prd_database_identity_unchanged"] is True
    assert state["prd_database_identity_unchanged"] is True
    assert state["status"] == "baseline_ready"
    assert state["previous_test_database_oid"] == 2
    assert state["new_test_database_oid"] == 3
    assert dbs["personal_fitness_test"]["oid"] == 3
    assert set(dbs) == {"personal_fitness", "personal_fitness_test"}
    assert "rollback_database" not in state
    assert state_path.stat().st_mode & 0o777 == 0o600
    assert conn.closed


def test_missing_test_database_is_created_directly_without_snapshot(tmp_path, monkeypatch):
    dbs, _ = _install_fake_recovery(monkeypatch, original_exists=False)
    result = recovery.restore_test_database(
        _config("test", "personal_fitness_test", "pifitness_test_app"),
        _config("prod", "personal_fitness", "devuser"),
        backup_dir=str(tmp_path), backup_glob="*.dump", state_file=str(tmp_path / "recovery.json"),
    )
    assert result["ok"] is True
    assert result["previous_test_database_discarded"] is False
    assert set(dbs) == {"personal_fitness", "personal_fitness_test"}


def test_restore_failure_removes_partial_test_database_and_records_failure(tmp_path, monkeypatch):
    dbs, _ = _install_fake_recovery(monkeypatch, restore_fails=True)
    state_path = tmp_path / "recovery.json"
    with pytest.raises(recovery.RecoveryError, match="partial replacement was removed"):
        recovery.restore_test_database(
            _config("test", "personal_fitness_test", "pifitness_test_app"),
            _config("prod", "personal_fitness", "devuser"),
            backup_dir=str(tmp_path), backup_glob="*.dump", state_file=str(state_path),
        )
    state = json.loads(state_path.read_text())
    assert state["status"] == "restore_failed_test_database_removed"
    assert "prd_database_identity_unchanged" not in state
    assert set(dbs) == {"personal_fitness"}


def test_live_prd_schema_change_after_nightly_backup_does_not_discard_restored_baseline(tmp_path, monkeypatch):
    dbs, _ = _install_fake_recovery(monkeypatch, schema_status="different")
    state_path = tmp_path / "recovery.json"
    result = recovery.restore_test_database(
        _config("test", "personal_fitness_test", "pifitness_test_app"),
        _config("prod", "personal_fitness", "devuser"),
        backup_dir=str(tmp_path), backup_glob="*.dump", state_file=str(state_path),
    )
    state = json.loads(state_path.read_text())
    assert result["status"] == "baseline_ready"
    assert state["status"] == "baseline_ready"
    assert state["schema_comparison"]["status"] == "different"
    assert dbs["personal_fitness_test"]["oid"] == 3


def test_recovery_reconciler_keeps_a_completed_prd_baseline(tmp_path, monkeypatch):
    dbs, _ = _install_fake_recovery(monkeypatch, original_exists=False)
    state_path = tmp_path / "test-db-recovery-baseline.json"
    result = recovery.restore_test_database(
        _config("test", "personal_fitness_test", "pifitness_test_app"),
        _config("prod", "personal_fitness", "devuser"),
        backup_dir=str(tmp_path), backup_glob="*.dump", state_file=str(state_path),
    )
    report = recovery.recover_pending_states(
        _config("test", "personal_fitness_test", "pifitness_test_app"),
        _config("prod", "personal_fitness", "devuser"),
        str(tmp_path),
    )
    assert result["status"] == "baseline_ready"
    assert str(state_path) in report["baseline_ready"]
    assert dbs["personal_fitness_test"]["oid"] == 3


def test_admin_preflight_failure_does_not_drop_existing_test(tmp_path, monkeypatch):
    dbs, _ = _install_fake_recovery(monkeypatch)
    monkeypatch.setattr(recovery, "_catalog_role_is_superuser", lambda cur, role: False)
    state_path = tmp_path / "recovery.json"
    with pytest.raises(recovery.RecoveryError, match="existing TEST database remains untouched"):
        recovery.restore_test_database(
            _config("test", "personal_fitness_test", "pifitness_test_app"),
            _config("prod", "personal_fitness", "devuser"),
            backup_dir=str(tmp_path), backup_glob="*.dump", state_file=str(state_path),
        )
    assert dbs["personal_fitness_test"]["oid"] == 2
    assert json.loads(state_path.read_text())["status"] == "failed_before_database_change"


def test_test_role_must_be_login_enabled_and_non_superuser_before_drop(tmp_path, monkeypatch):
    dbs, _ = _install_fake_recovery(monkeypatch)
    monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
        "can_login": True, "is_superuser": True,
    })
    state_path = tmp_path / "recovery.json"
    with pytest.raises(recovery.RecoveryError, match="must not be a PostgreSQL superuser"):
        recovery.restore_test_database(
            _config("test", "personal_fitness_test", "pifitness_test_app"),
            _config("prod", "personal_fitness", "devuser"),
            backup_dir=str(tmp_path), backup_glob="*.dump", state_file=str(state_path),
        )
    assert dbs["personal_fitness_test"]["oid"] == 2
    assert json.loads(state_path.read_text())["status"] == "failed_before_database_change"


def test_test_role_must_be_login_enabled_before_drop(tmp_path, monkeypatch):
    dbs, _ = _install_fake_recovery(monkeypatch)
    monkeypatch.setattr(recovery, "_role_attributes", lambda cur, role: {
        "can_login": False, "is_superuser": False,
    })
    state_path = tmp_path / "recovery.json"
    with pytest.raises(recovery.RecoveryError, match="cannot log in"):
        recovery.restore_test_database(
            _config("test", "personal_fitness_test", "pifitness_test_app"),
            _config("prod", "personal_fitness", "devuser"),
            backup_dir=str(tmp_path), backup_glob="*.dump", state_file=str(state_path),
        )
    assert dbs["personal_fitness_test"]["oid"] == 2
    assert json.loads(state_path.read_text())["status"] == "failed_before_database_change"


def test_bad_test_password_is_detected_before_dropping_existing_database(tmp_path, monkeypatch):
    dbs, _ = _install_fake_recovery(monkeypatch)
    monkeypatch.setattr(
        recovery, "_test_role_login_preflight",
        lambda *a, **k: (_ for _ in ()).throw(recovery.RecoveryError("TEST credentials rejected")),
    )
    state_path = tmp_path / "recovery.json"
    with pytest.raises(recovery.RecoveryError, match="existing TEST database remains untouched"):
        recovery.restore_test_database(
            _config("test", "personal_fitness_test", "pifitness_test_app"),
            _config("prod", "personal_fitness", "devuser"),
            backup_dir=str(tmp_path), backup_glob="*.dump", state_file=str(state_path),
        )
    assert dbs["personal_fitness_test"]["oid"] == 2
    assert json.loads(state_path.read_text())["status"] == "failed_before_database_change"


def test_test_role_credential_probe_checks_database_user_and_same_loopback_endpoint(monkeypatch):
    class Cursor:
        def __init__(self, row):
            self.row = row
            self.query = None
        def execute(self, query, params=None):
            self.query = (query, params)
        def fetchone(self):
            return self.row
        def close(self):
            pass

    class Connection:
        def __init__(self, row):
            self.row = row
            self.closed = False
        def cursor(self):
            return Cursor(self.row)
        def close(self):
            self.closed = True

    admin = Connection((True,))
    test_conn = Connection(("postgres", "pifitness_test_app", "::1/128", 5432))
    monkeypatch.setattr(recovery, "_connect", lambda config, db: test_conn if db == "postgres" else None)
    result = recovery._test_role_login_preflight(
        _config("test", "personal_fitness_test", "pifitness_test_app"),
        admin, {"server_address": "::1", "server_port": 5432},
    )
    assert result == {"checked": True, "database": "postgres", "user": "pifitness_test_app"}
    assert test_conn.closed is True


def test_test_role_credential_probe_skips_if_role_cannot_connect_to_postgres(monkeypatch):
    class Cursor:
        def execute(self, query, params=None):
            assert "has_database_privilege" in query
        def fetchone(self):
            return (False,)
        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

    monkeypatch.setattr(
        recovery, "_connect",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not connect without CONNECT privilege")),
    )
    result = recovery._test_role_login_preflight(
        _config("test", "personal_fitness_test", "pifitness_test_app"),
        Connection(), {"server_address": "::1", "server_port": 5432},
    )
    assert result["checked"] is False
    assert "verified against TEST after restore" in result["reason"]
