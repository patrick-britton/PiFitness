"""Safety and configuration tests for tools/db/migrate.py.

The runner takes all connection settings from an explicitly selected env file.
DEV and TEST require their matching database marker. PRD must be unmarked and
requires explicit --allow-prd authorization. Target checks must precede DDL,
and status must never write to the database.
"""

import hashlib
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

import db.migrate as migrate


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


def _config(app_env="dev", dbname="fitness_dev", host="localhost", port="5432"):
    return {
        "APP_ENV": app_env,
        "PG_HOST": host,
        "PG_PORT": str(port),
        "PG_DB": dbname,
        "PG_USER": "migration_user",
        "PG_PASSWORD": "migration_password",
        "PGSSLMODE": "disable",
    }


def _write_env(path, **overrides):
    values = {
        "APP_ENV": "dev",
        "PG_HOST": "localhost",
        "PG_PORT": "5432",
        "PG_DB": "fitness_dev",
        "PG_USER": "migration_user",
        "PG_PASSWORD": "migration_password",
        "PGSSLMODE": "disable",
    }
    values.update(overrides)
    path.write_text(
        "\n".join(f"{key}={value}" for key, value in values.items()) + "\n",
        encoding="utf-8",
    )
    return path


@pytest.mark.regression
def test_env_file_is_the_only_source_of_connection_values(tmp_path, monkeypatch):
    env_file = _write_env(
        tmp_path / "migration.env",
        APP_ENV="TEST",
        PG_HOST="LOCALHOST",
        PG_DB="fitness_test_from_file",
        PG_PASSWORD="file-password",
    )
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("PG_DB", "personal_fitness")
    monkeypatch.setenv("PG_PASSWORD", "inherited-password")

    config = migrate._load_env_file(str(env_file))

    assert config["APP_ENV"] == "test"
    assert config["PG_HOST"] == "localhost"
    assert config["PG_DB"] == "fitness_test_from_file"
    assert config["PG_PASSWORD"] == "file-password"


@pytest.mark.regression
def test_env_file_does_not_interpolate_inherited_variables(tmp_path, monkeypatch):
    env_file = _write_env(
        tmp_path / "migration.env",
        PG_PASSWORD="${MIGRATION_SECRET}",
    )
    monkeypatch.setenv("MIGRATION_SECRET", "must-not-be-expanded")

    config = migrate._load_env_file(str(env_file))

    assert config["PG_PASSWORD"] == "${MIGRATION_SECRET}"


@pytest.mark.regression
def test_missing_env_file_is_refused(tmp_path):
    with pytest.raises(RuntimeError, match="does not exist"):
        migrate._load_env_file(str(tmp_path / "missing.env"))


@pytest.mark.regression
def test_missing_required_settings_are_refused(tmp_path):
    env_file = tmp_path / "incomplete.env"
    env_file.write_text(
        "APP_ENV=test\nPG_HOST=localhost\nPG_DB=fitness_test\nPG_USER=runner\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="PG_PASSWORD"):
        migrate._load_env_file(str(env_file))


@pytest.mark.regression
def test_unsupported_environment_is_refused(tmp_path):
    env_file = _write_env(tmp_path / "migration.env", APP_ENV="staging")

    with pytest.raises(RuntimeError, match="Unsupported APP_ENV"):
        migrate._load_env_file(str(env_file))


@pytest.mark.regression
@pytest.mark.parametrize("port", ["not-a-port", "0", "65536"])
def test_invalid_port_is_refused(tmp_path, port):
    env_file = _write_env(tmp_path / "migration.env", PG_PORT=port)

    with pytest.raises(RuntimeError, match="PG_PORT"):
        migrate._load_env_file(str(env_file))


@pytest.mark.regression
def test_missing_port_defaults_to_postgresql_default(tmp_path):
    env_file = _write_env(tmp_path / "migration.env")
    env_file.write_text(
        env_file.read_text(encoding="utf-8").replace("PG_PORT=5432\n", ""),
        encoding="utf-8",
    )

    assert migrate._load_env_file(str(env_file))["PG_PORT"] == "5432"


# ---------------------------------------------------------------------------
# Fake database connection for safety-order and command tests
# ---------------------------------------------------------------------------


class FakeCursor:
    def __init__(
        self,
        *,
        dbname,
        marker,
        port=5432,
        server_addr="127.0.0.1",
        table_exists=False,
        applied=(),
        baseline_exists=False,
        metadata_columns=(),
        ledger_rows=None,
    ):
        self.dbname = dbname
        self.marker = marker
        self.port = port
        self.server_addr = server_addr
        self.table_exists = table_exists
        self.applied = list(applied)
        self.baseline_exists = baseline_exists
        self.metadata_columns = set(metadata_columns)
        self.ledger = dict(ledger_rows or {})
        for version in self.applied:
            self.ledger.setdefault(version, (None, None))
        self.executed = []
        self._one = None
        self._all = []
        self.closed = False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.executed.append((normalized, params))
        self._one = None
        self._all = []

        if normalized.startswith("CREATE TABLE IF NOT EXISTS schema_migrations"):
            self.table_exists = True
            self.metadata_columns.update({"filename", "checksum"})
        elif normalized.startswith("ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS"):
            self.table_exists = True
            if " filename " in " " + normalized + " ":
                self.metadata_columns.add("filename")
            if " checksum " in " " + normalized + " ":
                self.metadata_columns.add("checksum")
        elif "current_database()" in normalized:
            self._one = (self.dbname, self.server_addr, self.port)
        elif "current_setting('pifitness.env', true)" in normalized:
            self._one = (self.marker,)
        elif "to_regclass('schema_migrations')" in normalized:
            self._one = ("schema_migrations",) if self.table_exists else (None,)
        elif "FROM information_schema.columns" in normalized and "schema_migrations" in normalized:
            self._all = [(name,) for name in sorted(self.metadata_columns)]
        elif normalized.startswith("SELECT version, filename, checksum FROM schema_migrations"):
            self._all = [
                (version, self.ledger.get(version, (None, None))[0],
                 self.ledger.get(version, (None, None))[1])
                for version in sorted(self.applied)
            ]
        elif normalized.startswith("SELECT version FROM schema_migrations"):
            self._all = [(version,) for version in self.applied]
        elif normalized.startswith("SELECT 1 FROM schema_migrations"):
            self._one = (0,) if self.baseline_exists else None
        elif normalized.startswith("INSERT INTO schema_migrations") and params:
            if params[0] == 0:
                self.baseline_exists = True
            else:
                self.applied.append(params[0])
                if len(params) >= 3:
                    self.ledger[params[0]] = (params[1], params[2])
        elif normalized.startswith("UPDATE schema_migrations SET filename") and params:
            filename, checksum, version = params
            if version in self.applied:
                self.ledger[version] = (filename, checksum)
        elif normalized.startswith("DELETE FROM schema_migrations") and params:
            self.applied = [value for value in self.applied if value != params[0]]
            self.ledger.pop(params[0], None)

    def fetchone(self):
        return self._one

    def fetchall(self):
        return list(self._all)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self.fake_cursor = cursor
        self.committed = 0
        self.rolled_back = 0
        self.closed = False

    def cursor(self):
        return self.fake_cursor

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        self.closed = True


def _install_fake_connection(
    monkeypatch,
    config,
    marker,
    *,
    actual_dbname=None,
    actual_port=None,
    server_addr="127.0.0.1",
    table_exists=False,
    applied=(),
    baseline_exists=False,
    metadata_columns=(),
    ledger_rows=None,
):
    cursor = FakeCursor(
        dbname=actual_dbname or config["PG_DB"],
        marker=marker,
        port=int(actual_port if actual_port is not None else config["PG_PORT"]),
        server_addr=server_addr,
        table_exists=table_exists,
        applied=applied,
        baseline_exists=baseline_exists,
        metadata_columns=metadata_columns,
        ledger_rows=ledger_rows,
    )
    conn = FakeConnection(cursor)
    connect_calls = []

    def fake_connect(dbname, params):
        connect_calls.append((dbname, dict(params)))
        return conn

    monkeypatch.setattr(migrate, "_connect", fake_connect)
    return conn, cursor, connect_calls


# ---------------------------------------------------------------------------
# Environment and marker guards
# ---------------------------------------------------------------------------


@pytest.mark.regression
@pytest.mark.parametrize(
    "app_env,marker",
    [("dev", "dev"), ("test", "test"), ("prod", None)],
)
def test_matching_environment_marker_is_accepted(app_env, marker):
    migrate._guard(app_env, marker)


@pytest.mark.regression
@pytest.mark.parametrize(
    "app_env,marker",
    [("dev", None), ("dev", "test"), ("test", None), ("test", "dev")],
)
def test_dev_and_test_require_their_exact_marker(app_env, marker):
    with pytest.raises(RuntimeError):
        migrate._guard(app_env, marker)


@pytest.mark.regression
@pytest.mark.parametrize("marker", ["dev", "test", "unexpected"])
def test_prd_must_remain_unmarked(marker):
    with pytest.raises(RuntimeError, match="PRD must remain unmarked"):
        migrate._guard("prod", marker)


@pytest.mark.regression
def test_unknown_app_env_is_refused():
    with pytest.raises(RuntimeError, match="Unsupported APP_ENV"):
        migrate._guard("", None)


@pytest.mark.regression
def test_prd_requires_explicit_authorization_before_connecting(monkeypatch):
    config = _config("prod", "personal_fitness")
    connect_called = False

    def forbidden_connect(*args, **kwargs):
        nonlocal connect_called
        connect_called = True
        raise AssertionError("connection must not be attempted")

    monkeypatch.setattr(migrate, "_connect", forbidden_connect)

    with pytest.raises(RuntimeError, match="--allow-prd"):
        migrate._open_validated_connection(config)
    assert not connect_called


@pytest.mark.regression
def test_prd_with_explicit_authorization_and_no_marker_can_connect(monkeypatch):
    config = _config("prod", "personal_fitness")
    conn, _, calls = _install_fake_connection(monkeypatch, config, None)

    opened_conn, cursor = migrate._open_validated_connection(
        config, allow_prd=True
    )

    assert opened_conn is conn
    assert len(calls) == 1
    cursor.close()
    opened_conn.close()


@pytest.mark.regression
def test_allow_prd_is_rejected_for_dev_and_test():
    for app_env, dbname in (("dev", "fitness_dev"), ("test", "fitness_test")):
        config = _config(app_env, dbname)
        with pytest.raises(RuntimeError, match="only with APP_ENV=prod"):
            migrate._validate_target(
                config, migrate._conn_params(config), allow_prd=True
            )


@pytest.mark.regression
def test_remote_host_is_refused_before_connecting(monkeypatch):
    config = _config("test", "fitness_test", host="10.0.0.5")
    connect_called = False

    def forbidden_connect(*args, **kwargs):
        nonlocal connect_called
        connect_called = True
        raise AssertionError("remote connection must not be attempted")

    monkeypatch.setattr(migrate, "_connect", forbidden_connect)

    with pytest.raises(RuntimeError, match="remote host"):
        migrate._open_validated_connection(config)
    assert not connect_called


# ---------------------------------------------------------------------------
# Actual PostgreSQL identity checks
# ---------------------------------------------------------------------------


@pytest.mark.regression
def test_connection_identity_accepts_matching_loopback_database_and_port():
    cursor = FakeCursor(
        dbname="fitness_test",
        marker="test",
        port=5432,
        server_addr="127.0.0.1",
    )
    cursor.execute = lambda sql: setattr(
        cursor, "_one", ("fitness_test", "127.0.0.1", 5432)
    )

    migrate._validate_connection_identity(
        cursor,
        "fitness_test",
        {"port": 5432},
    )


@pytest.mark.regression
@pytest.mark.parametrize(
    "dbname,server_addr,port,match",
    [
        ("wrong_db", "127.0.0.1", 5432, "database"),
        ("fitness_test", "192.168.1.20", 5432, "not loopback"),
        ("fitness_test", None, 5432, "did not report a TCP"),
        ("fitness_test", "127.0.0.1", 15432, "port"),
    ],
)
def test_connection_identity_rejects_wrong_database_or_endpoint(
    dbname, server_addr, port, match
):
    cursor = FakeCursor(
        dbname=dbname,
        marker="test",
        port=port,
        server_addr=server_addr,
    )

    def result_for_identity(sql):
        cursor._one = (dbname, server_addr, port)

    cursor.execute = result_for_identity
    with pytest.raises(RuntimeError, match=match):
        migrate._validate_connection_identity(
            cursor,
            "fitness_test",
            {"port": 5432},
        )


@pytest.mark.regression
def test_open_connection_checks_identity_and_marker_before_returning(monkeypatch):
    config = _config("test", "fitness_test")
    conn, cursor, _ = _install_fake_connection(monkeypatch, config, "test")

    returned_conn, returned_cursor = migrate._open_validated_connection(config)

    statements = [sql for sql, _ in cursor.executed]
    assert "current_database(), inet_server_addr(), inet_server_port()" in statements[0]
    assert "current_setting('pifitness.env', true)" in statements[1]
    assert returned_conn is conn
    assert returned_cursor is cursor


@pytest.mark.regression
def test_failed_marker_check_closes_connection_before_raising(monkeypatch):
    config = _config("test", "fitness_test")
    conn, cursor, _ = _install_fake_connection(monkeypatch, config, "dev")

    with pytest.raises(RuntimeError, match="marker"):
        migrate._open_validated_connection(config)

    assert cursor.closed
    assert conn.closed


@pytest.mark.regression
def test_wrong_actual_database_is_rejected_before_marker_or_ddl(monkeypatch):
    config = _config("test", "fitness_test")
    conn, cursor, _ = _install_fake_connection(
        monkeypatch,
        config,
        "test",
        actual_dbname="personal_fitness",
    )

    with pytest.raises(RuntimeError, match="expected 'fitness_test'"):
        migrate._open_validated_connection(config)

    statements = [sql for sql, _ in cursor.executed]
    assert len(statements) == 1
    assert cursor.closed
    assert conn.closed


# ---------------------------------------------------------------------------
# Command behavior and DDL ordering
# ---------------------------------------------------------------------------


@pytest.mark.regression
def test_status_is_read_only_when_migration_table_is_missing(
    monkeypatch, tmp_path, capsys
):
    config = _config("test", "fitness_test")
    conn, cursor, _ = _install_fake_connection(
        monkeypatch, config, "test", table_exists=False
    )
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    (tmp_path / "001__first.sql").write_text("-- migrate:up\nSELECT 1;\n")
    (tmp_path / "README.txt").write_text("not a migration")

    migrate.cmd_status(config)

    statements = [sql.upper() for sql, _ in cursor.executed]
    assert any("TO_REGCLASS('SCHEMA_MIGRATIONS')" in sql for sql in statements)
    assert not any("CREATE TABLE" in sql for sql in statements)
    assert not any("INSERT INTO" in sql or "DELETE FROM" in sql for sql in statements)
    assert "pending" in capsys.readouterr().out
    assert cursor.closed
    assert conn.closed


@pytest.mark.regression
def test_status_reads_applied_versions_without_writing(
    monkeypatch, tmp_path, capsys
):
    config = _config("dev", "fitness_dev")
    conn, cursor, _ = _install_fake_connection(
        monkeypatch,
        config,
        "dev",
        table_exists=True,
        applied=(1,),
    )
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    (tmp_path / "001__first.sql").write_text("-- migrate:up\nSELECT 1;\n")
    (tmp_path / "002__second.sql").write_text("-- migrate:up\nSELECT 2;\n")

    migrate.cmd_status(config)

    output = capsys.readouterr().out
    assert "001__first.sql" in output and "applied" in output
    assert "002__second.sql" in output and "pending" in output
    assert not any("CREATE TABLE" in sql.upper() for sql, _ in cursor.executed)
    assert not any("INSERT INTO" in sql.upper() for sql, _ in cursor.executed)
    assert cursor.closed
    assert conn.closed


@pytest.mark.regression
def test_up_checks_identity_and_marker_before_creating_migration_table(
    monkeypatch, tmp_path
):
    config = _config("test", "fitness_test")
    conn, cursor, _ = _install_fake_connection(monkeypatch, config, "test")
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))

    migrate.cmd_up(config)

    statements = [sql for sql, _ in cursor.executed]
    identity_index = next(i for i, sql in enumerate(statements) if "current_database()" in sql)
    marker_index = next(i for i, sql in enumerate(statements) if "current_setting('pifitness.env', true)" in sql)
    ddl_index = next(i for i, sql in enumerate(statements) if "CREATE TABLE" in sql)
    assert identity_index < marker_index < ddl_index
    assert conn.committed >= 1
    assert cursor.closed
    assert conn.closed


@pytest.mark.regression
def test_baseline_checks_target_before_creating_migration_table(monkeypatch, tmp_path):
    config = _config("dev", "fitness_dev")
    conn, cursor, _ = _install_fake_connection(monkeypatch, config, "dev")
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))

    migrate.cmd_baseline(config)

    statements = [sql for sql, _ in cursor.executed]
    marker_index = next(i for i, sql in enumerate(statements) if "current_setting('pifitness.env', true)" in sql)
    ddl_index = next(i for i, sql in enumerate(statements) if "CREATE TABLE" in sql)
    assert marker_index < ddl_index
    assert cursor.closed
    assert conn.closed


# ---------------------------------------------------------------------------
# CLI contract
# ---------------------------------------------------------------------------


@pytest.mark.regression
def test_cli_requires_explicit_env_file():
    parser = migrate._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["status"])


@pytest.mark.regression
def test_cli_loads_selected_env_file_and_passes_prd_authorization(
    monkeypatch, tmp_path
):
    env_file = _write_env(
        tmp_path / "prd-migrate.env",
        APP_ENV="prod",
        PG_DB="personal_fitness",
    )
    observed = {}

    def fake_status(config, allow_prd=False, output_format="text"):
        observed["config"] = config
        observed["allow_prd"] = allow_prd
        observed["output_format"] = output_format

    monkeypatch.setattr(migrate, "cmd_status", fake_status)

    result = migrate.main(
        ["status", "--env-file", str(env_file), "--allow-prd"]
    )

    assert result == 0
    assert observed["config"]["APP_ENV"] == "prod"
    assert observed["config"]["PG_DB"] == "personal_fitness"
    assert observed["allow_prd"] is True
    assert observed["output_format"] == "text"


@pytest.mark.regression
def test_cli_refuses_prd_without_allow_flag(tmp_path, capsys):
    env_file = _write_env(
        tmp_path / "prd-migrate.env",
        APP_ENV="prod",
        PG_DB="personal_fitness",
    )

    result = migrate.main(["status", "--env-file", str(env_file)])

    assert result == 1
    assert "--allow-prd" in capsys.readouterr().err


@pytest.mark.regression
def test_cli_refuses_allow_prd_for_test(tmp_path, capsys):
    env_file = _write_env(
        tmp_path / "test-migrate.env",
        APP_ENV="test",
        PG_DB="fitness_test",
    )

    result = migrate.main(
        ["status", "--env-file", str(env_file), "--allow-prd"]
    )

    assert result == 1
    assert "only with APP_ENV=prod" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Structured output, per-file failure attribution, and checksum integrity
# ---------------------------------------------------------------------------


@pytest.mark.regression
def test_jsonl_status_emits_one_machine_readable_record_per_migration(
    monkeypatch, tmp_path, capsys
):
    config = _config("test", "fitness_test")
    _, cursor, _ = _install_fake_connection(monkeypatch, config, "test")
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    (tmp_path / "001__first.sql").write_text("-- migrate:up\nSELECT 1;\n")
    (tmp_path / "002__second.sql").write_text("-- migrate:up\nSELECT 2;\n")

    migrate.cmd_status(config, output_format="jsonl")

    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [event["event"] for event in events] == [
        "migration_status", "migration_status", "migration_summary"
    ]
    assert events[0]["file"] == "001__first.sql"
    assert events[0]["status"] == "pending"
    assert events[1]["file"] == "002__second.sql"
    assert events[1]["status"] == "pending"
    assert events[-1]["pending"] == 2
    assert events[-1]["ok"] is True
    assert not any("CREATE TABLE" in sql.upper() for sql, _ in cursor.executed)




@pytest.mark.regression
def test_checksum_is_stable_across_lf_and_crlf_files(tmp_path):
    lf_path = tmp_path / "lf.sql"
    crlf_path = tmp_path / "crlf.sql"
    content = "-- migrate:up\nSELECT 1;\n"
    lf_path.write_bytes(content.encode("utf-8"))
    crlf_path.write_bytes(content.replace("\n", "\r\n").encode("utf-8"))

    assert migrate._sha256_file(str(lf_path)) == migrate._sha256_file(str(crlf_path))
    assert migrate._sha256_file(str(lf_path)) == hashlib.sha256(content.encode("utf-8")).hexdigest()


@pytest.mark.regression
def test_status_detects_applied_file_checksum_mismatch(
    monkeypatch, tmp_path, capsys
):
    config = _config("test", "fitness_test")
    content = "-- migrate:up\nSELECT 1;\n"
    current_checksum = hashlib.sha256(content.encode()).hexdigest()
    _, _, _ = _install_fake_connection(
        monkeypatch,
        config,
        "test",
        table_exists=True,
        applied=(1,),
        metadata_columns=("filename", "checksum"),
        ledger_rows={1: ("001__first.sql", "0" * 64)},
    )
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    (tmp_path / "001__first.sql").write_text(content)

    with pytest.raises(migrate.MigrationIntegrityError, match="integrity check failed"):
        migrate.cmd_status(config, output_format="jsonl")

    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert events[0]["file"] == "001__first.sql"
    assert events[0]["integrity"] == "checksum_mismatch"
    assert events[-1]["integrity_errors"] == [
        {"version": 1, "file": "001__first.sql"}
    ]
    assert events[-1]["ok"] is False
    assert len(current_checksum) == 64


@pytest.mark.regression
def test_up_emits_started_and_succeeded_events_and_persists_checksum(
    monkeypatch, tmp_path, capsys
):
    config = _config("test", "fitness_test")
    conn, cursor, _ = _install_fake_connection(monkeypatch, config, "test")
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    content = "-- migrate:up\nSELECT 1;\n"
    filepath = tmp_path / "001__first.sql"
    filepath.write_text(content)
    # Path.write_text() uses the platform newline convention. The runner
    # normalizes CRLF/CR to LF before hashing so Windows and Linux agree.
    checksum = hashlib.sha256(content.replace("\r\n", "\n").replace("\r", "\n").encode()).hexdigest()

    migrate.cmd_up(config, output_format="jsonl")

    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [event["event"] for event in events] == [
        "migration_started", "migration_succeeded", "migration_summary"
    ]
    assert events[0]["file"] == "001__first.sql"
    assert events[1]["checksum"] == checksum
    assert cursor.ledger[1] == ("001__first.sql", checksum)
    assert conn.committed >= 2


@pytest.mark.regression
def test_up_failure_reports_exact_file_version_and_sqlstate(
    monkeypatch, tmp_path, capsys
):
    config = _config("test", "fitness_test")
    conn, cursor, _ = _install_fake_connection(monkeypatch, config, "test")
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    filepath = tmp_path / "007__failing.sql"
    filepath.write_text("-- migrate:up\nSELECT cause_failure;\n")
    original_execute = cursor.execute

    class SimulatedPostgresError(Exception):
        pgcode = "42P01"

    def failing_execute(sql, params=None):
        if "SELECT cause_failure" in " ".join(sql.split()):
            cursor.executed.append(("SIMULATED MIGRATION SQL", params))
            raise SimulatedPostgresError("relation does not exist")
        return original_execute(sql, params)

    monkeypatch.setattr(cursor, "execute", failing_execute)

    with pytest.raises(migrate.MigrationExecutionError) as exc_info:
        migrate.cmd_up(config, output_format="jsonl")

    assert exc_info.value.version == 7
    assert exc_info.value.filename == "007__failing.sql"
    assert exc_info.value.sqlstate == "42P01"
    assert conn.rolled_back == 1
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    failed = next(event for event in events if event["event"] == "migration_failed")
    assert failed["version"] == 7
    assert failed["file"] == "007__failing.sql"
    assert failed["sqlstate"] == "42P01"
    assert events[-1]["failed_file"] == "007__failing.sql"
    assert events[-1]["ok"] is False


@pytest.mark.regression
def test_up_rejects_missing_up_section_instead_of_silently_skipping(
    monkeypatch, tmp_path, capsys
):
    config = _config("test", "fitness_test")
    _, _, _ = _install_fake_connection(monkeypatch, config, "test")
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    (tmp_path / "008__missing_up.sql").write_text("-- migrate:down\nSELECT 1;\n")

    with pytest.raises(migrate.MigrationExecutionError, match="missing or empty"):
        migrate.cmd_up(config, output_format="jsonl")

    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    failed = next(event for event in events if event["event"] == "migration_failed")
    assert failed["file"] == "008__missing_up.sql"


@pytest.mark.regression
def test_up_requires_explicit_adoption_for_legacy_applied_rows(
    monkeypatch, tmp_path
):
    config = _config("test", "fitness_test")
    _, _, _ = _install_fake_connection(
        monkeypatch,
        config,
        "test",
        table_exists=True,
        applied=(1,),
    )
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    (tmp_path / "001__first.sql").write_text("-- migrate:up\nSELECT 1;\n")

    with pytest.raises(migrate.MigrationIntegrityError, match="--adopt-legacy-checksums"):
        migrate.cmd_up(config)


@pytest.mark.regression
def test_up_can_explicitly_adopt_legacy_checksums(
    monkeypatch, tmp_path, capsys
):
    config = _config("test", "fitness_test")
    _, cursor, _ = _install_fake_connection(
        monkeypatch,
        config,
        "test",
        table_exists=True,
        applied=(1,),
    )
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    content = "-- migrate:up\nSELECT 1;\n"
    (tmp_path / "001__first.sql").write_text(content)

    migrate.cmd_up(config, adopt_legacy_checksums=True, output_format="jsonl")

    expected = hashlib.sha256(content.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")).hexdigest()
    assert cursor.ledger[1] == ("001__first.sql", expected)
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert any(event["event"] == "migration_legacy_checksum_adopted" for event in events)


@pytest.mark.regression
def test_duplicate_migration_versions_are_rejected(monkeypatch, tmp_path):
    config = _config("test", "fitness_test")
    _, _, _ = _install_fake_connection(monkeypatch, config, "test")
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    (tmp_path / "001__first.sql").write_text("-- migrate:up\nSELECT 1;\n")
    (tmp_path / "001__second.sql").write_text("-- migrate:up\nSELECT 2;\n")

    with pytest.raises(RuntimeError, match="Duplicate migration version 1"):
        migrate.cmd_status(config)


@pytest.mark.regression
def test_up_emits_structured_integrity_error_and_runs_no_migrations(
    monkeypatch, tmp_path, capsys
):
    config = _config("test", "fitness_test")
    _, cursor, _ = _install_fake_connection(
        monkeypatch,
        config,
        "test",
        table_exists=True,
        applied=(1,),
        metadata_columns=("filename", "checksum"),
        ledger_rows={1: ("001__first.sql", "0" * 64)},
    )
    monkeypatch.setattr(migrate, "MIGRATIONS_DIR", str(tmp_path))
    (tmp_path / "001__first.sql").write_text("-- migrate:up\nSELECT 1;\n")
    (tmp_path / "002__second.sql").write_text("-- migrate:up\nSELECT 2;\n")

    with pytest.raises(migrate.MigrationIntegrityError):
        migrate.cmd_up(config, output_format="jsonl")

    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    issue = next(event for event in events if event["event"] == "migration_integrity_error")
    assert issue["version"] == 1
    assert issue["file"] == "001__first.sql"
    assert events[-1]["ok"] is False
    assert not any("SELECT 2" in sql for sql, _ in cursor.executed)
    assert 2 not in cursor.applied
