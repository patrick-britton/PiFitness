"""Unit tests for the MigraDiff-backed PiFitness schema comparison wrapper."""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from db import compare_schema


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.closed = False

    def execute(self, _sql):
        pass

    def fetchone(self):
        return self.row

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, row):
        self.fake_cursor = FakeCursor(row)
        self.closed = False

    def cursor(self):
        return self.fake_cursor

    def close(self):
        self.closed = True


def _config(env, db, *, host="localhost", port="5432", user=None, password="not-for-logging"):
    return {
        "APP_ENV": env,
        "PG_DB": db,
        "PG_HOST": host,
        "PG_PORT": str(port),
        "PG_USER": user or ("test_migrator" if env == "test" else "prd_migrator"),
        "PG_PASSWORD": password,
        "PGSSLMODE": "disable",
    }


def _install_identity_stubs(monkeypatch, *, test_marker="test", prd_marker=None):
    identities = {
        "fitness_test": ("fitness_test", "127.0.0.1", 5432, test_marker),
        "fitness_prd": ("fitness_prd", "127.0.0.1", 5432, prd_marker),
    }
    opened = []

    def fake_open(config, allow_prd=False):
        assert allow_prd is (config["APP_ENV"] == "prod")
        conn = FakeConnection(identities[config["PG_DB"]])
        opened.append(conn)
        return conn, conn.cursor()

    monkeypatch.setattr(
        compare_schema.migration_runner, "_open_validated_connection", fake_open
    )
    return opened


def _install_migradiff(monkeypatch, *, statements=None, stdout=None, returncode=0, stderr=""):
    statements = statements or []
    if stdout is None:
        stdout = json.dumps(
            {
                "version": "1.0",
                "source": "postgresql://test_migrator@localhost:5432/fitness_test",
                "target": "postgresql://prd_migrator@localhost:5432/fitness_prd",
                "generated_at": "2026-11-11T00:00:00Z",
                "summary": {
                    "total_statements": len(statements),
                    "has_destructive_operations": any(
                        item.get("risk") == "destructive" for item in statements
                    ),
                    "risk_level": "high" if any(
                        item.get("risk") == "destructive" for item in statements
                    ) else "low",
                },
                "statements": statements,
            }
        )

    class Result:
        def __init__(self):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        captured["pgpass_contents"] = Path(kwargs["env"]["PGPASSFILE"]).read_text(
            encoding="utf-8"
        )
        captured["pgpass_mode"] = (
            os.stat(kwargs["env"]["PGPASSFILE"]).st_mode & 0o777
            if os.name != "nt"
            else None
        )
        return Result()

    monkeypatch.setattr(compare_schema.importlib.metadata, "version", lambda _name: "1.7.2")
    monkeypatch.setattr(compare_schema, "_resolve_migradiff_executable", lambda _path=None: "/venv/bin/migra")
    monkeypatch.setattr(compare_schema.subprocess, "run", fake_run)
    return captured


@pytest.mark.unit
def test_validate_pair_requires_test_and_prod_envs_and_distinct_databases(monkeypatch):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch)
    test_id, prd_id = compare_schema._validate_pair(test, prd)
    assert test_id["marker"] == "test"
    assert prd_id["marker"] is None

    with pytest.raises(compare_schema.SchemaComparisonError, match="APP_ENV=test"):
        compare_schema._validate_pair(_config("dev", "fitness_test"), prd)
    with pytest.raises(compare_schema.SchemaComparisonError, match="same database"):
        compare_schema._validate_pair(test, _config("prod", "fitness_test"))


@pytest.mark.unit
def test_validate_pair_rejects_wrong_or_missing_markers(monkeypatch):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch, test_marker="dev")
    with pytest.raises(RuntimeError, match="marker"):
        compare_schema._validate_pair(test, prd)

    _install_identity_stubs(monkeypatch, prd_marker="prd")
    with pytest.raises(RuntimeError, match="marker"):
        compare_schema._validate_pair(test, prd)


@pytest.mark.unit
def test_validate_pair_requires_same_configured_and_actual_endpoint(monkeypatch):
    test = _config("test", "fitness_test", port=5432)
    prd = _config("prod", "fitness_prd", port=5433)
    with pytest.raises(compare_schema.SchemaComparisonError, match="same local host and port"):
        compare_schema._validate_pair(test, prd)

    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch)
    identities = iter([
        {"environment": "test", "database": "fitness_test", "server_address": "127.0.0.1", "server_port": 5432, "marker": "test"},
        {"environment": "prod", "database": "fitness_prd", "server_address": "::1", "server_port": 5432, "marker": None},
    ])
    monkeypatch.setattr(compare_schema, "_connection_identity", lambda *a, **k: next(identities))
    with pytest.raises(compare_schema.SchemaComparisonError, match="different PostgreSQL server endpoints"):
        compare_schema._validate_pair(test, prd)


@pytest.mark.unit
def test_validate_pair_rejects_non_loopback_hosts():
    test = _config("test", "fitness_test", host="db.example.com")
    prd = _config("prod", "fitness_prd", host="db.example.com")
    with pytest.raises(RuntimeError, match="remote host"):
        compare_schema._validate_pair(test, prd)


@pytest.mark.unit
def test_pgpass_escaping_and_database_url_never_include_password():
    config = _config("test", "fitness:test", user="name:with\\colon", password="p:a\\ss")
    entry = compare_schema._pgpass_entry(config)
    assert r"fitness\:test" in entry
    assert r"name\:with\\colon" in entry
    assert r"p\:a\\ss" in entry
    url = compare_schema._database_url(config)
    assert "p:a" not in url
    assert "name%3Awith%5Ccolon" in url
    assert "fitness%3Atest" in url


@pytest.mark.unit
def test_database_url_brackets_ipv6_and_includes_sslmode():
    config = _config("test", "fitness_test", host="::1")
    url = compare_schema._database_url(config)
    assert "@[::1]:5432/" in url
    assert "sslmode=disable" in url


@pytest.mark.unit
def test_compare_identical_schemas_uses_migradiff_json_without_applying_sql(monkeypatch):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch)
    captured = _install_migradiff(monkeypatch)

    report = compare_schema.compare_databases(test, prd)

    assert report["ok"] is True
    assert report["status"] == "identical"
    assert report["statement_count"] == 0
    assert report["comparison_engine"] == "migradiff"
    command = captured["command"]
    assert command[:4] == ["/venv/bin/migra", "--output", "json", "--force-destructive"]
    assert "--apply" not in command
    assert "not-for-logging" not in " ".join(command)
    assert captured["kwargs"]["env"]["PGCONNECT_TIMEOUT"] == "5"
    assert captured["kwargs"]["env"]["PGOPTIONS"] == "-c default_transaction_read_only=on"
    assert "PGPASSWORD" not in captured["kwargs"]["env"]
    assert "PGHOST" not in captured["kwargs"]["env"]
    assert "test_migrator" in captured["pgpass_contents"]
    assert "prd_migrator" in captured["pgpass_contents"]
    assert "not-for-logging" in captured["pgpass_contents"]
    if os.name != "nt":
        assert captured["pgpass_mode"] == 0o600
    assert not os.path.exists(captured["kwargs"]["env"]["PGPASSFILE"])


@pytest.mark.unit
def test_compare_structural_change_reports_different_and_risk(monkeypatch):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch)
    _install_migradiff(
        monkeypatch,
        statements=[
            {
                "sql": 'ALTER TABLE "public"."example" ALTER COLUMN "id" TYPE bigint;',
                "type": "ALTER TABLE",
                "operation": "ALTER",
                "object": '"public"."example"',
                "risk": "warning",
            }
        ],
    )
    report = compare_schema.compare_databases(test, prd)
    assert report["status"] == "different"
    assert report["statement_count"] == 1
    assert report["risk_level"] == "medium"
    assert "ALTER TABLE" in report["diff_preview"][0]


@pytest.mark.unit
def test_ledger_and_comments_are_excluded_from_structural_compatibility(monkeypatch):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch)
    _install_migradiff(
        monkeypatch,
        statements=[
            {
                "sql": 'CREATE TABLE "public"."schema_migrations" (version bigint);',
                "type": "CREATE TABLE",
                "operation": "CREATE",
                "object": '"public"."schema_migrations"',
                "risk": "safe",
            },
            {
                "sql": 'COMMENT ON TABLE "public"."example" IS \'description\';',
                "type": "COMMENT",
                "operation": "COMMENT",
                "object": "",
                "risk": "safe",
            },
        ],
    )
    report = compare_schema.compare_databases(test, prd)
    assert report["status"] == "identical"
    assert report["statement_count"] == 0
    assert report["ignored_statement_count"] == 2


@pytest.mark.unit
def test_diff_file_is_private_json_and_never_overwritten(monkeypatch, tmp_path):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch)
    _install_migradiff(
        monkeypatch,
        statements=[
            {"sql": 'ALTER TABLE "public"."example" ADD COLUMN "x" integer;', "risk": "safe"}
        ],
    )
    target = tmp_path / "schema-diff.json"
    report = compare_schema.compare_databases(test, prd, diff_file=str(target))
    assert report["diff_file"] == str(target)
    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload["report"]["status"] == "different"
    assert payload["migradiff"]["statements"]
    if os.name != "nt":
        assert target.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        compare_schema._write_diff_file(str(target), {"replacement": True})


@pytest.mark.unit
def test_migradiff_nonzero_exit_fails_closed_and_redacts_password(monkeypatch):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd", password="prd-secret")
    _install_identity_stubs(monkeypatch)
    _install_migradiff(monkeypatch, returncode=3, stdout="", stderr="connect failed prd-secret")
    with pytest.raises(compare_schema.SchemaComparisonError, match="connect failed") as exc:
        compare_schema.compare_databases(test, prd)
    assert "prd-secret" not in str(exc.value)
    assert "<redacted>" in str(exc.value)


@pytest.mark.unit
def test_invalid_migradiff_json_fails_closed(monkeypatch):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch)
    _install_migradiff(monkeypatch, stdout="not json")
    with pytest.raises(compare_schema.SchemaComparisonError, match="valid JSON"):
        compare_schema.compare_databases(test, prd)


@pytest.mark.unit
def test_malformed_statement_list_fails_closed(monkeypatch):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch)
    _install_migradiff(monkeypatch, stdout=json.dumps({"statements": ["not-an-object"]}))
    with pytest.raises(compare_schema.SchemaComparisonError, match="malformed statement"):
        compare_schema.compare_databases(test, prd)


@pytest.mark.unit
def test_migradiff_version_must_match_the_pinned_release(monkeypatch):
    test = _config("test", "fitness_test")
    prd = _config("prod", "fitness_prd")
    _install_identity_stubs(monkeypatch)
    _install_migradiff(monkeypatch)
    monkeypatch.setattr(compare_schema.importlib.metadata, "version", lambda _name: "1.7.1")
    with pytest.raises(compare_schema.SchemaComparisonError, match="pinned to 1.7.2"):
        compare_schema.compare_databases(test, prd)


@pytest.mark.unit
def test_missing_migradiff_fails_closed(monkeypatch):
    _install_identity_stubs(monkeypatch)
    monkeypatch.setattr(compare_schema.importlib.metadata, "version", lambda _name: "1.7.2")
    monkeypatch.setattr(compare_schema, "_resolve_migradiff_executable", lambda _path=None: (_ for _ in ()).throw(compare_schema.SchemaComparisonError("MigraDiff command not found")))
    with pytest.raises(compare_schema.SchemaComparisonError, match="not found"):
        compare_schema.compare_databases(_config("test", "fitness_test"), _config("prod", "fitness_prd"))


@pytest.mark.unit
def test_cli_defaults_to_existing_application_env_files():
    args = compare_schema._build_parser().parse_args(["--allow-prd"])

    assert args.test_env_file == compare_schema.migration_runner.DEFAULT_TEST_ENV_FILE
    assert args.prd_env_file == compare_schema.migration_runner.DEFAULT_PRD_ENV_FILE



@pytest.mark.unit
def test_cli_requires_explicit_prd_authorization():
    assert compare_schema.main([
        "--test-env-file", "unused-test.env",
        "--prd-env-file", "unused-prd.env",
    ]) == 1


@pytest.mark.unit
def test_cli_error_output_is_machine_readable(monkeypatch, capsys):
    monkeypatch.setattr(compare_schema.migration_runner, "_load_env_file", lambda _path: _config("test", "fitness_test"))
    monkeypatch.setattr(compare_schema, "compare_databases", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("test-failure")))
    code = compare_schema.main([
        "--test-env-file", "unused-test.env",
        "--prd-env-file", "unused-prd.env",
        "--allow-prd",
    ])
    assert code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["status"] == "error"
    assert "test-failure" in payload["error"]
