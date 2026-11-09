"""Guard tests for tools/db/migrate.py environment and marker rules.

DEV migration succeeds only with the DEV marker; TEST migration succeeds only
with explicit APP_ENV=test plus the TEST marker; PRD, unmarked, remote, and
mismatched targets are refused.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

import pytest

import db.migrate as migrate


class FakeCursor:
    def __init__(self, marker):
        self.marker = marker
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        for sql in reversed(self.executed):
            if "pifitness.env" in sql:
                return (self.marker,)
        return None

    def fetchall(self):
        return []

    def close(self):
        pass


class FakeConn:
    def __init__(self, marker):
        self.marker = marker
        self.closed = False
        self.committed = False

    def cursor(self):
        return FakeCursor(self.marker)

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def _install_fake_conn(monkeypatch, marker):
    conn = FakeConn(marker)
    monkeypatch.setattr(migrate, "_connect", lambda dbname: conn)
    return conn


@pytest.mark.regression
def test_dev_status_succeeds_with_dev_marker(monkeypatch):
    """APP_ENV=dev against pifitness_dev with marker dev runs normally."""
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("PG_DB", "pifitness_dev")
    monkeypatch.setenv("PG_HOST", "localhost")
    _install_fake_conn(monkeypatch, "dev")
    migrate.cmd_status("dev")  # must not raise


@pytest.mark.regression
def test_dev_status_refuses_test_marker(monkeypatch):
    """APP_ENV=dev against a database marked test is refused."""
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("PG_DB", "pifitness_dev")
    monkeypatch.setenv("PG_HOST", "localhost")
    _install_fake_conn(monkeypatch, "test")
    with pytest.raises(RuntimeError, match="marker is 'test'"):
        migrate.cmd_status("dev")


@pytest.mark.regression
def test_dev_status_refuses_unmarked(monkeypatch):
    """APP_ENV=dev against an unmarked database is refused."""
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("PG_DB", "pifitness_dev")
    monkeypatch.setenv("PG_HOST", "localhost")
    _install_fake_conn(monkeypatch, None)
    with pytest.raises(RuntimeError, match="unmarked"):
        migrate.cmd_status("dev")


@pytest.mark.regression
def test_test_status_succeeds_with_test_marker(monkeypatch):
    """APP_ENV=test against pifitness_test with marker test runs normally."""
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("PG_DB", "pifitness_test")
    monkeypatch.setenv("PG_HOST", "localhost")
    _install_fake_conn(monkeypatch, "test")
    migrate.cmd_status("test")  # must not raise


@pytest.mark.regression
def test_test_status_refuses_dev_marker(monkeypatch):
    """APP_ENV=test against a database marked dev is refused."""
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("PG_DB", "pifitness_test")
    monkeypatch.setenv("PG_HOST", "localhost")
    _install_fake_conn(monkeypatch, "dev")
    with pytest.raises(RuntimeError, match="marker is 'dev'"):
        migrate.cmd_status("test")


@pytest.mark.regression
def test_test_status_refuses_unmarked(monkeypatch):
    """APP_ENV=test against an unmarked database is refused."""
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("PG_DB", "pifitness_test")
    monkeypatch.setenv("PG_HOST", "localhost")
    _install_fake_conn(monkeypatch, None)
    with pytest.raises(RuntimeError, match="unmarked"):
        migrate.cmd_status("test")


@pytest.mark.regression
def test_prd_unset_refused(monkeypatch):
    """APP_ENV unset (PRD) is refused."""
    monkeypatch.delenv("APP_ENV", raising=False)
    monkeypatch.setenv("PG_DB", "pifitness_dev")
    _install_fake_conn(monkeypatch, None)
    with pytest.raises(RuntimeError, match="not permitted for APP_ENV"):
        migrate.cmd_status("")


@pytest.mark.regression
def test_prod_refused(monkeypatch):
    """APP_ENV=prod is refused."""
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("PG_DB", "pifitness_dev")
    _install_fake_conn(monkeypatch, None)
    with pytest.raises(RuntimeError, match="not permitted for APP_ENV"):
        migrate.cmd_status("prod")


@pytest.mark.regression
def test_remote_host_refused(monkeypatch):
    """A remote PG_HOST is refused."""
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("PG_HOST", "10.0.0.1")
    monkeypatch.setenv("PG_DB", "pifitness_dev")
    _install_fake_conn(monkeypatch, "dev")
    with pytest.raises(RuntimeError, match="remote host"):
        migrate.cmd_status("dev")


@pytest.mark.regression
def test_dev_db_in_test_mode_refused(monkeypatch):
    """PG_DB=pifitness_dev with APP_ENV=test is a mismatch and is refused."""
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_DB", "pifitness_dev")
    _install_fake_conn(monkeypatch, "test")
    with pytest.raises(RuntimeError, match="does not match the database implied"):
        migrate.cmd_status("test")


@pytest.mark.regression
def test_test_db_in_dev_mode_refused(monkeypatch):
    """PG_DB=pifitness_test with APP_ENV=dev is a mismatch and is refused."""
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_DB", "pifitness_test")
    _install_fake_conn(monkeypatch, "dev")
    with pytest.raises(RuntimeError, match="does not match the database implied"):
        migrate.cmd_status("dev")


@pytest.mark.regression
def test_db_name_resolution(monkeypatch):
    """APP_ENV implies the correct database name."""
    assert migrate._resolve_db_name("dev") == "pifitness_dev"
    assert migrate._resolve_db_name("test") == "pifitness_test"
