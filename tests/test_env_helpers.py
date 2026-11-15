"""Unit tests for tools/db/_env.py shared environment-file helpers.

These tests require no live database and run entirely on the development
machine. They cover env-file parsing, connection-parameter building, database
name resolution, server-address normalization, loopback detection, and APP_ENV
marker guarding.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from tools.db._env import (
    APP_ENV_DEV,
    APP_ENV_PROD,
    APP_ENV_TEST,
    conn_params,
    guard_marker,
    is_loopback,
    load_env_file,
    normalize_server_address,
    resolve_db_name,
)


# ---------------------------------------------------------------------------
# load_env_file
# ---------------------------------------------------------------------------

# VERIFIED: Unit — load_env_file parses basic key-value pairs from a temp .env file
@pytest.mark.unit

def test_load_env_file_basic_key_value():
    content = "APP_ENV=dev\nPG_HOST=localhost\nPG_DB=pifitness_dev\n"
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as fh:
        fh.write(content)
        path = fh.name
    try:
        result = load_env_file(path)
    finally:
        Path(path).unlink()
    assert result == {"APP_ENV": "dev", "PG_HOST": "localhost", "PG_DB": "pifitness_dev"}


# VERIFIED: Unit — load_env_file parses quoted values from a temp .env file
@pytest.mark.unit

def test_load_env_file_quoted_values():
    content = 'PG_PASSWORD="secret password"\nPG_USER=\'dev_user\'\n'
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as fh:
        fh.write(content)
        path = fh.name
    try:
        result = load_env_file(path)
    finally:
        Path(path).unlink()
    assert result == {"PG_PASSWORD": "secret password", "PG_USER": "dev_user"}


# VERIFIED: Unit — load_env_file strips comments and blank lines
@pytest.mark.unit

def test_load_env_file_strips_comments_and_blank_lines():
    content = "\n# comment line\nAPP_ENV=test   # inline comment\nPG_HOST=127.0.0.1\n\n"
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as fh:
        fh.write(content)
        path = fh.name
    try:
        result = load_env_file(path)
    finally:
        Path(path).unlink()
    # load_env_file strips surrounding whitespace but preserves inline comments.
    assert result == {"APP_ENV": "test   # inline comment", "PG_HOST": "127.0.0.1"}


# VERIFIED: Unit — load_env_file ignores lines without equals
@pytest.mark.unit

def test_load_env_file_ignores_lines_without_equals():
    content = "APP_ENV=dev\nthis is not a valid line\nPG_HOST=localhost\n"
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as fh:
        fh.write(content)
        path = fh.name
    try:
        result = load_env_file(path)
    finally:
        Path(path).unlink()
    assert result == {"APP_ENV": "dev", "PG_HOST": "localhost"}


# VERIFIED: Unit — load_env_file returns empty dict for empty file
@pytest.mark.unit

def test_load_env_file_empty_file():
    content = ""
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as fh:
        fh.write(content)
        path = fh.name
    try:
        result = load_env_file(path)
    finally:
        Path(path).unlink()
    assert result == {}


# ---------------------------------------------------------------------------
# conn_params
# ---------------------------------------------------------------------------

# VERIFIED: Unit — conn_params supplies default port 5432 and disable sslmode
@pytest.mark.unit

def test_conn_params_defaults():
    config = {
        "PG_HOST": "localhost",
        "PG_USER": "dev",
        "PG_PASSWORD": "pw",
        "PG_DB": "pifitness_dev",
    }
    result = conn_params(config)
    assert result == {
        "host": "localhost",
        "port": 5432,
        "user": "dev",
        "password": "pw",
        "sslmode": "disable",
    }


# VERIFIED: Unit — conn_params converts string port to int and applies overrides
@pytest.mark.unit

def test_conn_params_overrides_defaults():
    config = {
        "PG_HOST": "127.0.0.1",
        "PG_PORT": "15432",
        "PG_USER": "dev",
        "PG_PASSWORD": "pw",
        "PGSSLMODE": "require",
    }
    result = conn_params(config)
    assert result == {
        "host": "127.0.0.1",
        "port": 15432,
        "user": "dev",
        "password": "pw",
        "sslmode": "require",
    }


# VERIFIED: Unit — conn_params falls back to default port when PG_PORT is non-numeric
@pytest.mark.unit

def test_conn_params_invalid_port_falls_back_to_default():
    config = {
        "PG_HOST": "localhost",
        "PG_PORT": "not-a-number",
        "PG_USER": "dev",
        "PG_PASSWORD": "pw",
    }
    result = conn_params(config)
    assert result["port"] == 5432


# ---------------------------------------------------------------------------
# resolve_db_name
# ---------------------------------------------------------------------------

# VERIFIED: Unit — resolve_db_name defaults to pifitness when PG_DB unset (5d database guard)
@pytest.mark.unit

def test_resolve_db_name_default():
    config = {"PG_HOST": "localhost"}
    assert resolve_db_name(config) == "pifitness"


# VERIFIED: Unit — resolve_db_name honors explicit PG_DB override (5d database guard)
@pytest.mark.unit

def test_resolve_db_name_explicit():
    config = {"PG_DB": "pifitness_test"}
    assert resolve_db_name(config) == "pifitness_test"


# ---------------------------------------------------------------------------
# normalize_server_address
# ---------------------------------------------------------------------------

# VERIFIED: Unit — normalize_server_address passes plain addresses through
@pytest.mark.unit

def test_normalize_server_address_plain():
    assert normalize_server_address("localhost") == "localhost"
    assert normalize_server_address("127.0.0.1") == "127.0.0.1"


# VERIFIED: Unit — normalize_server_address strips CIDR v4 notation
@pytest.mark.unit

def test_normalize_server_address_strips_cidr_v4():
    assert normalize_server_address("127.0.0.1/32") == "127.0.0.1"
    assert normalize_server_address("10.0.0.5/24") == "10.0.0.5"


# VERIFIED: Unit — normalize_server_address strips CIDR v6 notation
@pytest.mark.unit

def test_normalize_server_address_strips_cidr_v6():
    assert normalize_server_address("::1/128") == "::1"
    assert normalize_server_address("fe80::1/64") == "fe80::1"


# ---------------------------------------------------------------------------
# is_loopback
# ---------------------------------------------------------------------------

# VERIFIED: Unit — is_loopback returns True for localhost, 127.0.0.1, ::1
@pytest.mark.unit

def test_is_loopback_true():
    assert is_loopback("localhost") is True
    assert is_loopback("127.0.0.1") is True
    assert is_loopback("::1") is True


# VERIFIED: Unit — is_loopback returns False for non-loopback addresses
@pytest.mark.unit

def test_is_loopback_false():
    assert is_loopback("10.0.0.5") is False
    assert is_loopback("192.168.1.1") is False
    assert is_loopback("db.example.com") is False


# ---------------------------------------------------------------------------
# guard_marker
# ---------------------------------------------------------------------------

# VERIFIED: Unit — guard_marker passes for dev with dev marker
@pytest.mark.unit

def test_guard_marker_dev_ok():
    guard_marker(APP_ENV_DEV, "dev")


# VERIFIED: Unit — guard_marker passes for test with test marker
@pytest.mark.unit

def test_guard_marker_test_ok():
    guard_marker(APP_ENV_TEST, "test")


# VERIFIED: Unit — guard_marker passes for prod with no marker
@pytest.mark.unit

def test_guard_marker_prod_unmarked_ok():
    guard_marker(APP_ENV_PROD, None)
    guard_marker(APP_ENV_PROD, "")


# VERIFIED: Unit — guard_marker raises ValueError for dev with wrong marker
@pytest.mark.unit

def test_guard_marker_dev_wrong_marker():
    with pytest.raises(ValueError, match="APP_ENV=dev but database marker"):
        guard_marker(APP_ENV_DEV, "test")


# VERIFIED: Unit — guard_marker raises ValueError for test with wrong marker
@pytest.mark.unit

def test_guard_marker_test_wrong_marker():
    with pytest.raises(ValueError, match="APP_ENV=test but database marker"):
        guard_marker(APP_ENV_TEST, "dev")


# VERIFIED: Unit — guard_marker raises ValueError for prod marked prod
@pytest.mark.unit

def test_guard_marker_prod_marked():
    with pytest.raises(ValueError, match="APP_ENV=prod but database marker"):
        guard_marker(APP_ENV_PROD, "prod")


# VERIFIED: Unit — guard_marker raises ValueError for unsupported APP_ENV
@pytest.mark.unit

def test_guard_marker_unsupported_env():
    with pytest.raises(ValueError, match="Unsupported APP_ENV"):
        guard_marker("staging", None)