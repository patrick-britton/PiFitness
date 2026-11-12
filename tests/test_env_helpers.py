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


@pytest.mark.unit

def test_conn_params_missing_keys_use_empty_defaults():
    config = {}
    result = conn_params(config)
    assert result == {
        "host": "localhost",
        "port": 5432,
        "user": "",
        "password": "",
        "sslmode": "disable",
    }


# ---------------------------------------------------------------------------
# resolve_db_name
# ---------------------------------------------------------------------------

@pytest.mark.unit

def test_resolve_db_name_default():
    config = {"PG_HOST": "localhost"}
    assert resolve_db_name(config) == "pifitness"


@pytest.mark.unit

def test_resolve_db_name_explicit():
    config = {"PG_DB": "pifitness_test"}
    assert resolve_db_name(config) == "pifitness_test"


# ---------------------------------------------------------------------------
# normalize_server_address
# ---------------------------------------------------------------------------

@pytest.mark.unit

def test_normalize_server_address_plain():
    assert normalize_server_address("localhost") == "localhost"
    assert normalize_server_address("127.0.0.1") == "127.0.0.1"


@pytest.mark.unit

def test_normalize_server_address_strips_cidr_v4():
    assert normalize_server_address("127.0.0.1/32") == "127.0.0.1"
    assert normalize_server_address("10.0.0.5/24") == "10.0.0.5"


@pytest.mark.unit

def test_normalize_server_address_strips_cidr_v6():
    assert normalize_server_address("::1/128") == "::1"
    assert normalize_server_address("fe80::1/64") == "fe80::1"


# ---------------------------------------------------------------------------
# is_loopback
# ---------------------------------------------------------------------------

@pytest.mark.unit

def test_is_loopback_true():
    assert is_loopback("localhost") is True
    assert is_loopback("127.0.0.1") is True
    assert is_loopback("::1") is True


@pytest.mark.unit

def test_is_loopback_false():
    assert is_loopback("10.0.0.5") is False
    assert is_loopback("192.168.1.1") is False
    assert is_loopback("db.example.com") is False


# ---------------------------------------------------------------------------
# guard_marker
# ---------------------------------------------------------------------------

@pytest.mark.unit

def test_guard_marker_dev_ok():
    guard_marker(APP_ENV_DEV, "dev")


@pytest.mark.unit

def test_guard_marker_test_ok():
    guard_marker(APP_ENV_TEST, "test")


@pytest.mark.unit

def test_guard_marker_prod_unmarked_ok():
    guard_marker(APP_ENV_PROD, None)
    guard_marker(APP_ENV_PROD, "")


@pytest.mark.unit

def test_guard_marker_dev_wrong_marker():
    with pytest.raises(ValueError, match="APP_ENV=dev but database marker"):
        guard_marker(APP_ENV_DEV, "test")


@pytest.mark.unit

def test_guard_marker_test_wrong_marker():
    with pytest.raises(ValueError, match="APP_ENV=test but database marker"):
        guard_marker(APP_ENV_TEST, "dev")


@pytest.mark.unit

def test_guard_marker_prod_marked():
    with pytest.raises(ValueError, match="APP_ENV=prod but database marker"):
        guard_marker(APP_ENV_PROD, "prod")


@pytest.mark.unit

def test_guard_marker_unsupported_env():
    with pytest.raises(ValueError, match="Unsupported APP_ENV"):
        guard_marker("staging", None)