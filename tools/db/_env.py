"""Shared environment-file helpers for PiFitness database tools.

This module replaces the non-migration-specific helpers that previously lived
in migrate.py (env-file parsing, connection-parameter building, database name
resolution, server-address normalization, and APP_ENV marker guarding).

It intentionally does NOT import psycopg2, does NOT manage the migration
ledger, and does NOT parse or apply migration SQL. Those concerns now live in
dbmate (the migration binary) and dbmate_guard.sh (the safety wrapper).
"""

from __future__ import annotations

import os
import re
from typing import Any

APP_ENV_DEV = "dev"
APP_ENV_TEST = "test"
APP_ENV_PROD = "prod"

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def load_env_file(path: str) -> dict[str, str]:
    """Parse a dotenv-style environment file into a dict.

    Handles KEY=value and KEY="value" forms, strips inline comments and
    surrounding whitespace. Does not perform shell interpolation.
    """
    values: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as stream:
        for raw_line in stream:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            if not key:
                continue
            # Strip surrounding quotes.
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            values[key] = value
    return values


def conn_params(config: dict[str, str]) -> dict[str, Any]:
    """Build psycopg2-compatible connection parameters from a loaded env dict.

    Does not open a connection. Returns host, port, user, password, sslmode.
    """
    port = config.get("PG_PORT", "5432")
    try:
        port_int = int(port)
    except ValueError:
        port_int = 5432

    return {
        "host": config.get("PG_HOST", "localhost"),
        "port": port_int,
        "user": config.get("PG_USER", ""),
        "password": config.get("PG_PASSWORD", ""),
        "sslmode": config.get("PGSSLMODE", "disable"),
    }


def resolve_db_name(config: dict[str, str]) -> str:
    """Resolve the target database name from a loaded env dict."""
    return config.get("PG_DB", "pifitness")


def normalize_server_address(address: str) -> str:
    """Normalize a PostgreSQL server address, stripping any CIDR prefix.

    Example: "127.0.0.1/32" -> "127.0.0.1"; "::1/128" -> "::1".
    """
    return address.split("/")[0]


def is_loopback(address: str) -> bool:
    """Return True if the (already normalized) server address is loopback."""
    return address in _LOCAL_HOSTS


def guard_marker(app_env: str, marker: str | None) -> None:
    """Validate that the database marker matches the expected APP_ENV.

    dev  -> marker must be "dev"
    test -> marker must be "test"
    prod -> marker must be unset/empty
    """
    if app_env == APP_ENV_DEV:
        if marker != APP_ENV_DEV:
            raise ValueError(
                f"APP_ENV=dev but database marker is {marker!r}; expected dev."
            )
    elif app_env == APP_ENV_TEST:
        if marker != APP_ENV_TEST:
            raise ValueError(
                f"APP_ENV=test but database marker is {marker!r}; expected test."
            )
    elif app_env == APP_ENV_PROD:
        if marker:
            raise ValueError(
                f"APP_ENV=prod but database marker is {marker!r}; PRD must remain unmarked."
            )
    else:
        raise ValueError(f"Unsupported APP_ENV={app_env!r}; expected dev, test, or prod.")