"""Shared helpers for read-only PostgreSQL data-dictionary tooling."""
from __future__ import annotations
import json
import os
import re
from pathlib import Path
from typing import Any

ALLOWED_HOSTS = {"localhost", "127.0.0.1", "::1"}
ALLOWED_PORT = 15432
ALLOWED_DATABASE = "pifitness_dev"
ALLOWED_USER = "dev"
SENSITIVE_PARTS = {
    "password", "passwd", "secret", "token", "credential", "api_key", "apikey",
    "email", "phone", "address", "latitude", "longitude", "lat", "lon", "location",
    "loc_point", "geom", "geometry", "geography", "path", "payload", "json", "raw",
    "body", "weight", "bmi", "heart", "heartrate", "hrv", "spo2", "stress", "sleep",
    "respiration", "photo", "image", "battery", "birth", "dob", "ip", "session",
    "username", "user_name", "uri", "url", "context", "notes", "free_text",
    "track_name", "artist_name", "album_name", "track_point", "trackpoints",
}
SAFE_CATEGORY_ENDINGS = (
    "_type", "_type_id", "_status", "_precision", "_unit", "_kind", "_source",
    "_mode", "_state", "_enabled", "_active", "_confirmed", "_downloaded",
    "_available", "_in_usa", "_in_us",
)


def load_env_file(path: str | Path | None) -> dict[str, str]:
    if not path:
        return {}
    env_path = Path(path)
    if not env_path.is_file():
        raise FileNotFoundError(f"Environment file not found: {env_path}")
    result: dict[str, str] = {}
    for raw in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        result[key] = value
    return result


def database_config(env_file: str | Path) -> dict[str, Any]:
    """Resolve and enforce the exact local DEV allowlist; never use URL fallbacks."""
    file_env = load_env_file(env_file)
    # Explicit process environment wins, but the exact allowlist still applies.
    merged = {**file_env, **os.environ}
    def first(*keys: str, default: str = "") -> str:
        for key in keys:
            value = merged.get(key)
            if value not in (None, ""):
                return str(value)
        return default
    host = first("PG_HOST", "PGHOST").strip().lower().strip("[]")
    port = int(first("PG_PORT", "PGPORT", default="5432"))
    dbname = first("PG_DB", "PGDATABASE")
    user = first("PG_USER", "PGUSER")
    if host not in ALLOWED_HOSTS:
        raise RuntimeError(f"Refusing database host {host!r}; only loopback is allowed.")
    if port != ALLOWED_PORT or dbname != ALLOWED_DATABASE or user != ALLOWED_USER:
        raise RuntimeError(
            "Refusing database target: expected localhost:15432 database pifitness_dev user dev; "
            f"found host={host!r}, port={port}, database={dbname!r}, user={user!r}."
        )
    return {
        "host": host, "port": port, "dbname": dbname, "user": user,
        "password": first("PG_PASSWORD", "PGPASSWORD") or None,
        "sslmode": first("PGSSLMODE", default="disable"),
        "connect_timeout": 5,
        "application_name": "cline-data-dictionary-readonly",
        "options": "-c default_transaction_read_only=on -c statement_timeout=30000 "
                   "-c lock_timeout=5000 -c idle_in_transaction_session_timeout=60000",
    }



def validate_connection_identity(identity: dict[str, Any], cfg: dict[str, Any]) -> None:
    """Validate DB identity after connecting to the already-allowlisted client endpoint.

    ``cfg['port']`` is the client-side host port (15432 in the Docker setup).
    ``inet_server_port()`` is PostgreSQL's listener port inside the container
    (commonly 5432 when Docker publishes 15432:5432), so it is informational
    and must not be compared to the host-side port.
    """
    expected = {
        "database": ALLOWED_DATABASE,
        "role": ALLOWED_USER,
        "transaction_read_only": "on",
    }
    mismatches = {
        key: {"expected": value, "actual": identity.get(key)}
        for key, value in expected.items()
        if identity.get(key) != value
    }
    if mismatches:
        endpoint = f"{cfg.get('host')}:{cfg.get('port')}"
        observed = {
            "database": identity.get("database"),
            "role": identity.get("role"),
            "server_address": identity.get("server_address"),
            "server_listener_port": identity.get("server_port"),
            "transaction_read_only": identity.get("transaction_read_only"),
        }
        raise RuntimeError(
            "Connected database identity did not match the approved DEV target. "
            f"Client endpoint (validated before connect): {endpoint}; "
            f"identity mismatches: {mismatches}; observed server identity: {observed}. "
            "Note: server_listener_port is PostgreSQL's port inside the container; "
            "it is not the Windows/Docker published host port."
        )


def connect_readonly(env_file: str | Path):
    cfg = database_config(env_file)
    try:
        import psycopg  # type: ignore
        connection = psycopg.connect(**cfg)
    except ImportError:
        try:
            import psycopg2  # type: ignore
            connection = psycopg2.connect(**cfg)
        except ImportError as exc:
            raise RuntimeError("Install psycopg or psycopg2 in the existing project venv; do not install implicitly.") from exc
    connection.autocommit = False
    with connection.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
    return connection


def quote_ident(value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("Invalid SQL identifier")
    return '"' + value.replace('"', '""') + '"'


def qualified_name(schema: str, name: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(name)}"


def sensitive_name(name: str) -> bool:
    tokens = set(re.findall(r"[a-z0-9]+", name.lower()))
    return bool(tokens & SENSITIVE_PARTS) or any(x in name.lower() for x in ("access_token", "refresh_token", "secret_key"))


def safe_category(name: str, data_type: str) -> bool:
    if sensitive_name(name):
        return False
    typ = data_type.lower()
    if typ == "boolean":
        return True
    if typ.startswith("user-defined"):
        return True
    return name.lower().endswith(SAFE_CATEGORY_ENDINGS) and any(
        t in typ for t in ("character", "text", "smallint", "integer", "boolean", "user-defined")
    )


def fetch_dicts(cursor) -> list[dict[str, Any]]:
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"<binary:{len(value)} bytes>"
    return str(value)


def write_json(path: str | Path, payload: Any) -> None:
    dest = Path(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=json_default) + "\n", encoding="utf-8")
    tmp.replace(dest)
