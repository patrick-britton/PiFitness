"""
Environment Configuration
=========================

Centralized configuration that detects the runtime environment (Windows dev vs.
Linux Pi production) and provides platform-aware database and server settings.

Usage:
    from backend.config import get_settings

    settings = get_settings()
    print(settings.db_host)  # on Windows, 'localhost' on Linux
"""

import os
import platform
from dataclasses import dataclass, field
from typing import List
from dotenv import load_dotenv


@dataclass
class Settings:
    """Application settings with platform-aware defaults."""

    # Database
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "personal_fitness"
    db_user: str = "devuser"
    db_password: str = ""
    db_sslmode: str = "disable"

    # Deployment environment (APP_ENV): '', 'dev', 'test', 'prod'
    # Empty/unset means production on the Pi; 'dev' on local DEV; 'test' on Pi test DB.
    app_env: str = ""

    # CORS
    cors_origins: List[str] = field(default_factory=lambda: [
        "http://localhost:3000",           # Next.js dev server
        "http://localhost:8000",           # FastAPI self
        "https://pifitness.duckdns.org",   # Production (Pi 5)
    ])

    # Server
    app_name: str = "PiFitness API"
    debug: bool = False
    api_prefix: str = "/api"

    @property
    def database_url(self) -> str:
        """Build a SQLAlchemy-compatible database URL."""
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


def _detect_platform() -> str:
    """
    Detect the runtime platform.

    Returns:
        str: 'windows', 'linux', or 'other'.
    """
    system = platform.system().lower()
    if system == "windows":
        return "windows"
    elif system == "linux":
        return "linux"
    return "other"


def _get_default_db_host(platform_name: str) -> str:
    """
    Return the sensible default DB host for the given platform.

    On Linux (Pi production), the database is localhost.
    On Windows (dev), there is no safe default — PG_HOST must be configured
    via backend/.env or environment variable. Silently falling through to
    the production IP is a production-safety bug.
    """
    if platform_name == "windows":
        raise RuntimeError(
            "PG_HOST not configured; is backend/.env present? "
            "Refusing to default to production IP."
        )
    return "localhost"


def read_db_marker() -> str | None:
    """
    Read the pifitness.env database marker from the connected DEV database.

    Returns the marker string (e.g. 'dev', 'test') or None if the database
    cannot be reached or the marker is not set. Never raises — callers treat
    None as "marker unknown" and skip the guard.
    """
    import psycopg2

    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "15432")),
            dbname=os.getenv("PG_DB", "pifitness_dev"),
            user=os.getenv("PG_USER", "dev"),
            password=os.getenv("PG_PASSWORD", "devpassword"),
            sslmode=os.getenv("PGSSLMODE", "disable"),
            connect_timeout=3,
        )
        cur = conn.cursor()
        cur.execute("SHOW pifitness.env")
        row = cur.fetchone()
        cur.close()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None


def guard_db_marker(app_env: str, marker: str | None) -> None:
    """
    Enforce D8 DEV-safety: refuse to start when APP_ENV and the database
    marker disagree.

    Rules:
      - APP_ENV=dev  requires marker == 'dev'
      - APP_ENV unset/prod  refuses marker == 'dev' or 'test'
      - APP_ENV=test  requires marker == 'test'

    marker=None (unknown) never triggers a refusal — the guard only fires
    when the marker can actually be read.
    """
    if marker is None:
        return

    if app_env == "dev" and marker != "dev":
        raise RuntimeError(
            f"APP_ENV=dev but database pifitness.env marker is '{marker}'; "
            "refusing to start. Run tools/db/dev_reset.py to stamp DEV."
        )
    if app_env in ("", "prod") and marker in ("dev", "test"):
        raise RuntimeError(
            f"APP_ENV={app_env or 'unset'} but database pifitness.env marker is "
            f"'{marker}'; refusing to start to protect {marker} data."
        )
    if app_env == "test" and marker != "test":
        raise RuntimeError(
            f"APP_ENV=test but database pifitness.env marker is '{marker}'; "
            "refusing to start."
        )


def get_settings() -> Settings:
    """
    Load and return application settings.

    Loads .env from the backend/ directory (if present), then applies
    platform-aware defaults. Explicit environment variables always win.
    Reads the database pifitness.env marker when reachable and refuses to
    start on APP_ENV/marker mismatch per D8.

    Returns:
        Settings: Fully populated settings dataclass.
    """
    # Try to load .env from backend/ directory
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)

    platform_name = _detect_platform()

    pg_host = os.getenv("PG_HOST")
    if pg_host is None:
        pg_host = _get_default_db_host(platform_name)

    app_env = os.getenv("APP_ENV", "")

    settings = Settings(
        db_host=pg_host,
        db_port=int(os.getenv("PG_PORT", "5432")),
        db_name=os.getenv("PG_DB", "personal_fitness"),
        db_user=os.getenv("PG_USER", "devuser"),
        db_password=os.getenv("PG_PASSWORD", ""),
        db_sslmode=os.getenv("PGSSLMODE", "disable"),
        debug=os.getenv("DEBUG", "false").lower() == "true",
        app_env=app_env,
    )

    # D8 guard: check database marker when reachable
    marker = read_db_marker()
    guard_db_marker(app_env, marker)

    return settings