"""
Environment Configuration
=========================

Centralized configuration that detects the runtime environment (Windows dev vs.
Linux Pi production) and provides platform-aware database and server settings.

Usage:
    from backend.config import get_settings

    settings = get_settings()
    print(settings.db_host)  # '192.168.86.104' on Windows, 'localhost' on Linux
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

    On Windows (dev), the database lives on the Pi at a LAN IP.
    On Linux (Pi production), the database is localhost.
    """
    if platform_name == "windows":
        return "192.168.86.104"
    return "localhost"


def get_settings() -> Settings:
    """
    Load and return application settings.

    Loads .env from the backend/ directory (if present), then applies
    platform-aware defaults. Explicit environment variables always win.

    Returns:
        Settings: Fully populated settings dataclass.
    """
    # Try to load .env from backend/ directory
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)

    platform_name = _detect_platform()
    default_host = _get_default_db_host(platform_name)

    return Settings(
        db_host=os.getenv("PG_HOST", default_host),
        db_port=int(os.getenv("PG_PORT", "5432")),
        db_name=os.getenv("PG_DB", "personal_fitness"),
        db_user=os.getenv("PG_USER", "devuser"),
        db_password=os.getenv("PG_PASSWORD", ""),
        db_sslmode=os.getenv("PGSSLMODE", "disable"),
        debug=os.getenv("DEBUG", "false").lower() == "true",
    )