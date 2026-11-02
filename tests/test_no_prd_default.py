"""Invariant: get_settings() cannot silently return the production host 192.168.86.104.

When PG_HOST is unresolvable (backend/.env absent or missing PG_HOST),
get_settings() must raise RuntimeError rather than defaulting to PRD.

When PG_HOST IS set (via backend/.env locally or env vars in prod/staging),
that value wins — existing behavior preserved.
"""

import sys

sys.path.insert(0, "backend")

import dotenv
import pytest

from pifitness.core.config import get_settings


@pytest.mark.regression
def test_get_settings_raises_when_pg_host_missing(monkeypatch):
    """Fail-loud: get_settings() must raise when PG_HOST is unresolvable."""
    # Block .env loading so backend/.env cannot mask the missing PG_HOST.
    # Patch config.load_dotenv (config.py does `from dotenv import load_dotenv`).
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "load_dotenv", lambda *a, **kw: None)
    # Remove PG_HOST from environment if present
    monkeypatch.delenv("PG_HOST", raising=False)

    with pytest.raises(RuntimeError, match="PG_HOST not configured"):
        get_settings()


@pytest.mark.regression
def test_get_settings_uses_pg_host_when_set(monkeypatch):
    """Existing behavior preserved: explicit PG_HOST wins."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "load_dotenv", lambda *a, **kw: None)
    monkeypatch.setenv("PG_HOST", "testhost")

    settings = get_settings()
    assert settings.db_host == "testhost"