"""Invariant: get_settings() cannot silently return the production host 192.168.86.104.

When PG_HOST is unresolvable (backend/.env absent or missing PG_HOST),
get_settings() must raise RuntimeError rather than defaulting to PRD.

When PG_HOST IS set (via backend/.env locally or env vars in prod/staging),
that value wins — existing behavior preserved.

D8 DEV-safety guard: APP_ENV and the database pifitness.env marker must agree.
"""

import sys

sys.path.insert(0, "backend")

import dotenv
import pytest

from pifitness.core.config import get_settings, guard_db_marker


# VERIFIED: Regression — get_settings uses PG_HOST when set
@pytest.mark.regression
def test_get_settings_uses_pg_host_when_set(monkeypatch):
    """Existing behavior preserved: explicit PG_HOST wins."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "load_dotenv", lambda *a, **kw: None)
    monkeypatch.setenv("PG_HOST", "testhost")

    settings = get_settings()
    assert settings.db_host == "testhost"


# --- D8 marker guard unit tests (mocked read_db_marker) ---

# VERIFIED: Regression — guard_db_marker allows dev with dev marker
@pytest.mark.regression
def test_guard_dev_env_with_dev_marker_ok(monkeypatch):
    """APP_ENV=dev with marker=dev is allowed."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "read_db_marker", lambda: "dev")
    monkeypatch.setenv("APP_ENV", "dev")

    guard_db_marker("dev", "dev")  # should not raise


# VERIFIED: Regression — guard_db_marker raises for dev with wrong marker
@pytest.mark.regression
def test_guard_dev_env_with_wrong_marker_raises(monkeypatch):
    """APP_ENV=dev with marker=test or None-marker-known-wrong raises."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "read_db_marker", lambda: "test")
    monkeypatch.setenv("APP_ENV", "dev")

    with pytest.raises(RuntimeError, match="APP_ENV=dev"):
        guard_db_marker("dev", "test")


# VERIFIED: Regression — guard_db_marker allows dev with unknown marker
@pytest.mark.regression
def test_guard_dev_env_unknown_marker_ok(monkeypatch):
    """APP_ENV=dev with marker unknown (None) does not raise."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "read_db_marker", lambda: None)
    monkeypatch.setenv("APP_ENV", "dev")

    guard_db_marker("dev", None)  # should not raise


# VERIFIED: Regression — guard_db_marker raises for unset env with dev marker
@pytest.mark.regression
def test_guard_unset_env_with_dev_marker_raises(monkeypatch):
    """APP_ENV unset/prod with marker=dev raises (protects dev data)."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "read_db_marker", lambda: "dev")
    monkeypatch.delenv("APP_ENV", raising=False)

    with pytest.raises(RuntimeError, match="APP_ENV="):
        guard_db_marker("", "dev")


# VERIFIED: Regression — guard_db_marker raises for prod with dev marker
@pytest.mark.regression
def test_guard_prod_env_with_dev_marker_raises(monkeypatch):
    """APP_ENV=prod with marker=dev raises."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "read_db_marker", lambda: "dev")
    monkeypatch.setenv("APP_ENV", "prod")

    with pytest.raises(RuntimeError, match="APP_ENV=prod"):
        guard_db_marker("prod", "dev")


# VERIFIED: Regression — guard_db_marker allows test with test marker
@pytest.mark.regression
def test_guard_test_env_with_test_marker_ok(monkeypatch):
    """APP_ENV=test with marker=test is allowed."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "read_db_marker", lambda: "test")
    monkeypatch.setenv("APP_ENV", "test")

    guard_db_marker("test", "test")  # should not raise


# VERIFIED: Regression — guard_db_marker raises for test with dev marker
@pytest.mark.regression
def test_guard_test_env_with_dev_marker_raises(monkeypatch):
    """APP_ENV=test with marker=dev raises."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "read_db_marker", lambda: "dev")
    monkeypatch.setenv("APP_ENV", "test")

    with pytest.raises(RuntimeError, match="APP_ENV=test"):
        guard_db_marker("test", "dev")


# VERIFIED: Regression — get_settings succeeds with dev env and dev marker
@pytest.mark.regression
def test_get_settings_dev_env_with_dev_marker(monkeypatch):
    """get_settings() with APP_ENV=dev and reachable marker=dev succeeds."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "load_dotenv", lambda *a, **kw: None)
    monkeypatch.setattr(cfg, "read_db_marker", lambda: "dev")
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("APP_ENV", "dev")

    settings = get_settings()
    assert settings.app_env == "dev"
    assert settings.db_host == "localhost"


# VERIFIED: Regression — get_settings raises for unset env with dev marker
@pytest.mark.regression
def test_get_settings_unset_env_with_dev_marker_raises(monkeypatch):
    """get_settings() with APP_ENV unset and marker=dev raises (protects dev)."""
    import pifitness.core.config as cfg
    monkeypatch.setattr(cfg, "load_dotenv", lambda *a, **kw: None)
    monkeypatch.setattr(cfg, "read_db_marker", lambda: "dev")
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.delenv("APP_ENV", raising=False)

    with pytest.raises(RuntimeError, match="APP_ENV="):
        get_settings()