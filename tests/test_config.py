"""
Tests for the app-wide configuration endpoints (009-002 T08).

GET /api/config/map returns `{ mapboxToken: str | null }`, gating the
Leaderboards replay's Mapbox styles on token presence (AC-13). The token is
resolved server-side via `mapbox_token()` and mocked here so no DB is needed.
"""

import pytest
from fastapi.testclient import TestClient

# Importing `backend.main` triggers `backend.config` to load `.env` (sets
# KEY_PATH/SECRET_KEY etc.) *before* `service_logins`/`credential_management`
# are imported, matching how the app boots. Importing `backend.api.config`
# directly first would break because credential_management needs KEY_PATH.
from backend.main import app
import backend.api.config as config_module

client = TestClient(app)


def test_map_config_with_token(monkeypatch):
    """Returns the Mapbox token when a credential is configured."""
    monkeypatch.setattr(config_module, "mapbox_token", lambda: "pk.test-token-123")
    response = client.get("/api/config/map")
    assert response.status_code == 200
    assert response.json() == {"mapboxToken": "pk.test-token-123"}


def test_map_config_without_token(monkeypatch):
    """Returns null mapboxToken when no Mapbox credential is configured."""
    monkeypatch.setattr(config_module, "mapbox_token", lambda: "")
    response = client.get("/api/config/map")
    assert response.status_code == 200
    assert response.json() == {"mapboxToken": None}


def test_map_config_token_resolution_error(monkeypatch):
    """A resolution error degrades to null (UI falls back to free styles)."""
    def boom():
        raise RuntimeError("credential store unavailable")

    monkeypatch.setattr(config_module, "mapbox_token", boom)
    response = client.get("/api/config/map")
    assert response.status_code == 200
    assert response.json() == {"mapboxToken": None}