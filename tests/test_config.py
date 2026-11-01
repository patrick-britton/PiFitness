"""GET /api/config/map exposes the Mapbox token to the frontend and degrades to null
when it is unavailable.
"""

import pytest
from fastapi.testclient import TestClient

# Importing `backend.main` triggers `backend.config` to load `.env` (sets
# KEY_PATH/SECRET_KEY etc.) *before* `service_logins`/`credential_management`
# are imported, matching how the app boots. Importing `backend.api.config`
# directly first would break because credential_management needs KEY_PATH.
from backend.main import app
import importlib
config_module = importlib.import_module('pifitness.config.router')

client = TestClient(app)


@pytest.mark.contract
def test_map_config_with_token(monkeypatch):
    """GET /api/config/map returns the configured Mapbox token as mapboxToken."""
    monkeypatch.setattr(config_module, "mapbox_token", lambda: "pk.test-token-123")
    response = client.get("/api/config/map")
    assert response.status_code == 200
    assert response.json() == {"mapboxToken": "pk.test-token-123"}


@pytest.mark.contract
def test_map_config_without_token(monkeypatch):
    """GET /api/config/map returns a null mapboxToken when no Mapbox credential is
    configured.
    """
    monkeypatch.setattr(config_module, "mapbox_token", lambda: "")
    response = client.get("/api/config/map")
    assert response.status_code == 200
    assert response.json() == {"mapboxToken": None}


@pytest.mark.contract
def test_map_config_token_resolution_error(monkeypatch):
    """GET /api/config/map answers 200 with a null mapboxToken when the credential
    lookup raises.
    """
    def boom():
        raise RuntimeError("credential store unavailable")

    monkeypatch.setattr(config_module, "mapbox_token", boom)
    response = client.get("/api/config/map")
    assert response.status_code == 200
    assert response.json() == {"mapboxToken": None}
