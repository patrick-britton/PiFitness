"""
Unit tests for Garmin token refresh slots in pifitness.integrations.garmin.auth.

Proves the DI and IT refresh paths work with mocked HTTP transport and without
touching Playwright, browser login, or create_browser_session. Structurally
guarantees that refresh does not fall back to fresh login under any condition.
"""

import ast
import inspect
import time

import httpx
import pytest

from pifitness.integrations.garmin.auth import (
    AuthManager,
    DI_TOKEN_URL,
    GarminAuthError,
    IT_TOKEN_URL,
    NativeTokenSlot,
    OAuth2Token,
    _extract_client_id_from_access_token,
)


def _make_token(*, refresh_token_expires_at_offset=86400):
    """Build a token whose refresh_token is valid for `offset` seconds from now."""
    now = int(time.time())
    return OAuth2Token(
        scope="test",
        token_type="Bearer",
        access_token="old_access_token",
        refresh_token="old_refresh_token",
        expires_in=3600,
        expires_at=now + 3600,
        refresh_token_expires_in=86400,
        refresh_token_expires_at=now + refresh_token_expires_at_offset,
    )


def _make_slot(*, refresh_token_expires_at_offset=86400, client_id="TEST_CLIENT"):
    return NativeTokenSlot(
        client_id=client_id,
        token=_make_token(refresh_token_expires_at_offset=refresh_token_expires_at_offset),
    )


def _token_response_json():
    return {
        "scope": "test",
        "token_type": "Bearer",
        "access_token": "new_access_token",
        "refresh_token": "new_refresh_token",
        "expires_in": 3600,
        "refresh_token_expires_in": 7776000,
    }


def _mock_client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler), timeout=1.0)


@pytest.mark.unit
def test_refresh_di_token_slot_success(tmp_path):
    captured = {}

    def handler(request):
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = request.content.decode()
        return httpx.Response(200, json=_token_response_json())

    manager = AuthManager(app_dir=tmp_path)
    slot = _make_slot(client_id="DI_CLIENT")
    new_slot = manager.refresh_di_token_slot(slot, client=_mock_client(handler))

    assert captured["method"] == "POST"
    assert captured["url"] == DI_TOKEN_URL
    assert "grant_type=refresh_token" in captured["body"]
    assert "refresh_token=old_refresh_token" in captured["body"]
    assert new_slot.token.access_token == "new_access_token"
    assert new_slot.token.refresh_token == "new_refresh_token"
    # New access token is not a JWT, so client_id falls back to the input slot.
    assert new_slot.client_id == "DI_CLIENT"




@pytest.mark.unit
def test_refresh_it_token_slot_success(tmp_path):
    captured = {}

    def handler(request):
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = request.content.decode()
        return httpx.Response(200, json=_token_response_json())

    manager = AuthManager(app_dir=tmp_path)
    slot = _make_slot(client_id="IT_CLIENT")
    new_slot = manager.refresh_it_token_slot(slot, client=_mock_client(handler))

    assert captured["method"] == "POST"
    assert captured["url"].startswith(IT_TOKEN_URL)
    assert "grant_type=refresh_token" in captured["url"]
    assert "client_id=IT_CLIENT" in captured["body"]
    assert "refresh_token=old_refresh_token" in captured["body"]
    assert new_slot.token.access_token == "new_access_token"
    assert new_slot.client_id == "IT_CLIENT"


@pytest.mark.unit
def test_refresh_it_token_slot_raises_when_refresh_token_expired(tmp_path):
    called = []

    def handler(request):
        called.append(request)
        return httpx.Response(200, json=_token_response_json())

    manager = AuthManager(app_dir=tmp_path)
    slot = _make_slot(refresh_token_expires_at_offset=-100)
    with pytest.raises(GarminAuthError, match="IT refresh token has expired"):
        manager.refresh_it_token_slot(slot, client=_mock_client(handler))
    assert called == []


@pytest.mark.unit
def test_refresh_it_token_slot_raises_on_http_error(tmp_path):
    def handler(request):
        return httpx.Response(500, text="server exploded")

    manager = AuthManager(app_dir=tmp_path)
    slot = _make_slot()
    with pytest.raises(GarminAuthError) as excinfo:
        manager.refresh_it_token_slot(slot, client=_mock_client(handler))
    msg = str(excinfo.value)
    assert "Native Garmin IT refresh failed" in msg
    assert "500" in msg
    assert "server exploded" in msg


@pytest.mark.unit
def test_refresh_paths_have_no_browser_or_session_dependency():
    """
    Structural guarantee: neither refresh_di_token_slot nor refresh_it_token_slot
    may reference login_via_browser or create_browser_session in any way. If a
    future change adds a browser fallback, this test fails.
    """
    from pifitness.integrations.garmin import auth as auth_mod

    src = inspect.getsource(auth_mod)
    tree = ast.parse(src)
    forbidden = {"login_via_browser", "create_browser_session"}
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name in (
            "refresh_di_token_slot",
            "refresh_it_token_slot",
        ):
            for sub in ast.walk(node):
                if isinstance(sub, ast.Name) and sub.id in forbidden:
                    violations.append(f"{node.name} references {sub.id} at line {sub.lineno}")
                elif isinstance(sub, ast.Attribute) and sub.attr in forbidden:
                    violations.append(f"{node.name} calls .{sub.attr} at line {sub.lineno}")
    assert not violations, violations


# Sanity check: the JWT client-id extractor is exercised indirectly above (falls
# back to slot.client_id for non-JWT tokens). Confirm it behaves as expected.
@pytest.mark.unit
def test_extract_client_id_from_non_jwt_returns_none():
    assert _extract_client_id_from_access_token("not-a-jwt-at-all") is None
@pytest.mark.unit
def test_refresh_di_token_slot_raises_when_refresh_token_expired(tmp_path):
    called = []

    def handler(request):
        called.append(request)
        return httpx.Response(200, json=_token_response_json())

    manager = AuthManager(app_dir=tmp_path)
    slot = _make_slot(refresh_token_expires_at_offset=-100)
    with pytest.raises(GarminAuthError, match="DI refresh token has expired"):
        manager.refresh_di_token_slot(slot, client=_mock_client(handler))
    # No HTTP request must have been attempted.
    assert called == []


@pytest.mark.unit
def test_refresh_di_token_slot_raises_on_http_error(tmp_path):
    def handler(request):
        return httpx.Response(401, text="unauthorized from mock")

    manager = AuthManager(app_dir=tmp_path)
    slot = _make_slot()
    with pytest.raises(GarminAuthError) as excinfo:
        manager.refresh_di_token_slot(slot, client=_mock_client(handler))
    msg = str(excinfo.value)
    assert "Native Garmin DI refresh failed" in msg
    assert "401" in msg
    assert "unauthorized from mock" in msg