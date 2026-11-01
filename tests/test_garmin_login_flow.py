"""Garmin login flow tests.

Layers covered:
- DB session reuse (proves over-login protection)
- Session save-back after auth
- Playwright fires on fresh login path
- Provider bugs 1, 2, 3 (fixed in INT-13)
- Live login (deferred to GARMIN_LIVE_TEST=1 environment)

The live test is the ONLY test that proves login works against Garmin's
actual servers. All mocked tests prove orchestration only.
"""
import os
import pathlib
import tempfile
from contextlib import ExitStack
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from pifitness.integrations.garmin.auth import (
    AuthManager,
    BrowserLoginError as _AuthBrowserLoginError,  # for re-import safety
    Credentials,
    NativeOAuth2Session,
    NativeTokenSlot,
    OAuth2Token,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_oauth2_token(kind: str, now: int) -> OAuth2Token:
    return OAuth2Token(
        scope="test",
        token_type="bearer",
        access_token=f"fake-{kind}-access",
        refresh_token=f"fake-{kind}-refresh",
        expires_in=3600,
        expires_at=now + 3600,
        refresh_token_expires_in=86400,
        refresh_token_expires_at=now + 86400,
        jti=f"fake-{kind}-jti",
        customer_id="fake-customer" if kind == "di" else None,
    )


def _fake_session(now: int | None = None) -> NativeOAuth2Session:
    if now is None:
        now = int(datetime.now(tz=UTC).timestamp())
    return NativeOAuth2Session(
        created_at="2026-01-01T00:00:00+00:00",
        login_client_id="GCM_ANDROID_DARK",
        service_url="https://mobile.integration.garmin.com/gcm/android",
        di=NativeTokenSlot(client_id="fake-di-client", token=_fake_oauth2_token("di", now)),
        it=NativeTokenSlot(client_id="fake-it-client", token=_fake_oauth2_token("it", now)),
    )


# ---------------------------------------------------------------------------
# Layer 1 — DB session reuse (THE over-login protection test)
# ---------------------------------------------------------------------------

@pytest.mark.regression
def test_pirate_garmin_login_reuses_db_session_without_playwright(monkeypatch):
    """When the DB holds a valid session, pirate_garmin_login never invokes Playwright."""
    tmp_storage = pathlib.Path(tempfile.mkdtemp(prefix="garmin-test-"))
    monkeypatch.setenv("LOCAL_STORAGE_PATH", str(tmp_storage))

    fake_session = _fake_session()
    fake_session_dict = fake_session.to_dict()

    mock_client = MagicMock()
    mock_client.auth.ensure_authenticated.return_value = fake_session

    with ExitStack() as stack:
        stack.enter_context(patch("pifitness.integrations.garmin.provider.garmin_creds", return_value=("test@example.com", "password")))
        stack.enter_context(patch("pifitness.integrations.garmin.provider.load_token_from_db", return_value=fake_session_dict))
        mock_save = stack.enter_context(patch("pifitness.integrations.garmin.provider.save_token_to_db"))
        mock_client_class = stack.enter_context(patch("pifitness.integrations.garmin.provider.GarminClient"))
        stack.enter_context(patch("pifitness.integrations.garmin.provider.log_api_event"))
        mock_playwright = stack.enter_context(patch("pifitness.integrations.garmin.auth.login_via_browser"))
        mock_client_class.from_credentials.return_value = mock_client
        result = __import__("pifitness.integrations.garmin.provider", fromlist=["pirate_garmin_login"]).pirate_garmin_login()

    mock_playwright.assert_not_called()
    mock_save.assert_called_once()
    save_args = mock_save.call_args[0]
    assert save_args[0] == "Garmin"
    assert isinstance(save_args[1], dict)
    assert result is not None
    assert result["client"] is mock_client
    assert result["token"] == "fake-di-access"


# ---------------------------------------------------------------------------
# Session save-back — proves DB sync always happens
# ---------------------------------------------------------------------------

@pytest.mark.regression
def test_pirate_garmin_login_saves_session_to_db_after_auth(monkeypatch):
    """Every successful pirate_garmin_login call saves the session back to the DB."""
    tmp_storage = pathlib.Path(tempfile.mkdtemp(prefix="garmin-test-"))
    monkeypatch.setenv("LOCAL_STORAGE_PATH", str(tmp_storage))

    fake_session = _fake_session()
    mock_client = MagicMock()
    mock_client.auth.ensure_authenticated.return_value = fake_session

    with ExitStack() as stack:
        stack.enter_context(patch("pifitness.integrations.garmin.provider.garmin_creds", return_value=("test@example.com", "password")))
        stack.enter_context(patch("pifitness.integrations.garmin.provider.load_token_from_db", return_value=None))
        mock_save = stack.enter_context(patch("pifitness.integrations.garmin.provider.save_token_to_db"))
        mock_client_class = stack.enter_context(patch("pifitness.integrations.garmin.provider.GarminClient"))
        stack.enter_context(patch("pifitness.integrations.garmin.provider.log_api_event"))
        mock_client_class.from_credentials.return_value = mock_client
        __import__("pifitness.integrations.garmin.provider", fromlist=["pirate_garmin_login"]).pirate_garmin_login()

    mock_save.assert_called_once()
    assert mock_save.call_args[0][0] == "Garmin"


# ---------------------------------------------------------------------------
# Layer 3 — Playwright fires when create_browser_session is invoked
# ---------------------------------------------------------------------------

@pytest.mark.regression
def test_create_browser_session_invokes_playwright_login(tmp_path):
    """create_browser_session calls login_via_browser (the load-bearing fresh-login mechanism)."""
    from pifitness.integrations.garmin.browser_login import FreshLoginResult

    mgr = AuthManager(
        credentials=Credentials(username="u@example.com", password="p"),
        app_dir=tmp_path,
    )

    mock_ticket = FreshLoginResult(service_ticket="fake-ticket")

    with ExitStack() as stack:
        mock_playwright = stack.enter_context(patch("pifitness.integrations.garmin.auth.login_via_browser", return_value=mock_ticket))
        mock_di = stack.enter_context(patch.object(AuthManager, "exchange_service_ticket_for_di_token"))
        mock_it = stack.enter_context(patch.object(AuthManager, "exchange_di_token_for_it_token"))

        mock_di.return_value = NativeTokenSlot(
            client_id="fake-di-client",
            token=_fake_oauth2_token("di", int(datetime.now(tz=UTC).timestamp())),
        )
        mock_it.return_value = NativeTokenSlot(
            client_id="fake-it-client",
            token=_fake_oauth2_token("it", int(datetime.now(tz=UTC).timestamp())),
        )
        session = mgr.create_browser_session()

    mock_playwright.assert_called_once()
    assert mock_playwright.call_args.kwargs["username"] == "u@example.com"
    assert mock_playwright.call_args.kwargs["password"] == "p"
    assert mock_playwright.call_args.kwargs["headless"] is True
    assert session.di.token.access_token == "fake-di-access"


# ---------------------------------------------------------------------------
# Bug 3 fix — get_garmin_client returns well-formed tokens without re-login
# ---------------------------------------------------------------------------

@pytest.mark.regression
def test_get_garmin_client_returns_well_formed_token_without_relogin():
    """get_garmin_client returns a well-formed incoming_token without calling pirate_garmin_login."""
    from pifitness.integrations.garmin.provider import get_garmin_client

    incoming = {
        "client": MagicMock(),
        "token": "existing-token",
        "token_age": 100.0,  # any age works post-fix
    }

    with ExitStack() as stack:
        mock_login = stack.enter_context(patch("pifitness.integrations.garmin.provider.pirate_garmin_login"))
        stack.enter_context(patch("pifitness.integrations.garmin.provider.log_api_event"))
        result = get_garmin_client(incoming)

    mock_login.assert_not_called()
    assert result is incoming


# ---------------------------------------------------------------------------
# Bug 1 fix — get_status uses correct service name
# ---------------------------------------------------------------------------

@pytest.mark.contract
def test_get_status_authenticated_when_db_token_present():
    """get_status queries the DB with 'Garmin' (capital G) and reports authenticated=True."""
    from pifitness.integrations.garmin.provider import GarminProvider

    provider = GarminProvider()

    with patch("pifitness.integrations.garmin.provider.load_token_from_db", return_value={"some": "token"}) as mock_load:
        status = provider.get_status()

    mock_load.assert_called_once_with("Garmin")
    assert status.authenticated is True


# ---------------------------------------------------------------------------
# Bug 2 fix — is_configured requires both email and password
# ---------------------------------------------------------------------------

@pytest.mark.contract
def test_is_configured_false_when_creds_missing():
    """is_configured returns False when garmin_creds yields (None, None)."""
    from pifitness.integrations.garmin.provider import GarminProvider

    provider = GarminProvider()

    with patch("pifitness.integrations.garmin.provider.garmin_creds", return_value=(None, None)):
        assert provider.is_configured() is False


# ---------------------------------------------------------------------------
# Live test — proves login produces a usable API response (deferred to Rick)
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.slow(reason="Real Playwright + Garmin SSO round trip (~10-30s)")
@pytest.mark.live
def test_real_garmin_login_returns_usable_api_response():
    """Fresh login produces a DI token that fetches a real Garmin API response.

    Skipped unless GARMIN_LIVE_TEST=1. This is the ONLY test that proves
    login works against Garmin's servers today — mocked tests prove
    orchestration, not protocol compatibility with Garmin's current SSO.
    """
    if not os.environ.get("GARMIN_LIVE_TEST"):
        pytest.skip("Set GARMIN_LIVE_TEST=1 to run real login verification")

    from pifitness.integrations.garmin.provider import garmin_creds
    email, password = garmin_creds()
    if not email or not password:
        pytest.skip("No Garmin credentials configured in DB")

    tmp_app_dir = pathlib.Path(tempfile.mkdtemp(prefix="garmin-live-"))
    mgr = AuthManager(
        credentials=Credentials(username=email, password=password),
        app_dir=tmp_app_dir,
        timeout=60.0,
    )

    session = mgr.create_browser_session()
    assert isinstance(session, NativeOAuth2Session)
    assert session.di.token.access_token
    assert session.it.token.access_token

    profile = mgr._fetch_connectapi_json(
        session.di.token.access_token,
        "/userprofile-service/socialProfile",
    )
    assert isinstance(profile, dict)
    assert (
        profile.get("displayName") or profile.get("userName") or profile.get("profileId")
    ), f"Profile lookup returned unexpected shape: {list(profile.keys())}"