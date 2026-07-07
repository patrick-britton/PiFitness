"""
Inlined pirate-garmin authentication module
============================================

Consolidated Garmin native Android OAuth2 authentication logic, extracted from
the `pirate-garmin_clone/` source directory. This eliminates the need for a
separate pip-installed package and avoids import confusion.

Source: pirate-garmin_clone/src/pirate_garmin/
    - auth.py       → AuthManager, OAuth2Token, NativeOAuth2Session, etc.
    - client.py     → GarminClient
    - browser_login.py → login_via_browser(), BrowserLoginError

Usage:
    from backend_functions.pirate_garmin_auth import (
        GarminAuthError, MissingCredentialsError, GarminClient
    )
"""

from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlencode

import httpx

# =============================================================================
# Constants
# =============================================================================

GARTH_CLIENT_ID = "GCM_ANDROID_DARK"
GARTH_LOGIN_URL = "https://mobile.integration.garmin.com/gcm/android"
MOBILE_SSO_BASE_URL = "https://sso.garmin.com"
CONNECT_API_BASE_URL = "https://connectapi.garmin.com"
SERVICES_BASE_URL = "https://services.garmin.com"
DI_TOKEN_URL = "https://diauth.garmin.com/di-oauth2-service/oauth/token"
IT_TOKEN_URL = "https://services.garmin.com/api/oauth/token"
DI_GRANT_TYPE = "https://connectapi.garmin.com/di-oauth2-service/oauth/grant/service_ticket"
DI_CLIENT_IDS = (
    "GARMIN_CONNECT_MOBILE_ANDROID_DI_2025Q2",
    "GARMIN_CONNECT_MOBILE_ANDROID_DI_2024Q4",
    "GARMIN_CONNECT_MOBILE_ANDROID_DI",
)
IT_CLIENT_IDS = (
    "GARMIN_CONNECT_MOBILE_ANDROID_2025Q2",
    "GARMIN_CONNECT_MOBILE_ANDROID_2024Q4",
    "GARMIN_CONNECT_MOBILE_ANDROID",
)
MOBILE_SSO_USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 13; sdk_gphone64_arm64 Build/TE1A.220922.025; wv) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/132.0.0.0 Mobile Safari/537.36"
)
DEFAULT_ACCEPT_LANGUAGE = "en-US,en;q=0.9"
GARMIN_ANDROID_VERSION = "5.23"
GARMIN_ANDROID_BUILD = "10861"
NATIVE_API_USER_AGENT = f"GCM-Android-{GARMIN_ANDROID_VERSION}"
NATIVE_X_GARMIN_USER_AGENT = (
    "com.garmin.android.apps.connectmobile/5.23; ; Google/sdk_gphone64_arm64/google; "
    "Android/33; Dalvik/2.1.0"
)
TOKEN_EXPIRY_SAFETY_SECONDS = 300
DEFAULT_TIMEOUT_SECONDS = 30.0

# Browser login constants
BROWSER_DEFAULT_LOGIN_CLIENT_ID = "GCM_ANDROID_DARK"
BROWSER_DEFAULT_SERVICE_URL = "https://mobile.integration.garmin.com/gcm/android"
BROWSER_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 13; sdk_gphone64_arm64 Build/TE1A.220922.025; wv) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/132.0.0.0 Mobile Safari/537.36"
)
USERNAME_SELECTORS = (
    'input[name="username"]',
    'input[name="email"]',
    'input[type="email"]',
    '#username',
)
PASSWORD_SELECTORS = (
    'input[name="password"]',
    'input[type="password"]',
    '#password',
)
SUBMIT_SELECTORS = (
    'button[type="submit"]',
    'button:has-text("Sign In")',
    'button:has-text("Log In")',
    'input[type="submit"]',
)


# =============================================================================
# Exception Classes
# =============================================================================


class GarminAuthError(RuntimeError):
    """Raised when Garmin authentication fails."""
    pass


class MissingCredentialsError(GarminAuthError):
    """Raised when Garmin credentials are missing."""
    pass


class BrowserLoginError(RuntimeError):
    """Raised when Playwright-based browser login fails."""
    pass


class BrowserDependencyError(BrowserLoginError):
    """Raised when Playwright is not installed."""
    pass


# =============================================================================
# Data Classes
# =============================================================================


@dataclass(slots=True)
class Credentials:
    username: str
    password: str


@dataclass(slots=True)
class OAuth2Token:
    scope: str
    token_type: str
    access_token: str
    refresh_token: str
    expires_in: int
    expires_at: int
    refresh_token_expires_in: int
    refresh_token_expires_at: int
    jti: str | None = None
    customer_id: str | None = None

    @classmethod
    def from_response(cls, payload: dict[str, Any]) -> OAuth2Token:
        now = int(datetime.now(tz=UTC).timestamp())
        expires_in = int(payload["expires_in"])
        refresh_expires_in = int(payload["refresh_token_expires_in"])
        return cls(
            scope=str(payload.get("scope", "")),
            token_type=str(payload["token_type"]),
            access_token=str(payload["access_token"]),
            refresh_token=str(payload["refresh_token"]),
            expires_in=expires_in,
            expires_at=now + expires_in,
            refresh_token_expires_in=refresh_expires_in,
            refresh_token_expires_at=now + refresh_expires_in,
            jti=_optional_str(payload.get("jti")),
            customer_id=_optional_str(payload.get("customerId")),
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> OAuth2Token:
        return cls(
            scope=str(payload.get("scope", "")),
            token_type=str(payload["token_type"]),
            access_token=str(payload["access_token"]),
            refresh_token=str(payload["refresh_token"]),
            expires_in=int(payload["expires_in"]),
            expires_at=int(payload["expires_at"]),
            refresh_token_expires_in=int(payload["refresh_token_expires_in"]),
            refresh_token_expires_at=int(payload["refresh_token_expires_at"]),
            jti=_optional_str(payload.get("jti")),
            customer_id=_optional_str(payload.get("customerId")),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "scope": self.scope,
            "token_type": self.token_type,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_in": self.expires_in,
            "expires_at": self.expires_at,
            "refresh_token_expires_in": self.refresh_token_expires_in,
            "refresh_token_expires_at": self.refresh_token_expires_at,
        }
        if self.jti:
            payload["jti"] = self.jti
        if self.customer_id:
            payload["customerId"] = self.customer_id
        return payload


@dataclass(slots=True)
class NativeTokenSlot:
    client_id: str
    token: OAuth2Token

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> NativeTokenSlot:
        return cls(
            client_id=str(payload["clientId"]),
            token=OAuth2Token.from_dict(payload["token"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "clientId": self.client_id,
            "token": self.token.to_dict(),
        }


@dataclass(slots=True)
class NativeOAuth2Session:
    created_at: str
    login_client_id: str
    service_url: str
    di: NativeTokenSlot
    it: NativeTokenSlot

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> NativeOAuth2Session:
        return cls(
            created_at=str(payload["createdAt"]),
            login_client_id=str(payload["loginClientId"]),
            service_url=str(payload["serviceUrl"]),
            di=NativeTokenSlot.from_dict(payload["di"]),
            it=NativeTokenSlot.from_dict(payload["it"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "createdAt": self.created_at,
            "loginClientId": self.login_client_id,
            "serviceUrl": self.service_url,
            "di": self.di.to_dict(),
            "it": self.it.to_dict(),
        }


@dataclass(slots=True)
class ProfileBundle:
    social_profile: dict[str, Any] | None = None
    settings: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ProfileBundle:
        return cls(
            social_profile=payload.get("socialProfile"),
            settings=payload.get("settings"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "socialProfile": self.social_profile,
            "settings": self.settings,
        }

    @property
    def display_name(self) -> str | None:
        if not self.social_profile:
            return None
        value = self.social_profile.get("displayName")
        return str(value) if value else None

    @property
    def time_zone(self) -> str | None:
        if not self.settings:
            return None
        value = self.settings.get("timeZone")
        return str(value) if value else None


@dataclass(slots=True)
class FreshLoginResult:
    service_ticket: str


# =============================================================================
# Browser Login (Playwright-based)
# =============================================================================


def build_sign_in_url(client_id: str, service_url: str) -> str:
    return (
        f"{MOBILE_SSO_BASE_URL}/mobile/sso/en_US/sign-in?"
        + urlencode({"clientId": client_id, "service": service_url})
    )


def parse_login_response_payload(payload: dict[str, Any]) -> FreshLoginResult:
    response_status = payload.get("responseStatus")
    response_type = ""
    if isinstance(response_status, dict):
        raw_type = response_status.get("type")
        response_type = str(raw_type).upper() if raw_type else ""

    service_ticket = payload.get("serviceTicketId")
    if response_type == "SUCCESSFUL" and service_ticket:
        return FreshLoginResult(service_ticket=str(service_ticket))

    serialized = _serialize_payload(payload)
    if response_type == "SUCCESSFUL":
        raise BrowserLoginError(
            f"Garmin browser login succeeded without serviceTicketId: {serialized}"
        )
    if response_type == "CAPTCHA_REQUIRED":
        raise BrowserLoginError(f"Garmin browser login requires CAPTCHA: {serialized}")
    if "MFA" in response_type or "TWO_FACTOR" in response_type:
        raise BrowserLoginError(f"Garmin browser login requires MFA: {serialized}")
    if response_type:
        raise BrowserLoginError(f"Garmin browser login failed ({response_type}): {serialized}")
    raise BrowserLoginError(f"Garmin browser login returned unexpected payload: {serialized}")


def login_via_browser(
    username: str,
    password: str,
    timeout: float,
    client_id: str = BROWSER_DEFAULT_LOGIN_CLIENT_ID,
    service_url: str = BROWSER_DEFAULT_SERVICE_URL,
    user_agent: str = BROWSER_DEFAULT_USER_AGENT,
    headless: bool = False,
) -> FreshLoginResult:
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise BrowserDependencyError(
            "Fresh Garmin login requires Playwright. Install the optional browser extra "
            "and Chromium with: `uv sync --extra browser` and `uv run playwright install chromium`."
        ) from exc

    timeout_ms = max(int(timeout * 1000), 1000)
    sign_in_url = build_sign_in_url(client_id, service_url)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=user_agent,
            locale="en-US",
            viewport={"width": 412, "height": 915},
            is_mobile=True,
            has_touch=True,
        )
        captured_results: list[dict[str, Any]] = []
        context.expose_binding(
            "pirateGarminCaptureLogin",
            lambda _source, payload: captured_results.append(payload),
        )
        page = context.new_page()
        page.add_init_script(_login_capture_init_script())
        try:
            try:
                page.goto(sign_in_url, wait_until="load", timeout=timeout_ms)
                _fill_first(page, USERNAME_SELECTORS, username, timeout_ms, PlaywrightError)
                _fill_first(page, PASSWORD_SELECTORS, password, timeout_ms, PlaywrightError)
                _submit_login_form(page, timeout_ms, PlaywrightError)
                try:
                    capture = _wait_for_captured_login_result(
                        page,
                        captured_results,
                        timeout_ms,
                        PlaywrightTimeoutError,
                    )
                except PlaywrightTimeoutError as exc:
                    raise BrowserLoginError(
                        "Timed out waiting for Garmin mobile login result. "
                        f"Current page: {page.url}. Page snippet: {_page_snippet(page)}"
                    ) from exc
                return _parse_captured_login_result(capture)
            except PlaywrightError as exc:
                raise BrowserLoginError(f"Garmin browser login automation failed: {exc}") from exc
        finally:
            context.close()
            browser.close()


def _fill_first(
    page: Any,
    selectors: tuple[str, ...],
    value: str,
    timeout_ms: int,
    playwright_error: type[Exception],
) -> None:
    locator = _first_visible_locator(page, selectors, timeout_ms, playwright_error)
    if locator is None:
        raise BrowserLoginError(
            f"Could not find Garmin login field. Tried selectors: {', '.join(selectors)}"
        )
    locator.fill(value)


def _submit_login_form(
    page: Any,
    timeout_ms: int,
    playwright_error: type[Exception],
) -> None:
    locator = _first_visible_locator(page, SUBMIT_SELECTORS, timeout_ms, playwright_error)
    if locator is not None:
        locator.click()
        return

    password_locator = _first_visible_locator(
        page,
        PASSWORD_SELECTORS,
        timeout_ms,
        playwright_error,
    )
    if password_locator is None:
        raise BrowserLoginError("Could not find Garmin password field to submit the login form")
    password_locator.press("Enter")


def _first_visible_locator(
    page: Any,
    selectors: tuple[str, ...],
    timeout_ms: int,
    playwright_error: type[Exception],
) -> Any | None:
    deadline = time.monotonic() + (timeout_ms / 1000)
    while time.monotonic() < deadline:
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                locator.wait_for(state="visible", timeout=750)
            except playwright_error:
                continue
            return locator
    return None


def _wait_for_captured_login_result(
    page: Any,
    captured_results: list[dict[str, Any]],
    timeout_ms: int,
    playwright_timeout_error: type[Exception],
) -> dict[str, Any]:
    deadline = time.monotonic() + (timeout_ms / 1000)
    while time.monotonic() < deadline:
        if captured_results:
            return captured_results[-1]
        page.wait_for_timeout(200)
    raise playwright_timeout_error("Timed out waiting for captured Garmin login result")


def _parse_captured_login_result(capture: Any) -> FreshLoginResult:
    if not isinstance(capture, dict):
        raise BrowserLoginError("Garmin browser login did not produce a capturable result")

    status = capture.get("status")
    response_text = str(capture.get("text") or "")
    payload = capture.get("payload")
    if status is not None and int(status) >= 400:
        raise BrowserLoginError(
            f"Garmin browser login failed with HTTP {status}: {_safe_snippet(response_text)}"
        )
    if isinstance(payload, dict):
        return parse_login_response_payload(payload)
    if response_text:
        raise BrowserLoginError(
            "Garmin browser login returned a non-JSON response: "
            f"{_safe_snippet(response_text)}"
        )
    raise BrowserLoginError("Garmin browser login returned an empty response")


def _login_capture_init_script() -> str:
    return """
(() => {
  if (window.__pirateGarminLoginHookInstalled) {
    return;
  }
  window.__pirateGarminLoginHookInstalled = true;

  const isLoginUrl = (url) => String(url || '').includes('/mobile/api/login');
  const setCapture = (status, url, text) => {
    let payload = null;
    if (typeof text === 'string' && text.length > 0) {
      try {
        payload = JSON.parse(text);
      } catch {
        payload = null;
      }
    }
    window.__pirateGarminLoginCapture = {
      status,
      url: String(url || ''),
      text: String(text || ''),
      payload,
    };
    if (typeof window.pirateGarminCaptureLogin === 'function') {
      void window.pirateGarminCaptureLogin(window.__pirateGarminLoginCapture);
    }
  };

  const originalFetch = window.fetch;
  window.fetch = async (...args) => {
    const response = await originalFetch(...args);
    try {
      const resource = args[0];
      const url = typeof resource === 'string' ? resource : resource && resource.url;
      if (isLoginUrl(url)) {
        const text = await response.clone().text();
        setCapture(response.status, url, text);
      }
    } catch (error) {
      window.__pirateGarminLoginCapture = {
        status: 599,
        url: '',
        text: String(error),
        payload: null,
      };
      if (typeof window.pirateGarminCaptureLogin === 'function') {
        void window.pirateGarminCaptureLogin(window.__pirateGarminLoginCapture);
      }
    }
    return response;
  };

  const originalOpen = XMLHttpRequest.prototype.open;
  const originalSend = XMLHttpRequest.prototype.send;

  XMLHttpRequest.prototype.open = function(method, url, ...rest) {
    this.__pirateGarminUrl = String(url || '');
    return originalOpen.call(this, method, url, ...rest);
  };

  XMLHttpRequest.prototype.send = function(...args) {
    this.addEventListener('loadend', function() {
      try {
        if (isLoginUrl(this.__pirateGarminUrl)) {
          setCapture(this.status, this.__pirateGarminUrl, this.responseText || '');
        }
      } catch (error) {
        window.__pirateGarminLoginCapture = {
          status: 599,
          url: this.__pirateGarminUrl || '',
          text: String(error),
          payload: null,
        };
        if (typeof window.pirateGarminCaptureLogin === 'function') {
          void window.pirateGarminCaptureLogin(window.__pirateGarminLoginCapture);
        }
      }
    }, { once: true });
    return originalSend.apply(this, args);
  };
})();
"""


def _page_snippet(page: Any) -> str:
    try:
        return _safe_snippet(str(page.locator("body").inner_text(timeout=1000)))
    except Exception:
        return ""


def _serialize_payload(payload: dict[str, Any]) -> str:
    try:
        return json.dumps(payload, sort_keys=True)
    except TypeError:
        return str(payload)


def _safe_snippet(text: str, length: int = 400) -> str:
    return " ".join(text.split())[:length]


# =============================================================================
# AuthManager
# =============================================================================


def default_app_dir() -> Path:
    configured = os.environ.get("PIRATE_GARMIN_APP_DIR")
    if configured:
        return Path(configured).expanduser()
    xdg_data_home = os.environ.get("XDG_DATA_HOME")
    if xdg_data_home:
        return Path(xdg_data_home).expanduser() / "pirate-garmin"
    return Path.home() / ".local" / "share" / "pirate-garmin"


class AuthManager:
    def __init__(
        self,
        credentials: Credentials | None = None,
        app_dir: Path | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.credentials = credentials
        self.app_dir = default_app_dir() if app_dir is None else Path(app_dir)
        self.timeout = timeout
        self.native_oauth2_path = self.app_dir / "native-oauth2.json"
        self.profile_path = self.app_dir / "profile.json"

    def ensure_app_dir(self) -> None:
        self.app_dir.mkdir(parents=True, exist_ok=True)

    def load_native_session(self) -> NativeOAuth2Session | None:
        if not self.native_oauth2_path.exists():
            return None
        return NativeOAuth2Session.from_dict(json.loads(self.native_oauth2_path.read_text()))

    def save_native_session(self, session: NativeOAuth2Session) -> None:
        self.ensure_app_dir()
        self.native_oauth2_path.write_text(json.dumps(session.to_dict(), indent=2) + "\n")

    def load_profile_bundle(self) -> ProfileBundle | None:
        if not self.profile_path.exists():
            return None
        return ProfileBundle.from_dict(json.loads(self.profile_path.read_text()))

    def save_profile_bundle(self, bundle: ProfileBundle) -> None:
        self.ensure_app_dir()
        self.profile_path.write_text(json.dumps(bundle.to_dict(), indent=2) + "\n")

    def ensure_authenticated(self) -> NativeOAuth2Session:
        stored = self.load_native_session()
        if stored is None:
            session = self.create_native_session()
            self.save_native_session(session)
            return session

        changed = False
        session = stored

        if _token_needs_refresh(session.di.token):
            if _refresh_token_expired(session.di.token):
                session = self.create_native_session()
                self.save_native_session(session)
                return session
            session = NativeOAuth2Session(
                created_at=_utc_now_iso(),
                login_client_id=session.login_client_id,
                service_url=session.service_url,
                di=self.refresh_di_token_slot(session.di),
                it=session.it,
            )
            changed = True

        if _token_needs_refresh(session.it.token):
            if not _refresh_token_expired(session.it.token):
                session = NativeOAuth2Session(
                    created_at=_utc_now_iso(),
                    login_client_id=session.login_client_id,
                    service_url=session.service_url,
                    di=session.di,
                    it=self.refresh_it_token_slot(session.it),
                )
                changed = True
            else:
                di_slot = session.di
                if _token_needs_refresh(di_slot.token):
                    if _refresh_token_expired(di_slot.token):
                        session = self.create_native_session()
                        self.save_native_session(session)
                        return session
                    di_slot = self.refresh_di_token_slot(di_slot)
                    changed = True
                it_slot = self.exchange_di_token_for_it_token(
                    di_slot.token.access_token,
                    _it_client_id_candidates(di_slot.client_id, session.it.client_id),
                )
                session = NativeOAuth2Session(
                    created_at=_utc_now_iso(),
                    login_client_id=session.login_client_id,
                    service_url=session.service_url,
                    di=di_slot,
                    it=it_slot,
                )
                changed = True

        if changed:
            self.save_native_session(session)
        return session

    def refresh_for_host(self, host: str) -> NativeOAuth2Session:
        session = self.load_native_session()
        if session is None:
            session = self.create_native_session()
            self.save_native_session(session)
            return session

        if host == "connectapi":
            if _refresh_token_expired(session.di.token):
                session = self.create_native_session()
            else:
                session = NativeOAuth2Session(
                    created_at=_utc_now_iso(),
                    login_client_id=session.login_client_id,
                    service_url=session.service_url,
                    di=self.refresh_di_token_slot(session.di),
                    it=session.it,
                )
        elif host == "services":
            if not _refresh_token_expired(session.it.token):
                session = NativeOAuth2Session(
                    created_at=_utc_now_iso(),
                    login_client_id=session.login_client_id,
                    service_url=session.service_url,
                    di=session.di,
                    it=self.refresh_it_token_slot(session.it),
                )
            elif not _refresh_token_expired(session.di.token):
                di_slot = session.di
                if _token_needs_refresh(di_slot.token):
                    di_slot = self.refresh_di_token_slot(di_slot)
                session = NativeOAuth2Session(
                    created_at=_utc_now_iso(),
                    login_client_id=session.login_client_id,
                    service_url=session.service_url,
                    di=di_slot,
                    it=self.exchange_di_token_for_it_token(
                        di_slot.token.access_token,
                        _it_client_id_candidates(di_slot.client_id, session.it.client_id),
                    ),
                )
            else:
                session = self.create_native_session()
        else:
            raise GarminAuthError(f"Unsupported host for native auth: {host}")

        self.save_native_session(session)
        return session

    def create_native_session(self) -> NativeOAuth2Session:
        credentials = self.require_credentials()
        try:
            fresh = login_via_browser(
                username=credentials.username,
                password=credentials.password,
                timeout=self.timeout,
                client_id=GARTH_CLIENT_ID,
                service_url=GARTH_LOGIN_URL,
                user_agent=MOBILE_SSO_USER_AGENT,
                headless=True,
            )
        except BrowserLoginError as exc:
            raise GarminAuthError(str(exc)) from exc

        with httpx.Client(follow_redirects=True, timeout=self.timeout) as client:
            di_slot = self.exchange_service_ticket_for_di_token(
                client, fresh.service_ticket, DI_CLIENT_IDS
            )
        it_slot = self.exchange_di_token_for_it_token(
            di_slot.token.access_token, _it_client_id_candidates(di_slot.client_id)
        )
        return NativeOAuth2Session(
            created_at=_utc_now_iso(),
            login_client_id=GARTH_CLIENT_ID,
            service_url=GARTH_LOGIN_URL,
            di=di_slot,
            it=it_slot,
        )

    def exchange_service_ticket_for_di_token(
        self,
        client: httpx.Client,
        service_ticket: str,
        client_ids: tuple[str, ...] | list[str],
    ) -> NativeTokenSlot:
        errors: list[str] = []
        for client_id in client_ids:
            response = client.post(
                DI_TOKEN_URL,
                headers=build_native_headers(
                    {
                        "authorization": _build_basic_authorization_header(client_id),
                        "accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                        "content-type": "application/x-www-form-urlencoded",
                        "cache-control": "no-cache",
                        "connection": "keep-alive",
                    }
                ),
                data={
                    "client_id": client_id,
                    "service_ticket": service_ticket,
                    "grant_type": DI_GRANT_TYPE,
                    "service_url": GARTH_LOGIN_URL,
                },
            )
            if response.status_code == 429:
                raise GarminAuthError(
                    f"Garmin native DI token exchange returned 429 for {client_id}"
                )
            if not response.is_success:
                errors.append(f"{client_id}: {response.status_code} {_safe_snippet(response.text)}")
                continue
            try:
                token = OAuth2Token.from_response(response.json())
            except Exception:
                errors.append(
                    f"{client_id}: unexpected non-token response {_safe_snippet(response.text)}"
                )
                continue
            return NativeTokenSlot(
                client_id=_extract_client_id_from_access_token(token.access_token) or client_id,
                token=token,
            )
        raise GarminAuthError("Garmin native DI token exchange failed:\n" + "\n".join(errors))

    def exchange_di_token_for_it_token(
        self,
        di_access_token: str,
        client_ids: tuple[str, ...] | list[str],
    ) -> NativeTokenSlot:
        errors: list[str] = []
        with httpx.Client(follow_redirects=True, timeout=self.timeout) as client:
            for client_id in client_ids:
                response = client.post(
                    f"{IT_TOKEN_URL}?grant_type=connect2_exchange",
                    headers=build_native_headers(
                        {
                            "accept": "application/json,text/plain,*/*",
                            "content-type": "application/x-www-form-urlencoded",
                        }
                    ),
                    data={
                        "client_id": client_id,
                        "connect_access_token": di_access_token,
                    },
                )
                if response.status_code == 429:
                    raise GarminAuthError(
                        f"Garmin native IT token exchange returned 429 for {client_id}"
                    )
                if not response.is_success:
                    errors.append(
                        f"{client_id}: {response.status_code} {_safe_snippet(response.text)}"
                    )
                    continue
                try:
                    token = OAuth2Token.from_response(response.json())
                except Exception:
                    errors.append(
                        f"{client_id}: unexpected non-token response {_safe_snippet(response.text)}"
                    )
                    continue
                return NativeTokenSlot(client_id=client_id, token=token)
        raise GarminAuthError("Garmin native IT token exchange failed:\n" + "\n".join(errors))

    def refresh_di_token_slot(self, slot: NativeTokenSlot) -> NativeTokenSlot:
        if _refresh_token_expired(slot.token):
            raise GarminAuthError("Native Garmin DI refresh token has expired")
        with httpx.Client(follow_redirects=True, timeout=self.timeout) as client:
            response = client.post(
                DI_TOKEN_URL,
                headers=build_native_headers(
                    {
                        "authorization": _build_basic_authorization_header(slot.client_id),
                        "accept": "application/json",
                        "content-type": "application/x-www-form-urlencoded",
                        "cache-control": "no-cache",
                        "connection": "keep-alive",
                    }
                ),
                data={
                    "grant_type": "refresh_token",
                    "client_id": slot.client_id,
                    "refresh_token": slot.token.refresh_token,
                },
            )
        if not response.is_success:
            raise GarminAuthError(
                "Native Garmin DI refresh failed: "
                f"{response.status_code} {_safe_snippet(response.text)}"
            )
        token = OAuth2Token.from_response(response.json())
        return NativeTokenSlot(
            client_id=_extract_client_id_from_access_token(token.access_token) or slot.client_id,
            token=token,
        )

    def refresh_it_token_slot(self, slot: NativeTokenSlot) -> NativeTokenSlot:
        if _refresh_token_expired(slot.token):
            raise GarminAuthError("Native Garmin IT refresh token has expired")
        with httpx.Client(follow_redirects=True, timeout=self.timeout) as client:
            response = client.post(
                f"{IT_TOKEN_URL}?grant_type=refresh_token",
                headers=build_native_headers(
                    {
                        "accept": "application/json,text/plain,*/*",
                        "content-type": "application/x-www-form-urlencoded",
                    }
                ),
                data={
                    "client_id": slot.client_id,
                    "refresh_token": slot.token.refresh_token,
                },
            )
        if not response.is_success:
            raise GarminAuthError(
                "Native Garmin IT refresh failed: "
                f"{response.status_code} {_safe_snippet(response.text)}"
            )
        return NativeTokenSlot(
            client_id=slot.client_id, token=OAuth2Token.from_response(response.json())
        )

    def fetch_profile_bundle(self, session: NativeOAuth2Session) -> ProfileBundle:
        social_profile = self._fetch_connectapi_json(
            session.di.token.access_token,
            "/userprofile-service/socialProfile",
        )
        try:
            settings = self._fetch_connectapi_json(
                session.di.token.access_token,
                "/userprofile-service/userprofile/settings",
            )
        except GarminAuthError:
            settings = None
        bundle = ProfileBundle(social_profile=social_profile, settings=settings)
        self.save_profile_bundle(bundle)
        return bundle

    def ensure_profile_bundle(self) -> ProfileBundle:
        bundle = self.load_profile_bundle()
        if bundle and bundle.display_name:
            return bundle
        session = self.ensure_authenticated()
        return self.fetch_profile_bundle(session)

    def _fetch_connectapi_json(
        self,
        access_token: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with httpx.Client(follow_redirects=True, timeout=self.timeout) as client:
            response = client.get(
                f"{CONNECT_API_BASE_URL}{path}",
                headers=build_native_headers(
                    {
                        "authorization": f"Bearer {access_token}",
                        "accept": "application/json",
                    }
                ),
                params=params,
            )
        if not response.is_success:
            raise GarminAuthError(
                f"{path} failed: {response.status_code} {_safe_snippet(response.text)}"
            )
        return response.json()

    def require_credentials(self) -> Credentials:
        if self.credentials is None:
            raise MissingCredentialsError(
                "Credentials are required for login. "
                "Set GARMIN_USERNAME and GARMIN_PASSWORD or pass CLI options."
            )
        return self.credentials


# =============================================================================
# GarminClient
# =============================================================================

Host = Literal["connectapi", "services"]
BASE_URLS: dict[Host, str] = {
    "connectapi": CONNECT_API_BASE_URL,
    "services": SERVICES_BASE_URL,
}


@dataclass(slots=True)
class GarminClient:
    auth: AuthManager
    timeout: float = DEFAULT_TIMEOUT_SECONDS

    @classmethod
    def from_credentials(
        cls,
        username: str | None = None,
        password: str | None = None,
        app_dir: str | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> GarminClient:
        credentials = None
        if username is not None or password is not None:
            if not username or not password:
                raise GarminAuthError("Both username and password must be provided together")
            credentials = Credentials(username=username, password=password)
        return cls(
            auth=AuthManager(credentials=credentials, app_dir=app_dir, timeout=timeout),
            timeout=timeout,
        )

    def whoami(self) -> dict[str, Any]:
        bundle = self.auth.ensure_profile_bundle()
        if bundle.social_profile is None:
            raise GarminAuthError("Garmin profile could not be loaded")
        return bundle.social_profile

    def get_profile_bundle(self) -> dict[str, Any]:
        bundle = self.auth.ensure_profile_bundle()
        return bundle.to_dict()

    def request_json(
        self,
        host: Host,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        session = self.auth.ensure_authenticated()
        token = (
            session.di.token.access_token if host == "connectapi" else session.it.token.access_token
        )
        response = self._get_json(host, token, path, params)
        if response.status_code == 401:
            session = self.auth.refresh_for_host(host)
            token = (
                session.di.token.access_token
                if host == "connectapi"
                else session.it.token.access_token
            )
            response = self._get_json(host, token, path, params)
        if not response.is_success:
            raise GarminAuthError(
                f"{path} failed: {response.status_code} {' '.join(response.text.split())[:240]}"
            )
        return response.json()

    def _get_json(
        self,
        host: Host,
        access_token: str,
        path: str,
        params: dict[str, Any] | None,
    ) -> httpx.Response:
        with httpx.Client(follow_redirects=True, timeout=self.timeout) as client:
            return client.get(
                f"{BASE_URLS[host]}{path}",
                headers=build_native_headers(
                    {
                        "authorization": f"Bearer {access_token}",
                        "accept": "application/json",
                    }
                ),
                params=params,
            )


# =============================================================================
# Header Builders & Helpers
# =============================================================================


def build_mobile_sso_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "user-agent": MOBILE_SSO_USER_AGENT,
        "accept-language": DEFAULT_ACCEPT_LANGUAGE,
    }
    if extra:
        headers.update(extra)
    return headers


def build_native_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "user-agent": NATIVE_API_USER_AGENT,
        "x-garmin-user-agent": NATIVE_X_GARMIN_USER_AGENT,
        "x-garmin-paired-app-version": GARMIN_ANDROID_BUILD,
        "x-garmin-client-platform": "Android",
        "x-app-ver": GARMIN_ANDROID_BUILD,
        "x-lang": "en",
        "x-gcexperience": "GC5",
        "accept-language": DEFAULT_ACCEPT_LANGUAGE,
    }
    if extra:
        headers.update(extra)
    return headers


def _build_basic_authorization_header(client_id: str) -> str:
    raw = f"{client_id}:".encode()
    return "Basic " + base64.b64encode(raw).decode()


def _optional_str(value: Any) -> str | None:
    return str(value) if value is not None else None


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _token_needs_refresh(token: OAuth2Token) -> bool:
    now = int(datetime.now(tz=UTC).timestamp())
    return token.expires_at <= now + TOKEN_EXPIRY_SAFETY_SECONDS


def _refresh_token_expired(token: OAuth2Token) -> bool:
    now = int(datetime.now(tz=UTC).timestamp())
    return token.refresh_token_expires_at <= now + TOKEN_EXPIRY_SAFETY_SECONDS


def _decode_jwt_payload(token: str) -> dict[str, Any] | None:
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload.encode()).decode())
    except Exception:
        return None


def _extract_client_id_from_access_token(access_token: str) -> str | None:
    payload = _decode_jwt_payload(access_token)
    if payload is None:
        return None
    value = payload.get("client_id")
    return str(value) if value else None


def _derive_it_client_id(di_client_id: str) -> str:
    if "_DI_" in di_client_id:
        return di_client_id.replace("_DI_", "_")
    if di_client_id.endswith("_DI"):
        return di_client_id[:-3]
    return IT_CLIENT_IDS[0]


def _unique_strings(values: list[str | None]) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return tuple(result)


def _it_client_id_candidates(di_client_id: str, preferred: str | None = None) -> tuple[str, ...]:
    return _unique_strings([preferred, _derive_it_client_id(di_client_id), *IT_CLIENT_IDS])