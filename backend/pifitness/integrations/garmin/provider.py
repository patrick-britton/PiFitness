"""Garmin AuthProvider + implementation.

This module owns the Garmin implementation functions (garmin_creds,
garmin_login, get_garmin_client) and the GarminProvider class.
backend_functions.service_logins imports from here; the dependency arrow points
inward from legacy callers to this module.

Vendored imports: this module imports GarminAuthError, MissingCredentialsError,
and GarminClient from pifitness.integrations.garmin.{auth,client} -- the
copies vendored in INT-1 from pirate-garmin_clone/src/pirate_garmin. Behavior
is identical to importing from the pirate_garmin editable install; only the
import path has changed.

The class GarminProvider satisfies the AuthProvider protocol in
pifitness.integrations.base and is validated by
tests/test_integration_protocol.py::test_garmin_provider_conforms.
"""

import hashlib
import importlib
import os
import random
import sys
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from garminconnect import (
    Garmin,
    GarminConnectAuthenticationError,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
)

from backend_functions.credential_management import decrypt_dict
from backend_functions.database_functions import get_conn, one_sql_result, qec
from backend_functions.helper_functions import random_sleep
from backend_functions.logging_functions import (
    elapsed_ms,
    log_api_event,
    log_app_event,
    start_timer,
)

from pifitness.integrations.base import (
    ProviderAuthError,
    ProviderNotConfiguredError,
    TokenStatus,
)
from pifitness.integrations._common import (
    load_api_credentials,
    load_token_from_db,
    save_token_to_db,
)

from pifitness.integrations.garmin.auth import (
    GarminAuthError,
    MissingCredentialsError,
)

from pifitness.integrations.garmin.client import GarminClient
def garmin_creds():
    # Loads the decrypted garmin credentials from the database and returns email & password
    creds = load_api_credentials('Garmin')
    if 'email' in creds:
        e = creds['email']
    else:
        e= None

    if 'password' in creds:
        p = creds['password']
    else:
        p = None
    return e, p


def garmin_login():
    """Legacy Garmin login using garminconnect library.
    
    DEPRECATED: Use pirate_garmin_login() instead, which uses Garmin's native
    Android auth flow and avoids Cloudflare 429 errors.
    
    Kept for backward compatibility — delegates to pirate_garmin_login().
    """
    return pirate_garmin_login()


def get_garmin_client(incoming_token=None):
    """Garmin client with reuse-first methodology (mirrors Spotify pattern).

    Methodology:
    1. If no incoming token → call pirate_garmin_login() for fresh session
    2. If malformed token → call pirate_garmin_login()
    3. If client is None → call pirate_garmin_login()
    4. Otherwise → return incoming_token as-is. The client's AuthManager
       refreshes on demand when request_json() is called, and the DB cache
       layer at pirate_garmin_login() ensures cross-machine reuse. No
       mid-flight validation needed.

    The pirate_garmin_login() function handles DB-backed session storage,
    so any machine can do the browser login and all machines share the session.
    """
    if incoming_token is None:
        new_token = pirate_garmin_login()
        if new_token:
            log_api_event('Garmin', 'New Token from none', token_age=0)
        return new_token

    if "client" not in incoming_token or "token_age" not in incoming_token:
        new_token = pirate_garmin_login()
        if new_token:
            log_api_event('Garmin', 'New Token from malformed dictionary', token_age=0)
        return new_token

    if incoming_token.get("client") is None:
        new_token = pirate_garmin_login()
        if new_token:
            log_api_event('Garmin', 'Client was in token, but was None', token_age=0)
        return new_token

    log_api_event('Garmin', 'Token reuse', token_age=time.time() - incoming_token["token_age"])
    return incoming_token


def pirate_garmin_login(headless=False):
    """Garmin login using pirate-garmin's native Android auth flow.
    
    Uses the same DB-backed session pattern as Spotify:
    1. Load session from _migration.api_tokens → write to local cache file
    2. Call AuthManager.ensure_authenticated() → handles refresh if needed
    3. Read refreshed session from cache file → save back to DB
    4. Return GarminClient with valid session
    
    This means any machine that does a browser login saves to the DB,
    and all other machines (including the Pi) pick up the same session.
    """
    import json
    from pathlib import Path
    
    # Get credentials
    email, password = garmin_creds()
    if not email or not password:
        log_api_event(service='Garmin', event='Login aborted — missing credentials')
        return None
    
    # Set up cache directory for pirate-garmin's auth files
    cache_dir = Path(os.getenv("LOCAL_STORAGE_PATH")) / "pirate-garmin"
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Load any stored session from DB and write to local cache file
    # This is the cross-device sync mechanism — same pattern as Spotify
    db_session = load_token_from_db('Garmin')
    if db_session:
        session_file = cache_dir / "native-oauth2.json"
        try:
            session_file.write_text(json.dumps(db_session, indent=2) + "\n")
        except OSError:
            log_api_event('Garmin', 'Could not write session to cache file (non-fatal)')
        else:
            log_api_event('Garmin', 'Session loaded from DB to local cache')
    
    # Step 2: Create GarminClient with pirate-garmin's AuthManager
    # This handles: load cache → refresh tokens → Playwright login if all expired
    try:
        client = GarminClient.from_credentials(
            username=email,
            password=password,
            app_dir=str(cache_dir)
        )
        
        # This triggers the full auth chain:
        # 1. Load cached session from native-oauth2.json (which we populated from DB)
        # 2. Check DI token expiry → refresh if needed
        # 3. Check IT token expiry → refresh if needed
        # 4. If all expired → run Playwright browser login (headless)
        session = client.auth.ensure_authenticated()
        
        # Step 3: Save refreshed session back to DB for cross-device sync
        session_dict = session.to_dict()
        save_token_to_db('Garmin', session_dict)
        
        log_api_event(
            service='Garmin',
            event='login with New Token' if db_session is None else 'Token reuse from cached session',
            token_age=0
        )
        
        return {
            "client": client,
            "token": session.di.token.access_token,
            "token_age": time.time()
        }
        
    except (GarminAuthError, MissingCredentialsError) as e:
        error_msg = str(e)
        log_api_event(service='Garmin', event='Login failed', err=error_msg)
        print(f"Garmin login failed: {error_msg}")
        return None
    except Exception as e:
        error_msg = str(e)
        log_api_event(service='Garmin', event='Login failed, uncaught', err=error_msg)
        print(f"Garmin login failed (uncaught): {error_msg}")
        return None




# --- AuthProvider implementation ---

class GarminProvider:
    """AuthProvider implementation for Garmin.

    get_status() is intentionally minimal: it reports authenticated=True/False
    based on the presence of a stored token, but does not inspect token
    freshness. The richer status (rate limits, last login, errors) is exposed
    by pifitness.auth.status.get_auth_status(), which is the canonical consumer.
    """

    name = "garmin"

    def is_configured(self) -> bool:
        try:
            email, password = garmin_creds()
            return bool(email and password)
        except Exception:
            return False

    def get_client(self) -> Any:
        if not self.is_configured():
            raise ProviderNotConfiguredError(
                "garmin credentials not configured"
            )
        try:
            return get_garmin_client()
        except ProviderAuthError:
            raise
        except Exception as e:
            raise ProviderAuthError(
                "garmin client init failed: %s" % e
            ) from e

    def get_status(self) -> TokenStatus:
        try:
            token = load_token_from_db("Garmin")
        except Exception as e:
            return TokenStatus(
                service=self.name,
                authenticated=False,
                notes="status lookup failed: %s" % e,
            )
        if not token:
            return TokenStatus(
                service=self.name,
                authenticated=False,
                notes="no stored token",
            )
        return TokenStatus(
            service=self.name,
            authenticated=True,
            notes="token present (refresh handled on demand by client)",
        )

    def refresh(self) -> TokenStatus:
        try:
            pirate_garmin_login()
        except Exception as e:
            raise ProviderAuthError(
                "garmin refresh failed: %s" % e
            ) from e
        return self.get_status()


garmin_provider = GarminProvider()
