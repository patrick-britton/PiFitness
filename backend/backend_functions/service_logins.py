import importlib
import os
import random
import sys
import time
import hashlib

import requests
import spotipy
from dotenv import load_dotenv
from garminconnect import Garmin, GarminConnectAuthenticationError, GarminConnectTooManyRequestsError, \
    GarminConnectConnectionError
from spotipy import SpotifyOAuth, SpotifyException
from spotipy.cache_handler import CacheFileHandler
from pathlib import Path
from backend_functions.credential_management import decrypt_dict
from backend_functions.database_functions import one_sql_result, get_conn, qec
from backend_functions.helper_functions import random_sleep
from backend_functions.logging_functions import log_api_event, log_app_event, start_timer, elapsed_ms

load_dotenv()

from pifitness.integrations._common import (
    load_api_credentials,
    load_token_from_db,
    save_token_to_db,
)

from pifitness.integrations.garmin.provider import (
    garmin_creds,
    garmin_login,
    get_garmin_client,
    pirate_garmin_login,
)

from pifitness.integrations.spotify.provider import (
    spotify_creds,
    get_spotify_client,
    sql_rate_limited,
    check_rate_limit_cached,
    log_rate_limitation,
    spotify_rate_limit_detection,
    sp_client,
    rate_limit_test,
    get_spotify_token,
    store_spotify_auth_metadata,
    clear_spotify_cache,
    get_spotify_token_expiry,
    insert_client,
)

def mapbox_token():
    creds = load_api_credentials('Mapbox')
    return creds.get('token')


def get_auth_status():
    """Return current auth status for all services as a structured dict.
    
    Returns:
        dict: {
            "services": {
                "Spotify": {
                    "status": "ok" | "expired" | "rate_limited" | "missing_credentials" | "error" | "unknown",
                    "last_login_utc": str | None,
                    "token_expires_utc": str | None,
                    "rate_limited_until": str | None
                },
                "Garmin": {
                    "status": "ok" | "missing_credentials" | "error" | "unknown",
                    "last_login_utc": str | None,
                    "token_expires_utc": str | None,
                    "last_error": str | None
                }
            }
        }
    """
    from backend_functions.database_functions import one_sql_result

    spotify_status = {
        "status": "unknown",
        "last_login_utc": None,
        "token_expires_utc": None,
        "rate_limited_until": None,
    }

    garmin_status = {
        "status": "unknown",
        "last_login_utc": None,
        "token_expires_utc": None,
        "last_error": None,
    }

    # --- Spotify ---
    # Check rate limit
    try:
        is_limited = sql_rate_limited()
        if is_limited:
            cleared = one_sql_result(
                "SELECT rate_limit_cleared_utc FROM api_services.api_service_list "
                "WHERE api_service_name = 'Spotify'"
            )
            spotify_status["rate_limited_until"] = str(cleared) if cleared else None
            spotify_status["status"] = "rate_limited"
    except Exception:
        pass

    # Check last login
    try:
        last = one_sql_result(
            "SELECT event_time_utc FROM logging.api_logins "
            "WHERE api_service_name = 'Spotify' "
            "AND event_name LIKE '%New Token%' "
            "ORDER BY event_time_utc DESC LIMIT 1"
        )
        spotify_status["last_login_utc"] = str(last) if last else None
    except Exception:
        pass

    # Check credentials exist
    try:
        cid, csec, uri = spotify_creds()
        if cid and csec and uri:
            if spotify_status["status"] == "unknown":
                spotify_status["status"] = "ok"
        else:
            spotify_status["status"] = "missing_credentials"
    except Exception:
        spotify_status["status"] = "error"

    # Check token expiry from _migration table if it exists
    try:
        expires = one_sql_result(
            "SELECT MAX(expires_at_utc) FROM _migration.spotify_auth_metadata "
            "WHERE is_active = true"
        )
        spotify_status["token_expires_utc"] = str(expires) if expires else None
    except Exception:
        pass

    # --- Garmin ---
    # Check last login
    try:
        last = one_sql_result(
            "SELECT event_time_utc FROM logging.api_logins "
            "WHERE api_service_name = 'Garmin' "
            "AND event_name LIKE '%New Token%' "
            "ORDER BY event_time_utc DESC LIMIT 1"
        )
        garmin_status["last_login_utc"] = str(last) if last else None
    except Exception:
        pass

    # Check last error
    try:
        last_err = one_sql_result(
            "SELECT error_text FROM logging.api_logins "
            "WHERE api_service_name = 'Garmin' "
            "AND error_text IS NOT NULL "
            "ORDER BY event_time_utc DESC LIMIT 1"
        )
        garmin_status["last_error"] = str(last_err) if last_err else None
    except Exception:
        pass

    # Check token expiry from _migration.api_tokens (Garmin native oauth session)
    # If the refresh token has expired, show yellow "Expired" — user can click to re-auth
    try:
        expires_val = one_sql_result(
            "SELECT refresh_token_expires_at_utc FROM _migration.api_tokens "
            "WHERE service_name = 'Garmin' AND is_active = true "
            "ORDER BY updated_at_utc DESC LIMIT 1"
        )
        garmin_status["token_expires_utc"] = str(expires_val) if expires_val else None
    except Exception:
        pass

    # Determine status from credentials + token expiry (never contacts the API provider)
    # The user clicks the button to actually test the connection.
    try:
        email, password = garmin_creds()
        if email and password:
            if garmin_status["status"] == "unknown":
                # If we have a token_expires_utc, check if it's expired
                if garmin_status["token_expires_utc"]:
                    try:
                        from datetime import datetime, timezone
                        expires_dt = datetime.fromisoformat(garmin_status["token_expires_utc"])
                        if expires_dt < datetime.now(timezone.utc):
                            garmin_status["status"] = "expired"
                        else:
                            garmin_status["status"] = "ok"
                    except (ValueError, TypeError):
                        garmin_status["status"] = "ok"
                else:
                    garmin_status["status"] = "ok"
        else:
            garmin_status["status"] = "missing_credentials"
    except Exception:
        garmin_status["status"] = "error"

    return {
        "services": {
            "Spotify": spotify_status,
            "Garmin": garmin_status,
        }
    }


def test_login(service_name):
    # Pulls the testable function from database and attempts login
    test_sql = f"""SELECT api_service_function from api_services.api_service_list
                WHERE api_service_name = '{service_name}'; """

    test_str = one_sql_result(test_sql)
    try:
        module_name, test_name = test_str.rsplit('.', 1)
    except Exception as e:
        return 'Service Invalid'
    module = importlib.import_module(module_name)
    svc_function = getattr(module, test_name)
    client = svc_function()
    return client is not None

def get_service_list(append_option=None):
    # Returns the known api services as a list
    sql="SELECT api_service_name from api_services.api_service_list"
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(sql)
    service_list = [row[0] for row in cursor.fetchall()]
    if append_option:
        service_list.append(append_option)
    return service_list


# Import Garmin auth classes from the pirate-garmin_clone library
from pirate_garmin.auth import GarminAuthError, MissingCredentialsError
from pirate_garmin.client import GarminClient
