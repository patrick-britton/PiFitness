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


def spotify_creds():
    creds = load_api_credentials('Spotify')
    if 'client_id' in creds:
        cid = creds['client_id']
    else:
        cid = None

    if 'client_secret' in creds:
        csec = creds['client_secret']
    else:
        csec = None

    if 'redirect_uri' in creds:
        uri = creds['redirect_uri']
    else:
        uri = None
    return cid, csec, uri

def mapbox_token():
    creds = load_api_credentials('Mapbox')
    return creds.get('token')


def get_spotify_client(incoming_token=None):
    # tests validity of incoming token and returns a client & token
    if incoming_token is None:
        new_spotify_token=spotify_rate_limit_detection(log_msg='New Token, no preexisting provided', token_age=0)
        return new_spotify_token

    if "token" not in incoming_token:
        new_spotify_token=spotify_rate_limit_detection(log_msg='New Token from malformed dictionary (token)', token_age=0)
        return new_spotify_token

    if "token_age" not in incoming_token:
        new_spotify_token=spotify_rate_limit_detection(log_msg='New Token from malformed dictionary (token age)', token_age=0)
        return new_spotify_token

    if incoming_token.get("client") is None:
        new_spotify_token=spotify_rate_limit_detection(log_msg='New Token from missing/none client', token_age=0)
        return new_spotify_token


    token_age = time.time() - incoming_token["token_age"]
    max_age = 1800 # seconds
    if token_age > max_age: #test token validity
        try:
            cl = incoming_token.get("client")
            cl.current_user()
            log_api_event('Spotify', 'Token reuse: client still active', token_age=token_age)
            return incoming_token
        except Exception as e:
            new_spotify_token=spotify_rate_limit_detection(log_msg=f'New Token from expired: {e}', token_age=token_age)
            return new_spotify_token
    else:
        try:
            cl = incoming_token.get("client")
            cl.current_user()
            log_api_event('Spotify', 'Token reuse: client still active', token_age=token_age)
            return incoming_token
        except Exception as e:
            new_spotify_token=spotify_rate_limit_detection(log_msg=f'New Token, non-timing error: {e}', token_age=token_age)
            return new_spotify_token



def sql_rate_limited():
    test_sql = """SELECT COALESCE(CURRENT_TIMESTAMP < rate_limit_cleared_utc, 1=0) as rate_limited
                FROM api_services.api_service_list
                WHERE api_service_name = 'Spotify'"""
    return one_sql_result(test_sql)


def check_rate_limit_cached():
    """
    Check rate limit with a short cache (30 seconds).

    Prevents hammering the DB on every UI interaction while still providing
    fresh rate limit status for user feedback. Returns cached result within
    30-second window, otherwise queries the database.

    Returns:
        bool: True if currently rate limited, False otherwise.
    """
    now = time.time()
    if hasattr(check_rate_limit_cached, '_last_check'):
        if now - check_rate_limit_cached._last_check < 30:
            return check_rate_limit_cached._cached_result

    result = sql_rate_limited()
    check_rate_limit_cached._last_check = now
    check_rate_limit_cached._cached_result = result
    return result

def log_rate_limitation():
    log_api_event(service='Spotify', event='Under rate limitations', token_age=0)
    return

def spotify_rate_limit_detection(log_msg, token_age):
    test_sql = """SELECT COALESCE(CURRENT_TIMESTAMP < rate_limit_cleared_utc, 1=0) as rate_limited
                FROM api_services.api_service_list
                WHERE api_service_name = 'Spotify'"""
    is_rate_limited = sql_rate_limited()
    new_spotify_token = get_spotify_token()
    # SQL knows I'm already rate-limited
    if is_rate_limited:
        new_spotify_token = insert_client(new_spotify_token, None)
        log_rate_limitation()
        return new_spotify_token

    # Test if any new rate limitations are in effect
    is_rate_limited, sleep_interval = rate_limit_test(new_spotify_token)
    if is_rate_limited:
        new_spotify_token = insert_client(new_spotify_token, None)
        update_sql = f""""UPDATE api_services.api_service_list 
                        SET rate_limit_detected_utc = CURRENT_TIMESTAMP,
                        rate_limit_cleared_utc = CURRENT_TIMESTAMP + Interval '%s seconds'
                        WHERE api_service_name = 'Spotify';
                        """
        params = [sleep_interval,]
        qec(update_sql, params)
        log_api_event(service='Spotify', event='New rate limitations detected', token_age=0)
        return new_spotify_token

    # Otherwise return a new token.
    new_spotify_token = insert_client(new_spotify_token, sp_client(new_spotify_token))
    log_api_event(service='Spotify', event=log_msg, token_age=token_age)
    return new_spotify_token
    return

def sp_client(t):
    c= spotipy.Spotify(
        auth=t["token"],
        retries=0,
        status_retries=0,
        requests_timeout=5,
    )
    return c


def rate_limit_test(sp_token=None):
    # 1. Get the token from your existing client

    # Build token if not passed
    if not sp_token:
        sp_token = get_spotify_token()

    token = sp_token["token"]


    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 2. Make the raw call (using the same endpoint Spotipy uses)
    # Using a fake playlist or a known one. A GET request is safer/cheaper than replace_items.
    playlist_id = '0OGtAcLTRWGdO4S8tuudyD'
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"

    try:
        # We send an empty list to 'replace'—this is the same as your sp.playlist_replace_items
        # Spotify's API uses PUT for replacing all items.
        response = requests.put(url, headers=headers, json={"uris": []}, timeout=10)

        # 3. Capture the 429 and the Retry-After header
        if response.status_code == 429:
            # requests.headers is case-insensitive
            retry_after = response.headers.get("Retry-After")

            if retry_after:
                return True, int(retry_after)

            # If 429 exists but header is missing, Spotify is being non-compliant.
            return True, 120

            # If 200, 201, or 404/403 (e.g. invalid playlist ID), you are NOT rate limited
        return False, None

    except Exception as e:
        print(f"Network error: {e}")
        return True, 60

def get_spotify_token():
    # Retrieve the Spotify credentials from the database
    # Attempt a login with the appropriate scopes
    # Load the login state to session state
    t0=start_timer()
    client_id, client_secret, redirect_uri = spotify_creds()

    if not all([client_id, client_secret, redirect_uri]):
        log_app_event(cat='API Login Failure', desc="Missing Spotify Credentials", exec_time=elapsed_ms(t0))
        return None

    # Step 1: Build scope and auth manager
    scope_list = ['user-read-recently-played',
                  'user-library-read',
                  'user-modify-playback-state',
                  'playlist-read-private',
                  'playlist-read-collaborative',
                  'playlist-modify-private',
                  'playlist-modify-public',
                  'playlist-read-private playlist-read-collaborative',
                  "user-library-modify",
                  'user-read-playback-state',
                  'user-read-recently-played']
    scope = ''
    for scope_type in scope_list:
        scope = scope + scope_type + ' '
    scope = scope.strip()

    cache_loc = Path(os.getenv("LOCAL_STORAGE_PATH"))

    # Create the auth manager
    auth_manager = SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope=scope,
        cache_handler=CacheFileHandler(cache_path=os.path.join(cache_loc, ".spotify_cache"))
    )

    # Step 2: Check DB for stored token (Pi fallback — no local cache file)
    db_token_info = load_token_from_db('Spotify')
    if db_token_info:
        try:
            # Use Spotipy's built-in validate_token to check expiry and automatically refresh if needed!
            validated_token = auth_manager.validate_token(db_token_info)
            if validated_token:
                # If the token was refreshed, update it in the database
                if validated_token.get("access_token") != db_token_info.get("access_token"):
                    log_api_event(service='Spotify', event='Token refreshed via DB stored refresh token')
                    save_token_to_db('Spotify', validated_token)
                else:
                    log_api_event(service='Spotify', event='Token reuse from DB')

                spotify_client = spotipy.Spotify(auth=validated_token["access_token"])
                login_time = time.time()
                final_token = {"client": spotify_client,
                               "token": validated_token["access_token"],
                               "token_age": login_time}
                return final_token
            else:
                log_api_event(service='Spotify', event='DB token invalid/expired and could not be refreshed')
        except SpotifyException as e:
            # Check for invalid_grant specifically — refresh token expired
            if hasattr(e, 'http_status') and e.http_status == 400:
                error_body = str(e)
                if 'invalid_grant' in error_body:
                    log_api_event(
                        service='Spotify',
                        event='Refresh token expired — re-authorization required',
                        err='invalid_grant: Refresh token expired. User must re-authorize.',
                    )
                    # Clear the cached token so next attempt starts fresh
                    clear_spotify_cache()
                    return None
            log_api_event(service='Spotify', event='DB token refresh failed with SpotifyException', err=e)
        except Exception as e:
            # DB token validation/refresh failed — fall through to local cache flow
            log_api_event(service='Spotify', event='DB token validation/refresh failed, falling through to cache', err=e)

    # Step 3: Fall back to local cache / full refresh
    try:
        token_info = auth_manager.get_access_token(as_dict=True)
        if not token_info or "access_token" not in token_info:
            log_api_event(service='Spotify', event='token acquisition failure — no token returned')
            return None

        access_token = token_info["access_token"]
        login_time = time.time()
        log_api_event(service='Spotify', event='login with New Token')

        # Store authorization metadata if we got a new refresh token
        if token_info.get("refresh_token"):
            store_spotify_auth_metadata(token_info["refresh_token"])

        # Save token to DB for cross-device sync (Pi fallback)
        save_token_to_db('Spotify', token_info)

        final_token = {"client": None,
                       "token": access_token,
                       "token_age": login_time}
        return final_token
    except SpotifyException as e:
        # Check for invalid_grant specifically — refresh token expired
        if hasattr(e, 'http_status') and e.http_status == 400:
            error_body = str(e)
            if 'invalid_grant' in error_body:
                log_api_event(
                    service='Spotify',
                    event='Refresh token expired — re-authorization required',
                    err='invalid_grant: Refresh token expired. User must re-authorize.',
                )
                # Clear the cached token so next attempt starts fresh
                clear_spotify_cache()
                return None

        # Generic Spotify failure
        log_api_event(service='Spotify', event='token acquisition failure', err=e)
        return None
    except Exception as e:
        log_api_event(service='Spotify', event='token acquisition failure', err=e)
        return None


def store_spotify_auth_metadata(refresh_token):
    """Store authorization timestamp and calculate 6-month expiry.

    Args:
        refresh_token: The Spotify refresh token obtained during authorization.
    """
    from backend_functions.database_functions import qec

    token_hash = hashlib.sha256(refresh_token.encode()).hexdigest()
    sql = """INSERT INTO _migration.spotify_auth_metadata 
             (authorized_at_utc, refresh_token_hash, expires_at_utc)
             VALUES (CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP + INTERVAL '6 months')
             ON CONFLICT (refresh_token_hash) 
             DO UPDATE SET authorized_at_utc = CURRENT_TIMESTAMP,
                           expires_at_utc = CURRENT_TIMESTAMP + INTERVAL '6 months'"""
    qec(sql, [token_hash])
    log_api_event(service='Spotify', event='Authorization metadata stored', err=f'hash={token_hash[:16]}...')


def clear_spotify_cache():
    """Delete the Spotify token cache file to force fresh OAuth flow."""
    cache_path = Path(os.getenv("LOCAL_STORAGE_PATH")) / ".spotify_cache"
    if cache_path.exists():
        cache_path.unlink()
        log_api_event(service='Spotify', event='Cache cleared for re-authorization')
        print(f"Spotify cache cleared: {cache_path}")


def get_spotify_token_expiry():
    """Return the estimated token expiry date for UI display.

    Returns:
        str | None: The latest expiry timestamp from _migration.spotify_auth_metadata,
                    or None if no data exists.
    """
    from backend_functions.database_functions import one_sql_result
    sql = "SELECT MAX(expires_at_utc) FROM _migration.spotify_auth_metadata WHERE is_active = true"
    return one_sql_result(sql)


def insert_client(incoming_dict, client):
    outgoing_dict = {"client": client,
                     "token": incoming_dict.get("token"),
                     "token_age": incoming_dict.get("token_age")}
    return outgoing_dict


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
