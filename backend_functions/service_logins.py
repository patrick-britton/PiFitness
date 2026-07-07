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
from pathlib import Path
from backend_functions.credential_management import decrypt_dict
from backend_functions.database_functions import one_sql_result, get_conn, qec
from backend_functions.helper_functions import random_sleep
from backend_functions.logging_functions import log_api_event, log_app_event, start_timer, elapsed_ms

load_dotenv()




def load_api_credentials(service=None):
    # loads and decrypts credentials for a specific service
    if not service:
        print('No service provided')
        return None

    t_sql = f"""
        SELECT api_credentials FROM api_services.credentials 
        WHERE api_service_name = '{service}';"""

    result = one_sql_result(t_sql)

    if result:
        return decrypt_dict(result)
    else:
        print(f'No results returned for service: {service}')
        return None


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
        cache_path=os.path.join(cache_loc, ".spotify_cache")
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


def load_token_from_db(service_name):
    """Load a stored token from the database for the given service.

    This is the Pi fallback path — when no local cache file exists (e.g. on
    a headless Pi), the token can be retrieved from the database where it
    was saved by the Windows dev machine.

    Args:
        service_name: 'Spotify' or other service name.

    Returns:
        dict | None: The deserialized token dict, or None if no token found.
    """
    from backend_functions.database_functions import one_sql_result
    import json

    sql = f"""SELECT token_data FROM _migration.api_tokens
             WHERE service_name = '{service_name}' AND is_active = true
             ORDER BY updated_at_utc DESC LIMIT 1"""
    result = one_sql_result(sql)
    if result:
        try:
            return json.loads(result)
        except (json.JSONDecodeError, TypeError):
            log_api_event(service=service_name, event='Failed to decode stored token from DB')
            return None
    return None


def save_token_to_db(service_name, token_info):
    """Save a token dict to the database for cross-device sync.

    After a successful OAuth flow, the token is stored in the DB so that
    other devices (e.g. the headless Pi) can retrieve it.

    Extracts and stores refresh token expiry when available:
    - Spotify: Uses store_spotify_auth_metadata() for 6-month tracking
    - Garmin: Extracts refresh_token_expires_at from di/it token slots

    Args:
        service_name: 'Spotify' or other service name.
        token_info: The token dict from Spotipy (or similar).
    """
    from backend_functions.database_functions import qec
    import json
    from datetime import datetime, timezone
    from psycopg2.extras import Json

    # Extract refresh token expiry timestamp for Garmin (from native-oauth2 session)
    refresh_expires_ts = None
    if service_name == 'Garmin':
        # Garmin session has 'di' and 'it' token slots, each with refresh_token_expires_at
        try:
            di_expires = token_info.get('di', {}).get('token', {}).get('refresh_token_expires_at')
            it_expires = token_info.get('it', {}).get('token', {}).get('refresh_token_expires_at')
            # Use the earlier expiry (most conservative)
            if di_expires and it_expires:
                refresh_expires_ts = min(di_expires, it_expires)
            elif di_expires:
                refresh_expires_ts = di_expires
            elif it_expires:
                refresh_expires_ts = it_expires
        except (AttributeError, TypeError):
            pass
    
    # Deactivate old tokens, then insert new (using parameterized queries)
    deactivate_sql = """UPDATE _migration.api_tokens
                        SET is_active = false
                        WHERE service_name = %s AND is_active = true"""
    qec(deactivate_sql, [service_name])
    
    if refresh_expires_ts:
        # Convert Unix timestamp to PostgreSQL timestamp
        expires_dt = datetime.fromtimestamp(refresh_expires_ts, tz=timezone.utc)
        insert_sql = """INSERT INTO _migration.api_tokens 
                          (service_name, token_data, updated_at_utc, refresh_token_expires_at_utc)
                          VALUES (%s, %s, CURRENT_TIMESTAMP, %s)"""
        qec(insert_sql, [service_name, Json(token_info), expires_dt])
    else:
        insert_sql = """INSERT INTO _migration.api_tokens 
                        (service_name, token_data, updated_at_utc)
                        VALUES (%s, %s, CURRENT_TIMESTAMP)"""
        qec(insert_sql, [service_name, Json(token_info)])
    
    log_api_event(service=service_name, event=f'Token saved to DB for {service_name}')


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
    4. If token age > 300s → validate with get_full_name(), reuse if valid
    5. If token age <= 300s → skip validation, reuse immediately
    
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

    token_age = time.time() - incoming_token["token_age"]
    max_age = 300  # seconds
    if token_age > max_age:  # test token validity
        try:
            cl = incoming_token.get("client")
            cl.get_full_name()
            log_api_event('Garmin', 'Token reuse: check and pass', token_age=token_age)
            return incoming_token
        except Exception as e:
            new_token = pirate_garmin_login()
            if new_token:
                log_api_event('Garmin', 'New Token from expired', token_age=token_age)
            return new_token
    else:
        log_api_event('Garmin', 'Token reuse: recency skip', token_age=token_age)
        return incoming_token


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


# Import pirate-garmin exception classes at module level with fallback
# This avoids UnboundLocalError if the installed package version doesn't export them
try:
    from pirate_garmin.auth import GarminAuthError, MissingCredentialsError
except ImportError:
    # Fallback stubs — allows except clauses to resolve even on version mismatch
    class GarminAuthError(Exception): pass
    class MissingCredentialsError(GarminAuthError): pass


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
        from pirate_garmin.client import GarminClient
        
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
