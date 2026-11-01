"""Shared primitives for provider integrations.

Moved verbatim from backend/backend_functions/service_logins.py in INT-5.
These helpers are re-exported from service_logins.py so existing importers
keep working unchanged (shim pattern).
"""

import json

from backend_functions.credential_management import decrypt_dict
from backend_functions.database_functions import one_sql_result, qec


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

    sql = f"""SELECT token_data FROM api_services.api_tokens
             WHERE service_name = '{service_name}' AND is_active = true
             ORDER BY updated_at_utc DESC LIMIT 1"""
    result = one_sql_result(sql)
    if result:
        try:
            return json.loads(result)
        except (json.JSONDecodeError, TypeError):
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

    # Deactivate old tokens, then insert new (using parameterized queries).
    # qec returns None on success, or a [msg1, msg2, msg3] list on failure.
    # Fail loud: silent loss of cross-device session sync is unacceptable.
    deactivate_sql = """UPDATE api_services.api_tokens
                        SET is_active = false
                        WHERE service_name = %s AND is_active = true"""
    result = qec(deactivate_sql, [service_name])
    if result:
        raise RuntimeError(
            f"save_token_to_db deactivate failed for {service_name}: {result}"
        )

    if refresh_expires_ts:
        expires_dt = datetime.fromtimestamp(refresh_expires_ts, tz=timezone.utc)
        insert_sql = """INSERT INTO api_services.api_tokens
                          (service_name, token_data, updated_at_utc, is_active, refresh_token_expires_at_utc)
                          VALUES (%s, %s, CURRENT_TIMESTAMP, true, %s)"""
        result = qec(insert_sql, [service_name, Json(token_info), expires_dt])
    else:
        insert_sql = """INSERT INTO api_services.api_tokens
                        (service_name, token_data, updated_at_utc, is_active)
                        VALUES (%s, %s, CURRENT_TIMESTAMP, true)"""
        result = qec(insert_sql, [service_name, Json(token_info)])
    if result:
        raise RuntimeError(
            f"save_token_to_db insert failed for {service_name}: {result}"
        )