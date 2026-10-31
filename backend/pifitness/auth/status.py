"""Aggregated auth status across provider integrations.

Extracted during INT-9-fix2 so that the source module can shrink to a thin shim. This module is intentionally
defensive: individual DB lookups are wrapped in try/except so a single query
failure does not break the whole status report.

Depends on pifitness.integrations.spotify.provider and
pifitness.integrations.garmin.provider for provider-specific helpers
(sql_rate_limited, spotify_creds, garmin_creds).

"""

from backend_functions.database_functions import one_sql_result
from pifitness.integrations.garmin.provider import garmin_creds
from datetime import datetime, timezone
from pifitness.integrations.spotify.provider import (
    sql_rate_limited,
    spotify_creds,
)


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
    spotify_status = {'status': 'unknown', 'last_login_utc': None, 'token_expires_utc': None, 'rate_limited_until': None}
    garmin_status = {'status': 'unknown', 'last_login_utc': None, 'token_expires_utc': None, 'last_error': None}
    try:
        is_limited = sql_rate_limited()
        if is_limited:
            cleared = one_sql_result("SELECT rate_limit_cleared_utc FROM api_services.api_service_list WHERE api_service_name = 'Spotify'")
            spotify_status['rate_limited_until'] = str(cleared) if cleared else None
            spotify_status['status'] = 'rate_limited'
    except Exception:
        pass
    try:
        last = one_sql_result("SELECT event_time_utc FROM logging.api_logins WHERE api_service_name = 'Spotify' AND event_name LIKE '%New Token%' ORDER BY event_time_utc DESC LIMIT 1")
        spotify_status['last_login_utc'] = str(last) if last else None
    except Exception:
        pass
    try:
        cid, csec, uri = spotify_creds()
        if cid and csec and uri:
            if spotify_status['status'] == 'unknown':
                spotify_status['status'] = 'ok'
        else:
            spotify_status['status'] = 'missing_credentials'
    except Exception:
        spotify_status['status'] = 'error'
    try:
        expires = one_sql_result('SELECT MAX(expires_at_utc) FROM _migration.spotify_auth_metadata WHERE is_active = true')
        spotify_status['token_expires_utc'] = str(expires) if expires else None
    except Exception:
        pass
    try:
        last = one_sql_result("SELECT event_time_utc FROM logging.api_logins WHERE api_service_name = 'Garmin' AND event_name LIKE '%New Token%' ORDER BY event_time_utc DESC LIMIT 1")
        garmin_status['last_login_utc'] = str(last) if last else None
    except Exception:
        pass
    try:
        last_err = one_sql_result("SELECT error_text FROM logging.api_logins WHERE api_service_name = 'Garmin' AND error_text IS NOT NULL ORDER BY event_time_utc DESC LIMIT 1")
        garmin_status['last_error'] = str(last_err) if last_err else None
    except Exception:
        pass
    try:
        expires_val = one_sql_result("SELECT refresh_token_expires_at_utc FROM _migration.api_tokens WHERE service_name = 'Garmin' AND is_active = true ORDER BY updated_at_utc DESC LIMIT 1")
        garmin_status['token_expires_utc'] = str(expires_val) if expires_val else None
    except Exception:
        pass
    try:
        email, password = garmin_creds()
        if email and password:
            if garmin_status['status'] == 'unknown':
                if garmin_status['token_expires_utc']:
                    try:
                        expires_dt = datetime.fromisoformat(garmin_status['token_expires_utc'])
                        if expires_dt < datetime.now(timezone.utc):
                            garmin_status['status'] = 'expired'
                        else:
                            garmin_status['status'] = 'ok'
                    except (ValueError, TypeError):
                        garmin_status['status'] = 'ok'
                else:
                    garmin_status['status'] = 'ok'
        else:
            garmin_status['status'] = 'missing_credentials'
    except Exception:
        garmin_status['status'] = 'error'
    return {'services': {'Spotify': spotify_status, 'Garmin': garmin_status}}
