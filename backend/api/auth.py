"""
Auth API Endpoints
==================

FastAPI endpoints for authentication status, token refresh, and OAuth flows.
Supports Spotify and Garmin login management for the React frontend.

Endpoints:
    GET  /api/auth/status              - Login status for all services
    POST /api/auth/spotify/refresh     - Force refresh Spotify token
    GET  /api/auth/spotify/auth-url    - Get Spotify OAuth authorization URL
    POST /api/auth/spotify/callback    - Complete Spotify OAuth with redirect URL
    POST /api/auth/garmin/login        - Trigger Garmin login attempt
    GET  /api/auth/health              - Proactive token health check
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any

from backend_functions.service_logins import (
    get_spotify_token,
    get_spotify_client,
    garmin_login,
    garmin_creds,
    spotify_creds,
    sql_rate_limited,
    get_service_list,
    test_login,
    get_auth_status,
    save_token_to_db,
)
from backend_functions.database_functions import sql_to_dict, one_sql_result
from backend_functions.logging_functions import log_api_event

router = APIRouter(prefix="/api/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------


class SpotifyCallbackRequest(BaseModel):
    redirect_url: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/status")
async def get_auth_status_endpoint():
    """
    Returns login status for all API services.

    Response:
    {
        "services": {
            "Spotify": {
                "status": "ok" | "expired" | "rate_limited" | "missing_credentials" | "error" | "unknown",
                "last_login_utc": "...",
                "token_expires_utc": "...",
                "rate_limited_until": null
            },
            "Garmin": {
                "status": "ok" | "missing_credentials" | "error" | "unknown",
                "last_login_utc": "...",
                "last_error": "..."
            }
        }
    }
    """
    try:
        return get_auth_status()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get auth status: {str(e)}",
        )


@router.post("/spotify/refresh")
async def refresh_spotify_token():
    """
    Force refresh of Spotify token.

    Attempts to acquire a new Spotify access token.
    Returns success/failure with details.
    Used by the React UI to trigger re-auth.
    """
    try:
        token = get_spotify_token()
        if token and token.get("token"):
            return {
                "status": "ok",
                "message": "Spotify token refreshed successfully",
                "token_preview": token["token"][:20] + "...",
            }
        else:
            return {
                "status": "error",
                "message": "Failed to acquire Spotify token — re-authorization may be required",
            }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh Spotify token: {str(e)}",
        )


@router.get("/spotify/auth-url")
async def get_spotify_auth_url():
    """
    Returns the Spotify authorization URL for browser-based OAuth.

    The React UI opens this URL in a new tab.
    After auth, the user pastes the redirect URL back to /api/auth/spotify/callback.
    Used for headless Pi re-authorization.
    """
    try:
        import spotipy
        from spotipy import SpotifyOAuth
        import os
        from pathlib import Path
        from dotenv import load_dotenv

        load_dotenv()

        cid, csec, redirect_uri = spotify_creds()
        if not all([cid, csec, redirect_uri]):
            raise HTTPException(
                status_code=400,
                detail="Spotify credentials not configured",
            )

        # Build scopes matching the existing application
        scope_list = [
            'user-read-recently-played',
            'user-library-read',
            'user-modify-playback-state',
            'playlist-read-private',
            'playlist-read-collaborative',
            'playlist-modify-private',
            'playlist-modify-public',
            'user-library-modify',
            'user-read-playback-state',
        ]
        scope = ' '.join(scope_list)

        auth_manager = SpotifyOAuth(
            client_id=cid,
            client_secret=csec,
            redirect_uri=redirect_uri,
            scope=scope,
            show_dialog=True,
        )

        auth_url = auth_manager.get_authorize_url()

        return {
            "status": "ok",
            "auth_url": auth_url,
            "redirect_uri": redirect_uri,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate auth URL: {str(e)}",
        )


@router.post("/spotify/callback")
async def spotify_callback(request: SpotifyCallbackRequest):
    """
    Accept a completed Spotify OAuth redirect URL.

    Extracts the authorization code from the redirect URL,
    exchanges it for tokens, and saves the result.

    This is the mechanism for headless Pi re-authorization:
    1. User gets auth URL from GET /api/auth/spotify/auth-url
    2. User opens in browser on any machine
    3. User authorizes Spotify
    4. User is redirected to localhost (which fails on Pi)
    5. User copies the full redirect URL from the address bar
    6. User pastes it into the React UI
    7. React sends it to this endpoint
    8. Server extracts the code and completes the OAuth flow
    """
    try:
        import urllib.parse
        import spotipy
        from spotipy import SpotifyOAuth
        import os
        from pathlib import Path
        from dotenv import load_dotenv

        load_dotenv()

        redirect_url = request.redirect_url

        # Parse the authorization code from the redirect URL
        parsed = urllib.parse.urlparse(redirect_url)
        query_params = urllib.parse.parse_qs(parsed.query)

        if "code" not in query_params:
            raise HTTPException(
                status_code=400,
                detail="No authorization code found in redirect URL. "
                       "Make sure to copy the full URL after authorizing.",
            )

        auth_code = query_params["code"][0]

        cid, csec, redirect_uri = spotify_creds()
        if not all([cid, csec, redirect_uri]):
            raise HTTPException(
                status_code=400,
                detail="Spotify credentials not configured",
            )

        scope_list = [
            'user-read-recently-played',
            'user-library-read',
            'user-modify-playback-state',
            'playlist-read-private',
            'playlist-read-collaborative',
            'playlist-modify-private',
            'playlist-modify-public',
            'user-library-modify',
            'user-read-playback-state',
        ]
        scope = ' '.join(scope_list)

        auth_manager = SpotifyOAuth(
            client_id=cid,
            client_secret=csec,
            redirect_uri=redirect_uri,
            scope=scope,
        )

        # Exchange the authorization code for tokens
        token_info = auth_manager.get_access_token(
            code=auth_code,
            as_dict=True,
        )

        if not token_info or not token_info.get("access_token"):
            raise HTTPException(
                status_code=400,
                detail="Failed to exchange authorization code for token. "
                       "The code may have expired or is invalid.",
            )

        # Log success
        log_api_event(
            service='Spotify',
            event='Re-authorization via callback — new token obtained',
        )

        # Save token to database for cross-device sync (Pi fallback)
        save_token_to_db('Spotify', token_info)

        return {
            "status": "ok",
            "message": "Spotify re-authorization successful",
            "token_preview": token_info["access_token"][:20] + "...",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to complete Spotify callback: {str(e)}",
        )


@router.post("/garmin/login")
async def trigger_garmin_login():
    """
    Trigger a Garmin login attempt.

    Used by the React UI to trigger Garmin sync.
    Returns success/failure with details.
    """
    try:
        client = garmin_login()
        if client:
            return {
                "status": "ok",
                "message": "Garmin login successful",
            }
        else:
            return {
                "status": "error",
                "message": "Garmin login failed — check credentials or rate limit status",
            }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger Garmin login: {str(e)}",
        )


@router.post("/spotify/test")
async def test_spotify_auth():
    """
    Test Spotify authentication - refresh token as needed and verify connectivity.
    
    Called when user clicks the Spotify status indicator in Admin module.
    Refreshes token if needed and verifies API connectivity.
    Returns success/failure with details.
    """
    try:
        import spotipy
        
        # Get token (will refresh if needed)
        token_dict = get_spotify_token()
        
        if token_dict and token_dict.get("token"):
            # Verify connectivity with lightweight API call
            client = spotipy.Spotify(auth=token_dict["token"])
            user = client.current_user()
            display_name = user.get("display_name", "unknown") if user else "unknown"
            
            return {
                "status": "ok",
                "message": "Spotify authentication verified",
                "user": display_name,
                "token_preview": token_dict["token"][:20] + "...",
            }
        else:
            return {
                "status": "error",
                "message": "Spotify token could not be obtained — re-authorization required",
            }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to test Spotify authentication: {str(e)}",
        )


@router.post("/garmin/test")
async def test_garmin_auth():
    """
    Test Garmin authentication - verify connectivity.
    
    Called when user clicks the Garmin status indicator in Admin module.
    Verifies the Garmin session is valid by checking the client exists.
    Returns success/failure with details.
    """
    try:
        client_dict = garmin_login()
        
        if client_dict and client_dict.get("client"):
            # Client exists - session is valid (pirate-garmin doesn't need get_full_name)
            return {
                "status": "ok",
                "message": "Garmin authentication verified",
            }
        else:
            return {
                "status": "error",
                "message": "Garmin login failed — check credentials or rate limit status",
            }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to test Garmin authentication: {str(e)}",
        )


@router.get("/health")
async def auth_health():
    """
    Proactive token health check.

    Validates tokens by making lightweight API calls.
    Can be called by a cron job for monitoring.
    Returns current health status for all services.
    """
    results: Dict[str, Any] = {}

    # Spotify: try to get current user
    try:
        import spotipy
        token = get_spotify_token()
        if token and token.get("token"):
            client = spotipy.Spotify(auth=token["token"])
            user = client.current_user()
            display_name = user.get("display_name", "unknown") if user else "unknown"
            results["spotify"] = {
                "status": "ok",
                "user": display_name,
            }
        else:
            results["spotify"] = {
                "status": "expired",
                "detail": "Could not acquire token",
            }
    except Exception as e:
        results["spotify"] = {
            "status": "error",
            "detail": str(e),
        }

    # Garmin: verify client exists (pirate-garmin doesn't expose get_full_name)
    try:
        client_dict = garmin_login()
        if client_dict and client_dict.get("client"):
            results["garmin"] = {
                "status": "ok",
                "detail": "Client obtained successfully",
            }
        else:
            results["garmin"] = {
                "status": "login_failed",
                "detail": "garmin_login() returned None",
            }
    except Exception as e:
        results["garmin"] = {
            "status": "error",
            "detail": str(e),
        }

    return results
