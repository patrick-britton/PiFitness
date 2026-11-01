"""
Spotify OAuth flow helpers.

Centralizes SpotifyOAuth() construction for the router endpoints.

This module deliberately does NOT use a CacheFileHandler. Router endpoints use
redirect flow (get_authorize_url + get_access_token); the resulting token is
persisted to the database by spotify_callback, not a disk cache. Adding
CacheFileHandler here would cause concurrent workers to race on a cache file
for tokens already persisted by the DB.

CacheFileHandler usage remains where it belongs: spotify/provider.py, which
builds the long-lived OAuth object used by get_spotify_token for refresh flow.
"""

from dotenv import load_dotenv
from spotipy import SpotifyOAuth

from backend_functions.service_logins import spotify_creds
from pifitness.integrations.base import ProviderAuthError
from pifitness.integrations.spotify.provider import (
    clear_spotify_cache,
    get_spotify_token,
)

load_dotenv()

# Scopes matching the existing application
_SCOPE_LIST = [
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


def _build_auth_manager(show_dialog=None):
    """Create a SpotifyOAuth object using stored credentials.

    Redirect-flow use only: no CacheFileHandler. Callers exchange the returned
    object for an authorize URL or a fresh token, then discard it.
    """
    cid, csec, redirect_uri = spotify_creds()
    if not all([cid, csec, redirect_uri]):
        raise ProviderAuthError("Spotify credentials not configured")
    kwargs = dict(
        client_id=cid,
        client_secret=csec,
        redirect_uri=redirect_uri,
        scope=' '.join(_SCOPE_LIST),
    )
    if show_dialog is not None:
        kwargs['show_dialog'] = show_dialog
    return SpotifyOAuth(**kwargs)


def build_authorize_url():
    """Return the Spotify authorization URL for browser-based OAuth.

    The React UI opens this URL in a new tab. After auth, the user pastes the
    redirect URL back to /api/auth/spotify/callback.
    """
    return _build_auth_manager(show_dialog=True).get_authorize_url()


def exchange_code(code):
    """Exchange an authorization code for a Spotify token dict.

    Returns the token dictionary from get_access_token. Caller is responsible
    for persisting it (see router.spotify_callback for the DB save pattern).
    """
    auth_manager = _build_auth_manager()
    token_info = auth_manager.get_access_token(code=code, as_dict=True)
    if not token_info or not token_info.get("access_token"):
        raise ProviderAuthError("Failed to exchange authorization code for token")
    return token_info


def force_refresh():
    """Clear the cached token and force a fresh refresh.

    Returns the resulting token dict. Delegates to spotify/provider.py for
    the actual refresh logic; this function is a convenience wrapper used by
    router.refresh_spotify_token.
    """
    clear_spotify_cache()
    return get_spotify_token()
