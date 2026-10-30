"""Spotify AuthProvider adapter.

This module is a FACADE over the existing spotify functions in
backend_functions.service_logins. INT-7b will move the function bodies here
and shrink service_logins.py; until then, this module imports and composes
those functions so the AuthProvider protocol can be tested against a real
spotify provider.

Same pattern as INT-6a's garmin facade. If this facade does not fit
AuthProvider cleanly, INT-7b (the code move) must not proceed until base.py
is reshaped.
"""

from typing import Any

from pifitness.integrations.base import (
    ProviderAuthError,
    ProviderNotConfiguredError,
    TokenStatus,
)
from pifitness.integrations._common import load_token_from_db

from backend_functions.service_logins import (
    clear_spotify_cache,
    get_spotify_client,
    get_spotify_token,
    spotify_creds,
)


class SpotifyProvider:
    """AuthProvider implementation for Spotify.

    is_configured() is DEFENSIVE: unlike GarminProvider.is_configured() (which
    uses bool(tuple) and returns True for (None, None)), this checks each
    credential value. The garmin bug is latent and deferred to INT-13.
    get_status() is intentionally minimal: reports authenticated=True/False
    based on presence of a stored token. Rich status (expiry, rate-limit
    state) reconciled in INT-13 (/health/auth endpoint).
    """

    name = "spotify"

    def is_configured(self) -> bool:
        try:
            creds = spotify_creds()
            if not creds:
                return False
            if isinstance(creds, dict):
                return bool(creds) and all(v for v in creds.values())
            if isinstance(creds, (tuple, list)):
                return bool(creds) and all(v for v in creds)
            return bool(creds)
        except Exception:
            return False

    def get_client(self) -> Any:
        if not self.is_configured():
            raise ProviderNotConfiguredError(
                "spotify credentials not configured"
            )
        try:
            return get_spotify_client()
        except ProviderAuthError:
            raise
        except Exception as e:
            raise ProviderAuthError(
                "spotify client init failed: %s" % e
            ) from e

    def get_status(self) -> TokenStatus:
        try:
            token = load_token_from_db("Spotify")
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
            notes="token present (expiry check deferred to INT-13)",
        )

    def refresh(self) -> TokenStatus:
        try:
            clear_spotify_cache()
            get_spotify_token()
        except Exception as e:
            raise ProviderAuthError(
                "spotify refresh failed: %s" % e
            ) from e
        return self.get_status()


spotify_provider = SpotifyProvider()
