"""INT-6a: GarminProvider facade over existing service_logins helpers.

This facade composes existing functions so the AuthProvider protocol can be
tested against a real provider without moving any code yet.

The purpose of this block is to prove that AuthProvider in
pifitness.integrations.base fits Garmin's natural shape BEFORE we commit to
moving code. See INT-3's survey and INT-5's conformance test for context.
"""

from typing import Any

from pifitness.integrations.base import (
    ProviderAuthError,
    ProviderNotConfiguredError,
    TokenStatus,
)

from backend_functions.service_logins import (
    garmin_creds,
    get_garmin_client,
    load_token_from_db,
    pirate_garmin_login,
)


class GarminProvider:
    """AuthProvider implementation for Garmin.

    get_status() is intentionally minimal in this block: it reports
    authenticated=True/False based on the presence of a stored token, but does
    not inspect token freshness. The richer status logic currently in
    service_logins.get_auth_status will be reconciled with this provider in
    INT-13 (/health/auth endpoint).
    """

    name = "garmin"

    def is_configured(self) -> bool:
        try:
            return bool(garmin_creds())
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
            token = load_token_from_db("garmin")
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
            pirate_garmin_login()
        except Exception as e:
            raise ProviderAuthError(
                "garmin refresh failed: %s" % e
            ) from e
        return self.get_status()


garmin_provider = GarminProvider()