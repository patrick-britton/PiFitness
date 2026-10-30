"""Shared interfaces for provider integrations.

This module defines the contract that each provider module under
``pifitness.integrations.<provider>`` is expected to satisfy. It contains
no provider-specific logic.

INT-5 onward will migrate provider code to implement ``AuthProvider``.
Until then, this module is not imported by any other module.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Protocol


class ProviderAuthError(Exception):
    """Raised when authentication with a provider fails.

    Wrap provider-specific auth exceptions in this class so callers do
    not need to import provider SDKs to handle auth failures.
    """


class ProviderNotConfiguredError(Exception):
    """Raised when a provider's credentials are not configured.

    Distinct from ``ProviderAuthError`` because the fix is different:
    credentials need to be supplied, not re-authenticated.
    """


@dataclass
class TokenStatus:
    """Uniform token status across providers.

    Fields:
        service: provider name (e.g. "spotify", "garmin")
        authenticated: True if a usable token is currently held
        expires_at: absolute expiry time if known, else None
        expires_soon: True if the token will expire soon and a refresh
            is advisable
        last_refresh: when the token was last refreshed, if known
        notes: free-form provider-specific context (safe to display in
            health-check output; must not contain secrets)
    """

    service: str
    authenticated: bool
    expires_at: Optional[datetime] = None
    expires_soon: bool = False
    last_refresh: Optional[datetime] = None
    notes: str = ""


class AuthProvider(Protocol):
    """Contract that each provider integration module must satisfy.

    Implementations are expected to be module-level singletons, not
    instantiated per-request. The ``name`` attribute is used as the key
    in status dictionaries (e.g. /health/auth output).

    Method semantics:

        is_configured() -> bool
            Return True only if credentials required to attempt auth
            are present in the environment. Does not verify the
            credentials are valid; use ``get_status`` for that.

        get_client() -> Any
            Return an authenticated client for this provider. Must
            reuse a cached/valid token if one exists, refresh if the
            token is expired, and re-auth if refresh fails. Should
            raise ``ProviderNotConfiguredError`` if credentials are
            missing and ``ProviderAuthError`` on auth failure.

        get_status() -> TokenStatus
            Return the current status. Must be cheap (no network calls
            if avoidable). Must never raise; return an unauthenticated
            ``TokenStatus`` on error.

        refresh() -> TokenStatus
            Force a token refresh. May perform network calls. Returns
            the resulting status. Raises ``ProviderAuthError`` if the
            refresh fails.
    """

    name: str

    def is_configured(self) -> bool: ...

    def get_client(self) -> Any: ...

    def get_status(self) -> TokenStatus: ...

    def refresh(self) -> TokenStatus: ...