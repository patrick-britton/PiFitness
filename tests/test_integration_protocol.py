"""Conformance tests for the AuthProvider protocol in base.py.

Exercises ``assert_conforms`` against a dummy provider (positive) and
against broken providers (negative) to prove the test can both pass and
fail. Real garmin/spotify providers are added in INT-6/INT-7.
"""

import pytest

from pifitness.integrations.base import AuthProvider, TokenStatus


def assert_conforms(provider: AuthProvider) -> None:
    """Assert that a provider object satisfies the AuthProvider protocol."""
    assert hasattr(provider, "name"), "provider missing 'name' attribute"
    assert isinstance(provider.name, str), "provider 'name' must be a string"
    assert callable(getattr(provider, "is_configured", None)), "provider missing callable is_configured"
    assert callable(getattr(provider, "get_client", None)), "provider missing callable get_client"
    assert callable(getattr(provider, "get_status", None)), "provider missing callable get_status"
    assert callable(getattr(provider, "refresh", None)), "provider missing callable refresh"


class _DummyProvider:
    """Minimal valid provider for validating the conformance test itself."""

    name = "dummy"

    def is_configured(self) -> bool:
        return True

    def get_client(self):
        return object()

    def get_status(self) -> TokenStatus:
        return TokenStatus(
            service="dummy",
            authenticated=True,
            expires_soon=False,
        )

    def refresh(self) -> TokenStatus:
        return TokenStatus(
            service="dummy",
            authenticated=True,
            expires_soon=False,
        )


class _MissingRefresh:
    """Provider missing the refresh method."""

    name = "broken"

    def is_configured(self) -> bool:
        return True

    def get_client(self):
        return object()

    def get_status(self) -> TokenStatus:
        return TokenStatus(service="broken", authenticated=False)


class _NonStringName:
    """Provider whose name is not a string."""

    name = 123

    def is_configured(self) -> bool:
        return True

    def get_client(self):
        return object()

    def get_status(self) -> TokenStatus:
        return TokenStatus(service="broken", authenticated=False)

    def refresh(self) -> TokenStatus:
        return TokenStatus(service="broken", authenticated=False)


class _MissingName:
    """Provider missing the name attribute entirely."""

    def is_configured(self) -> bool:
        return True

    def get_client(self):
        return object()

    def get_status(self) -> TokenStatus:
        return TokenStatus(service="broken", authenticated=False)

    def refresh(self) -> TokenStatus:
        return TokenStatus(service="broken", authenticated=False)


def test_dummy_provider_conforms():
    provider = _DummyProvider()
    assert_conforms(provider)


def test_missing_refresh_raises():
    provider = _MissingRefresh()
    with pytest.raises(AssertionError, match="missing callable refresh"):
        assert_conforms(provider)


def test_non_string_name_raises():
    provider = _NonStringName()
    with pytest.raises(AssertionError, match="provider 'name' must be a string"):
        assert_conforms(provider)


def test_missing_name_raises():
    provider = _MissingName()
    with pytest.raises(AssertionError, match="provider missing 'name' attribute"):
        assert_conforms(provider)