"""Conformance tests for the AuthProvider protocol in base.py.

Exercises ``assert_conforms`` against a dummy provider (positive) and
against broken providers (negative) to prove the test can both pass and
fail. Real garmin/spotify providers are added in INT-6/INT-7.
"""

import pytest

from pifitness.integrations.base import AuthProvider, TokenStatus


def assert_conforms(provider):
    """Verify a provider object satisfies the AuthProvider protocol.

    Safe to call in CI without credentials or network access. Does NOT call
    get_client() or refresh() because those may trigger live auth or browser
    flows; those are exercised by manual gates in the provider's own block.

    DOES call get_status(): the protocol requires it to be cheap and to never
    raise, so it is safe to invoke during conformance checks.
    """
    name = getattr(provider, "name", None)
    assert isinstance(name, str) and name, (
        "provider.name must be a non-empty string, got %r" % (name,)
    )
    for method in ("is_configured", "get_client", "get_status", "refresh"):
        fn = getattr(provider, method, None)
        assert callable(fn), (
            "provider.%s must be callable, got %r" % (method, fn)
        )
    configured = provider.is_configured()
    assert isinstance(configured, bool), (
        "is_configured() must return bool, got %s" % type(configured).__name__
    )

    status = provider.get_status()
    assert isinstance(status, TokenStatus), (
        "get_status() must return TokenStatus, got %s" % type(status).__name__
    )
    assert status.service == provider.name, (
        "status.service (%r) must equal provider.name (%r)"
        % (status.service, provider.name)
    )
    assert isinstance(status.authenticated, bool), (
        "status.authenticated must be bool, got %s"
        % type(status.authenticated).__name__
    )


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
    with pytest.raises(AssertionError, match="refresh must be callable"):
        assert_conforms(provider)


def test_non_string_name_raises():
    provider = _NonStringName()
    with pytest.raises(AssertionError, match="non-empty string"):
        assert_conforms(provider)


def test_missing_name_raises():
    provider = _MissingName()
    with pytest.raises(AssertionError, match="non-empty string"):
        assert_conforms(provider)


@pytest.mark.slow
def test_garmin_provider_conforms():
    """INT-6a: the real GarminProvider must satisfy AuthProvider.

    This is the first time the protocol in base.py is tested against a real
    provider's shape. If this test fails, the protocol is over-fit for Garmin
    and INT-6b (the code move) must not proceed until base.py is reshaped.
    """
    from pifitness.integrations.garmin.provider import garmin_provider

    assert_conforms(garmin_provider)


def test_assert_conforms_rejects_bad_status_type():
    """INT-6a: restore this test from INT-5's spec (was dropped).

    Enforces the TokenStatus shared-type contract. If a future regression in
    assert_conforms removes the isinstance(TokenStatus) check, this test will
    catch it -- the shared type is the only thing preventing provider-specific
    shapes from leaking into the shared layer.
    """

    class BadStatus:
        name = "bad_status"

        def is_configured(self):
            return False

        def get_client(self):
            return None

        def get_status(self):
            return {"authenticated": False}

        def refresh(self):
            return None

    with pytest.raises(AssertionError):
        assert_conforms(BadStatus())

@pytest.mark.slow
def test_spotify_provider_conforms():
    """INT-7a: the real SpotifyProvider must satisfy AuthProvider.

    If this fails, the protocol is over-fit for Spotify and INT-7b (the
    code move) must not proceed until base.py is reshaped.
    """
    from pifitness.integrations.spotify.provider import spotify_provider

    assert_conforms(spotify_provider)
