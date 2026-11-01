"""The real Garmin and Spotify providers conform to the AuthProvider protocol."""

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


@pytest.mark.contract
def test_garmin_provider_conforms():
    """The Garmin provider exposes a non-empty name and callable is_configured,
    get_client, get_status and refresh, and its get_status returns a TokenStatus for its
    own service.
    """
    from pifitness.integrations.garmin.provider import garmin_provider

    assert_conforms(garmin_provider)


@pytest.mark.contract
def test_spotify_provider_conforms():
    """The Spotify provider exposes a non-empty name and callable is_configured,
    get_client, get_status and refresh, and its get_status returns a TokenStatus for its
    own service.
    """
    from pifitness.integrations.spotify.provider import spotify_provider

    assert_conforms(spotify_provider)
