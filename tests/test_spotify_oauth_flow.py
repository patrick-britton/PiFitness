"""
Regression tests for the Spotify OAuth flow extracted in INT-8.

INT-8 extracted router.py's SpotifyOAuth construction into
pifitness.integrations.spotify.oauth_flow. A latent bug (force_refresh() not
returning a token) shipped without a test, so this module exists to ensure that
class of regression is caught.

These tests use monkeypatching only; no network calls, no real credentials,
no DB access.
"""

import pytest

from pifitness.integrations.spotify import oauth_flow


def test_force_refresh_clears_cache_and_returns_token(monkeypatch):
    """Regression for Bug 1: force_refresh() must return a token dict.

    Previously force_refresh() cleared the cache but never called
    get_spotify_token(), so router.refresh_spotify_token always returned
    an error. This test monkeypatches both dependencies and verifies the
    call chain and the return value.
    """
    sentinel_token = {
        'token': 'test-token-abc123',
        'expires_in': 3600,
        'refresh_token': 'rt-test',
    }

    cleared = False

    def fake_clear_cache():
        nonlocal cleared
        cleared = True

    def fake_get_token():
        return sentinel_token

    monkeypatch.setattr(oauth_flow, 'clear_spotify_cache', fake_clear_cache)
    monkeypatch.setattr(oauth_flow, 'get_spotify_token', fake_get_token)

    result = oauth_flow.force_refresh()

    assert cleared is True
    assert result is sentinel_token


def test_exchange_code_returns_token_dict(monkeypatch):
    """Regression: exchange_code() must return the token dict from get_access_token."""
    sentinel_token = {'access_token': 'tok', 'token_type': 'Bearer', 'expires_in': 3600}

    def fake_build_manager(show_dialog=None):
        class DummyOAuth:
            def get_access_token(self, code, as_dict=True):
                return sentinel_token
        return DummyOAuth()

    monkeypatch.setattr(oauth_flow, '_build_auth_manager', fake_build_manager)

    result = oauth_flow.exchange_code('test-code')

    assert result is sentinel_token
    assert result.get('access_token') == 'tok'


def test_build_authorize_url_calls_show_dialog(monkeypatch):
    """Regression: build_authorize_url() must request user consent dialog."""
    sentinels = {'called_with': None}

    def fake_build(show_dialog=None):
        sentinels['called_with'] = show_dialog
        class DummyOAuth:
            def get_authorize_url(self):
                return 'https://example.com/auth'
        return DummyOAuth()

    monkeypatch.setattr(oauth_flow, '_build_auth_manager', fake_build)

    url = oauth_flow.build_authorize_url()

    assert url == 'https://example.com/auth'
    assert sentinels['called_with'] is True
