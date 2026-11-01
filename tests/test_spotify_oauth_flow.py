"""The Spotify OAuth helpers return the tokens and URLs their callers rely on."""

import pytest

from pifitness.integrations.spotify import oauth_flow


@pytest.mark.regression
def test_force_refresh_clears_cache_and_returns_token(monkeypatch):
    """force_refresh clears the Spotify token cache and returns a freshly obtained token
    dict.
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


@pytest.mark.regression
def test_exchange_code_returns_token_dict(monkeypatch):
    """exchange_code returns the token dict from the auth manager's get_access_token."""
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


@pytest.mark.regression
def test_build_authorize_url_calls_show_dialog(monkeypatch):
    """build_authorize_url requests the consent dialog and returns the authorize URL."""
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
