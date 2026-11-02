"""get_auth_status returns the per-service status mapping that the auth status API
serves.
"""

import pytest
from pifitness.auth.status import get_auth_status


def _mock_db_returns(monkeypatch, sql_results):
    """Patch one_sql_result with a dispatcher based on query substring match."""
    def fake_one_sql_result(query, params=None):
        for pattern, value in sql_results.items():
            if pattern in query:
                return value
        return None
    monkeypatch.setattr(
        "pifitness.auth.status.one_sql_result",
        fake_one_sql_result,
    )


@pytest.mark.contract
def test_returns_expected_structure(monkeypatch):
    """get_auth_status returns a services mapping whose Spotify and Garmin entries carry
    exactly the documented status fields.
    """
    _mock_db_returns(monkeypatch, {})
    monkeypatch.setattr("pifitness.auth.status.sql_rate_limited", lambda: False)
    monkeypatch.setattr("pifitness.auth.status.spotify_creds", lambda: (None, None, None))
    monkeypatch.setattr("pifitness.auth.status.garmin_creds", lambda: (None, None))

    result = get_auth_status()

    assert isinstance(result, dict)
    assert "services" in result
    assert "Spotify" in result["services"]
    assert "Garmin" in result["services"]
    assert set(result["services"]["Spotify"].keys()) == {
        "status", "last_login_utc", "token_expires_utc", "rate_limited_until"
    }
    assert set(result["services"]["Garmin"].keys()) == {
        "status", "last_login_utc", "token_expires_utc", "last_error"
    }


@pytest.mark.contract
def test_missing_credentials_reports_missing(monkeypatch):
    """get_auth_status reports missing_credentials for both services when no credentials
    are configured.
    """
    _mock_db_returns(monkeypatch, {})
    monkeypatch.setattr("pifitness.auth.status.sql_rate_limited", lambda: False)
    monkeypatch.setattr("pifitness.auth.status.spotify_creds", lambda: (None, None, None))
    monkeypatch.setattr("pifitness.auth.status.garmin_creds", lambda: (None, None))

    result = get_auth_status()

    assert result["services"]["Spotify"]["status"] == "missing_credentials"
    assert result["services"]["Garmin"]["status"] == "missing_credentials"


@pytest.mark.contract
def test_ok_when_credentials_present(monkeypatch):
    """get_auth_status reports ok for both services when credentials exist and nothing
    overrides the status.
    """
    _mock_db_returns(monkeypatch, {})
    monkeypatch.setattr("pifitness.auth.status.sql_rate_limited", lambda: False)
    monkeypatch.setattr("pifitness.auth.status.spotify_creds", lambda: ("cid", "csec", "uri"))
    monkeypatch.setattr("pifitness.auth.status.garmin_creds", lambda: ("email", "pw"))

    result = get_auth_status()

    assert result["services"]["Spotify"]["status"] == "ok"
    assert result["services"]["Garmin"]["status"] == "ok"


@pytest.mark.contract
def test_spotify_rate_limited_reports_rate_limited(monkeypatch):
    """get_auth_status reports Spotify as rate_limited, with its clearance timestamp,
    while the rate limit is active.
    """
    _mock_db_returns(monkeypatch, {
        "rate_limit_cleared_utc": "2026-01-01T00:00:00",
    })
    monkeypatch.setattr("pifitness.auth.status.sql_rate_limited", lambda: True)
    monkeypatch.setattr("pifitness.auth.status.spotify_creds", lambda: ("cid", "csec", "uri"))
    monkeypatch.setattr("pifitness.auth.status.garmin_creds", lambda: ("email", "pw"))

    result = get_auth_status()

    assert result["services"]["Spotify"]["status"] == "rate_limited"
    assert result["services"]["Spotify"]["rate_limited_until"] == "2026-01-01T00:00:00"
