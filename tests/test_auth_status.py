"""Behavioral tests for get_auth_status.

Written BEFORE INT-9's extraction so we can prove the extraction preserves
behavior. These tests run twice:
  1. Pre-extraction: get_auth_status lives in backend_functions.service_logins
  2. Post-extraction: get_auth_status lives in pifitness.auth.status,
     re-exported from service_logins.

The test file imports from backend_functions.service_logins in both cases;
the re-export makes this stable. Dependencies are patched on the service_logins
module, which the extracted body resolves through via service_logins.X() calls
(see INT-9-fix spec for rationale).
"""

from backend_functions.service_logins import get_auth_status


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


def test_returns_expected_structure(monkeypatch):
    """get_auth_status always returns the documented dict shape."""
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


def test_missing_credentials_reports_missing(monkeypatch):
    """Both services report missing_credentials when creds are absent."""
    _mock_db_returns(monkeypatch, {})
    monkeypatch.setattr("pifitness.auth.status.sql_rate_limited", lambda: False)
    monkeypatch.setattr("pifitness.auth.status.spotify_creds", lambda: (None, None, None))
    monkeypatch.setattr("pifitness.auth.status.garmin_creds", lambda: (None, None))

    result = get_auth_status()

    assert result["services"]["Spotify"]["status"] == "missing_credentials"
    assert result["services"]["Garmin"]["status"] == "missing_credentials"


def test_ok_when_credentials_present(monkeypatch):
    """Both services report ok when creds exist and nothing else overrides."""
    _mock_db_returns(monkeypatch, {})
    monkeypatch.setattr("pifitness.auth.status.sql_rate_limited", lambda: False)
    monkeypatch.setattr("pifitness.auth.status.spotify_creds", lambda: ("cid", "csec", "uri"))
    monkeypatch.setattr("pifitness.auth.status.garmin_creds", lambda: ("email", "pw"))

    result = get_auth_status()

    assert result["services"]["Spotify"]["status"] == "ok"
    assert result["services"]["Garmin"]["status"] == "ok"


def test_spotify_rate_limited_reports_rate_limited(monkeypatch):
    """When Spotify is rate limited, its status overrides ok to rate_limited."""
    _mock_db_returns(monkeypatch, {
        "rate_limit_cleared_utc": "2026-01-01T00:00:00",
    })
    monkeypatch.setattr("pifitness.auth.status.sql_rate_limited", lambda: True)
    monkeypatch.setattr("pifitness.auth.status.spotify_creds", lambda: ("cid", "csec", "uri"))
    monkeypatch.setattr("pifitness.auth.status.garmin_creds", lambda: ("email", "pw"))

    result = get_auth_status()

    assert result["services"]["Spotify"]["status"] == "rate_limited"
    assert result["services"]["Spotify"]["rate_limited_until"] == "2026-01-01T00:00:00"