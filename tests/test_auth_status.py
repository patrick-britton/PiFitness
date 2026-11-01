"""
Unit tests for pifitness.auth.status.get_auth_status().

Covers shape, ok / missing_credentials / rate_limited branches for Spotify,
and the Garmin last_error branch. All DB and credential calls are mocked at
the pifitness.auth.status namespace (Rule D).

Contract-level coverage of the backend_functions.service_logins shim lives
in tests/test_service_logins_auth_status.py.
"""

from contextlib import ExitStack
from unittest.mock import patch

import pytest

from pifitness.auth.status import get_auth_status


def _patch_all(stack, *, rate_limited=False, spotify=None, garmin=None, sql_result=None):
    """Enter all four patches on the ExitStack. Later enter_context calls override earlier."""
    stack.enter_context(
        patch("pifitness.auth.status.sql_rate_limited", return_value=rate_limited)
    )
    stack.enter_context(
        patch("pifitness.auth.status.spotify_creds", return_value=spotify)
    )
    stack.enter_context(
        patch("pifitness.auth.status.garmin_creds", return_value=garmin)
    )
    stack.enter_context(
        patch("pifitness.auth.status.one_sql_result", return_value=sql_result)
    )


@pytest.mark.unit
def test_get_auth_status_returns_services_key():
    with ExitStack() as stack:
        _patch_all(
            stack,
            spotify=("id", "secret", "uri"),
            garmin=("email", "password"),
        )
        result = get_auth_status()
    assert "services" in result
    assert "Spotify" in result["services"]
    assert "Garmin" in result["services"]
    for svc in ("Spotify", "Garmin"):
        assert "status" in result["services"][svc]


@pytest.mark.unit
def test_spotify_ok_when_credentials_present():
    with ExitStack() as stack:
        _patch_all(
            stack,
            rate_limited=False,
            spotify=("id", "secret", "uri"),
            garmin=("email", "password"),
        )
        result = get_auth_status()
    assert result["services"]["Spotify"]["status"] == "ok"


@pytest.mark.unit
def test_spotify_missing_credentials():
    with ExitStack() as stack:
        _patch_all(
            stack,
            rate_limited=False,
            spotify=(None, None, None),
            garmin=("email", "password"),
        )
        result = get_auth_status()
    assert result["services"]["Spotify"]["status"] == "missing_credentials"


@pytest.mark.unit
def test_spotify_rate_limited():
    with ExitStack() as stack:
        _patch_all(
            stack,
            rate_limited=True,
            spotify=("id", "secret", "uri"),
            garmin=("email", "password"),
        )
        result = get_auth_status()
    assert result["services"]["Spotify"]["status"] == "rate_limited"


@pytest.mark.unit
def test_garmin_last_error_surfaced():
    with ExitStack() as stack:
        _patch_all(
            stack,
            rate_limited=False,
            spotify=("id", "secret", "uri"),
            garmin=("email", "password"),
        )
        stack.enter_context(
            patch(
                "pifitness.auth.status.one_sql_result",
                side_effect=lambda sql: "rate limit hit" if "error_text" in sql else None,
            )
        )
        result = get_auth_status()
    assert result["services"]["Garmin"]["last_error"] == "rate limit hit"